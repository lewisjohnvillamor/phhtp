//! Hand-rolled non-blocking event loop on `mio` (epoll on Linux).
//!
//! `--workers N` starts N completely independent event loops, each with its own
//! `epoll` instance and its own listening socket bound with `SO_REUSEPORT`, so the
//! kernel load-balances incoming connections across them. Nothing is shared
//! between workers except the read-only `App`; no work stealing, no locks.
//!
//! mio registers with edge-triggered semantics on Linux, so every readable event
//! must be drained to `WouldBlock` (or a short read) and every write must be
//! retried on the next writable edge. `/sleep` is implemented with a timer heap
//! that sets the `poll` timeout.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use phhtp_core::{conn, process, App, Flow};

use super::net::bind_reuseport;
use super::READ_CHUNK;
use crate::Config;

const LISTENER: Token = Token(usize::MAX);

/// (deadline, slab index, generation)
type Timer = Reverse<(Instant, usize, u64)>;

struct Conn {
    stream: TcpStream,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>,
    /// Read target; only the bytes that arrive are copied into `inbuf`.
    scratch: Vec<u8>,
    written: usize,
    /// A `/sleep` timer is pending: do not process or write until it fires.
    waiting: bool,
    /// Close once `outbuf` has been flushed.
    closing: bool,
    /// Read side saw EOF.
    peer_closed: bool,
    /// Guards against a timer firing for a recycled slab slot.
    gen: u64,
}

impl Conn {
    fn new(stream: TcpStream, gen: u64) -> Self {
        Self {
            stream,
            inbuf: Vec::with_capacity(READ_CHUNK),
            outbuf: Vec::with_capacity(READ_CHUNK),
            scratch: vec![0u8; READ_CHUNK],
            written: 0,
            waiting: false,
            closing: false,
            peer_closed: false,
            gen,
        }
    }
}

#[derive(Default)]
struct Slab {
    slots: Vec<Option<Conn>>,
    free: Vec<usize>,
}

impl Slab {
    fn insert(&mut self, c: Conn) -> usize {
        match self.free.pop() {
            Some(i) => {
                self.slots[i] = Some(c);
                i
            }
            None => {
                self.slots.push(Some(c));
                self.slots.len() - 1
            }
        }
    }
    fn get_mut(&mut self, i: usize) -> Option<&mut Conn> {
        self.slots.get_mut(i).and_then(|s| s.as_mut())
    }
    fn remove(&mut self, i: usize) -> Option<Conn> {
        let c = self.slots.get_mut(i).and_then(|s| s.take());
        if c.is_some() {
            self.free.push(i);
        }
        c
    }
}

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    let workers = cfg.workers.max(1);
    let first = bind_reuseport(cfg.bind, true)?;
    let addr = first.local_addr()?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    let mut listeners = vec![first];
    for _ in 1..workers {
        listeners.push(bind_reuseport(addr, true)?);
    }

    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let app = Arc::clone(&app);
            std::thread::Builder::new()
                .name(format!("phhtp-mio-{i}"))
                .spawn(move || worker(l, app))
        })
        .collect::<io::Result<_>>()?;

    for h in handles {
        h.join()
            .map_err(|_| io::Error::other("mio worker panicked"))??;
    }
    Ok(())
}

fn worker(listener: std::net::TcpListener, app: Arc<App>) -> io::Result<()> {
    let mut listener = TcpListener::from_std(listener);
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    let mut slab = Slab::default();
    let mut timers: BinaryHeap<Timer> = BinaryHeap::new();
    let mut next_gen: u64 = 0;
    let max_in = conn::max_inbuf(&app);

    loop {
        let timeout = timers
            .peek()
            .map(|Reverse((t, _, _))| t.saturating_duration_since(Instant::now()));
        match poll.poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }

        for event in events.iter() {
            match event.token() {
                LISTENER => accept_all(&mut listener, &mut slab, poll.registry(), &mut next_gen),
                Token(idx) => {
                    let Some(c) = slab.get_mut(idx) else { continue };
                    let mut keep = !event.is_error();
                    if keep && (event.is_readable() || event.is_read_closed()) {
                        keep = read_all(c, max_in);
                    }
                    if keep {
                        keep = advance(c, idx, &app, &mut timers);
                    }
                    if !keep {
                        close(&mut slab, idx, poll.registry());
                    }
                }
            }
        }

        if !timers.is_empty() {
            let now = Instant::now();
            while let Some(&Reverse((deadline, idx, gen))) = timers.peek() {
                if deadline > now {
                    break;
                }
                timers.pop();
                let Some(c) = slab.get_mut(idx) else { continue };
                if c.gen != gen || !c.waiting {
                    continue;
                }
                c.waiting = false;
                if !advance(c, idx, &app, &mut timers) {
                    close(&mut slab, idx, poll.registry());
                }
            }
        }
    }
}

fn accept_all(
    listener: &mut TcpListener,
    slab: &mut Slab,
    registry: &Registry,
    next_gen: &mut u64,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                let gen = *next_gen;
                *next_gen += 1;
                let idx = slab.insert(Conn::new(stream, gen));
                let c = slab.get_mut(idx).expect("just inserted");
                // Register both interests once. Edge-triggered: a writable event
                // only fires when the send buffer transitions from full to free,
                // so this does not cause busy wake-ups.
                if registry
                    .register(
                        &mut c.stream,
                        Token(idx),
                        Interest::READABLE | Interest::WRITABLE,
                    )
                    .is_err()
                {
                    slab.remove(idx);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                eprintln!("accept: {e}");
                break;
            }
        }
    }
}

fn close(slab: &mut Slab, idx: usize, registry: &Registry) {
    if let Some(mut c) = slab.remove(idx) {
        let _ = registry.deregister(&mut c.stream);
    }
}

/// Drain the socket into `inbuf`. Returns `false` if the connection must be dropped.
fn read_all(c: &mut Conn, max_in: usize) -> bool {
    loop {
        match c.stream.read(&mut c.scratch) {
            Ok(0) => {
                c.peer_closed = true;
                return true;
            }
            Ok(n) => {
                c.inbuf.extend_from_slice(&c.scratch[..n]);
                if c.inbuf.len() > max_in {
                    return false;
                }
                if n < c.scratch.len() {
                    // Short read: the receive buffer is drained; the next arrival
                    // produces a fresh edge. Saves one EAGAIN syscall per wake-up.
                    return true;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}

/// Flush pending output, then process pending input, repeating until the
/// connection is blocked on the socket or on a timer. Returns `false` to close.
fn advance(c: &mut Conn, idx: usize, app: &App, timers: &mut BinaryHeap<Timer>) -> bool {
    loop {
        if c.waiting {
            return true;
        }
        while c.written < c.outbuf.len() {
            match c.stream.write(&c.outbuf[c.written..]) {
                Ok(0) => return false,
                Ok(n) => c.written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        c.outbuf.clear();
        c.written = 0;
        if c.closing {
            return false;
        }
        if c.inbuf.is_empty() {
            return !c.peer_closed;
        }
        let step = process(app, &mut c.inbuf, &mut c.outbuf);
        if step.flow == Flow::Close {
            c.closing = true;
        }
        if let Some(ms) = step.delay_ms {
            c.waiting = true;
            timers.push(Reverse((
                Instant::now() + Duration::from_millis(ms),
                idx,
                c.gen,
            )));
            return true;
        }
        if c.outbuf.is_empty() {
            // Only a partial request is buffered; wait for more input.
            return !c.peer_closed;
        }
    }
}
