//! io_uring runtime, callback style, on `ringrt::raw` (the ring without the
//! executor). The same kernel features as `uring-async` with no futures in
//! between, so the gap between the two rows is the cost of `async`/`await`.
//!
//! * One ring, one `SO_REUSEPORT` listener and one provided-buffer ring per
//!   worker thread.
//! * Accept is multishot. Each connection arms **one multishot `RECV`** with
//!   `IOSQE_BUFFER_SELECT` for its whole life: completions deliver into
//!   kernel-provided buffers that are copied into the connection's input
//!   buffer and recycled immediately.
//! * Sends are single-shot from the connection's output buffer; at most one
//!   is in flight per connection. `/sleep` is a kernel `TIMEOUT` op.
//! * Both multishot forms fall back to single-shot on kernels that reject
//!   them (accept < 5.19, recv < 6.0), and recv falls back to a per-connection
//!   staging buffer if a buffer ring cannot be registered.
//!
//! Closing: a connection is dropped only after its recv op has ended
//! (EOF, error, or our `ASYNC_CANCEL` confirmed), so a completion can never
//! land on a recycled slot.

use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use phhtp_core::{conn, process, App, Flow, Step};
use ringrt::raw::{sys, BufRing, Cqe, KernelTimespec, Ring, Sqe};

use super::net::bind_reuseport;
use super::READ_CHUNK;
use crate::Config;

pub use ringrt::raw::available;

const ENTRIES: u32 = 4096;
/// 256 × 4 KiB = 1 MiB per worker; larger rings measured no faster
/// (see `ringrt::reactor`).
const BUF_ENTRIES: u32 = 256;
const BUF_SIZE: usize = 4096;

const KIND_ACCEPT: u64 = 1;
const KIND_RECV: u64 = 2;
const KIND_SEND: u64 = 3;
const KIND_TIMEOUT: u64 = 4;
/// Timer that re-arms accept after `EMFILE`/`ENFILE` without blocking the worker.
const KIND_ACCEPT_RETRY: u64 = 5;
/// `ASYNC_CANCEL` completions; carry no connection.
const KIND_CANCEL: u64 = 6;

fn user_data(kind: u64, idx: usize) -> u64 {
    ((idx as u64) << 8) | kind
}

struct Conn {
    fd: i32,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>,
    written: usize,
    /// A SEND is in flight.
    sending: bool,
    /// A TIMEOUT (for `/sleep`) is in flight; hold output until it fires.
    waiting: bool,
    /// Close as soon as nothing is in flight.
    closing: bool,
    /// The recv op is armed (multishot or single-shot).
    recv_armed: bool,
    /// Recv ended (EOF/error/cancel): no more input will come.
    recv_done: bool,
    /// We asked the kernel to cancel the recv; wait for its final CQE.
    cancel_sent: bool,
    /// Single-shot staging buffer when no buffer ring is available.
    fallback: Option<Box<[u8]>>,
    /// Lives in the `Box<Conn>` so its address is stable while a TIMEOUT is in flight.
    ts: KernelTimespec,
}

struct Worker {
    ring: Ring,
    bufs: Option<BufRing>,
    app: Arc<App>,
    listener_fd: i32,
    multishot_accept: bool,
    multishot_recv: bool,
    conns: Vec<Option<Box<Conn>>>,
    free: Vec<usize>,
    max_in: usize,
    retry_ts: KernelTimespec,
    recycled: bool,
}

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    Ring::new(8).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "io_uring unavailable ({e}); needs Linux >= 5.6 and kernel.io_uring_disabled=0"
            ),
        )
    })?;

    let workers = cfg.workers.max(1);
    let first = bind_reuseport(cfg.bind, false)?;
    let addr = first.local_addr()?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    let mut listeners = vec![first];
    for _ in 1..workers {
        listeners.push(bind_reuseport(addr, false)?);
    }

    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let app = Arc::clone(&app);
            std::thread::Builder::new()
                .name(format!("phhtp-uring-{i}"))
                .spawn(move || worker(l, app, i == 0))
        })
        .collect::<io::Result<_>>()?;

    for h in handles {
        h.join()
            .map_err(|_| io::Error::other("uring worker panicked"))??;
    }
    Ok(())
}

fn worker(listener: std::net::TcpListener, app: Arc<App>, log: bool) -> io::Result<()> {
    let ring = Ring::new(ENTRIES)?;
    let bufs = BufRing::new(ring.fd(), 0, BUF_ENTRIES, BUF_SIZE).ok();
    if log {
        eprintln!(
            "io_uring: setup_flags=0x{:x} features=0x{:x} buffer-ring={}",
            ring.setup_flags,
            ring.features,
            bufs.is_some()
        );
    }
    let mut w = Worker {
        max_in: conn::max_inbuf(&app),
        multishot_recv: bufs.is_some(),
        ring,
        bufs,
        app,
        listener_fd: listener.as_raw_fd(),
        multishot_accept: true,
        conns: Vec::new(),
        free: Vec::new(),
        retry_ts: KernelTimespec::default(),
        recycled: false,
    };
    w.submit_accept();

    let mut cqes: Vec<Cqe> = Vec::with_capacity(1024);
    loop {
        w.ring.submit(1)?;
        cqes.clear();
        w.ring.drain_completions(&mut cqes);
        for cqe in &cqes {
            w.handle(*cqe);
        }
        if w.recycled {
            if let Some(b) = &w.bufs {
                b.publish();
            }
            w.recycled = false;
        }
    }
}

impl Worker {
    fn push(&mut self, sqe: Sqe) {
        if !self.ring.push(sqe) {
            let _ = self.ring.submit(0);
            if !self.ring.push(sqe) {
                let _ = self.ring.submit(1);
                assert!(self.ring.push(sqe), "submission queue stuck");
            }
        }
    }

    fn submit_accept(&mut self) {
        self.push(Sqe {
            opcode: sys::OP_ACCEPT,
            fd: self.listener_fd,
            ioprio: if self.multishot_accept {
                sys::ACCEPT_MULTISHOT
            } else {
                0
            },
            op_flags: libc::SOCK_CLOEXEC as u32,
            user_data: user_data(KIND_ACCEPT, 0),
            ..Default::default()
        });
    }

    fn submit_recv(&mut self, idx: usize) {
        let multishot = self.multishot_recv;
        let bgid = self.bufs.as_ref().map(|b| b.bgid());
        let c = self.conns[idx].as_mut().expect("live conn");
        let sqe = if multishot {
            Sqe {
                opcode: sys::OP_RECV,
                fd: c.fd,
                ioprio: sys::RECV_MULTISHOT,
                flags: sys::IOSQE_BUFFER_SELECT,
                buf_index: bgid.expect("buffer ring present"),
                user_data: user_data(KIND_RECV, idx),
                ..Default::default()
            }
        } else {
            let buf = c
                .fallback
                .get_or_insert_with(|| vec![0u8; READ_CHUNK].into_boxed_slice());
            Sqe {
                opcode: sys::OP_RECV,
                fd: c.fd,
                addr: buf.as_mut_ptr() as u64,
                len: buf.len() as u32,
                user_data: user_data(KIND_RECV, idx),
                ..Default::default()
            }
        };
        c.recv_armed = true;
        self.push(sqe);
    }

    fn submit_send(&mut self, idx: usize) {
        let c = self.conns[idx].as_mut().expect("live conn");
        let fd = c.fd;
        let addr = c.outbuf[c.written..].as_ptr() as u64;
        let len = (c.outbuf.len() - c.written) as u32;
        c.sending = true;
        self.push(Sqe {
            opcode: sys::OP_SEND,
            fd,
            addr,
            len,
            op_flags: libc::MSG_NOSIGNAL as u32,
            user_data: user_data(KIND_SEND, idx),
            ..Default::default()
        });
    }

    fn submit_timeout(&mut self, idx: usize, ms: u64) {
        let c = self.conns[idx].as_mut().expect("live conn");
        c.ts = KernelTimespec {
            tv_sec: (ms / 1000) as i64,
            tv_nsec: ((ms % 1000) * 1_000_000) as i64,
        };
        c.waiting = true;
        let addr = &c.ts as *const KernelTimespec as u64;
        self.push(Sqe {
            opcode: sys::OP_TIMEOUT,
            fd: -1,
            addr,
            len: 1,
            user_data: user_data(KIND_TIMEOUT, idx),
            ..Default::default()
        });
    }

    fn submit_cancel_recv(&mut self, idx: usize) {
        let c = self.conns[idx].as_mut().expect("live conn");
        c.cancel_sent = true;
        self.push(Sqe {
            opcode: sys::OP_ASYNC_CANCEL,
            fd: -1,
            addr: user_data(KIND_RECV, idx),
            user_data: user_data(KIND_CANCEL, idx),
            ..Default::default()
        });
    }

    fn insert(&mut self, fd: i32) -> usize {
        let c = Box::new(Conn {
            fd,
            inbuf: Vec::with_capacity(READ_CHUNK),
            outbuf: Vec::with_capacity(READ_CHUNK),
            written: 0,
            sending: false,
            waiting: false,
            closing: false,
            recv_armed: false,
            recv_done: false,
            cancel_sent: false,
            fallback: None,
            ts: KernelTimespec::default(),
        });
        match self.free.pop() {
            Some(i) => {
                self.conns[i] = Some(c);
                i
            }
            None => {
                self.conns.push(Some(c));
                self.conns.len() - 1
            }
        }
    }

    /// Close if nothing is in flight; otherwise cancel the recv and wait.
    fn try_close(&mut self, idx: usize) {
        let c = self.conns[idx].as_mut().expect("live conn");
        c.closing = true;
        if c.sending || c.waiting {
            return;
        }
        if c.recv_armed && !c.recv_done {
            if !c.cancel_sent {
                self.submit_cancel_recv(idx);
            }
            return;
        }
        let c = self.conns[idx].take().expect("live conn");
        // SAFETY: fd owned by this connection; no op references it any more.
        unsafe {
            libc::close(c.fd);
        }
        self.free.push(idx);
    }

    fn handle(&mut self, cqe: Cqe) {
        let kind = cqe.user_data & 0xff;
        let idx = (cqe.user_data >> 8) as usize;
        match kind {
            KIND_ACCEPT => self.on_accept(cqe),
            KIND_ACCEPT_RETRY => self.submit_accept(),
            KIND_CANCEL => {}
            KIND_RECV if self.conns[idx].is_some() => self.on_recv(idx, cqe),
            KIND_SEND if self.conns[idx].is_some() => self.on_send(idx, cqe.res),
            KIND_TIMEOUT if self.conns[idx].is_some() => {
                self.conns[idx].as_mut().expect("live").waiting = false;
                self.advance(idx);
            }
            _ => {}
        }
    }

    fn on_accept(&mut self, cqe: Cqe) {
        let more = cqe.flags & sys::CQE_F_MORE != 0;
        if cqe.res < 0 {
            let err = -cqe.res;
            if err == libc::EINVAL && self.multishot_accept {
                eprintln!("io_uring: kernel rejected multishot accept; using single-shot");
                self.multishot_accept = false;
            } else if err == libc::EMFILE || err == libc::ENFILE {
                eprintln!("accept: {}", io::Error::from_raw_os_error(err));
                self.retry_ts = KernelTimespec {
                    tv_sec: 0,
                    tv_nsec: 10_000_000,
                };
                let addr = &self.retry_ts as *const KernelTimespec as u64;
                self.push(Sqe {
                    opcode: sys::OP_TIMEOUT,
                    fd: -1,
                    addr,
                    len: 1,
                    user_data: user_data(KIND_ACCEPT_RETRY, 0),
                    ..Default::default()
                });
                return;
            } else if err != libc::ECONNABORTED && err != libc::EAGAIN && err != libc::EINTR {
                eprintln!("accept: {}", io::Error::from_raw_os_error(err));
            }
            if !more {
                self.submit_accept();
            }
            return;
        }
        let fd = cqe.res;
        // SAFETY: fd is a fresh socket we own.
        unsafe {
            let one: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        let idx = self.insert(fd);
        self.submit_recv(idx);
        if !more {
            self.submit_accept();
        }
    }

    fn on_recv(&mut self, idx: usize, cqe: Cqe) {
        let more = cqe.flags & sys::CQE_F_MORE != 0;
        let multishot = self.multishot_recv;
        let max_in = self.max_in;
        let mut rearm = false;
        {
            let bufs = self.bufs.as_mut();
            let c = self.conns[idx].as_mut().expect("live conn");
            if cqe.res > 0 {
                let n = cqe.res as usize;
                if cqe.flags & sys::CQE_F_BUFFER != 0 {
                    let bid = (cqe.flags >> sys::CQE_BUFFER_SHIFT) as u16;
                    let b = bufs.expect("buffer completion without a buffer ring");
                    // SAFETY: the kernel wrote `n` bytes into buffer `bid`.
                    let data = unsafe { std::slice::from_raw_parts(b.buf(bid), n) };
                    c.inbuf.extend_from_slice(data);
                    b.recycle(bid);
                    self.recycled = true;
                } else if let Some(fb) = &c.fallback {
                    c.inbuf.extend_from_slice(&fb[..n]);
                }
                if c.inbuf.len() > max_in {
                    c.closing = true;
                }
            }
            if !more {
                c.recv_armed = false;
                match cqe.res {
                    r if r > 0 => rearm = true,
                    0 => c.recv_done = true,
                    r if -r == libc::ENOBUFS => rearm = true,
                    r if -r == libc::EINVAL && multishot => {
                        rearm = true; // handled below: flip to single-shot
                    }
                    _ => c.recv_done = true, // ECANCELED or a real error
                }
            }
        }
        if !more && cqe.res < 0 && -cqe.res == libc::EINVAL && self.multishot_recv {
            eprintln!("io_uring: kernel rejected multishot recv; using single-shot");
            self.multishot_recv = false;
        }
        if rearm {
            let closing = self.conns[idx].as_ref().expect("live").closing;
            if !closing {
                self.submit_recv(idx);
            }
        }
        self.advance(idx);
    }

    fn on_send(&mut self, idx: usize, res: i32) {
        {
            let c = self.conns[idx].as_mut().expect("live conn");
            c.sending = false;
            if res < 0 {
                c.closing = true;
                c.outbuf.clear();
                c.written = 0;
            } else {
                c.written += res as usize;
                if c.written >= c.outbuf.len() {
                    c.outbuf.clear();
                    c.written = 0;
                }
            }
        }
        self.advance(idx);
    }

    /// Make progress: flush output, then process buffered input, then close.
    fn advance(&mut self, idx: usize) {
        loop {
            let (sending, waiting, has_out, has_in, closing, recv_done) = {
                let c = self.conns[idx].as_ref().expect("live conn");
                (
                    c.sending,
                    c.waiting,
                    !c.outbuf.is_empty(),
                    !c.inbuf.is_empty(),
                    c.closing,
                    c.recv_done,
                )
            };
            if sending || waiting {
                return;
            }
            if has_out {
                self.submit_send(idx);
                return;
            }
            if closing {
                self.try_close(idx);
                return;
            }
            if has_in {
                let step: Step = {
                    let app = Arc::clone(&self.app);
                    let c = self.conns[idx].as_mut().expect("live conn");
                    process(&app, &mut c.inbuf, &mut c.outbuf)
                };
                if step.flow == Flow::Close {
                    self.conns[idx].as_mut().expect("live").closing = true;
                }
                if let Some(ms) = step.delay_ms {
                    self.submit_timeout(idx, ms);
                    return;
                }
                let produced = !self.conns[idx].as_ref().expect("live").outbuf.is_empty();
                if !produced {
                    // Only a partial request is buffered; wait for more input.
                    if recv_done {
                        self.try_close(idx);
                    }
                    return;
                }
                continue;
            }
            if recv_done {
                self.try_close(idx);
            }
            return;
        }
    }
}
