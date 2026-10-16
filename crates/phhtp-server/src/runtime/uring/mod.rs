//! io_uring runtime, callback style, on `ringrt::raw` (the ring without the executor).
//!
//! Unlike epoll (`mio`), io_uring is a *completion* model: we don't ask "is this
//! socket readable?" and then call `read`; we hand the kernel a `RECV` with a
//! buffer and get a completion when the bytes are already in it. Each worker
//! thread owns one ring and one `SO_REUSEPORT` listener, exactly like the `mio`
//! runtime, so the two are directly comparable.
//!
//! Per connection exactly one operation is in flight at a time:
//!
//! ```text
//! accept ──► RECV ──► process() ──► SEND ──► (inbuf left over? process : RECV)
//!                        │
//!                        └─ /sleep ──► TIMEOUT ──► SEND
//! ```
//!
//! Keeping it to one op per connection means the kernel never writes into a
//! buffer we are reading, and a connection can be closed the moment its only
//! outstanding completion arrives, so no stale completions are possible.
//!
//! Accept is multishot (one SQE yields a stream of completions) on kernels that
//! support it (5.19+) and transparently falls back to single-shot otherwise.

use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use phhtp_core::{conn, process, App, Flow, Step};

use ringrt::raw::{sys, Cqe, KernelTimespec, Ring, Sqe};

use super::net::bind_reuseport;
use super::READ_CHUNK;
use crate::Config;

pub use ringrt::raw::available;

const ENTRIES: u32 = 4096;

const KIND_ACCEPT: u64 = 1;
const KIND_RECV: u64 = 2;
const KIND_SEND: u64 = 3;
const KIND_TIMEOUT: u64 = 4;
/// Timer that re-arms accept after `EMFILE`/`ENFILE` without blocking the worker.
const KIND_ACCEPT_RETRY: u64 = 5;

fn user_data(kind: u64, idx: usize) -> u64 {
    ((idx as u64) << 8) | kind
}

struct Conn {
    fd: i32,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>,
    written: usize,
    closing: bool,
    /// Lives in the `Box<Conn>` so its address is stable while a TIMEOUT is in flight.
    ts: KernelTimespec,
}

struct Worker {
    ring: Ring,
    app: Arc<App>,
    listener_fd: i32,
    multishot: bool,
    conns: Vec<Option<Box<Conn>>>,
    free: Vec<usize>,
    max_in: usize,
    /// Address handed to the kernel for the accept-retry timer; `Worker` is
    /// never moved after construction so this stays valid.
    retry_ts: KernelTimespec,
}

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    // Fail fast with a readable message rather than from inside a worker.
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
    // The ring must be created on the thread that submits (SINGLE_ISSUER).
    let ring = Ring::new(ENTRIES)?;
    if log {
        eprintln!(
            "io_uring: setup_flags=0x{:x} features=0x{:x} multishot-accept=try",
            ring.setup_flags, ring.features
        );
    }
    let mut w = Worker {
        max_in: conn::max_inbuf(&app),
        ring,
        app,
        listener_fd: listener.as_raw_fd(),
        multishot: true,
        conns: Vec::new(),
        free: Vec::new(),
        retry_ts: KernelTimespec::default(),
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
    }
}

impl Worker {
    fn push(&mut self, sqe: Sqe) {
        if !self.ring.push(sqe) {
            // Queue full: flush without waiting, then it must fit.
            let _ = self.ring.submit(0);
            if !self.ring.push(sqe) {
                // Still full (kernel is backed up): block until something completes.
                let _ = self.ring.submit(1);
                assert!(self.ring.push(sqe), "submission queue stuck");
            }
        }
    }

    fn submit_accept(&mut self) {
        self.push(Sqe {
            opcode: sys::OP_ACCEPT,
            fd: self.listener_fd,
            ioprio: if self.multishot {
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
        let c = self.conns[idx].as_mut().expect("live conn");
        c.inbuf.reserve(READ_CHUNK);
        let spare = c.inbuf.capacity() - c.inbuf.len();
        // SAFETY: pointer into the Vec's spare capacity; the Vec is not touched
        // again until the RECV completes (one op in flight per connection).
        let addr = unsafe { c.inbuf.as_mut_ptr().add(c.inbuf.len()) } as u64;
        let fd = c.fd;
        self.push(Sqe {
            opcode: sys::OP_RECV,
            fd,
            addr,
            len: spare as u32,
            user_data: user_data(KIND_RECV, idx),
            ..Default::default()
        });
    }

    fn submit_send(&mut self, idx: usize) {
        let c = self.conns[idx].as_ref().expect("live conn");
        let fd = c.fd;
        let addr = c.outbuf[c.written..].as_ptr() as u64;
        let len = (c.outbuf.len() - c.written) as u32;
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

    fn close(&mut self, idx: usize) {
        if let Some(c) = self.conns[idx].take() {
            // SAFETY: fd owned by this connection; its only op has completed.
            unsafe {
                libc::close(c.fd);
            }
            self.free.push(idx);
        }
    }

    fn insert(&mut self, fd: i32) -> usize {
        let c = Box::new(Conn {
            fd,
            inbuf: Vec::with_capacity(READ_CHUNK),
            outbuf: Vec::with_capacity(READ_CHUNK),
            written: 0,
            closing: false,
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

    fn handle(&mut self, cqe: Cqe) {
        let kind = cqe.user_data & 0xff;
        let idx = (cqe.user_data >> 8) as usize;
        match kind {
            KIND_ACCEPT => self.on_accept(cqe),
            KIND_RECV => self.on_recv(idx, cqe.res),
            KIND_SEND => self.on_send(idx, cqe.res),
            // -ETIME is the normal "it fired" result for a TIMEOUT.
            KIND_TIMEOUT if self.conns[idx].is_some() => self.submit_send(idx),
            KIND_ACCEPT_RETRY => self.submit_accept(),
            _ => {}
        }
    }

    fn on_accept(&mut self, cqe: Cqe) {
        let more = cqe.flags & sys::CQE_F_MORE != 0;
        if cqe.res < 0 {
            let err = -cqe.res;
            if err == libc::EINVAL && self.multishot {
                // Kernel predates multishot accept (5.19): fall back.
                self.multishot = false;
            } else if err == libc::EMFILE || err == libc::ENFILE {
                // Out of descriptors. Back off with a kernel timer instead of
                // sleeping the thread, so this worker's live connections keep
                // being served; the timer completion re-arms accept.
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

    fn on_recv(&mut self, idx: usize, res: i32) {
        if res <= 0 {
            // EOF or error. Nothing can be pending for output here (we only
            // RECV with an empty outbuf), so just drop it.
            self.close(idx);
            return;
        }
        let step = {
            let c = self.conns[idx].as_mut().expect("live conn");
            // SAFETY: the kernel wrote exactly `res` bytes into the spare capacity.
            unsafe {
                c.inbuf.set_len(c.inbuf.len() + res as usize);
            }
            if c.inbuf.len() > self.max_in {
                None
            } else {
                Some(process(&self.app, &mut c.inbuf, &mut c.outbuf))
            }
        };
        match step {
            None => self.close(idx),
            Some(step) => self.after_process(idx, step),
        }
    }

    fn on_send(&mut self, idx: usize, res: i32) {
        if res < 0 {
            self.close(idx);
            return;
        }
        let next = {
            let c = self.conns[idx].as_mut().expect("live conn");
            c.written += res as usize;
            if c.written < c.outbuf.len() {
                Next::Send
            } else {
                c.outbuf.clear();
                c.written = 0;
                if c.closing {
                    Next::Close
                } else if c.inbuf.is_empty() {
                    Next::Recv
                } else {
                    Next::Step(process(&self.app, &mut c.inbuf, &mut c.outbuf))
                }
            }
        };
        match next {
            Next::Send => self.submit_send(idx),
            Next::Close => self.close(idx),
            Next::Recv => self.submit_recv(idx),
            Next::Step(step) => self.after_process(idx, step),
        }
    }

    fn after_process(&mut self, idx: usize, step: Step) {
        let (closing, has_out) = {
            let c = self.conns[idx].as_mut().expect("live conn");
            if step.flow == Flow::Close {
                c.closing = true;
            }
            (c.closing, !c.outbuf.is_empty())
        };
        if let Some(ms) = step.delay_ms {
            self.submit_timeout(idx, ms);
        } else if has_out {
            self.submit_send(idx);
        } else if closing {
            self.close(idx);
        } else {
            // Only a partial request is buffered; wait for the rest.
            self.submit_recv(idx);
        }
    }
}

enum Next {
    Send,
    Close,
    Recv,
    Step(Step),
}
