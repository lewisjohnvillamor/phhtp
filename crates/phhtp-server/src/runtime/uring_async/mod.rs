//! `uring-async`: an `async`/`await` runtime of our own on top of the raw
//! io_uring ring. No tokio, no mio, no futures crate — only `std::task`.
//!
//! * [`executor`] — single-threaded task scheduler with an `Rc`-backed waker.
//! * [`reactor`]  — the I/O driver: slots map completions back to wakers.
//! * [`io`]       — `accept`, `recv`, `send_all`, `sleep_ms` futures.
//!
//! The per-connection task below has the same shape as the Tokio one in
//! `tokio_rt.rs`, so the difference between the `tokio` and `uring-async` rows
//! is purely "Tokio's scheduler + epoll" versus "our scheduler + io_uring".
//! Workers are independent threads with their own ring, executor and
//! `SO_REUSEPORT` listener, like the `mio` and `uring` runtimes.

pub mod executor;
pub mod io;
pub mod reactor;

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;

use phhtp_core::{conn, process, App, Flow};

use self::executor::{spawn, Executor};
use self::reactor::Reactor;
use super::net::bind_reuseport;
use super::READ_CHUNK;
use crate::Config;

const ENTRIES: u32 = 4096;

pub fn run(cfg: Config, app: Arc<App>) -> std::io::Result<()> {
    super::uring::ring::Ring::new(8).map_err(|e| {
        std::io::Error::new(
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
                .name(format!("phhtp-uring-async-{i}"))
                .spawn(move || worker(l, app, i == 0))
        })
        .collect::<std::io::Result<_>>()?;

    for h in handles {
        h.join()
            .map_err(|_| std::io::Error::other("uring-async worker panicked"))??;
    }
    Ok(())
}

fn worker(listener: std::net::TcpListener, app: Arc<App>, log: bool) -> std::io::Result<()> {
    Reactor::install(ENTRIES)?;
    Executor::install();
    if log {
        eprintln!(
            "uring-async: setup_flags=0x{:x}",
            Reactor::with(|r| r.setup_flags())
        );
    }
    let listener_fd = listener.as_raw_fd();
    spawn(acceptor(listener_fd, app));

    loop {
        Executor::run_ready();
        Reactor::with(|r| r.wait())?;
    }
}

async fn acceptor(listener_fd: i32, app: Arc<App>) {
    loop {
        match io::accept(listener_fd).await {
            Ok(fd) => {
                // SAFETY: a fresh fd from accept that nothing else owns.
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };
                set_nodelay(fd.as_raw_fd());
                spawn(serve_connection(Arc::clone(&app), fd));
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionAborted => {}
            Err(e) => {
                eprintln!("accept: {e}");
                io::sleep_ms(10).await;
            }
        }
    }
}

fn set_nodelay(fd: i32) {
    // SAFETY: fd is a socket we own.
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
}

/// Same shape as `tokio_rt::serve_connection`; the socket closes when `fd` drops.
async fn serve_connection(app: Arc<App>, fd: OwnedFd) {
    let raw = fd.as_raw_fd();
    let max_in = conn::max_inbuf(&app);
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    loop {
        let (r, b) = io::recv(raw, inbuf).await;
        inbuf = b;
        match r {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        if inbuf.len() > max_in {
            return;
        }
        loop {
            let step = process(&app, &mut inbuf, &mut outbuf);
            if let Some(ms) = step.delay_ms {
                io::sleep_ms(ms).await;
            }
            if !outbuf.is_empty() {
                let (r, b) = io::send_all(raw, outbuf).await;
                outbuf = b;
                outbuf.clear();
                if r.is_err() {
                    return;
                }
            }
            if step.flow == Flow::Close {
                return;
            }
            if step.delay_ms.is_none() || inbuf.is_empty() {
                break;
            }
        }
    }
}
