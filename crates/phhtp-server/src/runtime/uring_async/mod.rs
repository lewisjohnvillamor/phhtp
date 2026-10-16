//! `uring-async`: the server on top of the [`ringrt`] crate, our own
//! `async`/`await` runtime on raw io_uring (no tokio, no mio, no futures crate).
//!
//! `serve_connection` below has the same shape as the Tokio one in
//! `tokio_rt.rs`, so the `tokio` and `uring-async` rows compare "Tokio's
//! scheduler + epoll" against "ringrt's scheduler + io_uring" with identical
//! handler code. Workers are independent threads, each with its own `Runtime`
//! and `SO_REUSEPORT` listener.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use phhtp_core::{conn, process, App, Flow};
use ringrt::net::{TcpListener, TcpStream};
use ringrt::{spawn, Runtime};

use super::READ_CHUNK;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    if !Runtime::available() {
        return Err(io::Error::other(
            "io_uring unavailable; needs Linux >= 5.6 and kernel.io_uring_disabled=0",
        ));
    }
    let workers = cfg.workers.max(1);
    let first = TcpListener::bind_reuseport(cfg.bind)?;
    let addr = first.local_addr()?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    let mut listeners = vec![first];
    for _ in 1..workers {
        listeners.push(TcpListener::bind_reuseport(addr)?);
    }

    // A bound listener is plain `Send` data; only the accept *future* is
    // thread-bound. Each worker takes one listener into its own runtime.
    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let app = Arc::clone(&app);
            std::thread::Builder::new()
                .name(format!("phhtp-uring-async-{i}"))
                .spawn(move || worker(listener, app))
        })
        .collect::<io::Result<_>>()?;

    for h in handles {
        h.join()
            .map_err(|_| io::Error::other("uring-async worker panicked"))??;
    }
    Ok(())
}

fn worker(listener: TcpListener, app: Arc<App>) -> io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async move {
        loop {
            match listener.accept().await {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    spawn(serve_connection(Arc::clone(&app), stream));
                }
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {}
                Err(e) => {
                    eprintln!("accept: {e}");
                    ringrt::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })?
}

/// Same shape as `tokio_rt::serve_connection`; the socket closes when `stream` drops.
async fn serve_connection(app: Arc<App>, stream: TcpStream) {
    let max_in = conn::max_inbuf(&app);
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    loop {
        let (r, b) = stream.recv(inbuf).await;
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
                ringrt::time::sleep(Duration::from_millis(ms)).await;
            }
            if !outbuf.is_empty() {
                let (r, b) = stream.send_all(outbuf).await;
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
