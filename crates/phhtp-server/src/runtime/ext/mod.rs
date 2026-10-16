//! The open-source io_uring runtimes, each driving the same `process()` with a
//! `serve_connection` of the same shape as `tokio_rt` / `uring_async`, so the
//! rows are comparable. Every one is thread-per-core here: `--workers` threads,
//! each with its own runtime and its own `SO_REUSEPORT` listener.

pub mod compio_rt;
pub mod glommio_rt;
pub mod monoio_rt;
pub mod tokio_uring_rt;

use std::io;
use std::net::TcpListener;
use std::sync::Arc;

use phhtp_core::App;

use super::net::bind_reuseport;
use crate::Config;

/// Bind one listener per worker and run `worker` on each thread.
pub(crate) fn per_core(
    cfg: Config,
    app: Arc<App>,
    name: &'static str,
    nonblocking: bool,
    worker: fn(TcpListener, Arc<App>) -> io::Result<()>,
) -> io::Result<()> {
    let workers = cfg.workers.max(1);
    let first = bind_reuseport(cfg.bind, nonblocking)?;
    let addr = first.local_addr()?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    let mut listeners = vec![first];
    for _ in 1..workers {
        listeners.push(bind_reuseport(addr, nonblocking)?);
    }
    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let app = Arc::clone(&app);
            std::thread::Builder::new()
                .name(format!("phhtp-{name}-{i}"))
                .spawn(move || worker(l, app))
        })
        .collect::<io::Result<_>>()?;
    for h in handles {
        h.join()
            .map_err(|_| io::Error::other(format!("{name} worker panicked")))??;
    }
    Ok(())
}
