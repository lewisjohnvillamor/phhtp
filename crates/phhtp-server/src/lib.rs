//! `phhtp` – one HTTP/1.1 server, selectable I/O runtime.
//!
//! ```text
//! phhtp --runtime tokio --bind 0.0.0.0:8080
//! ```

pub mod runtime;

use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use phhtp_core::App;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Runtime {
    /// Blocking std sockets, one OS thread per connection.
    Threads,
    /// Blocking std sockets, fixed pool of worker threads (`--workers`).
    Pool,
    /// Non-blocking epoll via `mio`, `--workers` independent event loops sharing the port.
    Mio,
    /// Tokio multi-thread scheduler with `--workers` threads.
    Tokio,
    /// Tokio current-thread scheduler (single thread, no work stealing).
    TokioSt,
    /// io_uring completion loop on raw syscalls, `--workers` rings sharing the port.
    Uring,
    /// Our own async/await executor + reactor on raw io_uring (no tokio, no futures crate).
    UringAsync,
}

impl Runtime {
    pub fn name(self) -> &'static str {
        match self {
            Runtime::Threads => "threads",
            Runtime::Pool => "pool",
            Runtime::Mio => "mio",
            Runtime::Tokio => "tokio",
            Runtime::TokioSt => "tokio-st",
            Runtime::Uring => "uring",
            Runtime::UringAsync => "uring-async",
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "phhtp", version, about)]
pub struct Cli {
    /// Which I/O model drives the sockets.
    #[arg(long, short, value_enum, default_value_t = Runtime::Tokio)]
    pub runtime: Runtime,

    /// Address to listen on.
    #[arg(long, short, default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    /// Worker count. Meaning depends on the runtime: pool size, number of mio
    /// event loops, or tokio worker threads. Defaults: pool = max(64, 16×cores),
    /// mio/tokio/uring = cores.
    #[arg(long, short)]
    pub workers: Option<usize>,

    /// HMAC secret for /sign and /verify.
    #[arg(long, env = "PHHTP_SECRET", default_value = "change-me")]
    pub secret: String,

    /// Maximum request body size in bytes.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub max_body: usize,

    /// Maximum request head (request line + headers) size in bytes.
    #[arg(long, default_value_t = 16 * 1024)]
    pub max_head: usize,
}

/// Everything a runtime needs to start.
pub struct Config {
    pub bind: SocketAddr,
    pub workers: usize,
    /// Receives the address actually bound (useful when binding port 0).
    pub ready: Option<std::sync::mpsc::Sender<SocketAddr>>,
}

pub fn cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub fn default_workers(rt: Runtime) -> usize {
    match rt {
        Runtime::Threads => 0,
        Runtime::Pool => (16 * cores()).max(64),
        Runtime::Mio | Runtime::Tokio | Runtime::Uring | Runtime::UringAsync => cores(),
        Runtime::TokioSt => 1,
    }
}

/// Run the chosen runtime. Never returns unless the listener fails.
pub fn serve(rt: Runtime, cfg: Config, app: Arc<App>) -> std::io::Result<()> {
    match rt {
        Runtime::Threads => runtime::threads::run(cfg, app),
        Runtime::Pool => runtime::pool::run(cfg, app),
        Runtime::Mio => runtime::mio_loop::run(cfg, app),
        Runtime::Tokio => runtime::tokio_rt::run(cfg, app, true),
        Runtime::TokioSt => runtime::tokio_rt::run(cfg, app, false),
        Runtime::Uring => runtime::uring::run(cfg, app),
        Runtime::UringAsync => runtime::uring_async::run(cfg, app),
    }
}
