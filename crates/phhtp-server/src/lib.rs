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
    /// ByteDance's monoio (thread-per-core, io_uring). Needs the `ext` feature.
    Monoio,
    /// compio (thread-per-core, completion-based). Needs the `ext` feature.
    Compio,
    /// Datadog's glommio (thread-per-core, io_uring). Needs the `ext` feature.
    Glommio,
    /// tokio-uring (tokio current-thread + io_uring driver). Needs the `ext` feature.
    TokioUring,
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
            Runtime::Monoio => "monoio",
            Runtime::Compio => "compio",
            Runtime::Glommio => "glommio",
            Runtime::TokioUring => "tokio-uring",
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

    /// Serve `GET /static/<name>` from this directory (threads, pool, mio,
    /// tokio, tokio-st and uring-async; the others answer 501).
    #[arg(long)]
    pub static_dir: Option<std::path::PathBuf>,

    /// Close a connection that has sent nothing for this many seconds
    /// (uring-async only; 0 disables).
    #[arg(long, default_value_t = 30)]
    pub idle_timeout: u64,

    /// Shed new connections beyond this many open ones per worker
    /// (uring-async only; 0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub max_conns: usize,

    /// On SIGTERM/SIGINT: stop accepting, let in-flight connections finish
    /// for up to this many seconds, then close them (uring-async only).
    #[arg(long, default_value_t = 10)]
    pub shutdown_grace: u64,
}

/// Everything a runtime needs to start.
pub struct Config {
    pub bind: SocketAddr,
    pub workers: usize,
    /// Receives the address actually bound (useful when binding port 0).
    pub ready: Option<std::sync::mpsc::Sender<SocketAddr>>,
    /// Idle-connection timeout (honoured by `uring-async`).
    pub idle_timeout: Option<std::time::Duration>,
    /// Open-connection cap per worker (honoured by `uring-async`).
    pub max_conns: Option<usize>,
    /// Drain time on shutdown (honoured by `uring-async`).
    pub shutdown_grace: std::time::Duration,
}

impl Config {
    /// A config with the defaults the CLI uses.
    pub fn new(bind: SocketAddr, workers: usize) -> Config {
        Config {
            bind,
            workers,
            ready: None,
            idle_timeout: Some(std::time::Duration::from_secs(30)),
            max_conns: None,
            shutdown_grace: std::time::Duration::from_secs(10),
        }
    }
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
        Runtime::Mio
        | Runtime::Tokio
        | Runtime::Uring
        | Runtime::UringAsync
        | Runtime::Monoio
        | Runtime::Compio
        | Runtime::Glommio
        | Runtime::TokioUring => cores(),
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
        #[cfg(feature = "ext")]
        Runtime::Monoio => runtime::ext::monoio_rt::run(cfg, app),
        #[cfg(feature = "ext")]
        Runtime::Compio => runtime::ext::compio_rt::run(cfg, app),
        #[cfg(feature = "ext")]
        Runtime::Glommio => runtime::ext::glommio_rt::run(cfg, app),
        #[cfg(feature = "ext")]
        Runtime::TokioUring => runtime::ext::tokio_uring_rt::run(cfg, app),
        #[cfg(not(feature = "ext"))]
        Runtime::Monoio | Runtime::Compio | Runtime::Glommio | Runtime::TokioUring => {
            Err(std::io::Error::other("built without the `ext` feature"))
        }
    }
}
