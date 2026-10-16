//! # ringrt
//!
//! A small `async`/`await` runtime on **raw Linux io_uring**, with nothing in
//! between: no tokio, no mio, no `io-uring` crate, no `futures`. The only
//! dependency is `libc` for syscall numbers and `mmap`.
//!
//! It exists to answer "what does Tokio cost, and what would replacing it
//! take?" for a thread-per-core network server. See the repository README for
//! the benchmark that motivated it and the trade-off study.
//!
//! ```no_run
//! use ringrt::{net::TcpListener, spawn, Runtime};
//!
//! fn main() -> std::io::Result<()> {
//!     let rt = Runtime::new()?;
//!     rt.block_on(async {
//!         let listener = TcpListener::bind("127.0.0.1:8080".parse().unwrap())?;
//!         loop {
//!             let stream = listener.accept().await?;
//!             spawn(async move {
//!                 let mut buf = Vec::with_capacity(16 * 1024);
//!                 loop {
//!                     match stream.recv(&mut buf).await {
//!                         Ok(0) | Err(_) => return,
//!                         Ok(_) => {}
//!                     }
//!                     let (r, b) = stream.send_all(buf).await;
//!                     buf = b;
//!                     buf.clear();
//!                     if r.is_err() {
//!                         return;
//!                     }
//!                 }
//!             });
//!         }
//!     })?
//! }
//! ```
//!
//! ## Design
//!
//! * One [`Runtime`] per thread. Tasks are `!Send`; there is no work stealing.
//!   Scale by running one runtime per core, each with its own
//!   [`net::TcpListener::bind_reuseport`] listener.
//! * `recv` borrows `&mut Vec<u8>` like Tokio's `read`. It is sound with a
//!   completion API because the kernel never sees your memory: one multishot
//!   `RECV` per stream delivers into a registered provided-buffer ring, the
//!   reactor copies out and recycles the buffer at completion time. On kernels
//!   before 6.0 it degrades to single-shot `RECV` into reactor-owned staging.
//! * `send_all` takes its `Vec<u8>` **by value** and hands it back: the kernel
//!   reads from it, and a dropped future parks it in the reactor until the
//!   completion lands.
//! * `accept` is one multishot op per listener (5.19+, single-shot before).
//! * `sleep` is a kernel `TIMEOUT` op; there is no user-space timer wheel.
//!   [`time::timeout`] wraps any future with a deadline.
//! * [`spawn`] returns a [`JoinHandle`] that can be awaited or aborted.
//! * [`fs`]: `openat`, `statx` and `read` as ring ops, no blocking pool.
//! * [`net::TcpStream::send_detached_zc`]: `SEND_ZC` for large bodies.
//! * One `io_uring_enter` per scheduler turn submits every queued op and waits
//!   for completions.
//!
//! ## Requirements
//!
//! Linux ≥ 5.6 and `kernel.io_uring_disabled = 0`. Docker's default seccomp
//! profile blocks `io_uring_setup`; [`Runtime::available`] tells you up front.
//!
//! ## Modules
//!
//! * [`net`] / [`time`]: the high-level API.
//! * [`op`]: the raw operation futures the high-level API is built from.
//! * [`raw`]: the ring itself, for building a non-async event loop.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]
#![allow(clippy::needless_doctest_main)]

#[allow(missing_docs)]
pub mod bufring;
mod executor;
#[allow(missing_docs)]
pub mod fs;
pub mod net;
#[allow(missing_docs)]
pub mod op;
mod reactor;
mod ring;
mod runtime;
#[doc(hidden)]
#[allow(missing_docs)]
pub mod sys;
pub mod time;

pub use executor::{spawn, yield_now, JoinHandle, YieldNow};
pub use runtime::Runtime;

/// The ring without the executor: `Sqe`/`Cqe` layouts, opcodes and a
/// submission/completion wrapper, for writing a callback-style event loop.
pub mod raw {
    pub use crate::bufring::BufRing;
    pub use crate::ring::{available, Ring};
    pub use crate::sys;
    pub use crate::sys::{Cqe, KernelTimespec, Sqe};
}
