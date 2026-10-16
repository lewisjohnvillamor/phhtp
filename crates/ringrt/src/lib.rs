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
//!                     let (r, b) = stream.recv(buf).await;
//!                     buf = b;
//!                     match r {
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
//! * Buffers are passed **by value** to `recv` / `send_all` and handed back
//!   with the result. With a completion API the kernel owns the buffer until
//!   it says otherwise; this is the only sound way to express that in safe
//!   Rust without a lifetime the kernel cannot see. A future dropped
//!   mid-flight parks its buffer in the reactor until the completion lands.
//! * `sleep` is a kernel `TIMEOUT` op; there is no user-space timer wheel.
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

mod executor;
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

pub use executor::{spawn, yield_now, YieldNow};
pub use runtime::Runtime;

/// The ring without the executor: `Sqe`/`Cqe` layouts, opcodes and a
/// submission/completion wrapper, for writing a callback-style event loop.
pub mod raw {
    pub use crate::ring::{available, Ring};
    pub use crate::sys;
    pub use crate::sys::{Cqe, KernelTimespec, Sqe};
}
