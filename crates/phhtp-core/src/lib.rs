//! `phhtp-core`: a small HTTP/1.1 engine with **no I/O in it at all**.
//!
//! The crate is deliberately "sans-IO": it turns bytes that arrived on a socket
//! into bytes that should be written back, and nothing else. Every server
//! runtime in this repository (blocking threads, a thread pool, a hand-rolled
//! `mio` event loop, Tokio multi-thread, Tokio current-thread) feeds the exact
//! same [`conn::process`] function. That is what makes the benchmark fair: the
//! only thing that differs between runtimes is how the sockets are driven.
//!
//! Layout:
//! * [`http`]  – request parsing on top of `httparse`, response encoding.
//! * [`date`]  – a per-thread cached `Date:` header formatter.
//! * [`sig`]   – HMAC-SHA256 sign / constant-time verify.
//! * [`app`]   – the router and endpoint handlers.
//! * [`conn`]  – the per-connection "drain input buffer, fill output buffer" loop.
//! * [`stats`] – `/stats`: the process's own CPU, memory, context-switch and disk counters.

pub mod app;
pub mod conn;
pub mod date;
pub mod http;
pub mod sig;
pub mod stats;

pub use app::{App, Outcome};
pub use conn::{process, Flow, Step};
pub use http::{parse, HttpError, Limits, Method, Parse, Request, Response};
