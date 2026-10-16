pub mod blocking;
pub mod mio_loop;
pub mod net;
pub mod pool;
pub mod threads;
pub mod tokio_rt;
pub mod uring;

/// Bytes requested from the kernel per `read` call.
pub const READ_CHUNK: usize = 16 * 1024;
