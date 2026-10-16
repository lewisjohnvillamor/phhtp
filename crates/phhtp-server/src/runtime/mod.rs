pub mod blocking;
#[cfg(feature = "ext")]
pub mod ext;
pub mod mio_loop;
pub mod net;
pub mod pool;
pub mod threads;
pub mod tokio_rt;
pub mod uring;
pub mod uring_async;

/// Bytes requested from the kernel per `read` call.
pub const READ_CHUNK: usize = 16 * 1024;
