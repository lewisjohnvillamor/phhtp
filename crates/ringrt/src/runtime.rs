use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::rc::Rc;

use crate::executor::{spawn, Executor};
use crate::reactor::Reactor;

/// A single-threaded runtime: one io_uring ring, one task queue, this thread.
///
/// For a thread-per-core server, create one per thread. Dropping it cancels
/// in-flight operations and closes the ring.
pub struct Runtime {
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Runtime {
    /// Create a runtime with a 4096-entry submission queue.
    pub fn new() -> io::Result<Runtime> {
        Self::with_entries(4096)
    }

    /// Create a runtime with a submission queue of `entries` SQEs (power of two).
    pub fn with_entries(entries: u32) -> io::Result<Runtime> {
        if Reactor::is_installed() || Executor::is_installed() {
            return Err(io::Error::other(
                "a ringrt runtime is already installed on this thread",
            ));
        }
        Reactor::install(entries)?;
        Executor::install();
        Ok(Runtime {
            _not_send: std::marker::PhantomData,
        })
    }

    /// The `IORING_SETUP_*` flags the kernel accepted for this ring.
    pub fn setup_flags(&self) -> u32 {
        Reactor::with(|r| r.setup_flags())
    }

    /// `true` if this kernel and sandbox allow creating a ring at all.
    pub fn available() -> bool {
        crate::ring::available()
    }

    /// Run `future` to completion, driving every spawned task meanwhile.
    ///
    /// Returns an error if the future is still pending but nothing can ever
    /// wake it (no runnable tasks and no I/O in flight).
    pub fn block_on<F: Future + 'static>(&self, future: F) -> io::Result<F::Output> {
        let out: Rc<RefCell<Option<F::Output>>> = Rc::new(RefCell::new(None));
        let slot = Rc::clone(&out);
        spawn(async move {
            let v = future.await;
            *slot.borrow_mut() = Some(v);
        });
        loop {
            Executor::run_ready();
            if let Some(v) = out.borrow_mut().take() {
                return Ok(v);
            }
            Reactor::with(|r| r.wait())?;
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        Executor::uninstall();
        Reactor::uninstall();
    }
}
