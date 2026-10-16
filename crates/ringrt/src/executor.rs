//! A single-threaded task executor built on `std::task` only.
//!
//! One executor per worker thread, stored in a thread-local. Tasks are
//! `Rc<Task>`; the waker is a hand-rolled `RawWaker` whose data pointer is the
//! `Rc`. That makes `wake()` a bump of a `Cell<bool>` and a `VecDeque::push_back`,
//! with no atomics and no locks.
//!
//! Invariant that makes the `Rc`-backed waker sound: wakers never leave the
//! thread that created them. The only code that stores a waker is the reactor
//! in `reactor.rs`, which is itself thread-local, and no task is `Send`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;
type Queue = Rc<RefCell<VecDeque<Rc<Task>>>>;

struct Task {
    future: RefCell<Option<BoxFuture>>,
    queued: Cell<bool>,
    queue: Queue,
}

impl Task {
    fn schedule(self: &Rc<Self>) {
        if !self.queued.replace(true) {
            self.queue.borrow_mut().push_back(Rc::clone(self));
        }
    }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);

// SAFETY (all four): `p` was produced by `Rc::into_raw` in `waker_for`, and
// every call balances the strong count it implies.
unsafe fn clone_raw(p: *const ()) -> RawWaker {
    unsafe { Rc::increment_strong_count(p as *const Task) };
    RawWaker::new(p, &VTABLE)
}
unsafe fn wake_raw(p: *const ()) {
    let task = unsafe { Rc::from_raw(p as *const Task) };
    task.schedule();
}
unsafe fn wake_by_ref_raw(p: *const ()) {
    let task = ManuallyDrop::new(unsafe { Rc::from_raw(p as *const Task) });
    task.schedule();
}
unsafe fn drop_raw(p: *const ()) {
    drop(unsafe { Rc::from_raw(p as *const Task) });
}

fn waker_for(task: &Rc<Task>) -> Waker {
    let ptr = Rc::into_raw(Rc::clone(task)) as *const ();
    // SAFETY: the vtable above upholds the RawWaker contract for an Rc pointer,
    // under the single-thread invariant documented at the top of this file.
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

pub struct Executor {
    queue: Queue,
}

thread_local! {
    static EXECUTOR: RefCell<Option<Executor>> = const { RefCell::new(None) };
}

impl Executor {
    /// Install a fresh executor on this thread.
    pub fn install() {
        EXECUTOR.with(|e| {
            *e.borrow_mut() = Some(Executor {
                queue: Rc::new(RefCell::new(VecDeque::new())),
            })
        });
    }

    fn with<R>(f: impl FnOnce(&Executor) -> R) -> R {
        EXECUTOR.with(|e| {
            f(e.borrow()
                .as_ref()
                .expect("no ringrt runtime on this thread"))
        })
    }

    pub fn is_installed() -> bool {
        EXECUTOR.with(|e| e.borrow().is_some())
    }

    /// Drop this thread's executor and every task on it. Dropping a task's
    /// future drops its in-flight op futures, which orphan their buffers into
    /// the reactor; uninstall the reactor *after* this.
    pub fn uninstall() {
        let ex = EXECUTOR.with(|e| e.borrow_mut().take());
        if let Some(ex) = ex {
            ex.queue.borrow_mut().clear();
        }
    }

    /// Run every task that is ready. Returns `false` if nothing was runnable.
    pub fn run_ready() -> bool {
        let queue = Self::with(|e| Rc::clone(&e.queue));
        let mut ran = false;
        loop {
            let next = queue.borrow_mut().pop_front();
            let Some(task) = next else { break };
            ran = true;
            task.queued.set(false);
            let waker = waker_for(&task);
            let mut cx = Context::from_waker(&waker);
            let mut slot = task.future.borrow_mut();
            if let Some(fut) = slot.as_mut() {
                if fut.as_mut().poll(&mut cx).is_ready() {
                    *slot = None;
                }
            }
        }
        ran
    }
}

/// Spawn a task on this thread's executor.
pub fn spawn(future: impl Future<Output = ()> + 'static) {
    let queue = Executor::with(|e| Rc::clone(&e.queue));
    let task = Rc::new(Task {
        future: RefCell::new(Some(Box::pin(future))),
        queued: Cell::new(false),
        queue,
    });
    task.schedule();
}

/// Resolves on the first poll with `Pending`, then `Ready` on the next: lets a
/// long-running task hand the CPU to its siblings.
pub struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Let other tasks run before continuing.
pub fn yield_now() -> YieldNow {
    YieldNow(false)
}
