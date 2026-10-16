//! The I/O driver: owns the ring and maps completions back to wakers.
//!
//! Every in-flight operation has a slot; `user_data` is the slot index. A slot
//! holds the waker of the future that submitted it until the completion
//! arrives, then holds the result until the future is polled again. If the
//! future is dropped first, the slot becomes an orphan that keeps the buffer
//! alive until the kernel is done with it.

use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::task::Waker;

use super::super::uring::ring::Ring;
use super::super::uring::sys::{self, Cqe, Sqe};

/// `user_data` of cancel requests, whose completions carry no slot.
const CANCEL_TAG: u64 = u64::MAX;

enum Slot {
    Free,
    Pending {
        waker: Waker,
    },
    Done {
        res: i32,
    },
    /// The future went away; drop `keep` once the completion lands.
    Orphan {
        keep: Option<Box<dyn Any>>,
    },
}

pub struct Reactor {
    ring: Ring,
    slots: Vec<Slot>,
    free: Vec<usize>,
    cqes: Vec<Cqe>,
    in_flight: usize,
}

thread_local! {
    static REACTOR: RefCell<Option<Reactor>> = const { RefCell::new(None) };
}

impl Reactor {
    pub fn install(entries: u32) -> io::Result<()> {
        let ring = Ring::new(entries)?;
        REACTOR.with(|r| {
            *r.borrow_mut() = Some(Reactor {
                ring,
                slots: Vec::new(),
                free: Vec::new(),
                cqes: Vec::with_capacity(1024),
                in_flight: 0,
            })
        });
        Ok(())
    }

    pub fn with<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
        REACTOR.with(|r| {
            f(r.borrow_mut()
                .as_mut()
                .expect("no reactor installed on this thread"))
        })
    }

    pub fn setup_flags(&self) -> u32 {
        self.ring.setup_flags
    }

    /// Queue `sqe`, tagging it with a fresh slot that will wake `waker`.
    pub fn submit(&mut self, mut sqe: Sqe, waker: Waker) -> usize {
        let slot = match self.free.pop() {
            Some(i) => i,
            None => {
                self.slots.push(Slot::Free);
                self.slots.len() - 1
            }
        };
        sqe.user_data = slot as u64;
        self.slots[slot] = Slot::Pending { waker };
        self.in_flight += 1;
        self.push(sqe);
        slot
    }

    fn push(&mut self, sqe: Sqe) {
        if self.ring.push(sqe) {
            return;
        }
        // Submission queue full: flush, and if the kernel is still backed up,
        // reap completions until there is room.
        let _ = self.ring.submit(0);
        while !self.ring.push(sqe) {
            let _ = self.ring.submit(1);
            self.reap();
        }
    }

    /// Called by a future on every poll after submission. Replaces the stored
    /// waker, or returns the completion result and frees the slot.
    pub fn poll_slot(&mut self, slot: usize, waker: &Waker) -> Option<i32> {
        match &mut self.slots[slot] {
            Slot::Done { res } => {
                let res = *res;
                self.slots[slot] = Slot::Free;
                self.free.push(slot);
                Some(res)
            }
            Slot::Pending { waker: stored } => {
                if !stored.will_wake(waker) {
                    *stored = waker.clone();
                }
                None
            }
            Slot::Free | Slot::Orphan { .. } => unreachable!("polling a slot that is not ours"),
        }
    }

    /// The future was dropped. Keep `keep` (its buffer) alive until the kernel
    /// has finished with it, and ask the kernel to cancel the op.
    pub fn orphan(&mut self, slot: usize, keep: Option<Box<dyn Any>>) {
        match self.slots[slot] {
            Slot::Done { .. } => {
                self.slots[slot] = Slot::Free;
                self.free.push(slot);
            }
            Slot::Pending { .. } => {
                self.slots[slot] = Slot::Orphan { keep };
                self.push(Sqe {
                    opcode: sys::OP_ASYNC_CANCEL,
                    fd: -1,
                    addr: slot as u64,
                    user_data: CANCEL_TAG,
                    ..Default::default()
                });
            }
            Slot::Free | Slot::Orphan { .. } => unreachable!("orphaning a slot that is not ours"),
        }
    }

    /// Submit everything queued and block until at least one completion
    /// arrives, then dispatch all of them. Returns the number dispatched.
    pub fn wait(&mut self) -> io::Result<usize> {
        if self.in_flight == 0 {
            // Nothing can ever complete; the caller has nothing runnable either.
            return Err(io::Error::other("reactor idle with no tasks and no I/O"));
        }
        self.ring.submit(1)?;
        Ok(self.reap())
    }

    fn reap(&mut self) -> usize {
        self.cqes.clear();
        self.ring.drain_completions(&mut self.cqes);
        let n = self.cqes.len();
        for i in 0..n {
            let cqe = self.cqes[i];
            if cqe.user_data == CANCEL_TAG {
                continue;
            }
            let slot = cqe.user_data as usize;
            self.in_flight -= 1;
            match std::mem::replace(&mut self.slots[slot], Slot::Done { res: cqe.res }) {
                Slot::Pending { waker } => waker.wake(),
                Slot::Orphan { keep } => {
                    drop(keep);
                    self.slots[slot] = Slot::Free;
                    self.free.push(slot);
                }
                Slot::Free | Slot::Done { .. } => unreachable!("completion for an idle slot"),
            }
        }
        n
    }
}
