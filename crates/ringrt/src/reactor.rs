//! The I/O driver: owns the ring and maps completions back to wakers.
//!
//! Every in-flight operation has a slot; `user_data` is the slot index.
//!
//! * A **single-shot** slot holds the waker until the completion arrives, then
//!   the result until the future is polled again. If the future is dropped
//!   first, the slot becomes an orphan that keeps the buffer alive until the
//!   kernel is done with it.
//! * A **multishot** slot (accept, recv) stays armed across many completions.
//!   It belongs to a `TcpListener` / `TcpStream`, not to a future, and queues
//!   what the kernel delivered until the owner asks for it. Recv data lands in
//!   a provided buffer, is copied into the slot's staging area and the buffer
//!   is recycled immediately, so no user memory is ever lent to the kernel.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::task::Waker;

use crate::bufring::BufRing;
use crate::ring::Ring;
use crate::sys::{self, Cqe, Sqe};

/// `user_data` of cancel requests, whose completions carry no slot.
const CANCEL_TAG: u64 = u64::MAX;

/// Provided buffers per worker ring and their size (defaults; override with
/// `RINGRT_BUF_ENTRIES` / `RINGRT_BUF_SIZE`). The ring only needs as many
/// buffers as completions arrive between two reaps, and running out is
/// harmless (ENOBUFS → re-arm). Small is fast: the kernel rotates through
/// the ring, so a 1 MiB ring stays in L2 while an 8 MiB one costs a cold
/// page per request (measured: 2048 buffers 123k req/s, 256 buffers 163k
/// at 1024 connections on this repository's benchmark).
const BUF_ENTRIES: u32 = 256;
const BUF_SIZE: usize = 4096;

fn env_num<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
/// Staging buffer for single-shot recv fallback (kernels without buffer rings).
const FALLBACK_RECV: usize = 16 * 1024;

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
    Multi(Box<Multi>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The multishot stopped benignly (buffer exhaustion, single-shot mode); re-arm.
    Rearm,
    /// `recv` saw EOF.
    Eof,
    /// An errno.
    Err(i32),
}

pub enum MultiKind {
    Accept {
        fds: VecDeque<i32>,
    },
    Recv {
        staged: Vec<u8>,
        fallback: Option<Box<[u8]>>,
    },
}

pub struct Multi {
    fd: i32,
    kind: MultiKind,
    waker: Option<Waker>,
    /// An SQE for this slot is in flight.
    armed: bool,
    /// The in-flight SQE used the multishot form (so EINVAL means "unsupported").
    armed_multishot: bool,
    ended: Option<Ended>,
    /// The owner is gone; free the slot when the in-flight op completes.
    orphaned: bool,
}

pub struct Reactor {
    ring: Ring,
    bufs: Option<BufRing>,
    slots: Vec<Slot>,
    free: Vec<usize>,
    cqes: Vec<Cqe>,
    in_flight: usize,
    multishot_accept: bool,
    multishot_recv: bool,
}

thread_local! {
    static REACTOR: RefCell<Option<Reactor>> = const { RefCell::new(None) };
}

/// `RINGRT_TRACE=1` prints every multishot arm and completion to stderr.
fn trace() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("RINGRT_TRACE").is_some())
}

impl Reactor {
    pub fn install(entries: u32) -> io::Result<()> {
        let ring = Ring::new(entries)?;
        // Buffer rings need 5.19; multishot recv needs 6.0. Either missing
        // means single-shot recv into reactor-owned staging buffers.
        // `RINGRT_NO_MULTISHOT=1` forces that path (for measurement).
        let no_multishot = std::env::var_os("RINGRT_NO_MULTISHOT").is_some();
        let bufs = if no_multishot {
            None
        } else {
            BufRing::new(
                ring.fd(),
                0,
                env_num("RINGRT_BUF_ENTRIES", BUF_ENTRIES),
                env_num("RINGRT_BUF_SIZE", BUF_SIZE),
            )
            .ok()
        };
        let multishot_recv = bufs.is_some();
        REACTOR.with(|r| {
            *r.borrow_mut() = Some(Reactor {
                ring,
                bufs,
                slots: Vec::new(),
                free: Vec::new(),
                cqes: Vec::with_capacity(1024),
                in_flight: 0,
                multishot_accept: !no_multishot,
                multishot_recv,
            })
        });
        Ok(())
    }

    pub fn with<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
        REACTOR.with(|r| {
            f(r.borrow_mut()
                .as_mut()
                .expect("no ringrt runtime on this thread"))
        })
    }

    /// Like [`Reactor::with`], but `None` when no reactor is installed. Used by
    /// `Drop` impls: during runtime teardown the reactor has already cancelled
    /// and reaped every op before the owners are dropped.
    pub fn try_with<R>(f: impl FnOnce(&mut Reactor) -> R) -> Option<R> {
        REACTOR.with(|r| r.borrow_mut().as_mut().map(f))
    }

    pub fn is_installed() -> bool {
        REACTOR.with(|r| r.borrow().is_some())
    }

    pub fn uninstall() {
        let reactor = REACTOR.with(|r| r.borrow_mut().take());
        drop(reactor);
    }

    pub fn setup_flags(&self) -> u32 {
        self.ring.setup_flags
    }

    /// Whether provided buffers + multishot recv are in use on this kernel.
    pub fn has_buffer_ring(&self) -> bool {
        self.bufs.is_some()
    }

    fn alloc_slot(&mut self) -> usize {
        match self.free.pop() {
            Some(i) => i,
            None => {
                self.slots.push(Slot::Free);
                self.slots.len() - 1
            }
        }
    }

    fn free_slot(&mut self, slot: usize) {
        self.slots[slot] = Slot::Free;
        self.free.push(slot);
    }

    fn push(&mut self, sqe: Sqe) {
        if self.ring.push(sqe) {
            return;
        }
        let _ = self.ring.submit(0);
        while !self.ring.push(sqe) {
            let _ = self.ring.submit(1);
            self.reap();
        }
    }

    // ------------------------------------------------------------ single-shot

    /// Queue `sqe`, tagging it with a fresh slot that will wake `waker`.
    pub fn submit(&mut self, mut sqe: Sqe, waker: Waker) -> usize {
        let slot = self.alloc_slot();
        sqe.user_data = slot as u64;
        self.slots[slot] = Slot::Pending { waker };
        self.in_flight += 1;
        self.push(sqe);
        slot
    }

    pub fn poll_slot(&mut self, slot: usize, waker: &Waker) -> Option<i32> {
        match &mut self.slots[slot] {
            Slot::Done { res } => {
                let res = *res;
                self.free_slot(slot);
                Some(res)
            }
            Slot::Pending { waker: stored } => {
                if !stored.will_wake(waker) {
                    *stored = waker.clone();
                }
                None
            }
            _ => unreachable!("polling a slot that is not ours"),
        }
    }

    pub fn orphan(&mut self, slot: usize, keep: Option<Box<dyn Any>>) {
        match &mut self.slots[slot] {
            Slot::Done { .. } => self.free_slot(slot),
            Slot::Pending { .. } => {
                self.slots[slot] = Slot::Orphan { keep };
                self.cancel(slot);
            }
            Slot::Multi(m) => {
                m.orphaned = true;
                m.waker = None;
                if m.armed {
                    self.cancel(slot);
                } else {
                    self.free_slot(slot);
                }
            }
            _ => unreachable!("orphaning a slot that is not ours"),
        }
    }

    fn cancel(&mut self, slot: usize) {
        self.push(Sqe {
            opcode: sys::OP_ASYNC_CANCEL,
            fd: -1,
            addr: slot as u64,
            user_data: CANCEL_TAG,
            ..Default::default()
        });
    }

    // ------------------------------------------------------------- multishot

    /// Create a multishot accept slot for `listener_fd` (not yet armed).
    pub fn new_accept_slot(&mut self, listener_fd: i32) -> usize {
        self.new_multi(
            listener_fd,
            MultiKind::Accept {
                fds: VecDeque::new(),
            },
        )
    }

    /// Create a multishot recv slot for `fd` (not yet armed).
    pub fn new_recv_slot(&mut self, fd: i32) -> usize {
        self.new_multi(
            fd,
            MultiKind::Recv {
                staged: Vec::new(),
                fallback: None,
            },
        )
    }

    fn new_multi(&mut self, fd: i32, kind: MultiKind) -> usize {
        let slot = self.alloc_slot();
        self.slots[slot] = Slot::Multi(Box::new(Multi {
            fd,
            kind,
            waker: None,
            armed: false,
            armed_multishot: false,
            ended: None,
            orphaned: false,
        }));
        slot
    }

    fn arm(&mut self, slot: usize) {
        let (multishot_accept, multishot_recv) = (self.multishot_accept, self.multishot_recv);
        let bgid = self.bufs.as_ref().map(|b| b.bgid());
        let Slot::Multi(m) = &mut self.slots[slot] else {
            unreachable!()
        };
        let sqe = match &mut m.kind {
            MultiKind::Accept { .. } => Sqe {
                opcode: sys::OP_ACCEPT,
                fd: m.fd,
                ioprio: if multishot_accept {
                    sys::ACCEPT_MULTISHOT
                } else {
                    0
                },
                op_flags: libc::SOCK_CLOEXEC as u32,
                user_data: slot as u64,
                ..Default::default()
            },
            MultiKind::Recv { fallback, .. } => {
                if multishot_recv {
                    Sqe {
                        opcode: sys::OP_RECV,
                        fd: m.fd,
                        ioprio: sys::RECV_MULTISHOT,
                        flags: sys::IOSQE_BUFFER_SELECT,
                        buf_index: bgid.expect("buffer ring present when multishot_recv"),
                        user_data: slot as u64,
                        ..Default::default()
                    }
                } else {
                    let buf =
                        fallback.get_or_insert_with(|| vec![0u8; FALLBACK_RECV].into_boxed_slice());
                    Sqe {
                        opcode: sys::OP_RECV,
                        fd: m.fd,
                        addr: buf.as_mut_ptr() as u64,
                        len: buf.len() as u32,
                        user_data: slot as u64,
                        ..Default::default()
                    }
                }
            }
        };
        if trace() {
            eprintln!(
                "ringrt: arm slot={slot} fd={} opcode={} ioprio={} flags={}",
                m.fd, sqe.opcode, sqe.ioprio, sqe.flags
            );
        }
        m.armed = true;
        m.ended = None;
        self.in_flight += 1;
        self.push(sqe);
    }

    /// Poll a multishot accept slot.
    pub fn poll_accept(&mut self, slot: usize, waker: &Waker) -> Option<io::Result<i32>> {
        loop {
            let Slot::Multi(m) = &mut self.slots[slot] else {
                unreachable!()
            };
            let MultiKind::Accept { fds } = &mut m.kind else {
                unreachable!()
            };
            if let Some(fd) = fds.pop_front() {
                return Some(Ok(fd));
            }
            match m.ended.take() {
                Some(Ended::Err(e)) => return Some(Err(io::Error::from_raw_os_error(e))),
                Some(Ended::Eof) => return Some(Err(io::ErrorKind::BrokenPipe.into())),
                Some(Ended::Rearm) | None if !m.armed => {
                    self.arm(slot);
                    continue;
                }
                _ => {
                    m.waker = Some(waker.clone());
                    return None;
                }
            }
        }
    }

    /// Poll a multishot recv slot, appending staged bytes to `out`.
    pub fn poll_recv(
        &mut self,
        slot: usize,
        waker: &Waker,
        out: &mut Vec<u8>,
    ) -> Option<io::Result<usize>> {
        loop {
            let Slot::Multi(m) = &mut self.slots[slot] else {
                unreachable!()
            };
            let MultiKind::Recv { staged, .. } = &mut m.kind else {
                unreachable!()
            };
            if !staged.is_empty() {
                let n = staged.len();
                out.extend_from_slice(staged);
                staged.clear();
                return Some(Ok(n));
            }
            match m.ended {
                Some(Ended::Eof) => return Some(Ok(0)),
                Some(Ended::Err(e)) => {
                    m.ended = None;
                    return Some(Err(io::Error::from_raw_os_error(e)));
                }
                Some(Ended::Rearm) | None if !m.armed => {
                    self.arm(slot);
                    continue;
                }
                _ => {
                    m.waker = Some(waker.clone());
                    return None;
                }
            }
        }
    }

    // ----------------------------------------------------------------- reap

    /// Submit everything queued and block until at least one completion
    /// arrives, then dispatch all of them. Returns the number dispatched.
    pub fn wait(&mut self) -> io::Result<usize> {
        if self.in_flight == 0 {
            return Err(io::Error::other("reactor idle with no tasks and no I/O"));
        }
        self.ring.submit(1)?;
        Ok(self.reap())
    }

    fn reap(&mut self) -> usize {
        self.cqes.clear();
        self.ring.drain_completions(&mut self.cqes);
        let n = self.cqes.len();
        let mut recycled = false;
        for i in 0..n {
            let cqe = self.cqes[i];
            if cqe.user_data == CANCEL_TAG {
                continue;
            }
            let slot = cqe.user_data as usize;
            match &mut self.slots[slot] {
                Slot::Multi(_) => recycled |= self.on_multi(slot, cqe),
                _ => {
                    self.in_flight -= 1;
                    match std::mem::replace(&mut self.slots[slot], Slot::Done { res: cqe.res }) {
                        Slot::Pending { waker } => waker.wake(),
                        Slot::Orphan { keep } => {
                            drop(keep);
                            self.free_slot(slot);
                        }
                        _ => unreachable!("completion for an idle slot"),
                    }
                }
            }
        }
        if recycled {
            if let Some(b) = &self.bufs {
                b.publish();
            }
        }
        n
    }

    /// Returns `true` if a provided buffer was recycled.
    fn on_multi(&mut self, slot: usize, cqe: Cqe) -> bool {
        let more = cqe.flags & sys::CQE_F_MORE != 0;
        if trace() {
            let Slot::Multi(m) = &self.slots[slot] else {
                unreachable!()
            };
            eprintln!(
                "ringrt: cqe slot={slot} fd={} kind={} res={} flags=0x{:x} more={more} armed={} ended={:?}",
                m.fd,
                match m.kind { MultiKind::Accept { .. } => "accept", MultiKind::Recv { .. } => "recv" },
                cqe.res, cqe.flags, m.armed, m.ended
            );
        }
        let mut recycled = false;
        let mut lost_multishot_accept = false;
        let mut lost_multishot_recv = false;
        let bufs = self.bufs.as_mut();
        let Slot::Multi(m) = &mut self.slots[slot] else {
            unreachable!()
        };

        match &mut m.kind {
            MultiKind::Accept { fds } => {
                if cqe.res >= 0 {
                    fds.push_back(cqe.res);
                }
            }
            MultiKind::Recv { staged, fallback } => {
                if cqe.res > 0 {
                    let n = cqe.res as usize;
                    if cqe.flags & sys::CQE_F_BUFFER != 0 {
                        let bid = (cqe.flags >> sys::CQE_BUFFER_SHIFT) as u16;
                        let b = bufs.expect("buffer completion without a buffer ring");
                        // SAFETY: the kernel wrote `n` bytes into buffer `bid`.
                        let data = unsafe { std::slice::from_raw_parts(b.buf(bid), n) };
                        staged.extend_from_slice(data);
                        b.recycle(bid);
                        recycled = true;
                    } else if let Some(fb) = fallback {
                        staged.extend_from_slice(&fb[..n]);
                    }
                }
            }
        }

        if !more {
            m.armed = false;
            self.in_flight -= 1;
            m.ended = Some(match cqe.res {
                r if r > 0 => Ended::Rearm,
                0 => match m.kind {
                    MultiKind::Recv { .. } => Ended::Eof,
                    MultiKind::Accept { .. } => Ended::Rearm,
                },
                r if -r == libc::ENOBUFS => Ended::Rearm,
                r if -r == libc::EINVAL && m.armed_multishot => {
                    // The kernel rejected the multishot form (pre-5.19 accept,
                    // pre-6.0 recv): fall back to single-shot for good. Every
                    // op armed as multishot before the flag flipped gets the
                    // same EINVAL and the same re-arm.
                    match m.kind {
                        MultiKind::Accept { .. } => lost_multishot_accept = true,
                        MultiKind::Recv { .. } => lost_multishot_recv = true,
                    }
                    Ended::Rearm
                }
                r => Ended::Err(-r),
            });
            if m.orphaned {
                self.free_slot(slot);
                return recycled;
            }
        }
        if let Some(w) = m.waker.take() {
            w.wake();
        }
        if lost_multishot_accept && self.multishot_accept {
            eprintln!("ringrt: kernel rejected multishot accept; using single-shot");
            self.multishot_accept = false;
        }
        if lost_multishot_recv && self.multishot_recv {
            eprintln!("ringrt: kernel rejected multishot recv; using single-shot");
            self.multishot_recv = false;
        }
        recycled
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // 1. Detach every waker (each may be the last reference to a parked
        //    task) so nothing runs task code while we drain.
        let mut wakers = Vec::new();
        for slot in 0..self.slots.len() {
            match &mut self.slots[slot] {
                Slot::Pending { .. } => {
                    let Slot::Pending { waker } =
                        std::mem::replace(&mut self.slots[slot], Slot::Orphan { keep: None })
                    else {
                        unreachable!()
                    };
                    wakers.push(waker);
                }
                Slot::Multi(m) => {
                    m.orphaned = true;
                    if let Some(w) = m.waker.take() {
                        wakers.push(w);
                    }
                    if !m.armed {
                        self.free_slot(slot);
                    }
                }
                _ => {}
            }
        }
        // 2. Cancel everything in flight and wait for the kernel to confirm,
        //    so no buffer is released while a write into it may be pending.
        for slot in 0..self.slots.len() {
            if matches!(self.slots[slot], Slot::Orphan { .. } | Slot::Multi(_)) {
                self.cancel(slot);
            }
        }
        while self.in_flight > 0 {
            if self.ring.submit(1).is_err() {
                break;
            }
            self.reap();
        }
        // 3. Now the tasks (and their futures / streams) can go; their `Drop`
        //    finds no reactor via `try_with` and does nothing, which is correct
        //    because every op has completed.
        drop(wakers);
        // `bufs` (the registered buffer ring) drops after this, then `ring`.
    }
}
