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
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::task::Waker;

use crate::bufring::BufRing;
use crate::ring::Ring;
use crate::sys::{self, Cqe, Sqe};

/// `user_data` of cancel requests, whose completions carry no slot.
const CANCEL_TAG: u64 = u64::MAX;

/// Provided buffers per worker ring and their size (defaults; override with
/// `RINGRT_BUF_ENTRIES` / `RINGRT_BUF_SIZE`). The ring needs as many buffers
/// as completions can arrive between two reaps; running out is survivable
/// (ENOBUFS → re-arm) but not free. With 64 to 16 000 *active* connections
/// 256 / 1024 / 2048 entries measured the same to within noise. With 10 000
/// connections that each sent half a request and then stalled, 256 entries
/// gave p99 1.9–4.5 ms on the hot connections against 0.8–0.9 ms with 4096
/// (docs/EXPANDED.md §8): the connect burst exhausts a small ring and every
/// affected recv has to be re-armed. 4096 × 4 KiB is 16 MiB per worker.
const BUF_ENTRIES: u32 = 4096;
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
    /// A fire-and-forget send: the buffer returns to the pool on completion
    /// and the owning stream's in-flight count drops (waking it if it waits).
    Detached {
        buf: Vec<u8>,
        owner: Rc<DetachedState>,
    },
    /// A zero-copy detached send: the buffer is pinned by the kernel until the
    /// notification CQE arrives, which comes after the result CQE.
    DetachedZc {
        fd: i32,
        buf: Vec<u8>,
        owner: Rc<DetachedState>,
        /// The result CQE has been seen; waiting for the notification.
        sent: bool,
    },
}

/// Per-stream bookkeeping for detached sends.
pub struct DetachedState {
    pub in_flight: Cell<usize>,
    pub waker: RefCell<Option<Waker>>,
    /// First error seen on a detached send, reported by the next `recv`.
    pub error: Cell<Option<i32>>,
}

impl DetachedState {
    pub fn new() -> Rc<DetachedState> {
        Rc::new(DetachedState {
            in_flight: Cell::new(0),
            waker: RefCell::new(None),
            error: Cell::new(None),
        })
    }
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
    /// Buffers handed back by completed detached sends, for reuse.
    pool: Vec<Vec<u8>>,
    /// `SEND_ZC` is accepted by this kernel (cleared on the first EINVAL /
    /// EOPNOTSUPP, after which zero-copy requests become plain sends).
    send_zc: bool,
}

/// Most buffers kept in the pool; beyond this they are freed.
const POOL_MAX: usize = 1024;

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
                pool: Vec::new(),
                send_zc: std::env::var_os("RINGRT_NO_ZC").is_none(),
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

    /// Take a buffer from the pool (empty, capacity retained) or a new one.
    pub fn take_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        match self.pool.pop() {
            Some(mut b) => {
                b.clear();
                if b.capacity() < min_capacity {
                    b.reserve(min_capacity);
                }
                b
            }
            None => Vec::with_capacity(min_capacity),
        }
    }

    /// Return a buffer to the pool.
    pub fn give_buffer(&mut self, buf: Vec<u8>) {
        if self.pool.len() < POOL_MAX {
            self.pool.push(buf);
        }
    }

    /// Submit a send whose completion nobody awaits. The buffer lives in the
    /// slot until the kernel is done, then returns to the pool.
    pub fn submit_detached_send(&mut self, fd: i32, buf: Vec<u8>, owner: &Rc<DetachedState>) {
        let slot = self.alloc_slot();
        let sqe = Sqe {
            opcode: sys::OP_SEND,
            fd,
            addr: buf.as_ptr() as u64,
            len: buf.len() as u32,
            op_flags: libc::MSG_NOSIGNAL as u32,
            user_data: slot as u64,
            ..Default::default()
        };
        owner.in_flight.set(owner.in_flight.get() + 1);
        self.slots[slot] = Slot::Detached {
            buf,
            owner: Rc::clone(owner),
        };
        self.in_flight += 1;
        self.push(sqe);
    }

    /// Like [`Reactor::submit_detached_send`] but with `SEND_ZC`: the kernel
    /// reads from the buffer's pages directly instead of copying them into
    /// socket memory, and keeps the buffer pinned until it posts a
    /// notification. Worth it for bodies of tens of KiB and up; for small
    /// buffers the extra CQE costs more than the copy it saves. Falls back to
    /// a plain send when the kernel rejects the opcode.
    pub fn submit_detached_send_zc(&mut self, fd: i32, buf: Vec<u8>, owner: &Rc<DetachedState>) {
        if !self.send_zc {
            return self.submit_detached_send(fd, buf, owner);
        }
        let slot = self.alloc_slot();
        let sqe = Sqe {
            opcode: sys::OP_SEND_ZC,
            fd,
            addr: buf.as_ptr() as u64,
            len: buf.len() as u32,
            op_flags: libc::MSG_NOSIGNAL as u32,
            user_data: slot as u64,
            ..Default::default()
        };
        owner.in_flight.set(owner.in_flight.get() + 1);
        self.slots[slot] = Slot::DetachedZc {
            fd,
            buf,
            owner: Rc::clone(owner),
            sent: false,
        };
        self.in_flight += 1;
        self.push(sqe);
    }

    fn finish_detached(&mut self, slot: usize, buf: Vec<u8>, owner: Rc<DetachedState>, res: i32) {
        self.in_flight -= 1;
        self.free.push(slot);
        owner.in_flight.set(owner.in_flight.get() - 1);
        // A short send of a detached buffer cannot be resumed; treat it like
        // an error and let the next recv report it.
        if res < 0 {
            if owner.error.get().is_none() {
                owner.error.set(Some(-res));
            }
        } else if res != i32::MAX && (res as usize) < buf.len() && owner.error.get().is_none() {
            owner.error.set(Some(libc::EPIPE));
        }
        if let Some(w) = owner.waker.borrow_mut().take() {
            w.wake();
        }
        self.give_buffer(buf);
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
                if out.is_empty() {
                    // Hand the staged bytes over without copying: the caller's
                    // (empty) Vec becomes the new staging buffer.
                    std::mem::swap(out, staged);
                } else {
                    out.extend_from_slice(staged);
                    staged.clear();
                }
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
                Slot::Detached { .. } => {
                    let Slot::Detached { buf, owner } =
                        std::mem::replace(&mut self.slots[slot], Slot::Free)
                    else {
                        unreachable!()
                    };
                    self.finish_detached(slot, buf, owner, cqe.res);
                }
                Slot::DetachedZc { sent, .. } => {
                    let is_notif = cqe.flags & sys::CQE_F_NOTIF != 0;
                    if !*sent && !is_notif && cqe.flags & sys::CQE_F_MORE != 0 {
                        // Result CQE; the notification follows. Record a short
                        // or failed send now, free the buffer on the notification.
                        *sent = true;
                        let Slot::DetachedZc { buf, owner, .. } = &self.slots[slot] else {
                            unreachable!()
                        };
                        if cqe.res < 0 {
                            if owner.error.get().is_none() {
                                owner.error.set(Some(-cqe.res));
                            }
                        } else if (cqe.res as usize) < buf.len() && owner.error.get().is_none() {
                            owner.error.set(Some(libc::EPIPE));
                        }
                        continue;
                    }
                    let Slot::DetachedZc {
                        fd,
                        buf,
                        owner,
                        sent,
                    } = std::mem::replace(&mut self.slots[slot], Slot::Free)
                    else {
                        unreachable!()
                    };
                    if !sent
                        && cqe.res < 0
                        && (-cqe.res == libc::EINVAL || -cqe.res == libc::EOPNOTSUPP)
                    {
                        // Kernel without SEND_ZC: resend every rejected buffer
                        // as a plain send, and stop issuing SEND_ZC.
                        if self.send_zc {
                            eprintln!("ringrt: kernel rejected SEND_ZC; using plain sends");
                            self.send_zc = false;
                        }
                        self.in_flight -= 1;
                        self.free.push(slot);
                        owner.in_flight.set(owner.in_flight.get() - 1);
                        self.submit_detached_send(fd, buf, &owner);
                        continue;
                    }
                    // Notification (buffer released) or a terminal result
                    // without MORE (error, or zero-length).
                    let res = if sent { 0 } else { cqe.res };
                    self.finish_detached(slot, buf, owner, if sent { i32::MAX } else { res });
                }
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
            if matches!(
                self.slots[slot],
                Slot::Orphan { .. } | Slot::Multi(_) | Slot::Detached { .. } | Slot::DetachedZc { .. }
            ) {
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
