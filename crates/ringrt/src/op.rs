//! Operation futures. `Accept` and `Recv` drive a long-lived multishot slot
//! owned by the listener / stream; `Send` and `Sleep` are single-shot and own
//! what they lend to the kernel.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::reactor::Reactor;
use crate::sys::{self, KernelTimespec, Sqe};

fn errno(res: i32) -> io::Error {
    io::Error::from_raw_os_error(-res)
}

// ---------------------------------------------------------------- accept

pub struct Accept<'a> {
    pub(crate) fd: i32,
    pub(crate) slot: &'a Cell<Option<usize>>,
}

impl Future for Accept<'_> {
    type Output = io::Result<i32>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = match self.slot.get() {
            Some(s) => s,
            None => {
                let s = Reactor::with(|r| r.new_accept_slot(self.fd));
                self.slot.set(Some(s));
                s
            }
        };
        match Reactor::with(|r| r.poll_accept(slot, cx.waker())) {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------- recv

pub struct Recv<'a> {
    pub(crate) fd: i32,
    pub(crate) slot: &'a Cell<Option<usize>>,
    pub(crate) out: &'a mut Vec<u8>,
    pub(crate) detached: &'a std::rc::Rc<crate::reactor::DetachedState>,
}

impl Future for Recv<'_> {
    type Output = io::Result<usize>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(e) = this.detached.error.get() {
            return Poll::Ready(Err(io::Error::from_raw_os_error(e)));
        }
        let slot = match this.slot.get() {
            Some(s) => s,
            None => {
                let s = Reactor::with(|r| r.new_recv_slot(this.fd));
                this.slot.set(Some(s));
                s
            }
        };
        match Reactor::with(|r| r.poll_recv(slot, cx.waker(), this.out)) {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

/// Release a multishot slot when its owner (listener / stream) drops.
pub(crate) fn release_slot(slot: &Cell<Option<usize>>) {
    if let Some(s) = slot.take() {
        Reactor::try_with(|r| r.orphan(s, None));
    }
}

// ---------------------------------------------------------------- send

pub struct Send {
    fd: i32,
    buf: Option<Vec<u8>>,
    offset: usize,
    slot: Option<usize>,
}

impl Future for Send {
    type Output = (io::Result<usize>, Vec<u8>);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.slot {
            None => {
                let fd = self.fd;
                let offset = self.offset;
                let buf = self.buf.as_ref().expect("polled after completion");
                let sqe = Sqe {
                    opcode: sys::OP_SEND,
                    fd,
                    addr: buf[offset..].as_ptr() as u64,
                    len: (buf.len() - offset) as u32,
                    op_flags: libc::MSG_NOSIGNAL as u32,
                    ..Default::default()
                };
                self.slot = Some(Reactor::with(|r| r.submit(sqe, cx.waker().clone())));
                Poll::Pending
            }
            Some(slot) => match Reactor::with(|r| r.poll_slot(slot, cx.waker())) {
                None => Poll::Pending,
                Some(res) => {
                    self.slot = None;
                    let buf = self.buf.take().expect("buffer present");
                    Poll::Ready(if res < 0 {
                        (Err(errno(res)), buf)
                    } else {
                        (Ok(res as usize), buf)
                    })
                }
            },
        }
    }
}

impl Drop for Send {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let keep = self
                .buf
                .take()
                .map(|b| Box::new(b) as Box<dyn std::any::Any>);
            Reactor::try_with(|r| r.orphan(slot, keep));
        }
    }
}

/// Send `buf[offset..]` once; the result may be short.
pub fn send(fd: i32, buf: Vec<u8>, offset: usize) -> Send {
    Send {
        fd,
        buf: Some(buf),
        offset,
        slot: None,
    }
}

/// Send all of `buf`, re-issuing on short writes. Hands the buffer back.
pub async fn send_all(fd: i32, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
    let mut offset = 0;
    let mut buf = buf;
    while offset < buf.len() {
        let (r, b) = send(fd, buf, offset).await;
        buf = b;
        match r {
            Ok(0) => return (Err(io::ErrorKind::WriteZero.into()), buf),
            Ok(n) => offset += n,
            Err(e) => return (Err(e), buf),
        }
    }
    (Ok(()), buf)
}

// ---------------------------------------------------------------- detached send

/// Most detached sends a stream may have in flight before `send_detached`
/// waits for one to complete (backpressure for peers that stop reading).
pub const MAX_DETACHED: usize = 8;

pub struct SendDetached<'a> {
    pub(crate) fd: i32,
    pub(crate) buf: Option<Vec<u8>>,
    pub(crate) owner: &'a std::rc::Rc<crate::reactor::DetachedState>,
}

impl Future for SendDetached<'_> {
    type Output = io::Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(e) = self.owner.error.get() {
            return Poll::Ready(Err(io::Error::from_raw_os_error(e)));
        }
        if self.owner.in_flight.get() >= MAX_DETACHED {
            *self.owner.waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let buf = self.buf.take().expect("polled after completion");
        if buf.is_empty() {
            Reactor::with(|r| r.give_buffer(buf));
            return Poll::Ready(Ok(()));
        }
        let fd = self.fd;
        let owner = self.owner;
        Reactor::with(|r| r.submit_detached_send(fd, buf, owner));
        Poll::Ready(Ok(()))
    }
}

/// Resolves once every detached send on the stream has completed.
pub struct Flush<'a> {
    pub(crate) owner: &'a std::rc::Rc<crate::reactor::DetachedState>,
}

impl Future for Flush<'_> {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.owner.in_flight.get() == 0 {
            return Poll::Ready(match self.owner.error.get() {
                Some(e) => Err(io::Error::from_raw_os_error(e)),
                None => Ok(()),
            });
        }
        *self.owner.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

// ---------------------------------------------------------------- sleep

pub struct Sleep {
    /// Boxed so the address handed to the kernel never moves.
    ts: Option<Box<KernelTimespec>>,
    slot: Option<usize>,
}

pub fn sleep_ms(ms: u64) -> Sleep {
    Sleep {
        ts: Some(Box::new(KernelTimespec {
            tv_sec: (ms / 1000) as i64,
            tv_nsec: ((ms % 1000) * 1_000_000) as i64,
        })),
        slot: None,
    }
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.slot {
            None => {
                let addr = &**self.ts.as_ref().expect("polled after completion")
                    as *const KernelTimespec as u64;
                let sqe = Sqe {
                    opcode: sys::OP_TIMEOUT,
                    fd: -1,
                    addr,
                    len: 1,
                    ..Default::default()
                };
                self.slot = Some(Reactor::with(|r| r.submit(sqe, cx.waker().clone())));
                Poll::Pending
            }
            Some(slot) => match Reactor::with(|r| r.poll_slot(slot, cx.waker())) {
                None => Poll::Pending,
                Some(_etime) => {
                    self.slot = None;
                    self.ts = None;
                    Poll::Ready(())
                }
            },
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let keep = self.ts.take().map(|b| b as Box<dyn std::any::Any>);
            Reactor::try_with(|r| r.orphan(slot, keep));
        }
    }
}
