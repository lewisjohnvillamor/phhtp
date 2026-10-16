//! Raw operation futures: each is a future that submits one SQE on first poll
//! and resolves when its completion is reaped by the reactor.
//!
//! Buffers are passed **by value** and handed back with the result. That is the
//! only ownership model that is sound with a completion API: if the future is
//! dropped mid-flight, the buffer moves into the reactor's orphan slot and lives
//! until the kernel says it is done with it.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::reactor::Reactor;
use crate::sys::{self, KernelTimespec, Sqe};

/// Minimum spare capacity a `recv` reserves in its buffer.
pub const READ_CHUNK: usize = 16 * 1024;

fn errno(res: i32) -> io::Error {
    io::Error::from_raw_os_error(-res)
}

// ---------------------------------------------------------------- accept

pub struct Accept {
    fd: i32,
    slot: Option<usize>,
}

pub fn accept(listener_fd: i32) -> Accept {
    Accept {
        fd: listener_fd,
        slot: None,
    }
}

impl Future for Accept {
    type Output = io::Result<i32>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.slot {
            None => {
                let sqe = Sqe {
                    opcode: sys::OP_ACCEPT,
                    fd: self.fd,
                    op_flags: libc::SOCK_CLOEXEC as u32,
                    ..Default::default()
                };
                self.slot = Some(Reactor::with(|r| r.submit(sqe, cx.waker().clone())));
                Poll::Pending
            }
            Some(slot) => match Reactor::with(|r| r.poll_slot(slot, cx.waker())) {
                None => Poll::Pending,
                Some(res) => {
                    self.slot = None;
                    Poll::Ready(if res < 0 { Err(errno(res)) } else { Ok(res) })
                }
            },
        }
    }
}

impl Drop for Accept {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            Reactor::try_with(|r| r.orphan(slot, None));
        }
    }
}

// ---------------------------------------------------------------- recv

pub struct Recv {
    fd: i32,
    buf: Option<Vec<u8>>,
    slot: Option<usize>,
}

/// Receive into the spare capacity of `buf` (at least [`READ_CHUNK`] bytes).
/// Resolves to `(Ok(n), buf)` with `n` bytes appended, `Ok(0)` on EOF.
pub fn recv(fd: i32, buf: Vec<u8>) -> Recv {
    Recv {
        fd,
        buf: Some(buf),
        slot: None,
    }
}

impl Future for Recv {
    type Output = (io::Result<usize>, Vec<u8>);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.slot {
            None => {
                let fd = self.fd;
                let buf = self.buf.as_mut().expect("polled after completion");
                buf.reserve(READ_CHUNK);
                let len = buf.len();
                let spare = buf.capacity() - len;
                // SAFETY: pointer into spare capacity of a Vec this future owns
                // and will not touch until the completion arrives.
                let addr = unsafe { buf.as_mut_ptr().add(len) } as u64;
                let sqe = Sqe {
                    opcode: sys::OP_RECV,
                    fd,
                    addr,
                    len: spare as u32,
                    ..Default::default()
                };
                self.slot = Some(Reactor::with(|r| r.submit(sqe, cx.waker().clone())));
                Poll::Pending
            }
            Some(slot) => match Reactor::with(|r| r.poll_slot(slot, cx.waker())) {
                None => Poll::Pending,
                Some(res) => {
                    self.slot = None;
                    let mut buf = self.buf.take().expect("buffer present");
                    if res < 0 {
                        return Poll::Ready((Err(errno(res)), buf));
                    }
                    // SAFETY: the kernel wrote exactly `res` bytes after `len`.
                    unsafe { buf.set_len(buf.len() + res as usize) };
                    Poll::Ready((Ok(res as usize), buf))
                }
            },
        }
    }
}

impl Drop for Recv {
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
