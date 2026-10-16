//! Timers: each `sleep` is one kernel `TIMEOUT` op, so it rides the same
//! `io_uring_enter` as everything else and needs no timer wheel.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::op;

/// Resolve after `d` (millisecond resolution).
pub fn sleep(d: Duration) -> op::Sleep {
    op::sleep_ms(d.as_millis().min(u64::MAX as u128) as u64)
}

/// The future did not finish within its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for std::io::Error {
    fn from(_: Elapsed) -> Self {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "deadline has elapsed")
    }
}

/// Run `future` with a deadline. Each call arms one kernel `TIMEOUT` op and
/// cancels it when the future finishes first, so this costs two SQEs on top
/// of the operation: fine around an upstream request, wasteful around every
/// `recv` of a hot loop. For idle-connection timeouts prefer one periodic
/// sweeper per worker that calls [`crate::net::TcpStream::shutdown`].
pub fn timeout<F: Future>(d: Duration, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep(d),
    }
}

/// Future returned by [`timeout`].
pub struct Timeout<F> {
    future: F,
    sleep: op::Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: structural pinning of `future`; it is never moved out of
        // `self` and `Timeout` has no `Unpin` impl that would contradict it.
        let this = unsafe { self.get_unchecked_mut() };
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(v) = fut.poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}
