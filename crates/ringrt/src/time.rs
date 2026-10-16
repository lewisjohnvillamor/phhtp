//! Timers: each `sleep` is one kernel `TIMEOUT` op, so it rides the same
//! `io_uring_enter` as everything else and needs no timer wheel.

use std::time::Duration;

use crate::op;

/// Resolve after `d` (millisecond resolution).
pub fn sleep(d: Duration) -> op::Sleep {
    op::sleep_ms(d.as_millis().min(u64::MAX as u128) as u64)
}
