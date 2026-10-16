//! A minimal submission/completion ring built directly on the mmap'd io_uring
//! memory. Single-threaded by design: one `Ring` per worker thread.

use std::io;
use std::sync::atomic::{AtomicU32, Ordering};

use super::sys::{self, Cqe, Params, Sqe};

struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    fn map(fd: i32, len: usize, offset: i64) -> io::Result<Mmap> {
        // SAFETY: mapping a kernel-provided ring region with the documented offset.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Mmap {
            ptr: ptr as *mut u8,
            len,
        })
    }

    unsafe fn at<T>(&self, off: u32) -> *mut T {
        self.ptr.add(off as usize) as *mut T
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: unmapping what we mapped.
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

pub struct Ring {
    fd: i32,
    _sq_ring: Mmap,
    _cq_ring: Option<Mmap>,
    _sqes: Mmap,

    sq_head: *const AtomicU32,
    sq_tail: *const AtomicU32,
    sq_mask: u32,
    sq_entries: u32,
    sq_array: *mut u32,
    sqes: *mut Sqe,
    /// Tail as seen by us; entries in `[kernel head, local tail)` are pending.
    sq_tail_local: u32,

    cq_head: *const AtomicU32,
    cq_tail: *const AtomicU32,
    cq_mask: u32,
    cqes: *const Cqe,

    pub features: u32,
    pub setup_flags: u32,
}

impl Ring {
    /// Create a ring with `entries` SQEs, trying progressively less demanding
    /// setup flags so the same binary runs on older kernels.
    pub fn new(entries: u32) -> io::Result<Ring> {
        let attempts = [
            sys::SETUP_SINGLE_ISSUER | sys::SETUP_COOP_TASKRUN | sys::SETUP_DEFER_TASKRUN,
            sys::SETUP_SINGLE_ISSUER | sys::SETUP_COOP_TASKRUN,
            0,
        ];
        let mut last = None;
        for flags in attempts {
            match Self::with_flags(entries, flags) {
                Ok(r) => return Ok(r),
                Err(e) if e.raw_os_error() == Some(libc::EINVAL) && flags != 0 => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| io::Error::other("io_uring setup failed")))
    }

    fn with_flags(entries: u32, flags: u32) -> io::Result<Ring> {
        let mut p = Params {
            flags: flags | sys::SETUP_CQSIZE,
            cq_entries: entries * 4,
            ..Default::default()
        };
        let fd = sys::setup(entries, &mut p)?;
        let guard = FdGuard(fd);

        let sq_size = p.sq_off.array as usize + p.sq_entries as usize * 4;
        let cq_size = p.cq_off.cqes as usize + p.cq_entries as usize * std::mem::size_of::<Cqe>();
        let single = p.features & sys::FEAT_SINGLE_MMAP != 0;

        let sq_ring = Mmap::map(
            fd,
            if single {
                sq_size.max(cq_size)
            } else {
                sq_size
            },
            sys::OFF_SQ_RING,
        )?;
        let cq_ring = if single {
            None
        } else {
            Some(Mmap::map(fd, cq_size, sys::OFF_CQ_RING)?)
        };
        let sqes = Mmap::map(
            fd,
            p.sq_entries as usize * std::mem::size_of::<Sqe>(),
            sys::OFF_SQES,
        )?;
        let cq_base = cq_ring.as_ref().unwrap_or(&sq_ring);

        // SAFETY: offsets come from the kernel for the mappings above.
        let ring = unsafe {
            Ring {
                fd,
                sq_head: sq_ring.at(p.sq_off.head),
                sq_tail: sq_ring.at(p.sq_off.tail),
                sq_mask: *sq_ring.at::<u32>(p.sq_off.ring_mask),
                sq_entries: *sq_ring.at::<u32>(p.sq_off.ring_entries),
                sq_array: sq_ring.at(p.sq_off.array),
                sqes: sqes.at(0),
                sq_tail_local: (*sq_ring.at::<AtomicU32>(p.sq_off.tail)).load(Ordering::Relaxed),
                cq_head: cq_base.at(p.cq_off.head),
                cq_tail: cq_base.at(p.cq_off.tail),
                cq_mask: *cq_base.at::<u32>(p.cq_off.ring_mask),
                cqes: cq_base.at(p.cq_off.cqes),
                features: p.features,
                setup_flags: p.flags,
                _sq_ring: sq_ring,
                _cq_ring: cq_ring,
                _sqes: sqes,
            }
        };
        std::mem::forget(guard);
        Ok(ring)
    }

    /// Queue an SQE. Returns `false` if the submission queue is full
    /// (call [`Ring::submit`] and retry).
    pub fn push(&mut self, sqe: Sqe) -> bool {
        // SAFETY: the ring pointers are valid for the life of `self`.
        unsafe {
            let head = (*self.sq_head).load(Ordering::Acquire);
            if self.sq_tail_local.wrapping_sub(head) >= self.sq_entries {
                return false;
            }
            let idx = self.sq_tail_local & self.sq_mask;
            *self.sqes.add(idx as usize) = sqe;
            *self.sq_array.add(idx as usize) = idx;
            self.sq_tail_local = self.sq_tail_local.wrapping_add(1);
        }
        true
    }

    /// Hand pending SQEs to the kernel and block until at least `wait`
    /// completions are available (0 = don't block).
    pub fn submit(&mut self, wait: u32) -> io::Result<u32> {
        // SAFETY: see `push`.
        let to_submit = unsafe {
            (*self.sq_tail).store(self.sq_tail_local, Ordering::Release);
            self.sq_tail_local
                .wrapping_sub((*self.sq_head).load(Ordering::Acquire))
        };
        let flags = if wait > 0 { sys::ENTER_GETEVENTS } else { 0 };
        if to_submit == 0 && wait == 0 {
            return Ok(0);
        }
        sys::enter(self.fd, to_submit, wait, flags)
    }

    /// Copy every available completion into `out` and release them to the kernel.
    pub fn drain_completions(&mut self, out: &mut Vec<Cqe>) {
        // SAFETY: see `push`. The kernel publishes CQEs before bumping the tail.
        unsafe {
            let mut head = (*self.cq_head).load(Ordering::Relaxed);
            let tail = (*self.cq_tail).load(Ordering::Acquire);
            while head != tail {
                out.push(*self.cqes.add((head & self.cq_mask) as usize));
                head = head.wrapping_add(1);
            }
            (*self.cq_head).store(head, Ordering::Release);
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // SAFETY: closing the ring fd we own; mappings drop after.
        unsafe {
            libc::close(self.fd);
        }
    }
}

struct FdGuard(i32);
impl Drop for FdGuard {
    fn drop(&mut self) {
        // SAFETY: closing an fd we own on the error path.
        unsafe {
            libc::close(self.0);
        }
    }
}

/// `true` if this kernel/sandbox lets us create a ring at all.
pub fn available() -> bool {
    Ring::new(8).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nop_roundtrip() {
        if !available() {
            eprintln!("io_uring unavailable, skipping");
            return;
        }
        let mut ring = Ring::new(4).unwrap();
        for i in 0..3u64 {
            assert!(ring.push(Sqe {
                opcode: sys::OP_NOP,
                user_data: 100 + i,
                ..Default::default()
            }));
        }
        assert_eq!(ring.submit(3).unwrap(), 3);
        let mut cqes = Vec::new();
        ring.drain_completions(&mut cqes);
        let mut ud: Vec<u64> = cqes.iter().map(|c| c.user_data).collect();
        ud.sort();
        assert_eq!(ud, vec![100, 101, 102]);
        assert!(cqes.iter().all(|c| c.res == 0));
    }

    #[test]
    fn full_queue_is_reported() {
        if !available() {
            return;
        }
        let mut ring = Ring::new(2).unwrap();
        assert!(ring.push(Sqe::default()));
        assert!(ring.push(Sqe::default()));
        assert!(!ring.push(Sqe::default()));
        ring.submit(2).unwrap();
        assert!(ring.push(Sqe::default()));
    }
}
