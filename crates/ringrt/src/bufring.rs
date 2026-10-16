//! A provided-buffer ring (`IORING_REGISTER_PBUF_RING`, Linux 5.19+).
//!
//! The kernel picks a buffer from this ring for each `RECV` completion and
//! tells us which one in `cqe.flags`. We copy the bytes out and hand the
//! buffer straight back, so the pool never needs more buffers than there are
//! completions in one reap. With multishot `RECV` (6.0+) one submission then
//! serves a connection for its whole life.

use std::io;
use std::sync::atomic::{AtomicU16, Ordering};

use crate::sys::{self, Buf, BufReg};

pub struct BufRing {
    ring_fd: i32,
    bgid: u16,
    entries: u32,
    buf_size: usize,
    ring: *mut Buf,
    ring_len: usize,
    bufs: *mut u8,
    bufs_len: usize,
    tail: u16,
}

impl BufRing {
    /// Register `entries` buffers of `buf_size` bytes as buffer group `bgid`.
    /// `entries` must be a power of two.
    pub fn new(ring_fd: i32, bgid: u16, entries: u32, buf_size: usize) -> io::Result<BufRing> {
        assert!(entries.is_power_of_two() && entries <= 32768);
        let page = 4096;
        let ring_len = (entries as usize * std::mem::size_of::<Buf>()).div_ceil(page) * page;
        let bufs_len = entries as usize * buf_size;
        // SAFETY: anonymous private mappings; checked for MAP_FAILED below.
        let ring = unsafe { anon_map(ring_len)? } as *mut Buf;
        let bufs = match unsafe { anon_map(bufs_len) } {
            Ok(p) => p,
            Err(e) => {
                unsafe { libc::munmap(ring as *mut libc::c_void, ring_len) };
                return Err(e);
            }
        };

        let reg = BufReg {
            ring_addr: ring as u64,
            ring_entries: entries,
            bgid,
            ..Default::default()
        };
        if let Err(e) = sys::register(ring_fd, sys::REGISTER_PBUF_RING, &reg, 1) {
            // SAFETY: unmapping what we mapped above.
            unsafe {
                libc::munmap(ring as *mut libc::c_void, ring_len);
                libc::munmap(bufs as *mut libc::c_void, bufs_len);
            }
            return Err(e);
        }

        let mut br = BufRing {
            ring_fd,
            bgid,
            entries,
            buf_size,
            ring,
            ring_len,
            bufs,
            bufs_len,
            tail: 0,
        };
        for bid in 0..entries as u16 {
            br.push(bid);
        }
        br.publish();
        Ok(br)
    }

    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Address of the data for buffer `bid` (valid for `buf_size` bytes).
    pub fn buf(&self, bid: u16) -> *const u8 {
        debug_assert!((bid as u32) < self.entries);
        // SAFETY: bid < entries, so this stays inside the buffer mapping.
        unsafe { self.bufs.add(bid as usize * self.buf_size) }
    }

    /// Return a buffer to the kernel. Call [`BufRing::publish`] afterwards
    /// (once per batch) to make returned buffers visible.
    pub fn recycle(&mut self, bid: u16) {
        self.push(bid);
    }

    fn push(&mut self, bid: u16) {
        let idx = (self.tail as u32 & (self.entries - 1)) as usize;
        // SAFETY: idx < entries; the ring mapping is `entries` entries long.
        unsafe {
            *self.ring.add(idx) = Buf {
                addr: self.buf(bid) as u64,
                len: self.buf_size as u32,
                bid,
                resv: 0,
            };
        }
        self.tail = self.tail.wrapping_add(1);
    }

    /// Publish recycled buffers to the kernel (release store of the tail,
    /// which lives in the `resv` area of entry 0 at byte offset 14).
    pub fn publish(&self) {
        // SAFETY: entry 0 exists; offset 14 within it is the kernel's tail slot.
        unsafe {
            let tail = (self.ring as *mut u8).add(14) as *const AtomicU16;
            (*tail).store(self.tail, Ordering::Release);
        }
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        let reg = BufReg {
            bgid: self.bgid,
            ..Default::default()
        };
        let _ = sys::register(self.ring_fd, sys::UNREGISTER_PBUF_RING, &reg, 1);
        // SAFETY: unmapping what we mapped; the kernel no longer references it.
        unsafe {
            libc::munmap(self.ring as *mut libc::c_void, self.ring_len);
            libc::munmap(self.bufs as *mut libc::c_void, self.bufs_len);
        }
    }
}

unsafe fn anon_map(len: usize) -> io::Result<*mut u8> {
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(p as *mut u8)
    }
}
