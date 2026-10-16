//! Raw io_uring ABI: struct layouts, constants and the three syscalls.
//!
//! Transcribed from `<linux/io_uring.h>`. Nothing here is borrowed from
//! liburing or an io_uring crate; the layouts are asserted at compile time.

#![allow(dead_code)]

use std::io;

pub const SYS_SETUP: libc::c_long = libc::SYS_io_uring_setup;
pub const SYS_ENTER: libc::c_long = libc::SYS_io_uring_enter;

// Opcodes (subset).
pub const OP_NOP: u8 = 0;
pub const OP_TIMEOUT: u8 = 11;
pub const OP_ACCEPT: u8 = 13;
pub const OP_ASYNC_CANCEL: u8 = 14;
pub const OP_CLOSE: u8 = 19;
pub const OP_SEND: u8 = 26;
pub const OP_RECV: u8 = 27;

// io_uring_setup flags.
pub const SETUP_CQSIZE: u32 = 1 << 3;
pub const SETUP_COOP_TASKRUN: u32 = 1 << 8;
pub const SETUP_SINGLE_ISSUER: u32 = 1 << 12;
pub const SETUP_DEFER_TASKRUN: u32 = 1 << 13;

// io_uring_params.features.
pub const FEAT_SINGLE_MMAP: u32 = 1 << 0;
pub const FEAT_NODROP: u32 = 1 << 1;

// io_uring_enter flags.
pub const ENTER_GETEVENTS: u32 = 1 << 0;

// mmap offsets.
pub const OFF_SQ_RING: i64 = 0;
pub const OFF_CQ_RING: i64 = 0x0800_0000;
pub const OFF_SQES: i64 = 0x1000_0000;

// sqe.ioprio bit for multishot accept (5.19+).
pub const ACCEPT_MULTISHOT: u16 = 1 << 0;

// cqe.flags.
pub const CQE_F_MORE: u32 = 1 << 1;

/// `struct io_uring_sqe` (64 bytes, non-SQE128).
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct Sqe {
    pub opcode: u8,
    pub flags: u8,
    pub ioprio: u16,
    pub fd: i32,
    /// `off` / `addr2`
    pub off: u64,
    pub addr: u64,
    pub len: u32,
    /// `rw_flags` / `msg_flags` / `timeout_flags` / `accept_flags` / ...
    pub op_flags: u32,
    pub user_data: u64,
    pub buf_index: u16,
    pub personality: u16,
    /// `splice_fd_in` / `file_index` / `addr_len`+pad
    pub splice_fd_in: i32,
    pub addr3: u64,
    pub pad2: u64,
}

/// `struct io_uring_cqe` (16 bytes, non-CQE32).
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct SqringOffsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub flags: u32,
    pub dropped: u32,
    pub array: u32,
    pub resv1: u32,
    pub user_addr: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct CqringOffsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub overflow: u32,
    pub cqes: u32,
    pub flags: u32,
    pub resv1: u32,
    pub user_addr: u64,
}

/// `struct io_uring_params`.
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct Params {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub flags: u32,
    pub sq_thread_cpu: u32,
    pub sq_thread_idle: u32,
    pub features: u32,
    pub wq_fd: u32,
    pub resv: [u32; 3],
    pub sq_off: SqringOffsets,
    pub cq_off: CqringOffsets,
}

/// `struct __kernel_timespec`.
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct KernelTimespec {
    pub tv_sec: i64,
    pub tv_nsec: i64,
}

const _: () = assert!(std::mem::size_of::<Sqe>() == 64);
const _: () = assert!(std::mem::size_of::<Cqe>() == 16);
const _: () = assert!(std::mem::size_of::<SqringOffsets>() == 40);
const _: () = assert!(std::mem::size_of::<CqringOffsets>() == 40);
const _: () = assert!(std::mem::size_of::<Params>() == 120);
const _: () = assert!(std::mem::size_of::<KernelTimespec>() == 16);

pub fn setup(entries: u32, params: &mut Params) -> io::Result<i32> {
    // SAFETY: plain syscall with a valid, properly laid out params struct.
    let r = unsafe { libc::syscall(SYS_SETUP, entries, params as *mut Params) };
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r as i32)
    }
}

pub fn enter(fd: i32, to_submit: u32, min_complete: u32, flags: u32) -> io::Result<u32> {
    loop {
        // SAFETY: plain syscall; no sigset passed.
        let r = unsafe {
            libc::syscall(
                SYS_ENTER,
                fd,
                to_submit,
                min_complete,
                flags,
                std::ptr::null::<libc::sigset_t>(),
                0usize,
            )
        };
        if r >= 0 {
            return Ok(r as u32);
        }
        let e = io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::EINTR) => continue,
            // Completions are pending and the kernel wants us to reap them first.
            Some(libc::EBUSY) | Some(libc::EAGAIN) if to_submit > 0 => return Ok(0),
            _ => return Err(e),
        }
    }
}
