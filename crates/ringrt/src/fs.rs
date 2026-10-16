//! Files through the ring: `openat`, `statx`, `read` as io_uring ops, so a
//! file-backed response never blocks the worker and needs no thread pool.
//! Reads land in a `Vec` the caller hands over (owned-buffer, like `send`).

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

use crate::op::Single;
use crate::sys::{self, Sqe};

fn errno(res: i32) -> io::Error {
    io::Error::from_raw_os_error(-res)
}

/// An open file. Closed (synchronously) when dropped.
pub struct File {
    fd: OwnedFd,
}

/// `statx` result, the fields a server needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub size: u64,
    pub is_file: bool,
    pub is_dir: bool,
    /// Modification time, seconds since the epoch.
    pub mtime_s: i64,
}

impl File {
    /// Open `path` read-only (`O_RDONLY | O_CLOEXEC`, `O_NOFOLLOW` is not set).
    pub async fn open(path: &Path) -> io::Result<File> {
        let c = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        let (res, _keep) = Single::new(c, |c| Sqe {
            opcode: sys::OP_OPENAT,
            fd: libc::AT_FDCWD,
            addr: c.as_ptr() as u64,
            op_flags: (libc::O_RDONLY | libc::O_CLOEXEC) as u32,
            len: 0, // mode (unused without O_CREAT)
            ..Default::default()
        })
        .await;
        if res < 0 {
            return Err(errno(res));
        }
        // SAFETY: fresh fd from openat that nothing else owns.
        Ok(File {
            fd: unsafe { OwnedFd::from_raw_fd(res) },
        })
    }

    /// `statx` on the open file.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        // SAFETY: statx is plain data; zeroed is a valid value.
        let stx: libc::statx = unsafe { std::mem::zeroed() };
        let empty = CString::new("").unwrap();
        let fd = self.fd.as_raw_fd();
        let (res, keep) = Single::new((stx, empty), |k| Sqe {
            opcode: sys::OP_STATX,
            fd,
            addr: k.1.as_ptr() as u64,
            len: libc::STATX_SIZE | libc::STATX_TYPE | libc::STATX_MTIME,
            off: &mut k.0 as *mut libc::statx as u64,
            op_flags: libc::AT_EMPTY_PATH as u32,
            ..Default::default()
        })
        .await;
        if res < 0 {
            return Err(errno(res));
        }
        let stx = keep.0;
        let mode = stx.stx_mode as u32;
        Ok(Metadata {
            size: stx.stx_size,
            is_file: mode & libc::S_IFMT == libc::S_IFREG,
            is_dir: mode & libc::S_IFMT == libc::S_IFDIR,
            mtime_s: stx.stx_mtime.tv_sec,
        })
    }

    /// Read into `buf[buf.len()..capacity]` at `offset`, appending what the
    /// kernel returned. Hands the buffer back with the byte count (0 = EOF).
    pub async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> (io::Result<usize>, Vec<u8>) {
        let start = buf.len();
        let room = buf.capacity() - start;
        if room == 0 {
            buf.reserve(64 * 1024);
        }
        let room = buf.capacity() - start;
        let fd = self.fd.as_raw_fd();
        let (res, mut keep) = Single::new(buf, |b| Sqe {
            opcode: sys::OP_READ,
            fd,
            addr: unsafe { b.as_mut_ptr().add(start) } as u64,
            len: room as u32,
            off: offset,
            ..Default::default()
        })
        .await;
        if res < 0 {
            return (Err(errno(res)), *keep);
        }
        // SAFETY: the kernel wrote `res` bytes into the spare capacity.
        unsafe { keep.set_len(start + res as usize) };
        (Ok(res as usize), *keep)
    }

    /// Read the whole file (of known `size`) into a new `Vec`.
    pub async fn read_to_end(&self, size: usize) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(size.max(1));
        let mut off = 0u64;
        loop {
            let (r, b) = self.read_at(buf, off).await;
            buf = b;
            match r {
                Ok(0) => return Ok(buf),
                Ok(n) => {
                    off += n as u64;
                    if buf.len() >= size {
                        return Ok(buf);
                    }
                    if buf.capacity() == buf.len() {
                        buf.reserve(64 * 1024);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Open and read a whole file through the ring.
pub async fn read(path: &Path) -> io::Result<Vec<u8>> {
    let f = File::open(path).await?;
    let m = f.metadata().await?;
    if !m.is_file {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    f.read_to_end(m.size as usize).await
}
