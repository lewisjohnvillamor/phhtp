//! TCP listener and stream with owned-buffer `recv` / `send_all`.

use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use crate::op;

/// Accept backlog requested by [`TcpListener::bind`] and
/// [`TcpListener::bind_reuseport`]; the kernel clamps it to `net.core.somaxconn`.
pub const BACKLOG: i32 = 65535;

/// A listening socket. The first `accept` arms one multishot `ACCEPT` that
/// then delivers every connection for the listener's lifetime.
pub struct TcpListener {
    fd: OwnedFd,
    slot: Cell<Option<usize>>,
}

impl TcpListener {
    /// Bind with `SO_REUSEADDR` and a [`BACKLOG`]-deep accept queue.
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        Self::bind_opts(addr, false)
    }

    /// Bind with `SO_REUSEPORT` so one listener per worker thread can share a
    /// port; the kernel spreads new connections across them. This is how a
    /// thread-per-core server scales without any shared state.
    pub fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
        Self::bind_opts(addr, true)
    }

    fn bind_opts(addr: SocketAddr, reuseport: bool) -> io::Result<TcpListener> {
        let (domain, sa, len) = sockaddr(addr);
        // SAFETY: plain socket syscalls with a correctly sized sockaddr.
        unsafe {
            let fd = libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let one: libc::c_int = 1;
            let opts: &[libc::c_int] = if reuseport {
                &[libc::SO_REUSEADDR, libc::SO_REUSEPORT]
            } else {
                &[libc::SO_REUSEADDR]
            };
            for &opt in opts {
                if libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    opt,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            if libc::bind(
                fd.as_raw_fd(),
                &sa as *const _ as *const libc::sockaddr,
                len,
            ) < 0
            {
                return Err(io::Error::last_os_error());
            }
            if libc::listen(fd.as_raw_fd(), BACKLOG) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(TcpListener {
                fd,
                slot: Cell::new(None),
            })
        }
    }

    /// Adopt a bound std listener (its fd moves into the runtime).
    pub fn from_std(l: std::net::TcpListener) -> TcpListener {
        TcpListener {
            fd: l.into(),
            slot: Cell::new(None),
        }
    }

    /// Stop accepting: `shutdown(2)` on the listening socket makes the armed
    /// accept complete with an error, so an accept loop that checks a stop
    /// flag can exit. The socket closes when the listener drops.
    pub fn shutdown(&self) -> io::Result<()> {
        // SAFETY: shutdown on a socket we own.
        if unsafe { libc::shutdown(self.fd.as_raw_fd(), libc::SHUT_RDWR) } < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// The address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: borrowing the fd as a std listener just to ask its address.
        let std = unsafe { std::net::TcpListener::from_raw_fd(self.fd.as_raw_fd()) };
        let r = std.local_addr();
        let _ = std.into_raw_fd();
        r
    }

    /// Accept one connection. Resolves to a stream with `TCP_NODELAY` unset.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        let fd = op::Accept {
            fd: self.fd.as_raw_fd(),
            slot: &self.slot,
        }
        .await?;
        // SAFETY: fresh fd from accept that nothing else owns.
        Ok(TcpStream {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            slot: Cell::new(None),
            detached: crate::reactor::DetachedState::new(),
        })
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        op::release_slot(&self.slot);
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// A connected socket. Closed when dropped. An in-flight `recv` or `send`
/// whose future was dropped keeps its buffer alive in the reactor until the
/// kernel is done, so dropping the stream mid-operation is safe.
pub struct TcpStream {
    fd: OwnedFd,
    slot: Cell<Option<usize>>,
    detached: std::rc::Rc<crate::reactor::DetachedState>,
}

impl TcpStream {
    /// Adopt a connected std stream (its fd moves into the runtime).
    pub fn from_std(s: std::net::TcpStream) -> TcpStream {
        TcpStream {
            fd: s.into(),
            slot: Cell::new(None),
            detached: crate::reactor::DetachedState::new(),
        }
    }

    /// Set `TCP_NODELAY`.
    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        let v: libc::c_int = on as libc::c_int;
        // SAFETY: setsockopt on a socket we own.
        let r = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &v as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if r < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Append whatever has arrived to `buf`. Resolves with the number of bytes
    /// appended; `Ok(0)` is EOF.
    pub fn recv<'a>(&'a self, buf: &'a mut Vec<u8>) -> op::Recv<'a> {
        op::Recv {
            fd: self.fd.as_raw_fd(),
            slot: &self.slot,
            out: buf,
            detached: &self.detached,
        }
    }

    /// Send one chunk; may be short. Prefer [`TcpStream::send_all`].
    pub fn send(&self, buf: Vec<u8>) -> op::Send {
        op::send(self.fd.as_raw_fd(), buf, 0)
    }

    /// Send all of `buf`, re-issuing on short writes, and hand it back.
    pub async fn send_all(&self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        op::send_all(self.fd.as_raw_fd(), buf).await
    }

    /// Send `buf` without waiting for the completion: the buffer goes to the
    /// runtime's pool when the kernel is done, and this call returns as soon as
    /// the send is submitted. Saves a task wake per response. Backpressure:
    /// with [`op::MAX_DETACHED`] sends already in flight it waits for one. A
    /// failed or short detached send is reported by the next `recv` (or the
    /// next `send_detached`) as an error. Get buffers from
    /// [`TcpStream::take_buffer`] to reuse pooled ones.
    pub fn send_detached(&self, buf: Vec<u8>) -> op::SendDetached<'_> {
        op::SendDetached {
            fd: self.fd.as_raw_fd(),
            buf: Some(buf),
            owner: &self.detached,
        }
    }

    /// Like [`TcpStream::send_detached`] but zero-copy (`SEND_ZC`, 6.0+): the
    /// kernel reads the buffer's pages directly and returns the buffer to the
    /// pool only when its notification arrives. Pays off from a few tens of
    /// KiB; for small buffers it costs an extra completion. Falls back to a
    /// plain send on kernels without it.
    pub fn send_detached_zc(&self, buf: Vec<u8>) -> op::SendDetachedZc<'_> {
        op::SendDetachedZc {
            fd: self.fd.as_raw_fd(),
            buf: Some(buf),
            owner: &self.detached,
        }
    }

    /// `shutdown(2)` on the socket. `Shutdown::Both` makes a pending `recv`
    /// on this stream resolve with `Ok(0)`, which is how another task (an
    /// idle-connection sweeper, a shutdown sequence) ends a connection it
    /// does not own.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        // SAFETY: shutdown on a socket we own.
        if unsafe { libc::shutdown(self.fd.as_raw_fd(), how) } < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Wait until every detached send on this stream has completed. Call it
    /// before dropping a stream whose last responses must reach the peer.
    pub fn flush(&self) -> op::Flush<'_> {
        op::Flush {
            owner: &self.detached,
        }
    }

    /// An empty buffer with at least `min_capacity`, from the pool that
    /// detached sends return to, or newly allocated.
    pub fn take_buffer(min_capacity: usize) -> Vec<u8> {
        crate::reactor::Reactor::with(|r| r.take_buffer(min_capacity))
    }

    /// The remote address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: borrowing the fd as a std stream just to ask its peer.
        let std = unsafe { std::net::TcpStream::from_raw_fd(self.fd.as_raw_fd()) };
        let r = std.peer_addr();
        let _ = std.into_raw_fd();
        r
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        op::release_slot(&self.slot);
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

fn sockaddr(addr: SocketAddr) -> (libc::c_int, libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: sockaddr_storage is plain data; zeroed is a valid value.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(a) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(a.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            // SAFETY: sockaddr_in fits in sockaddr_storage.
            unsafe { std::ptr::write(&mut storage as *mut _ as *mut libc::sockaddr_in, sa) };
            (
                libc::AF_INET,
                storage,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(a) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: a.ip().octets(),
                },
                sin6_scope_id: a.scope_id(),
            };
            // SAFETY: sockaddr_in6 fits in sockaddr_storage.
            unsafe { std::ptr::write(&mut storage as *mut _ as *mut libc::sockaddr_in6, sa) };
            (
                libc::AF_INET6,
                storage,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}
