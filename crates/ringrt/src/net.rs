//! TCP listener and stream with owned-buffer `recv` / `send_all`.

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use crate::op;

/// A listening socket. `accept` is an io_uring `ACCEPT` op.
pub struct TcpListener {
    fd: OwnedFd,
}

impl TcpListener {
    /// Bind with the standard library's defaults (`SO_REUSEADDR`).
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        Ok(Self::from_std(std::net::TcpListener::bind(addr)?))
    }

    /// Bind with `SO_REUSEPORT` so one listener per worker thread can share a
    /// port; the kernel spreads new connections across them. This is how a
    /// thread-per-core server scales without any shared state.
    pub fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
        let (domain, sa, len) = sockaddr(addr);
        // SAFETY: plain socket syscalls with a correctly sized sockaddr.
        unsafe {
            let fd = libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let one: libc::c_int = 1;
            for opt in [libc::SO_REUSEADDR, libc::SO_REUSEPORT] {
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
            if libc::listen(fd.as_raw_fd(), 4096) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(TcpListener { fd })
        }
    }

    /// Adopt a bound std listener (its fd moves into the runtime).
    pub fn from_std(l: std::net::TcpListener) -> TcpListener {
        TcpListener { fd: l.into() }
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
        let fd = op::accept(self.fd.as_raw_fd()).await?;
        // SAFETY: fresh fd from accept that nothing else owns.
        Ok(TcpStream {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
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
}

impl TcpStream {
    /// Adopt a connected std stream (its fd moves into the runtime).
    pub fn from_std(s: std::net::TcpStream) -> TcpStream {
        TcpStream { fd: s.into() }
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

    /// Receive into the spare capacity of `buf` (at least [`op::READ_CHUNK`]
    /// bytes are reserved) and hand the buffer back with the byte count.
    /// `Ok(0)` is EOF.
    pub fn recv(&self, buf: Vec<u8>) -> op::Recv {
        op::recv(self.fd.as_raw_fd(), buf)
    }

    /// Send one chunk; may be short. Prefer [`TcpStream::send_all`].
    pub fn send(&self, buf: Vec<u8>) -> op::Send {
        op::send(self.fd.as_raw_fd(), buf, 0)
    }

    /// Send all of `buf`, re-issuing on short writes, and hand it back.
    pub async fn send_all(&self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        op::send_all(self.fd.as_raw_fd(), buf).await
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
