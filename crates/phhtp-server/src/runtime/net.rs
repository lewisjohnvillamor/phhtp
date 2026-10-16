//! Socket helpers shared by the runtimes.

use std::io;
use std::net::{SocketAddr, TcpListener};

use socket2::{Domain, Protocol, Socket, Type};

/// Requested accept backlog. The kernel clamps it to `net.core.somaxconn`
/// (4096 on modern kernels), so this just means "as much as the box allows".
/// `std::net::TcpListener::bind` asks for 128, which overflows the moment a
/// few hundred clients connect at once; an overflowed backlog drops SYNs and
/// handshake ACKs, which shows up as multi-second first-request latency.
pub const BACKLOG: i32 = 65535;

/// Bind a plain (non-reuseport) listener with [`BACKLOG`].
pub fn bind(addr: SocketAddr, nonblocking: bool) -> io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(nonblocking)?;
    socket.bind(&addr.into())?;
    socket.listen(BACKLOG)?;
    Ok(socket.into())
}

/// Bind `addr` with `SO_REUSEPORT` so several workers can each own a listening
/// socket on the same port and let the kernel spread connections across them.
pub fn bind_reuseport(addr: SocketAddr, nonblocking: bool) -> io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(nonblocking)?;
    socket.bind(&addr.into())?;
    socket.listen(BACKLOG)?;
    Ok(socket.into())
}
