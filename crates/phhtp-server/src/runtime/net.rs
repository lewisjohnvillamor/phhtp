//! Socket helpers shared by the runtimes that run one listener per worker.

use std::io;
use std::net::{SocketAddr, TcpListener};

use socket2::{Domain, Protocol, Socket, Type};

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
    socket.listen(4096)?;
    Ok(socket.into())
}
