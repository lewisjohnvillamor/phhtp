//! Shared connection driver for the blocking runtimes (`threads`, `pool`).

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use phhtp_core::{conn, process, App, Flow};

use super::READ_CHUNK;

/// Serve one connection to completion on the current thread.
pub fn serve_connection(app: &App, stream: &mut TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let max_in = conn::max_inbuf(app);

    loop {
        let n = read_append(stream, &mut inbuf)?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        if inbuf.len() > max_in {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "input buffer overflow",
            ));
        }
        loop {
            let step = process(app, &mut inbuf, &mut outbuf);
            if let Some(ms) = step.delay_ms {
                // A blocking runtime has no other way to wait: the thread sleeps.
                std::thread::sleep(Duration::from_millis(ms));
            }
            if !outbuf.is_empty() {
                stream.write_all(&outbuf)?;
                outbuf.clear();
            }
            if step.flow == Flow::Close {
                return Ok(());
            }
            if step.delay_ms.is_none() || inbuf.is_empty() {
                break;
            }
        }
    }
}

/// Read up to [`READ_CHUNK`] bytes and append them to `buf`. Returns bytes read.
pub fn read_append(stream: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<usize> {
    let start = buf.len();
    buf.resize(start + READ_CHUNK, 0);
    let r = stream.read(&mut buf[start..]);
    let n = *r.as_ref().unwrap_or(&0);
    buf.truncate(start + n);
    r
}
