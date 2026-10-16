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
    let mut scratch = vec![0u8; READ_CHUNK];
    let max_in = conn::max_inbuf(app);

    loop {
        let n = read_append(stream, &mut inbuf, &mut scratch)?;
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

/// Read up to `scratch.len()` bytes and append them to `buf`. Returns bytes read.
///
/// Reading through a scratch buffer and copying only what arrived is cheaper
/// than growing `buf` with zeroes on every read: a request is ~100 bytes,
/// the zero-fill would be 16 KiB, and the untouched scratch pages stay
/// non-resident, which is what keeps RSS per idle connection low.
pub fn read_append(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    scratch: &mut [u8],
) -> io::Result<usize> {
    loop {
        match stream.read(scratch) {
            Ok(n) => {
                buf.extend_from_slice(&scratch[..n]);
                return Ok(n);
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}
