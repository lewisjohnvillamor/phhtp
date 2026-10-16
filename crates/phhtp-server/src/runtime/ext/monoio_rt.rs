//! monoio: ByteDance's thread-per-core runtime on io_uring (owned buffers).

use std::io;
use std::sync::Arc;
use std::time::Duration;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use phhtp_core::{conn, process, App, Flow};

use super::per_core;
use crate::runtime::READ_CHUNK;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    per_core(cfg, app, "monoio", true, worker)
}

fn worker(listener: std::net::TcpListener, app: Arc<App>) -> io::Result<()> {
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .with_entries(4096)
        .build()
        .map_err(io::Error::other)?;
    rt.block_on(async move {
        let listener = TcpListener::from_std(listener)?;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    monoio::spawn(serve_connection(Arc::clone(&app), stream));
                }
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {}
                Err(e) => {
                    eprintln!("accept: {e}");
                    monoio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
}

async fn serve_connection(app: Arc<App>, mut stream: TcpStream) {
    let max_in = conn::max_inbuf(&app);
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    // Owned read buffer: monoio fills it from the start and sets its length.
    let mut scratch: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    loop {
        scratch.clear();
        let (r, b) = stream.read(scratch).await;
        scratch = b;
        match r {
            Ok(0) | Err(_) => return,
            Ok(_) => inbuf.extend_from_slice(&scratch),
        }
        if inbuf.len() > max_in {
            return;
        }
        loop {
            let step = process(&app, &mut inbuf, &mut outbuf);
            if let Some(ms) = step.delay_ms {
                monoio::time::sleep(Duration::from_millis(ms)).await;
            }
            if !outbuf.is_empty() {
                let (r, b) = stream.write_all(outbuf).await;
                outbuf = b;
                outbuf.clear();
                if r.is_err() {
                    return;
                }
            }
            if step.flow == Flow::Close {
                return;
            }
            if step.delay_ms.is_none() || inbuf.is_empty() {
                break;
            }
        }
    }
}
