//! Tokio, in two flavours: the multi-thread work-stealing scheduler and the
//! current-thread scheduler. Same task body for both.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use phhtp_core::{conn, process, App, Flow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::READ_CHUNK;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>, multi_thread: bool) -> io::Result<()> {
    let rt = if multi_thread {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.workers.max(1))
            .thread_name("phhtp-tokio")
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    };

    rt.block_on(async move {
        let listener = TcpListener::bind(cfg.bind).await?;
        if let Some(tx) = cfg.ready {
            let _ = tx.send(listener.local_addr()?);
        }
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("accept: {e}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            let app = Arc::clone(&app);
            tokio::spawn(async move {
                let _ = serve_connection(&app, stream).await;
            });
        }
    })
}

async fn serve_connection(app: &App, mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let max_in = conn::max_inbuf(app);

    loop {
        let start = inbuf.len();
        inbuf.resize(start + READ_CHUNK, 0);
        let n = match stream.read(&mut inbuf[start..]).await {
            Ok(n) => n,
            Err(e) => {
                inbuf.truncate(start);
                return Err(e);
            }
        };
        inbuf.truncate(start + n);
        if n == 0 {
            return Ok(());
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
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            if !outbuf.is_empty() {
                stream.write_all(&outbuf).await?;
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
