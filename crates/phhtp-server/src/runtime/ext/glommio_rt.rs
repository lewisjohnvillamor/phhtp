//! glommio: Datadog's thread-per-core runtime on io_uring. Its streams expose
//! the `futures` `AsyncRead`/`AsyncWrite` traits (borrowed buffers, copied
//! internally), and its listener binds with `SO_REUSEPORT` itself.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{TcpListener, TcpStream};
use glommio::{LocalExecutorBuilder, Placement};
use phhtp_core::{conn, process, App, Flow};

use crate::runtime::READ_CHUNK;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    let workers = cfg.workers.max(1);
    // Bind once through std to learn the port (and to fail early), then let
    // each glommio executor bind its own reuseport listener on that address.
    let probe = crate::runtime::net::bind_reuseport(cfg.bind, false)?;
    let addr = probe.local_addr()?;
    drop(probe);
    let (tx, rx) = std::sync::mpsc::channel::<io::Result<()>>();

    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let app = Arc::clone(&app);
            let tx = tx.clone();
            LocalExecutorBuilder::new(Placement::Unbound)
                .name(&format!("phhtp-glommio-{i}"))
                .spawn(move || async move {
                    match TcpListener::bind(addr) {
                        Ok(listener) => {
                            let _ = tx.send(Ok(()));
                            acceptor(listener, app).await;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(io::Error::other(e.to_string())));
                        }
                    }
                })
                .map_err(|e| io::Error::other(e.to_string()))
        })
        .collect::<io::Result<_>>()?;

    for _ in 0..workers {
        rx.recv()
            .map_err(|_| io::Error::other("glommio worker died"))??;
    }
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    for h in handles {
        h.join().map_err(|e| io::Error::other(e.to_string()))?;
    }
    Ok(())
}

async fn acceptor(listener: TcpListener, app: Arc<App>) {
    loop {
        match listener.accept().await {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                glommio::spawn_local(serve_connection(Arc::clone(&app), stream)).detach();
            }
            Err(e) => {
                eprintln!("accept: {e}");
                glommio::timer::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn serve_connection(app: Arc<App>, mut stream: TcpStream) {
    let max_in = conn::max_inbuf(&app);
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut scratch = vec![0u8; READ_CHUNK];

    loop {
        let n = match stream.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        inbuf.extend_from_slice(&scratch[..n]);
        if inbuf.len() > max_in {
            return;
        }
        loop {
            let step = process(&app, &mut inbuf, &mut outbuf);
            if let Some(ms) = step.delay_ms {
                glommio::timer::sleep(Duration::from_millis(ms)).await;
            }
            if !outbuf.is_empty() {
                if stream.write_all(&outbuf).await.is_err() {
                    return;
                }
                outbuf.clear();
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
