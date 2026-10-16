//! TCP echo server: `cargo run --release -p ringrt --example echo`, then
//! `nc 127.0.0.1 9000`.

use ringrt::net::TcpListener;
use ringrt::{spawn, Runtime};

fn main() -> std::io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:9000".parse().unwrap())?;
        eprintln!("echo on {}", listener.local_addr()?);
        loop {
            let stream = listener.accept().await?;
            spawn(async move {
                let mut buf = Vec::with_capacity(16 * 1024);
                loop {
                    let (r, b) = stream.recv(buf).await;
                    buf = b;
                    if matches!(r, Ok(0) | Err(_)) {
                        return;
                    }
                    let (r, b) = stream.send_all(buf).await;
                    buf = b;
                    buf.clear();
                    if r.is_err() {
                        return;
                    }
                }
            });
        }
    })?
}
