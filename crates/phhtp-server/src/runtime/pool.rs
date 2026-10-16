//! Fixed thread pool over blocking sockets.
//!
//! An acceptor thread hands connections to `workers` threads through a channel.
//! A worker owns a connection until it closes, so with keep-alive the pool size
//! is a hard cap on *concurrent connections*: the (N+1)-th connection waits in
//! the channel until a worker frees up. That queueing shows up directly in p90.

use std::io;
use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use phhtp_core::App;

use super::blocking::serve_connection;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    let listener = super::net::bind(cfg.bind, false)?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(listener.local_addr()?);
    }
    let workers = cfg.workers.max(1);
    let (tx, rx) = mpsc::channel::<TcpStream>();
    let rx = Arc::new(Mutex::new(rx));

    for i in 0..workers {
        let rx = Arc::clone(&rx);
        let app = Arc::clone(&app);
        std::thread::Builder::new()
            .name(format!("phhtp-pool-{i}"))
            .spawn(move || loop {
                let next = rx.lock().expect("pool receiver poisoned").recv();
                match next {
                    Ok(mut stream) => {
                        let _ = serve_connection(&app, &mut stream);
                    }
                    Err(_) => return, // acceptor gone
                }
            })?;
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if tx.send(s).is_err() {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => continue,
            Err(e) => {
                eprintln!("accept: {e}");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
    Ok(())
}
