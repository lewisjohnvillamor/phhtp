//! Thread-per-connection. The simplest possible model: `accept`, spawn, block.

use std::io;
use std::net::TcpListener;
use std::sync::Arc;

use phhtp_core::App;

use super::blocking::serve_connection;
use crate::Config;

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    let listener = TcpListener::bind(cfg.bind)?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(listener.local_addr()?);
    }
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => continue,
            Err(e) => {
                // EMFILE and friends: don't spin, but keep serving.
                eprintln!("accept: {e}");
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
        };
        let app = Arc::clone(&app);
        let spawned = std::thread::Builder::new()
            .name("phhtp-conn".into())
            .stack_size(256 * 1024)
            .spawn(move || {
                let _ = serve_connection(&app, &mut stream);
            });
        if let Err(e) = spawned {
            // Out of threads (EAGAIN: RLIMIT_NPROC, pid_max, max_map_count).
            // The closure owned `stream`, so the connection is already closed.
            // Shed load instead of taking the whole server down.
            eprintln!("spawn: {e}; dropping connection");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    Ok(())
}
