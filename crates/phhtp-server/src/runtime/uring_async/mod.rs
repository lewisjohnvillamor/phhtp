//! `uring-async`: the server on top of the [`ringrt`] crate, our own
//! `async`/`await` runtime on raw io_uring (no tokio, no mio, no futures crate).
//!
//! `serve_connection` below has the same shape as the Tokio one in
//! `tokio_rt.rs`, so the `tokio` and `uring-async` rows compare "Tokio's
//! scheduler + epoll" against "ringrt's scheduler + io_uring" with identical
//! handler code. Workers are independent threads, each with its own `Runtime`
//! and `SO_REUSEPORT` listener.
//!
//! This is also the hardened runtime of the edge target (docs/NEXT.md):
//!
//! * **idle timeout**: one sweeper task per worker closes connections that
//!   have sent nothing for `idle_timeout` (zero per-request cost; no timer
//!   op per recv);
//! * **connection cap**: beyond `max_conns` per worker, new connections are
//!   accepted and closed at once (shed) instead of exhausting fds;
//! * **graceful shutdown**: SIGTERM / SIGINT stops accepting, lets in-flight
//!   connections finish for `shutdown_grace`, then closes them;
//! * **files through the ring**: `/static/<name>` is `openat` + `statx` +
//!   `read` as io_uring ops into one buffer that already holds the response
//!   head; open files are cached per worker (like nginx's `open_file_cache`),
//!   and `SEND_ZC` is available (`PHHTP_ZC_MIN`) but off by default.

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use phhtp_core::{conn, process_fs, App, FileReq, Flow};
use ringrt::net::{TcpListener, TcpStream};
use ringrt::{spawn, Runtime};

use super::READ_CHUNK;
use crate::Config;

/// Set by the signal handler (or [`request_shutdown`]); every worker polls it.
static STOP: AtomicBool = AtomicBool::new(false);

/// Bodies at least this large go out with `SEND_ZC` (`PHHTP_ZC_MIN`; default
/// 0 = never). Off by default because on loopback zero-copy *loses*: the
/// kernel still copies into the receiving socket, and page pinning plus the
/// notification CQE cost more than the memcpy they replace (EXPANDED.md §10:
/// 57 k vs 39 k req/s on 64 KiB files). It is a real-NIC question; measure it
/// on the droplet before turning it on.
fn zc_min() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PHHTP_ZC_MIN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

/// Open files kept per worker (`PHHTP_FILE_CACHE`, 0 = none): the fd and the
/// `statx` result, re-checked after `FILE_CACHE_TTL`. Saves `openat`, `statx`
/// and `close` per request, the same idea as nginx's `open_file_cache`.
fn file_cache_size() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PHHTP_FILE_CACHE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024)
    })
}
const FILE_CACHE_TTL: Duration = Duration::from_secs(10);

struct CachedFile {
    file: Rc<ringrt::fs::File>,
    size: usize,
    checked: Instant,
}

#[derive(Default)]
struct FileCache {
    map: RefCell<std::collections::HashMap<std::path::PathBuf, CachedFile>>,
}

impl FileCache {
    /// Open (or reuse) `path` and learn its size. A cached entry older than
    /// the TTL is re-`statx`ed; a vanished or changed file is reopened.
    async fn get(&self, path: &std::path::Path) -> io::Result<(Rc<ringrt::fs::File>, usize)> {
        let cap = file_cache_size();
        if cap > 0 {
            let hit = self
                .map
                .borrow()
                .get(path)
                .map(|c| (Rc::clone(&c.file), c.size, c.checked));
            if let Some((file, size, checked)) = hit {
                if checked.elapsed() < FILE_CACHE_TTL {
                    return Ok((file, size));
                }
                match file.metadata().await {
                    Ok(m) if m.is_file && m.size as usize == size => {
                        if let Some(c) = self.map.borrow_mut().get_mut(path) {
                            c.checked = Instant::now();
                        }
                        return Ok((file, size));
                    }
                    _ => {
                        self.map.borrow_mut().remove(path);
                    }
                }
            }
        }
        let file = ringrt::fs::File::open(path).await?;
        let m = file.metadata().await?;
        if !m.is_file {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "not a regular file",
            ));
        }
        let file = Rc::new(file);
        let size = m.size as usize;
        if cap > 0 {
            let mut map = self.map.borrow_mut();
            if map.len() >= cap {
                // Drop the stalest entry; a full scan is fine at this size.
                if let Some(k) = map
                    .iter()
                    .min_by_key(|(_, c)| c.checked)
                    .map(|(k, _)| k.clone())
                {
                    map.remove(&k);
                }
            }
            map.insert(
                path.to_path_buf(),
                CachedFile {
                    file: Rc::clone(&file),
                    size,
                    checked: Instant::now(),
                },
            );
        }
        Ok((file, size))
    }
}

/// Ask every worker to stop accepting and drain (what SIGTERM does).
pub fn request_shutdown() {
    STOP.store(true, Ordering::SeqCst);
}

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

#[derive(Clone)]
struct Opts {
    idle_timeout: Option<Duration>,
    max_conns: Option<usize>,
    shutdown_grace: Duration,
    /// This server's own stop flag (`Config::stop`), checked alongside `STOP`.
    stop: Option<Arc<AtomicBool>>,
}

impl Opts {
    fn stopping(&self) -> bool {
        STOP.load(Ordering::Relaxed)
            || self
                .stop
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
    }
}

pub fn run(cfg: Config, app: Arc<App>) -> io::Result<()> {
    if !Runtime::available() {
        return Err(io::Error::other(
            "io_uring unavailable; needs Linux >= 5.6 and kernel.io_uring_disabled=0",
        ));
    }
    // SAFETY: installing an async-signal-safe handler that only stores a flag.
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }
    let workers = cfg.workers.max(1);
    let first = TcpListener::bind_reuseport(cfg.bind)?;
    let addr = first.local_addr()?;
    if let Some(tx) = cfg.ready {
        let _ = tx.send(addr);
    }
    let mut listeners = vec![first];
    for _ in 1..workers {
        listeners.push(TcpListener::bind_reuseport(addr)?);
    }
    let opts = Opts {
        idle_timeout: cfg.idle_timeout,
        max_conns: cfg.max_conns,
        shutdown_grace: cfg.shutdown_grace,
        stop: cfg.stop,
    };

    // A bound listener is plain `Send` data; only the accept *future* is
    // thread-bound. Each worker takes one listener into its own runtime.
    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let app = Arc::clone(&app);
            let opts = opts.clone();
            std::thread::Builder::new()
                .name(format!("phhtp-uring-async-{i}"))
                .spawn(move || worker(listener, app, opts, i == 0))
        })
        .collect::<io::Result<_>>()?;

    for h in handles {
        h.join()
            .map_err(|_| io::Error::other("uring-async worker panicked"))??;
    }
    Ok(())
}

/// Per-connection bookkeeping the sweeper and the shutdown sequence use.
struct ConnMeta {
    fd: i32,
    last_activity: Cell<Instant>,
}

#[derive(Default)]
struct Shared {
    conns: RefCell<Vec<Rc<ConnMeta>>>,
    shed: Cell<u64>,
}

impl Shared {
    fn active(&self) -> usize {
        self.conns.borrow().len()
    }
    fn register(&self, fd: i32) -> Rc<ConnMeta> {
        let m = Rc::new(ConnMeta {
            fd,
            last_activity: Cell::new(Instant::now()),
        });
        self.conns.borrow_mut().push(Rc::clone(&m));
        m
    }
    fn unregister(&self, m: &Rc<ConnMeta>) {
        let mut v = self.conns.borrow_mut();
        if let Some(i) = v.iter().position(|x| Rc::ptr_eq(x, m)) {
            v.swap_remove(i);
        }
    }
    /// `shutdown(2)` every connection whose last activity is older than `idle`.
    fn close_idle(&self, idle: Duration) -> usize {
        let now = Instant::now();
        let mut n = 0;
        for m in self.conns.borrow().iter() {
            if now.duration_since(m.last_activity.get()) > idle {
                // SAFETY: shutdown on a socket the connection task still owns;
                // its pending recv resolves with EOF and the task exits.
                unsafe { libc::shutdown(m.fd, libc::SHUT_RDWR) };
                n += 1;
            }
        }
        n
    }
    fn close_all(&self) {
        for m in self.conns.borrow().iter() {
            // SAFETY: as above.
            unsafe { libc::shutdown(m.fd, libc::SHUT_RDWR) };
        }
    }
}

fn worker(listener: TcpListener, app: Arc<App>, opts: Opts, log: bool) -> io::Result<()> {
    let rt = Runtime::new()?;
    if log {
        eprintln!(
            "uring-async: setup_flags=0x{:x} buffer-ring={} idle_timeout={:?} max_conns={:?}",
            rt.setup_flags(),
            rt.has_buffer_ring(),
            opts.idle_timeout,
            opts.max_conns
        );
    }
    rt.block_on(async move {
        let shared = Rc::new(Shared::default());
        let files = Rc::new(FileCache::default());
        let listener = Rc::new(listener);
        spawn(sweeper(Rc::clone(&shared), Rc::clone(&listener), opts.clone()));

        loop {
            match listener.accept().await {
                Ok(stream) => {
                    if opts.stopping() {
                        break;
                    }
                    if let Some(cap) = opts.max_conns {
                        if shared.active() >= cap {
                            shared.shed.set(shared.shed.get() + 1);
                            drop(stream); // shed: close at once, keep serving the rest
                            continue;
                        }
                    }
                    let _ = stream.set_nodelay(true);
                    let meta = shared.register(stream.as_raw_fd());
                    let app = Arc::clone(&app);
                    let shared = Rc::clone(&shared);
                    let files = Rc::clone(&files);
                    spawn(async move {
                        serve_connection(app, stream, &meta, &files).await;
                        shared.unregister(&meta);
                    });
                }
                Err(_) if opts.stopping() => break,
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {}
                Err(e) => {
                    eprintln!("accept: {e}");
                    ringrt::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        // Graceful shutdown: no new connections, drain, then cut the rest.
        drop(listener);
        let deadline = Instant::now() + opts.shutdown_grace;
        while shared.active() > 0 && Instant::now() < deadline {
            ringrt::time::sleep(Duration::from_millis(50)).await;
        }
        let cut = shared.active();
        if cut > 0 {
            shared.close_all();
            ringrt::time::sleep(Duration::from_millis(100)).await;
        }
        if log {
            eprintln!(
                "uring-async: shutdown complete (drained, {cut} connections cut, {} shed over the run)",
                shared.shed.get()
            );
        }
    })
}

/// Once a second: close idle connections; on STOP, unblock the accept loop.
async fn sweeper(shared: Rc<Shared>, listener: Rc<TcpListener>, opts: Opts) {
    loop {
        ringrt::time::sleep(Duration::from_millis(1000)).await;
        if opts.stopping() {
            let _ = listener.shutdown();
            return;
        }
        if let Some(idle) = opts.idle_timeout {
            shared.close_idle(idle);
        }
    }
}

/// `read` through the ring into `buf`, after whatever `buf` already holds
/// (the response head), until `size` bytes or EOF.
async fn read_file_into(f: &ringrt::fs::File, size: usize, buf: &mut Vec<u8>) -> io::Result<usize> {
    let start = buf.len();
    buf.reserve(size);
    let mut off = 0u64;
    let mut owned = std::mem::take(buf);
    while owned.len() - start < size {
        let (r, b) = f.read_at(owned, off).await;
        owned = b;
        match r {
            Ok(0) => break,
            Ok(n) => off += n as u64,
            Err(e) => {
                *buf = owned;
                return Err(e);
            }
        }
    }
    *buf = owned;
    Ok(size)
}

/// Serve a file request: head and body in one buffer so a single (zero-copy
/// when large) send carries both and ordering is never in question.
async fn serve_file(cache: &FileCache, outbuf: &mut Vec<u8>, f: &FileReq) -> io::Result<()> {
    // Learn the size first so the head can advertise it.
    let (file, size) = match cache.get(&f.path).await {
        Ok(x) => x,
        Err(e) => {
            conn::file_response(outbuf, f, Err(e), None);
            return Ok(());
        }
    };
    conn::file_response(outbuf, f, Ok(Vec::new()), Some(size));
    if f.head_only {
        return Ok(());
    }
    // On a read error the head is already in `outbuf`; the caller closes.
    read_file_into(&file, size, outbuf).await?;
    Ok(())
}

/// Same shape as `tokio_rt::serve_connection`, except that responses are sent
/// *detached*: the buffer goes to the runtime's pool when the kernel is done
/// and the task goes straight back to `recv`, saving one wake per request.
/// Large buffers go out with `SEND_ZC`. The socket closes when `stream` drops.
async fn serve_connection(app: Arc<App>, stream: TcpStream, meta: &ConnMeta, files: &FileCache) {
    let max_in = conn::max_inbuf(&app);
    let mut inbuf: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut outbuf: Vec<u8> = TcpStream::take_buffer(READ_CHUNK);
    let zc_min = zc_min();

    loop {
        match stream.recv(&mut inbuf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        meta.last_activity.set(Instant::now());
        if inbuf.len() > max_in {
            return;
        }
        loop {
            let sf = process_fs(&app, &mut inbuf, &mut outbuf);
            let step = sf.step;
            if let Some(f) = &sf.file {
                if serve_file(files, &mut outbuf, f).await.is_err() {
                    return;
                }
            }
            if let Some(ms) = step.delay_ms {
                ringrt::time::sleep(Duration::from_millis(ms)).await;
            }
            if !outbuf.is_empty() {
                let full = std::mem::replace(&mut outbuf, TcpStream::take_buffer(READ_CHUNK));
                let sent = if zc_min > 0 && full.len() >= zc_min {
                    stream.send_detached_zc(full).await
                } else {
                    stream.send_detached(full).await
                };
                if sent.is_err() {
                    return;
                }
            }
            if step.flow == Flow::Close {
                // Let the in-flight responses drain before the fd closes.
                let _ = stream.flush().await;
                return;
            }
            if (step.delay_ms.is_none() && sf.file.is_none()) || inbuf.is_empty() {
                break;
            }
        }
    }
}
