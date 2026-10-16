# ringrt

A small `async`/`await` runtime on **raw Linux io_uring**. No tokio, no mio, no
`io-uring` crate, no `futures`; the only dependency is `libc`.

```rust
use ringrt::{net::TcpListener, spawn, Runtime};

fn main() -> std::io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8080".parse().unwrap())?;
        loop {
            let stream = listener.accept().await?;
            spawn(async move {
                let mut buf = Vec::with_capacity(16 * 1024);
                loop {
                    if matches!(stream.recv(&mut buf).await, Ok(0) | Err(_)) { return; }
                    let (r, b) = stream.send_all(buf).await;
                    buf = b;
                    buf.clear();
                    if r.is_err() { return; }
                }
            });
        }
    })?
}
```

* One `Runtime` per thread, tasks are `!Send`, no work stealing. Scale with one
  runtime per core and `TcpListener::bind_reuseport`.
* `recv(&mut Vec<u8>)` borrows like Tokio's `read`: one multishot `RECV` per
  stream delivers into a kernel-registered provided-buffer ring, and the reactor
  copies out and recycles the buffer at completion time, so the kernel never
  sees your memory. Single-shot fallback before Linux 6.0.
* `send_all(Vec<u8>)` lends its buffer to the kernel and hands it back; a dropped
  future parks it in the reactor until the completion lands.
* `accept` is one multishot op per listener; `time::sleep` is a kernel `TIMEOUT`
  op; no timer wheel.
* `ringrt::raw` exposes the ring alone for callback-style loops.

Requires Linux ≥ 5.6 and `kernel.io_uring_disabled = 0` (multishot accept
5.19+, multishot recv with buffer rings 6.0+, with transparent single-shot
fallback below). Docker's default seccomp profile blocks io_uring;
`Runtime::available()` tells you up front.

Tuning: `RINGRT_BUF_ENTRIES` (default 4096) and `RINGRT_BUF_SIZE` (default
4096) size the per-runtime provided-buffer ring (16 MiB per worker by
default). A ring that is too small for a connect burst costs tail latency on
every connection, not just the burst's: see `docs/EXPANDED.md` §8. `RINGRT_NO_MULTISHOT=1` forces single-shot ops, `RINGRT_TRACE=1`
prints every multishot arm and completion.

This crate was extracted from [phhtp](https://github.com/lewisjohnvillamor/phhtp),
an experiment in replacing Tokio for a thread-per-core HTTP server. The
repository has the benchmark against Tokio, mio and blocking threads, and a
study of the trade-offs against monoio, glommio, compio and tokio-uring.
