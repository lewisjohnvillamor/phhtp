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
                    let (r, b) = stream.recv(buf).await;
                    buf = b;
                    if matches!(r, Ok(0) | Err(_)) { return; }
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
* Buffers go **by value** into `recv` / `send_all` and come back with the result,
  because with a completion API the kernel owns the buffer until it says
  otherwise. A dropped future parks its buffer in the reactor until then.
* `time::sleep` is a kernel `TIMEOUT` op; no timer wheel.
* `ringrt::raw` exposes the ring alone for callback-style loops.

Requires Linux ≥ 5.6 and `kernel.io_uring_disabled = 0`. Docker's default
seccomp profile blocks io_uring; `Runtime::available()` tells you up front.

This crate was extracted from [phhtp](https://github.com/lewisjohnvillamor/phhtp),
an experiment in replacing Tokio for a thread-per-core HTTP server. The
repository has the benchmark against Tokio, mio and blocking threads, and a
study of the trade-offs against monoio, glommio, compio and tokio-uring.
