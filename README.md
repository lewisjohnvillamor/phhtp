# phhtp

An HTTP/1.1 server written from scratch in Rust, built to answer one question:
**on a $12/month DigitalOcean droplet, which I/O model gives the best p50 and p90
latency — and is Tokio worth it?**

The same protocol engine runs under seven different runtimes, so the only variable
in the benchmark is how the sockets are driven.

| `--runtime` | Model | Threads |
|---|---|---|
| `threads`  | Blocking `std::net`, one OS thread per connection | unbounded |
| `pool`     | Blocking `std::net`, fixed worker pool, connections queue behind it | `--workers` (default `max(64, 16×cores)`) |
| `mio`      | Hand-rolled edge-triggered `epoll` loop via `mio`, one loop per worker, `SO_REUSEPORT`, no sharing | `--workers` (default cores) |
| `tokio`    | Tokio multi-thread work-stealing scheduler | `--workers` (default cores) |
| `tokio-st` | Tokio current-thread scheduler | 1 |
| `uring`    | **io_uring on raw syscalls**: hand-laid-out SQE/CQE structs, mmap'd rings, multishot accept, `RECV`/`SEND`/`TIMEOUT` ops, one ring per worker, `SO_REUSEPORT`. No io_uring crate. | `--workers` (default cores) |
| `uring-async` | **Our own `async`/`await` runtime** on the same raw ring: a `std::task` executor with an `Rc`-backed waker, a reactor mapping completions to wakers, and `accept`/`recv`/`send`/`sleep` futures. The Tokio replacement candidate. | `--workers` (default cores) |

Built on: [`httparse`](https://crates.io/crates/httparse) (request parsing),
[`serde_json`](https://crates.io/crates/serde_json) (JSON endpoints),
[`hmac`](https://crates.io/crates/hmac) + [`sha2`](https://crates.io/crates/sha2)
(signing endpoints), [`mio`](https://crates.io/crates/mio),
[`tokio`](https://crates.io/crates/tokio), [`socket2`](https://crates.io/crates/socket2),
[`libc`](https://crates.io/crates/libc) (syscall numbers and `mmap` for the io_uring runtime),
[`hdrhistogram`](https://crates.io/crates/hdrhistogram) (the load generator's percentiles).
Everything HTTP — framing, keep-alive, pipelining, response encoding, routing — is
hand-written in `crates/phhtp-core`, and the io_uring runtime talks to the kernel
directly: `io_uring_setup`, `io_uring_enter`, and the shared-memory rings.

## Layout

```
crates/
  ringrt/         OUR RUNTIME, publishable: async/await on raw io_uring, libc only
    src/sys.rs      io_uring ABI: struct layouts, constants, the syscalls
    src/ring.rs     mmap'd submission/completion ring
    src/executor.rs task scheduler + Rc-backed waker, std::task only
    src/reactor.rs  completion → waker dispatch, orphan slots, cancel on drop
    src/op.rs       raw accept / recv / send / sleep futures (owned buffers)
    src/net.rs      TcpListener (bind, bind_reuseport, accept), TcpStream (recv, send_all)
    src/time.rs     sleep
    src/runtime.rs  Runtime::new / block_on; one per thread
  phhtp-core/     sans-IO engine: parse bytes → route → encode bytes. No sockets.
    src/http.rs     request parsing (httparse), Response encoding, limits
    src/conn.rs     process(): the per-connection step every runtime calls
    src/app.rs      router + handlers
    src/sig.rs      HMAC-SHA256 sign / constant-time verify, SHA-256 chain
    src/date.rs     cached Date: header
  phhtp-server/   the `phhtp` binary and the seven runtimes (src/runtime/*)
    src/runtime/mio_loop.rs      edge-triggered epoll loop
    src/runtime/uring/mod.rs     callback-style loop on ringrt::raw (the ring alone)
    src/runtime/uring_async/mod.rs  async fn serve_connection on ringrt, same shape as tokio
    tests/runtimes.rs   boots every runtime and checks identical behaviour on a real socket
  phhtp-bench/    the `phhtp-bench` load generator (keep-alive, HDR histogram, p50/p90/p99)
scripts/bench.sh  runs the runtime × endpoint matrix and prints a Markdown table
scripts/scale.sh  connection-count and worker-count sweeps with server RSS/threads
scripts/stress.sh fd exhaustion, thread exhaustion, CPU overload, with recovery probes
deploy/           systemd unit and sysctl for the droplet
bench/results/    sample results
docs/LANDSCAPE.md the study: existing io_uring runtimes, and what ringrt trades off
docs/SCALE.md     how far each runtime goes, what breaks first, and how it fails
```

The core is deliberately **sans-IO**: `phhtp_core::process(app, inbuf, outbuf)`
consumes every complete request at the front of `inbuf`, appends the responses to
`outbuf`, and tells the runtime whether to keep the connection open and whether to
hold the output back (for `/sleep`). Each runtime is only the loop around that call.
That is what makes the comparison fair.

## Endpoints

| Method | Path | What it measures |
|---|---|---|
| GET  | `/`              | pure framing overhead (13-byte `text/plain`) |
| GET  | `/health`        | `{"status":"ok"}` |
| GET  | `/json`          | small `serde_json` document |
| POST | `/echo`          | body copy, `Content-Type` passthrough |
| POST | `/sign`          | HMAC-SHA256 of the body → `{"alg":"HMAC-SHA256","bytes":N,"signature":"<hex>"}` |
| POST | `/verify`        | `X-Signature: <hex>` checked against the body, constant time → 200 / 401 |
| GET  | `/compute?n=N`   | N chained SHA-256 rounds. **CPU-bound** handler (max 100000) |
| GET  | `/sleep?ms=M`    | responds after M ms without blocking other connections. **Simulated upstream I/O** (max 10 s) |

HTTP/1.1 behaviour: keep-alive by default, `Connection: close` honoured both ways,
HTTP/1.0 defaults to close, pipelining, `HEAD`, `405` with `Allow`, `400` on a
missing `Host`, `413` when `Content-Length` exceeds `--max-body` (checked before
reading the body), `431` for oversized heads, `501` for `Transfer-Encoding: chunked`
request bodies. Every protocol error closes the connection.

## Quick start

```sh
cargo build --release
./target/release/phhtp --runtime tokio --bind 0.0.0.0:8080
curl -s localhost:8080/json
curl -s -X POST --data 'hello' localhost:8080/sign
```

```
phhtp --runtime <threads|pool|mio|tokio|tokio-st|uring|uring-async> [--bind ADDR] [--workers N]
      [--secret S | PHHTP_SECRET=S] [--max-body BYTES] [--max-head BYTES]
```

## The io_uring runtime

`--runtime uring` is written against the kernel ABI with nothing in between.
The ring lives in the `ringrt` crate (`ringrt::raw`); the server loop is in
`crates/phhtp-server/src/runtime/uring/mod.rs`:

* `sys.rs` transcribes `struct io_uring_sqe`, `io_uring_cqe`, `io_uring_params`
  and the offsets structs from `<linux/io_uring.h>`, with compile-time size
  assertions, and wraps the `io_uring_setup` / `io_uring_enter` syscalls.
* `ring.rs` mmaps the submission ring, completion ring and SQE array
  (one mapping when the kernel reports `IORING_FEAT_SINGLE_MMAP`), and
  implements the head/tail protocol with acquire/release atomics on the shared
  memory. Setup tries `SINGLE_ISSUER | COOP_TASKRUN | DEFER_TASKRUN` first and
  falls back on older kernels.
* `mod.rs` is the server: one ring per worker thread, each with its own
  `SO_REUSEPORT` listener. Accept is multishot (one SQE, a stream of
  completions; falls back to single-shot before 5.19). Every connection has
  exactly one operation in flight: `RECV` into the spare capacity of its input
  buffer, then `process()`, then `SEND` (re-issued on short writes), and for
  `/sleep` a `TIMEOUT` op whose `__kernel_timespec` lives in the boxed
  connection so its address stays valid. One op per connection means the
  kernel never writes a buffer we are reading and there are no stale
  completions to guard against.

It is the same shape as the `mio` loop — one thread, one listener, one queue —
so the difference between the two rows is readiness-plus-syscalls versus
completions-plus-shared-memory, nothing else.

Requirements: Linux ≥ 5.6 (5.19+ for multishot accept) and
`kernel.io_uring_disabled = 0`. Some container runtimes block `io_uring_setup`
via seccomp; the server then exits with a clear message and the integration
test skips this runtime.

## The `uring-async` runtime: replacing Tokio, and the `ringrt` crate

`uring` above is callback-shaped: handlers cannot `await`. To be a real Tokio
replacement the runtime also needs a task scheduler, so `uring-async` adds one,
written from scratch on `std::task` / `std::future` / `std::pin`. There is no
tokio, mio, futures, or async-task crate anywhere in it.

That runtime is now its own publishable crate, **[`ringrt`](crates/ringrt)**
(`libc` is its only dependency; the name is free on crates.io as of 2026-10-16):

```rust
use ringrt::{net::TcpListener, spawn, Runtime};

Runtime::new()?.block_on(async {
    let listener = TcpListener::bind("0.0.0.0:8080".parse().unwrap())?;
    loop {
        let stream = listener.accept().await?;
        spawn(async move { /* stream.recv(buf).await, stream.send_all(buf).await */ });
    }
})?
```

`cargo publish -p ringrt --dry-run` passes. Publishing for real is one command
once you own the crates.io name. **Read [`docs/LANDSCAPE.md`](docs/LANDSCAPE.md)
first**: it surveys monoio, glommio, compio, tokio-uring, ringline and ringcore,
and lists what `ringrt` gives up against Tokio (p99, blocking pool, ecosystem,
io_uring being blocked by Docker's default seccomp profile) and what it gains.

* **Executor** (`executor.rs`): tasks are `Rc<Task>` holding a boxed future and a
  `queued` flag; the run queue is a `VecDeque`. The waker is a hand-written
  `RawWakerVTable` whose data pointer *is* the `Rc`, so `wake()` is one `Cell`
  write and one `push_back`: no atomics, no locks. This is sound because wakers
  never cross threads: the only thing that stores one is the thread-local reactor.
* **Reactor** (`reactor.rs`): owns the ring. Each in-flight op gets a slot whose
  index is the SQE's `user_data`; the slot holds the waker until the CQE lands,
  then the result until the future is polled. If a future is dropped mid-flight,
  the slot becomes an *orphan* that keeps the buffer alive until the kernel is
  done and an `ASYNC_CANCEL` is issued. One `io_uring_enter` per scheduler
  turn submits everything queued and waits for completions.
* **I/O futures** (`io.rs`): `accept(fd)`, `recv(fd, Vec<u8>)`,
  `send_all(fd, Vec<u8>)`, `sleep_ms(ms)`. Buffers are passed **by value** and
  handed back with the result. That is the only ownership model that is sound
  with a completion API (the same choice tokio-uring and glommio make), and it
  is the one visible API difference from Tokio's `read(&mut buf)`.
* **Server** (`mod.rs`): `async fn serve_connection` is line-for-line the same
  shape as the Tokio version. The `tokio` and `uring-async` rows therefore
  compare *Tokio's scheduler + epoll* against *our scheduler + io_uring* with
  identical handler code.

What it does not have yet, and what a production replacement would need:
work stealing across workers (each worker is an island; `SO_REUSEPORT` balances
connections, not load), a blocking pool for CPU-heavy handlers, a `JoinHandle`,
cancellation of in-flight ops on connection close (today a dropped `recv`
future orphans its buffer until the peer sends or closes), timers that
coalesce (each `sleep` is a kernel `TIMEOUT` op), and the ecosystem: nothing
that depends on `tokio::io::AsyncRead` can run on it.

## Benchmarking

```sh
scripts/bench.sh                          # every runtime × every scenario, 10 s each
RUNTIMES="tokio mio uring" DURATION=5 scripts/bench.sh
WORKERS=1 scripts/bench.sh                # pin all runtimes to one worker
TARGET=http://<droplet-ip>:8080 scripts/bench.sh   # drive a remote server
```

Scenarios (override with `SCENARIOS=...`): `plaintext` (`GET /`), `json`, `sign`
(POST, 256-byte body), `compute` (`n=2000`), `sleep` (`ms=5`, `SLEEP_CONNS=256`
connections). All others use `CONNS=64` keep-alive connections, one request in
flight per connection.

Or drive it by hand:

```sh
./target/release/phhtp-bench run http://127.0.0.1:8080/json -c 64 -d 10
./target/release/phhtp-bench run http://127.0.0.1:8080/sign -X POST --body-size 256 --json >> r.jsonl
./target/release/phhtp-bench report r.jsonl      # Markdown table
```

`phhtp-bench` records every request's latency in an HDR histogram (1 µs resolution,
3 significant figures) and reports p50 / p90 / p99 / p99.9 / max / mean plus
throughput, errors and non-2xx counts. Cross-check with
[`oha`](https://github.com/hatoo/oha) if you like: `oha -z 10s -c 64 http://127.0.0.1:8080/`.

### Sample results (4-core container, client and server on the same box)

Full table in [`bench/results/sample-4core-container.md`](bench/results/sample-4core-container.md)
(unpinned, all seven runtimes, one 4-second run each). Indicative only: on this
shared box two identical runs differ by up to ±15 %, so differences below that
are noise. For decisions, the interleaved repeated measurement below is the one
to read.

| runtime | `GET /json` req/s | p50 | p90 | p99 | `GET /sleep?ms=5` ×256 req/s | p50 | p90 |
|---|---:|---:|---:|---:|---:|---:|---:|
| threads     | 87k  | 0.67ms | 1.18ms | 2.25ms | 45.5k | 5.46ms | 6.24ms |
| pool(64)    | 90k  | 0.65ms | 1.12ms | 2.20ms | **12.0k** | 5.29ms | 5.51ms |
| mio         | 142k | 0.34ms | 0.91ms | 1.74ms | 42.2k | 6.01ms | 6.56ms |
| tokio       | 137k | 0.42ms | **0.73ms** | **1.25ms** | 37.9k | 6.71ms | 7.55ms |
| tokio-st    | 129k | 0.46ms | 0.73ms | 1.27ms | 33.6k | 7.59ms | 8.63ms |
| uring       | 131k | 0.39ms | 0.94ms | 1.75ms | 45.2k | 5.50ms | 6.33ms |
| uring-async | **155k** | **0.32ms** | 0.80ms | 1.50ms | **45.7k** | **5.43ms** | **6.22ms** |

**Tokio versus `ringrt` after multishot recv + provided buffers**, server pinned
to 2 cores, client on the other 2, `GET /`, three interleaved repetitions,
medians (`bench/results/multishot-reps.txt`):

| connections | tokio req/s (range) | uring-async req/s (range) | tokio p50 / p99 | uring-async p50 / p99 |
|---:|---:|---:|---|---|
| 1 024 | 148k (136–169k) | 130k (129–158k) | 2.80 / 45.3 ms | 2.84 / 46.6 ms |
| 16 000 | 48k (41–62k) | 92k (89–93k) | 5.44 / 1117 ms | 4.45 / 1167 ms |

And at 64 connections in a same-session A/B before the environment changed:
`uring-async` 178k vs tokio 177k; the callback `uring` loop 198k.

What the shapes show:

* **At light and moderate load `ringrt` now matches Tokio**: equal throughput
  and p50 at 64 connections, 10 % behind at 1 024 with overlapping ranges.
  Multishot recv with a provided-buffer ring was worth ~10 % on its own
  (163k → 178k at 64 connections in the A/B).
* **Under a 16 000-connection load `ringrt` is now ~2× Tokio and far more
  stable** (89–93k across runs versus 41–62k). One `RECV` per connection for
  its lifetime instead of one per request is what changes at that scale.
* **Tokio keeps the tightest p99 at moderate load** (work stealing), and the
  single-core `tokio` tail problem from the scale study is unchanged.
* **The async layer costs ~10 % over the callback loop** on the same ring
  (178k vs 198k at 64 connections): boxed task, waker clone, `RefCell` borrow.
* **Where the server waits** (`/sleep`), io_uring's kernel timers keep the
  lead they had before: 45–46k vs 38k req/s.
* **Provided-buffer ring size did not matter** once measured properly (256,
  1 024 and 2 048 entries within noise at both 1 024 and 16 000 connections);
  an earlier single run that suggested otherwise was noise. Default is 256.

### Scale and failure modes

[`docs/SCALE.md`](docs/SCALE.md) has the connection sweep to 16 000 keep-alive
connections, the worker sweep on a pinned 2-core server, and the stress
experiments (fd exhaustion, thread exhaustion, CPU overload), with the
failure taxonomy in the order a droplet will meet them. Headline: the first
thing that breaks is the kernel's accept backlog under a reconnect storm,
not any runtime; `threads` is the only model that degrades from ~1 000
connections; `pool` fails silently by never serving connections beyond its
size; nothing crashes any more.

## Deploying to a $12 DigitalOcean droplet

The $12/month Basic droplet is **1 vCPU, 2 GB RAM**. Implications:

* Build **on** the droplet, or cross-compile for `x86_64-unknown-linux-gnu` and
  `scp` the binary. A release build needs ~1.5 GB of RAM and a few minutes on 1 vCPU.
* `--workers` defaults to the core count, so `mio` and `tokio` will run one
  worker. Try `--workers 2` anyway: on a shared vCPU, hiding syscall latency with
  a second thread sometimes helps and sometimes hurts, and that is exactly the
  kind of thing this repo is for.
* **Run the load generator from a different machine** (a second droplet in the same
  region, or your laptop). Running it on the 1-vCPU droplet would measure the
  client as much as the server. Use `TARGET=http://<ip>:8080 scripts/bench.sh`
  from the client, and switch `--runtime` on the droplet between runs.
  Network RTT adds a constant to p50; the *differences* between runtimes and the
  p90/p50 ratio are what to compare.

```sh
# on the droplet (Ubuntu)
sudo apt-get install -y build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && . ~/.cargo/env
git clone https://github.com/lewisjohnvillamor/phhtp && cd phhtp
cargo build --release
sudo cp target/release/phhtp /usr/local/bin/
sudo cp deploy/sysctl-phhtp.conf /etc/sysctl.d/90-phhtp.conf && sudo sysctl --system
sudo cp deploy/phhtp.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl enable --now phhtp
sudo systemctl edit phhtp      # set Environment=PHHTP_RUNTIME=mio etc., then restart
```

The unit raises `LimitNOFILE` to 65536 (each connection is a file descriptor) and
runs as a dynamic unprivileged user.

## Tests

```sh
cargo test --workspace
```

Unit tests cover the parser (partial input, pipelining, HTTP/1.0 semantics, every
error path), the response encoder, the `Date` formatter against known timestamps,
HMAC against RFC 4231 test case 2, and the `process` loop. The integration test boots
all five runtimes on ephemeral ports and verifies identical behaviour over real
sockets: keep-alive, pipelining, split bodies, `HEAD`, `Connection: close`,
sign/verify, every protocol error, and that `/sleep` never blocks other connections.

## Not implemented (on purpose, for now)

Chunked request bodies (501), `Expect: 100-continue`, TLS, HTTP/2, request
timeouts / idle-connection reaping, graceful shutdown. Each would be a fair
addition once the runtime question is settled.
