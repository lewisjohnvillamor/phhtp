# phhtp

An HTTP/1.1 server written from scratch in Rust, built to answer one question:
**on a $12/month DigitalOcean droplet, which I/O model gives the best p50 and p90
latency — and is Tokio worth it?**

The same protocol engine runs under eleven runtimes, seven written here and four
open-source io_uring runtimes pulled in for comparison, so the only variable in
the benchmark is how the sockets are driven.

| `--runtime` | Model | Threads |
|---|---|---|
| `threads`  | Blocking `std::net`, one OS thread per connection | unbounded |
| `pool`     | Blocking `std::net`, fixed worker pool, connections queue behind it | `--workers` (default `max(64, 16×cores)`) |
| `mio`      | Hand-rolled edge-triggered `epoll` loop via `mio`, one loop per worker, `SO_REUSEPORT`, no sharing | `--workers` (default cores) |
| `tokio`    | Tokio multi-thread work-stealing scheduler | `--workers` (default cores) |
| `tokio-st` | Tokio current-thread scheduler | 1 |
| `uring`    | **io_uring on raw syscalls**: hand-laid-out SQE/CQE structs, mmap'd rings, multishot accept, `RECV`/`SEND`/`TIMEOUT` ops, one ring per worker, `SO_REUSEPORT`. No io_uring crate. | `--workers` (default cores) |
| `uring-async` | **Our own `async`/`await` runtime** on the same raw ring: a `std::task` executor with an `Rc`-backed waker, a reactor mapping completions to wakers, and `accept`/`recv`/`send`/`sleep` futures. The Tokio replacement candidate. | `--workers` (default cores) |
| `monoio`   | [ByteDance's monoio](https://github.com/bytedance/monoio) 0.2, thread-per-core on io_uring, owned buffers | `--workers` (default cores) |
| `compio`   | [compio](https://github.com/compio-rs/compio) 0.19, completion-based thread-per-core (io_uring on Linux), the runtime Apache Iggy moved to | `--workers` (default cores) |
| `glommio`  | [Datadog's glommio](https://github.com/DataDog/glommio) 0.9, thread-per-core on io_uring, `futures` traits | `--workers` (default cores) |
| `tokio-uring` | [tokio-uring](https://github.com/tokio-rs/tokio-uring) 0.5, tokio current-thread with an io_uring driver, one runtime per worker | `--workers` (default cores) |

The four external runtimes live in `crates/phhtp-server/src/runtime/ext/` behind
the default `ext` cargo feature (`--no-default-features` skips their compile
time). Each drives the identical `process()` with a `serve_connection` of the
same shape as ours, one runtime and one `SO_REUSEPORT` listener per worker.

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
  phhtp-server/   the `phhtp` binary and the eleven runtimes (src/runtime/*)
    src/runtime/mio_loop.rs      edge-triggered epoll loop
    src/runtime/uring/mod.rs     callback-style loop on ringrt::raw (the ring alone)
    src/runtime/uring_async/mod.rs  async fn serve_connection on ringrt, same shape as tokio
    src/runtime/ext/             monoio, compio, glommio, tokio-uring (feature `ext`)
    tests/runtimes.rs   boots every runtime and checks identical behaviour on a real socket
  phhtp-bench/    the `phhtp-bench` load generator (keep-alive, HDR histogram, p50/p90/p99)
scripts/bench.sh  runs the runtime × endpoint matrix and prints a Markdown table
scripts/scale.sh  connection-count and worker-count sweeps with server RSS/threads
scripts/stress.sh fd exhaustion, thread exhaustion, CPU overload, with recovery probes
scripts/report.py renders the JSONL dataset as an HTML report of charts (+ SVGs)
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
phhtp --runtime <threads|pool|mio|tokio|tokio-st|uring|uring-async|monoio|compio|glommio|tokio-uring>
      [--bind ADDR] [--workers N]
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
flight per connection. `REPS=3` repeats every measurement and records medians
(identical runs on a shared box differ by up to ±15 %).

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

### What is measured, per row

| Column | Source | Why it matters |
|---|---|---|
| req/s, p50 / p90 / p99 / p99.9 / max | client-side HDR histogram | the usual throughput and latency |
| **CPU % and µs CPU per request** | server's `GET /stats` (`/proc/self/stat` user + system time), fetched at the start and end of the measured window | two runtimes can serve the same req/s while one burns twice the CPU; on a 1-vCPU droplet this is the number that decides. User time is the runtime's own cost, system time is the kernel's. |
| context switches per request | sum of every thread's `voluntary` + `nonvoluntary` switches | shows whether a runtime sleeps and wakes per request or batches |
| RSS and peak RSS, threads | `VmRSS` / `VmHWM` / `Threads` at the end of the window (sweeps also sample mid-run) | memory per connection; the thread count of `threads` |
| page faults | `minflt` / `majflt` deltas | allocation churn |
| **disk read / write bytes** | `/proc/self/io` `read_bytes` / `write_bytes` deltas | the server does no disk I/O and the column proves it; a runtime that logged or paged would show here |
| **client CPU % and µs per request** | the load generator's own `getrusage` | proves whether the generator, not the server, is the ceiling. On this 4-core box it is, for the fastest servers: 186–197 % of its two cores |
| mode | `closed-loop` / `open-loop N req/s` / `connection per request` | see below |

The server samples itself, so the same columns are recorded when the load
generator runs on another machine. `--no-stats` skips it; it is silently absent
for servers that do not serve `/stats`. Not measured: system-wide softirq time
(network receive processing the kernel attributes to whichever context runs it)
and syscall counts (needs `perf`/`strace`, both of which distort the run).

### Three load shapes, because closed-loop alone misleads

* **Closed loop** (default): one request in flight per connection, the next sent
  when the previous completes. Throughput is the server's capacity; latency at
  64 connections is mostly *queueing* (64 in flight ÷ 150k/s ≈ 0.4 ms), and a
  stalled server is invisible because the client politely waits (coordinated
  omission).
* **Open loop** (`--rate N`): requests are due at fixed intervals and latency is
  measured from the *intended* send time. At a rate under capacity it shows the
  true unloaded service time (60 µs p50 for `/json` here); over capacity it
  shows the queue growing (454 ms p50 at a 200k target against a 137k server).
  This is the number an SLO is written against.
* **Churn** (`--close`): `Connection: close` on every request, a fresh connection
  each time, the connect inside the latency. Exercises accept and close, where
  multishot accept matters. On this box the generator spends 79 µs of CPU per
  request against the server's 42, so churn here measures the client as much as
  the server.

### Results: eleven runtimes, charted

The full dataset (every row `phhtp-bench` recorded, 129 measurements, each with the server's CPU, memory, context-switch and disk counters) is committed
in [`bench/results/dataset/`](bench/results/dataset/) and rendered as an
interactive report with charts, hover tooltips and a sortable table in
[`bench/report.html`](bench/report.html), also hosted at
<https://claude.ai/artifact/Kd38ay5T3BLRxneBd3d1co> (`scripts/report.py` regenerates it). Matrix rows are
**medians of three repetitions**; sweep rows are single runs. Identical runs
on this shared 4-core box differ by up to ±15 %, so treat smaller gaps as noise.
Color is the I/O-model family; ◆ marks the runtimes written in this repository.

![GET /json throughput, all runtimes](bench/charts/matrix-json-rps.svg)

![GET /json p99 latency, all runtimes](bench/charts/matrix-json-p99.svg)

![GET /sleep throughput, all runtimes](bench/charts/matrix-sleep-rps.svg)

![GET /json server CPU per request, all runtimes](bench/charts/matrix-json-cpu.svg)

![Throughput versus connections, one panel per runtime](bench/charts/conns-rps.svg)

| runtime | `GET /json` req/s (median of 3, range) | p50 | p99 | CPU per request | `/sleep?ms=5` ×256 req/s | 16 000 conns req/s | RSS at 16 000 |
|---|---:|---:|---:|---:|---:|---:|---:|
| ◆ uring | 147k (139–148k) | 0.34ms | 1.59ms | 9.9 µs | 45.2k | 100k | 138 MB |
| ◆ mio | 139k (127–140k) | 0.36ms | 1.77ms | 10.6 µs | 41.3k | 87k | 260 MB |
| ◆ uring-async | 131k (120–131k) | 0.39ms | 1.83ms | 11.6 µs | 44.8k | 108k | 145 MB |
| ◆ tokio | 130k (128–133k) | 0.44ms | 1.29ms | 9.3 µs | 37.6k | 113k | 274 MB |
| ◆ pool | 123k (106–135k) | 0.49ms | 1.33ms | not sampled | 11.9k | 45k | fails |
| ◆ tokio-st | 115k (104–117k) | 0.52ms | 1.41ms | 7.8 µs | 34.0k | 70k | 388 MB |
| compio | 105k (105–109k) | 0.47ms | 2.41ms | 15.3 µs | 46.2k | 74k | 213 MB |
| tokio-uring | 103k (101–105k) | 0.48ms | 2.42ms | 15.8 µs | 37.5k | 81k | 205 MB |
| monoio | 103k (102–112k) | 0.49ms | 2.43ms | 15.1 µs | 37.7k | 75k | 214 MB |
| ◆ threads | 98k (95–99k) | 0.59ms | 2.00ms | 18.4 µs | 44.9k | fails | 50 MB |
| glommio | 75k (70–77k) | 0.64ms | 4.47ms | 31.1 µs | 42.0k | 57k | 286 MB |

What the eleven-way comparison shows:

* **The from-scratch runtimes top the throughput and p50 columns on this box.**
  `uring` leads every published io_uring runtime on `/json` by 40 % over compio,
  monoio and tokio-uring and nearly 2× glommio, driving the same handler and the
  same per-worker listener layout. The difference is runtime overhead.
* **CPU per request is where Tokio holds its ground.** At equal throughput Tokio
  spends 9.3 µs of CPU per request, `uring` 9.9, `uring-async` 11.6, the
  published io_uring runtimes 15–16, glommio 31. Nearly all of it is kernel time
  in the TCP stack; the user-mode slice is under 15 %. On a 1-vCPU droplet this
  column decides.
* **One worker is the droplet case** (pinned, 1 024 connections): `uring` 112k
  req/s at 8.7 µs/req with a 16 ms p99; Tokio 107k at 9.1 µs but a 164 ms p99,
  because its multi-thread scheduler starves connections with nothing to steal
  from; `uring-async` 96k at 10.1 µs with a 19 ms p99.
* **At 16 000 connections** Tokio (113k) and `uring-async` (108k) lead, at
  274 MB and 145 MB resident respectively; the published runtimes sit at
  57–81k and burn 19–33 µs of CPU per request there.
* **Under `/sleep`** the completion runtimes with kernel timers win throughput
  (compio and ours at 45–46k against Tokio's 38k), but mio is the most
  CPU-efficient by far at 13 µs/req: a user-space timer heap is cheaper than a
  kernel `TIMEOUT` op per request.
* **`pool` and `threads` fail at 16 000 connections** (the queue and the thread
  limit); everything event-driven serves them. `pool`'s CPU is not sampled
  because its 64 workers are all busy and cannot serve the `/stats` probe.

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
