# phhtp

An HTTP/1.1 server written from scratch in Rust, built to answer one question:
**on a $12/month DigitalOcean droplet, which I/O model gives the best p50 and p90
latency — and is Tokio worth it?**

The same protocol engine runs under six different runtimes, so the only variable
in the benchmark is how the sockets are driven.

| `--runtime` | Model | Threads |
|---|---|---|
| `threads`  | Blocking `std::net`, one OS thread per connection | unbounded |
| `pool`     | Blocking `std::net`, fixed worker pool, connections queue behind it | `--workers` (default `max(64, 16×cores)`) |
| `mio`      | Hand-rolled edge-triggered `epoll` loop via `mio`, one loop per worker, `SO_REUSEPORT`, no sharing | `--workers` (default cores) |
| `tokio`    | Tokio multi-thread work-stealing scheduler | `--workers` (default cores) |
| `tokio-st` | Tokio current-thread scheduler | 1 |
| `uring`    | **io_uring on raw syscalls**: hand-laid-out SQE/CQE structs, mmap'd rings, multishot accept, `RECV`/`SEND`/`TIMEOUT` ops, one ring per worker, `SO_REUSEPORT`. No io_uring crate. | `--workers` (default cores) |

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
  phhtp-core/     sans-IO engine: parse bytes → route → encode bytes. No sockets.
    src/http.rs     request parsing (httparse), Response encoding, limits
    src/conn.rs     process(): the per-connection step every runtime calls
    src/app.rs      router + handlers
    src/sig.rs      HMAC-SHA256 sign / constant-time verify, SHA-256 chain
    src/date.rs     cached Date: header
  phhtp-server/   the `phhtp` binary and the six runtimes (src/runtime/*)
    src/runtime/mio_loop.rs   edge-triggered epoll loop
    src/runtime/uring/sys.rs  io_uring ABI: struct layouts, constants, the syscalls
    src/runtime/uring/ring.rs mmap'd submission/completion ring
    src/runtime/uring/mod.rs  the completion-driven server loop
    tests/runtimes.rs   boots every runtime and checks identical behaviour on a real socket
  phhtp-bench/    the `phhtp-bench` load generator (keep-alive, HDR histogram, p50/p90/p99)
scripts/bench.sh  runs the runtime × endpoint matrix and prints a Markdown table
deploy/           systemd unit and sysctl for the droplet
bench/results/    sample results
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
phhtp --runtime <threads|pool|mio|tokio|tokio-st|uring> [--bind ADDR] [--workers N]
      [--secret S | PHHTP_SECRET=S] [--max-body BYTES] [--max-head BYTES]
```

## The io_uring runtime

`--runtime uring` is written against the kernel ABI with nothing in between:

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

Full table in [`bench/results/sample-4core-container.md`](bench/results/sample-4core-container.md).
Indicative only — the load generator was competing with the server for CPU.

| runtime | `GET /json` req/s | p50 | p90 | p99 | `GET /sleep?ms=5` ×256 req/s | p90 | `GET /compute?n=2000` p90 |
|---|---:|---:|---:|---:|---:|---:|---:|
| threads  | 106k | 0.55ms | 0.95ms | 1.86ms | 46.5k | 5.96ms | 18.2ms |
| pool(64) | 110k | 0.54ms | 0.92ms | 1.61ms | **12.0k** | 5.51ms | 19.1ms |
| mio      | **160k** | **0.31ms** | 0.78ms | 1.47ms | 42.6k | 6.45ms | **15.3ms** |
| tokio    | 151k | 0.38ms | **0.63ms** | **1.03ms** | 37.8k | 7.58ms | 17.5ms |
| tokio-st | 123k | 0.49ms | 0.78ms | 1.23ms | 34.5k | 8.31ms | **50.4ms** |

What the shapes already show, independent of the exact numbers:

* **Framing cost dominates on trivial endpoints.** `mio` wins on raw throughput and
  p50 because it does the least per request: no task allocation, no scheduler, no
  wakers. Tokio's work-stealing buys the *tightest tail* (best p90/p99).
* **Blocking threads are not slow.** On a few dozen keep-alive connections the
  thread-per-connection model is within 30–40% of the async runtimes and has the
  simplest code. It loses when connections go into the thousands.
* **The thread pool caps concurrency, not CPU.** With 64 workers and 256
  connections on `/sleep?ms=5`, throughput is exactly `64 / 5 ms ≈ 12.8k req/s`:
  192 connections sit in the accept queue. p50 looks fine; *the queue is invisible
  in latency because queued connections aren't being measured yet* — watch req/s.
* **CPU-bound handlers on a single-threaded runtime are a cliff.** `tokio-st`
  serialises every `/compute` behind one core (p90 50 ms vs 15–19 ms). On a
  1-vCPU droplet *every* runtime is in this situation; the question becomes how
  cheaply the runtime multiplexes I/O around the CPU work, which `WORKERS=1
  scripts/bench.sh` lets you measure.

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
