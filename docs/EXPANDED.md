# Expanded tests, and what is actually different about `ringrt`

Companion to [SCALE.md](SCALE.md) (connection/worker sweeps, failure modes)
and [LANDSCAPE.md](LANDSCAPE.md) (the other io_uring runtimes). This document
covers the second round of load shapes, added because the first round left
three questions open: how much of the per-request cost is the syscall boundary,
how each runtime behaves when the traffic is *not* a tight keep-alive loop, and
where each runtime's latency knee is under open-loop load.

Box: the same 4-core shared benchmark container as the rest of the dataset,
kernel 6.18, server pinned to cores 0–1 and the load generator to cores 2–3.
Rows are in `bench/results/dataset/expand.jsonl`; `scripts/expand.sh` and
`scripts/syscalls.sh` regenerate them.

## 1. System calls per request

`strace -c -f` attached to the server for a 4 s window of `GET /json` on 64
keep-alive connections (`scripts/syscalls.sh`). strace slows every traced call,
so the req/s column is **not** comparable to the matrix; the counts are what
matter, and an untraced run makes the same calls.

| runtime | syscalls/req | what they are |
|---|---:|---|
| threads | 2.01 | `recvfrom` 1.00, `sendto` 1.00 |
| pool | 1.95 | `recvfrom` 0.97, `sendto` 0.97 |
| mio | 2.05 | `recvfrom` 0.99, `sendto` 0.99, `epoll_wait` 0.06 |
| tokio | 2.05 | `recvfrom` 0.99, `sendto` 0.99, `futex` 0.03, `epoll_wait` 0.03 |
| tokio-st | 2.03 | `recvfrom` 1.00, `sendto` 1.00, `epoll_wait` 0.02 |
| glommio | 2.24 | `recvfrom` 0.99, `sendto` 0.99, `io_uring_enter` 0.26 |
| tokio-uring | 2.20 | `epoll_wait` + `io_uring_enter` + `recvfrom`/`sendto` (polls the ring through epoll) |
| compio | 0.18 | `io_uring_enter` 0.18 |
| monoio | 0.17 | `io_uring_enter` 0.17 |
| **uring** ◆ | **0.18** | `io_uring_enter` 0.18 |
| **uring-async** ◆ | **0.17** | `io_uring_enter` 0.17 |

The readiness model has a floor of two calls per request (one to pull the
bytes up, one to push the reply down); `epoll_wait` is already amortized to
almost nothing. The completion model with multishot recv and a provided-buffer
ring makes about one call per six requests, and that call is the batched
submit of the responses. Two io_uring runtimes do not get there: glommio tries
a non-blocking `recvfrom`/`sendto` first and only falls into the ring on
`EAGAIN`; tokio-uring waits on the ring's fd with epoll.

## 2. HTTP/1.1 pipelining (16 requests per write, 64 connections)

![pipelining](../bench/charts/expand-pipeline-rps.svg)

| runtime | req/s | p50 | p99 | p99.99 | server µs CPU/req |
|---|---:|---:|---:|---:|---:|
| mio | 1 870 k | 0.55 ms | 1.24 ms | 4.9 ms | 0.9 |
| tokio | 1 826 k | 0.55 ms | 1.20 ms | 3.6 ms | 1.0 |
| uring ◆ | 1 739 k | 0.59 ms | 1.29 ms | 4.9 ms | 1.0 |
| uring-async ◆ | 1 669 k | 0.63 ms | 1.73 ms | 9.4 ms | 1.1 |
| threads | 1 495 k | 0.71 ms | 2.28 ms | 21.2 ms | 1.4 |
| tokio-uring | 1 456 k | 0.69 ms | 1.64 ms | 7.7 ms | 1.3 |
| pool | 1 437 k | 0.66 ms | 2.59 ms | 12.6 ms | (not sampled) |
| compio | 1 373 k | 0.74 ms | 1.75 ms | 5.7 ms | 1.5 |
| monoio | 1 300 k | 0.79 ms | 2.27 ms | 11.8 ms | 1.6 |
| glommio | 1 122 k | 0.89 ms | 1.94 ms | 8.4 ms | 1.8 |
| tokio-st | 1 001 k | 0.95 ms | 2.64 ms | 6.8 ms | 1.1 |

With sixteen requests per packet the sans-IO engine answers a whole batch in
one pass and the per-request cost of *every* runtime collapses to about one
microsecond of server CPU. The load generator is at 187–192 % of its two
cores on the top rows, so the ordering among the first four is the client's,
not the server's. Takeaway: on pipelined or batched traffic the I/O model is
irrelevant; the parser and the response builder are everything.

## 3. 10 000 stalled connections (slowloris) around 64 hot ones

![idle](../bench/charts/expand-idle-p99.svg)

Each idle socket sent half a request line and went silent for the whole run.

| runtime | req/s | p50 | p99 | p99.9 | RSS | threads | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| tokio | 189 k | 0.33 ms | **0.73 ms** | 1.5 ms | 254 MB | 3 | 0 |
| mio | 168 k | 0.39 ms | 0.90 ms | 2.0 ms | 232 MB | 3 | 0 |
| uring-async ◆ | 155 k | 0.38 ms | 1.95 ms | 7.1 ms | **160 MB** | 3 | 0 |
| tokio-st | 150 k | 0.40 ms | 0.94 ms | 2.2 ms | 352 MB | 1 | 0 |
| compio | 139 k | 0.45 ms | 1.04 ms | 2.3 ms | 191 MB | 3 | 0 |
| uring ◆ | 137 k | 0.41 ms | 4.52 ms | 14.1 ms | **133 MB** | 3 | 0 |
| tokio-uring | 135 k | 0.48 ms | 1.22 ms | 2.5 ms | 202 MB | 3 | 0 |
| monoio | 131 k | 0.47 ms | 1.21 ms | 2.6 ms | 265 MB | 3 | 0 |
| pool | 125 k | 0.32 ms | 1.00 ms | 2.3 ms | (not sampled) | | 47 |
| threads | 122 k | 0.52 ms | 2.17 ms | 10.9 ms | 479 MB | **9 967** | 0 |
| glommio | 110 k | 0.57 ms | 1.26 ms | 3.0 ms | 333 MB | 7 | 0 |

Three results. `threads` really does hold 9 967 threads and 479 MB for
10 000 idle clients, and still serves. `pool` is the one runtime that
*fails* the scenario: its 64 workers are all parked on stalled sockets, the
hot connections time out, and it records errors; a blocking pool is
slowloris-vulnerable by construction. And the two ringrt runtimes keep the
lowest memory (133–160 MB against 254 MB for Tokio) but pay in tail: p99
1.95 ms (`uring-async`) and 4.52 ms (`uring`) against 0.73 ms. The
provided-buffer ring is 256 × 4 KiB; §8 below tests whether that is the cause.

## 4. Mixed paths (/json, /compute?n=2000, / on every connection)

| runtime | req/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| compio | 8 369 | 8.95 ms | 23.5 ms | 36.9 ms |
| tokio-uring | 8 216 | 8.62 ms | 18.7 ms | 26.8 ms |
| mio | 8 168 | 7.64 ms | 18.1 ms | 22.6 ms |
| uring-async ◆ | 8 152 | 7.71 ms | 18.7 ms | 23.9 ms |
| uring ◆ | 8 132 | 7.55 ms | 18.5 ms | 22.2 ms |
| pool | 8 062 | 7.58 ms | 34.7 ms | 61.1 ms |
| glommio | 8 036 | 7.68 ms | 21.2 ms | 27.2 ms |
| tokio | 7 869 | 8.05 ms | **15.8 ms** | **19.3 ms** |
| monoio | 7 863 | 7.16 ms | 23.3 ms | 27.9 ms |
| threads | 7 844 | 7.69 ms | 39.0 ms | 77.1 ms |
| tokio-st | 3 755 | 17.2 ms | 39.5 ms | 43.9 ms |

The compute path dominates (≈ 250 µs CPU per request on every runtime), so
throughput is identical to within noise and the only signal is the tail: the
cheap requests queue behind the expensive one on the same core. Tokio's work
stealing gives it the shortest p99 and p99.9; the thread-per-core runtimes
are 15–20 % behind; the blocking runtimes are 2× behind.

## 5. A noisy co-tenant on one of the server's two cores

![contend](../bench/charts/expand-contend-p99.svg)

A busy loop pinned to core 1 for the whole run (a cron job, a log shipper, a
neighbour on a shared vCPU).

| runtime | req/s | p50 | p99 | p99.9 | server CPU % |
|---|---:|---:|---:|---:|---:|
| uring-async ◆ | **157 k** | 0.27 ms | 4.46 ms | 8.3 ms | 108 |
| mio | 151 k | 0.23 ms | 4.50 ms | 8.2 ms | 106 |
| uring ◆ | 151 k | 0.24 ms | 4.44 ms | 8.6 ms | 106 |
| tokio-st | 138 k | 0.44 ms | 1.12 ms | 2.6 ms | 98 |
| tokio | 137 k | 0.43 ms | **1.22 ms** | 4.4 ms | 104 |
| monoio | 132 k | 0.30 ms | 4.58 ms | 8.4 ms | 118 |
| tokio-uring | 131 k | 0.30 ms | 4.54 ms | 8.3 ms | 120 |
| pool | 128 k | 0.48 ms | 1.91 ms | 5.2 ms | (not sampled) |
| threads | 125 k | 0.46 ms | 2.25 ms | 5.5 ms | 162 |
| compio | 119 k | 0.32 ms | 4.59 ms | 8.5 ms | 117 |
| glommio | 93 k | 0.47 ms | 5.29 ms | 9.0 ms | 127 |

This is the clearest result of the round. Every thread-per-core runtime,
ours included, shows the same p99 of 4.4–4.6 ms: the worker that shares core
1 with the hog gets the scheduler's time slice and its connections wait for
it, and because connections are partitioned by `SO_REUSEPORT` nothing can
move them. Tokio's work-stealing scheduler takes the woken tasks to the free
core and keeps p99 at 1.2 ms. `tokio-st` wins by accident (one thread, which
the scheduler places on the free core). ringrt still has the highest
throughput and the lowest median under contention, but the tail belongs to
work stealing. On a 1-vCPU droplet there is no second core to steal to, so
this does not apply there; on a shared 2-vCPU box it does.

## 6. Long-window tails (20 s, 256 connections)

![tail](../bench/charts/expand-tail-p999.svg)

| runtime | req/s | p99 | p99.9 | p99.99 | note |
|---|---:|---:|---:|---:|---|
| tokio | 180 k | 2.83 ms | 4.31 ms | 11.3 ms | best p99 |
| uring-async ◆ | 166 k | 3.52 ms | 5.33 ms | **8.5 ms** | best p99.99 |
| uring ◆ | 160 k | 5.87 ms | 15.4 ms | 42.7 ms | synchronous send path |
| mio | 150 k | 3.84 ms | 5.85 ms | 12.2 ms | |
| compio | 136 k | 4.40 ms | 9.23 ms | 16.8 ms | |
| monoio | 135 k | 4.78 ms | 7.73 ms | 12.5 ms | |
| tokio-uring | 134 k | 4.40 ms | 7.94 ms | 66.4 ms | |
| pool | 128 k | 1.35 ms | 2.67 ms | 8.3 ms | 192 errors (64 workers < 256 conns) |
| threads | 120 k | 7.06 ms | 14.9 ms | 27.8 ms | |
| tokio-st | 109 k | 4.51 ms | 7.80 ms | 17.0 ms | |
| glommio | 108 k | 5.01 ms | 6.78 ms | 21.9 ms | |

Over three million samples per row. `uring-async` has the best p99.99 of all
eleven; the callback-style `uring` loop has a long tail (its send path is
synchronous per connection, so one slow socket holds the loop). `pool` with
256 connections and 64 workers times out the surplus, hence the errors.

## 7. Open-loop rate sweep: where the knee is

![rate](../bench/charts/rate-p99.svg)

256 connections, latency from the intended send time, p99 in ms per offered
rate; "✗" means the server did not reach the offered rate.

| runtime | 10 k | 25 k | 50 k | 75 k | 100 k | 125 k |
|---|---:|---:|---:|---:|---:|---:|
| tokio | 0.7 | 0.8 | 0.5 | 1.0 | 2.6 | 29.5 |
| uring-async ◆ | 0.8 | 0.5 | 0.7 | 2.7 | 3.6 | **9.2** |
| uring ◆ | 1.0 | 2.0 | 1.9 | 24.3 | 4.0 | 68.0 |
| mio | 2.1 | 103.7 | 1.5 | 1.8 | 13.5 | 12.7 |
| compio | 0.7 | 0.9 | 2.1 | 4.9 | 8.9 | 142 |
| tokio-uring | 1.4 | 0.7 | 1.3 | 6.2 | 43.4 | 191 |
| monoio | 1.1 | 1.0 | 1.1 | 136 | 95.0 | 279 |
| tokio-st | 0.9 | 0.6 | 1.6 | 2.0 | 95.2 | 349 |
| threads | 0.7 | 0.6 | 3.3 | 186 | 99.8 | 698 ✗ |
| glommio | 0.5 | 1.4 | 3.3 | 10.1 | 130 | 1871 ✗ |

Server CPU per request falls with rate on every runtime (Tokio 23 → 12 µs,
uring-async 21 → 10 µs) because completions batch. The knee on two cores:
`uring-async` is the only runtime still under 10 ms p99 at 125 k req/s;
Tokio holds the flattest curve to 100 k and bends at 125 k; the three
external io_uring runtimes and the blocking ones have fallen over by then
(p50 in the tens or hundreds of milliseconds, i.e. a queue that grows
without bound). The isolated spikes (mio at 25 k, uring at 75 k) are single
runs and look like the ±15 % box noise in its tail form; the sweep has no
repetitions.

## 8. Does the buffer ring explain the idle-connection tail?

The ringrt runtimes in §3 had the best memory and the worst p99. The one
thing that differs between them and the other io_uring runtimes under 10 000
stalled sockets is the provided-buffer ring, 256 × 4 KiB per worker by
default at the time. Same scenario, single runs, two knobs:

| runtime | buffer ring | req/s | p50 | p99 | p99.9 | RSS |
|---|---|---:|---:|---:|---:|---:|
| uring-async ◆ | 256 (default in §3) | 155 k | 0.38 ms | 1.95 ms | 7.1 ms | 160 MB |
| uring-async ◆ | **4096** | **170 k** | 0.34 ms | **0.92 ms** | 3.2 ms | 122 MB |
| uring-async ◆ | single-shot recv, no buffer ring | 146 k | 0.40 ms | 1.19 ms | 4.7 ms | 168 MB |
| uring ◆ | 256 (default in §3) | 137 k | 0.41 ms | 4.52 ms | 14.1 ms | 133 MB |
| uring ◆ | **4096** | **168 k** | 0.36 ms | **0.82 ms** | 1.9 ms | 87 MB |
| uring ◆ | single-shot recv, no buffer ring | 156 k | 0.39 ms | 0.93 ms | 2.6 ms | 87 MB |

It was the ring. With 4096 entries both runtimes beat Tokio's 0.73 ms p99
closely (0.82 / 0.92 ms) at lower memory than before, and throughput rises
by a tenth to a quarter. The mechanism: 10 000 connections each delivered
half a request on connect; a 256-entry ring is exhausted by that burst, every
multishot recv that finds no buffer completes with `ENOBUFS` and has to be
re-armed, and the re-arm work lands in the same loop that serves the hot
connections. Plain single-shot recv (no buffer ring at all) is also better
than the small ring, which says the cost was the re-arm storm, not the
buffer selection itself.

The earlier buffer-ring study (`bench/results/multishot-reps.txt`) varied the
ring at 1 024 and 16 000 *active* connections and found nothing, because
active connections recycle buffers as fast as they take them. Stalled
connections do not. The default is now 4096 entries (16 MiB per worker).
A confirmation run of the same scenario on the new default, with no
environment override (`bench/results/idle-default-4096.jsonl`):
`uring-async` 175 k req/s, p50 0.34 ms, p99 0.83 ms, RSS 121 MB;
`uring` 153 k req/s, p99 1.07 ms, RSS 87 MB. This is the first measurable
engineering change to come out of the expanded shapes, and it is a one-line
change in a file we own.

## 9. What we did differently from the existing tools

The honest framing first: `ringrt` does not win every column, and the matrix
in the README says so. Tokio still has the best CPU per request at saturation
and a shorter p99 on several endpoints. What this project did differently is
less about one runtime and more about *how the comparison was made* and *which
design decisions were taken from scratch rather than inherited*.

### 9.1 In the measurement

Published runtime benchmarks (TechEmpower, the READMEs of `monoio`, `glommio`,
`compio`, `may-minihttp`) share four weaknesses. Each of them is addressed here.

| Weakness in the usual comparison | What this harness does instead |
|---|---|
| Each runtime runs *its own* HTTP server, so parser, response building and allocation differ as much as the I/O model does. | One sans-IO engine (`phhtp-core`) serves every runtime. A row differs from another only in how bytes reach the socket. |
| Only req/s and client-side latency are reported; the server's cost is invisible. | Every row carries the server's own CPU (user + system), µs CPU per request, RSS and peak RSS, context switches per request, page faults and disk bytes, read from `/stats` at the edges of the window, plus the load generator's CPU so a client-bound row is visible as such. |
| Closed-loop clients hide stalls (coordinated omission): a server that freezes for 50 ms just receives fewer requests. | Three load shapes per endpoint: closed loop, connection-per-request churn, and open loop at a fixed offered rate with latency measured from the *intended* send time. This round adds pipelining, stalled connections, mixed paths, a noisy co-tenant and a rate sweep. |
| One run, one number. | `--reps` medians with interleaved repetitions, the req/s range stored with the row, and provenance per row (`somaxconn`, worker count, kernel flags), after a run where a container restart silently reset the accept backlog and produced a "collapse" that was not the server's. |

Two further things no runtime README reports: the number of system calls per
request (§1, from `strace -c`), and how the server behaves at *failure*, not
only at its best point (file-descriptor exhaustion, thread-spawn failure,
accept-backlog overflow, `EMFILE` in the accept loop; see SCALE.md §4–5).
Three of those were real bugs in our own runtimes that the stress tests found
and that the ordinary benchmark would never have shown.

### 9.2 In the runtime

`ringrt` is ~1 600 lines with one dependency (`libc`). The table in
LANDSCAPE.md §2 lists features; this is what is *different in kind*:

1. **The ring is ours, end to end.** `io_uring_setup`, `io_uring_enter`,
   `io_uring_register`, the two `mmap`s, the SQ/CQ head–tail protocol with the
   right acquire/release fences, the provided-buffer ring's tail at byte offset
   14, and every SQE layout are transcribed from the kernel ABI in `sys.rs`
   with size assertions. Every other runtime in the comparison goes through
   the `io-uring` crate. That is not a performance claim (the crate is thin);
   it is a *knowledge* claim: there is no layer in this runtime whose behaviour
   we cannot read, and when multishot recv silently failed with `EINVAL`
   because one flag bit was wrong (`IOSQE_BUFFER_SELECT` is `1 << 5`, not
   `1 << 4`) the fix was one line in a file we own.

2. **Multishot everything, with fallback, from the first commit.** One
   `accept` SQE per listener and one `recv` SQE per connection stay armed for
   the socket's lifetime; the kernel picks a buffer from a ring we registered
   and posts a completion per message. The measured effect is in §1: 0.17–0.18
   `io_uring_enter` calls per request, against 2.0+ syscalls per request for
   every epoll runtime, and against `glommio`'s 2.24, because glommio tries a
   non-blocking `recvfrom`/`sendto` first and only falls into the ring on
   `EAGAIN`. `monoio` and `compio` reach the same 0.17 only on their newest
   versions; `tokio-uring` cannot (it polls the ring through epoll).

3. **A borrowed-buffer API on a completion runtime.** Owned buffers are the
   price every io_uring runtime charges (`monoio`'s `IoBuf`, `compio`'s
   `IoBuf`, `tokio-uring`'s `BufResult`): the caller gives the buffer away and
   gets it back with the result, which does not compose with any code written
   against `AsyncRead`. `ringrt::TcpStream::recv(&mut Vec<u8>)` looks like
   Tokio's and is still sound, because the kernel never writes into the
   caller's buffer: it writes into the provided-buffer ring, and the runtime
   either copies into the caller's `Vec` or, when that `Vec` is empty, *swaps*
   it with the ring buffer and recycles the caller's allocation. Sends take
   an owned `Vec` (unavoidable for soundness) but `send_detached` keeps up to
   eight in flight per connection from a pool and applies backpressure beyond
   that, so a handler never waits for the previous response to be acknowledged
   before building the next one.

4. **Teardown is correct, not just fast.** Dropping a `Recv` or `Accept`
   future that still has an SQE in the kernel marks its slot *orphaned*,
   submits `ASYNC_CANCEL`, and keeps the slot and its buffer alive until the
   completion lands. `Runtime::drop` cancels, reaps and only then drops the
   tasks' wakers. The in-process tests (`crates/ringrt/tests`) exercise this
   path. Cancellation safety is where two of the historical attempts
   (`ringbahn`, `iou`) ran out of road.

5. **Accept-path hardening that the stress tests forced.** Listen backlog
   65 535 everywhere, a deploy `sysctl` for `somaxconn`, `EMFILE` on accept
   retried through a kernel `TIMEOUT` op instead of sleeping the worker
   thread (23 k → 159 k req/s under fd pressure), and load shed instead of
   process exit when a thread cannot be spawned. None of the external
   runtimes' examples do these; most production deployments learn them in an
   outage.

### 9.3 What is *not* different, said plainly

* Thread-per-core with no work stealing: the same choice as `monoio`,
  `glommio` and `compio`, with the same consequence that one slow handler
  blocks every connection on its core. `/compute?n=2000` shows it.
* Per-operation kernel cost is io_uring's, not ours: at moderate open-loop
  load Tokio spends 13 µs CPU per request to our 21–24, because an
  `io_uring_enter` that submits one SQE and waits is dearer than one
  `epoll_wait` that returns a batch. We win the syscall *count*; the kernel
  decides the syscall *price*.
* No registered files, no registered send buffers, no zero-copy send, no
  SQPOLL: the four remaining knobs, listed in LANDSCAPE.md.

The one-line answer: the difference is not that `ringrt` is faster than
Tokio on every row (it is not), it is that for the first time the same
handler has been measured on eleven runtimes under the same load shapes with
the server's own resources in every row, and that the io_uring runtime in
that comparison is one whose every line we can read and change.
