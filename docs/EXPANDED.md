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

RESULTS_PLACEHOLDER

## 7. What we did differently from the existing tools

The honest framing first: `ringrt` does not win every column, and the matrix
in the README says so. Tokio still has the best CPU per request at saturation
and a shorter p99 on several endpoints. What this project did differently is
less about one runtime and more about *how the comparison was made* and *which
design decisions were taken from scratch rather than inherited*.

### 7.1 In the measurement

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

### 7.2 In the runtime

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

### 7.3 What is *not* different, said plainly

* Thread-per-core with no work stealing: the same choice as `monoio`,
  `glommio` and `compio`, with the same consequence that one slow handler
  blocks every connection on its core. `/compute?n=2000` shows it.
* Per-operation kernel cost is io_uring's, not ours: at moderate open-loop
  load Tokio spends 13 µs CPU per request to our 21–24, because an
  `io_uring_enter` that submits one SQE and waits is dearer than one
  `epoll_wait` that returns a batch. We win the syscall *count*; the kernel
  decides the syscall *price*.
* No registered files, no registered send buffers, no zero-copy send, no
  SQPOLL: the four remaining knobs, listed in §6 and in LANDSCAPE.md.

The one-line answer: the difference is not that `ringrt` is faster than
Tokio on every row (it is not), it is that for the first time the same
handler has been measured on eleven runtimes under the same load shapes with
the server's own resources in every row, and that the io_uring runtime in
that comparison is one whose every line we can read and change.
