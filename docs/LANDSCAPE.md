# Landscape and trade-offs: `ringrt` versus the existing io_uring runtimes

This is the study behind the question "should we publish our own runtime, and
what are we trading away compared to what already exists?" Written October 2026.
Versions and statuses come from `cargo search` on that date and from the sources
listed at the end; verify before relying on them.

## 1. What already exists

| Crate | Latest | Model | Platform | Status (Oct 2026) | Notes |
|---|---|---|---|---|---|
| `io-uring` (tokio-rs) | 0.7.15 | low-level bindings, no runtime | Linux | actively maintained | What most runtimes build on. Safe wrappers over SQE/CQE, no scheduler. |
| `tokio-uring` | 0.5.0 | tokio current-thread + io_uring driver | Linux ≥ 5.11 | stalled: no changelog since 2022, unfinished release, old open issues | Polls the ring fd through epoll, so it adds an indirection layer. Owned-buffer API. |
| `monoio` (ByteDance) | 0.2.4 (Aug 2024) | thread-per-core, io_uring with epoll/kqueue fallback | Linux ≥ 5.6, macOS | last release 2024, unanswered maintenance questions | Production-proven inside ByteDance. Owned-buffer `IoBuf` traits. |
| `glommio` (Datadog) | 0.9.0 | thread-per-core, io_uring only | Linux ≥ 5.8 | maintained slowly | Needs ≥ 512 KiB locked memory. Rich: file I/O, DMA, timers, channels. |
| `compio` | 0.20.0-beta.1 | thread-per-core, completion-based | Linux (io_uring), Windows (IOCP), macOS/BSD (polling) | active, releases every 1–3 months, API breaks each minor | Apache Iggy migrated to it from Tokio. Owned-buffer API, `IoBuf` traits. |
| `ringline` | 0.5.0 | thread-per-core, io_uring (mio fallback) | Linux ≥ 6.0 | new in 2026 | Uses multishot recv, ring-provided buffers, zero-copy send, fixed files. |
| `ringcore` | 0.1.0 | single-threaded "white-box" io_uring runtime | Linux | educational, new | Closest in spirit to `ringrt`: an executor plus SQE mapping, built to show how futures map to kernel I/O. |
| `rio` (sled) | 0.6.x | completion futures, no scheduler | Linux | dormant | GPL-3. Backpressure-aware CQ design. |
| `iou` / `ringbahn` | – | bindings + proto-runtime | Linux | abandoned (2020–21) | Historically important: first serious attempt at a safe io_uring runtime in Rust. |

Two facts frame everything below:

* **Tokio is the ecosystem.** It reports around 150 million downloads a month and
  some 20 000 dependent crates. Every HTTP, database and RPC library of note
  assumes `tokio::io::AsyncRead`/`AsyncWrite` and a `Send` work-stealing scheduler.
* **Every io_uring runtime that survived is thread-per-core with owned buffers.**
  `monoio`, `glommio`, `compio`, `tokio-uring`, `ringline` and `ringrt` all
  converged on the same two decisions, for the same reason: a completion API
  gives the kernel ownership of the buffer until the completion lands, and a
  task that may migrate threads mid-operation makes that ownership unexpressible.

## 2. Where `ringrt` sits

`ringrt` is ~1 600 lines, one dependency (`libc`), and makes the same two
architectural choices as the runtimes above. What distinguishes it:

| | `ringrt` | `monoio` / `compio` | `glommio` | `tokio-uring` |
|---|---|---|---|---|
| Dependencies | `libc` | `io-uring` + ~15 crates | `io-uring` + many | `tokio` + `io-uring` |
| Ring access | own ABI transcription, own mmap, own head/tail protocol | `io-uring` crate | `io-uring` crate | `io-uring` crate |
| Scheduler | `Rc` waker, `VecDeque`, no atomics | similar, more features | similar, plus task queues with priorities/shares | tokio current-thread |
| Blocking on ring | `io_uring_enter(GETEVENTS)` | same | same | epoll on the ring fd |
| Multishot accept | yes (both runtimes, single-shot fallback) | yes | yes | no |
| Multishot recv / provided buffers | yes (6.0+, single-shot fallback); `recv(&mut Vec)` borrows like Tokio | compio: partial | yes | no |
| Fixed files / registered buffers | no | yes | yes | yes |
| Timers | one kernel `TIMEOUT` op per sleep | timer wheel (monoio) / kernel | timer wheel | tokio's wheel |
| File I/O | no | yes | yes (DMA) | yes |
| Spawn across threads | no | no | no (explicit channels) | no |
| `JoinHandle` | no | yes | yes | yes |
| Cancel on drop | `ASYNC_CANCEL` + orphaned buffer | same idea | same idea | same idea |
| Fallback when io_uring is blocked | none (`Runtime::available()`) | epoll/kqueue | none | none |
| Ecosystem (`AsyncRead` compat) | none | adapters, partial | partial | tokio compat for non-io_uring parts |

In short: `ringrt` is the smallest possible thing that is still a real
`async`/`await` runtime on io_uring. Everything in the rows marked "no" is a
feature the others have and we do not.

## 3. The trade-offs, concretely

These are what you give up or take on by choosing `ringrt` over Tokio, measured
on this repository's own benchmark (see the README) and on the sources below.

### 3.1 What you gain

* **Latency where the server waits.** On `/sleep?ms=5` with 256 connections,
  `uring-async` beat `tokio` on throughput (46k vs 38k req/s) and on p50 and p90
  (5.4 vs 6.7 ms, 6.2 vs 7.5 ms). One `io_uring_enter` per scheduler turn
  submits every op and timer; Tokio needs `epoll_wait` plus a timer wheel plus a
  `read`/`write` syscall per op.
* **Better p50 on hot paths.** On `/json`, 0.34 ms versus 0.41 ms at equal
  throughput. Fewer syscalls, no `Send` bound, no atomics in the scheduler.
* **No `Send`/`Sync` tax.** Tasks are `!Send`, so per-connection state can be
  `Rc<RefCell<_>>` and thread-locals are safe across `.await`. Apache Iggy
  reports this as the biggest ergonomic win *and* the biggest source of bugs
  (RefCell panics across await points) in their move off Tokio.
* **Control.** You can read every line. There is no scheduler heuristic you
  cannot change, which is the reason Iggy gave for leaving Tokio.
* **No dependency surface.** `libc` only. Relevant if you audit dependencies.

### 3.2 What you give up

* **The tail.** Tokio's work stealing produced the best p99 on `/json`
  (1.1 ms vs 1.7 ms). With `SO_REUSEPORT` the kernel balances *connections*,
  not *load*; one worker with two heavy connections cannot hand one off. For a
  latency SLO defined at p99, this matters more than the p50 win.
* **CPU-bound handlers.** There is no blocking pool. A handler that computes
  for 10 ms stalls every connection on that worker. Tokio has `spawn_blocking`;
  a thread-per-core runtime needs you to shard the work or add a pool yourself.
* **The ecosystem.** Nothing built on `tokio::io` runs on `ringrt`: no hyper,
  reqwest, sqlx, tonic, rustls adapters. You write the protocol layer (this
  repository's `phhtp-core` is exactly that) or write adapters. This is the
  cost everyone who leaves Tokio reports, including Iggy and the monoio docs.
* **Deployability.** io_uring is blocked by Docker's default seccomp profile,
  GKE Autopilot, ChromeOS, Android apps, and Google's own servers, after 60% of
  2022 kernel exploits in Google's bug bounty targeted it. A $12 droplet is
  fine; a managed Kubernetes cluster may need a seccomp override or an epoll
  fallback, which `ringrt` does not have and `monoio`/`compio` do.
* **Kernel floor.** Linux ≥ 5.6; multishot accept needs 5.19; the fast setup
  flags we try first need 6.0. Ubuntu 24.04's 6.8 kernel covers all of it.
* **The missing features.** No file I/O, no `JoinHandle`, no `select!`, no
  cross-thread channels, no registered files or zero-copy send (what is
  left of `ringline`'s feature list), no graceful shutdown, no metrics.
* **Soundness surface is ours.** The runtime has ~15 `unsafe` blocks (ring
  memory, the `Rc` waker, `set_len` after recv). They are small and commented,
  and the owned-buffer design removes the classic use-after-free, but nobody
  else has reviewed them.
* **Maintenance.** `tokio-uring` stalled, `monoio` has gone quiet, `rio` and
  `ringbahn` died. A niche runtime is only as alive as its one or two
  maintainers.

### 3.3 What the numbers do *not* say yet

* The benchmark ran the load generator on the same 4-core box. The syscall
  savings io_uring brings are muted when the server is CPU-starved; the
  `/sleep` advantage should grow on the droplet with real network RTT.
* The $12 droplet has 1 vCPU. Every runtime is single-worker there; the
  comparison becomes "how cheaply does each multiplex I/O around CPU work."
  `WORKERS=1 scripts/bench.sh` previews that.

## 4. Recommendation

* **Publish `ringrt`.** It is coherent, small, documented and tested, and the
  owned-buffer thread-per-core design is the one the field converged on. The
  crate name is free. Its honest positioning is *a reference runtime for
  thread-per-core io_uring servers you fully control*, not a Tokio replacement
  for ecosystem code.
* **Do not replace Tokio in anything that needs hyper, reqwest or sqlx.** That
  decision is about the ecosystem, not performance; `compio` is the active
  project to watch if you want io_uring *and* adapters.
* **For this server on the droplet, the realistic choices are `tokio`
  (best p99, blocking pool, ecosystem) or `uring-async` (best p50 and best
  behaviour under I/O waits).** Decide with the droplet numbers, not these.
* **If `ringrt` goes forward,** the next features in value order are:
  `JoinHandle`, a small blocking pool, registered files (`IOSQE_FIXED_FILE`),
  then an epoll fallback for environments that block io_uring. Multishot
  accept and multishot recv with provided buffers are done; see the README
  for what they bought.

## 5. Naming

`cargo search` on 2026-10-16: free — `phhtp`, `phhtp-core`, `phttp`, `ringrt`,
`uringrt`, `uringo`, `kuring`, `iour`, `corering`, `ringflow`, `ringbase`,
`ourring`; taken — `ringline`, `ringcore`, `ringo`, `urt`, `ringlet`. We chose
`ringrt` ("ring runtime") for the runtime and kept `phhtp-core` for the HTTP
engine. crates.io convention discourages a `rust-` prefix, so `rust-phttp` was
not considered.

## Sources

* [Choose the async runtime: monoio vs compio vs tokio (ruxen issue #272)](https://github.com/gvozdetsky/ruxen/issues/272)
* [Apache Iggy's migration journey to thread-per-core architecture powered by io_uring](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
* [Comparing monoio with Tokio and Glommio](https://zread.ai/bytedance/monoio/30-comparing-with-tokio-and-glommio)
* [monoio on docs.rs](https://docs.rs/monoio)
* [monoio benchmark](https://github.com/ukernel/monoio/blob/master/docs/en/benchmark.md)
* [Status of tokio_uring? (users.rust-lang.org)](https://users.rust-lang.org/t/status-of-tokio-uring/114481)
* [tokio-uring on docs.rs](https://docs.rs/crate/tokio-uring/^0.4.0)
* [glommio 0.8 on docs.rs](https://docs.rs/crate/glommio/^0.8.0)
* [compio on docs.rs](https://docs.rs/crate/compio/latest)
* [ringline on docs.rs](https://docs.rs/ringline)
* [ringcore on docs.rs](https://docs.rs/ringcore)
* [rio on docs.rs](https://docs.rs/crate/rio/0.6.2)
* [iou / ringbahn on docs.rs](https://docs.rs/crate/iou/^0.0.0-ringbahn)
* [Google Limiting IO_uring Use Due To Security Vulnerabilities (Phoronix)](https://www.phoronix.com/news/Google-Restricting-IO_uring)
* [io_uring is blocked by the default seccomp profile (moby #47532)](https://github.com/moby/moby/issues/47532)
* [io_uring (Wikipedia)](https://en.wikipedia.org/wiki/Io_uring)
* [Thread-Per-Core Async in Rust (Chrysostomos Nanakos)](https://www.include.gr/writing/rust-thread-per-core-async.html)
* [Rust Async Runtimes: Tokio vs async-std vs smol (2026)](https://lucaberton.com/blog/rust-async-runtimes-tokio-2026/)
