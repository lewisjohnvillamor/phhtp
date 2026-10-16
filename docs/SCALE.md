# Scale: how far each runtime goes, and what breaks first

Measured October 2026 on a 4-core, 16 GB cloud container, Linux 6.18, with
`scripts/scale.sh` and `scripts/stress.sh`. The server was pinned to cores 0–1
and the load generator to cores 2–3, so every number below is a **2-core
server** with the client on separate cores but the same kernel. "Powerful
setup" numbers are extrapolated in §6; they are not measured here. Raw tables:
`bench/results/scale.txt`, `bench/results/stress.txt`,
`bench/results/scale-herd-unbounded.txt`.

Box limits that matter, and where they come from:

| Limit | Value here | Set by | What hits it |
|---|---:|---|---|
| open files per process | 20 000 | `ulimit -n` (hard) | one fd per connection, server *and* client |
| threads system-wide | ≈32 000 | `pid_max` 32768, `vm.max_map_count` 65530 (2 maps per thread stack) | `threads` runtime |
| accept backlog | 4 096 default; 65 535 for these runs | `net.core.somaxconn`, `listen()` | connect herds |
| ephemeral ports | 28 232 | `ip_local_port_range` | one client IP → one server port |
| RAM | 16 GB | – | per-connection buffers, thread stacks |

The $12 droplet has 1 vCPU and 2 GB, and Ubuntu's default `ulimit -n` is 1024
(the systemd unit in `deploy/` raises it to 65 536).

## 1. The first thing that broke was not the server

The first sweep opened all 16 000 connections at once against the default
4 096-deep accept queue. Every runtime, including io_uring, collapsed to the
same ~3 000 req/s with a 3–5 s p90 (`bench/results/scale-herd-unbounded.txt`).
The server was idle; the kernel was dropping handshakes. With
`tcp_abort_on_overflow = 0` a full accept queue drops the client's final ACK
while the client already believes it is connected, so the first request sits
in TCP retransmit back-off (200 ms, 400 ms, 800 ms, …) for seconds.

Two independent fixes each restored ~97 000 req/s on the same 16 000
connections: pacing the client to 256 connection attempts at a time, or
raising `somaxconn` to 65 535. The listen-overflow counters in
`/proc/net/netstat` did not move, so the drop happens earlier in the SYN path
than the classic overflow counter; the cure is the same either way.

Consequences, now applied:

* every runtime asks for a 65 535 backlog (std's `TcpListener::bind` asks
  for 128, Tokio's for 1 024; both are far too small for a reconnect storm);
* `deploy/sysctl-phhtp.conf` sets `somaxconn` and `tcp_max_syn_backlog` to
  65 535;
* `phhtp-bench` paces connects (`--connect-concurrency 256`; `0` for the burst).

**Production reading:** after a restart with N keep-alive clients reconnecting
at once, a default Ubuntu box stalls for several seconds once N is a few
thousand, whatever the runtime. This is the most likely "crash" a $12 droplet
will show, and it is a sysctl, not code.

## 2. Connection sweep

2-core server, `GET /`, keep-alive, one request in flight per connection,
connects paced, `somaxconn` 65 535. RSS and thread count sampled mid-run.

| runtime | conns | req/s | p50 | p90 | p99 | max | RSS MB | threads |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| threads | 64 | 125k | 0.47ms | 0.75ms | 1.51ms | 19ms | 6 | 65 |
| threads | 1 024 | 108k | 6.91ms | 17.6ms | 55.7ms | 829ms | 38 | 1 025 |
| threads | 4 096 | 102k | 26.8ms | 52.4ms | 148ms | 2 623ms | 113 | 3 260 |
| threads | 16 000 | 66k | 7.05ms | 475ms | 4 616ms | 4 997ms | 219 | 6 458 |
| pool(64) | 64 | 126k | 0.46ms | 0.82ms | 1.20ms | 16ms | 6 | 65 |
| pool(64) | 1 024 | 129k | 0.43ms | 0.77ms | 1.80ms | 1 996ms | 6 | 65 |
| pool(64) | 4 096 | 113k | 0.51ms | 0.87ms | 4.78ms | 2 021ms | 6 | 65 |
| pool(64) | 16 000 | 60k | 0.56ms | 1.00ms | 1 359ms | 2 253ms | 6 | 65 |
| mio | 64 | 144k | 0.42ms | 0.68ms | 0.97ms | 7.5ms | 4 | 3 |
| mio | 1 024 | 125k | 3.29ms | 26.0ms | 48.6ms | 66ms | 16 | 3 |
| mio | 4 096 | 123k | 3.40ms | 150ms | 257ms | 314ms | 47 | 3 |
| mio | 16 000 | 80k | 4.93ms | 700ms | 1 105ms | 1 149ms | 93 | 3 |
| tokio | 64 | 184k | 0.32ms | 0.51ms | 0.77ms | 5.1ms | 4 | 3 |
| tokio | 1 024 | 163k | 2.32ms | 23.9ms | 45.0ms | 72ms | 8 | 3 |
| tokio | 4 096 | 182k | 2.27ms | 106ms | 167ms | 193ms | 58 | 3 |
| tokio | 16 000 | 116k | 3.60ms | 572ms | 1 038ms | 1 068ms | 183 | 3 |
| tokio-st | 64 | 148k | 0.41ms | 0.57ms | 1.19ms | 8.6ms | 5 | 1 |
| tokio-st | 1 024 | 114k | 8.49ms | 11.2ms | 16.2ms | 18ms | 24 | 1 |
| tokio-st | 4 096 | 87k | 43.6ms | 57.7ms | 61.0ms | 63ms | 84 | 1 |
| tokio-st | 16 000 | 56k | 205ms | 295ms | 1 208ms | 1 276ms | 132 | 1 |
| uring | 64 | 161k | 0.38ms | 0.59ms | 0.89ms | 4.8ms | 5 | 3 |
| uring | 1 024 | 159k | 2.35ms | 23.2ms | 44.3ms | 61ms | 12 | 3 |
| uring | 4 096 | 142k | 2.95ms | 134ms | 198ms | 230ms | 37 | 3 |
| uring | 16 000 | 98k | 4.28ms | 646ms | 1 165ms | 1 204ms | 114 | 3 |
| uring-async | 64 | 166k | 0.36ms | 0.58ms | 0.88ms | 6.2ms | 5 | 3 |
| uring-async | 1 024 | 155k | 2.43ms | 24.1ms | 47.1ms | 69ms | 13 | 3 |
| uring-async | 4 096 | 143k | 2.84ms | 134ms | 202ms | 243ms | 38 | 3 |
| uring-async | 16 000 | 93k | 4.59ms | 712ms | 1 207ms | 1 232ms | 117 | 3 |

Reading it:

* **Throughput is flat to ~4 096 connections for every event-driven runtime**
  (within the ±10 % run-to-run noise of a 4-second window) and drops 30–40 %
  at 16 000. Latency follows Little's law: 16 000 requests in flight at
  ~100k req/s is a 160 ms service round, and that is where p90 goes. More
  connections than `req/s × target latency` is a capacity problem, not a
  runtime defect.
* **The p50/p90 split at 4 096+ connections is the same for every runtime**,
  including Tokio with a single shared listener, so it is not `SO_REUSEPORT`
  imbalance. It is the kernel and the 2-core client sharing the box: at
  16 000 sockets the measurement includes the load generator. A real number
  for the tail at this scale needs a second machine.
* **`threads` degrades from 1 024 connections on.** 1 025 OS threads on
  2 cores is already 7 ms p50 (time slicing); 3 260 threads is 27 ms p50
  with a 2.6 s max; at 16 000 the kernel had only spawned 6 458 threads by
  mid-run and the tail is seconds. The model is fine to a few hundred
  connections and wrong beyond. Memory: ~34 KB per connection, mostly
  stack that is touched even when idle.
* **`pool` serves exactly 64 connections.** Every other connection waits in the
  accept queue forever (keep-alive never frees a worker); the bench reports
  them as timeouts after warm-up. Its latency looks perfect because only the
  lucky 64 are measured. This is the silent failure: no error in the server
  log, no crash, clients that never get a response.
* **Memory per idle connection** (RSS at 16 000 ÷ 16 000): `mio` 5.8 KB,
  `uring` 7.1 KB, `uring-async` 7.3 KB, `tokio-st` 8.3 KB, `tokio` 11.5 KB.
  Before this session's fix the readiness-based runtimes grew the input buffer
  by 16 KB of zeroes on every read, which made Tokio 290 MB at 16 000
  connections; reading through a scratch buffer and copying what arrived
  halved it. At the droplet's 2 GB the event-driven runtimes could hold on
  the order of 150 000 idle keep-alive connections before the OOM killer;
  the 65 536 fd limit in the unit file is reached first, and 1 vCPU could not
  serve them anyway.

## 3. Worker sweep

2-core server, 1 024 connections. `workers = 1` is the droplet case.

| runtime | workers | req/s | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| mio | 1 | 110k | 9.09ms | 12.3ms | 17.3ms | 56ms |
| mio | 2 | 139k | 2.72ms | 25.0ms | 45.7ms | 65ms |
| mio | 4 | 134k | 2.81ms | 27.1ms | 49.4ms | 73ms |
| tokio | 1 | 141k | 1.55ms | 2.49ms | **150ms** | 173ms |
| tokio | 2 | 165k | 2.29ms | 22.9ms | 45.1ms | 70ms |
| tokio | 4 | 151k | 3.13ms | 21.4ms | 38.4ms | 53ms |
| tokio-st | 1 | 97k | 10.1ms | 13.8ms | 17.1ms | 20ms |
| uring | 1 | 125k | 7.88ms | 10.5ms | 14.6ms | 54ms |
| uring | 2 | 144k | 2.61ms | 27.3ms | 46.7ms | 69ms |
| uring | 4 | 163k | 2.25ms | 22.4ms | 43.9ms | 73ms |
| uring-async | 1 | 112k | 8.73ms | 12.6ms | 22.5ms | 69ms |
| uring-async | 2 | 156k | 2.35ms | 24.8ms | 45.4ms | 64ms |
| uring-async | 4 | 167k | 2.28ms | 21.8ms | 41.6ms | 63ms |

(`pool` with 1, 2, 4 workers serves 1, 2, 4 connections and is omitted;
`tokio-st` ignores `--workers`.)

Reading it:

* **One worker, 1 024 connections: Tokio has the highest throughput and the
  worst tail.** 141k req/s with p50 1.5 ms but p99 150 ms: its multi-thread
  scheduler with one worker starves some connections (the LIFO slot and the
  global queue favour recently woken tasks). `uring` gives 125k at p99 15 ms,
  `uring-async` 112k at p99 23 ms, `tokio-st` 97k at p99 17 ms. For a 1-vCPU
  droplet with a p99 target, the thread-per-core loops or `tokio-st` are the
  safer choices; multi-thread Tokio with one worker is not.
* **Two workers on two cores** gains 25–40 % for every runtime. Four workers
  on two cores (oversubscribed) is within noise of two: the io_uring loops
  gained a little this run and lost a little in the previous one; Tokio lost
  a little. No runtime is hurt badly by oversubscription at this scale.
* **Single-core efficiency order:** tokio ≈ uring > uring-async ≈ mio >
  tokio-st, with the caveat about Tokio's tail above. The ~10 % between
  `uring` and `uring-async` is the price of `async`/`await` (boxed future,
  waker clone, `RefCell` borrow per poll).

## 4. Stress: what breaks, how, and does it come back

Server limited with `ulimit`/`prlimit`, hit with twice what it can hold, then
probed with 16 connections to confirm it still serves. `errors` counts
requests that timed out after warm-up; connections that never got accepted
are reported differently by the client depending on when they time out, so
compare req/s and p99, not the error column.

| runtime | fd exhaustion (512 fds, 1 024 clients) | recovery | thread exhaustion (600 threads, 1 200 clients) | CPU overload (`/compute?n=20000`, 256 conns) | after overload |
|---|---|---|---|---|---|
| threads | 93k, p99 15ms, alive | 83k | 80k, p99 35ms, **alive** (used to exit) | 554 req/s, p50 447ms, p99 759ms | 80k |
| pool | 105k, p99 1.9ms, alive | 83k | – | 594 req/s, p50 107ms, p99 178ms (64 served) | 81k |
| mio | 169k, p99 8.9ms, alive | 50k | – | 596 req/s, p50 441ms, p99 562ms | 91k |
| tokio | 175k, p99 8.5ms, alive | 117k | – | 578 req/s, p50 427ms, p99 **879ms** | 104k |
| tokio-st | 114k, p99 9.4ms, alive | 112k | – | 136 req/s, p50 1 843ms (one core) | 119k |
| uring | 159k, p99 9.7ms, alive (23k before the fix) | 90k | – | 548 req/s, p50 472ms, p99 622ms | 82k |
| uring-async | 148k, p99 11ms, alive | 86k | – | 571 req/s, p50 453ms, p99 520ms | 84k |

What the experiments did and what the outcome means:

* **fd exhaustion.** Expected and observed: `accept` fails with `EMFILE` for
  the clients that do not fit; the ones that fit are served normally; the
  process stays up; when the load goes away a fresh probe is served. The
  callback `uring` loop originally slept its whole thread for 10 ms per
  rejected accept, which stalled every live connection on that worker and
  showed as 23k req/s at p50 22 ms. It now backs off with a kernel `TIMEOUT`
  op and matches the others. The blocking runtimes sleep only their accept
  thread, which serves nobody, so they were unaffected; `uring-async` sleeps
  asynchronously; `mio` relies on the next edge.
* **thread exhaustion** (`threads` only). Before this session, a failed spawn
  propagated out of the accept loop and **exited the process**: the one true
  crash in the code base. It now logs, drops that connection, and keeps
  serving; the probe afterwards gets full speed.
* **CPU overload.** `n=20000` is ~3.4 ms of SHA-256 per request; 256
  connections keep both cores saturated. Nothing crashes; latency is the
  queue depth. Note the tails: Tokio's work stealing gave the best p50
  (427 ms) and the worst p99 (879 ms) under CPU saturation, while the
  thread-per-core loops with `SO_REUSEPORT` gave an even split (p99 520–620
  ms). `tokio-st` on one core took 1.8 s per request. A real server needs a
  blocking pool or a bounded compute endpoint; the handler already clamps `n`.

## 4b. After multishot recv (same box, after a restart)

The sweeps above were taken before `ringrt` gained multishot accept/recv with
provided buffers. The container restarted before the re-measurement (which
also reset `somaxconn` to 4 096; it is back to 65 535 for the numbers below)
and the box became noticeably noisier: identical runs now differ by ±15 %.
So the comparison that counts is interleaved and repeated (three runs per
configuration, medians; `bench/results/multishot-reps.txt`; `phhtp-bench
--reps 3` does this for you):

| runtime | conns | req/s median (range) | p50 | p99 |
|---|---:|---:|---:|---:|
| tokio | 1 024 | 148k (136–169k) | 2.80ms | 45.3ms |
| uring-async (256 bufs) | 1 024 | 130k (129–158k) | 2.84ms | 46.6ms |
| uring-async (1024 bufs) | 1 024 | 135k (123–164k) | 2.92ms | 49.5ms |
| uring-async (2048 bufs) | 1 024 | 135k (122–142k) | 2.84ms | 49.1ms |
| tokio | 16 000 | **48k (41–62k)** | 5.44ms | 1 117ms |
| uring-async (256 bufs) | 16 000 | **92k (89–93k)** | 4.45ms | 1 167ms |
| uring-async (1024 bufs) | 16 000 | 85k (77–94k) | 4.68ms | 1 297ms |
| uring-async (2048 bufs) | 16 000 | 95k (77–104k) | 4.05ms | 1 095ms |

* At 1 024 connections Tokio and `ringrt` overlap; Tokio's median is 10 %
  higher. At 16 000, `ringrt` is about twice Tokio's throughput and far more
  stable run to run: a single multishot `RECV` per connection for its lifetime
  replaces one submission per request, and that is the per-request work that
  dominates once every connection has a request in flight.
* Provided-buffer ring size (256 / 1 024 / 2 048 entries) makes no
  difference outside noise at either scale. 256 is the default.
* Tokio's 16 000-connection figure before the restart was 116k; the drop to
  48k is the environment, not a code change on its side (its read path was
  unchanged between the two runs). Treat cross-session comparisons in this
  document as invalid; only interleaved same-session rows compare.

## 5. The failure taxonomy

In the order you will meet them on a droplet:

1. **Accept backlog** (seconds-long stalls, no errors anywhere): sysctl, §1.
2. **`ulimit -n`** (`EMFILE`, connections refused): the unit file sets 65 536.
3. **Thread count** (`threads` only): ~32k on a default kernel; shed load,
   now handled. The other runtimes never spawn per connection.
4. **Queueing** (p50 climbs linearly with connections ÷ req/s): not a crash,
   Little's law; add cores or shed load.
5. **Memory**: 6–12 KB per idle event-driven connection, ~34 KB per `threads`
   connection, plus the request and response bytes per *active* one. The
   2 GB droplet OOMs at roughly 150k idle event-driven connections; `threads`
   hits the thread limit long before memory.
6. **Ephemeral ports**: one client IP cannot open more than ~28k connections
   to one server port; irrelevant for the server, decisive for a load
   generator. Use several client IPs or several server ports.
7. **Not implemented yet**, and what an adversary would use: no idle
   timeout (a slowloris holds fds for free), no cap on concurrent
   connections, no request timeout. Each is a small addition; none is in the
   benchmark path.

## 6. Extrapolating to a powerful machine

Not measured; reasoning from the 1→2 worker scaling above and from the
published experience of thread-per-core deployments (monoio, glommio, Iggy;
see `docs/LANDSCAPE.md`).

* **16–64 cores, one listener per worker with `SO_REUSEPORT`** (mio, uring,
  uring-async): throughput scales near-linearly as long as the kernel's
  hash spreads connections evenly, which it does for many similar connections
  and does *not* for a few heavy ones. A handful of hot connections pinned to
  one worker is the classic thread-per-core pathology; the mitigation is
  user-space accept redistribution (an eBPF `SO_REUSEPORT` program or a front
  accept thread), neither of which is implemented here.
* **Tokio** keeps scaling with work stealing and has no hot-worker problem,
  at the cost of cross-core cache traffic per task migration and the tail
  behaviour seen under CPU saturation in §4.
* **NUMA**: beyond one socket, thread-per-core wins again if each worker's
  memory is local; a work-stealing runtime migrating tasks across sockets
  pays for it. None of the runtimes here is NUMA-aware.
* **The kernel is the ceiling**: around 1M req/s per machine the TCP stack
  (softirq, conntrack, NIC queues) dominates and the user-space runtime
  choice matters less than RSS/XPS queue pinning and `SO_BUSY_POLL`.
  io_uring's remaining tricks (multishot recv, provided buffer rings,
  registered files, zero-copy send) are what `ringline` and `glommio` use
  to push past that; `ringrt` has none of them yet.

## 7. What a powerful setup would show that this box could not

A 16-core droplet plus a separate load-generator box would answer:

1. Does `uring-async` keep its single-core efficiency with 8–16 independent
   rings, or does `SO_REUSEPORT` imbalance hand Tokio the p99?
2. The real tail at 16 000+ connections, without the client sharing the kernel.
3. The true per-connection RSS at 100k+ connections (needs several client IPs).

`TARGET=http://<ip>:8080 scripts/bench.sh` from the second machine runs the
endpoint matrix against it; `scripts/scale.sh` and `scripts/stress.sh` run on
the server box.
