# Scale: how far each runtime goes, and what breaks first

Measured October 2026 on a 4-core, 16 GB cloud container, Linux 6.18, with
`scripts/scale.sh` and `scripts/stress.sh`. The server was pinned to cores 0–1
and the load generator to cores 2–3, so every number below is a **2-core
server**. "Powerful setup" numbers are extrapolated in §6; they are not
measured here.

Box limits that matter, and where they come from:

| Limit | Value here | Set by | What hits it |
|---|---:|---|---|
| open files per process | 20 000 | `ulimit -n` (hard) | one fd per connection, server *and* client |
| threads system-wide | ≈32 000 | `pid_max` 32768, `vm.max_map_count` 65530 (2 maps per thread stack) | `threads` runtime |
| accept backlog | 4 096 default, 65 535 for these runs | `net.core.somaxconn`, `listen()` | connect herds |
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
than the classic overflow counter; the cure is the same.

Consequences, now applied:

* every runtime asks for a 65 535 backlog (std's `TcpListener::bind` asks
  for 128, Tokio's for 1 024; both are far too small for a reconnect storm);
* `deploy/sysctl-phhtp.conf` sets `somaxconn` and `tcp_max_syn_backlog` to
  65 535;
* `phhtp-bench` paces connects (`--connect-concurrency 256`, `0` for the burst).

**Production reading:** after a restart with N keep-alive clients reconnecting
at once, a default Ubuntu box stalls for several seconds once N is a few
thousand, whatever the runtime. This is the most likely "crash" a $12 droplet
will show, and it is a sysctl, not code.

## 2. Connection sweep (2-core server, `GET /`, keep-alive, one request in flight per connection)

RESULTS_CONNS

Reading it:

* **Throughput is flat to ~4 096 connections for every event-driven runtime**
  and falls off at 16 000 because 16 000 requests in flight on 2 cores means a
  queue 16 000 deep: at ~100k req/s that is a 160 ms service round, and p50
  shows exactly that. This is Little's law, not a runtime defect. Fewer
  connections or more cores are the only fixes.
* **`threads` degrades from 1 024 connections on.** 1 024 OS threads on 2 cores
  is already 10 ms p50 (scheduler time slices); at 4 096 it is 40 ms p50 with
  a 2 s max; at 16 000 it is 12 900 threads and 490 MB RSS for 3 000 req/s.
  The model is fine to a few hundred connections and wrong beyond.
* **`pool` serves exactly 64 connections.** Every other connection waits in the
  accept queue forever (keep-alive never frees a worker), which the bench
  reports as timeouts. Its latency looks perfect because only the lucky 64 are
  measured. This is the "silent" failure mode: no error in the server log, no
  crash, just clients that never get a response.
* **Memory per idle connection** (RSS at 16 000 ÷ 16 000): see the RSS column.
  The io_uring runtimes sit around 7–8 KB per connection, `tokio` and `mio`
  near 5–6 KB after the zero-fill fix, `threads` ~30 KB (a stack is touched
  even when idle). At the droplet's 2 GB that is roughly 60 000 idle
  keep-alive connections for the event-driven runtimes before the OOM killer,
  far beyond what one vCPU could serve anyway.

## 3. Worker sweep (2-core server, 1 024 connections)

RESULTS_WORKERS

Reading it:

* **One worker is the droplet case.** On 1 vCPU every runtime is single
  threaded; compare the `workers = 1` rows. `uring` (callback) is the fastest
  single core, `uring-async` next, then `tokio`, `mio`, `tokio-st`.
* **Two workers on two cores** is the honest multi-core number: each runtime
  gains 40–60 %. Four workers on two cores (oversubscribed) gains nothing or
  loses a little for the io_uring runtimes and still gains for Tokio, whose
  work stealing tolerates oversubscription better than one-loop-per-thread
  designs do.
* **Tokio with one worker has a bad tail** (p99 164 ms versus 17 ms for
  `uring-async`): its multi-thread scheduler's LIFO slot and global queue
  interplay starves some connections when there is only one thread to steal
  from. On a 1-vCPU droplet, `tokio-st` (current-thread) is the fairer Tokio
  configuration.
* **`pool` with 1, 2, 4 workers serves 1, 2, 4 connections.** The req/s column
  is those connections alone.

## 4. Stress: what breaks, how, and does it come back

RESULTS_STRESS

What the experiments did and what the outcome means:

* **fd exhaustion** (server limited to 512 fds, 1 024 clients). Expected and
  observed: `accept` fails with `EMFILE` for the clients that do not fit;
  the ones that fit are served normally; the process stays up; when the load
  goes away a fresh probe is served. One wrinkle per model: the blocking
  runtimes and the callback `uring` loop sleep 10 ms on `EMFILE` to avoid a
  hot loop, which stalls that worker's *other* connections for 10 ms per
  rejected client; `mio` breaks out of its accept loop and relies on the next
  edge; `uring-async` sleeps asynchronously and stalls nothing.
* **thread exhaustion** (`threads` runtime limited to 600 threads, 1 200
  clients). Before this session, a failed spawn propagated out of the accept
  loop and **exited the process** — that was the one true crash in the code
  base. It now logs, drops that connection, and keeps serving.
* **CPU overload** (`/compute?n=20000`, 256 connections, ~90 ms of SHA-256
  per request). Nothing crashes; latency is the full queue depth. The
  thread-per-core runtimes stall every connection on the busy worker;
  Tokio stalls every connection on the busy *thread* but can keep serving
  from the others; `threads` degrades most gracefully here because the
  kernel scheduler time-slices the compute. A real server needs a blocking
  pool or a bounded `/compute`; the handler already clamps `n`.

## 5. The failure taxonomy

In the order you will meet them on a droplet:

1. **Accept backlog** (seconds-long stalls, no errors anywhere): sysctl, §1.
2. **`ulimit -n`** (`EMFILE`, connections refused): the unit file sets 65 536.
3. **Thread count** (`threads` only): ~32k on a default kernel; shed load,
   now handled. The other runtimes never spawn per connection.
4. **Queueing** (p50 climbs linearly with connections ÷ req/s): not a crash,
   Little's law; add cores or shed load.
5. **Memory**: ~6–8 KB per idle event-driven connection, ~30 KB per `threads`
   connection, plus 16 KB input + response size per *active* one. The 2 GB
   droplet OOMs at roughly 60k idle or ~50k active connections for the
   event-driven runtimes, ~40k for `threads` if the thread limit let it get
   there (it will not).
6. **Ephemeral ports**: a single client IP cannot open more than ~28k
   connections to one server port; irrelevant for a server, decisive for a
   load generator. Use several client IPs or several server ports.
7. **Things this server does not do yet** and that an adversary would use:
   no idle-connection timeout (a slowloris holds fds for free), no cap on
   concurrent connections, no request timeout. Each is a small addition; none
   is in the benchmark path.

## 6. Extrapolating to a powerful machine

Not measured; reasoning from the 1→2 worker scaling above and from the
published experience of thread-per-core deployments (monoio, glommio, Iggy;
see `docs/LANDSCAPE.md`).

* **16–64 cores, one listener-per-worker with `SO_REUSEPORT`** (mio, uring,
  uring-async): throughput scales near-linearly as long as the kernel's
  hash spreads connections evenly, which it does for many short-lived or
  many similar connections and does *not* for a few heavy ones. A handful of
  hot connections pinned to one worker is the classic thread-per-core
  pathology; the mitigation is a user-space `accept` redistribution
  (eBPF `SO_REUSEPORT` steering or a front accept thread), neither of which
  is implemented here.
* **Tokio** keeps scaling with work stealing and does not have the hot-worker
  problem, at the cost of cross-core cache traffic per task migration. Its
  p99 advantage in the small benchmark should persist and widen on big
  machines with uneven load.
* **NUMA**: beyond one socket, thread-per-core wins again if each worker's
  memory is local; a work-stealing runtime migrating tasks across sockets
  pays for it. None of the runtimes here is NUMA-aware.
* **The kernel is the ceiling**: at ~1M req/s per machine the TCP stack
  (softirq, conntrack, loopback vs NIC queues) dominates and the user-space
  runtime choice matters less than RSS/XPS queue pinning and `SO_BUSY_POLL`.
  io_uring's remaining tricks (multishot recv, provided buffer rings,
  registered files, zero-copy send) are what `ringline` and `glommio` use
  to push past that; `ringrt` has none of them yet.

## 7. What a "powerful setup" would show that this box could not

A 16-core droplet run would answer three questions this box cannot:

1. Does `uring-async` keep its single-core lead with 8–16 independent rings,
   or does `SO_REUSEPORT` imbalance give Tokio the win at p99?
2. Where does the `threads` model fall over with real memory: 4 096 threads
   was 154 MB here; 32k would be the kernel limit before memory.
3. The true per-connection RSS at 100k+ connections (needs several client
   IPs; see §5 item 6).

`TARGET=http://<ip>:8080 scripts/scale.sh` from a second machine runs the
sweeps against it; `scripts/stress.sh` must run on the server box.
