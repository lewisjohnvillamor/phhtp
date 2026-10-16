# What we need to have, and the next target

Written October 2026 after the expanded round (EXPANDED.md) and the prior-art
review (LANDSCAPE.md §6). This is the plan that follows from the evidence,
not from the original ambition of "replace Tokio".

## 1. What the evidence says the target must be

Four facts constrain the choice.

1. **The runtime only matters where the handler is tiny and the connections
   are many.** On `/compute` all eleven runtimes measured the same; a webapp
   lives in its database driver. A general-purpose webapp on ringrt would be
   the wrong problem: it needs hyper, axum, sqlx and rustls, and would gain
   nothing measurable from the I/O model.
2. **Where ringrt measurably wins is the edge shape**: many keep-alive or
   idle connections, small or file-backed responses, memory per connection,
   median latency, throughput at high offered rate (136 vs 254 MB at 16 000
   connections; 0.13 vs 0.27 ms p50; only runtime under 10 ms p99 at
   125 k req/s). That is a static-asset server, a reverse proxy, a WebSocket
   fan-out, a metrics ingest.
3. **io_uring's clearest published advantage is file I/O**, not sockets:
   Tokio does file I/O on a blocking thread pool, io_uring does it in the
   ring (one test: 2.7× on file reads). We have not used that at all yet;
   `/blob` serves from memory.
4. **The honest competitor for an edge tier is nginx or Caddy**, not Tokio.
   ByteDance's claim for Monoio was "up to 20 % over nginx". Nobody deploys
   a hello-world; people deploy nginx in front of their app. That is the
   comparison that would mean something on a $12 droplet.

## 2. The next target: `phhtp` as the edge in front of a real webapp

One binary on the droplet, on ringrt, doing the three things an edge does,
with a Tokio (axum) webapp behind it on the same box:

| Role | What it needs from us | Why it is the right test of ringrt |
|---|---|---|
| **Static assets** | file I/O through the ring (`openat`, `statx`, `read` into provided buffers), registered files, `SEND_ZC` for bodies ≥ 16 KiB, `sendfile`/`splice` where cheaper, ETag/`Last-Modified`/`Range`, precompressed `.br`/`.gz` lookup | io_uring's strongest published case; the one place where Tokio *must* use a thread pool |
| **Reverse proxy** | upstream keep-alive pool to the app over loopback/Unix socket, HTTP/1.1 streaming body relay (chunked encoding, which the engine lacks), `X-Forwarded-*`, connection and header timeouts | many client connections, tiny per-request work, exactly the shape where the median and memory wins show |
| **TLS termination** | rustls (sans-IO, runtime-agnostic), ALPN h1 only at first, session resumption | removes the "but production is HTTPS" objection; rustls does the CPU work, the runtime does the bytes |

The webapp behind it stays on Tokio and axum and keeps the whole ecosystem.
The edge is where the hardware gets maximized, and it is the part we fully
control.

## 3. What we need to have, in order

### 3.1 Runtime surface (ringrt), before anything else is built on it

| # | Item | Why now |
|---|---|---|
| 1 | **Idle and header timeouts per connection** | the slowloris run showed ringrt holds 10 000 stalled sockets forever; an edge must shed them |
| 2 | **Connection cap and accept backpressure** | fd exhaustion was a real failure in SCALE.md; cap, then shed |
| 3 | **`JoinHandle` and task cancellation** | the proxy needs to await an upstream request and drop it on client disconnect |
| 4 | **Graceful shutdown** (stop accepting, drain, deadline) | deploys on a droplet restart the process |
| 5 | **File ops through the ring** (`openat`, `statx`, `read`, `close`) and **registered files** | item 2 of the target; also removes the only blocking call an edge would otherwise make |
| 6 | **`SEND_ZC` with notification handling** and `splice` | the throughput side for large bodies, measured on `/blob` today at 114–119 k req/s client-bound |
| 7 | **Small blocking pool** | DNS, anything we cannot route through the ring |
| 8 | **Availability check with a clear error, then an epoll fallback** | containers block io_uring; the droplet does not, so the fallback is last, not first, but the check and the message are day one |

### 3.2 Protocol surface (phhtp-core)

* chunked transfer encoding in and out (streaming relay cannot buffer whole bodies)
* `Range`, `If-None-Match`, `If-Modified-Since`, `HEAD` on files
* upstream client side of HTTP/1.1 (we only have the server side)
* request and response header size limits are there; add body size limits per route

### 3.3 Measurement, so the result means something

* **The droplet run with a separate load generator over a real NIC.** First.
  Until then every ordering among the fast runtimes is a loopback artifact.
* **nginx and Caddy in the same harness**, same files, same upstream app, same
  shapes, same `/stats`-equivalent accounting via `/proc/<pid>`.
* **Repetitions on every sweep row**, not only the matrix; the rate sweep's
  two spikes are single runs.
* **A real webapp as the upstream** (axum + sqlx + Postgres on the droplet),
  measured with and without the edge, so the question "does the edge help
  the app or just itself" has a number.

## 4. What we stop doing

* Chasing Tokio on CPU per request at saturation on loopback. The
  per-`io_uring_enter` cost is the kernel's; the prior art and our data agree
  the gap is 0–15 % and inside noise on this box.
* Adding runtimes to the comparison. Eleven is enough; nginx and Caddy are
  the two that still matter, and they are not Rust runtimes.
* Treating "publish to crates.io" as the goal. It can be published as
  experimental any time; it becomes worth using when the edge above runs on
  it for a week on the droplet.

## 5. Milestones

| Milestone | Done when |
|---|---|
| M0 Droplet baseline | the eleven-runtime matrix and the rate sweep reproduced from a second droplet as client; ordering of `uring`, `uring-async`, `mio`, `tokio` settled or shown to be within noise on real hardware |
| M1 Hardened runtime | **done** (idle sweeper, connection cap with shedding, graceful shutdown, `JoinHandle`/abort, `time::timeout`, `TcpStream::shutdown`; see EXPANDED.md §10) |
| M2 Files through the ring | **done on loopback** (`openat`/`statx`/`read` as ring ops, `SEND_ZC`, nginx `sendfile` in the harness; EXPANDED.md §10); registered files and `splice` still open; the droplet comparison is M0's |
| M3 Proxy | streaming HTTP/1.1 relay to an axum app with keep-alive upstream pool; measured against nginx `proxy_pass` |
| M4 TLS | rustls termination; HTTPS rows for M2 and M3 |
| M5 One week on the droplet | the edge in front of a real app, with `/stats` sampled hourly; then decide whether to publish |

If M0 shows the loopback advantages do not survive a real NIC, the right
outcome is to say so and keep the harness; the study will have answered its
question either way.
