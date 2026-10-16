# Sample run: 4-core cloud container, Linux 6.18 (client and server on the same host)

Generated 2026-10-16T07:47Z with `DURATION=4 WARMUP=1 scripts/bench.sh`, all six runtimes at their default worker counts (4). Indicative only: the load generator shared the 4 cores with the server. Re-run on the droplet for real numbers.

| runtime | endpoint | conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| threads | `GET /` | 64 | 96598 | 0.60ms | 1.04ms | 2.04ms | 21.52ms | 0 |
| pool | `GET /` | 64 | 104198 | 0.56ms | 0.96ms | 1.92ms | 12.79ms | 0 |
| mio | `GET /` | 64 | 152250 | 0.32ms | 0.83ms | 1.62ms | 24.43ms | 0 |
| tokio | `GET /` | 64 | 144496 | 0.41ms | 0.65ms | 1.04ms | 8.29ms | 0 |
| tokio-st | `GET /` | 64 | 126418 | 0.46ms | 0.78ms | 1.23ms | 9.38ms | 0 |
| uring | `GET /` | 64 | 151296 | 0.32ms | 0.84ms | 1.55ms | 8.07ms | 0 |
| threads | `GET /json` | 64 | 104863 | 0.56ms | 0.96ms | 1.87ms | 12.38ms | 0 |
| pool | `GET /json` | 64 | 101088 | 0.57ms | 1.00ms | 1.93ms | 20.69ms | 0 |
| mio | `GET /json` | 64 | 168199 | 0.29ms | 0.75ms | 1.50ms | 21.41ms | 0 |
| tokio | `GET /json` | 64 | 138894 | 0.40ms | 0.71ms | 1.29ms | 33.47ms | 0 |
| tokio-st | `GET /json` | 64 | 122484 | 0.49ms | 0.78ms | 1.21ms | 6.92ms | 0 |
| uring | `GET /json` | 64 | 140078 | 0.35ms | 0.90ms | 1.66ms | 8.50ms | 0 |
| threads | `POST /sign` | 64 | 94656 | 0.61ms | 1.08ms | 2.06ms | 10.02ms | 0 |
| pool | `POST /sign` | 64 | 95634 | 0.61ms | 1.05ms | 2.11ms | 15.17ms | 0 |
| mio | `POST /sign` | 64 | 139443 | 0.34ms | 0.93ms | 1.79ms | 9.79ms | 0 |
| tokio | `POST /sign` | 64 | 121871 | 0.47ms | 0.83ms | 1.38ms | 8.95ms | 0 |
| tokio-st | `POST /sign` | 64 | 79599 | 0.73ms | 1.09ms | 2.00ms | 7.96ms | 0 |
| uring | `POST /sign` | 64 | 128662 | 0.40ms | 0.92ms | 2.01ms | 11.38ms | 0 |
| threads | `GET /compute?n=2000` | 64 | 5391 | 10.25ms | 19.76ms | 32.67ms | 55.62ms | 0 |
| pool | `GET /compute?n=2000` | 64 | 5238 | 10.80ms | 20.45ms | 35.42ms | 79.30ms | 0 |
| mio | `GET /compute?n=2000` | 64 | 5585 | 11.29ms | 13.48ms | 18.40ms | 24.02ms | 0 |
| tokio | `GET /compute?n=2000` | 64 | 5453 | 11.64ms | 17.50ms | 23.28ms | 32.51ms | 0 |
| tokio-st | `GET /compute?n=2000` | 64 | 1430 | 43.65ms | 46.85ms | 63.33ms | 71.81ms | 0 |
| uring | `GET /compute?n=2000` | 64 | 5254 | 12.67ms | 16.45ms | 21.65ms | 47.04ms | 0 |
| threads | `GET /sleep?ms=5` | 256 | 44938 | 5.45ms | 6.33ms | 8.65ms | 42.78ms | 0 |
| pool | `GET /sleep?ms=5` | 256 | 12004 | 5.27ms | 5.50ms | 6.49ms | 14.03ms | 0 |
| mio | `GET /sleep?ms=5` | 256 | 42477 | 5.96ms | 6.49ms | 7.86ms | 22.91ms | 0 |
| tokio | `GET /sleep?ms=5` | 256 | 37946 | 6.67ms | 7.55ms | 8.97ms | 12.74ms | 0 |
| tokio-st | `GET /sleep?ms=5` | 256 | 33298 | 7.68ms | 8.76ms | 10.10ms | 13.84ms | 0 |
| uring | `GET /sleep?ms=5` | 256 | 46838 | 5.32ms | 5.90ms | 7.47ms | 18.19ms | 0 |
