# Sample run: 4-core cloud container, Linux 6.18 (client and server on the same host)

Generated 2026-10-16T11:22Z with `DURATION=4 WARMUP=1 scripts/bench.sh`, all seven runtimes at default worker counts (4), after ringrt gained multishot accept/recv with provided buffers. Indicative only: unpinned, and identical runs on this box differ by up to ±15%.

| runtime | endpoint | conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| threads | `GET /` | 64 | 94161 | 0.62ms | 1.07ms | 2.08ms | 10.38ms | 0 |
| pool | `GET /` | 64 | 90097 | 0.65ms | 1.11ms | 2.13ms | 17.55ms | 0 |
| mio | `GET /` | 64 | 144414 | 0.34ms | 0.87ms | 1.68ms | 9.38ms | 0 |
| tokio | `GET /` | 64 | 128837 | 0.46ms | 0.77ms | 1.29ms | 7.01ms | 0 |
| tokio-st | `GET /` | 64 | 115654 | 0.52ms | 0.82ms | 1.45ms | 5.37ms | 0 |
| uring | `GET /` | 64 | 131917 | 0.38ms | 0.93ms | 1.72ms | 16.30ms | 0 |
| uring-async | `GET /` | 64 | 141633 | 0.36ms | 0.87ms | 1.64ms | 10.65ms | 0 |
| threads | `GET /json` | 64 | 86850 | 0.67ms | 1.18ms | 2.25ms | 16.48ms | 0 |
| pool | `GET /json` | 64 | 89633 | 0.65ms | 1.12ms | 2.20ms | 12.29ms | 0 |
| mio | `GET /json` | 64 | 141814 | 0.34ms | 0.91ms | 1.74ms | 8.84ms | 0 |
| tokio | `GET /json` | 64 | 136610 | 0.42ms | 0.73ms | 1.25ms | 9.01ms | 0 |
| tokio-st | `GET /json` | 64 | 129437 | 0.46ms | 0.73ms | 1.27ms | 5.23ms | 0 |
| uring | `GET /json` | 64 | 130576 | 0.39ms | 0.94ms | 1.75ms | 8.54ms | 0 |
| uring-async | `GET /json` | 64 | 154687 | 0.32ms | 0.80ms | 1.50ms | 7.76ms | 0 |
| threads | `POST /sign` | 64 | 92580 | 0.63ms | 1.09ms | 2.18ms | 13.24ms | 0 |
| pool | `POST /sign` | 64 | 90389 | 0.65ms | 1.10ms | 2.14ms | 9.00ms | 0 |
| mio | `POST /sign` | 64 | 122066 | 0.40ms | 1.04ms | 2.04ms | 10.26ms | 0 |
| tokio | `POST /sign` | 64 | 112396 | 0.53ms | 0.89ms | 1.60ms | 6.32ms | 0 |
| tokio-st | `POST /sign` | 64 | 76147 | 0.70ms | 1.50ms | 2.06ms | 14.34ms | 0 |
| uring | `POST /sign` | 64 | 115406 | 0.45ms | 1.02ms | 2.16ms | 24.02ms | 0 |
| uring-async | `POST /sign` | 64 | 120899 | 0.43ms | 0.97ms | 1.93ms | 8.28ms | 0 |
| threads | `GET /compute?n=2000` | 64 | 5235 | 10.94ms | 20.69ms | 33.22ms | 90.88ms | 0 |
| pool | `GET /compute?n=2000` | 64 | 4949 | 11.49ms | 21.92ms | 35.90ms | 64.83ms | 0 |
| mio | `GET /compute?n=2000` | 64 | 4782 | 14.26ms | 20.46ms | 25.17ms | 30.88ms | 0 |
| tokio | `GET /compute?n=2000` | 64 | 4764 | 13.37ms | 19.86ms | 25.52ms | 39.81ms | 0 |
| tokio-st | `GET /compute?n=2000` | 64 | 1205 | 48.70ms | 72.25ms | 74.88ms | 75.65ms | 0 |
| uring | `GET /compute?n=2000` | 64 | 4816 | 13.05ms | 20.38ms | 24.46ms | 40.29ms | 0 |
| uring-async | `GET /compute?n=2000` | 64 | 4831 | 11.97ms | 18.91ms | 24.30ms | 41.22ms | 0 |
| threads | `GET /sleep?ms=5` | 256 | 45498 | 5.46ms | 6.24ms | 7.90ms | 15.34ms | 0 |
| pool | `GET /sleep?ms=5` | 256 | 12007 | 5.29ms | 5.51ms | 6.05ms | 8.89ms | 0 |
| mio | `GET /sleep?ms=5` | 256 | 42248 | 6.01ms | 6.56ms | 7.41ms | 12.67ms | 0 |
| tokio | `GET /sleep?ms=5` | 256 | 37897 | 6.71ms | 7.55ms | 8.65ms | 16.24ms | 0 |
| tokio-st | `GET /sleep?ms=5` | 256 | 33604 | 7.59ms | 8.63ms | 10.35ms | 13.29ms | 0 |
| uring | `GET /sleep?ms=5` | 256 | 45205 | 5.50ms | 6.33ms | 7.78ms | 11.63ms | 0 |
| uring-async | `GET /sleep?ms=5` | 256 | 45660 | 5.43ms | 6.22ms | 7.72ms | 14.94ms | 0 |
