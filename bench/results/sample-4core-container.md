# Sample run: 4-core cloud container (client and server on the same host)

Generated 2026-10-16T07:26Z with `DURATION=4 WARMUP=1 scripts/bench.sh`. Indicative only: the load generator shared the 4 cores with the server. Re-run on the droplet for real numbers.

| runtime | endpoint | conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| threads | `GET /` | 64 | 111916 | 0.53ms | 0.89ms | 1.65ms | 12.37ms | 0 |
| pool | `GET /` | 64 | 105809 | 0.56ms | 0.95ms | 1.82ms | 8.54ms | 0 |
| mio | `GET /` | 64 | 157381 | 0.31ms | 0.81ms | 1.49ms | 8.20ms | 0 |
| tokio | `GET /` | 64 | 141154 | 0.42ms | 0.66ms | 1.08ms | 5.78ms | 0 |
| tokio-st | `GET /` | 64 | 130963 | 0.41ms | 0.75ms | 1.36ms | 32.30ms | 0 |
| threads | `GET /json` | 64 | 106490 | 0.55ms | 0.95ms | 1.86ms | 10.62ms | 0 |
| pool | `GET /json` | 64 | 109594 | 0.54ms | 0.92ms | 1.61ms | 13.85ms | 0 |
| mio | `GET /json` | 64 | 160230 | 0.31ms | 0.78ms | 1.47ms | 8.45ms | 0 |
| tokio | `GET /json` | 64 | 151118 | 0.38ms | 0.63ms | 1.03ms | 7.87ms | 0 |
| tokio-st | `GET /json` | 64 | 123194 | 0.49ms | 0.78ms | 1.23ms | 5.14ms | 0 |
| threads | `POST /sign` | 64 | 98890 | 0.60ms | 0.99ms | 1.94ms | 14.26ms | 0 |
| pool | `POST /sign` | 64 | 94979 | 0.61ms | 1.05ms | 2.11ms | 27.20ms | 0 |
| mio | `POST /sign` | 64 | 140489 | 0.34ms | 0.91ms | 1.90ms | 11.21ms | 0 |
| tokio | `POST /sign` | 64 | 113045 | 0.52ms | 0.87ms | 1.52ms | 7.49ms | 0 |
| tokio-st | `POST /sign` | 64 | 83460 | 0.71ms | 0.95ms | 1.99ms | 5.39ms | 0 |
| threads | `GET /compute?n=2000` | 64 | 5761 | 10.18ms | 18.22ms | 27.70ms | 58.88ms | 0 |
| pool | `GET /compute?n=2000` | 64 | 5540 | 10.38ms | 19.10ms | 30.08ms | 58.40ms | 0 |
| mio | `GET /compute?n=2000` | 64 | 5433 | 11.56ms | 15.29ms | 19.92ms | 26.34ms | 0 |
| tokio | `GET /compute?n=2000` | 64 | 5441 | 11.68ms | 17.45ms | 22.51ms | 30.85ms | 0 |
| tokio-st | `GET /compute?n=2000` | 64 | 1409 | 43.65ms | 50.40ms | 70.85ms | 73.28ms | 0 |
| threads | `GET /sleep?ms=5` | 256 | 46513 | 5.36ms | 5.96ms | 7.71ms | 23.23ms | 0 |
| pool | `GET /sleep?ms=5` | 256 | 11982 | 5.27ms | 5.51ms | 6.69ms | 11.75ms | 0 |
| mio | `GET /sleep?ms=5` | 256 | 42576 | 5.97ms | 6.45ms | 7.41ms | 11.43ms | 0 |
| tokio | `GET /sleep?ms=5` | 256 | 37808 | 6.70ms | 7.58ms | 8.88ms | 13.40ms | 0 |
| tokio-st | `GET /sleep?ms=5` | 256 | 34542 | 7.36ms | 8.31ms | 9.40ms | 13.90ms | 0 |
