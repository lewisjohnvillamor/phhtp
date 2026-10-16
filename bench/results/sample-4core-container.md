# Sample run: 4-core cloud container, Linux 6.18 (client and server on the same host)

Generated 2026-10-16T09:35Z with `DURATION=4 WARMUP=1 scripts/bench.sh`, all seven runtimes at their default worker counts (4). Indicative only: the load generator shared the 4 cores with the server. Re-run on the droplet for real numbers.

| runtime | endpoint | conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| threads | `GET /` | 64 | 98426 | 0.59ms | 1.03ms | 1.84ms | 19.14ms | 0 |
| pool | `GET /` | 64 | 88153 | 0.64ms | 1.19ms | 2.46ms | 13.55ms | 0 |
| mio | `GET /` | 64 | 146772 | 0.33ms | 0.87ms | 1.68ms | 12.07ms | 0 |
| tokio | `GET /` | 64 | 144927 | 0.40ms | 0.67ms | 1.04ms | 6.66ms | 0 |
| tokio-st | `GET /` | 64 | 141162 | 0.38ms | 0.74ms | 1.07ms | 4.16ms | 0 |
| uring | `GET /` | 64 | 165086 | 0.30ms | 0.76ms | 1.33ms | 10.34ms | 0 |
| uring-async | `GET /` | 64 | 141241 | 0.34ms | 0.89ms | 1.67ms | 133.89ms | 0 |
| threads | `GET /json` | 64 | 93168 | 0.63ms | 1.07ms | 2.04ms | 8.65ms | 0 |
| pool | `GET /json` | 64 | 96156 | 0.60ms | 1.07ms | 2.01ms | 12.88ms | 0 |
| mio | `GET /json` | 64 | 128905 | 0.37ms | 0.97ms | 2.18ms | 28.16ms | 0 |
| tokio | `GET /json` | 64 | 139530 | 0.41ms | 0.70ms | 1.12ms | 13.10ms | 0 |
| tokio-st | `GET /json` | 64 | 114302 | 0.53ms | 0.87ms | 1.33ms | 4.48ms | 0 |
| uring | `GET /json` | 64 | 150920 | 0.33ms | 0.83ms | 1.46ms | 7.41ms | 0 |
| uring-async | `GET /json` | 64 | 142670 | 0.34ms | 0.86ms | 1.69ms | 9.77ms | 0 |
| threads | `POST /sign` | 64 | 95706 | 0.61ms | 1.05ms | 2.08ms | 12.97ms | 0 |
| pool | `POST /sign` | 64 | 103782 | 0.56ms | 0.97ms | 1.88ms | 12.41ms | 0 |
| mio | `POST /sign` | 64 | 130405 | 0.35ms | 1.00ms | 2.31ms | 30.99ms | 0 |
| tokio | `POST /sign` | 64 | 110550 | 0.53ms | 0.89ms | 1.64ms | 18.09ms | 0 |
| tokio-st | `POST /sign` | 64 | 74364 | 0.79ms | 1.18ms | 2.03ms | 9.16ms | 0 |
| uring | `POST /sign` | 64 | 141430 | 0.36ms | 0.85ms | 1.69ms | 6.61ms | 0 |
| uring-async | `POST /sign` | 64 | 127071 | 0.40ms | 0.93ms | 2.12ms | 11.89ms | 0 |
| threads | `GET /compute?n=2000` | 64 | 5187 | 10.97ms | 20.67ms | 33.63ms | 77.50ms | 0 |
| pool | `GET /compute?n=2000` | 64 | 5160 | 10.72ms | 21.28ms | 39.62ms | 72.58ms | 0 |
| mio | `GET /compute?n=2000` | 64 | 5120 | 11.57ms | 17.66ms | 31.18ms | 36.73ms | 0 |
| tokio | `GET /compute?n=2000` | 64 | 5515 | 11.52ms | 17.21ms | 22.75ms | 32.11ms | 0 |
| tokio-st | `GET /compute?n=2000` | 64 | 1357 | 45.53ms | 55.62ms | 70.59ms | 77.38ms | 0 |
| uring | `GET /compute?n=2000` | 64 | 5680 | 10.91ms | 15.49ms | 22.99ms | 32.53ms | 0 |
| uring-async | `GET /compute?n=2000` | 64 | 5464 | 10.99ms | 15.70ms | 20.99ms | 23.26ms | 0 |
| threads | `GET /sleep?ms=5` | 256 | 44048 | 5.60ms | 6.63ms | 8.65ms | 17.71ms | 0 |
| pool | `GET /sleep?ms=5` | 256 | 12029 | 5.28ms | 5.48ms | 6.22ms | 13.10ms | 0 |
| mio | `GET /sleep?ms=5` | 256 | 42128 | 5.99ms | 6.55ms | 8.41ms | 19.77ms | 0 |
| tokio | `GET /sleep?ms=5` | 256 | 38057 | 6.68ms | 7.52ms | 8.59ms | 11.59ms | 0 |
| tokio-st | `GET /sleep?ms=5` | 256 | 33550 | 7.56ms | 8.74ms | 10.34ms | 17.93ms | 0 |
| uring | `GET /sleep?ms=5` | 256 | 46044 | 5.38ms | 6.11ms | 7.87ms | 21.30ms | 0 |
| uring-async | `GET /sleep?ms=5` | 256 | 45850 | 5.43ms | 6.17ms | 7.63ms | 10.78ms | 0 |
