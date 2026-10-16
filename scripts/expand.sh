#!/usr/bin/env bash
# Expanded load shapes, beyond the matrix and the scale sweeps:
#
#   pipeline  HTTP/1.1 pipelining, PIPELINE requests per write (parser + batching cost)
#   idle      IDLE stalled connections (half a request line, then silence) held open
#             while CONNS hot connections are measured: slowloris / mobile-idle tolerance
#   mix       one connection rotates through /json, /compute?n=2000 and / (cache and
#             branch behaviour of a realistic mixed path set)
#   tail      a long window (TAIL_DURATION s) at TAIL_CONNS connections for p99.9/p99.99
#   contend   a busy co-tenant pinned to one of the server's two cores for the whole run
#             (cron job, log shipper, noisy neighbour): who degrades gracefully?
#   rate      open-loop sweep over RATES to find each runtime's latency knee
#
#   scripts/expand.sh                         # everything, all runtimes
#   MODES="pipeline idle" RUNTIMES="tokio uring-async" scripts/expand.sh
#
# Server pinned to SERVER_CPUS, client to CLIENT_CPUS (taskset) as in scale.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

MODES=${MODES:-"pipeline idle mix tail contend rate"}
RUNTIMES=${RUNTIMES:-"threads pool mio tokio tokio-st uring uring-async monoio compio glommio tokio-uring"}
CONNS=${CONNS:-64}
PIPELINE=${PIPELINE:-16}
IDLE=${IDLE:-10000}
TAIL_CONNS=${TAIL_CONNS:-256}
TAIL_DURATION=${TAIL_DURATION:-20}
RATES=${RATES:-"10000 25000 50000 75000 100000 125000"}
RATE_CONNS=${RATE_CONNS:-256}
DURATION=${DURATION:-5}
WARMUP=${WARMUP:-2}
REPS=${REPS:-2}
PORT=${PORT:-8080}
SERVER_CPUS=${SERVER_CPUS:-0-1}
HOG_CPU=${HOG_CPU:-1}
CLIENT_CPUS=${CLIENT_CPUS:-2-3}
OUT=${OUT:-bench/results/expand-$(date +%Y%m%d-%H%M%S).jsonl}
NOFILE=${NOFILE:-$(ulimit -Hn)}

ulimit -n "$NOFILE" || true
cargo build --release --quiet
mkdir -p "$(dirname "$OUT")"
SERVER=./target/release/phhtp
BENCH=./target/release/phhtp-bench
PIN_S=(); PIN_C=(); PIN_H=()
if command -v taskset >/dev/null; then
  PIN_S=(taskset -c "$SERVER_CPUS"); PIN_C=(taskset -c "$CLIENT_CPUS"); PIN_H=(taskset -c "$HOG_CPU")
fi

wait_port() {
  for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  echo "server did not come up on port $1" >&2; return 1
}

start_server() {
  "${PIN_S[@]}" "$SERVER" --runtime "$1" --bind "127.0.0.1:$PORT" 2>"bench/results/server-$1.log" &
  SPID=$!
  trap 'kill $SPID ${HPID:-} 2>/dev/null || true' EXIT
  wait_port "$PORT"
}
stop_server() { kill "$SPID" 2>/dev/null || true; wait "$SPID" 2>/dev/null || true; trap - EXIT; }

bench() { # <label> <note> <args...>
  local label=$1 note=$2; shift 2
  "${PIN_C[@]}" "$BENCH" run "$@" --label "$label" --json --note "$note somaxconn=$(cat /proc/sys/net/core/somaxconn 2>/dev/null || echo ?)" | tee -a "$OUT"
}

for rt in $RUNTIMES; do
  for mode in $MODES; do
    echo "== $rt $mode" >&2
    start_server "$rt"
    base="http://127.0.0.1:$PORT"
    case $mode in
      pipeline) bench "$rt" "expand=pipeline depth=$PIPELINE" "$base/json" -c "$CONNS" -d "$DURATION" -w "$WARMUP" --reps "$REPS" --pipeline "$PIPELINE" ;;
      idle)     bench "$rt" "expand=idle idle=$IDLE" "$base/json" -c "$CONNS" -d "$DURATION" -w "$WARMUP" --reps "$REPS" --idle "$IDLE" --idle-partial --timeout 3 ;;
      mix)      bench "$rt" "expand=mix" "$base/json" -c "$CONNS" -d "$DURATION" -w "$WARMUP" --reps "$REPS" --mix "/compute?n=2000" --mix "/" ;;
      tail)     bench "$rt" "expand=tail" "$base/json" -c "$TAIL_CONNS" -d "$TAIL_DURATION" -w "$WARMUP" ;;
      contend)
        "${PIN_H[@]}" sh -c 'while :; do :; done' & HPID=$!
        bench "$rt" "expand=contend hog_cpu=$HOG_CPU" "$base/json" -c "$CONNS" -d "$DURATION" -w "$WARMUP" --reps "$REPS"
        kill "$HPID" 2>/dev/null || true; wait "$HPID" 2>/dev/null || true; HPID=
        ;;
      rate)
        for r in $RATES; do
          bench "$rt" "sweep=rate rate=$r" "$base/json" -c "$RATE_CONNS" -d "$DURATION" -w "$WARMUP" --rate "$r"
        done
        ;;
      *) echo "unknown mode $mode" >&2; stop_server; exit 1 ;;
    esac
    stop_server
  done
done

echo >&2
echo "results: $OUT" >&2
"$BENCH" report "$OUT"
