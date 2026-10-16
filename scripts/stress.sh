#!/usr/bin/env bash
# Deliberate failure experiments: what breaks, how, and does it recover.
#
#   scripts/stress.sh            # all runtimes
#   RUNTIMES="threads uring-async" scripts/stress.sh
#
# 1. fd exhaustion: server limited to NOFILE_SMALL fds, hit with 2× that many
#    connections, then probed with a small load to confirm it still serves.
# 2. thread exhaustion (threads runtime): server limited to NPROC_SMALL
#    threads, hit with 2× that many connections, then probed.
# 3. CPU overload: /compute?n=20000 with 256 connections.
set -euo pipefail
cd "$(dirname "$0")/.."

RUNTIMES=${RUNTIMES:-"threads pool mio tokio tokio-st uring uring-async"}
NOFILE_SMALL=${NOFILE_SMALL:-512}
NPROC_SMALL=${NPROC_SMALL:-600}
PORT=${PORT:-8080}
DURATION=${DURATION:-3}

ulimit -n "$(ulimit -Hn)" || true
cargo build --release --quiet
SERVER=./target/release/phhtp
BENCH=./target/release/phhtp-bench

wait_port() {
  for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  echo "server did not come up on port $1" >&2; return 1
}

summarize() { # summarize <label>  (bench JSON on stdin)
  python3 -c '
import json, sys
d = json.loads(sys.stdin.read())
ms = lambda u: "%.2fms" % (u / 1000)
print("    %-26s req/s %9.0f  p50 %8s  p99 %8s  errors %d" % (sys.argv[1], d["rps"], ms(d["p50_us"]), ms(d["p99_us"]), d["errors"] + d["non_2xx"]))
' "$1"
}

start() { # start <runtime> [env prefix...]
  local rt=$1; shift
  "$@" "$SERVER" --runtime "$rt" --bind "127.0.0.1:$PORT" 2>"bench/results/stress-$rt.log" &
  PID=$!; trap 'kill $PID 2>/dev/null || true' EXIT; wait_port "$PORT"
}
stop() { kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; trap - EXIT; sleep 1; }
alive() { if kill -0 "$PID" 2>/dev/null; then echo "    server process: alive"; else echo "    server process: DEAD (exit $(wait $PID 2>/dev/null; echo $?))"; fi; }

for rt in $RUNTIMES; do
  echo "== $rt: fd exhaustion (server ulimit -n $NOFILE_SMALL, $((NOFILE_SMALL*2)) connections)"
  start "$rt" bash -c 'ulimit -n '"$NOFILE_SMALL"'; exec "$@"' --
  "$BENCH" run "http://127.0.0.1:$PORT/" -c $((NOFILE_SMALL*2)) -d "$DURATION" -w 2 --timeout 3 --json | summarize "under fd pressure"
  alive
  "$BENCH" run "http://127.0.0.1:$PORT/" -c 16 -d 2 -w 1 --timeout 3 --json | summarize "recovery probe (16 conns)"
  grep -m1 -E "accept|spawn" "bench/results/stress-$rt.log" | sed 's/^/    server log: /' || true
  stop

  if [[ "$rt" == threads ]]; then
    echo "== $rt: thread exhaustion (server ulimit -u $NPROC_SMALL, $((NPROC_SMALL*2)) connections)"
    start "$rt" prlimit --nproc="$NPROC_SMALL"
    "$BENCH" run "http://127.0.0.1:$PORT/" -c $((NPROC_SMALL*2)) -d "$DURATION" -w 2 --timeout 3 --json | summarize "under thread pressure"
    alive
    "$BENCH" run "http://127.0.0.1:$PORT/" -c 16 -d 2 -w 1 --timeout 3 --json | summarize "recovery probe (16 conns)"
    grep -m1 -E "spawn" "bench/results/stress-$rt.log" | sed 's/^/    server log: /' || true
    stop
  fi

  echo "== $rt: CPU overload (/compute?n=20000, 256 connections)"
  start "$rt"
  "$BENCH" run "http://127.0.0.1:$PORT/compute?n=20000" -c 256 -d "$DURATION" -w 2 --timeout 30 --json | summarize "cpu overload"
  "$BENCH" run "http://127.0.0.1:$PORT/" -c 16 -d 2 -w 1 --timeout 3 --json | summarize "after overload (16 conns)"
  alive
  stop
done
