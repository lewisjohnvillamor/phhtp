#!/usr/bin/env bash
# System calls per request, per runtime, from `strace -c -f` on the server
# during a fixed keep-alive window. strace slows every traced call down, so
# the req/s in this table is NOT comparable to the matrix; the *count* of
# calls per request is what it measures (an untraced run would make the same
# calls). Needs strace and ptrace permission.
#
#   scripts/syscalls.sh                      # all runtimes
#   RUNTIMES="tokio uring-async" scripts/syscalls.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RUNTIMES=${RUNTIMES:-"threads pool mio tokio tokio-st uring uring-async monoio compio glommio tokio-uring"}
CONNS=${CONNS:-64}
DURATION=${DURATION:-4}
PORT=${PORT:-8080}
OUT=${OUT:-bench/results/syscalls.txt}

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

printf '| runtime | req/s (traced) | syscalls/req | top calls (per request) |\n|---|---:|---:|---|\n' | tee "$OUT"
for rt in $RUNTIMES; do
  "$SERVER" --runtime "$rt" --bind "127.0.0.1:$PORT" 2>/dev/null &
  spid=$!
  wait_port "$PORT"
  # Warm up untraced (connections, buffers), then attach for the measured window.
  "$BENCH" run "http://127.0.0.1:$PORT/json" -c "$CONNS" -d 1 -w 1 --no-stats >/dev/null
  strace -c -f -S calls -o "bench/results/strace-$rt.txt" -p "$spid" &
  tpid=$!
  sleep 0.3
  json=$("$BENCH" run "http://127.0.0.1:$PORT/json" -c "$CONNS" -d "$DURATION" -w 1 --no-stats --json)
  kill -INT "$tpid" 2>/dev/null || true; wait "$tpid" 2>/dev/null || true
  kill "$spid" 2>/dev/null || true; wait "$spid" 2>/dev/null || true
  reqs=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['requests'])" "$json")
  rps=$(python3 -c "import json,sys; print(round(json.loads(sys.argv[1])['rps']))" "$json")
  # strace -c summary: "% time  seconds  usecs/call  calls  errors  syscall"; the
  # window includes 1 s of warm-up traffic, so scale by measured/(measured+1).
  python3 - "$rt" "$reqs" "$rps" "$DURATION" "bench/results/strace-$rt.txt" <<'PY' | tee -a "$OUT"
import sys
rt, reqs, rps, dur, path = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5]
rows = []
for line in open(path):
    f = line.split()
    if len(f) >= 5 and f[0].replace('.', '').isdigit() and f[-1] not in ('total',):
        try:
            calls = int(f[3]); name = f[-1]
        except ValueError:
            continue
        rows.append((calls, name))
scale = dur / (dur + 1)  # window held 1 s of warm-up traffic too
total = sum(c for c, _ in rows) * scale
rows.sort(reverse=True)
top = ", ".join(f"{n} {c*scale/max(reqs,1):.2f}" for c, n in rows[:4])
print(f"| {rt} | {rps} | {total/max(reqs,1):.2f} | {top} |")
PY
done
