#!/usr/bin/env bash
# Connection-count and worker-count sweeps, with server RSS and thread count.
#
#   scripts/scale.sh                 # both sweeps, all runtimes
#   MODE=conns CONNS_LIST="64 1024 4096 16000" scripts/scale.sh
#   MODE=workers WORKERS_LIST="1 2 4" RUNTIMES="mio tokio uring uring-async" scripts/scale.sh
#
# The server is pinned to SERVER_CPUS and the load generator to CLIENT_CPUS
# (taskset) so the client's CPU use does not pollute the server's numbers.
# Needs: taskset, python3, `ulimit -n` high enough for the largest CONNS.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=${MODE:-both}
RUNTIMES=${RUNTIMES:-"threads pool mio tokio tokio-st uring uring-async monoio compio glommio tokio-uring"}
CONNS_LIST=${CONNS_LIST:-"64 1024 4096 16000"}
WORKERS_LIST=${WORKERS_LIST:-"1 2 4"}
WORKERS_CONNS=${WORKERS_CONNS:-1024}
PATH_=${ENDPOINT:-/}
DURATION=${DURATION:-4}
WARMUP=${WARMUP:-3}
REPS=${REPS:-1}
PORT=${PORT:-8080}
SERVER_CPUS=${SERVER_CPUS:-0-1}
CLIENT_CPUS=${CLIENT_CPUS:-2-3}
OUT=${OUT:-bench/results/scale-$(date +%Y%m%d-%H%M%S).jsonl}
NOFILE=${NOFILE:-$(ulimit -Hn)}

ulimit -n "$NOFILE" || true
cargo build --release --quiet
mkdir -p "$(dirname "$OUT")"
SERVER=./target/release/phhtp
BENCH=./target/release/phhtp-bench
PIN_S=(); PIN_C=()
if command -v taskset >/dev/null; then PIN_S=(taskset -c "$SERVER_CPUS"); PIN_C=(taskset -c "$CLIENT_CPUS"); fi

wait_port() {
  for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  echo "server did not come up on port $1" >&2; return 1
}

# run_case <runtime> <workers|""> <conns> <note>
run_case() {
  local rt=$1 workers=$2 conns=$3 note=$4
  # Provenance: the accept backlog decides whether a connect herd stalls, and
  # containers have been seen resetting it mid-run. Record it with every row.
  note="$note somaxconn=$(cat /proc/sys/net/core/somaxconn 2>/dev/null || echo ?)"
  local args=(--runtime "$rt" --bind "127.0.0.1:$PORT")
  [[ -n "$workers" ]] && args+=(--workers "$workers")
  "${PIN_S[@]}" "$SERVER" "${args[@]}" 2>"bench/results/server-$rt.log" &
  local pid=$!
  trap 'kill $pid 2>/dev/null || true' EXIT
  wait_port "$PORT"
  echo "== $rt workers=${workers:-default} conns=$conns" >&2
  # Sample the server's footprint in the middle of the measured window, while
  # every connection is open (threads of `threads` exit as soon as the bench
  # closes its sockets, so sampling afterwards under-reports).
  local sample="bench/results/.sample-$$"
  ( sleep $((WARMUP + DURATION / 2)); awk '/VmRSS/ {r=$2} /Threads/ {t=$2} END {print r, t}' "/proc/$pid/status" > "$sample" 2>/dev/null ) &
  local sampler=$!
  local line
  line=$("${PIN_C[@]}" "$BENCH" run "http://127.0.0.1:$PORT$PATH_" -c "$conns" -d "$DURATION" -w "$WARMUP" --timeout 5 --reps "$REPS" --label "$rt" --note "$note" --json)
  wait "$sampler" 2>/dev/null || true
  local rss threads
  read -r rss threads < "$sample" || { rss=0; threads=0; }
  rm -f "$sample"
  python3 - "$line" "$rss" "$threads" "$workers" >> "$OUT" <<'PY'
import json, sys
d = json.loads(sys.argv[1]); d["rss_mb"] = round(int(sys.argv[2]) / 1024, 1); d["threads"] = int(sys.argv[3]); d["workers"] = sys.argv[4] or "default"
print(json.dumps(d))
PY
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  trap - EXIT
  sleep 1  # let TIME_WAIT/ports settle a little
}

if [[ "$MODE" == conns || "$MODE" == both ]]; then
  for rt in $RUNTIMES; do for c in $CONNS_LIST; do run_case "$rt" "" "$c" "sweep=conns"; done; done
fi
if [[ "$MODE" == workers || "$MODE" == both ]]; then
  for rt in $RUNTIMES; do
    case $rt in threads) continue;; esac
    for w in $WORKERS_LIST; do run_case "$rt" "$w" "$WORKERS_CONNS" "sweep=workers"; done
  done
fi

echo >&2; echo "results: $OUT" >&2
python3 - "$OUT" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
ms = lambda us: f"{us/1000:.2f}ms"
for sweep in ("sweep=conns", "sweep=workers"):
    rs = [r for r in rows if r.get("note", "").startswith(sweep)]
    if not rs: continue
    print(f"\n### {sweep}\n")
    print("| runtime | workers | conns | req/s | p50 | p90 | p99 | max | errors | RSS MB | threads |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rs:
        print(f"| {r['label']} | {r['workers']} | {r['connections']} | {r['rps']:.0f} | {ms(r['p50_us'])} | {ms(r['p90_us'])} | {ms(r['p99_us'])} | {ms(r['max_us'])} | {r['errors']+r['non_2xx']} | {r['rss_mb']} | {r['threads']} |")
PY
