#!/usr/bin/env bash
# Run the full runtime × endpoint matrix and print a Markdown table.
#
#   scripts/bench.sh                       # all runtimes, defaults
#   RUNTIMES="tokio mio" DURATION=5 scripts/bench.sh
#   WORKERS=1 scripts/bench.sh             # pin every runtime to 1 worker (emulates 1 vCPU)
#   TARGET=http://10.0.0.5:8080 scripts/bench.sh   # bench a remote server (does not start one)
#
# Env knobs: RUNTIMES, CONNS (64), SLEEP_CONNS (256), DURATION (10), WARMUP (2),
# PORT (8080), WORKERS (runtime default), OUT (bench/results/<timestamp>.jsonl),
# SCENARIOS (space separated subset of: plaintext json sign compute sleep).
set -euo pipefail
cd "$(dirname "$0")/.."

RUNTIMES=${RUNTIMES:-"threads pool mio tokio tokio-st uring"}
CONNS=${CONNS:-64}
SLEEP_CONNS=${SLEEP_CONNS:-256}
DURATION=${DURATION:-10}
WARMUP=${WARMUP:-2}
PORT=${PORT:-8080}
SCENARIOS=${SCENARIOS:-"plaintext json sign compute sleep"}
OUT=${OUT:-bench/results/$(date +%Y%m%d-%H%M%S).jsonl}
TARGET=${TARGET:-}
BENCH_THREADS=${BENCH_THREADS:-}

cargo build --release --quiet
mkdir -p "$(dirname "$OUT")"
SERVER=./target/release/phhtp
BENCH=./target/release/phhtp-bench

wait_port() {
  for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  echo "server did not come up on port $1" >&2
  return 1
}

run_scenario() {
  local label=$1 base=$2 name=$3
  local extra=()
  [[ -n "$BENCH_THREADS" ]] && extra+=(--threads "$BENCH_THREADS")
  case $name in
    plaintext) "$BENCH" run "$base/"              -c "$CONNS"       -d "$DURATION" -w "$WARMUP" --label "$label" --json "${extra[@]}" ;;
    json)      "$BENCH" run "$base/json"          -c "$CONNS"       -d "$DURATION" -w "$WARMUP" --label "$label" --json "${extra[@]}" ;;
    sign)      "$BENCH" run "$base/sign" -X POST --body-size 256 -c "$CONNS" -d "$DURATION" -w "$WARMUP" --label "$label" --json "${extra[@]}" ;;
    compute)   "$BENCH" run "$base/compute?n=2000" -c "$CONNS"      -d "$DURATION" -w "$WARMUP" --label "$label" --json "${extra[@]}" ;;
    sleep)     "$BENCH" run "$base/sleep?ms=5"    -c "$SLEEP_CONNS" -d "$DURATION" -w "$WARMUP" --label "$label" --json "${extra[@]}" ;;
    *) echo "unknown scenario $name" >&2; return 1 ;;
  esac
}

if [[ -n "$TARGET" ]]; then
  # Remote mode: the server is already running somewhere else.
  for sc in $SCENARIOS; do
    echo "== $TARGET $sc" >&2
    run_scenario "remote" "$TARGET" "$sc" | tee -a "$OUT"
  done
else
  for rt in $RUNTIMES; do
    args=(--runtime "$rt" --bind "127.0.0.1:$PORT")
    [[ -n "${WORKERS:-}" ]] && args+=(--workers "$WORKERS")
    "$SERVER" "${args[@]}" 2>"bench/results/server-$rt.log" &
    pid=$!
    trap 'kill $pid 2>/dev/null || true' EXIT
    wait_port "$PORT"
    for sc in $SCENARIOS; do
      echo "== $rt $sc" >&2
      run_scenario "$rt" "http://127.0.0.1:$PORT" "$sc" | tee -a "$OUT"
    done
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    trap - EXIT
  done
fi

echo >&2
echo "results: $OUT" >&2
"$BENCH" report "$OUT"
