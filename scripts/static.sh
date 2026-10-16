#!/usr/bin/env bash
# Static files: the runtimes that read files (threads, mio, tokio, uring-async)
# against nginx (sendfile) on the same files, same box, same load generator.
#
#   scripts/static.sh                               # 4 KiB, 64 KiB, 1 MiB files
#   RUNTIMES="tokio uring-async" SIZES="65536" scripts/static.sh
#   ZC_AB=1 scripts/static.sh                       # also uring-async with SEND_ZC off
#
# Server pinned to SERVER_CPUS, client to CLIENT_CPUS (taskset), as in scale.sh.
# nginx: worker_processes = 2, sendfile on, keepalive, access_log off; skipped
# if the binary is not installed.
set -euo pipefail
cd "$(dirname "$0")/.."

RUNTIMES=${RUNTIMES:-"threads mio tokio uring-async"}
SIZES=${SIZES:-"4096 65536 1048576"}
CONNS=${CONNS:-64}
DURATION=${DURATION:-5}
WARMUP=${WARMUP:-2}
REPS=${REPS:-2}
PORT=${PORT:-8080}
SERVER_CPUS=${SERVER_CPUS:-0-1}
CLIENT_CPUS=${CLIENT_CPUS:-2-3}
OUT=${OUT:-bench/results/static-$(date +%Y%m%d-%H%M%S).jsonl}
ZC_AB=${ZC_AB:-0}
NGINX=${NGINX:-$(command -v nginx || true)}
DIR=$PWD/bench/static

cargo build --release --quiet
mkdir -p "$(dirname "$OUT")" "$DIR"
for sz in $SIZES; do
  f="$DIR/f$sz.bin"
  [[ -f $f && $(stat -c %s "$f") == "$sz" ]] || head -c "$sz" /dev/urandom > "$f"
done
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

bench() { # <label> <note> <path>
  local label=$1 note=$2 path=$3
  "${PIN_C[@]}" "$BENCH" run "http://127.0.0.1:$PORT$path" -c "$CONNS" -d "$DURATION" -w "$WARMUP" --reps "$REPS" \
    --label "$label" --json --note "$note" | tee -a "$OUT"
}

run_phhtp() { # <runtime> <label> <note> [env...]
  local rt=$1 label=$2 note=$3; shift 3
  env "$@" "${PIN_S[@]}" "$SERVER" --runtime "$rt" --bind "127.0.0.1:$PORT" --static-dir "$DIR" 2>"bench/results/server-$rt.log" &
  local pid=$!
  trap 'kill $pid 2>/dev/null || true' EXIT
  wait_port "$PORT"
  for sz in $SIZES; do
    echo "== $label static $sz" >&2
    bench "$label" "static=$sz $note" "/static/f$sz.bin"
  done
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
}

for rt in $RUNTIMES; do
  run_phhtp "$rt" "$rt" "zc=default"
done
if [[ $ZC_AB == 1 ]]; then
  run_phhtp uring-async "uring-async-nozc" "zc=off" PHHTP_ZC_MIN=0
fi

if [[ -n $NGINX ]]; then
  prefix=$(mktemp -d)
  mkdir -p "$prefix/logs" "$prefix/tmp"
  cat > "$prefix/nginx.conf" <<CONF
daemon off;
worker_processes 2;
pid $prefix/nginx.pid;
error_log $prefix/logs/error.log warn;
events { worker_connections 65535; multi_accept on; }
http {
  access_log off;
  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  keepalive_requests 1000000;
  open_file_cache max=1000 inactive=60s;
  client_body_temp_path $prefix/tmp; proxy_temp_path $prefix/tmp; fastcgi_temp_path $prefix/tmp; uwsgi_temp_path $prefix/tmp; scgi_temp_path $prefix/tmp;
  server {
    listen 127.0.0.1:$PORT reuseport backlog=65535;
    root $DIR;
    location /static/ { alias $DIR/; }
    location /json { return 200 '{"message":"Hello, World!"}'; default_type application/json; }
  }
}
CONF
  "${PIN_S[@]}" "$NGINX" -c "$prefix/nginx.conf" -p "$prefix" 2>"bench/results/server-nginx.log" &
  pid=$!
  trap 'kill $pid 2>/dev/null || true' EXIT
  wait_port "$PORT"
  for sz in $SIZES; do
    echo "== nginx static $sz" >&2
    bench nginx "static=$sz nginx=$($NGINX -v 2>&1 | sed 's/.*nginx\///')" "/static/f$sz.bin"
  done
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
  rm -rf "$prefix"
fi

echo >&2; echo "results: $OUT" >&2
"$BENCH" report "$OUT"
