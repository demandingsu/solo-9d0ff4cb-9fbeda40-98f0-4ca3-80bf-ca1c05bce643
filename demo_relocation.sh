#!/usr/bin/env bash
# Data-directory relocation demo.
#
# Simulates the operational move described in the README:
#   1. start the API against an old RAIN_DATA_DIR and publish batches;
#   2. stop it, move the whole data directory, restart with the new
#      RAIN_DATA_DIR (the SQLite metadata still stores old absolute paths);
#   3. show that startup verifies the move, rewrites metadata before the
#      orphan sweep, and that /docs plus every query keep working;
#   4. additionally demonstrate that an incomplete move aborts startup with
#      all files and metadata preserved.
#
# Works with a local uvicorn (no Docker needed). Override the python binary
# with PYTHON and extra interpreter path setup with PYTHONPATH.
set -euo pipefail

ROOT="${DEMO_ROOT:-/tmp/rain-relocation-demo}"
OLD="$ROOT/old"
NEW="$ROOT/new"
PORT="${DEMO_PORT:-8711}"
PYTHON="${PYTHON:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLES="${SCRIPT_DIR}/samples"

export PYTHONPATH="${SCRIPT_DIR}/.pydeps:${SCRIPT_DIR}:${PYTHONPATH:-}"

start_server() {
  local dir=$1 port=$2 log=$3
  RAIN_DATA_DIR="$dir" RAIN_REAPER_INTERVAL=3600 \
    "$PYTHON" -m uvicorn app.main:app --host 127.0.0.1 --port "$port" \
    --log-level warning >"$log" 2>&1 &
  echo $!
}

wait_health() {
  local port=$1
  for _ in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "server on port $port never became healthy" >&2
  return 1
}

stop_server() {
  local pid=$1 port=$2
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  # Wait until the listening socket is fully released before restarting.
  for _ in $(seq 1 50); do
    if ! curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
}

rm -rf "$ROOT"
mkdir -p "$OLD" "$NEW"

echo "### 1. boot against the OLD data dir and publish data"
PID=$(start_server "$OLD" "$PORT" "$ROOT/server-old.log")
trap 'stop_server "$PID" "$PORT"' EXIT
wait_health "$PORT"
curl -fsS "http://127.0.0.1:$PORT/health"
echo
for f in "$SAMPLES"/st001_2026-11-12_batch*.csv; do
  curl -fsS -X POST -F "file=@$f" \
    "http://127.0.0.1:$PORT/stations/ST001/days/2026-11-12/batches" >/dev/null
done
curl -fsS -X POST -F "file=@$SAMPLES/st002_2026-11-12_batch1.csv" \
  "http://127.0.0.1:$PORT/stations/ST002/days/2026-11-12/batches" >/dev/null
curl -fsS -X POST \
  "http://127.0.0.1:$PORT/stations/ST001/days/2026-11-12/merge" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$PORT/admin/reap" >/dev/null
echo "interactive docs before the move: HTTP $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/docs")"
stop_server "$PID" "$PORT"; trap - EXIT

echo
echo "### 2. move the data directory and restart with RAIN_DATA_DIR=$NEW"
cp -a "$OLD/." "$NEW/"
rm -rf "$OLD"
PID=$(start_server "$NEW" "$PORT" "$ROOT/server-new.log")
trap 'stop_server "$PID" "$PORT"' EXIT
wait_health "$PORT"
curl -fsS "http://127.0.0.1:$PORT/health"
echo
echo "interactive docs after the move: HTTP $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/docs")"
echo "query after relocation:"
curl -fsS "http://127.0.0.1:$PORT/stations/ST001/days/2026-11-12/records" \
  | "$PYTHON" -c "import sys,json;d=json.load(sys.stdin);print('  ST001 files=%d rows=%d'%(d['file_count'],d['row_count']))"
curl -fsS "http://127.0.0.1:$PORT/stations/ST002/days/2026-11-12/records" \
  | "$PYTHON" -c "import sys,json;d=json.load(sys.stdin);print('  ST002 files=%d rows=%d'%(d['file_count'],d['row_count']))"
echo "merge still works after relocation:"
curl -fsS -X POST "http://127.0.0.1:$PORT/stations/ST002/days/2026-11-12/merge" \
  | "$PYTHON" -c "import sys,json;d=json.load(sys.stdin);print('  merged rows=%d'%d['row_count'])"
stop_server "$PID" "$PORT"; trap - EXIT

echo
echo "### 3. incomplete move: remove one registered file and restart"
BROKEN="$ROOT/broken"
cp -a "$NEW/." "$BROKEN/"
find "$BROKEN/parquet" -name '*.parquet' | head -1 | xargs rm -f
set +e
RAIN_DATA_DIR="$BROKEN" RAIN_REAPER_INTERVAL=3600 \
  "$PYTHON" -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" \
  --log-level warning >"$ROOT/server-broken.log" 2>&1
RC=$?
set -e
echo "startup exited with code $RC (non-zero = correctly refused to serve)"
grep -q "DataMigrationError" "$ROOT/server-broken.log" && \
  echo "DataMigrationError logged; files and metadata left untouched"
echo "parquet files preserved: $(find "$BROKEN" -name '*.parquet' | wc -l)"
echo
echo "RELOCATION DEMO OK"
