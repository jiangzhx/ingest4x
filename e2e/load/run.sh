#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOAD_DIR="$ROOT_DIR/e2e/load"
RUNTIME_DIR="${LOAD_RUNTIME_DIR:-$LOAD_DIR/runtime}"
RESULTS_DIR="${LOAD_RESULTS_DIR:-$RUNTIME_DIR/results}"
CONFIG_PATH="${LOAD_CONFIG:-$LOAD_DIR/ingest4x.load.toml}"

START_SERVER="${START_SERVER:-1}"
RESET_LOAD_STATE="${RESET_LOAD_STATE:-$START_SERVER}"
INGEST_URL="${INGEST_URL:-http://127.0.0.1:18091}"
ADMIN_URL="${ADMIN_URL:-http://127.0.0.1:18092}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-ingest4x-load}"
INGEST_TOKEN="${INGEST_TOKEN:-igx_loadtest_token}"
LOADTEST_APPID="${LOADTEST_APPID:-LOADTEST_APP}"
LOADTEST_PROJECT_NAME="${LOADTEST_PROJECT_NAME:-loadtest_app}"
LOADTEST_TARGET_ID="${LOADTEST_TARGET_ID:-loadtest_blackhole}"
LOADTEST_SINK_ID="${LOADTEST_SINK_ID:-loadtest_events}"
LOADTEST_PROCESSOR_KEY="${LOADTEST_PROCESSOR_KEY:-loadtest_blackhole_processor}"
LOADTEST_SINK_MODE="${LOADTEST_SINK_MODE:-ok}"
LOADTEST_DELAY_MS="${LOADTEST_DELAY_MS:-0}"
LOADTEST_RUN_ID="${LOADTEST_RUN_ID:-$(date +%Y%m%d%H%M%S)}"
WAIT_DRAIN_TIMEOUT="${WAIT_DRAIN_TIMEOUT:-60}"

export ADMIN_URL ADMIN_PASSWORD INGEST_TOKEN LOADTEST_APPID LOADTEST_PROJECT_NAME
export LOADTEST_TARGET_ID LOADTEST_SINK_ID LOADTEST_PROCESSOR_KEY
export LOADTEST_SINK_MODE LOADTEST_DELAY_MS LOADTEST_RUN_ID
export INGEST_URL

mkdir -p "$RUNTIME_DIR" "$RESULTS_DIR"

SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "$RESET_LOAD_STATE" == "1" ]]; then
  rm -rf "$RUNTIME_DIR"
  mkdir -p "$RUNTIME_DIR" "$RESULTS_DIR"
fi

wait_for_url() {
  local url="$1"
  local timeout="${2:-30}"
  local started
  started="$(date +%s)"
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( "$(date +%s)" - started >= timeout )); then
      echo "Timed out waiting for $url" >&2
      return 1
    fi
    sleep 1
  done
}

if [[ "$START_SERVER" == "1" ]]; then
  echo "Starting ingest4x with $CONFIG_PATH"
  (
    cd "$ROOT_DIR"
    cargo run --bin ingest4x -- server -c "$CONFIG_PATH"
  ) >"$RUNTIME_DIR/server.log" 2>&1 &
  SERVER_PID="$!"
fi

wait_for_url "$ADMIN_URL/healthz" 60

cat <<EOF >"$RESULTS_DIR/setup.json"
{
  "admin_url": "${ADMIN_URL}",
  "project": "${LOADTEST_PROJECT_NAME}",
  "ingest_token": "${INGEST_TOKEN}",
  "delivery_target": "${LOADTEST_TARGET_ID}",
  "event_sink": "${LOADTEST_SINK_ID}",
  "processor_script": "${LOADTEST_PROCESSOR_KEY}",
  "mode": "${LOADTEST_SINK_MODE}",
  "delay_ms": ${LOADTEST_DELAY_MS},
  "source": "seeded_or_preconfigured"
}
EOF
cat "$RESULTS_DIR/setup.json"
echo "Using preconfigured loadtest project/sink/processor resources."

if ! command -v k6 >/dev/null 2>&1; then
  echo "k6 is required to run the load scenario. Install it, then rerun this script." >&2
  echo "macOS: brew install k6" >&2
  exit 127
fi

curl -fsS "$ADMIN_URL/metrics" >"$RESULTS_DIR/metrics-before.prom"

echo "Running k6: rate=${LOAD_RATE:-100}/s duration=${LOAD_DURATION:-1m} mode=$LOADTEST_SINK_MODE delay_ms=$LOADTEST_DELAY_MS"
K6_EXIT_CODE=0
set +e
k6 run \
  --summary-export "$RESULTS_DIR/k6-summary.json" \
  "$LOAD_DIR/scenarios/blackhole.js"
K6_EXIT_CODE=$?
set -e
if [[ "$K6_EXIT_CODE" != "0" ]]; then
  echo "k6 exited with status $K6_EXIT_CODE; continuing to collect service metrics." >&2
fi

curl -fsS "$ADMIN_URL/metrics" >"$RESULTS_DIR/metrics-after.prom"

metric_value() {
  local file="$1"
  local metric="$2"
  awk -v metric="$metric" '$1 == metric { print $2; found = 1; exit } END { if (!found) print "" }' "$file"
}

if [[ "$LOADTEST_SINK_MODE" != "fail" ]]; then
  echo "Waiting for wal_replay_lag_lsn to drain"
  started="$(date +%s)"
  while true; do
    curl -fsS "$ADMIN_URL/metrics" >"$RESULTS_DIR/metrics-drain.prom"
    lag="$(metric_value "$RESULTS_DIR/metrics-drain.prom" "wal_replay_lag_lsn")"
    if [[ "${lag:-}" == "0" || "${lag:-}" == "0.0" ]]; then
      break
    fi
    if (( "$(date +%s)" - started >= WAIT_DRAIN_TIMEOUT )); then
      echo "Timed out waiting for WAL drain; wal_replay_lag_lsn=${lag:-unknown}" >&2
      exit 1
    fi
    sleep 1
  done
  cp "$RESULTS_DIR/metrics-drain.prom" "$RESULTS_DIR/metrics-after-drain.prom"
else
  echo "Skipping WAL drain wait because LOADTEST_SINK_MODE=fail is expected to keep lag."
fi

echo "Results:"
echo "  setup: $RESULTS_DIR/setup.json"
echo "  k6 summary: $RESULTS_DIR/k6-summary.json"
echo "  metrics before: $RESULTS_DIR/metrics-before.prom"
echo "  metrics after: $RESULTS_DIR/metrics-after.prom"
if [[ "$K6_EXIT_CODE" != "0" ]]; then
  exit "$K6_EXIT_CODE"
fi
