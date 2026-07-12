#!/usr/bin/env bash
#
# Teradata BTEQ baseline stress test.
# Measures native Teradata query performance for comparison with the Trino path.
#
# Usage: ./teradata_stress_test.sh [concurrency] [duration_seconds]
#
# Required env (or dev/local.env via scripts/run_stress.sh bteq):
#   TD_HOST, TD_LOGON_USER, TD_LOGON_PASSWORD
# Optional:
#   TD_DATABASE (default: DBC)
#
set -euo pipefail

TD_HOST="${TD_HOST:-}"
TD_USER="${TD_LOGON_USER:-${TD_USER:-}}"
TD_PASSWORD="${TD_LOGON_PASSWORD:-${TD_PASSWORD:-}}"
TD_DATABASE="${TD_DATABASE:-DBC}"

CONCURRENCY="${1:-10}"
DURATION="${2:-60}"

if [[ -z "$TD_HOST" || -z "$TD_USER" || -z "$TD_PASSWORD" ]]; then
  echo "ERROR: Set TD_HOST, TD_LOGON_USER, and TD_LOGON_PASSWORD (no secrets in-repo)." >&2
  exit 1
fi

if ! command -v bteq >/dev/null 2>&1; then
  echo "ERROR: bteq not found on PATH." >&2
  exit 1
fi

QUERIES=(
  "SELECT 1"
  "SELECT * FROM dbc.tables SAMPLE 1"
  "SELECT * FROM dbc.databases SAMPLE 1"
  "SELECT databasename, tablename FROM dbc.tables SAMPLE 1"
  "SELECT * FROM dbc.columns SAMPLE 1"
  "SELECT databasename, tablename, tablekind FROM dbc.tables SAMPLE 1"
)

echo "======================================================================"
echo "  TERADATA BTEQ STRESS TEST (Baseline Performance)"
echo "======================================================================"
echo "  Host:         $TD_HOST"
echo "  User:         $TD_USER"
echo "  Database:     $TD_DATABASE"
echo "  Concurrency:  $CONCURRENCY threads"
echo "  Duration:     $DURATION seconds"
echo "  Queries:      ${#QUERIES[@]} query types"
echo "======================================================================"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

COUNTER_FILE="$TEMP_DIR/counter"
TIMES_FILE="$TEMP_DIR/times.log"
RUNNING_FILE="$TEMP_DIR/running"
LOCK_FILE="$TEMP_DIR/lock"
echo "0" > "$COUNTER_FILE"
touch "$TIMES_FILE"
echo "0" > "$RUNNING_FILE"

atomic_incr() {
  (
    flock -x 200
    val=$(cat "$1")
    echo $((val + 1)) > "$1"
  ) 200>"$LOCK_FILE"
}

atomic_add() {
  (
    flock -x 200
    val=$(cat "$1")
    echo $((val + $2)) > "$1"
  ) 200>"$LOCK_FILE"
}

run_worker() {
  local worker_id=$1
  local end_time=$2

  atomic_incr "$RUNNING_FILE"

  while [[ $(date +%s) -lt $end_time ]]; do
    local query_idx=$((RANDOM % ${#QUERIES[@]}))
    local query="${QUERIES[$query_idx]}"
    local bteq_script="$TEMP_DIR/query_${worker_id}_$$.bteq"

    cat > "$bteq_script" <<EOF
.LOGON $TD_HOST/$TD_USER,$TD_PASSWORD
DATABASE $TD_DATABASE;
.SET WIDTH 250
$query;
.LOGOFF
.QUIT
EOF

    local start_ms end_ms elapsed exit_code
    start_ms=$(date +%s%3N)
    set +e
    bteq < "$bteq_script" >/dev/null 2>&1
    exit_code=$?
    set -e
    end_ms=$(date +%s%3N)
    elapsed=$((end_ms - start_ms))
    rm -f "$bteq_script"

    if [[ $exit_code -eq 0 ]]; then
      echo "$elapsed" >> "$TIMES_FILE"
      atomic_incr "$COUNTER_FILE"
    fi
  done

  atomic_add "$RUNNING_FILE" -1
}

echo ""
echo "Starting $CONCURRENCY workers..."

END_TIME=$(($(date +%s) + DURATION))

for i in $(seq 1 "$CONCURRENCY"); do
  run_worker "$i" "$END_TIME" &
done

echo ""
echo "--- LIVE METRICS ---"
printf "%-8s %8s %8s %8s %10s %10s\n" "Time" "Total" "Success" "Running" "Avg(ms)" "Rate"

LAST_COUNT=0
LAST_TIME=$(date +%s)
sleep 2

while true; do
  sleep 5

  NOW=$(date +%s)
  ELAPSED=$((NOW - (END_TIME - DURATION)))
  CURRENT_COUNT=$(cat "$COUNTER_FILE" 2>/dev/null || echo 0)
  RUNNING=$(cat "$RUNNING_FILE" 2>/dev/null || echo 0)

  DELTA=$((CURRENT_COUNT - LAST_COUNT))
  DELTA_TIME=$((NOW - LAST_TIME))
  if [[ $DELTA_TIME -gt 0 ]]; then
    RATE=$(echo "scale=1; $DELTA / $DELTA_TIME" | bc 2>/dev/null || echo "0")
  else
    RATE="0"
  fi

  if [[ -s "$TIMES_FILE" ]]; then
    AVG=$(awk '{s+=$1; c++} END {if(c>0) printf "%.0f", s/c; else print 0}' "$TIMES_FILE" 2>/dev/null || echo 0)
  else
    AVG=0
  fi

  printf "%02d:%02d    %8d %8d %8d %10d %8s qps\n" \
    $((ELAPSED / 60)) $((ELAPSED % 60)) "$CURRENT_COUNT" "$CURRENT_COUNT" "$RUNNING" "$AVG" "$RATE"

  LAST_COUNT=$CURRENT_COUNT
  LAST_TIME=$NOW

  if [[ $RUNNING -eq 0 && $(date +%s) -ge $END_TIME ]]; then
    break
  fi
  if [[ $ELAPSED -gt $((DURATION + 30)) ]]; then
    break
  fi
done

wait

echo ""
echo "======================================================================"
echo "  BTEQ STRESS TEST FINAL REPORT"
echo "======================================================================"

TOTAL_QUERIES=$(cat "$COUNTER_FILE")

if [[ $TOTAL_QUERIES -gt 0 ]]; then
  SORTED_TIMES=$(sort -n "$TIMES_FILE" | grep -v '^$' || true)
  COUNT=$(echo "$SORTED_TIMES" | grep -c . || echo 0)

  if [[ $COUNT -gt 0 ]]; then
    P50_IDX=$((COUNT * 50 / 100)); [[ $P50_IDX -lt 1 ]] && P50_IDX=1
    P90_IDX=$((COUNT * 90 / 100)); [[ $P90_IDX -lt 1 ]] && P90_IDX=1
    P95_IDX=$((COUNT * 95 / 100)); [[ $P95_IDX -lt 1 ]] && P95_IDX=1
    P99_IDX=$((COUNT * 99 / 100)); [[ $P99_IDX -lt 1 ]] && P99_IDX=1
    P50=$(echo "$SORTED_TIMES" | sed -n "${P50_IDX}p")
    P90=$(echo "$SORTED_TIMES" | sed -n "${P90_IDX}p")
    P95=$(echo "$SORTED_TIMES" | sed -n "${P95_IDX}p")
    P99=$(echo "$SORTED_TIMES" | sed -n "${P99_IDX}p")
    MAX=$(echo "$SORTED_TIMES" | tail -1)
  else
    P50=0; P90=0; P95=0; P99=0; MAX=0
  fi

  QPS=$(echo "scale=2; $TOTAL_QUERIES / $DURATION" | bc)
  echo "  Duration:           ${DURATION}s"
  echo "  Concurrency:        $CONCURRENCY"
  echo "  Total Queries:      $TOTAL_QUERIES"
  echo "  Throughput:         $QPS queries/sec"
  echo ""
  echo "  Response Time Percentiles:"
  echo "    P50:  ${P50:-0} ms"
  echo "    P90:  ${P90:-0} ms"
  echo "    P95:  ${P95:-0} ms"
  echo "    P99:  ${P99:-0} ms"
  echo "    Max:  ${MAX:-0} ms"
else
  echo "  No queries completed successfully"
fi

echo "======================================================================"
echo ""
echo "Compare with Trino stress (./scripts/run_stress.sh test):"
echo "  - BTEQ fast / Trino slow  -> bottleneck in connector or data plane"
echo "  - BTEQ also slow          -> bottleneck in Teradata itself"
echo ""
