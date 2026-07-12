#!/usr/bin/env bash
# Concurrent stress / bottleneck tools against a live Trino + Teradata lab.
#
# Usage:
#   ./scripts/run_stress.sh [test|analyze|escalate|quick|heavy|bteq] [options...]
#
# Examples:
#   ./scripts/run_stress.sh quick
#   ./scripts/run_stress.sh test --concurrency 50 --duration 120
#   ./scripts/run_stress.sh analyze connection
#   ./scripts/run_stress.sh bteq 10 60
#
# Env (or dev/local.env): TRINO_JDBC_URL, TRINO_USER, TD_HOST, TD_LOGON_USER, TD_LOGON_PASSWORD
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

cd "$PROJECT_DIR"

export TRINO_JDBC_URL="${TRINO_JDBC_URL:-jdbc:trino://localhost:8080/tdexport}"
export TRINO_USER="${TRINO_USER:-trino}"

MODULE="testing/trino-teradata-stress"
MODULE_DIR="$PROJECT_DIR/$MODULE"

build_module() {
  echo "Building stress module..."
  $MVN -pl "$MODULE" -am package -DskipTests -q
}

classpath() {
  local jar
  jar="$(ls -1 "$MODULE_DIR"/target/trino-teradata-stress-*.jar 2>/dev/null | head -1 || true)"
  if [[ -z "$jar" || ! -d "$MODULE_DIR/target/lib" ]]; then
    build_module
    jar="$(ls -1 "$MODULE_DIR"/target/trino-teradata-stress-*.jar | head -1)"
  fi
  echo "$jar:$MODULE_DIR/target/lib/*"
}

run_java() {
  local main="$1"
  shift
  local cp
  cp="$(classpath)"
  java -cp "$cp" \
    -Dtrino.jdbc.url="$TRINO_JDBC_URL" \
    -Dtrino.user="$TRINO_USER" \
    "$main" "$@"
}

COMMAND="${1:-test}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$COMMAND" in
  test)
    echo "Running stress test (URL=$TRINO_JDBC_URL USER=$TRINO_USER)..."
    run_java io.trino.stress.StressTestRunner "$@"
    ;;
  analyze)
    echo "Running bottleneck analyzer..."
    run_java io.trino.stress.BottleneckAnalyzer "$@"
    ;;
  escalate)
    echo "Running load escalation..."
    run_java io.trino.stress.BottleneckAnalyzer escalation "$@"
    ;;
  quick)
    echo "Quick smoke stress (5 workers, 10s)..."
    run_java io.trino.stress.StressTestRunner \
      --concurrency 5 --duration 10 --ramp-up 0 --query-type small "$@"
    ;;
  heavy)
    echo "Heavy load stress (100 workers, 60s)..."
    run_java io.trino.stress.StressTestRunner \
      --concurrency 100 --duration 60 --ramp-up 10 --query-type mixed "$@"
    ;;
  bteq)
    exec "$MODULE_DIR/teradata_stress_test.sh" "$@"
    ;;
  help|-h|--help)
    cat <<'EOF'
Usage: ./scripts/run_stress.sh <command> [options]

Commands:
  test       StressTestRunner (default); pass --concurrency N --duration N ...
  analyze    BottleneckAnalyzer; optional: connection|threadpool|buffer|serialize|escalation
  escalate   Load escalation only
  quick      5 workers, 10 seconds
  heavy      100 workers, 60 seconds
  bteq       Direct Teradata BTEQ baseline (needs bteq + TD_* env)

Env:
  TRINO_JDBC_URL  default jdbc:trino://localhost:8080/tdexport
  TRINO_USER      default trino
  TD_HOST / TD_LOGON_USER / TD_LOGON_PASSWORD / TD_DATABASE  (for bteq)
EOF
    ;;
  *)
    echo "Unknown command: $COMMAND (try: help)" >&2
    exit 1
    ;;
esac
