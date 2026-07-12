#!/usr/bin/env bash
# Run the integration test suite against a live Trino + Teradata environment.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

cd "$PROJECT_DIR"

export TRINO_JDBC_URL="${TRINO_JDBC_URL:-jdbc:trino://localhost:8080/tdexport/trinoexport}"
export TRINO_USER="${TRINO_USER:-trino}"
export TRINO_SERVER_LOG="${TRINO_SERVER_LOG:-$HOME/tdconnector/trino_server/trino-server-479/data/var/log/server.log}"

MVN_ARGS=(
  -pl testing/trino-teradata-tests
  -am
  test
  -DskipITs=false
  "-Dtrino.jdbc.url=${TRINO_JDBC_URL}"
  "-Dtrino.user=${TRINO_USER}"
  "-Dtrino.server.log=${TRINO_SERVER_LOG}"
)

if [[ -n "${1:-}" ]]; then
  TEST="$1"
  if [[ "$TEST" != *.* ]]; then
    TEST="io.trino.tests.tdexport.$TEST"
  fi
  echo "Running test: $TEST"
  $MVN "${MVN_ARGS[@]}" "-Dtest=$TEST"
else
  echo "Running full integration suite..."
  echo "  TRINO_JDBC_URL=$TRINO_JDBC_URL"
  echo "  TRINO_USER=$TRINO_USER"
  echo "  TRINO_SERVER_LOG=$TRINO_SERVER_LOG"
  $MVN "${MVN_ARGS[@]}"
fi
