#!/usr/bin/env bash
# Build the Trino plugin and package runtime dependencies.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

cd "$PROJECT_DIR"

echo "=== Building trino-teradata-direct ==="
echo "PROJECT_DIR=$PROJECT_DIR"
if [[ -n "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME=$JAVA_HOME"
fi

# Package plugin; skip integration tests by default
mvn -pl trino-plugin -am clean package -DskipTests -q

echo "Plugin JAR: $PROJECT_DIR/trino-plugin/target/$PLUGIN_ARTIFACT"
echo "Dependencies: $PROJECT_DIR/trino-plugin/target/dependency/"
echo "=== Build complete ==="
