#!/usr/bin/env bash
# Build the Trino plugin using official trino-plugin packaging.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

cd "$PROJECT_DIR"

echo "=== Building trino-teradata-direct ==="
echo "PROJECT_DIR=$PROJECT_DIR"
echo "JAVA_HOME=${JAVA_HOME:-<unset>}"
echo "MVN=$MVN"

# Package plugin module (runs unit tests for that module)
$MVN -pl "$PLUGIN_MODULE" -am clean package -DskipITs

PLUGIN_DIR="$PROJECT_DIR/$PLUGIN_MODULE/target/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}"
PLUGIN_ZIP="$PROJECT_DIR/$PLUGIN_MODULE/target/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}.zip"

if [[ ! -d "$PLUGIN_DIR" ]]; then
  echo "ERROR: Expected plugin directory not found: $PLUGIN_DIR" >&2
  echo "Contents of target:" >&2
  ls -la "$PROJECT_DIR/$PLUGIN_MODULE/target" >&2 || true
  exit 1
fi

echo "Plugin directory: $PLUGIN_DIR"
echo "Plugin ZIP:       $PLUGIN_ZIP"
echo "JARs packaged:    $(ls -1 "$PLUGIN_DIR"/*.jar 2>/dev/null | wc -l)"
echo "=== Build complete ==="
