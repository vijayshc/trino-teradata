#!/usr/bin/env bash
# Deploy the trino-plugin packaging output into one or more Trino installations.
#
# Required:
#   TRINO_HOME              path to Trino server (coordinator)
#   TERADATA_JDBC_JAR       path to terajdbc4.jar (proprietary)
# Optional:
#   TRINO_WORKER_1          path to worker installation root
#   EXTRA_TRINO_HOMES       space-separated additional roots
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

if [[ -z "${TRINO_HOME:-}" ]]; then
  echo "ERROR: TRINO_HOME is not set. Example: export TRINO_HOME=/opt/trino" >&2
  exit 1
fi
if [[ -z "${TERADATA_JDBC_JAR:-}" || ! -f "${TERADATA_JDBC_JAR}" ]]; then
  echo "ERROR: TERADATA_JDBC_JAR must point to terajdbc4.jar" >&2
  exit 1
fi

PLUGIN_SRC="$PROJECT_DIR/$PLUGIN_MODULE/target/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}"
if [[ ! -d "$PLUGIN_SRC" ]]; then
  echo "Plugin package not found; building first..."
  "$SCRIPT_DIR/build.sh"
fi

deploy_one() {
  local root="$1"
  local label="${2:-$root}"
  local plugin_dir="$root/plugin/$PLUGIN_NAME"
  echo "Deploying to $label -> $plugin_dir"
  mkdir -p "$plugin_dir"
  # Clean previous install (avoid stale deps)
  find "$plugin_dir" -maxdepth 1 -type f -name '*.jar' -delete 2>/dev/null || true
  cp "$PLUGIN_SRC/"*.jar "$plugin_dir/"
  cp "$TERADATA_JDBC_JAR" "$plugin_dir/terajdbc4.jar"
  echo "  JARs installed: $(ls -1 "$plugin_dir"/*.jar 2>/dev/null | wc -l)"
}

deploy_one "$TRINO_HOME" "coordinator($TRINO_HOME)"

if [[ -n "${TRINO_WORKER_1:-}" ]]; then
  deploy_one "$TRINO_WORKER_1" "worker1($TRINO_WORKER_1)"
fi

if [[ -n "${EXTRA_TRINO_HOMES:-}" ]]; then
  for root in $EXTRA_TRINO_HOMES; do
    deploy_one "$root" "extra($root)"
  done
fi

echo "=== Deploy complete ==="
echo "Restart Trino nodes to load the plugin."
