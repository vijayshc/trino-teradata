#!/usr/bin/env bash
# Shared env loading for project scripts.
# Prefer: env vars already set > dev/local.env > sensible defaults.

_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_DIR="${PROJECT_DIR:-$_ROOT}"

if [[ -f "$PROJECT_DIR/dev/local.env" ]]; then
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/dev/local.env"
fi

export PLUGIN_NAME="${PLUGIN_NAME:-teradata-export}"
# Trino-standard plugin artifact layout from packaging=trino-plugin
export PLUGIN_MODULE="${PLUGIN_MODULE:-plugin/trino-teradata}"
export PLUGIN_ARTIFACT_ID="${PLUGIN_ARTIFACT_ID:-trino-teradata}"
export PROJECT_VERSION="${PROJECT_VERSION:-479-1-SNAPSHOT}"

# Prefer ./mvnw when present (Maven 3.9+ required by trino-maven-plugin)
if [[ -x "$PROJECT_DIR/mvnw" ]]; then
  export MVN="${MVN:-$PROJECT_DIR/mvnw}"
else
  export MVN="${MVN:-mvn}"
fi
