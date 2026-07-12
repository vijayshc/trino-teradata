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
export PLUGIN_ARTIFACT="${PLUGIN_ARTIFACT:-trino-teradata-export-0.1.0-SNAPSHOT.jar}"
