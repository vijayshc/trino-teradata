#!/usr/bin/env bash
# Generate a BTEQ script from the template and optionally run it.
#
# Required env:
#   TD_HOST, TD_LOGON_USER, TD_LOGON_PASSWORD
# Optional:
#   TD_UDF_DATABASE (default: TrinoExport)
#   UDF_SRC_DIR (default: <repo>/teradata-udf)
#   RUN_BTEQ=1 to execute immediately (requires bteq on PATH)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

: "${TD_HOST:?Set TD_HOST}"
: "${TD_LOGON_USER:?Set TD_LOGON_USER}"
: "${TD_LOGON_PASSWORD:?Set TD_LOGON_PASSWORD}"

UDF_SRC_DIR="${UDF_SRC_DIR:-$PROJECT_DIR/teradata-udf}"
TD_UDF_DATABASE="${TD_UDF_DATABASE:-TrinoExport}"
OUT="$PROJECT_DIR/scripts/register.generated.bteq"

# Resolve absolute path (Teradata UDF EXTERNAL NAME needs a path the TD node can read)
UDF_SRC_DIR="$(cd "$UDF_SRC_DIR" && pwd)"

sed \
  -e "s|__TD_HOST__|${TD_HOST}|g" \
  -e "s|__TD_LOGON_USER__|${TD_LOGON_USER}|g" \
  -e "s|__TD_LOGON_PASSWORD__|${TD_LOGON_PASSWORD}|g" \
  -e "s|__UDF_DATABASE__|${TD_UDF_DATABASE}|g" \
  -e "s|__UDF_SRC_DIR__|${UDF_SRC_DIR}|g" \
  "$PROJECT_DIR/scripts/register.bteq.template" > "$OUT"

echo "Generated: $OUT"
echo "UDF sources: $UDF_SRC_DIR"

if [[ "${RUN_BTEQ:-0}" == "1" ]]; then
  if ! command -v bteq >/dev/null 2>&1; then
    echo "ERROR: bteq not found on PATH" >&2
    exit 1
  fi
  echo "Running bteq..."
  bteq < "$OUT"
  echo "UDF registration complete."
else
  echo "Review the generated file, then run:"
  echo "  RUN_BTEQ=1 $0"
  echo "  # or: bteq < $OUT"
fi
