#!/usr/bin/env bash
# Build, deploy, and restart a multi-node Trino lab cluster.
#
# Usage:
#   ./scripts/build_deploy_restart.sh
#   ./scripts/build_deploy_restart.sh --restart-only
#
# Requires TRINO_HOME (and optionally TRINO_WORKER_1) via environment or dev/local.env
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

RESTART_ONLY=false
if [[ "${1:-}" == "--restart-only" ]]; then
  RESTART_ONLY=true
fi

if [[ -z "${TRINO_HOME:-}" ]]; then
  echo -e "${RED}TRINO_HOME is required${NC}" >&2
  exit 1
fi

echo -e "${GREEN}=== Multi-Node Trino Build, Deploy & Restart ===${NC}"
echo "PROJECT_DIR:  $PROJECT_DIR"
echo "Coordinator:  $TRINO_HOME"
echo "Worker 1:     ${TRINO_WORKER_1:-<none>}"
echo ""

if [[ "$RESTART_ONLY" == "false" ]]; then
  echo -e "${YELLOW}[1/4] Building...${NC}"
  "$SCRIPT_DIR/build.sh"
  echo -e "${YELLOW}[2/4] Deploying...${NC}"
  "$SCRIPT_DIR/deploy.sh"
else
  echo -e "${YELLOW}[SKIP] Build and deploy skipped (--restart-only).${NC}"
fi

echo -e "${YELLOW}[3/4] Restarting Coordinator...${NC}"
"$TRINO_HOME/bin/launcher" restart --etc-dir="$TRINO_HOME/etc" 2>/dev/null || \
  "$TRINO_HOME/bin/launcher" start --etc-dir="$TRINO_HOME/etc"
echo -e "${GREEN}Coordinator restarting...${NC}"

if [[ -n "${TRINO_WORKER_1:-}" ]]; then
  echo -e "${YELLOW}[4/4] Restarting Worker 1...${NC}"
  "$TRINO_HOME/bin/launcher" restart \
    --etc-dir="$TRINO_WORKER_1/etc" \
    --data-dir="$TRINO_WORKER_1/data" \
    --pid-file="$TRINO_WORKER_1/data/var/run/launcher.pid" \
    --launcher-log-file="$TRINO_WORKER_1/data/var/log/launcher.log" \
    --server-log-file="$TRINO_WORKER_1/data/var/log/server.log" 2>/dev/null || \
  "$TRINO_HOME/bin/launcher" start \
    --etc-dir="$TRINO_WORKER_1/etc" \
    --data-dir="$TRINO_WORKER_1/data" \
    --pid-file="$TRINO_WORKER_1/data/var/run/launcher.pid" \
    --launcher-log-file="$TRINO_WORKER_1/data/var/log/launcher.log" \
    --server-log-file="$TRINO_WORKER_1/data/var/log/server.log"
  echo -e "${GREEN}Worker 1 restarting...${NC}"
else
  echo -e "${YELLOW}[4/4] No TRINO_WORKER_1 configured; skipping worker restart.${NC}"
fi

echo ""
echo -e "${YELLOW}Waiting for cluster startup (30s)...${NC}"
sleep 30

COORD_STATUS=$("$TRINO_HOME/bin/launcher" status --etc-dir="$TRINO_HOME/etc" 2>/dev/null | grep -oE 'Running|Stopped' || echo "Unknown")
if [[ "$COORD_STATUS" == "Running" ]]; then
  echo -e "  Coordinator: ${GREEN}Running${NC}"
else
  echo -e "  Coordinator: ${RED}$COORD_STATUS${NC}"
fi

if [[ -n "${TRINO_WORKER_1:-}" ]]; then
  WORKER_STATUS=$("$TRINO_HOME/bin/launcher" status \
    --etc-dir="$TRINO_WORKER_1/etc" \
    --pid-file="$TRINO_WORKER_1/data/var/run/launcher.pid" 2>/dev/null | grep -oE 'Running|Stopped' || echo "Unknown")
  if [[ "$WORKER_STATUS" == "Running" ]]; then
    echo -e "  Worker 1:    ${GREEN}Running${NC}"
  else
    echo -e "  Worker 1:    ${RED}$WORKER_STATUS${NC}"
  fi
fi

echo ""
echo -e "${GREEN}=== Cluster Ready ===${NC}"
