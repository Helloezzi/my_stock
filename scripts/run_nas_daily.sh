#!/bin/sh
set -eu

PROJECT_DIR="/volume1/docker/my_stock"
DOCKER_BIN="/usr/local/bin/docker"
CONTAINER_NAME="my-stock"

cd "$PROJECT_DIR"

echo "[daily] $(date '+%Y-%m-%d %H:%M:%S') start"

$DOCKER_BIN compose up -d
$DOCKER_BIN exec -i "$CONTAINER_NAME" python download_daily_fdr.py --start "$(date '+%Y-%m-%d')" --end "$(date '+%Y-%m-%d')"
$DOCKER_BIN exec -i "$CONTAINER_NAME" python scripts/build_today_picks.py --market ALL --limit 10
$DOCKER_BIN exec -i "$CONTAINER_NAME" python scripts/check_daily_outputs.py

echo "[daily] $(date '+%Y-%m-%d %H:%M:%S') done"
