@echo off
REM Copy this file to deploy_nas.config.local.bat and fill in your real values.

set "NAS_HOST=your-user@your-nas-host"
set "NAS_PORT=22"
set "REMOTE_DIR=/your/deploy/path/my_stock"
set "DOCKER_BIN=/usr/local/bin/docker"
set "BACKFILL_START=2026-02-28"
set "BACKFILL_END=2026-04-15"
