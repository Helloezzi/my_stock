@echo off
setlocal

REM Update these values for your NAS environment if they change.
set "NAS_HOST=dasol@192.168.124.101"
set "NAS_PORT=2222"
set "REMOTE_DIR=/volume1/docker/my_stock"
set "BACKFILL_START=2026-02-28"
set "BACKFILL_END=2026-04-15"

if "%~1"=="" goto :help
if /I "%~1"=="deploy" goto :deploy
if /I "%~1"=="status" goto :status
if /I "%~1"=="logs" goto :logs
if /I "%~1"=="backfill" goto :backfill
if /I "%~1"=="shell" goto :shell
goto :help

:deploy
echo [deploy] git pull + docker compose up -d --build
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo docker compose up -d --build"
goto :eof

:status
echo [status] docker ps
ssh -t -p %NAS_PORT% %NAS_HOST% "sudo docker ps"
goto :eof

:logs
echo [logs] docker logs my-stock
ssh -t -p %NAS_PORT% %NAS_HOST% "sudo docker logs --tail 200 my-stock"
goto :eof

:backfill
echo [backfill] %BACKFILL_START% to %BACKFILL_END%
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && sudo docker exec -it my-stock python download_daily_fdr.py --start %BACKFILL_START% --end %BACKFILL_END%"
goto :eof

:shell
echo [shell] opening remote shell
ssh -p %NAS_PORT% %NAS_HOST%
goto :eof

:help
echo Usage:
echo   deploy_nas.bat deploy
echo   deploy_nas.bat status
echo   deploy_nas.bat logs
echo   deploy_nas.bat backfill
echo   deploy_nas.bat shell
echo.
echo Current settings:
echo   NAS_HOST=%NAS_HOST%
echo   NAS_PORT=%NAS_PORT%
echo   REMOTE_DIR=%REMOTE_DIR%
echo   BACKFILL_START=%BACKFILL_START%
echo   BACKFILL_END=%BACKFILL_END%
exit /b 1
