@echo off
setlocal

if exist "%~dp0deploy_nas.config.local.bat" (
    call "%~dp0deploy_nas.config.local.bat"
) else (
    call "%~dp0deploy_nas.config.example.bat"
)

if /I "%NAS_HOST%"=="your-user@your-nas-host" (
    echo Please create deploy_nas.config.local.bat from deploy_nas.config.example.bat and set your real NAS values.
    exit /b 1
)

if "%~1"=="" goto :help
if /I "%~1"=="quick" goto :quick
if /I "%~1"=="deploy" goto :deploy
if /I "%~1"=="fast" goto :fast
if /I "%~1"=="full" goto :full
if /I "%~1"=="fastfull" goto :fastfull
if /I "%~1"=="status" goto :status
if /I "%~1"=="logs" goto :logs
if /I "%~1"=="backfill" goto :backfill
if /I "%~1"=="shell" goto :shell
goto :help

:quick
echo [quick] git pull + rebuild/restart only ^(for UI/text/layout updates^)
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo %DOCKER_BIN% rm -f my-stock >/dev/null 2>&1 || true && sudo %DOCKER_BIN% compose up -d --build"
goto :eof

:deploy
echo [deploy] git pull + remove stale container + docker compose up -d --build
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo %DOCKER_BIN% rm -f my-stock >/dev/null 2>&1 || true && sudo %DOCKER_BIN% compose up -d --build"
goto :eof

:fast
echo [fast] git pull + recreate container without rebuild
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo %DOCKER_BIN% rm -f my-stock >/dev/null 2>&1 || true && sudo %DOCKER_BIN% compose up -d"
goto :eof

:full
echo [full] deploy + backfill + publish picks + logs
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo %DOCKER_BIN% rm -f my-stock >/dev/null 2>&1 || true && sudo %DOCKER_BIN% compose up -d --build && sudo %DOCKER_BIN% exec -i my-stock python download_daily_fdr.py --start %BACKFILL_START% --end %BACKFILL_END% && sudo %DOCKER_BIN% exec -i my-stock python scripts/build_today_picks.py --market ALL --limit 10 && sudo %DOCKER_BIN% logs --tail 120 my-stock"
goto :eof

:fastfull
echo [fastfull] fast restart + backfill + publish picks + logs
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && git pull && sudo %DOCKER_BIN% rm -f my-stock >/dev/null 2>&1 || true && sudo %DOCKER_BIN% compose up -d && sudo %DOCKER_BIN% exec -i my-stock python download_daily_fdr.py --start %BACKFILL_START% --end %BACKFILL_END% && sudo %DOCKER_BIN% exec -i my-stock python scripts/build_today_picks.py --market ALL --limit 10 && sudo %DOCKER_BIN% logs --tail 120 my-stock"
goto :eof

:status
echo [status] docker ps
ssh -t -p %NAS_PORT% %NAS_HOST% "sudo %DOCKER_BIN% ps"
goto :eof

:logs
echo [logs] docker logs my-stock
ssh -t -p %NAS_PORT% %NAS_HOST% "sudo %DOCKER_BIN% logs --tail 200 my-stock"
goto :eof

:backfill
echo [backfill] %BACKFILL_START% to %BACKFILL_END% + publish picks
ssh -t -p %NAS_PORT% %NAS_HOST% "cd %REMOTE_DIR% && sudo %DOCKER_BIN% exec -it my-stock python download_daily_fdr.py --start %BACKFILL_START% --end %BACKFILL_END% && sudo %DOCKER_BIN% exec -it my-stock python scripts/build_today_picks.py --market ALL --limit 10"
goto :eof

:shell
echo [shell] opening remote shell
ssh -p %NAS_PORT% %NAS_HOST%
goto :eof

:help
echo Usage:
echo   deploy_nas.bat quick
echo   deploy_nas.bat deploy
echo   deploy_nas.bat fast
echo   deploy_nas.bat full
echo   deploy_nas.bat fastfull
echo   deploy_nas.bat status
echo   deploy_nas.bat logs
echo   deploy_nas.bat backfill
echo   deploy_nas.bat shell
echo.
echo Config source:
if exist "%~dp0deploy_nas.config.local.bat" (
    echo   deploy_nas.config.local.bat
) else (
    echo   deploy_nas.config.example.bat ^(example values only^)
)
echo.
echo Current settings:
echo   NAS_HOST=%NAS_HOST%
echo   NAS_PORT=%NAS_PORT%
echo   REMOTE_DIR=%REMOTE_DIR%
echo   DOCKER_BIN=%DOCKER_BIN%
echo   BACKFILL_START=%BACKFILL_START%
echo   BACKFILL_END=%BACKFILL_END%
exit /b 1
