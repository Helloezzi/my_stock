@echo off
setlocal

set "PS_SCRIPT=%~dp0release_nas.ps1"

if not exist "%PS_SCRIPT%" (
    echo release_nas.ps1 not found.
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%" %*
exit /b %ERRORLEVEL%
