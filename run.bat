@echo off
setlocal

REM 이 배치파일 위치로 작업 폴더 이동
cd /d "%~dp0"

REM (선택) UTF-8 출력
chcp 65001 >nul

REM Streamlit 실행
streamlit run ".\app.py"

endlocal