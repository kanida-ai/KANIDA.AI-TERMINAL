@echo off
REM ============================================================
REM start_backend.bat
REM
REM Launches the FastAPI backend on port 8001. Triggered by
REM Windows Task Scheduler at user logon (task name:
REM KanidaBackendAutoStart). Can also be double-clicked manually
REM to start the backend without rebooting.
REM
REM Spawns a parallel cache pre-warmer (warm_cache.bat) so the
REM first user visit to /power/today after a reboot is instant
REM instead of waiting 53 seconds for the cold computation.
REM
REM Logs go to logs\backend.log (appended on each start).
REM ============================================================

set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "UVICORN=C:\Users\SPS\anaconda3\Scripts\uvicorn.exe"
set "LOG_FILE=%PROJECT_DIR%\logs\backend.log"

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo Backend start at %DATE% %TIME% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

REM Spawn cache pre-warmer in parallel (headless, no extra window)
start /b /min "" cmd /c ""%PROJECT_DIR%\scripts\warm_cache.bat""

cd /d "%PROJECT_DIR%\backend"
"%UVICORN%" main:app --port 8001 --host 127.0.0.1 >> "%LOG_FILE%" 2>&1
