@echo off
REM ============================================================
REM run_auth_worker.bat
REM
REM Wrapper invoked by the Scheduled Task "KanidaZerodhaAuth".
REM Runs the standalone auth worker in a FRESH process every 30 min.
REM Fix 1 (2026-06-02): decouple Zerodha Playwright auth from the
REM long-lived backend that kept aging into the Chromium-launch failure.
REM Does NOT call conda activate (that pollutes PATH and breaks Playwright).
REM Output appended to logs\auth_worker.log.
REM ============================================================

setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "WORKER=%PROJECT_DIR%\scripts\auth_worker.py"
set "LOG_FILE=%PROJECT_DIR%\logs\auth_worker.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo auth_worker run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" "%WORKER%" >> "%LOG_FILE%" 2>&1

echo auth_worker exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal