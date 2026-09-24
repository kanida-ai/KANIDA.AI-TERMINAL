@echo off
REM ============================================================
REM run_vortex_auth.bat
REM Wrapper invoked by the Scheduled Task "KanidaVortexAuth".
REM Runs the standalone Vortex auth worker in a FRESH process every 30 min.
REM Self-gates to weekday 06:00-15:30 IST; skips if the token is already valid.
REM Playwright browsers MUST be in the machine-wide path (Task Scheduler can't
REM see %LOCALAPPDATA%\ms-playwright). Output appended to logs\vortex_auth.log.
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "WORKER=%PROJECT_DIR%\scripts\vortex_auth_worker.py"
set "LOG_FILE=%PROJECT_DIR%\logs\vortex_auth.log"
set "PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo vortex_auth run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" "%WORKER%" >> "%LOG_FILE%" 2>&1

echo vortex_auth exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
