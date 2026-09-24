@echo off
REM ============================================================
REM run_workflow_watchdog.bat
REM
REM Wrapper invoked by the Scheduled Task "KanidaWorkflowWatchdog".
REM Runs the workflow health watchdog in a FRESH process every ~15 min.
REM Mirrors run_auth_worker.bat: fresh short-lived process avoids the
REM aged-process Playwright/CIM pathology. Self-heals only the safe,
REM idempotent things (missing auth task); everything else is alert-only.
REM Does NOT call conda activate (that pollutes PATH and breaks Playwright).
REM Output appended to logs\workflow_watchdog.log.
REM ============================================================

setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "WORKER=%PROJECT_DIR%\scripts\workflow_watchdog.py"
set "LOG_FILE=%PROJECT_DIR%\logs\workflow_watchdog.log"

REM Machine-wide Playwright path (same rationale as run_auth_worker.bat): the
REM user-profile %LOCALAPPDATA%\ms-playwright is INVISIBLE to the non-interactive
REM Task Scheduler logon session. The watchdog does not launch Playwright itself,
REM but it imports backend modules that may probe it, so we set this for parity.
set "PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo workflow_watchdog run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" "%WORKER%" >> "%LOG_FILE%" 2>&1

echo workflow_watchdog exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
