@echo off
REM ============================================================
REM run_orderflow_poller.bat
REM Wrapper invoked by the Scheduled Task "KanidaOrderFlowPoller".
REM Started every 30 min; self-gates to the IST market window (08:50-15:30
REM weekdays) and, once inside it, loops every minute until 15:30 then exits.
REM MultipleInstances=IgnoreNew means only one all-day loop runs; if it dies,
REM the next 30-min trigger restarts it. Output appended to logs\orderflow_poller.log.
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "SCRIPT=%PROJECT_DIR%\scripts\orderflow_poller.py"
set "LOG_FILE=%PROJECT_DIR%\logs\orderflow_poller.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo orderflow_poller run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" "%SCRIPT%" >> "%LOG_FILE%" 2>&1

echo orderflow_poller exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
