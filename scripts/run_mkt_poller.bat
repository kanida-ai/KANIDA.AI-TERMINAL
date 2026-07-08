@echo off
REM ============================================================
REM run_mkt_poller.bat  (invoked by Scheduled Task "KanidaOrderFlowPoller")
REM LIVE Kite market-data poller -> mkt_orderflow_1min cluster.
REM Fired every 30 min; self-gates to the IST window (waits 08:50->09:15,
REM polls every minute to 15:30, then exits). MultipleInstances=IgnoreNew so
REM only one all-day loop runs; if it dies the next 30-min trigger restarts it.
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "SCRIPT=%PROJECT_DIR%\scripts\mkt_poller.py"
set "LOG_FILE=%PROJECT_DIR%\logs\mkt_poller.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo mkt_poller run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" -u "%SCRIPT%" >> "%LOG_FILE%" 2>&1

echo mkt_poller exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
