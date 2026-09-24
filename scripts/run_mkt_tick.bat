@echo off
REM ============================================================
REM run_mkt_tick.bat  (Scheduled Task "KanidaTickCapture")
REM LIVE tick-flow capture (KiteTicker websocket) -> mkt_trades_1min.
REM Fired every 30 min; self-gates 09:15->15:30 IST, then exits.
REM MultipleInstances=IgnoreNew so only one session runs; restarts if it dies.
REM Runs in parallel with KanidaOrderFlowPoller (poller = book, ticker = trades).
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "SCRIPT=%PROJECT_DIR%\scripts\mkt_tick_capture.py"
set "LOG_FILE=%PROJECT_DIR%\logs\mkt_tick.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo mkt_tick run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" -u "%SCRIPT%" >> "%LOG_FILE%" 2>&1

echo mkt_tick exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
