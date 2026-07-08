@echo off
REM ============================================================
REM run_mkt_backfill.bat  (Scheduled Task "KanidaMktBackfill")
REM Post-market historical 1-min OHLCV backfill -> mkt_ohlc_1min.
REM Runs daily at 04:00 PDT (= 16:30 IST, after the 15:30 close), so it never
REM contends with the live poller. Resumable+idempotent: first run does the deep
REM 2018+ backfill; later runs just append the latest day.
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "SCRIPT=%PROJECT_DIR%\scripts\mkt_backfill_ohlc.py"
set "LOG_FILE=%PROJECT_DIR%\logs\mkt_backfill.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo mkt_backfill run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\backend"
"%PYTHON%" -u "%SCRIPT%" >> "%LOG_FILE%" 2>&1

echo mkt_backfill exit code: %ERRORLEVEL%>> "%LOG_FILE%"
endlocal
