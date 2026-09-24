@echo off
REM ============================================================
REM run_flow_eod.bat  (Scheduled Task "KanidaFlowEODReport")
REM Runs the full-day flow report + validation after close.
REM 03:15 PDT = 15:45 IST (after the 15:30 close / 15:29 data captured).
REM Writes docs\ops\FLOW_EOD_<date>.xlsx and appends the text summary to
REM logs\flow_eod.log (so the summary can be surfaced at EOD).
REM ============================================================
setlocal
set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
set "PYTHON=C:\Users\SPS\anaconda3\python.exe"
set "SCRIPT=%PROJECT_DIR%\scripts\flow_eod_report.py"
set "LOG_FILE=%PROJECT_DIR%\logs\flow_eod.log"

echo.>> "%LOG_FILE%"
echo ============================================================>> "%LOG_FILE%"
echo flow_eod run at %DATE% %TIME%>> "%LOG_FILE%"

cd /d "%PROJECT_DIR%\scripts"
"%PYTHON%" -u "%SCRIPT%" >> "%LOG_FILE%" 2>&1
endlocal
