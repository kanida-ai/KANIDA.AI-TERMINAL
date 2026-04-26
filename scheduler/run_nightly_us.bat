@echo off
REM KANIDA.AI Nightly Refresh — US market
setlocal
set PYTHONIOENCODING=utf-8
set ROOT=C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL
set PY=C:\Users\SPS\miniconda3\python.exe
set LOGDIR=%ROOT%\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set DT=%%a
set STAMP=%DT:~0,8%_%DT:~8,6%
set LOG=%LOGDIR%\nightly_us_%STAMP%.log
cd /d "%ROOT%"
echo [%date% %time%] Starting KANIDA US nightly >> "%LOG%" 2>&1
"%PY%" "%ROOT%\backend\scripts\run_nightly_worker.py" --market US >> "%LOG%" 2>&1
echo [%date% %time%] Done (exit %ERRORLEVEL%) >> "%LOG%" 2>&1
endlocal
