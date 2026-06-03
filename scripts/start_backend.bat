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
set "ANACONDA=C:\Users\SPS\anaconda3"
set "UVICORN=%ANACONDA%\Scripts\uvicorn.exe"
set "LOG_FILE=%PROJECT_DIR%\logs\backend.log"

REM 2026-06-02: point Playwright at the machine-wide browser path so the
REM backend's boot preflight (and any in-process Playwright use) resolves the
REM Chromium binary regardless of how the backend was launched. The default
REM %LOCALAPPDATA%\ms-playwright is invisible to non-interactive logon
REM sessions (Task Scheduler auto-start) — the true root cause of the
REM recurring BROWSER_LAUNCH_FAILED. C:\ProgramData is visible to all sessions.
set "PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright"

REM 2026-05-27: REMOVED the `call activate.bat` line that lived here.
REM
REM My 2026-05-24 commit message claimed conda activation was REQUIRED for
REM Playwright. That was WRONG. Investigation today proved the opposite:
REM conda activate prepends anaconda3\Library\mingw-w64\bin to PATH, which
REM conflicts with Visual C++ runtime DLLs that Playwright's bundled
REM chrome-headless-shell.exe loads. Result: Playwright fails with the
REM misleading "Executable doesn't exist" error, even though the binary
REM is right there on disk.
REM
REM A direct uvicorn invocation WITHOUT conda activate works perfectly
REM (verified by spawning the same backend via raw `uvicorn main:app`
REM from a non-conda bash shell). uvicorn.exe in anaconda3\Scripts is a
REM self-contained shebang that doesn't need its env activated — it
REM resolves its bundled Python automatically.
REM
REM Keeping the `set ANACONDA=...` and `%UVICORN%` references below as-is.
REM Just removing the activate call.

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo Backend start at %DATE% %TIME% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

REM Spawn cache pre-warmer in parallel (headless, no extra window)
start /b /min "" cmd /c ""%PROJECT_DIR%\scripts\warm_cache.bat""

cd /d "%PROJECT_DIR%\backend"
"%UVICORN%" main:app --port 8001 --host 127.0.0.1 >> "%LOG_FILE%" 2>&1
