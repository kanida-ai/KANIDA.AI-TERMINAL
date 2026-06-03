@echo off
REM ============================================================
REM repair_playwright.bat
REM
REM One-click recovery for the "Playwright Chromium can't launch"
REM failure mode that's hit us 8+ times. Run this when /power/admin
REM shows a BROWSER_LAUNCH_FAILED diagnostic.
REM
REM CRITICAL — launch from a FRESH cmd.exe shell, NOT from:
REM   - Anaconda Prompt          (pollutes PATH with mingw-w64\bin)
REM   - Anaconda PowerShell      (same)
REM   - VS Code integrated term  (often inherits IDE env)
REM   - Git Bash                 (POSIX path conversion can confuse)
REM
REM Open Start menu → type "cmd" → Enter → run this from there.
REM ============================================================

setlocal
set "ANACONDA=C:\Users\SPS\anaconda3"
set "PYTHON=%ANACONDA%\python.exe"
set "PIP=%ANACONDA%\Scripts\pip.exe"

REM 2026-06-02: install browsers to the MACHINE-WIDE path, not the user
REM profile. %LOCALAPPDATA%\ms-playwright is invisible to the non-interactive
REM Task Scheduler logon session (proven root cause of recurring
REM BROWSER_LAUNCH_FAILED). C:\ProgramData is visible to all sessions.
set "PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright"

echo.
echo ============================================================
echo  Playwright repair  --  %DATE% %TIME%
echo ============================================================
echo.

REM Sanity: is this shell polluted?
echo Checking shell for conda PATH pollution...
echo %PATH% | findstr /I /C:"anaconda3\Library\mingw-w64\bin" >nul
if not errorlevel 1 (
    echo.
    echo   X  WARNING: your PATH contains anaconda3\Library\mingw-w64\bin.
    echo   X  That's the conda activation pollution that causes the bug.
    echo   X
    echo   X  Close this shell. Open a FRESH cmd.exe directly from Start menu.
    echo   X  Do NOT open an Anaconda Prompt. Re-run this script there.
    echo.
    pause
    exit /b 1
)
echo   ok  PATH is clean.
echo.

echo Step 1: install / reinstall the pinned Playwright python package...
echo.
"%PIP%" install --upgrade --force-reinstall "playwright==1.59.0"
if errorlevel 1 (
    echo   X  pip install failed. Check your internet + pip credentials.
    pause
    exit /b 1
)
echo.

echo Step 2: download Chromium binaries for this Playwright version...
echo (This is a ~150 MB download. Allow Windows Defender to scan it.)
echo.
"%PYTHON%" -m playwright install chromium
if errorlevel 1 (
    echo   X  Chromium install failed. Check network + disk space.
    pause
    exit /b 1
)
echo.

echo Step 3: verify the browser can actually launch...
echo.
REM Run the verify probe as a clean child Python — same probe the
REM playwright_preflight module uses at boot, so a pass here means the
REM backend's auth bot will also work.
"%PYTHON%" "%~dp0_verify_playwright.py"
if errorlevel 1 (
    echo.
    echo   X  Browser launch FAILED. Manual investigation needed:
    echo   X    1. Open admin panel - check browser_health.error_summary
    echo   X    2. Look in C:\Users\SPS\AppData\Local\ms-playwright for the binary
    echo   X    3. Check Windows Defender quarantine
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Repair complete.
echo.
echo  Next steps:
echo    1. RESTART the backend so it picks up the fresh install:
echo         taskkill /F /IM uvicorn.exe ^&^& taskkill /F /IM python.exe
echo         scripts\start_backend.bat
echo    2. Watch /power/admin - browser_health should flip to "available: true"
echo    3. Click "Refresh token now" to fire a manual auth cycle
echo ============================================================
echo.
pause
endlocal
