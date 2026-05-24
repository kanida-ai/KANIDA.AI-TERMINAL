@echo off
REM ============================================================
REM warm_cache.bat
REM
REM Polls localhost:8001 until the backend is ready, then fires
REM one request to /api/power/today/falcon-top-20 to populate
REM the Falcon Top 10 explainer cache.
REM
REM Spawned in parallel by start_backend.bat. Without this pre-
REM warm, the first user visit to /power/today after a reboot
REM waits 53 seconds for the cold computation (and often hits
REM Cloudflare 100s timeout). With it, every real user gets the
REM cache-hit path (about 8ms).
REM ============================================================

set "PROJECT_DIR=C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
REM Separate log file from backend.log — uvicorn holds backend.log locked
REM in append mode while running, so a second writer fails with sharing
REM violation on Windows.
set "LOG_FILE=%PROJECT_DIR%\logs\warmer.log"
set "READY_URL=http://127.0.0.1:8001/openapi.json"
set "WARM_URL=http://127.0.0.1:8001/api/power/today/falcon-top-20?universe=all500"

set /a count=0
:retry
set /a count+=1
if %count% gtr 60 goto :timeout
curl -sf -m 2 -o nul "%READY_URL%" >nul 2>&1
if errorlevel 1 (
    timeout /t 1 /nobreak >nul
    goto :retry
)
goto :ready

:timeout
echo [warm] %DATE% %TIME% backend never came up after 60s, bailing >> "%LOG_FILE%"
exit /b 1

:ready
echo [warm] %DATE% %TIME% backend up after %count%s, firing cache pre-warm... >> "%LOG_FILE%"
curl -s -m 120 -o nul "%WARM_URL%"
if errorlevel 1 (
    echo [warm] %DATE% %TIME% curl FAILED, cache NOT warmed >> "%LOG_FILE%"
    exit /b 1
) else (
    echo [warm] %DATE% %TIME% cache warmed successfully >> "%LOG_FILE%"
)
