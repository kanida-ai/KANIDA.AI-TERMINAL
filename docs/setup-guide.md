# Setup & Operations Runbook

The backend runs as a single FastAPI/uvicorn process on a Windows laptop. This
doc captures how to start it, deploy the frontend, and recover from the failure
modes we've actually hit. **Read the "Hard-won gotchas" section — it encodes
~9 incidents' worth of debugging.**

## Environment

- **Backend:** Python (Anaconda) at `C:\Users\SPS\anaconda3\python.exe`,
  uvicorn on `127.0.0.1:8001`.
- **Frontend:** Next.js 16 on Vercel (`www.kanida.ai`), deploys from branch
  `feat/power-user-portal`.
- **Tunnel:** Cloudflared named tunnel → `api.kanida.ai`.
- **Laptop timezone:** PDT. Market/IST logic is computed explicitly in code
  (never inferred from the wall clock).

## Secrets (`config/.env`, gitignored)

| Var | Used by |
|-----|---------|
| `KITE_API_KEY`, `KITE_API_SECRET` | Falcon trade + auth bot |
| `ZERODHA_USERNAME`, `ZERODHA_PASSWORD`, `ZERODHA_TOTP_SECRET` | Playwright auth bot |
| `POWER_ADMIN_EMAIL`, `POWER_ADMIN_SECRET` | Power User admin bootstrap |
| `POWER_JWT_SECRET` | JWT signing |
| `KANIDA_DB_PATH` | `data_freshness.py` (absolute path to the **legacy** db) |

Vercel env: `BACKEND_ORIGIN=https://api.kanida.ai`, plus `SITE_USER`/`SITE_PASS`
for the operator-surface Basic Auth.

## Starting the backend

```powershell
# Stop any running instance
Get-Process uvicorn,python -ErrorAction SilentlyContinue |
  Where-Object { $_.Path -like '*anaconda*' -or $_.ProcessName -eq 'uvicorn' } |
  Stop-Process -Force

# Start fresh (ALWAYS via this .bat — never from an Anaconda Prompt)
Start-Process "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts\start_backend.bat"
```

Wait ~20s, then confirm:
```powershell
Get-NetTCPConnection -LocalPort 8001 -State Listen
Invoke-WebRequest http://127.0.0.1:8001/openapi.json -UseBasicParsing | Select StatusCode
```

## Zerodha token auth

Auth runs as a **standalone Windows Scheduled Task** (`KanidaZerodhaAuth`), NOT
inside the backend. It fires every 30 min; `auth_worker.py` self-gates to the
weekday 06:00–16:30 IST window. Register/re-register with:
```powershell
& "...\scripts\register_auth_task.ps1"
```
Watch it:
```powershell
Get-Content "...\logs\auth_worker.log" -Tail 30 -Wait
```
On-demand refresh from the UI: `/power/admin` → "Refresh token now".

## Deploying the frontend

Push to `feat/power-user-portal`; Vercel auto-builds. (The default branch
`main` is stale and NOT what's deployed — confirmed historically.)

---

## Hard-won gotchas (DO NOT relearn these the hard way)

### 1. Playwright browsers MUST live in a machine-wide path
**Symptom:** `BROWSER_LAUNCH_FAILED` / "Executable doesn't exist" even though
the binary is on disk and launches fine when you test it interactively.

**Root cause (proven 2026-06-02):** the default `%LOCALAPPDATA%\ms-playwright`
folder is **invisible to the non-interactive logon session** that Windows uses
for Scheduled Tasks and auto-start. `os.path.exists()` on the binary returns
`True` interactively but `False` from the task context — same user, same env.

**Fix:** browsers are installed to `C:\ProgramData\ms-playwright` (visible to all
sessions). Every entry point sets `PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright`
(`run_auth_worker.bat`, `start_backend.bat`, `repair_playwright.bat`). If you ever
reinstall Playwright, install there:
```cmd
set PLAYWRIGHT_BROWSERS_PATH=C:\ProgramData\ms-playwright
C:\Users\SPS\anaconda3\python.exe -m playwright install chromium
```
Or just run `scripts\repair_playwright.bat` from a **fresh cmd.exe** (NOT an
Anaconda Prompt — conda activation pollutes PATH with mingw DLLs that break
Playwright).

### 2. Never `call activate.bat` / conda-activate before launching Playwright
Conda activation prepends `anaconda3\Library\mingw-w64\bin` to PATH, which
conflicts with the Visual C++ runtime DLLs Chromium loads. `start_backend.bat`
and the worker invoke `anaconda3\python.exe` directly for this reason.

### 3. The pipeline "done" check is by emitted signal_date, not run timestamp
A run that fires before 16:05 IST emits *yesterday's* signal_date. The gate
(`_signals_fresh_for_now`) compares `MAX(signal_date)` to the expected date for
the current IST window. `daily_signals` raises `StaleSignalsError` rather than
recording a false success. Don't "fix" this by reverting to a timestamp check.

### 4. Laptop sleep kills the backend process
If the laptop sleeps for hours, the uvicorn process can die (clean log cutoff,
no traceback) — the whole site then 503s through the tunnel. **Mitigation:
disable sleep + a watchdog task that restarts the backend if :8001 is dead.**
The real fix is moving off the laptop (see CHANGELOG "open items").

### 5. Recovery checklist when "EOD didn't fire" / token INVALID
1. `repair_playwright.bat` from a fresh cmd (rules out browser issues).
2. Restart the backend via `start_backend.bat`.
3. `Start-ScheduledTask -TaskName KanidaZerodhaAuth` → check `auth_worker.log`
   for `SUCCESS — token written`.
4. If past 16:05 IST and signals are stale, the backend boot catch-up fires the
   V7 pipeline automatically; or trigger it from `/power/admin` → "Run pipeline now".
5. Confirm `/power/today` shows today's `signal_date`.
