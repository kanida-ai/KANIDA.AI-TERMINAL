# Kanida.AI — Deploy (M1 Infra)

This folder holds **host configuration** for running the backend off the laptop
on always-on infra (Railway or a Linux VPS), with **Supabase Postgres** as the
App DB. The full step-by-step operator procedure is in
[`docs/launch/RUNBOOK_deploy.md`](../docs/launch/RUNBOOK_deploy.md). This README
is the index + the bits that belong next to the host config.

> The working **`Dockerfile`**, **`railway.json`**, **`entrypoint.sh`**, and
> **`entrypoint_cron.sh`** live at the **repo root** — they are the source of
> truth and are NOT duplicated here. This folder only adds what's missing for
> the hosted-Postgres end state.

---

## Files in this folder

| File | Purpose |
|---|---|
| `README.md` | This index. |
| `Procfile` | Process definitions for VPS / non-Railway hosts (Heroku-style). On Railway the root `railway.json` + `entrypoint.sh` are used instead. |
| `CLOUDFLARE_TUNNEL_SETUP.md` | (Pre-existing) Laptop-tunnel fallback — **superseded** by hosted deploy. Kept for reference only; do NOT use for the Stage-1 launch. |
| `verify-deploy.sh` | (Pre-existing) Post-deploy smoke checks. |
| `cloudflared/` | (Pre-existing) Tunnel config — fallback only. |

---

## Architecture (end state)

```
                ┌─────────────────────────── HOST (Railway / VPS, always-on) ──────────────────┐
   user ──────► │  web:  uvicorn main:app   ──► reads/writes ──►  Supabase Postgres (App DB)   │
                │  cron: falcon.jobs.*       (daily 16:30–16:35 IST / weekly Sun)               │
                │  auth: zerodha token refresh (host scheduled job, headless Playwright)        │
                └───────────────────────────────────────────────────────────────────────────────┘
                                                          ▲
                                                          │  weekly pattern publish (B4)
                ┌─────────────────────────── YOUR MACHINE (offline) ───────────────────────────┐
                │  universe_engine/data/db/kanida_universe.db  (14G R&D warehouse)              │
                │  NEVER in a request path. Mines patterns, publishes a small set to App DB.    │
                └───────────────────────────────────────────────────────────────────────────────┘
```

The single switch that flips SQLite → Postgres is the **`DATABASE_URL`** env
var (see `backend/db.py` → `IS_POSTGRES`). With it set, every `get_conn()` call
goes to Supabase. Without it, the app uses bundled SQLite (local dev only).

---

## Three processes the host must run

1. **web** — the API. `cd /app/backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
   (this is what root `entrypoint.sh` already does).
2. **cron** — the daily V7 pipeline. Root `entrypoint_cron.sh` runs one job per
   invocation via `FALCON_JOB`. Schedules (UTC, since hosts use UTC):
   - `daily_data_refresh` → `30 11 * * 1-5` (16:30 IST)
   - `daily_features` → `32 11 * * 1-5` (16:32 IST)
   - `daily_signals` → `35 11 * * 1-5` (16:35 IST)
   - `weekly_remine` → `30 12 * * 0` (Sun 18:00 IST)
3. **auth refresh** — Zerodha access-token refresh, HEADLESS. On the laptop this
   was a Windows Scheduled Task; on the host it becomes a scheduled job that runs
   `scripts/auth_worker.py` (see RUNBOOK §6). Requires Playwright Chromium +
   `pyotp` in the image.

> On Railway: web = the main service (root `railway.json`); cron = separate cron
> services each with `FALCON_JOB` set; auth refresh = a cron service running the
> auth worker (or rely on the in-process `auth_scheduler` thread the web service
> already starts — see RUNBOOK §6 for the trade-off).

---

## Image prerequisites for headless token refresh (IMPORTANT)

`backend/services/zerodha_auto_auth.py` drives Playwright Chromium and uses
`pyotp`. Neither is in the base image yet. M1 adds them:

- `requirements.txt` must include `playwright` and `pyotp`.
- The Docker image must run `playwright install --with-deps chromium` so the
  headless browser + its shared libraries exist on Linux.

Without these, `auth_scheduler` returns `BROWSER_LAUNCH_FAILED` every cycle and
the token never refreshes (the system's preflight will flag it, but the host
still can't auth). See RUNBOOK §6 + §9.

---

## What stays on the laptop

- The 14G R&D warehouse `universe_engine/data/db/kanida_universe.db` and all
  mining / backtest / winB scratch DBs. Set `POWER_RND_DB_PATH` to it **on the
  laptop only**; it is never set on the host.
- The weekly publish job that pushes the promoted-pattern set into the App DB.
