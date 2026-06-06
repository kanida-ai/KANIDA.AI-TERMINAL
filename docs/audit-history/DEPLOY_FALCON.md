# Falcon V7.1 — Production Deployment Guide

Step-by-step to take Falcon from local to Railway + Vercel. Estimated time:
~45 minutes if Railway/Vercel projects already exist.

## Pre-requisites

- Railway project (existing for legacy KANIDA backend) with persistent volume on `/app/data`
- Vercel project (existing for KANIDA frontend)
- Supabase project at `https://supabase.com/dashboard/project/ientuxgwupzckepcomxw` (auth only — DB stays SQLite for now)
- Kite Connect credentials in Railway env vars (already configured for legacy)

## 1 · Verify the local repo

```bash
cd "Kanida.ai Terminal Quant Intelligence Engine"

# Falcon DB must exist (1.1 GB). It's the offline-mined source of truth.
ls -la universe_engine/data/db/kanida_universe.db

# Spot check: tables present
python -c "
import sqlite3
con = sqlite3.connect('universe_engine/data/db/kanida_universe.db')
for t in ('falcon_promoted_patterns','falcon_features','falcon_outcomes','ohlc_daily','ohlc_weekly'):
    n = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    print(f'{t:30s} {n:,}')
"
# Expected:
# falcon_promoted_patterns       846
# falcon_features            391,287
# falcon_outcomes            527,410
# ohlc_daily                 415,000+
# ohlc_weekly                102,442
```

## 2 · Local smoke test

```bash
cd backend
uvicorn main:app --reload --port 8001

# In another terminal:
curl http://localhost:8001/api/falcon/admin/status
curl http://localhost:8001/api/falcon/patterns/stats
# /signals/today returns 404 until we run daily_signals once.

cd ..
python -m backend.falcon.jobs.daily_signals
# Now /signals/today returns the latest picks.
```

Frontend (separate terminal):
```bash
cd frontend
npm install
npm run dev
# Visit http://localhost:3000/falcon
```

## 3 · Push to Railway

```bash
git add backend/falcon frontend/app/falcon frontend/lib/falcon-api.ts \
        Dockerfile entrypoint.sh entrypoint_cron.sh railway.json railway.cron.json \
        backend/falcon/README.md DEPLOY_FALCON.md backend/main.py
git commit -m "Falcon V7.1: production backend, frontend pages, cron jobs"
git push
```

Railway auto-builds. Watch the build log for:
- `[entrypoint] Falcon DB OK (...)` ← Falcon DB seeded
- `[entrypoint] Falcon schema extensions applied`
- `Application startup complete` ← uvicorn ready

## 4 · Bundle size sanity

The Docker image now bundles `kanida_universe.db` (~1.1 GB). Railway image
build limits and image storage cost will roughly double. Verify the build
completes; if it OOMs, swap to mounting the DB on a separate volume.

## 5 · Configure Railway cron services

In Railway dashboard: **New Service → Cron**

Repeat 4 times with these env-var sets (use `railway.cron.json` as the deploy config for each):

| Service name | FALCON_JOB | Schedule (UTC) | IST equivalent |
|---|---|---|---|
| falcon-cron-data    | `daily_data_refresh` | `30 11 * * 1-5` | Mon-Fri 16:30 |
| falcon-cron-features| `daily_features`     | `32 11 * * 1-5` | Mon-Fri 16:32 |
| falcon-cron-signals | `daily_signals`      | `35 11 * * 1-5` | Mon-Fri 16:35 |
| falcon-cron-remine  | `weekly_remine`      | `30 12 * * 0`   | Sun 18:00 |

Each cron service:
- Same Dockerfile, same env vars as main backend
- Sets `FALCON_JOB` to one of the four
- Uses `entrypoint_cron.sh` as start command
- Set `restartPolicyType: NEVER` (one-shot per schedule)

## 6 · Vercel — frontend

The new pages (`/app/falcon/*`) are part of the existing Next.js app. Just
push and Vercel auto-deploys.

Set env var on Vercel:
```
NEXT_PUBLIC_API_URL=https://<your-railway-app>.up.railway.app
```

## 7 · Verify production

After deploy:
```bash
# Backend
curl https://<railway-url>/api/falcon/admin/status
curl https://<railway-url>/api/falcon/patterns/stats

# Trigger first signal generation manually (cron may not have fired yet)
curl -X POST https://<railway-url>/api/falcon/admin/rerun/daily_signals
sleep 30
curl https://<railway-url>/api/falcon/signals/today
```

Frontend: visit `https://<vercel-url>/falcon`. Should show today's picks.

## 8 · Email notifications (optional)

Set Railway env vars:
```
FALCON_EMAIL_FROM=falcon@yourdomain
FALCON_EMAIL_TO=pudhuraja@kanida.ai,team@kanida.ai
FALCON_SMTP_HOST=smtp.gmail.com   # or sendgrid, ses, etc.
FALCON_SMTP_PORT=587
FALCON_SMTP_USER=...
FALCON_SMTP_PASS=...
```

Test:
```bash
curl -X POST https://<railway-url>/api/falcon/admin/rerun/daily_signals
# Check inbox + falcon_notifications_out table
```

## 9 · Day-2 ops

- **Kite token expires daily**: legacy engine already handles this via
  `services/kite_auth.py` and the admin panel UI. Falcon's `daily_data_refresh`
  reuses the same token cache — no separate auth work needed.
- **DB grows**: Each day adds ~500 rows × 30 cols of features and ~500 rows
  of outcomes. Linear growth, ~5 MB/month. Plan for migration to Supabase
  Postgres once approaching 5 GB.
- **Pattern decay**: Re-mine weekly (Sunday cron). If new patterns improve
  OOS metrics, they auto-promote and start firing. Track via
  `/api/falcon/patterns/stats`.

## 10 · Rollback

If Falcon misbehaves:
- Comment out the `falcon_*_router` includes in `backend/main.py`
- Redeploy
- Frontend pages return errors but legacy stays intact

To fully remove: `git rm -r backend/falcon frontend/app/falcon`. Done.
