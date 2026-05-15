# Falcon V7.1 — Production Backend

The production runtime layer of the Falcon Pattern Mining Engine. Coexists with
the legacy engine inside `backend/`. All Falcon endpoints live under
`/api/falcon/*`.

## Folder map

```
backend/falcon/
├── __init__.py
├── config.py                ← env vars, paths, thresholds
├── db.py                    ← SQLite connection helpers
├── db_schema_extensions.sql ← runtime tables (signals_live, runs, notifications)
├── db_init.py               ← idempotent schema apply
├── routers/
│   ├── signals_router.py    ← /api/falcon/signals/*
│   ├── portfolio_router.py  ← /api/falcon/portfolio/*
│   ├── patterns_router.py   ← /api/falcon/patterns/*
│   └── admin_router.py      ← /api/falcon/admin/*
├── services/
│   ├── pattern_loader.py    ← load promoted patterns (V7.1 filter)
│   ├── signal_runner.py     ← evaluate patterns + persist top-N
│   └── notification.py      ← in-app + SMTP email queue
└── jobs/
    ├── _run_log.py          ← every cron writes to falcon_signal_runs
    ├── daily_data_refresh.py    ← Kite incremental OHLC fetch
    ├── daily_features.py        ← weekly aggregate + features for new dates
    ├── daily_signals.py         ← run signal generator + persist + notify
    └── weekly_remine.py         ← cold path: re-mine + re-validate (Sundays)
```

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/falcon/signals/today?top_n=N` | Latest emitted picks |
| GET | `/api/falcon/signals/by-date?date=YYYY-MM-DD` | Picks for a date |
| GET | `/api/falcon/signals/dates` | Date picker data |
| GET | `/api/falcon/signals/stock/:symbol` | History per stock |
| GET | `/api/falcon/patterns?...` | Browse promoted patterns |
| GET | `/api/falcon/patterns/stats` | Counts by class/target |
| GET | `/api/falcon/portfolio/summary` | Latest backtest headline |
| GET | `/api/falcon/portfolio/trades` | Trade ledger |
| GET | `/api/falcon/admin/status` | Engine + DB status |
| GET | `/api/falcon/admin/runs` | Recent cron runs |
| POST | `/api/falcon/admin/rerun/:job_name` | Trigger a job manually |

## Env vars (Railway)

```
FALCON_DB_PATH=/app/data/db/kanida_universe.db   # default — auto via entrypoint
FALCON_TOP_N=25
FALCON_MIN_FIRES=2
FALCON_MIN_LIQ_CR=5.0

# Email notifications (optional)
FALCON_EMAIL_FROM=falcon@kanida.ai
FALCON_EMAIL_TO=you@example.com
FALCON_SMTP_HOST=smtp.example.com
FALCON_SMTP_PORT=587
FALCON_SMTP_USER=…
FALCON_SMTP_PASS=…
```

## Cron schedule (IST → UTC)

```
daily_data_refresh   16:30 IST  =  11:00 UTC  Mon-Fri
daily_features       16:32 IST  =  11:02 UTC  Mon-Fri
daily_signals        16:35 IST  =  11:05 UTC  Mon-Fri
weekly_remine        18:00 IST  =  12:30 UTC  Sun
```

Each cron runs as a separate Railway service via `entrypoint_cron.sh`, with
`FALCON_JOB` env var selecting which job module to run.

## Local dev

```bash
# Start FastAPI with both legacy + Falcon routers
cd backend
uvicorn main:app --reload --port 8001

# Run a cron job manually
python -m falcon.jobs.daily_signals
python -m falcon.jobs.daily_features

# Check status
curl http://localhost:8001/api/falcon/admin/status
```

## Tables (Falcon SQLite)

Existing (offline-mined):
- `falcon_features`         — feature panel
- `falcon_outcomes`         — forward-return labels
- `falcon_pattern_candidates` — mined rules
- `falcon_pattern_validations` — OOS test results
- `falcon_promoted_patterns`   — survivors (847)
- `falcon_sectors`             — sector mapping
- `ohlc_daily` / `ohlc_weekly` — universe price data
- `universe_master`            — eligible stocks

Added by Falcon production runtime:
- `falcon_signals_live`        — emitted picks per day
- `falcon_signal_runs`         — cron audit log
- `falcon_notifications_out`   — in-app + email queue
