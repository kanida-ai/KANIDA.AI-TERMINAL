# Folder Structure & Conventions

## Layout

```
kanida.ai/
├── README.md                  Entry point
├── docs/                      All documentation (you are here)
│   ├── architecture.md
│   ├── api-map.md
│   ├── setup-guide.md         Ops runbook + hard-won gotchas
│   ├── folder-structure.md
│   ├── audit-report.md        Full codebase audit (2026-06)
│   ├── CHANGELOG.md
│   └── audit-history/         Older research/DD memos (markdown)
│
├── backend/                   FastAPI app (uvicorn :8001)
│   ├── main.py                App + lifespan + router mounts + scheduler threads
│   ├── falcon/                AUTO TRADE product (V7 pipeline, trade execution)
│   │   ├── jobs/              V7 pipeline steps (_pipeline.py orchestrates)
│   │   ├── services/          signal_runner, etc.
│   │   ├── trade/             position monitor, order placement, KiteTicker
│   │   ├── routers/           /api/falcon/* (5)
│   │   └── README.md
│   ├── power_user/            POWER USER product (invite-only beta)
│   │   ├── routers/           /api/power/* (8)
│   │   ├── services/          persona_simulator, portfolio_engine,
│   │   │                      falcon_top20_explainer, pattern_narrator,
│   │   │                      web_push, playwright… (see service files)
│   │   ├── tests/             unit tests
│   │   ├── SPEC/              Requirements/Design/Tasks
│   │   └── README.md
│   ├── services/              SHARED — kite_auth, zerodha_auto_auth,
│   │                          auth_scheduler, playwright_preflight, data_freshness
│   ├── routers/               LEGACY operator API (11; audit before removal)
│   └── _archive/              Already-archived old code
│
├── frontend/                  Next.js 16 (Vercel)
│   └── app/
│       ├── power/             Power User pages (active)
│       ├── falcon/            Auto-trade operator pages (active)
│       └── {admin,analysis*,dashboard,engine,terminal,welcome,login}/
│                              Legacy (candidates for app/_legacy/ after audit)
│
├── universe_engine/           R&D — mining + walk-forward; holds 13+ GB RND DB
├── data/db/                   PROD + LEGACY SQLite DBs (+ _backups/)
├── scripts/                   Ops/data/research scripts + Windows .bat/.ps1
├── config/                    .env (gitignored), templates, AUTH_BOT_SETUP.md
├── deploy/                    Cloudflared tunnel config
├── scheduler/                 Windows Task Scheduler XML
└── archive/                   LOCAL-ONLY stale artifacts (gitignored)
```

## Conventions

### Where new code goes
- Power User feature → `backend/power_user/` + `frontend/app/power/`
- Auto-trade feature → `backend/falcon/` + `frontend/app/falcon/`
- Something both products need (e.g. Kite) → `backend/services/`

### Archiving (no more numbered duplicates)
- **Never** create `file_v2.py`, `page.final.tsx`, `x.legacy-backup`. If a new
  version is needed, the OLD one moves to `archive/` (gitignored) **before** the
  new one lands in the active tree.
- `archive/` is local tidiness only — multi-MB data/CSVs/logs that aren't
  version-worthy. It is gitignored so the repo stays lean.
- Markdown worth keeping (design notes, DD memos) → `docs/` (tracked), not archive.

### Two `scripts/` directories
There is `scripts/` (repo root, ops + research) and `backend/scripts/` (DB init
+ OHLC sync). Don't confuse them. Ops entry points (`start_backend.bat`,
`run_auth_worker.bat`, `repair_playwright.bat`, `register_auth_task.ps1`) live in
the **root** `scripts/`.

### Runtime junk
`logs/`, `outputs/`, `*.log`, `archive/`, `.claude/`, `.railway/` are gitignored.
Don't commit run logs or generated artifacts.

## Restructure status (2026-06-02 — usage-audited + executed)

Done after a read-only usage audit confirmed safety:
- ✅ Legacy frontend cluster (welcome, engine, login, terminal, dashboard,
  analysis-v2/v3, `.legacy-backup`) → `app/_legacy/` (unrouted).
- ✅ Unmounted dead routers `orders` + `strategy`; archived to `backend/_archive/`.
- ✅ Archived `backend/scheduler.py` + top-level `engine/` (zero imports).
- ✅ Deleted empty `backend/agents/`, `backend/signals/`.

**Intentionally NOT done** (the audit found these are load-bearing):
- `backend/routers/` is **not** renamed to `legacy/` — the active Falcon
  operator pages consume those routers via `lib/admin-api.ts` +
  `lib/backtest-api.ts`. Renaming would churn live imports for no gain.
- `/admin` and `/analysis` pages stay put — active Falcon pages link to `/admin`
  (token refresh) and the "Full Kanida.AI mode" toggle links to `/analysis`.
