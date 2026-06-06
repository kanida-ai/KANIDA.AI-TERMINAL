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

## Deferred restructure (NOT yet done — needs usage audit + backend restart)

These were identified in the audit but intentionally **not** executed yet
because the backend is live and they carry real risk:
- Rename `backend/routers/` → `backend/legacy/routers/` (one import in main.py;
  requires a backend restart).
- Move legacy `frontend/app/{terminal,dashboard,engine,welcome,login}/` →
  `app/_legacy/` (Next.js routes — must verify nothing links to them first).
- Unmount dead legacy routers (audit each for live consumers).

Do these as a separate, gated step. See `docs/audit-report.md` §13.
