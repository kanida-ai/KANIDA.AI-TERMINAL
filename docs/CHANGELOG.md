# Changelog

Reverse-chronological log of meaningful changes. Operational incidents and their
root-cause fixes are recorded here so we don't relearn them.

## 2026-06-02

- **Phase 2/3 cleanup — removed verified-dead code (usage-audited first).**
  A read-only usage audit grepped every legacy route/router/module for live
  consumers before touching anything. Results:
  - **Backend:** unmounted `orders_router` + `strategy_router` (zero consumers);
    archived `backend/scheduler.py`, the two routers, and the top-level `engine/`
    research package to `backend/_archive/`; deleted empty `agents/`+`signals/`.
    Verified post-restart: dead routes 404, all active routers 200.
  - **Frontend:** relocated the self-referential legacy cluster (welcome, engine,
    login, terminal, dashboard, analysis-v2/v3, `.legacy-backup` files) to the
    unrouted `app/_legacy/`. tsc passes; nothing active links in.
  - **Kept (load-bearing):** `/admin` + `/analysis` pages (operator links to
    them) and the legacy routers consumed by active Falcon pages via
    `lib/admin-api.ts` + `lib/backtest-api.ts`.
  `app/` now holds only active routes: `power/`, `falcon/`, `admin/`, `analysis/`,
  `api/` (+ `_legacy/`).
- **Docs + folder cleanup (Phase 1).** Added `docs/` (architecture, api-map,
  setup-guide, folder-structure, audit-report, this changelog) and a top-level
  `README.md`. Moved root audit memos → `docs/audit-history/`, research CSVs +
  logs → local `archive/` (gitignored). Removed a 0-byte stray `kanida_quant.db`
  from git. **No code or routes moved** — the risky restructure (router rename,
  legacy route relocation) is deferred to a gated step.
- **ROOT-CAUSE FIX: machine-wide Playwright path.** The true cause behind ~9
  "BROWSER_LAUNCH_FAILED / EOD didn't fire" incidents: the per-user
  `%LOCALAPPDATA%\ms-playwright` folder is **invisible to the non-interactive
  Task Scheduler logon session** (proven: `os.path.exists` returns True
  interactively, False from the task — same user/env). Browsers reinstalled to
  `C:\ProgramData\ms-playwright`; every entry point now sets
  `PLAYWRIGHT_BROWSERS_PATH`. Verified: auth SUCCEEDS from the task context that
  always failed before.
- **Narrative accuracy fix.** The pick narrative claimed "approaching its
  one-year high" for stocks in deep drawdowns (e.g. JKCEMENT, 31% below). Cause:
  the narrator described the pattern's loose *threshold*, not the stock's
  *actual* value. Now grounded in real feature values with a floor — drawdown
  stocks read "trading below / deep below its high", near-high stocks still read
  "right at / near". (`pattern_narrator.py`, `falcon_top20_explainer.py`.)

## 2026-06 (Fix 1)

- **Decoupled Zerodha auth from the backend.** Auth moved out of the long-lived
  backend into a standalone Windows Scheduled Task (`KanidaZerodhaAuth` →
  `run_auth_worker.bat` → `auth_worker.py`), a fresh process every 30 min. Worker
  self-gates to weekday 06:00–16:30 IST. Backend now only *reads* the token.

## 2026-05-29 — Playwright preflight + reliability

- Added `playwright_preflight.py`: boot-time + hourly subprocess-isolated browser
  health check with full env diagnostics, classified failures, immediate push,
  and a scheduler skip-gate (no more 21 wasted cycles). `repair_playwright.bat`
  one-click recovery. Pinned `playwright==1.59.0`.

## 2026-05-27 — EOD pipeline recurrence fix

- Gate the pipeline by emitted `signal_date` vs the current IST window, not by a
  run-log timestamp (a pre-16:05 run was falsely marking the day "done").
- `daily_signals` raises `StaleSignalsError` instead of recording a stale
  success. Auth retries extended across the trading day with a live token-health
  check. (`_pipeline.py`, `main.py`, `auth_scheduler.py`, `daily_signals.py`.)
- Co-Trader EOD moved into the V7 chain (was bolted to the legacy pipeline that
  aborted on bad tokens, freezing portfolios).

## Open items / known risks

- **Laptop-sleep kills the backend process** (whole-site 503). Mitigation:
  disable sleep + a watchdog that restarts on dead :8001. Real fix: move off the
  laptop to a small always-on VPS.
- **Residual parity drift** (~1–2pp) between `portfolio_engine` (live) and
  `persona_simulator` (backtest) equity math — align in a focused pass.
- **Deferred restructure** — see `docs/folder-structure.md` "Deferred restructure".
