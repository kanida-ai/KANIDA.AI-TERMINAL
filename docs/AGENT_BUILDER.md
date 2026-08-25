# Agent Builder — cross-session coordination (SINGLE SOURCE OF TRUTH)

Two Claude Code sessions collaborate on this feature. **This file is the shared board.**
READ it at the start of every Agent-Builder task; UPDATE it (deltas + changelog) at the end.
**Git is the sync substrate** — both sessions commit to THIS repo, so nothing drifts.

## Sessions & ownership
- **`kanida-falcon` session** (folder `Documents/Kanida_Falcon`): owns the Builder **UX + engine/features**, and the **engine/data** that produces the Parquet. Iterates + tests LOCALLY.
- **`cloud-migration` session** (this folder): owns **cloud infra, deploy, portal integration, data pipeline to S3**. Deploys to cloud when the falcon session is satisfied locally.
- We can message each other directly in Claude (cross-session) — no human relay needed.

## Canonical location (avoid the "two copies" drift — this was the whole problem)
The Agent Builder **code lives ONLY in this repo (`KANIDA.AI-TERMINAL`)**:
- backend module: `backend/agent_builder/`
- frontend page:  `frontend/app/power/(app)/builder/page.tsx`
- frontend api:   `frontend/lib/builder-api.ts`
- nav item:       `frontend/components/power/shell/nav-config.tsx`

Develop it **HERE** (falcon session: `cd` into this repo or use a git worktree of it).
**Do NOT re-copy from `Documents/Kanida_Falcon/agent_builder_service/`** — that was the seed and is now STALE; re-copying would overwrite the cloud fixes below. Kanida_Falcon stays for the ENGINE + data only.

## Cloud deltas already applied here (do NOT re-introduce the old versions)
1. `backend/main.py`: router mounted (try/except wrapped) at `/api/builder/*`.
2. **ROOT `requirements.txt`** (NOT `backend/requirements.txt`): added `duckdb`, `pyarrow`. The Dockerfile installs the ROOT file — the seed's `backend/requirements.txt` is not installed.
3. `deploy/entrypoint.sh`: `AGENT_DATA_URI=s3://kanida-cb-src-389642461326/kanida/daily/`, `AGENT_NIFTY_SYMBOL="NIFTY 50"`, `AWS_REGION=ap-south-1`.
4. `frontend/lib/builder-api.ts`: API base is `NEXT_PUBLIC_API_URL || ''` (relative/same-origin) — **NOT `localhost:8000`** (that caused "backend offline").
5. `frontend/next.config.ts`: rewrite `/api/builder/:path*` -> `${BACKEND_ORIGIN}/api/builder/:path*` (same-origin, no CORS).
6. `frontend/middleware.ts`: `/api/builder` added to `POWER_PORTAL_PATHS` (exempt from the site HTTP Basic Auth, like `/api/power`).
7. `frontend/components/power/shell/nav-config.tsx`: `"Build an Agent"` primary nav item (key `builder`, href `/power/builder`, IconSpark).
8. Data pipeline: `kanida.db.ohlc_daily` (bar_time schema) -> per-symbol Parquet -> `s3://kanida-cb-src-389642461326/kanida/daily/` (1561 stocks, 3817 daily bars, 2013->2026). Re-run when the fixed-engine data is ready.

## Backend <-> frontend contract (v0)
- Endpoints: `/api/builder/{indicators, quote, backtest, wallet, wallet/topup, health}`.
- Auth: **X-User-Id stub — UNAUTHENTICATED.** !!! Replace with power-JWT before public launch.
- Data: Parquet on S3 read via DuckDB (`httpfs`/`aws` exts; needs the ECS task role's `s3:GetObject`+`kms`, granted via inline policy `agent-builder-data`).

## State
- **DEPLOYED (cloud):** backend live on ECS (`api.kanida.ai/api/builder/*`), frontend live on Vercel (`/power/builder`). Full backtest verified in the UI 2026-08-25.
- Cloud currently un-paused for this; nightly Falcon signal cron DISABLED (engine rebuild in progress).

## Local-first dev loop
1. **falcon session** iterates Builder UX/features in this repo, runs it LOCALLY:
   - backend: `cd backend && uvicorn main:app --port 8001` (set `AGENT_SQLITE_FALLBACK=<path>/kanida.db` for local data, or point `AGENT_DATA_URI` at a local Parquet dir).
   - frontend: `cd frontend && npm run dev` (talks to the local backend via the same rewrite).
2. When satisfied locally: commit to a branch (suggest `feat/agent-builder`), push.
3. **cloud session** reviews + deploys: backend via CodeBuild+ECS roll, frontend via Vercel (push to main). Update the changelog here.

## TODO / backlog (to reach GTM-grade)
- [~] **UX cleanup** (falcon session) — v0.2 landed (grouped sections, carded condition builder, plain-English read-back, starter presets, cost breakdown, polished results + verdict + Worlds legend, responsive, offline state). More UX depth below (A).
- [ ] Replace `X-User-Id` stub with power-JWT auth. *(cross-cut — cloud session owns the JWT dependency; falcon wires the client)*
- [ ] Wire wallet to `power_user_users.token_balance` + Razorpay top-ups.
- [ ] 1-min data tier (daily only for now).

### Feature roadmap toward GTM (falcon session) — mandate: "everything, lots more functionalities"
Operator wants the full build-out, not one item. Proposed sequencing; operator's own feature brain-dump lands on top of this via the cloud session.
**A. Composer depth** — save/load + version agents (per user); more indicators (ADX, OBV, Donchian, VWAP-dist, 52w hi/lo, sector RS, event/earnings flags); nested condition groups (AND-of-ORs); "signal persists N bars"; universe/segment filters (price/liquidity/index/sector); multi-leg entry/exit; long+short in one agent.
**B. Evidence depth** — portfolio-level backtest (equity curve, CAGR, max DD, Sharpe/Calmar) not just per-trade expectancy; drawdown + monthly-returns heatmap; trade distribution + top/bottom trades; per-year table; NIFTY benchmark overlay; robustness (param-sensitivity sweep, OOS/era split, cost sensitivity); shareable evidence card.
**C. Tokens/wallet/GTM** — power-JWT (with cloud); wallet→RDS + Razorpay + ledger UI; plan-linked monthly token grants; big-run (1-min) confirm modal; usage history/receipts.
**D. Deploy-an-agent (bridge to the rest of KANIDA)** — "Promote to paper-trading" → live paper agent w/ Algorithm-1 identity + self-journal; user-agent leaderboard; "your agent signalled today" alerts; guarded, human-gated Co-Trading/AutoTrade handoff.
**E. Data/engine** — 1-min tier live; refresh Parquet from the fixed engine; fundamentals/events source.

## Changelog
- 2026-08-25 (cloud session): folded Agent Builder v0 into this repo + deployed; verified end-to-end (health, quote, 240k-trade backtest, wallet decrement). Applied cloud deltas 1-8 above. Created this coordination doc.
- 2026-08-25 (falcon session): **UX cleanup v0.2** of `frontend/app/power/(app)/builder/page.tsx` — no backend/contract change. Added: grouped sections (Identity/Entry/Exit/Data) with hierarchy; carded, labelled condition builder (min-one-condition guard); live plain-English strategy sentence; 4 starter presets; clearer token-cost breakdown; polished results panel (honest verdict chip, Market-Worlds heat + legend + hover sample-size, loading/empty/offline states); responsive (useBreakpoint) two-col→one-col. Verified LOCALLY against real data (backend on :8011 SQLite fallback = kanida.db, 1561 stocks × 3817 bars; Next dev :3007 w/ NEXT_PUBLIC_PREVIEW_NO_AUTH=1): page compiles, catalog+quote+wallet-topup work through the rewrite, backtest returns valid card (RSI<30 → n=113,294, win 47.2%, +0.28%/trade, edge −0.17%, pf 1.11; charged 276, balance 5000→4724). Appended the GTM feature roadmap (A–E) above. Ready for cloud review on `feat/agent-builder`.
