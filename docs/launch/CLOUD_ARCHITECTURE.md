# Kanida.AI — Cloud Architecture (canonical reference)

**Status:** LOCKED 2026-06-13 (IST). Supersedes the Postgres-first framing in earlier MASTER_SPEC §1.5.
**Grounded in:** `docs/launch/DB_DEPENDENCY_MAP.md` (code-traced) + an external architecture review, reconciled.

> **North Star:** the product depends on **cloud production**, never on the laptop. The laptop only creates better intelligence and *publishes it up*. If the laptop is lost, the product keeps running on the last published intelligence — it does **not** go down.

---

## 1. Two environments, one-way publish wall

```
   RESEARCH (laptop now; optional cheap VM later)        CLOUD PRODUCTION (always-on)
   ┌──────────────────────────────────────┐            ┌─────────────────────────────────┐
   │ 14G research DB:                       │            │ Lean production DB (on a volume) │
   │  full OHLC history, ohlc_1min (87.8M), │  publish   │  signals, patterns, features,    │
   │  ohlc_5min, deep falcon_outcomes,      │ ─patterns─▶│  recent OHLC, summaries,         │
   │  mining/validation/backtest scratch    │ +summaries │  user/billing/portfolio          │
   │                                        │  (weekly)  │                                  │
   │ Runs: weekly mining + validation +     │            │ Runs: daily OHLC fetch →         │
   │ backtests → produces compact outputs   │            │  features → signals → portfolio  │
   └──────────────────────────────────────┘            │  EOD; serves the app + billing   │
        laptop down ⇒ no NEW patterns this week         └─────────────────────────────────┘
        but the product stays UP on last publish              ▲ users hit ONLY this
```

**The rule:** anything a user request touches lives in cloud production. The 14 GB research warehouse is **batch-only** and never in a request path.

---

## 2. What lives where (code-traced — see DB_DEPENDENCY_MAP.md)

### Cloud production DB (lean)
`universe_master`, `falcon_sectors`, `ohlc_daily` (rolling window), `ohlc_weekly`, `falcon_features` (rolling for scoring; see §3 caveat), `falcon_pattern_candidates`, `falcon_promoted_patterns`, `falcon_pattern_taxonomy`, `falcon_signals_live`, `falcon_signal_runs`, `falcon_top10_audit`, `falcon_replay_cache`, `falcon_live_decisions`, `power_user_*` (users/billing/invites/watchlist/request_log), `portfolio_*`, `falcon_trade_*` / `falcon_position_*` / `falcon_premarket_staging` / `*_config`, `kite_tokens` + `falcon_auth_log` (cloud owns the daily jobs).

### Research warehouse (stays off-cloud)
Full multi-year `ohlc_daily`, `ohlc_weekly`; `ohlc_1min` (87.8M); `ohlc_5min`; deep `falcon_outcomes` (827k); `falcon_pattern_candidates`/validations/mining; walk-forward, `paper_trades`, `stock_efficacy`, `regime_calendar`; `intraday_mining.db`, `kanida_universe_winB.db`.

---

## 3. The two facts that drive the design

1. **Daily signal generation needs only ~252 trading days of `ohlc_daily`** (`falcon_features.py` skips <252 bars; `dist_high_252` is the deepest feature). → the cloud needs a **rolling window**, not full history (~tens of MB). **Operational target: keep 300 daily bars in production** — 252 is the hard code minimum; the extra ~48 absorb weekly-feature lookback, missing/holiday days, and recompute/catch-up gaps. (Used as the Phase 3 prune target.)

2. **CAVEAT (the catch that resequences pruning):** the **Historical Evidence** panel (`falcon_top20_explainer.py:163,1009,1021-1025`) replays pattern rules against **`falcon_features` back to `2021-01-01`** (~5 years) AND reads `falcon_outcomes` from the research DB at request time (`falcon_top20_router.py:94`). Persona endpoints **re-simulate at request time** (`persona_simulator.py`), reaching the research pattern catalog + multi-year OHLC. → **You cannot prune `falcon_features` (or drop `falcon_outcomes`) until these reads are replaced by precomputed summaries.** Prune is gated on precompute.

---

## 4. Request-time dependencies to clean (before pruning, NOT before launch)

| # | Offending read | File | Fix |
|---|---|---|---|
| L1 | `falcon_outcomes` from research DB per request | `falcon_top20_router.py:94`, `falcon_top20_explainer.py:1103` | precompute → `pattern_stock_evidence` + `stock_signal_evidence` in cloud DB |
| L2 | `falcon_features` replayed back to 2021 per request | `falcon_top20_explainer.py:1021-1025` | precompute the same evidence summaries; then features can be pruned |
| L3 | persona backtest re-simulated per request | `persona_simulator.py:408-417` | precompute → `persona_backtest_summary/yearly/monthly/trades` |

### Materialized summary tables to add (Option B — recommended)
```
pattern_stock_evidence(symbol, pattern_id, n_fires, n_with_outcome, hit_rate,
                       avg_return, avg_win, avg_loss, worst_drawdown, last_fire_date)
stock_signal_evidence(symbol, lookback_start, lifetime_baseline_hit_rate,
                      n_total_outcomes, updated_at)
persona_backtest_summary / persona_backtest_yearly / persona_backtest_monthly / persona_backtest_trades
```
Generated on the research/daily side, shipped to cloud. App then reads a row → instant + cheap, and the request path stops touching raw OHLC / research DB.

---

## 5. The migration — 4 phases (SQLite-volume FIRST, Postgres LAST)

**Why not Postgres first:** the codebase uses direct `sqlite3` file connections throughout (kite_auth, publish, per-request `get_db`). Switching to Postgres is a **code refactor of every DB call**, not a data move. Leading with it front-loads the hardest work. Going SQLite-on-a-volume first achieves the real goal (laptop not a production dependency) with minimal code change — and **defers the three Postgres-portability snags** (webhook dedupe / kite_tokens / publish script) to Phase 4, where they belong.

### Phase 1 — Get off the laptop (SQLite on a cloud volume). LAUNCH HAPPENS HERE.
- Move backend + the **whole 573 MB production DB** + daily jobs to a cloud host with a **persistent volume**. Ship the DB **as-is** (no pruning) — it already holds the multi-year features/OHLC that Historical Evidence + personas need, so everything works untouched.
- Copy the small `falcon_outcomes` (827k) into the cloud DB and point the evidence read at it, so no request reaches the 14 GB research DB. (Closes L1's *transport* need without the full precompute.)
- 14 GB research DB **stays on the laptop**.
- Daily jobs (OHLC fetch → features → signals → portfolio EOD) run **in the cloud** (cloud owns the Kite token in the volume's SQLite file — no Postgres refactor needed). Verify Playwright auth works headless.
- **Design the laptop→cloud publish transport** (see §6).
- **Billing/paywall/signup (already built, SQLite-ready) goes live here.** Off-laptop + charging, minimal code risk.
- **Goal:** laptop loss ≠ product down. Money can be collected.

**Phase 1 is DONE only when ALL of these are verified (none optional):**
- [ ] Cloud app **boots from the persistent SQLite volume** (kill + restart → data survives, no fresh DB created).
- [ ] **Daily jobs run in the cloud** on schedule (OHLC fetch → features → signals → portfolio EOD wrote today's rows).
- [ ] **Billing works end-to-end** in cloud (test-mode Razorpay → webhook → `billing_plan='paid'`).
- [ ] **Auth/session/token storage works** in cloud (login persists across restart; Kite token read/write from the volume DB; Playwright auth refresh succeeds headless).
- [ ] **`/power/today` no longer depends on the laptop** (laptop powered off → page still loads signals + Historical Evidence; nothing reads the 14 GB research DB).
- [ ] **Backups + restore tested** (take a volume snapshot, restore to a scratch instance, app boots from it).
- [ ] **Laptop→cloud publish is authenticated + atomic** (endpoint requires a secret; a partial/failed bundle never leaves the cloud DB half-updated — import wrapped in a transaction).

### Phase 2 — Materialize expensive request-time reads
- Build the §4 summary tables; switch `/power/today` evidence + `/api/power/personas/*` to read them. Stop opening the research DB and stop request-time re-simulation.

### Phase 3 — Prune production DB
- After Phase 2 proves the app no longer needs deep history at request time: prune cloud `ohlc_daily`/`falcon_features` to ~300 days, keep patterns + summaries. Cloud DB shrinks to a lean core. (Pure cost optimization.)

### Phase 4 — Optional Postgres/Supabase
- Only when a **scale trigger** hits: you need **multiple backend instances** (can't share one SQLite file) OR **managed point-in-time backups**. Then refactor the DB-access layer to Postgres and use the porting script + migration already built (`scripts/migrate_to_supabase.py`, `backend/power_user/migrations/0001_billing.sql`). The webhook-dedupe / kite_tokens / publish portability fixes land here.

---

## 6. The laptop → cloud publish transport (design point both diagrams gloss)

With the cloud DB on a volume, the laptop **cannot** `sqlite3.connect` it remotely. The weekly publish needs an explicit channel:
- **Option A (recommended):** a small authenticated cloud endpoint `POST /api/admin/publish-intelligence` that ingests a compact bundle (promoted patterns + taxonomy + evidence summaries + persona summaries) and upserts into the cloud DB.
- **Option B:** laptop drops a bundle file to object storage; a cloud job imports it.
Either way the laptop only uploads **compact outputs** (KB–MB), never the 14 GB.

---

## 7. Future intraday/live product — already supported, stays cheap

Cloud already holds **pattern definitions + backtest summaries**, so "show this pattern's track record" is instant. To add live overlap: feed the **Kite WebSocket** (`kite_ticker`, already built) → compute live features intraday → match against pattern definitions → surface "stocks lighting up strong patterns now." This needs the **live stream + mined patterns**, NOT 1-minute *history* — so `ohlc_1min` stays research-only even in the future product. No cloud-cost blow-up.

---

## 8. One-line model
> **Cloud = lean serving DB + the daily signal loop (+ billing). Laptop = full history + weekly mining that publishes compact intelligence up. Live data and summaries go to the cloud; raw history never does. Launch on SQLite-on-a-volume; reach for Postgres only when scale demands it.**
