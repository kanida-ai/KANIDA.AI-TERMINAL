# Kanida.AI — DB Dependency & Usage Map

> **Supporting artifact, not the plan.** The canonical cloud/deploy plan is
> **[CLOUD_ARCHITECTURE.md](CLOUD_ARCHITECTURE.md)**. This map is the evidence underneath it.

**Purpose:** Factual base for the production-cloud vs research-laptop DB split.
**Method:** Code-traced (static analysis of source + schema files). Findings cite `file:line`; they reflect what the code *does*, and should be spot-checked against a live DB before any destructive step (e.g. pruning).
**Date:** 2026-06-13 (code state at time of trace). READ-ONLY — nothing in the repo was modified.

---

## 0. DB-path constants → physical files

These constants decide which file every reader/writer touches. All resolved in config modules:

| Constant | Defined in | Resolves to (prod canonical) | Notes |
|---|---|---|---|
| `POWER_DB_PATH` | `backend/power_user/config.py:31-44` | `data/db/kanida_universe.db` (**573M PROD**) | Falls back to `universe_engine/.../kanida_universe.db` only if canonical missing. Power-user tables co-located here. |
| `FALCON_DB` | `backend/falcon/config.py:23-38` | `data/db/kanida_universe.db` (**same 573M PROD**) | Same resolution logic; `entrypoint.sh` seeds it in prod. `connect_falcon()` opens this. |
| `POWER_RND_DB_PATH` | `backend/power_user/config.py:50-53` | `universe_engine/data/db/kanida_universe.db` (**14G R&D**) | Holds `falcon_outcomes` (827k rows) + `ohlc_1min` (87.8M rows). Comment at `config.py:46-49`. |
| `LEGACY_DB` | `backend/falcon/config.py:20` | `data/db/kanida_quant.db` (**83M legacy**) | `connect_legacy()` — kite_tokens + legacy auth only. |
| `KANIDA_DB_PATH` (`backend/db.py:27`) | `backend/db.py` | `data/db/kanida_quant.db` (legacy) | The generic `get_conn()` factory. **Do NOT confuse with POWER_DB_PATH** — power_user/config.py:29 explicitly warns not to fall through to it. |
| RND path (persona) | `persona_simulator.py:62-71` | `universe_engine/data/db/kanida_universe.db` (14G R&D) | Independent re-derivation of the same RND path. |
| Intraday path | `persona_simulator.py:74-79` | `universe_engine/data/db/intraday_mining.db` (**30M**) | `intraday_dataset_v2` — used by persona P2's 09:30 filter only. |

**Key fact:** PROD app DB and Falcon engine DB are the **same physical file** (`data/db/kanida_universe.db`). Power-user tables, Falcon engine output tables, `ohlc_daily`, `falcon_features`, and `falcon_pattern_taxonomy` all co-locate there by design (avoids cross-DB ATTACH).

---

## 1. DB → table inventory

### A. `data/db/kanida_universe.db` (573M) — **PROD app DB** (= FALCON_DB = POWER_DB_PATH)

Holds three layers in one file:

**Engine output (written by Falcon jobs; read-only contract for power_user):**
- `falcon_signals_live` — daily emitted picks (schema: `db_schema_extensions.sql:6-24`)
- `falcon_signal_runs` — cron run audit log (`:27-37`)
- `falcon_notifications_out` — email/in-app queue (`:40-52`)
- `falcon_top10_audit` — live Top-10 audit trail (`:61-82`)
- `falcon_features` — daily feature snapshot (39 cols; schema in `universe_engine/engine/falcon_features.py:30-60`)
- `falcon_pattern_taxonomy` — promoted patterns + regime + plain-English + `rule_json` (1,943 patterns per MEMORY; read at `falcon_top20_explainer.py:405-410`)
- `falcon_sectors` — symbol→sector map (read `signal_runner.py:77-78`)
- `falcon_promoted_patterns` / `falcon_patterns` / `falcon_outcomes` / `falcon_validations` — referenced as "existing falcon_* tables" in `db_schema_extensions.sql:2-3`. **`falcon_outcomes` here is sparse/partial; the deep copy lives in RND** (see `falcon_top20_explainer.py:1003-1016`).
- `ohlc_daily` — daily OHLC source of truth (schema `universe_engine/data/db/init_db.py:33-45`)
- `ohlc_weekly` — weekly resample (rebuilt nightly, `daily_features.py:48-92`)
- `universe_master` — symbol universe + index membership flags (`init_db.py:16-30`)

**Falcon trade/operator runtime (`db_schema_extensions.sql`):**
- `falcon_trade_runs` (`:85`), `falcon_trade_orders` (`:111`), `falcon_position_state` (`:138`), `falcon_trade_events` (`:161`), `falcon_position_first_seen` (`:177`), `falcon_trail_config` (`:191`), `falcon_engine_config` (`:218`), `falcon_premarket_staging` (`:233`)

**Power-user portal (`db_schema.sql` + `db_schema_portfolios.sql`, listed in `db_init.py:23-48`):**
- Auth/invite/billing: `power_user_users`, `power_user_invite_codes`, `power_user_waitlist`, `power_user_watchlists`, `power_user_subscriptions`, `power_user_billing_events`, `power_user_magic_links`, `power_user_push_subscriptions`
- Live decisions / replay / auth-log: `falcon_live_decisions`, `falcon_replay_cache`, `falcon_auth_log`
- Observability: `power_user_request_log`
- Portfolio/persona: `portfolio_definitions`, `portfolio_positions`, `portfolio_equity_history`, `portfolio_event_log`, `portfolio_yearly_performance`, `portfolio_monthly_performance`

### B. `data/db/kanida_quant.db` (83M) — **legacy** (= LEGACY_DB / KANIDA_DB_PATH)
- `kite_tokens` + legacy auth/universe tables. Touched only via `connect_legacy()` (`falcon/db.py:29-34`) and the generic `backend/db.py` factory. Not part of the power_user serving path.

### C. `universe_engine/data/db/kanida_universe.db` (14G) — **R&D warehouse** (= POWER_RND_DB_PATH)
- Full schema from `universe_engine/data/db/init_db.py`: `universe_master`, `ohlc_daily`, **`ohlc_1min` (87.8M rows)**, `regime_calendar`, `strategies`, `strategy_efficacy_walkforward`, `walkfwd_trades`, `stock_efficacy`, `live_signals`, `paper_trades`, `run_log`
- Plus the **deep `falcon_outcomes`** (827k historical outcome rows — `config.py:46`), full `falcon_pattern_*` mining catalog, and the complete multi-year `ohlc_daily` / `falcon_features` history used by backtests.

### D. `universe_engine/data/db/intraday_mining.db` (30M) — research scratch
- `intraday_dataset_v2` (read at request time by persona **P2 only** — see §3 caveat).

### E. `universe_engine/data/db/kanida_universe_winB.db` (843M) — research scratch
- Window-B mining experiment DB (`run_window_b.py`). No app reader found. RESEARCH-only.

---

## 2. Per-table readers & writers

Cadence legend: **D**=daily cron, **W**=weekly cron, **R**=request-time, **B**=boot, **O**=one-off/admin.

| Table | WRITERS (file:line, cadence) | READERS (file:line, cadence) |
|---|---|---|
| `ohlc_daily` (PROD) | `daily_data_refresh.run` `daily_data_refresh.py:43` (**D** 16:30 IST) | signal_runner `signal_runner.py:80-84` (D); falcon_top20_explainer `_sector_20d_return_pct`/`_day_return_pct` `falcon_top20_explainer.py:649,823` (**R**); portfolios `_last_close` `portfolios_router.py:623-629` (**R**); picks `_next_trading_day` `picks_router.py:64-66` (R); feature ingest reads it (D) |
| `ohlc_weekly` (PROD) | `daily_features._refresh_weekly` `daily_features.py:48-92` (**D** 16:32) | `extract_universe_features` (D, feature calc) |
| `falcon_features` (PROD) | `daily_features._refresh_features` → `extract_universe_features` `daily_features.py:142-198` (**D** 16:32) | signal_runner `signal_runner.py:69-72` (D); falcon_top20_explainer bucket1/bucket2 replay `falcon_top20_explainer.py:432,1023` (**R**); picks `_latest_signal_date` (R) |
| `falcon_signals_live` (PROD) | signal_runner `signal_runner.py:171-186` (**D** 16:35) | falcon_top20_router/explainer `falcon_top20_router.py:80`, `falcon_top20_explainer.py:211,253,376,530` (**R**); persona simulator (R, cached) |
| `falcon_pattern_taxonomy` (PROD) | weekly_remine / publish job (**W**) | falcon_top20_explainer `_load_full_taxonomy` `falcon_top20_explainer.py:405` (**R**) |
| `falcon_sectors` (PROD) | ingest (O/W) | signal_runner `signal_runner.py:77` (D); explainer `_sector_20d_return_pct` (R) |
| `falcon_outcomes` (**RND**) | mining jobs (offline) | **falcon_top20_explainer bucket2 `falcon_top20_explainer.py:1103` + lifetime baseline `:1033,1094` (R) — reads RND at request time** |
| `falcon_signal_runs` (PROD) | every cron via `_run_log.log_job` (D/W) | admin_router status widget (R, admin) |
| `falcon_live_decisions` (PROD) | live_tier `live_tier.py:355` via scheduler (**D** 09:30/09:45/10:00 IST) | picks `/picks/live` → `get_decisions` `picks_router.py:259` (**R**); scheduler dedupe `scheduler.py:117` |
| `falcon_replay_cache` (PROD) | replay_cache `:151,239` / replay_warmer `replay_warmer.py:79` (B + admin re-cache) | picks replay endpoints `replay_cache.py:176,215` (**R**) |
| `falcon_auth_log` (PROD) | auth bot / auth_scheduler (D, 21 cycles) | admin auth widget (R, admin) |
| `falcon_trade_*`, `falcon_position_*`, `falcon_premarket_staging`, `falcon_engine_config`, `falcon_trail_config` (PROD) | Falcon trade/monitor/premarket flows (R, operator-only) | Falcon `/trade`, `/premarket`, `/config`, monitor (R, operator-only) |
| `portfolio_definitions` (PROD) | `portfolio_engine.seed_*` `portfolio_engine.py:75` (**B**) | portfolios_router (**R**) |
| `portfolio_positions` / `portfolio_equity_history` / `portfolio_event_log` (PROD) | `portfolio_engine.run_eod_for_date` `portfolio_engine.py:562,601,613,748` (**D** EOD) | portfolios_router detail/positions/trades/equity `portfolios_router.py:108-620` (**R**) |
| `portfolio_yearly_performance` / `portfolio_monthly_performance` (PROD) | `scripts/import_persona_excels.py` (**O**, on Excel re-publish) | portfolios `/{slug}/performance` `portfolios_router.py:505-517` (**R**) |
| `power_user_users` (PROD) | auth/invite/billing routers (R) | paywall gate `dependencies.py:155` (**R**, every gated call); auth (R) |
| `power_user_subscriptions` / `power_user_billing_events` (PROD) | billing_router / Razorpay webhook (R) | billing_router (R) |
| `power_user_request_log` (PROD) | `dependencies.log_request` `dependencies.py:310` (**R**, every request) | anon rate-limit `dependencies.py:284` (R); DAU metrics (O) |
| `power_user_invite_codes` / `power_user_waitlist` / `power_user_watchlists` / `power_user_magic_links` / `power_user_push_subscriptions` (PROD) | invites/auth/push routers (R) | invites/auth/push routers (R) |
| `intraday_dataset_v2` (**intraday_mining.db**) | offline intraday mining (O) | persona_simulator P2 `_load_intraday_lookup` `persona_simulator.py:96-104` (**R via persona sim, cached 24h**) |
| RND `ohlc_daily` / `falcon_features` / pattern catalog (**RND**) | offline mining (O) | persona_simulator `load_full_patterns(rnd_db)` `persona_simulator.py:408-409` (**R, cached**); OHLC panel/bars come from PROD not RND (`:412-417`) |
| `ohlc_1min` (**RND**) | offline 1-min fetcher (not yet live — `admin_router.py:500` "not_implemented") | **None at request time.** RESEARCH/mining only. |

---

## 3. PROD-serve / PROD-generate / RESEARCH-only classification

| Table | DB | Class | Justification |
|---|---|---|---|
| `falcon_signals_live` | PROD | **PROD-serve + PROD-generate** | Written daily by signal_runner; read at request time by `/today/falcon-top-20`. |
| `falcon_features` | PROD | **PROD-serve + PROD-generate** | Daily feature snapshot; read at request time for pattern replay (bucket1/2). |
| `ohlc_daily` | PROD | **PROD-serve + PROD-generate** | Daily fetch; read at request time (sector return, day return, last-close MTM). Needs 252-day rolling window (see §4). |
| `ohlc_weekly` | PROD | **PROD-generate** | Rebuilt daily; consumed only by feature calc (not request-time). |
| `falcon_pattern_taxonomy` | PROD | **PROD-serve** | Read every Top-20 request; regenerated weekly. |
| `falcon_sectors` | PROD | **PROD-serve + PROD-generate** | Daily signal-gen + request-time sector calc. |
| `falcon_outcomes` | **RND** | **PROD-serve (cross-DB!)** | **Read at request time from the 14G RND DB** by Top-20 bucket2/baseline. See §4 flag. |
| `falcon_live_decisions` | PROD | **PROD-serve + PROD-generate** | Written 09:30/45/00; read by `/picks/live`. |
| `falcon_replay_cache` | PROD | **PROD-serve** | Request-time replay; warmed at boot. |
| `portfolio_definitions` | PROD | **PROD-serve** | Seeded at boot; read by portfolios. |
| `portfolio_positions/equity_history/event_log` | PROD | **PROD-serve + PROD-generate** | Written by EOD `run_eod_for_date`; read at request time. **Precomputed summaries.** |
| `portfolio_yearly/monthly_performance` | PROD | **PROD-serve** | Precomputed (Excel import); read by `/performance`. |
| `power_user_users` | PROD | **PROD-serve** | Read on every paywalled request (billing gate). |
| `power_user_subscriptions/billing_events` | PROD | **PROD-serve** | Billing flow. |
| `power_user_request_log` | PROD | **PROD-serve** | Written + read (rate limit) every request. |
| `power_user_invite_codes/waitlist/watchlists/magic_links/push_subscriptions` | PROD | **PROD-serve** | Auth/onboarding request paths. |
| `falcon_auth_log` | PROD | **PROD-generate** (ops) | Auth-bot cadence; read by admin widget only. |
| `falcon_signal_runs` / `falcon_notifications_out` / `falcon_top10_audit` | PROD | **PROD-generate** (ops/audit) | Cron-written; admin/internal reads. |
| `falcon_trade_*`, `falcon_position_*`, `falcon_premarket_staging`, `falcon_engine_config`, `falcon_trail_config` | PROD | **PROD-serve (operator-only)** | Single-operator auto-trade panel. Not end-user serving, but live request paths. |
| `intraday_dataset_v2` | intraday_mining.db | **PROD-serve (thin, persona P2 only)** | Read at request time via persona sim (cached). P2 is the BTST persona. |
| RND pattern catalog (`load_full_patterns`) | **RND** | **PROD-serve (cross-DB, cached)** | Persona sim reads RND for full pattern set at request time, but result is cached 24h and warmed at 03:00 restart. |
| `ohlc_1min` | **RND** | **RESEARCH-only** | No request-time reader. Fetcher not yet live. |
| `walkfwd_trades`, `stock_efficacy`, `strategy_efficacy_walkforward`, `live_signals`, `paper_trades`, `run_log`, `regime_calendar`, `strategies` | RND | **RESEARCH-only** | Offline mining/backtest tables; no app reader. |
| Everything in `kanida_universe_winB.db` (843M) | winB | **RESEARCH-only** | Window-B experiment; no reader. |
| `kanida_quant.db` (83M) tables | legacy | mostly **dead/legacy** | Only `kite_tokens` is live (operator auth). |

---

## 4. OHLC specifics (cost-critical)

### OHLC tables & approximate sizes
| Table | DB | Approx rows | Request-time reader? |
|---|---|---|---|
| `ohlc_daily` | PROD (573M) + RND (14G) | PROD ~recent window; RND multi-year full history | **YES (PROD)** — sector/day return, last-close MTM |
| `ohlc_weekly` | PROD | small (1 row/symbol/week) | No (feature calc only) |
| `ohlc_1min` | RND only | **~87.8M** (`config.py:48`, `admin_router.py:500`) | **NO** |
| `ohlc_5min` | universe_engine resample (`resample.py:96`) | research | No |

### Daily-OHLC lookback for signal generation — **the cloud rolling-window figure**

The binding constraint is **feature engineering**, computed in `universe_engine/engine/falcon_features.py`:

- `falcon_features.py:116` — `if len(daily_bars) < 252: return []` (whole symbol skipped without 252 bars)
- `falcon_features.py:133` — `if idx < 252: continue   # need 252 days for trailing-year high`
- `falcon_features.py:191` — `dist_high_252` uses `highs[idx-251 : idx+1]` (252-bar trailing-year high)
- `falcon_features.py:148` — `sma200` (200-bar SMA) — also < 252
- Weekly features need 21 weeks (`falcon_features.py:200`) ≈ 105 trading days — also < 252

**➡ Minimum rolling window the cloud DB must hold for `ohlc_daily` = 252 trading days (~1 calendar year) per symbol.**
That is the longest-lookback feature (`dist_high_252`). A safe operational buffer (holidays, the weekly-resample alignment, and the `daily_features` 60-day catch-up window at `daily_features.py:45`) suggests provisioning ~**260–300 trading days** of `ohlc_daily` per symbol. Liquidity (`avg_value_60d`) needs only 60 days (`signal_runner.py:83`), so 252 dominates.

### ohlc_1min / ohlc_5min at request time — **confirmed NOT read by any app path**
- Grep for `ohlc_1min|ohlc_5min` across the repo: every hit is in `universe_engine/` (research), or in `backend/power_user/config.py:48` (a comment) and `admin_router.py:500` (a "not_implemented" job descriptor), or in docs.
- No `backend/power_user/routers/*` or `backend/falcon/services/*` request path queries `ohlc_1min`/`ohlc_5min`.
- Intraday data consumed at request time comes from `intraday_dataset_v2` in `intraday_mining.db` (persona P2 only), **not** raw `ohlc_1min`.

**➡ `ohlc_1min` (87.8M rows, in the 14G RND DB) does NOT need to ship to the cloud.**

---

## 5. Backtest data — where it lives & how it's served

Two distinct backtest surfaces:

### 5a. Co-Trader portfolios (`/api/power/portfolios/*`) — **PRECOMPUTED summaries, PROD DB**
- Tables: `portfolio_positions` (every position open/closed), `portfolio_equity_history` (daily MTM), `portfolio_event_log`, `portfolio_yearly_performance`, `portfolio_monthly_performance`, `portfolio_definitions`. All in **PROD** (`db_schema_portfolios.sql`).
- Writer: `portfolio_engine.run_eod_for_date` (daily EOD) materialises positions + equity; `import_persona_excels.py` loads the year/month grids.
- Reader: `portfolios_router.py` reads these precomputed rows directly. The only raw-OHLC touch at request time is `_last_close` (`portfolios_router.py:623`) for current MTM of OPEN positions — a single-row `ORDER BY trade_date DESC LIMIT 1` per open symbol against PROD `ohlc_daily`. **No request-time re-simulation of history.**

### 5b. Persona backtests (`/api/power/personas/*`) — **RECOMPUTED at request time, cached 24h**
- Served by `persona_backtest_router.py` → `persona_simulator.simulate_persona(slug)` (single source of truth; no DB summary table).
- At request time `simulate_persona` (`persona_simulator.py:408-417`):
  - reads the **full pattern catalog from RND** (`load_full_patterns(rnd_db)`),
  - reads **OHLC panel + all bars + sectors + trading-days from PROD** (`load_panel/load_all_bars/build_sector_map/trading_days(PROD_DB, ...)`),
  - persona P2 additionally reads `intraday_dataset_v2` from `intraday_mining.db`.
- This is a true multi-year recompute, but **cached 24h** (`_STRAT_WR_TTL_SECONDS=86400`, `falcon_top20_explainer.py:47`) and warmed at the 03:00 IST restart, so it runs ~once/day, not per user request.
- The Top-20 cards' per-stock "Historical track record" also derives from this persona sim (`falcon_top20_explainer.py:52-109`), same 24h cache.

---

## 6. The flagged finding — APP-SERVING router reads the R&D DB at request time

**CONFIRMED.** Two request-time cross-DB reads into the 14G RND DB exist:

1. **`falcon_top20_router.py:94-96`** opens `POWER_RND_DB_PATH` read-only **per request**, passes `rnd_con` into `build_falcon_top20`. Inside, **`falcon_top20_explainer.py:1103`** queries `falcon_outcomes` from RND for each pick's historical evidence (bucket 2), and `:1033/:1094` query the per-stock lifetime baseline from RND. This is the "historical evidence bucket" the prompt suspected — **the suspicion is correct.** Mitigated by a 10-min in-process LRU cache (`falcon_top20_router.py:39-56`), but a cold cache still hits the 14G RND DB live on the user-facing `/power/today` endpoint.

2. **`persona_simulator.py:408-409`** (`load_full_patterns(rnd_db)`) reads the RND pattern catalog at request time, but only the pattern *definitions* (not OHLC) and behind a 24h cache.

**Cloud implication:** to serve `/power/today` without shipping the full 14G RND DB, the cloud needs `falcon_outcomes` (827k rows) — and the patterns used by persona sim — extracted/replicated into the PROD DB (or a small dedicated "evidence" DB). The raw `ohlc_1min` (87.8M) and the mining/walk-forward tables do **not** need to ship.

---

## 7. Minimal cloud DB — derived recommendation

**Ship to cloud (PROD-serve + PROD-generate):**
- Power-user tables (auth/billing/request_log/invites/watchlist/push/magic_links)
- `falcon_signals_live`, `falcon_features`, `falcon_pattern_taxonomy`, `falcon_sectors`, `falcon_live_decisions`, `falcon_replay_cache`
- `portfolio_*` (definitions/positions/equity_history/event_log/yearly/monthly)
- `ohlc_daily` — **rolling 252 trading days (provision ~260–300) per symbol**
- `ohlc_weekly` (small)
- `falcon_outcomes` — **must be replicated from RND** (827k rows) to keep Top-20 bucket-2 serving without the 14G DB
- Falcon operator-runtime tables (trade/position/premarket/config) if the auto-trade panel runs in cloud
- `intraday_dataset_v2` (30M) if persona P2 must serve

**Keep on research laptop (RESEARCH-only):**
- `ohlc_1min` (87.8M), `ohlc_5min`, `kanida_universe_winB.db` (843M), full multi-year `ohlc_daily`/`falcon_features` history beyond the rolling window, `walkfwd_trades`, `stock_efficacy`, `strategy_efficacy_walkforward`, `live_signals`, `paper_trades`, `regime_calendar`, `strategies`, the full pattern mining catalog (only promoted/taxonomy + outcomes needed in cloud)
- The weekly mining/remine pipeline that regenerates `falcon_pattern_taxonomy` (publish output to cloud)
