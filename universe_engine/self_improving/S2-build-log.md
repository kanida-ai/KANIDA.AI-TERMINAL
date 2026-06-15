# Step 2 Build Log — Falcon Top 10 Baseline Populator

**Date:** 2026-06-14 · **Agent:** BuildAgent-S2 · **Worktree:** `C:\Users\SPS\Desktop\kanida-dev` (branch `feat/self-improving-engine`)
**Scope:** Create `universe_engine/self_improving/build_baseline.py` — runs the parity-validated Falcon Top 10 walk-forward and populates `falcon_baseline_trades` + `falcon_pattern_contributions` with the full intra-period journey. No commits. No existing/shared code modified (INV2). Not executed (no Python in env — validated by careful reading).

## File created
| File | Purpose |
|---|---|
| `build_baseline.py` | Phase-1 baseline populator. Imports & re-runs the persona engine; computes journey + flags; writes the two RND tables idempotently. CLI: `--rnd-db`, `--prod-db`, `--years`, `--limit-year`, `--dry-run`. |

## How the engine is reused (parity preserved BY CONSTRUCTION)

The trade set is **not re-implemented** — it is produced by the same functions `persona_simulator.simulate_persona("falcon-top-10")` calls, with an EXACT replica of its per-year body:

`simulate_persona` (sim = `backend/power_user/services/persona_simulator.py`):
- Loads patterns from **RND** via `load_full_patterns` (sim:409 → core:61), panel+bars from **PROD** via `load_panel`/`load_all_bars` (sim:412-415 → core:86/109), `build_sector_map` (sim:416 → core:140), `trading_days` (sim:417 → core:127).
- Per year Y in the window (sim:450-474):
  - `elig = eligible_patterns_for_year(all_pats, Y)` (core:169)
  - `sigs = compute_year_signals(X, sym, dt, year_mask, elig, min_fires=run_cfg.min_fires)` (core:181)
  - Falcon Top 10 only: rewrite each `s["score"] = s["avg_lift"]` (sim:462-464)
  - group sigs by `signal_date` (sim:465-467)
  - `r = simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)` (core:311)

`build_baseline._build_falcon_run_context` + `_simulate_one_year` reproduce this **line-for-line**, using the **same `PERSONA_CONFIGS["falcon-top-10"]` `run_cfg`** (top_n=10, ₹50k, hold=7, −7%/+12%/10d Donchian, `sort_key="avg_lift"`, `min_fires=10`, `group_by_signal_date=True`), the same `sim_start`/`sim_end`, and the same ₹5L yearly reset. Because every input and every call is identical, **`simulate_year`'s `closed_trades` + `open_at_end_trades` are the same trades the parity run produces** (`test_persona_parity.py` is the parity gate). I import — never copy — `simulate_year`, `compute_year_signals`, `eligible_patterns_for_year`, the loaders, and `rule_mask`.

**Why not just call `simulate_persona()`?** It returns API-clean rows (`_trade_to_api_row`) and exposes raw dicts only for the *latest* year (`_current_year`). I need raw per-trade fields (`entry_bar_idx`, `n_fires`, `score`, `exit_reason`, …) **and** the loaded `bars` for **all** years to build the journey. The instructions explicitly permit replicating the persona_simulator setup exactly and calling `simulate_year` per year — which is what this does. No shared code was changed, so parity is intact.

**Import root:** the backend runs with `<repo>/backend` on `sys.path` (imports as `power_user.*`; see `backend/main.py`; there is no `backend/__init__.py`). The engine's relative `from .. import config` (= `power_user.config`) only resolves under that root, so `build_baseline` prepends `<repo>/backend` to `sys.path` and imports `power_user.services.*`.

## Journey math (Falcon Top 10, 7-day hold)

For each trade, computed from the **same `bars`** the sim traded (no refetch), using the trade's own `entry_bar_idx` (== `_bars_start_idx`) into `bars[symbol]`:
- entry reference = the entry bar's **raw open** (`bs[entry_bar_idx]["open"]`, unslipped) — so the journey reflects the true market path; consistent with how `simulate_year` measures `high_water = close/entry_px`.
- For hold day d=1..7 (only days that have a bar — the sim itself truncates at `len(bs)-1`, trailing days stay NULL): `dN_{open,high,low,close}_ret = bar/entry_open − 1` (in %).
- `peak_ret_during_hold` = max daily-high ret (+ `peak_day`); `trough_ret_during_hold` = min daily-low ret (+ `trough_day`).
- `peak_before_trough` / `trough_before_peak` from the day ordering; `peak_sustained` = final-hold-day close ret ≥ 0.9 × peak ret (for positive peaks; ≥ peak for non-positive).

**Table-8 Falcon Top 10 flags:**
- `big_winner_flag` = peak_ret > **12%** at any D+1..D+7.
- `big_loser_flag` = trough_ret < **−7%** at any point **OR** `exit_reason == INIT_STOP`.
- `big_winner_peak_day` (when winner), `big_loser_trough_day` (only when the −7% trough drove it, not the stop), `big_winner_sustained` (final close still > 12%), `big_loser_recovered` (final close back above −7% after a −7% trough).

## Pattern contributions — faithfully sourced, nothing fabricated

The parity path **cannot** tell us which patterns fired per pick:
- `load_full_patterns` (core:61) **drops** `pattern_id`/`regime`/`classification` — keeps only `mined_year`/`rule`/`lift`.
- `compute_year_signals` (core:181) stores only the **count** (`n_fires`) and **sum** (`sum_lift`) of fires — never the set of patterns.

So fired pattern_ids are **re-derived read-only**: `load_patterns_with_ids` runs the **identical** query/JOIN/`in_drawdown_bounce` filter as the parity loader but additionally selects `pattern_id`, `mined_year`, `classification`, `precision_pct`. Restricted to the year via the **same** `eligible_patterns_for_year`, each pattern's `rule_mask` is re-evaluated against the **trade's own feature row** (`X[ridx]`, recovered by `(symbol, signal_date)`). Identical query + identical eligibility + identical `rule_mask` ⇒ identical fire-set. A **parity guard** asserts the re-derived fire count == the trade's `n_fires` (the robust invariant; survives the avg_lift score rewrite). Any mismatch is printed as a PARITY WARNING (not silently swallowed). This path is entirely separate from the sim run — it never touches the parity trades.

Per fired pattern, one `falcon_pattern_contributions` row: `pattern_id`, `pattern_mined_year`, `lift_pp` = `avg_oos_year_lift_pp` (the exact value the sim summed), `oos_hit_rate` = candidate `precision_pct/100` (closest available proxy), `realized_outcome` = the trade's `net_ret_pct`, `sector`, `big_winner_flag`/`big_loser_flag` from the trade.

## Columns populated vs NULL (`falcon_baseline_trades`)

**Populated (Phase-1 producible):** persona, signal_date, entry_date, exit_date, symbol, sector, engine_rank, avg_lift, n_fires, sum_lift, top_3_pattern_ids (JSON, lift-sorted), pattern_mined_years_used (JSON), entry_price, exit_price, exit_reason, hold_days_calendar, hold_days_trading, shares, actual_deployed, gross_pnl, fees, net_pnl, net_ret_pct, **all d1..d7 open/high/low/close ret**, peak/trough ret+day, peak_before_trough, trough_before_peak, peak_sustained, big_winner_flag, big_loser_flag, big_winner_peak_day, big_loser_trough_day, big_winner_sustained, big_loser_recovered.

**Left NULL (need later data / later steps), and why:**
- `stock_name` — no symbol→name source in the parity data path (sector map only).
- **Intraday** (`intraday_intervals_json`, `peak_ret_during_session`, …) — persona='intraday' only; N/A for EOD Falcon Top 10.
- **Sector/market regime** (`sector_rank_on_signal_date`, `sector_20d_rs`, `sector_tailwind`, `n_sector_peers_in_top10`, `market_regime_on_signal_date`, `sector_regime_on_signal_date`) — regime/sector-attribution step (no regime data in pattern tables; sector RS not computed here).
- **Sector attribution** (`sector_ret_same_period`, `stock_vs_sector`, `move_type`, `*_post_hold`) — needs sector index series + post-hold bars (later step).
- **Signal timing** (`signal_still_valid_at_open`, `signal_validity_days`, `entry_timing_quality`, `intraday_confirmed`, …) — `falcon_signal_validity` step.
- **Repeater intelligence** (`prior_appearances_*`, `consecutive_days_in_top50`, `last_outcome_when_repeated`, `repeater_type`) — repeater step.
- **Self-improved ranking** (`improved_score`, `improved_rank`, multipliers, `rank_change*`) — Phase 2.
- **Auto-trade** (`autotrade_decision`, `autotrade_block_reason`) — Phase 2 auto-trade readiness.
- **Post-exit D+8..D+60** — owned by the separate child table `falcon_post_exit_tracking` (per spec: incremental, never on the main row).

**`engine_rank`** = rank within the pick's own `signal_date` cohort by score (avg_lift) descending — the exact order `simulate_year` accepts candidates in; faithful and derived from the trade set itself.

## Columns populated vs NULL (`falcon_pattern_contributions`)
**Populated:** trade_id (post-insert FK), signal_date, symbol, persona, pattern_id, pattern_mined_year, lift_pp, oos_hit_rate, realized_outcome, sector, big_winner_flag, big_loser_flag.
**NULL:** `regime`, `market_regime`, `sector_regime` (no regime column in the pattern tables — classification ≠ market regime, deliberately not conflated), `move_type` (sector-attribution step).

## Idempotency & safety
- Single transaction (`BEGIN` … `commit`/`rollback`), `PRAGMA foreign_keys=ON`.
- Clean rebuild per run: `DELETE FROM falcon_pattern_contributions WHERE persona='falcon_top10'` then `DELETE FROM falcon_baseline_trades WHERE persona='falcon_top10'` (child before parent), then INSERT. Other personas' rows untouched.
- Parameterized inserts (column lists `_BASELINE_COLS` / `_CONTRIB_COLS`); contributions get `trade_id` from `cursor.lastrowid` of the parent insert.
- `--dry-run` runs the full sim + journey + contribution compute and prints the per-year summary (return%, n_closed, contribs) **and** the parity comparison table, writing nothing.
- All rows are built **before** any write; on exception the transaction rolls back.

## Parity self-check the run prints
Per-year table compares computed return% / n_closed against the locked numbers:
`2021 +5.35%/15 · 2022 +76.36%/317 · 2023 +564.47%/810 · 2024 +469.05%/846 · 2025 +346.64%/644` (match = within 0.05pp and exact n). This is an eyeball aid; the authoritative gate remains `pytest backend/power_user/tests/test_persona_parity.py` (Falcon Top 10).

## Risks / notes for the audit agent
- **Not executed** (no Python in env). Validate first with `--dry-run` against the RND DB, then confirm the printed per-year table matches the locked parity numbers AND that "parity guard: all re-derived contributions reproduced trade n_fires exactly" prints with zero warnings.
- **`sim_end` is dynamic** (`_SIM_END_TODAY`, computed at `persona_simulator` import). The latest partial year's return won't match a fixed locked number — only 2021-2025 are locked. 2026 is partial/open.
- **Data staleness (STEP0 §C):** RND `ohlc_daily` ends 2026-05-07; the persona uses **PROD** OHLC/features (`PROD_DB`), which is fresher. Parity is defined against the persona sim (PROD-sourced), so this is correct — but the audit agent should confirm the chosen `--prod-db` is the same DB the parity test runs on.
- **`oos_hit_rate` proxy:** uses candidate `precision_pct` (in-mining precision), not a true held-out OOS hit rate (the validator stores per-test hit rates in `falcon_pattern_validations`; wiring that in is a later enrichment). Documented, not fabricated.
- **FK enforcement** requires `PRAGMA foreign_keys=ON` (set in `_write`). The S1 schema declares the FK inline; deletes are ordered child-first so enforcement won't block the rebuild.
- **`top_3_pattern_ids`** is the 3 highest-`lift_pp` fired patterns (lift-sorted), stored as a JSON array per the SQLite translation (`INTEGER[]` → TEXT).
