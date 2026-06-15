# Step 0 Audit — Self-Improving Engine (READ-ONLY findings)

**Date:** 2026-06-14 · **Branch:** `feat/self-improving-engine` (in `kanida-dev` worktree) · **No code written.**
Per the spec: report all findings before Step 1. This is that report.

> Verdict: the engine, patterns, walk-forward simulator, and journey data sources all exist and are strong — BUT **two load-bearing assumptions in the spec do not match the code**, and there are real **data-coverage limits**. Both force decisions before Step 1.

---

## A. Spec ↔ code discrepancies (decision-forcing)

### A1. 🔴 There is NO "one bot per stock"
Spec: *"The Falcon engine already has one pattern mining bot per stock … extend the existing bots."*
Reality: mining is per **(year × outcome-target × scope)**, scope ∈ {`universe`, `sector:XYZ`} — never per stock. One shallow DecisionTree per slice. (`universe_engine/engine/falcon_miner.py:237-238`, scopes `:14-16`, tree `:167-171`.) The miner explicitly **defers** per-stock: *"Stock-specific mining deferred to Phase 5; per-stock samples are too thin"* (`falcon_miner.py:18-20`).
**Impact:** building "per-stock bots" is NEW work the miner deliberately avoided (thin per-stock samples), not an extension.

### A2. 🔴 Parity reference is mismatched
Spec says reference `sim_p1_v3_fixed35k.py`, but the locked numbers (avg 258.34%, worst +5.35%, **min_fires=10**, **₹50k/10 pos**) are the **Falcon Top 10 persona**, not sim_p1.
- `sim_p1_v3_fixed35k.py` = ₹35k, top-**14**, **min_fires=2**, ranks by **sum_lift** (`sim_p1…:41-51`, `:99-101`) → produces DIFFERENT numbers.
- Falcon Top 10 persona = ₹50k, top-10, min_fires=10, **avg_lift** (`persona_simulator.py:291-333`) → matches the locked numbers. Real parity test: `backend/power_user/tests/test_persona_parity.py`.
**Impact:** Phase-1 parity must target the **Falcon Top 10 persona**, not sim_p1.

### A3. 🟠 `weekly_remine.py` is publish-only
It does NOT mine/label/validate — those are manual R&D CLI scripts (`universe_engine/scripts/falcon_{mine,validate}.py`, `falcon_build_dataset.py`) with **no scheduled trigger**. `weekly_remine.py` only calls `publish_patterns()` (`weekly_remine.py:1-12`). "Extend the weekly pipeline" = extend the R&D CLI chain.

### A4. 🟠 Minor
- `avg_lift = sum_lift/n_fires` lives only in the productized core (`persona_engine_core.py:219-221`), not sim_p1.
- 38-feature `FEATURE_COLS` is **duplicated in 4+ files** — any new data feature must be added to all (`falcon_miner.py:125`, `falcon_validator.py:59`, `signal_runner.py:22`, `sim_sweep.py:28`, `feature_cols.py`).
- `sector_specific` patterns are promoted but never used (all consumers filter to universal+regime_dependent).

---

## B. Step 0 items — confirmed facts

| Spec item | Finding |
|---|---|
| Per-stock bots | None — slice-based (A1). `falcon_miner.py` |
| Weekly mining pipeline | R&D CLI (mine→label→validate) + `weekly_remine.py` publish-only (A3) |
| Promotion logic | `falcon_validator.promote_patterns` thresholds: MIN_OOS_LIFT_PP=5, MIN_OOS_YEARS_PASS=2, MIN_OOS_OBS=30; classification universal/regime_dependent/sector_specific (`falcon_validator.py:53-56, 260-318`). Publish cutoff `mined_year >= year - mining_window_years` (default 4) (`publish_patterns.py:151-155`) |
| `falcon_pattern_taxonomy` | **865 rows confirmed** (PROD). 13 cols: pattern_id, mined_year, classification, target, regime, oos_hit_rate, lift_pp, base_rate_pct, n_years_passed, rule_text, english, blurb, rule_json |
| `sim_p1_v3_fixed35k.py` | Found at the spec's path. Cash-only walk-forward; full mechanics documented (group_by_signal_date, skip_already_held, rolling-4yr, next-open entry, −7%/+12%/10d-Donchian, 5bps×2+30bps, ₹5L/yr reset). NOTE: it's ₹35k/top14/min_fires=2/sum_lift (≠ Falcon Top 10). Imports helpers from `sim_sweep.py` (a 3× MTF sweep — leverage NOT used by P1). |
| `PERSONA_CONFIGS["falcon-top-10"]` | Confirmed: min_fires=10, sort_key="avg_lift", group_by_signal_date=True, ₹50k, top_n=10, hold=7, −7%/+12%/10d (`persona_simulator.py:291-333`) |
| Subagent framework | 4 workflows (daily `_pipeline.py`, weekly R&D+publish, realtime `falcon/trade/*`, user-action). Personas via `PERSONA_CONFIGS` + `PersonaRunConfig`. Signal gen: `signal_runner.generate_signals_for_date` |

---

## C. RND DB data coverage (drives intraday/futures scope)

| Table | Range | Symbols | Notes |
|---|---|---|---|
| `ohlc_daily` (RND) | 2016-01-01 → **2026-05-07** | 2,122 | 2.03M rows. **~5 weeks STALE** vs PROD signals (Jun 12) — walk-forward "to today" needs backfill |
| `ohlc_1min` | **2024-05-13** → 2026-05-11 | 499 | **1-min start = 2024-05-13** → intraday/live-detection limited to this window, no extrapolation |
| `ohlc_5min` | 2025-10-28 → 2026-04-30 | 143 | thin |
| `ohlc_futures_daily` | 2026-02-25 → 2026-04-30 | 123 | only ~2 months, 7,872 rows → **futures personas cannot be backtested** until backfilled |
| `falcon_outcomes` | — | — | 827,379 rows |
| `falcon_promoted_patterns` (RND) | mined 2017-2025 | — | **1,943** total (1,815 universal / 11 regime_dependent / 117 sector_specific) |
| `falcon_pattern_candidates` (RND) | mined 2017-2025 | — | 4,794 |

**Tables that DON'T exist yet (all NEW):** `index_expiry_calendar`, `fo_stock_master`, and all 8 new falcon_* journey/study tables. No options-chain or index-expiry data anywhere.

---

## D. Runtime-confirm items — DEFERRED (Kite token currently INVALID, weekend)
Per spec these are runtime confirmations; cannot pull now (token invalid, markets closed):
- Current F&O-eligible list, lot sizes, margins → from Kite (runtime).
- Index expiry calendar (Nifty weekly Thu→Tue change) → from NSE + Kite.
- 1-min confirmed start date → **answered from RND: 2024-05-13** (Kite re-confirm at backfill time).

---

## E. Decisions required before Step 1 (with recommendations)

1. **Per-stock bots (A1).** Recommend: **build the self-improvement/journey/classification layer on the EXISTING slice-mined patterns + resolved trades** (this is ~90% of the spec's value: journey capture, big-winner/loser study, walk-forward baseline, per-persona suitability, weekly self-improvement, auto-trade readiness). Treat true per-stock mining as a later phase (the miner's own "Phase 5"). Reconcile the spec's "extend per-stock bots" wording to "derive per-stock pattern intelligence from existing patterns + outcomes."
2. **Parity reference (A2).** Recommend: parity = **Falcon Top 10 persona** (`persona_engine_core` + `test_persona_parity.py`), not sim_p1. Run the existing parity test in Step 2.
3. **DB dialect/placement (D4).** Recommend: new heavy journey/study tables live in the **RND research DB** (SQLite), with compact summaries published to PROD per the cloud architecture; the `falcon_pattern_taxonomy` ALTER applies in both. Schemas are spec'd in Postgres — translate to SQLite for now (Postgres = Phase 4).
4. **Data limits (C).** Acknowledge: intraday only from **2024-05-13**; **futures personas effectively unavailable** (2 months) until Kite backfill; RND daily needs a ~5-week backfill (weekday + valid token) for walk-forward "to today." Falcon Top 10 (EOD daily 2021→2026) is **fully buildable now** from existing 2016-2026 daily coverage (minus the recent 5-week tail).

**Falcon Top 10 (BUILD FIRST) is not blocked** — daily coverage + patterns + the parity persona all exist. The blockers above only affect intraday/futures/options personas (which are stubs in Phase 1 anyway).
