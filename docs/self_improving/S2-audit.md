# S2 Audit — build_baseline.py (Falcon Top 10 baseline populator)

**Agent:** AuditAgent-S2 (read-only on code) · **Date:** 2026-06-14
**Target:** `universe_engine/self_improving/build_baseline.py` (+ `S2-build-log.md`)
**Method:** Read the actual source of `build_baseline.py`, `persona_simulator.py`, `persona_engine_core.py`, `schema_self_improving.sql`, `feature_cols.py`; verified git status. Did not execute anything.

## VERDICT: GREEN

No RED findings. The trade set is reproduced by construction, no shared code or PROD/existing tables are touched, journey/flag math is correct, and the INSERT matches the schema. `--dry-run` is safe to run as the gate.

---

## Critical #1 — PARITY PRESERVED BY CONSTRUCTION ✅ CONFIRMED

`_build_falcon_run_context` + `_simulate_one_year` reproduce `persona_simulator.simulate_persona("falcon-top-10")`'s per-year body (sim:401-474) call-for-call, with the SAME objects:

| Element | persona_simulator | build_baseline | Match |
|---|---|---|---|
| run_cfg | `PERSONA_CONFIGS["falcon-top-10"]["run_cfg"]` (top_n=10, ₹50k, hold=7, init_stop −0.07, trail 0.12, lookback 10, target None, sort_key avg_lift, min_fires=10, group_by_signal_date=True) | same dict object (`PERSONA_CONFIGS[PERSONA_SLUG]`, build:136) | ✅ identical object — not a re-declared copy |
| patterns | `load_full_patterns(rnd_db)` from RND (sim:409) | `load_full_patterns(rnd_db)` (build:142) | ✅ |
| panel/bars | `load_panel(PROD_DB,…)`, `load_all_bars(PROD_DB,"2020-12-01",sim_end)` (sim:412-415) | `load_panel(prod_db,…)`, `load_all_bars(prod_db,"2020-12-01",sim_end)` (build:143-144) | ✅ same loaders, same pad-start "2020-12-01" |
| eligibility | `eligible_patterns_for_year(all_pats, Y)` (sim:451) | `eligible_patterns_for_year(all_pats, Y)` (build:188) | ✅ rolling-4yr, same defaults |
| signals | `compute_year_signals(X,sym,dt,year_mask,elig,min_fires=run_cfg.min_fires)` (sim:454) | identical (build:190) | ✅ min_fires=10 plumbed |
| avg_lift rewrite | `if sort_key=="avg_lift": s["score"]=s["avg_lift"]` (sim:462-464) | identical (build:194-196) | ✅ |
| group-by-signal_date | `sigs_by_sd[s["signal_date"]].append(s)` (sim:465-467) | identical (build:198-200) | ✅ |
| year window | `year_end = sim_end if last year else f"{Y}-12-31"` (sim:469) | identical (build:202-204) | ✅ |
| simulate_year call | `simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)` (sim:474) | identical (build:208) | ✅ |
| cash reset | `cash_per_year` = ₹5L, fresh per year (sim:405,474) | `cash_start = cfg_dict["cash_per_year"]`, passed per year (build:140,208) | ✅ ₹5L yearly reset |
| years window | `sorted({int(d[:4]) for d in all_td})` (sim:437) | identical (build:149) | ✅ |

`simulate_year` is **imported, never copied** (build:111). Closed + open-at-end trades are therefore the exact parity trade set. The only divergence vs `simulate_persona` is that build_baseline does NOT call the P2 intraday filter wiring or the P4 `extra_entry_set` / `split_15_mode` setup — correctly, because the Falcon Top 10 config has neither `_uses_intraday_filter` nor `_first_td_of_month_only` nor `split_15_mode`. No trade-set impact.

**Re-derivation does not alter the trade set.** Contributions are computed from a separate read-only `load_patterns_with_ids` query and `rule_mask` evaluation against `X[ridx]`; they never feed back into `simulate_year`. The parity guard (build:402-405) asserts re-derived fire count == trade `n_fires` and only emits a warning, never mutates.

## Critical #2 — NO SHARED-CODE / PROD / EXISTING-TABLE MUTATION ✅ CONFIRMED

- `git status` on `C:\Users\SPS\Desktop\kanida-dev` shows ONLY two new untracked files: `universe_engine/self_improving/build_baseline.py` and `S2-build-log.md`. (This audit adds `docs/self_improving/S2-audit.md`.) `git diff HEAD -- persona_engine_core.py persona_simulator.py` is **empty**. No shared code modified (INV2 holds).
- build_baseline only IMPORTS from `power_user.services.*` (build:98-116). All imported names resolve, incl. `in_drawdown_bounce` (re-exported into `persona_engine_core`'s namespace at core:38 via `from .feature_cols import …`).
- **Writes go only to the RND DB** (`rnd_db`, build:705/717) and only to the two NEW tables `falcon_baseline_trades` + `falcon_pattern_contributions`. PROD DB is opened **read-only in practice** — `load_panel`/`load_all_bars`/`build_sector_map`/`trading_days` only `SELECT`. No write path touches PROD or any pre-existing table.

## #3 — Journey math ✅ correct, no off-by-one

- Entry reference = `bs[eidx]["open"]` where `eidx = entry_bar_idx == _bars_start_idx` — the next-open bar after signal_date that `simulate_year` entered on (core:354-374, 519). Same `bars` the sim used (no refetch). ✅
- **Indexing is correct:** for hold day `d` (1-based), bar = `bs[eidx + (d-1)]`, so d1 IS the entry bar (build:261). This matches the sim's own `trading_hold = bar_idx - entry_bar_idx + 1` convention (core:432) where the entry day counts as day 1. No off-by-one.
- `n_days = min(hold_days, len(bs) - eidx)` correctly truncates trailing days past available data → those d-columns stay NULL (build:259), mirroring the sim truncating at `len(bs)-1`.
- peak/trough from per-day high/low + day; `peak_before_trough`/`trough_before_peak` are mutually-exclusive on strict `<` (equal-day → both 0, correct), NULL when a side is missing (build:281-288). ✅
- **Big-winner/loser flags match Table 8:** `big_winner = peak_ret > 12%` any D+1..7 (build:300); `big_loser = trough_ret < −7% any point OR exit_reason == INIT_STOP` (build:301-303). Thresholds (12.0 / −7.0 / INIT_STOP) are correct. ✅ `big_winner_peak_day` set only when winner; `big_loser_trough_day` attributed only when the −7% trough (not the stop) drove it — a sound, documented choice. `big_winner_sustained` / `big_loser_recovered` keyed off final-available-hold-day close.

Minor (non-blocking): for **open-at-end** trades `exit_reason` is forced NULL (build:455) before `compute_journey` reads `trade.get("exit_reason")`. An open-at-end position can never carry `INIT_STOP` anyway (the sim only tags it `OPEN_AT_BACKTEST_END_MTM`), so the `big_loser_stop` branch is correctly inert for them; the `trough < −7%` branch still applies. No bug — flagged only for completeness.

## #4 — Pattern-contributions re-derivation ✅ sound

- `load_patterns_with_ids` uses the **identical** source query + JOIN + `WHERE classification IN ('universal','regime_dependent')` + `in_drawdown_bounce` filter as `load_full_patterns` (core:69-83), enriched only with `pattern_id`, `mined_year`, `classification`, `precision_pct` (build:344-366). Same fire-set by construction.
- Restricted to the year via the **same** `eligible_patterns_for_year(pats_with_ids, Y, retention=RETENTION_YEARS, mining_start=MINING_START_YEAR)` (build:593) — explicit args equal the defaults used in the sim path, so the eligible window is identical.
- `rule_mask` (imported, core:154) re-evaluated against the trade's own feature row `X[ridx]` recovered by `(symbol, signal_date)` (build:386-398).
- **Parity guard present** (build:402-405): re-derived fire count must equal `trade.n_fires`; mismatches accumulate as PARITY WARNINGS and print (build:689-695). `n_fires` is the robust invariant (survives the avg_lift score rewrite).
- **Honest sourcing:** `lift_pp = avg_oos_year_lift_pp` (the exact value summed into the score); `oos_hit_rate = precision_pct/100` (labeled in code+log as the closest available proxy, NOT a true held-out OOS rate); `regime = NULL` (no regime column exists in the pattern tables — `classification` is deliberately not conflated with market regime). Nothing fabricated.
- Re-derivation is fully read-only and does not alter the trade set.

## #5 — Write safety / idempotency ✅

- Writes only to the two NEW RND tables. `PRAGMA foreign_keys=ON` (build:719). Single transaction `BEGIN … commit`, `rollback` on any exception (build:720,744-747).
- Idempotent rebuild scoped to `persona='falcon_top10'`: DELETE contributions (FK child) THEN baseline (parent), then INSERT (build:723-728). Other personas untouched; child-first delete order respects FK enforcement.
- Parameterized inserts via `_BASELINE_COLS` / `_CONTRIB_COLS` placeholder lists (build:730-742); `trade_id` taken from parent `cursor.lastrowid`.
- `--dry-run` runs the full sim + journey + contribution compute + prints summaries and writes **nothing** (build:700-702), returning before `_write`.

## #6 — Column population vs schema ✅ no mismatch

- All 67 names in `_BASELINE_COLS` (build:510-529) exist in `falcon_baseline_trades` (schema:34-128) — verified incl. all d1..d7 open/high/low/close, peak/trough+day, the three boolean journey cols, and all six big-winner/loser cols. No typos.
- All 16 names in `_CONTRIB_COLS` (build:531-536) exist in `falcon_pattern_contributions` (schema:134-153). `trade_id` is set post-insert before each contrib INSERT (build:741). No typos.
- **NOT NULL safety:** `falcon_baseline_trades` NOT NULL cols are persona/signal_date/entry_date/symbol — all always populated (build:470-475; persona is a constant). `falcon_pattern_contributions` NOT NULL cols are signal_date/symbol/persona/pattern_id/pattern_mined_year — all populated from fired patterns (build:645-661). `id` is AUTOINCREMENT; `created_at` has a DEFAULT. No NOT NULL column left unset → INSERT will not crash on a constraint.
- NULLs only where documented (stock_name, intraday block, sector/market regime, sector attribution, signal timing, repeater, Phase-2 ranking, auto-trade, post-exit) — all of these are nullable in the schema.

## Notes for orchestrator

- **`--dry-run` is SAFE to run as the gate** — it performs zero writes (open/SELECT only on PROD; no DB connection opened to RND for writing in dry-run). The gate is: confirm the printed per-year table matches the locked parity numbers (2021 +5.35%/15 · 2022 +76.36%/317 · 2023 +564.47%/810 · 2024 +469.05%/846 · 2025 +346.64%/644 — within 0.05pp and exact n) AND that "parity guard: all re-derived contributions reproduced trade n_fires exactly" prints with **zero** PARITY WARNINGS.
- 2026 is partial/open (`sim_end = _SIM_END_TODAY`, dynamic) and is correctly not locked. Confirm `--prod-db` is the same DB the parity test (`test_persona_parity.py`) runs against; default resolves to `config.POWER_DB_PATH` (PROD) which is correct.
- The authoritative parity gate remains `pytest backend/power_user/tests/test_persona_parity.py` (Falcon Top 10); the per-year print is an eyeball aid only.
