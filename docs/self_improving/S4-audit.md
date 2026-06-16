# S4 Audit — build_signal_validity.py

**Auditor:** AuditAgent-S4 (READ-ONLY) · **Date:** 2026-06-15 (IST)
**Target:** `universe_engine/self_improving/build_signal_validity.py` (+ `S4-build-log.md`)
**Schema:** `falcon_signal_validity` (schema_self_improving.sql:227-249) + 6 taxonomy signal-timing cols (taxonomy_columns.sql:46-51)

## VERDICT: GREEN

No RED conditions found. All 6 verification points pass. The deferred deep intraday rule-detection is documented (build-log §"Deferred", code header lines 129-134) and is EXPECTED, not RED.

---

## 1. Decay re-sims reuse the parity exit faithfully — PASS

- `_resim_entry_at` (build_signal_validity.py:257-269) calls **only** `sds.simulate_independent_pick(sig, bars_sym, entry_idx, run_cfg)` (line 266) — the line-for-line copy of `persona_engine_core.simulate_year:384-436`. No exit math is re-implemented anywhere in the file. A `Grep` for `exit_reason|exit_px|init_stop_lvl|trail_stop|stop_lvl|target_lvl|net_pnl|gross_pnl|high_water` returns only reads of `net_pnl`/`actual_deployed` off the returned trade dict (`_net_ret_from_trade`, lines 250-254). **Zero divergent exit logic.**
- Entry-bar shift only: D+1 = `sd_idx` (build_signal_validity.py:307), D+2 = `sd_idx+1` (308), D+3 = `sd_idx+2` (309). `sd_idx` = first bar with `date > signal_date` (build_signal_validity.py:1012), identical to core:354 and to the study's `sd_idx` (build_signal_day_study.py:1065).
- Exit-parity cross-check against persona_engine_core: simulate_independent_pick's init_stop_lvl (core:400), trail window (core:406-408), target_lvl (core:409-410), priority SL→TARGET→TIME with gap-down `min(stop_lvl,open)` / `max(target_lvl,open)` (core:415-423), slip/fees (core:426-429) all match line-for-line.
- **D+1 == study value (sanity):** the study's `net_ret_pct` = `net_pnl/actual_deployed*100` (build_signal_day_study.py:562-565), computed from `simulate_independent_pick(sig, bs, sd_idx, run_cfg)` (build_signal_day_study.py:1068). S4's D+1 leg = `_net_ret_from_trade(simulate_independent_pick(sig, bars_sym, sd_idx, run_cfg))` = `net_pnl/actual_deployed*100`. Same sig, same bars, same `sd_idx`, same `run_cfg` ⇒ identical trade dict ⇒ **identical net_ret_pct**. CONFIRMED.

## 2. Validity rule sound + documented — PASS

- Single source `_valid_band(gap_frac, run_cfg)` (build_signal_validity.py:272-282): `init_stop < gap < target` with `target` falling back to `trail_trigger` (+0.12) then a documented 0.12 literal. Matches the locked Falcon Top 10 band (−7%, +12%).
- D+1: `gap = next_open/sd_close − 1` (build_signal_validity.py:302), base = **signal-date close** (`sd_close`, looked up at line 1010). Intraday 10am/11am/1pm: same `_valid_band`, current price = checkpoint price, **same base** sd_close (`_valid_at`, lines 441-446). Consistent band, correct base.
- No look-ahead: D+1 uses only the D+1 open; checkpoints use `_price_at_or_after` = open of first bar at/after that clock time (lines 383-391) — only price available at that instant. Rule documented in S4-build-log.md §"The VALIDITY RULE" and code header lines 32-48.

## 3. 1-min query bounded — PASS (no full-table scan)

- `_load_minute_bars_for` (build_signal_validity.py:359-380) queries `WHERE symbol = ? AND bar_time >= ? AND bar_time < ?` with `('{day} 00:00:00','{day} 23:59:59')` — a per-(symbol,date) index-prefix range scan on PK `(symbol, bar_time)`. **Never** a full-table scan of the 87.8M rows.
- The only other ohlc_1min read is `_one_min_window` (lines 338-356): a one-time `MIN/MAX(substr(bar_time,1,10))` aggregate to detect the window from data — not per-signal.
- Window detected from data (live MIN/MAX authoritative; constants 2024-05-13→2026-05-11 are a documented fallback, lines 210-213). Out-of-window entry dates → `null_row` (lines 433-434); symbols with no minute bars that day → `null_row` (lines 437-438). All intraday fields NULL, **no fabrication**.

## 4. HFCL — PASS

- Validity/decay taxonomy cols set only at `len(sig_rows) >= 18` (build_signal_validity.py:584); else the rec stays at its NULL defaults (lines 574-581).
- Intraday taxonomy cols set only at `len(inw) >= 18` in-window signals (line 603); else NULL. `HFCL_MIN_N = 18` (line 222).
- "Signals" = resolved contributed signals joined via `falcon_pattern_contributions` (persona=`falcon_top10`) AND `falcon_baseline_trades.net_ret_pct IS NOT NULL` (`_load_signal_to_patterns`, lines 503-533). All 6 cols (`signal_validity_next_day_pct`, `avg_signal_decay_days`, `signal_best_entry_window`, `intraday_detectable`, `intraday_detection_accuracy`, `intraday_false_positive_rate`) NULL when threshold unmet.

## 5. Idempotent + additive + safe — PASS

- `falcon_signal_validity`: `DELETE WHERE persona='falcon_top10_signal_validity'` then INSERT, single `BEGIN`/`commit` transaction with rollback-on-error (lines 799-832). Scoped to this persona tag.
- Taxonomy UPDATE touches ONLY the 6 signal-timing cols: `write_cols = [c for c in _TAXONOMY_WRITE_COLS if c in tax_cols_present]` (line 815); `_TAXONOMY_WRITE_COLS` (lines 225-232) == the 6 cols and nothing else. `quality_flag`/`weight_multiplier`/etc. never in the SET clause. CONFIRMED no clobber.
- Schema: `CREATE TABLE IF NOT EXISTS` via `executescript(schema_self_improving.sql)` (idempotent/additive) + PRAGMA-guarded `ALTER ADD COLUMN` for each of the 6 cols, skipping existing and tolerating duplicate-column (lines 718-755).
- `--dry-run` returns at line 1089 **before** `apply_schema`/`_write`/`write_report` — writes nothing. Safe gate.
- `_SV_COLS` (lines 764-772) = exactly the 19 non-id/created_at columns of `falcon_signal_validity` (schema lines 229-247); INSERT placeholders match (lines 804-807). `_sv_value` routes the 9 intraday cols to `r['_intraday']` and the rest to top-level (lines 775-783) — consistent with `part_a['_intraday']=part_b` (line 1030).

## 6. No PROD / shared-engine mutation (INV2) — PASS

- All writes target `rnd_db` only (`apply_schema(rnd_db)`, `_write(rnd_db,...)`). PROD opened only via read-only loaders `load_panel(prod_db,...)`, `load_all_bars(prod_db,...)`, `trading_days(prod_db,...)` inside `sds._build_run_context` (build_signal_day_study.py:162-169). 1-min + sectors read from RND (build_signal_day_study.py:168; build_signal_validity 1-min queries use `rnd_db`). No engine/PROD source file modified — the script imports persona_engine_core / build_signal_day_study, never mutates them.

---

## Explicit confirmations
- **(a) Decay re-sims reuse the parity exit; D+1 == study value:** YES. D+1 leg is `_net_ret_from_trade(simulate_independent_pick(sig, bars, sd_idx, run_cfg))`, byte-identical inputs to the study's `net_ret_pct` ⇒ same value. No re-implemented exit.
- **(b) 1-min is per-(symbol,date), not full-scan:** YES. `WHERE symbol=? AND bar_time>=? AND <?` on PK `(symbol,bar_time)`; only other read is a one-time MIN/MAX aggregate.
- **(c) Pre-2024-05-13 intraday is NULL, not fabricated:** YES. Out-of-window entry dates and symbols without minute bars both return the all-NULL `null_row`; window is data-detected.
- **--dry-run is the safe gate:** YES. With no Python in the build env, run `--dry-run` first; it computes + prints summaries and writes nothing (returns before any schema/DB/report write).

## Notes (non-blocking)
- ohlc_1min CREATE TABLE / PK definition is not present in the kanida-dev tree (the 87.8M-row table is built in the external RND DB pipeline). The PK `(symbol, bar_time)` is asserted in S4-build-log.md:49 and is consistent with the codebase's other bar tables. The query shape is correct **regardless** of index presence: it is filtered to one symbol + one day, so it can never become a full-table scan. Recommend a one-line runtime confirmation of the PK/index when the RND DB is available, but this is not a RED condition.
