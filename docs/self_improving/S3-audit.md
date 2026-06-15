# S3 Audit — classify_patterns.py (Pattern Suitability Classification)

**Agent:** AuditAgent-S3 (READ-ONLY on code; only this file written)
**Date:** 2026-06-15 (IST)
**Target:** `universe_engine/self_improving/classify_patterns.py` + `S3-build-log.md`
**Verified against:** spec `kanida_sp.md`, Step-1 schema `schema_self_improving.sql` + `taxonomy_columns.sql`, and `sim_sweep.build_sector_indices` (engine worktree).

---

## VERDICT: GREEN

No HFCL violation, no fabricated deferred/no-data field, no sector-math look-ahead or sign bug, no schema column mismatch, no PROD / shared-code mutation. Recommendations (swing_suitable + unit notes) are advisory only.

---

## Must-fix
None.

---

## Checklist findings

### 1. HFCL discipline — ENFORCED EVERYWHERE (critical) ✅
`HFCL_MIN_N = 18`. Every flag is gated on n>=18 resolved occurrences AND the relevant EV/sub-gate:
- **`big_loser_risk`** (`classify_one_pattern` L458-461): `1`/`0` only when `n >= 18` AND `bl_rate` AND `ev` both non-None; positive flag additionally needs `bl_rate >= 0.30` AND `ev <= 0`. Below 18 → `None` (NULL), explicitly "not 0-as-fact".
- **`quality_flag`** (`_quality_flag` L578-589): `n < 18` → `INSUFFICIENT_DATA`. STOCK_SPECIFIC_ALPHA needs headwind WR>=50% AND `n_headwind >= 10`; SECTOR_FOLLOWER needs tailwind WR>=60% AND headwind WR<40%. Matches spec Phase-2 Update-1 exactly. The `n_headwind>=10` sub-gate is correctly a sub-gate (< HFCL_MIN_N) applied only *after* the n>=18 master gate is passed.
- **`pattern_maturity`** (`_maturity` L416-423): `n < 18` → `insufficient_data`; lowest tier == HFCL gate.
- **`swing_suitable`** (L503-505): only set when `n >= 18` AND `ev is not None`; else NULL.
- **"Resolved" denominator** (`_load_pattern_occurrences` L382-385): JOIN filters `bt.net_ret_pct IS NOT NULL`, so n counts closed trades only — the true resolved count. win_rate/EV computed over all resolved occurrences, never a single example.

No single-example conclusion path exists. RED criterion (flag without n>=18+EV) not triggered.

### 2. No fabrication of deferred fields ✅
Deferred columns are **absent from `_TAXONOMY_WRITE_COLS`** (L594-603) so they are never written — they retain Step-1 defaults / NULL:
`signal_validity_next_day_pct`, `avg_signal_decay_days`, `signal_best_entry_window`, `intraday_detectable`, `intraday_detection_accuracy`, `intraday_false_positive_rate`, `realized_lift_60d`, `weight_multiplier`, `previous_multiplier`, `status`, `regime_best_fit`, `sector_regime_fit`, `typical_peak_interval`.
No-data persona-suitability flags (`positional/longterm/btst/intraday/short/index_suitable`, `fo_only`) are set to `None` in `classify_one_pattern` (L507-513) AND are in the write list, so they are explicitly written NULL ("no data for persona") rather than invented as 0. `quality_flag` deliberately never emits `REGIME_SPECIFIC` because no regime tags exist on the trades — honest NULL via `''` sentinel (L586-589, writer maps `''`→NULL at L768-769). Correct.

### 3. swing_suitable — judgment call (see Recommendations) ⚠️
Honest proxy, not a RED. Build sets it from falcon_top10 resolved trades (EV>0 over n>=18), documented as "falcon_top10 IS the 7-day EOD swing data."

### 4. Sector attribution math (Part A) — CORRECT, no look-ahead, correct sign ✅
- `_build_sector_indices` (L153-208) is a faithful verbatim port of `sim_sweep.build_sector_indices` (worktree `infallible-mcclintock-225f46/sim_sweep.py:115-140`): equal-weight, daily-rebalanced, member-mean daily return, base 100, carry-forward on no-return days. Identical math. Defensible additions: `sec is not None` / `cl is not None` guards and `peer_count` for the thin-sector guard (sim_sweep had none).
- `sector_ret_same_period` (`_sector_ret_over_window` L258-276) uses index levels at **`entry_date` and `exit_date` only** — strictly inside the trade's own hold window. **No look-ahead.**
- Sign: `stock_vs_sector = stock_ret - sec_ret` (L329); `sector_tailwind = 1 if sec_ret > 0 else 0` (L331). `_classify_move_type` (L211-231) signs correct (stock-up+sector-down → SECTOR_HEADWIND, etc.).
- Thin-sector (<3 peers), open-trade (`exit_date` NULL or `net_ret_pct` NULL), no-sector, and date-outside-window cases are all left NULL with per-bucket counts (L307-324). Correct.

### 5. Unit consistency — CONSISTENT, labels match ✅
`win_rate_sector_tailwind/headwind/neutral` are PERCENTS 0-100 (`_pct_win` L550-555 returns `wins/len*100`), required so the 50/60/40 quality thresholds compare like-for-like. `big_winner_rate`/`big_loser_rate`/`peak rates` are fractions 0-1 (`_rate` L408-409). Report headers (`win_rate_pct`, `win_rate_sector_*` vs `big_winner_rate`/`big_loser_rate`) and build-log §"Risks 2" match the code. Internally consistent. (Minor cosmetic note in Recommendations.)

### 6. Safety / idempotency ✅
- **RND-only writes.** `_write` (L727-784) UPDATEs only `falcon_baseline_trades` + `falcon_pattern_taxonomy` on `rnd_db`. PROD DB opened read-only (OHLC + falcon_sectors only). No write to `falcon_big_winner_loser_study` (that is Step 5) — confirmed absent.
- **Single transaction.** `BEGIN` … `commit`, `rollback` on exception (L739-781).
- **Idempotent / re-runnable.** Overwrites only owned columns in place; no DROP/rename/retype, no inserts; other personas' baseline rows and unclassified taxonomy rows untouched.
- **PRAGMA column guard.** `_taxonomy_columns` + `write_cols = [c for c in _TAXONOMY_WRITE_COLS if c in tax_cols]` (L756); missing table or missing cols degrade gracefully (Part A still commits, Part B skipped/partial) — cannot crash the UPDATE on a column mismatch.
- **`--dry-run`** writes nothing (L841-852): no DB UPDATE, no Excel.
- No PROD or shared/engine-code mutation (sim_sweep re-implemented, not imported/modified — INV2 honoured).

### 7. Schema column-name match — ALL MATCH ✅
Every column in `_TAXONOMY_WRITE_COLS` exists in `taxonomy_columns.sql`:
`n_resolved_trades_total`, `n_resolved_trades_60d`, `typical_peak_day`, `peak_sustained_rate`, `early_peak_rate`, `late_peak_rate`, `win_rate_sector_tailwind/headwind/neutral`, `big_winner_rate_by_persona`, `big_loser_rate_by_persona`, `big_loser_risk`, `quality_flag`, `pattern_maturity`, `swing_suitable`, `positional/longterm/btst/intraday/short/index_suitable`, `fo_only`, `last_classification_date`, `classification_version`. No typos. Part A target columns (`sector_ret_same_period`, `stock_vs_sector`, `move_type`, `sector_tailwind`) all exist in `falcon_baseline_trades` (schema_self_improving.sql L92,97-99). Even on a column mismatch the PRAGMA guard prevents a crash.

---

## Recommendations (advisory — not blocking)

1. **swing_suitable — KEEP, with the documented proxy rationale.** Treating falcon_top10 (7-day EOD hold) as the swing persona's data is an honest proxy, not an over-claim: the horizon (7 trading days) sits inside the spec's Weekly-Swing definition (5–7 days), the flag is HFCL-gated (n>=18 + EV>0), and the build log §6 already states the assumption and where to flip it. Recommend keeping it ON but **renaming the proxy intent in a one-line column comment / report header note** (e.g. "swing_suitable = profitable on 7d EOD hold (falcon_top10 proxy)") so a downstream reader does not mistake it for output of a dedicated Weekly-Swing backtest. Acceptable to leave as-is; do NOT set NULL.

2. **Unit-mix readability.** The percent (sector WRs) vs fraction (bw/bl/peak rates) split is internally consistent and intentional, but mixed within one report row. Optional: suffix headers (`win_rate_sector_tailwind_pct`, `big_winner_rate_frac`) to make the units self-describing in the Excel. Not a correctness issue.

3. **move_type tie-break.** STOCK_LED uses `stock_ret >= sector_ret` (ties → STOCK_LED). Spec wording is "beat" (strict). Immaterial (exact ties ~never occur with net-vs-gross legs); flag only if spec-literal bucketing is required.

4. **Bucket counts unverified in this env (no Python).** Build log defers actual maturity/quality/big_loser_risk counts to a `--dry-run`. Recommend the orchestrator run `--dry-run` once and confirm: (a) most closed trades attributed in Part A, (b) `big_loser_risk=1` count is small (HFCL should make it rare), (c) a sizeable `insufficient_data` tail is expected and OK.

---

## Summary for orchestrator
- **HFCL enforced everywhere** — confirmed at all five flag sites; resolved denominator = closed trades only; no single-example conclusion.
- **No deferred/no-data field fabricated** — deferred cols excluded from writes; no-data persona flags written explicit NULL; REGIME_SPECIFIC never invented.
- **Part A sector math** = verbatim sim_sweep port, hold-window-only (no look-ahead), correct sign.
- **swing_suitable**: keep as honest 7d-EOD proxy with a clarifying label; do not NULL it.
- **GREEN — no must-fix.**
