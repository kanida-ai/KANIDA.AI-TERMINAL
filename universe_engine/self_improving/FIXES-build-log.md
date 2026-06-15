# FIXES build log — post-review targeted fixes (Self-Improving Engine)

Branch: `feat/self-improving-engine` (dev worktree `kanida-dev`).
Scope: post-review fixes only. **No production-tree or shared-engine-code changes.**
Nothing was executed or committed in this session.

## Fixes applied

### Fix 1 — `quality_flag` NEUTRAL bucket (`classify_patterns.py`)
`_quality_flag(...)` no longer returns the `''`→NULL sentinel when `n >= 18` and
neither STOCK_SPECIFIC_ALPHA nor SECTOR_FOLLOWER is earned. It now returns
**`'NEUTRAL'`**. `INSUFFICIENT_DATA` is still returned for `n < 18`. This was
leaving ~257 patterns (a third of the library) unclassified (NULL). The HFCL
`n >= 18` gate and all other thresholds are **unchanged**. Inline threshold doc +
function docstring updated to list NEUTRAL; `S3-build-log.md` updated.

### Fix 2 — sector source switched PROD → RND
- `classify_patterns.py`: `_build_sector_indices` now reads `falcon_sectors` from
  the **RND DB** (new `rnd_db` arg) and reads `ohlc_daily` from **PROD** (unchanged).
  Caller `compute_part_a` passes `rnd_db`. Docstrings / CLI help / run() prints updated.
- `build_signal_day_study.py`: `build_sector_map(prod_db)` → `build_sector_map(rnd_db)`
  for the `sector_map` only. OHLC/features/bars (`load_panel`, `load_all_bars`)
  still come from PROD — unchanged.
Reason: PROD's `falcon_sectors` is missing GUJGASLTD/LTIM/ZOMATO → null sector →
null move_type. RND is where those mappings are backfilled.

### Fix 3 — new `fix_sector_backfill.py` (RND-only, stdlib, idempotent)
`--rnd-db` required, `--dry-run` supported. Single transaction, rollback on error.
- INSERT OR IGNORE into RND `falcon_sectors` (columns introspected via PRAGMA —
  detected as `symbol`,`sector`): GUJGASLTD→'Oil Gas & Consumable Fuels',
  LTIM→'Information Technology', ZOMATO→'Consumer Services'.
- UPDATE `falcon_baseline_trades` SET sector=<map> WHERE symbol IN (3) AND
  sector IS NULL AND persona='falcon_top10'.
- UPDATE `falcon_signal_day_study` SET sector=<map> WHERE symbol IN (3) AND
  sector IS NULL AND persona='falcon_top10_daily'.
- Prints counts changed per step. `--dry-run` rolls the transaction back (writes nothing).

### Fix 4 — `--out` output-path param
Confirmed all three accept `--out` and write the workbook there:
- `classify_patterns.py` — `--out` (dir, default `./out`). Already present.
- `build_signal_day_study.py` — `--out` (dir, default `./out`). Already present.
- `build_baseline_summary.py` — `--out` (full file path). Already present.
No code change needed. Orchestrator can pass `_v2` filenames at run time.

### Fix 5 — `n_resolved_60d` note in `falcon_pattern_health_report`
`write_report` now appends a trailing Notes line (both xlsx and csv branches):
"n_resolved_60d is ~0 for most patterns in this backtest because there is no
forward 60-day window at the backtest tail — the weekly multiplier logic only
becomes active in live running. This is a backtest-tail artifact, not a data gap."

## Deferred — NO code change

### Rank-11 (PVRINOX 2024-11-04)
Benign 1-row `signal_date`-labeling artifact. **Deferred to the next Step-2 rebuild;
no effect on parity / returns / learning.** No code change made. `build_baseline.py`
is intentionally **NOT** modified or re-run (re-running wipes Step 3).

## Confirmations
- `build_baseline.py` — **untouched** (not opened for edit, not run).
- No production tree touched; no shared/engine code modified.
- Nothing executed; nothing committed.
