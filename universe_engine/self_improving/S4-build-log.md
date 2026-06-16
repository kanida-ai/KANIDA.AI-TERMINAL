# Step 4 Build Log — Signal Validity + Intraday Detection

**Date:** 2026-06-15 (IST) · **Agent:** BuildAgent-S4 · **Worktree:** `C:\Users\SPS\Desktop\kanida-dev` (branch `feat/self-improving-engine`)
**Scope:** Create `universe_engine/self_improving/build_signal_validity.py` — Part A signal freshness + decay (all years, daily-derivable), Part B intraday-detection comparison (1-min window only), Part C `falcon_signal_validity_report.xlsx`. Writes RND DB only (`falcon_signal_validity` + 6 taxonomy signal-timing cols). **Not executed** (no Python in env — validated by reading). No commits. No PROD / shared-engine-code change (INV2). Provides `--dry-run`.

## File created
| File | Purpose |
|---|---|
| `build_signal_validity.py` | S4 study. Reuses the S2B signal-day study's selection + the engine exit walk (imported as a module); writes `falcon_signal_validity` rows + taxonomy signal-timing cols; emits the report. CLI: `--rnd-db`, `--prod-db`, `--years`, `--top-n`, `--dry-run`, `--out` (orchestrator passes `out/v4`). |

## Signal universe (reused, not re-selected)
Imports `build_signal_day_study` as a module and calls `sds._build_run_context` + `sds._year_signals_by_signal_date` — the SAME machinery that reproduces the parity Falcon Top 10 signal generation (eligible patterns for the year → `compute_year_signals(min_fires=10)` → `score = avg_lift` → grouped by `signal_date`). Then top-N per signal_date by `avg_lift` desc. One row per `(signal_date, symbol)`. No skip_already_held / no cash constraint (it's a per-signal study, like S2B).

## PART A — freshness + decay (the gate deliverable)

### The VALIDITY RULE (documented)
A signal's D+1 open is still **actionable** iff the overnight gap (vs the **signal-date close** — the price the signal fired at) has NOT crossed the locked trade band:
```
gap = (D+1 open / signal_date_close − 1)
valid_at_next_open = 1  iff  init_stop < gap < target      (i.e. −7% < gap < +12%)
```
- `gap >= +12%` → the move we wanted already happened overnight → entering buys the top → **STALE** (0).
- `gap <= −7%` → the setup broke past our stop overnight → **STALE** (0).

`init_stop`/`target` come straight from the Falcon Top 10 `run_cfg` (`init_stop=−0.07`; `target` is `None` so the band top falls back to `trail_trigger=+0.12`, the persona's profit objective). Implemented in `_valid_band`. The SAME band rule is reused for the intraday checkpoints in Part B (the "current price" becomes the checkpoint price; base stays the signal-date close).

### DECAY METHOD (engine exit REUSED — entry bar shifted)
For each signal I recompute the trade outcome for entry at the **D+1, D+2, D+3 opens** by calling `sds.simulate_independent_pick` (which is copied **line-for-line** from `persona_engine_core.simulate_year:384-436`) with a **shifted entry-bar index**:
- D+1 = `sd_idx` (first bar with `date > signal_date`, = core:354 next-open entry).
- D+2 = `sd_idx + 1`, D+3 = `sd_idx + 2`.

Only the entry bar moves; the **identical** 7-day / −7% / +12% / 10-day-Donchian exit logic runs off the later entry. **No exit rule is re-implemented** — the requirement to "reuse the engine's exit mechanics for the alternate-entry re-sims" is met by shifting the entry into the proven engine walk (`_resim_entry_at`). Each leg's `net_ret_pct` is stored:
- `eod_signal_outcome` = the D+1 (standard) leg → also the baseline Part B compares the intraday-early entry against.
- `_ret_d2`, `_ret_d3` = report-only decay legs (the DB schema only carries `eod_signal_outcome`; the three legs feed the Decay Summary sheet + per-pattern peak).

`n_days_signal_remained_valid` = how many of {D+1, D+2, D+3} re-simmed entries are still **positive-EV** (`net_ret_pct > 0`). 0..3. (Documented validity rule for the decay leg.)

### Per-pattern taxonomy (HFCL n≥18)
A pattern's "signals" = the top-N signals it **contributed to**, taken from the EXISTING `falcon_pattern_contributions` table (persona=`falcon_top10`), restricted to **resolved** occurrences (parent baseline trade closed, `net_ret_pct IS NOT NULL`) — joined in `_load_signal_to_patterns`. **Reused, not re-derived** (Step 2 already recorded which patterns fired per pick). For patterns with **n ≥ 18** of these signals:
- `signal_validity_next_day_pct` = % with `valid_at_next_open == 1`.
- `avg_signal_decay_days` = the entry-day (1/2/3) at which the pattern's **average** `net_ret_pct` **peaks** across D+1/D+2/D+3 (where the edge is best / how fast it decays).
- `signal_best_entry_window` = `D+1` / `D+2` / `D+3` (same peak as a label).
Below 18 → all three NULL (never fabricated).

## PART B — intraday detection (price-based freshness; 1-min window only)

### 1-min windowing + per-(symbol,date) query
- Authoritative window read once via `SELECT MIN/MAX(substr(bar_time,1,10)) FROM ohlc_1min` (`_one_min_window`). Constants `ONE_MIN_START=2024-05-13` / `ONE_MIN_END=2026-05-11` are a documented fallback/sanity bound (STEP0_AUDIT §C); the live MIN/MAX is authoritative. If `ohlc_1min` is absent → Part B entirely skipped, every intraday field NULL.
- Per signal whose **entry date** (= D+1, the bar entered into) is in the window, I pull that symbol's minute bars for **that one day only** via `_load_minute_bars_for` — an indexed range scan on the PK `(symbol, bar_time)` prefix (`bar_time >= 'YYYY-MM-DD 00:00:00' AND < '...23:59:59'`). **Never** a full-table scan of the 87.8M-row table. `ohlc_1min` schema (from `universe_engine/data/db/init_db.py:48-55`): `symbol, bar_time ("%Y-%m-%d %H:%M:%S"), open, high, low, close, volume`, PK `(symbol, bar_time)`.

### Intraday fields computed
- Prices at **09:15** (open), **10:00**, **11:00**, **13:00** via `_price_at_or_after` (open of first bar at/after the checkpoint).
- `valid_at_next_10am / _11am / _1pm` — same band rule as `valid_at_next_open`, current price = checkpoint price, base = signal-date close.
- `intraday_entry_price` = open of the first bar in **[09:15, 09:30)** (first-15-min), else the 09:15 session open (`_first_15_entry_price`); `intraday_detection_time` = that bar's clock time.
- `intraday_early_entry_outcome` — **engine exit REUSED**: run `simulate_independent_pick` on a **synthetic** bar list whose entry-bar `open` is overwritten with `intraday_entry_price` (all later bars unchanged). The engine never evaluates the entry-day bar for an exit (`core:441-525` adds the position AFTER that day's exit checks), so replacing only its open is faithful — the −7%/+12%/10d/7-day exits still begin D+2. No exit rule re-implemented.
- `eod_signal_outcome` — the standard D+1-open `net_ret_pct` (from Part A).
- `detected_intraday` = 1 iff actionable at the first readable checkpoint (open→1pm) AND a 1-min entry price exists.
- `intraday_false_positive` = looked actionable intraday but the D+1 realized outcome ≤ 0 (a loss). `false_positive_reason` names which checks passed vs the loss.

### Per-pattern intraday taxonomy (HFCL n≥18 IN-WINDOW)
For patterns with ≥ 18 **in-window** signals:
- `intraday_detectable` = 1 if any were detected intraday (else 0).
- `intraday_detection_accuracy` = % of detections whose D+1 outcome was a win.
- `intraday_false_positive_rate` = % of detections that were false positives.
Below 18 in-window → NULL.

### Window in/out reporting
The run prints `n_in_window` vs `n_out_window` (and `n_detected_intraday`, `n_compared`). Signals before 2024-05-13 / symbols without 1-min → all intraday fields NULL.

## Columns populated vs NULL
**`falcon_signal_validity`** (per signal): `signal_date, symbol, persona, engine_rank, avg_lift, valid_at_next_open, n_days_signal_remained_valid, next_day_open_price, entry_price_delta_pct, eod_signal_outcome` always (daily-derivable). `valid_at_next_10am/_11am/_1pm, detected_intraday, intraday_detection_time, intraday_entry_price, intraday_early_entry_outcome, intraday_false_positive, false_positive_reason` populated only for in-window signals with 1-min data; NULL otherwise.

**`falcon_pattern_taxonomy`** (only these 6 cols UPDATEd; everything else untouched): `signal_validity_next_day_pct, avg_signal_decay_days, signal_best_entry_window` (n≥18 signals else NULL); `intraday_detectable, intraday_detection_accuracy, intraday_false_positive_rate` (n≥18 in-window else NULL).

## HFCL points
1. `signal_validity_next_day_pct` / `avg_signal_decay_days` / `signal_best_entry_window` — only set when a pattern has ≥ 18 contributed (resolved) signals; else NULL.
2. `intraday_detectable` / `intraday_detection_accuracy` / `intraday_false_positive_rate` — only set when a pattern has ≥ 18 **in-window** signals; else NULL.
3. "Resolved" denominator for the join = parent baseline trade closed (`net_ret_pct IS NOT NULL`), mirroring Step 3.

## Idempotency / safety
- `falcon_signal_validity`: `DELETE WHERE persona='falcon_top10_signal_validity'` then re-INSERT, single transaction.
- Taxonomy: `UPDATE ... SET <6 signal-timing cols> WHERE pattern_id=?` — only the 6 columns this step owns; `quality_flag`, `weight_multiplier`, etc. never touched. Only cols actually present on the table are written (guards a partial-ALTER RND DB).
- `apply_schema` runs `CREATE TABLE IF NOT EXISTS` (canonical `schema_self_improving.sql`) + PRAGMA-guarded ADD COLUMN for the 6 taxonomy cols (additive; skips existing; tolerates duplicate-column).
- RND-only writes. OHLC/features read from PROD (fresh); 1-min + sectors from RND. INV2 honoured — nothing in the engine/PROD tree mutated.

## Deferred (documented, NOT built here)
**Deep per-pattern intraday RULE detection** — re-evaluating each pattern's mined rule on intraday-computed features every 15 min (the spec's live-detection scan: 30-min hold-before-fire, sector-momentum + volume cross-checks, auto-disable at FP>30%). That needs an intraday FEATURE pipeline (the 38 `FEATURE_COLS` recomputed on partial-day 1-min bars) which does not exist yet. THIS S4 pass is a **price-based intraday-freshness comparison** only (is the signal still actionable intraday, and does an earlier intraday entry beat the next-open entry). The `intraday_false_positive_rate` here is the price-freshness FP, not the rule-detection FP that Constitutional Rule #19 gates on; the deeper version supersedes it when the feature pipeline lands.

## Report — `falcon_signal_validity_report.xlsx` (`--out` default `out/v4`)
- **Per-Pattern Validity**: pattern_id, n_signals, signal_validity_next_day_pct, avg_signal_decay_days, signal_best_entry_window, intraday_detection_accuracy, intraday_false_positive_rate.
- **Decay Summary**: avg net_ret by entry D+1/D+2/D+3 (+ counts) + valid_at_open %, overall + per-year.
- **Intraday Coverage**: 1-min window bounds; n signals in/out of window; detected/compared counts; avg (intraday_early − next_open) ret; intraday-early vs next-open win rates; detection accuracy + FP rate.
openpyxl if available, else 3-CSV fallback (printed note).
