# S-CONTEXT audit — `build_signal_day_context.py`

**Auditor:** AuditAgent (read-only)
**Target:** `universe_engine/self_improving/build_signal_day_context.py` (+ `S-CONTEXT-build-log.md`)
**Date:** 2026-06-18
**Method:** line-by-line read of the target + the reused study builder (`build_signal_day_study.py`) + the persona-simulator import surface (`backend/power_user/services/persona_simulator.py`). No code executed.

---

## VERDICT: GREEN

No mutation of `falcon_signal_day_study` or `out/v3`; the five core formulas are correct including the per-symbol prior-trading-day calendar; nulls are never imputed; the analysis cohort is resolved + non-extension only. The two NOTED items (circuit ±18% proxy, prior_appearances_30d cosmetic gap) are present and are not RED.

---

## 1. No mutation of existing data — CONFIRMED

- Grep over the whole file for `UPDATE | INSERT INTO falcon_signal_day_study | DROP TABLE..study | ALTER..study` → **zero hits**. The only reference to `falcon_signal_day_study` that touches the DB is the read at **line 331**: `SELECT * FROM falcon_signal_day_study WHERE persona = ? ORDER BY id` (in `load_study_rows`). Pure read.
- `out/v3` appears **only** in docstrings/comments (lines 5, 12, 21–28, 46, 874–876) asserting it is untouched. No `Path`/open/write ever resolves to `v3`. The actual output dir defaults to `out/v8` (line 1125) and is the only dir `.mkdir`'d (lines 726, 792).
- Study rows are read verbatim and re-emitted: `write_study_with_context` builds the header as `["id"] + original study_cols + context-extra cols` (lines 732–734) and row values pull every original column straight from the study-row dict (line 739). All 86 original columns are preserved; the 3 sheets are all present — `All Signal-Day Trades` (line 750), `Per-Year Efficacy` (line 757), `Overall` (line 764) — the latter two regenerated from `study.compute_efficacy` over the original rows (lines 873–894), matching v3's builder.
- The context table writer (`write_context_table`, lines 484–509) targets `rnd_db` only and creates/inserts only `falcon_signal_day_context`.

## 2. Formula correctness — CONFIRMED (5/5 core formulas + calendar)

All formulas read from `compute_context` (lines 203–316). PROD OHLCV with volume loaded via `load_ohlcv` (lines 108–126: `SELECT symbol, trade_date, open, high, low, close, volume FROM ohlc_daily`) — hits **PROD** (`prod_db` arg, ultimately `config.POWER_DB_PATH`), includes volume, sorted by date per symbol.

- **prev_close calendar (load-bearing):** `sd_i = didx.get(signal_date)` indexes the symbol's own date-sorted bar list; `prev = bars_sym[sd_i-1]`, `prev_prev = bars_sym[sd_i-2]` (lines 219–231), each bounds-checked (`if sd_i-1 >= 0`, `if sd_i-2 >= 0`). Because the bar list is the symbol's actual trading days from PROD, `sd_i-1` IS the actual prior trading day (weekends/holidays skipped implicitly) — **not** a fixed −1 calendar day. Correct. If `signal_date` isn't itself a bar, falls to NULL context (line 219–221).
- **Denominators (each verified individually):**
  - `signal_day_ret/high/low/range_pct` ÷ `prev_close` (lines 243–246). ✓
  - `prev_day_ret/high/low_pct` ÷ `prev_prev_close` (lines 258–260). ✓
  - `two_day_ret_pct` ÷ `prev_prev_close` (line 264). ✓
  - `entry_gap_pct` ÷ `signal_day_close` (`sd_close`, line 279). ✓
  - All guarded against `None`/`0` denominators (`not in (None, 0)`).
- **avg_20d_volume** = mean of `bars_sym[sd_i-20 : sd_i]` (line 268) — exactly the 20 bars **strictly before** the signal bar (Python slice excludes `sd_i`). Requires `sd_i >= 20` AND `len(vols) == 20` after dropping None volumes (line 270), else stays NULL. `signal_day_vol_ratio = signal_day_volume / avg_20d_volume` (line 274), guarded. ✓
- **circuit proxy:** UPPER if `sd_close >= prev_close*1.18`, LOWER if `<= prev_close*0.82`, else NONE (lines 248–253); NULL if no prev_close. Matches spec; ±18% is the NOTED approximate flag. ✓
- **OHLCV source segregation:** `compute_context` uses ONLY the `load_ohlcv` (volume-bearing PROD) bars. The volume-less `ctx["bars"]` from `study._build_run_context` (which uses `load_all_bars`, OHLC-only) is used **exclusively** in `build_extension_rows` for the sim path (lines 369, 390–409), never in any context formula. Verified by grep. ✓

## 3. NULL never imputed — CONFIRMED

`_null_context` initializes every field to `None` (line 194). Each formula writes a value **only** when its inputs are present and denominators non-zero; otherwise the field remains `None`. No `0`-fill, no forward-fill, no `or 0` defaults anywhere in `compute_context`. Booleans are the one deliberate exception and are correct: a flag is `1` only when all its inputs are non-None and the threshold holds, else `0` (lines 290–298) — i.e. "not asserted", never a false positive from a missing input. `entry_context` defaults to the string `"NORMAL"` when no flag fires (line 310–315), which is a label not an imputed numeric.

## 4. entry_context booleans + priority — CONFIRMED

Thresholds (lines 290–298) match the spec/build-log: fresh_breakout (`sdr>5 & -1<=pdr<=1`), extended_move (`twr>10`), gap_up (`egp>2`), gap_down (`egp<-2`), signal_day_circuit (`circ!=NONE`), high_vol_confirm (`vr>2 & sdr>3`), low_vol_warning (`vr<0.8`). Single-label priority is deterministic and documented: `ENTRY_CONTEXT_PRIORITY` (lines 92–101), resolved first-match in `compute_context` (lines 311–315). Order: SIGNAL_DAY_CIRCUIT > HIGH_VOL_CONFIRM > EXTENDED_MOVE > FRESH_BREAKOUT > GAP_UP > GAP_DOWN > LOW_VOL_WARNING > NORMAL.

## 5. Analysis cohort + buckets — CONFIRMED

- **Cohort:** built at lines 1056–1063 — iterates `study_rows` only (extensions excluded by construction), and `continue`s when `net_ret_pct is None` (line 1058). So S1–S7 see **resolved + non-extension** rows only. `build_analysis` docstring restates this (lines 598–600).
- **Bucket boundaries exhaustive + non-overlapping:** `_bucket` (lines 525–534) uses `[lo, hi)` — lo inclusive, hi exclusive — with open ends as `None`. The bucket tables (SIGNAL_RET/ENTRY_GAP/TWO_DAY/VOL_RATIO, lines 579–595) are contiguous: each band's `hi` equals the next band's `lo`, first band `lo=None`, last band `hi=None`. No gaps, no overlaps; every finite value lands in exactly one bucket; a boundary value (e.g. exactly `5`) falls into the band where it is the inclusive `lo`. No double-count: each row appended to one bucket list (lines 665–667). Rows whose field is None are skipped (not mis-binned) — correct, since None means a missing bar, not a zero.
- **S6 cross-tab** (lines 676–695): WR matrix, signal_day_ret buckets × vol_ratio buckets; each row counted into one cell only when both buckets resolve.
- **S7** (lines 642–657): G1 proxy = `d1_close_ret > 0.5` (line 643); enterprise proxy `engine_rank <= 3`. Both documented as proxies. Operates on the resolved cohort.

## 6. Extension rows — CONFIRMED

- Marked `is_extension = 1` (line 1014); synthetic ids assigned beyond `max(study id)` (lines 1010–1013) so the join key stays unique and the originals' ids are untouched.
- Excluded from analysis: the `joined` loop iterates `study_rows` only (line 1057), so extensions never enter S1–S7. Parity `orig_ctx` also filters `is_extension != 1` (line 904).
- **Reuses the study sim** — not a divergent computation: `build_extension_rows` (lines 345–411) calls `study._build_run_context`, `study._year_signals_by_signal_date`, `study.simulate_independent_pick`, `study.build_prior_appearances_index`, `study.build_row`. The entry-index expression `next((i for i, b in enumerate(bs) if b["date"] > sd), None)` (line 390) is **character-identical** to the study's own (`build_signal_day_study.py:1065`), so `sd_idx` (= first bar with date > signal_date, the next-open entry bar) matches `simulate_year`'s `_bars_start_idx` parity contract.
- Recent picks whose 7-day hold hasn't elapsed yield `OPEN_AT_BACKTEST_END_MTM` with NULL `net_ret_pct`, so they are auto-excluded from the resolved cohort too — double safety.
- Original 10,073 rows are never altered by extension processing (separate list, separate id range).

## 7. Safety / idempotency — CONFIRMED

- `write_context_table` (lines 484–509): single transaction (`BEGIN` … `DROP TABLE IF EXISTS` … `CREATE` … `executemany INSERT` … `commit`; `rollback` on exception). RND-only. Re-runnable (DROP+CREATE each run).
- PROD opened read-only in practice (only SELECTs in `load_ohlcv` and `_extension_fallback`).
- `--dry-run` (lines 1089–1091): returns before any write — no table, no Excel. Confirmed nothing is written on the path above that guard except in-memory compute + prints.
- RND vs PROD are distinct files (`_resolve_rnd_db_path` → `universe_engine/data/db/kanida_universe.db`; `PROD_DB` → `config.POWER_DB_PATH`), so RND writes cannot hit PROD.

---

## NOTED (not RED, per audit charter)

1. **Circuit ±18% proxy** — real NSE bands are 2/5/10/20%; documented as approximate at lines 86–89, 247, and in the build log.
2. **prior_appearances_30d on extension rows** — `build_prior_appearances_index` is computed over the extension key set in isolation (line 403), so an extension symbol's prior appearances within the *study* window aren't counted. Cosmetic only; this column is not used in any analysis sheet. Documented as a known gap.

## Verification confirmations requested by the charter

- **(a) existing study / v3 untouched:** YES. `falcon_signal_day_study` is read-only (`SELECT *`, line 331); `out/v3` is never resolved as a path; output is `out/v8` only.
- **(b) five core formulas correct (incl. prev_close calendar):** YES. prev/prev_prev = per-symbol prior *trading* days via `bars_sym[sd_i-1/-2]` (bounds-checked); denominators are prev_close / prev_prev_close / sd_close respectively; avg_20d_volume = mean of the 20 bars strictly before the signal bar (NULL if <20); vol_ratio = sd_vol / avg20.
- **(c) nulls not imputed:** YES. All fields init to None; values written only when inputs present and denominators non-zero; booleans default 0 only when not assertable; no fill/ffill.
- **(d) analysis cohort = resolved + non-extension:** YES. `joined` iterates study rows, skips `net_ret_pct is None`, and never includes `is_extension=1` rows.
