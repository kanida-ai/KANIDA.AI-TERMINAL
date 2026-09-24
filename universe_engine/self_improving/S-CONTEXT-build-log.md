# S-CONTEXT build log — signal-day pre-entry context layer

**File written:** `universe_engine/self_improving/build_signal_day_context.py`
**Worktree:** `C:\Users\SPS\Desktop\kanida-dev` (branch `feat/self-improving-engine`). RND-only writes; PROD read-only; v3 Excel + `falcon_signal_day_study` untouched. NOT committed.

## What it does
Additive layer on the existing per-signal-day study. For every study row it computes the SIGNAL-DAY (pre-entry) market context from PROD `ohlc_daily`, then:
1. writes new RND table `falcon_signal_day_context` (keyed by study `id`),
2. regenerates the study Excel **with** context columns → `out/v8/falcon_signal_day_study_with_context.xlsx` (v3 NOT overwritten),
3. writes 7-sheet `out/v8/falcon_signal_day_context_analysis.xlsx` (resolved rows only),
4. appends extension rows for recent signal days post-dating the study (2026-06-13→latest),
5. prints parity checks + a threshold report.

## Key design decisions (verified against source / DBs)
- **Study rows are read from the RND `falcon_signal_day_study` table, NOT the v3 Excel.** Confirmed by extracting the v3 sheet1 header (inline strings): it equals `build_signal_day_study._COLS` exactly and has **no `id` column**. The context table is keyed by `id`, so the DB is the only id-bearing source. The DB rows hold the identical data the v3 Excel holds (same builder), so the regenerated workbook preserves every original column AND gains a real join key (`id` prepended).
- **OHLCV with volume loaded separately from PROD.** The shared engine's `persona_engine_core.load_all_bars` (verified, lines 109-124) selects only OHLC, no volume. To avoid modifying shared engine code I wrote my own `load_ohlcv` (`SELECT ... volume FROM ohlc_daily`). PROD is the OHLC source (RND ohlc_daily stale to 2026-05-07).
- **Per-symbol trading calendar from PROD bars.** prev / prev-prev trading day = bars at `sd_i-1` / `sd_i-2` in the symbol's own date-sorted bar list → weekends/holidays handled implicitly. 20-day avg volume = the 20 bars strictly before the signal bar (`bars[sd_i-20:sd_i]`); NULL if fewer than 20 prior bars.
- **NULL discipline:** every field is NULL when a needed bar is missing; booleans are 0 (not asserted) unless all inputs are present.
- **Circuit is a documented PROXY** (close vs prev_close ±18% → UPPER/LOWER/NONE; real NSE limits vary 2/5/10/20%).
- **entry_context priority order** (documented in code): SIGNAL_DAY_CIRCUIT > HIGH_VOL_CONFIRM > EXTENDED_MOVE > FRESH_BREAKOUT > GAP_UP_ENTRY > GAP_DOWN_ENTRY > LOW_VOL_WARNING > NORMAL.

## Extension handling
- Reuses the study builder's engine path (imported as `import build_signal_day_study as study`): `_build_run_context`, `_year_signals_by_signal_date`, `simulate_independent_pick`, `build_prior_appearances_index`, `build_row`, `compute_efficacy`, `_COLS`. So extension rows are computed **identically** to the study; recent picks whose 7-day hold hasn't elapsed come out `OPEN_AT_BACKTEST_END_MTM` with NULL `net_ret_pct` (like the study's edge rows).
- `falcon-top-10` persona `sim_end = _SIM_END_TODAY` (today IST), so re-running year 2026 naturally regenerates picks through the latest PROD trading day. Extension = picks with `signal_date > study_max_signal_date` (2026-06-12); weekend dates 06-13/14 have no signals so 06-15/16/17 are the natural extension days.
- Synthetic `id`s assigned beyond `max(study id)`; `is_extension=1`. Extension rows are EXCLUDED from all Part-3 analysis (and from resolved-row stats since net_ret is NULL).
- **Fallback** (documented, only on engine failure): read recent top-N from PROD `falcon_signals_live` (confirmed it exists via `backend/falcon/config.py` + `db_schema_extensions.sql`), set entry_price = entry-day open, compute d1 directly, leave full-hold cols NULL.

## Column → formula mapping
| Column | Formula |
|---|---|
| signal_day_open/high/low/close | signal_date bar raw OHLC |
| signal_day_volume / prev_day_volume | volume at signal bar / prev trading bar |
| prev_close / prev_prev_close | close 1 / 2 trading days before signal_date |
| signal_day_ret_pct | (sd_close − prev_close)/prev_close×100 |
| signal_day_high_pct / low_pct | (sd_high|low − prev_close)/prev_close×100 |
| signal_day_range_pct | (sd_high − sd_low)/prev_close×100 |
| signal_day_circuit | UPPER if sd_close≥prev×1.18, LOWER if ≤prev×0.82, else NONE (PROXY) |
| prev_day_ret_pct | (prev_close − prev_prev_close)/prev_prev_close×100 |
| prev_day_high_pct / low_pct | (prev_high|low − prev_prev_close)/prev_prev_close×100 |
| avg_20d_volume | mean volume over 20 bars strictly before signal_date (NULL if <20) |
| signal_day_vol_ratio | signal_day_volume / avg_20d_volume |
| entry_gap_pct | (entry_price − sd_close)/sd_close×100 |
| two_day_ret_pct | (sd_close − prev_prev_close)/prev_prev_close×100 |
| is_fresh_breakout | sd_ret>5 AND prev_day_ret∈[−1,1] |
| is_extended_move | two_day_ret>10 |
| is_gap_up_entry / is_gap_down_entry | entry_gap_pct>2 / <−2 |
| is_signal_day_circuit | circuit≠NONE |
| is_high_vol_confirm | vol_ratio>2 AND sd_ret>3 |
| is_low_vol_warning | vol_ratio<0.8 |
| entry_context | priority-resolved label (order above) |
| is_extension | 0 study / 1 extension |

## Analysis sheets (S1–S7), resolved + non-extension only
S1 by entry_context · S2 signal_day_ret buckets (<-5…>15) · S3 entry_gap buckets · S4 two_day momentum · S5 vol_ratio buckets · S6 ret×vol WR cross-tab · S7 combined gate (G1 proxy `d1_close_ret>0.5`, enterprise proxy `engine_rank<=3` — both documented in code).

## Parity checks printed
(a) all original study ids present in context; (b) entry_gap_pct∈[-10,10] >95%; (c) signal_day_ret_pct NULLs <100; (d) entry_date = signal_date+1..5 cal days. Plus row counts original vs +extension.

## Threshold report
From S2, scans high-runup buckets (lower bound ≥5, N≥20) and reports the first whose WR falls below the N-weighted baseline WR as the pre-entry filter ("entering next morning after the stock ran >X% on the signal day drops WR to Y% / avg Z%").

## CLI
`python build_signal_day_context.py --rnd-db <p> --prod-db <p> --out out/v8 [--top-n 10] [--dry-run]`
Defaults: rnd via `_resolve_rnd_db_path()`, prod via `PROD_DB`, out `./out/v8`. `--dry-run` computes + prints everything, writes nothing.

## Could not self-run
No Python/sqlite3 available to this agent (per instructions — orchestrator runs it). DB facts relied on the supplied VERIFIED FACTS; engine signatures and v3 sheet header verified by reading source + unzipping the xlsx. Recommend the orchestrator run `--dry-run` first to confirm parity (a)/(d) PASS and the extension note before APPLY.
