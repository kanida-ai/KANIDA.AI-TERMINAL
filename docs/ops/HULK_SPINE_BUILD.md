# FALCON HULK V1 — DATA SPINE BUILD LOG

Built: 2026-07-25 (IST). Isolated, point-in-time, leak-free feature+label
foundation for **RELIANCE** and **ADANIENT**. This is a NEW research engine and
touches **no** legacy `falcon_*` / `portfolio_*` table.

- **Output DB:** `data/db/falcon_hulk.db` (128.5 MB, created fresh)
- **Source (READ-ONLY, `mode=ro`):** `universe_engine/data/db/kanida_universe.db`
  - `mkt_ohlc_1min` (1-min bars, CASH segment) — drives the trading calendar and all labels
  - `ohlc_daily` (daily bars, 2016+) — drives daily features (2016–2017 used as rolling warmup)
- **Build scripts:** `hulk/hulk_build.py`, `hulk/hulk_verify.py`
- **Python:** `C:/Users/SPS/anaconda3/python.exe`
- **All times IST** (source market data is already IST; stored verbatim).

## Hard rules enforced
- **2026 sealed:** every source read has `bar_time < '2026-01-01'` / `trade_date < '2026-01-01'`
  in its SQL WHERE clause, plus a runtime assertion on the max loaded timestamp. Verified: every
  written table has max date ≤ `2025-12-31`.
- **Point-in-time:** every feature at T uses only bars ≤ T; every forward label uses only bars after T.
- **Isolation:** writes only to `falcon_hulk.db`; source opened read-only.
- **Window:** 2018-01-01 → 2025-12-31. Symbols: RELIANCE, ADANIENT.
- **Gaps never bridged:** the calendar comes from `mkt_ohlc_1min`, so RELIANCE's missing
  **March-2020** and **September-2022** (both entire months absent from 1-min) simply do not exist
  as sessions. Multi-day labels whose T→T+10 span crosses a calendar gap > 4 days get `gap_flag=1`.

## Source facts discovered
- Normal session = **375 bars**, 09:15→15:29. `complete_flag = 1` iff `n_bars ≥ 370`.
- 60-bar days = **Muhurat** evening sessions (e.g. 18:15–19:14) → `complete_flag=0`, excluded from the intraday grid.
- RELIANCE 1-min: **no** 2020-03 and **no** 2022-09 (whole months). `ohlc_daily` *does* have them, so
  the gap is 1-min-only. RELIANCE session counts drop accordingly: 2020 = 216 (vs ADANIENT 252), 2022 = 208 (vs 248).
- 6 special sessions (Budget Saturdays / Muhurat) exist in 1-min but not in `ohlc_daily`
  (2019-10-27, 2020-02-01, 2023-11-12, 2024-01-20, 2024-03-02, …).

## Design decisions (documented, not guessed)
- **Session OHLC for labels** is aggregated from the 1-min bars of each calendar session
  (open=first bar, high=max, low=min, close=**last bar (15:29) close**, vol=sum). This keeps labels
  internally consistent with the 1-min-derived calendar and makes independent verification clean.
- **Daily features** use a **unified daily series** = `ohlc_daily` (2016→2025) with the 6 special
  session dates appended from 1-min aggregation, sorted. Rolling windows use the whole series
  (all real bars ≤ T); feature **rows** are emitted only for dates in the 1-min session calendar
  within [2018,2025]. This gives full warmup for SMA-200 / dist_high_252.
- **Multi-day horizons are counted from the signal date T** for both entry kinds. For
  `entry_kind='close'` entry is T-close and ret_t1 = close(T+1)/close(T)−1. For `entry_kind='nextopen'`
  entry is open(T+1) and ret_t1 = close(T+1)/open(T+1)−1, ret_t2 = close(T+2)/open(T+1)−1, etc.
  This keeps `label_end_date` (= T+10 session date) aligned across entry kinds for downstream purge/embargo.
- **Short return** = −(exit/entry−1). MFE/MAE/peak are **direction-adjusted** (MFE = max favorable
  excursion via session/minute extremes; MAE = max adverse). `giveback = peak_ret − ret` (final horizon).
- **Column-name mapping** for the spec's `+0.5pct`/`+1pct`: multiday `days_to_p05`,`days_to_p1`;
  intraday `min_to_p05`,`min_to_p1`. Not-reached → NULL.
- **Intraday grid:** every 5 min from 09:20 to 14:30 (63 minutes) on complete sessions only.
  Label entry price = the grid minute's **close**; forward path uses bars strictly after the entry minute → 15:29.
- **Intraday `opening_range_pos`** uses the 09:15–09:30 OR and is **NULL before 09:30** (OR not yet
  formed) — strictly point-in-time. **`vwap_dev`** uses cumulative typical-price VWAP ((H+L+C)/3) up to
  the minute. **`cum_vol_vs_20d_avg`** = cumulative session volume at the minute ÷ mean cumulative
  volume at the same minute-of-day over the prior 20 complete sessions (strictly earlier dates).

## Tables & row counts

| table | RELIANCE | ADANIENT | total |
|---|---|---|---|
| `hulk_sessions` | 1,907 | 1,983 | 3,890 |
| `hulk_labels_multiday` | 7,626 | 7,930 | 15,556 |
| `hulk_labels_intraday` | 238,514 | 247,712 | 486,226 |
| `hulk_daily_features` | 1,907 | 1,983 | 3,890 |
| `hulk_intraday_features` | 119,257 | 123,856 | 243,113 |

Multiday = sessions × 2 directions × 2 entry_kinds, minus the final session's `nextopen` (no T+1).

### Feature columns
- **`hulk_daily_features`:** roc_1, roc_5, roc_10, roc_20, roc_60, dist_sma_20, dist_sma_50,
  dist_sma_200, slope_sma_20, rsi_14, atr_20_pct, range_pct, gap_pct, close_loc, dist_high_20,
  dist_high_60, dist_high_120, dist_high_252, vol_vs_20d, n_higher_highs_5, n_higher_lows_5.
- **`hulk_intraday_features`:** min_since_open, ret_since_open, opening_range_pos, vwap_dev,
  cum_vol_vs_20d_avg, rolling_5min_ret, rolling_15min_ret.

## Fully-worked sample label (RELIANCE, long, close-entry, 2023-03-28)
entry_price = 1070.95 (T-close). Horizon exit sessions: T+1 2023-03-29, T+2 2023-03-31,
T+3 2023-04-03, T+5 2023-04-06, T+10 2023-04-17.

| field | value |
|---|---|
| ret_t1 | −0.00182 |
| ret_t2 | +0.03674 |
| ret_t3 | +0.03712 |
| ret_t5 | +0.04141 |
| ret_t10 | +0.05355 |
| mfe_t5 | +0.04757 |
| mae_t5 | −0.01601 |
| days_to_p05 | 2 |
| days_to_p1 | 2 |
| peak_ret_t10 | +0.07914 |
| giveback_from_peak_t10 | +0.02558 |
| gap_flag | 0 |
| label_end_date | 2023-04-17 |

## Verification (`hulk/hulk_verify.py`) — 27/27 PASS, independent recompute from raw source
- **(a) 2026 never touched:** max date/time in all 5 tables ≤ 2025-12-31 (intraday features max = `2025-12-31 14:30:00`).
- **(b) Feature point-in-time:** `dist_high_20` and Wilder `rsi_14` recomputed from `ohlc_daily`≤T for
  3 (symbol,date) samples (chosen before the first special session so the pure-daily recompute matches
  the unified series exactly); `ret_since_open`, `vwap_dev`, `min_since_open` recomputed from 1-min≤11:00. All match.
- **(c) Label correctness:** multiday `ret_t5` and intraday `ret_to_close` recomputed from raw 1-min
  session closes; entry prices match 1-min bars; short = −long confirmed.
- **(d) Gap handling:** RELIANCE 2020-03 and 2022-09 have **zero** fabricated sessions; the last pre-gap
  session's multi-day label carries `gap_flag=1` (2020-02-19→next actual 2020-04-20; 2022-08-05→2022-10-07);
  a clean 2019-06-14 entry is `gap_flag=0`.
- **(e) Row counts** per table/symbol/year printed (see script output).

## How to rebuild / re-verify
```
C:/Users/SPS/anaconda3/python.exe hulk/hulk_build.py     # ~55s, recreates falcon_hulk.db
C:/Users/SPS/anaconda3/python.exe hulk/hulk_verify.py    # prints 27 checks + row counts
```

## Notes for the downstream miners
- MFE/MAE are currently on session extremes (daily granularity); the multiday MFE/MAE can be refined
  later with the 1-min path (source data is present) without touching the schema.
- Use `gap_flag` and `label_end_date` for purge/embargo when training on the multiday labels.
- Intraday tables cover complete sessions only; Muhurat/short sessions are intentionally excluded there
  but still present in `hulk_sessions` (with `complete_flag=0`) so they can act as T+n calendar sessions.
