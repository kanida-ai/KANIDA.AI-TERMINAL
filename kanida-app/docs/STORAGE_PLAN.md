# Storage plan — stop storing price history, keep a rolling window

**Written:** 19 Sep 2026 · **Status:** proposal, nothing has been changed · **Read-only work only**

The owner's instruction, in his words:

> "we should not use the OHLCV data we should fetch online from kite now and with vendor later.
> only the backtest which is a separate DB we should keep. for derivative we should store only
> what we need and overwrite periodically."

This document measures what is actually on disk, works out the smallest amount of price history
the product genuinely needs, and sets out a safe order for getting there.

**Two rules were followed while writing this.** Nothing was deleted, moved, compacted or changed.
Every database was opened read-only. Every number below was measured on this machine, or is
labelled as a projection with the measurement it came from.

---

## 0. The three things worth knowing before anything else

**1. The 147 GB file is already off the live path.** The scanner does not read it for prices any
more. It reads `market15.db`. Measured from the running scanner at 12:25 on 19 Sep:

| What the live scanner reports | Value |
| --- | --- |
| Price source | `market15` |
| Database file | `db\market15.db` |
| Patterns running | 107 |
| Stocks in the universe | 1,431 |
| Stocks it actually had prices for | 513 |
| Stocks it had no prices for | 918 |
| Time for a full scan, all 4 timeframes | 183 seconds |

The 147 GB file (`kanida.db`) last received a price on **31 Jul 2026**. It is seven weeks stale and
nothing writes to it. The only thing the live product still takes from it is a **1,431-row list of
stock names and sectors** that measures **0.2 MB**.

So: 147.16 GB is being kept alive to serve 0.2 MB.

**2. The "live product tables" in that file are empty.** `users`, `orders`, `positions`, `signals`,
`signal_outcomes`, `virtual_portfolios`, `virtual_positions`, `virtual_trades`,
`autotrade_sessions`, `broker_accounts`, `instruments` — every one of them has **exactly zero rows**
(measured with `count(*)`, which is exact). They are empty shells from an old design. The real
product state lives in `kanida-app\var\pilot.sqlite3`, which is **1.25 MB**.

This means the "split the live tables off the bulk data" job is mostly already done. There is
nothing to move.

**3. The answer to "no OHLCV" is "a rolling window, and it is about 2 GB."** That number is
derived in section 2.4 from the detector code itself, not guessed.

---

## 1. What is on disk today — measured

### 1.1 How it was measured, and why you can trust it

Counting rows on a 147 GB file with `count(*)` would have taken hours. Two faster methods were
used instead.

| What | Method | Exact or estimated? |
| --- | --- | --- |
| File size | Windows file size + SQLite's own page count | **Exact** |
| Size of each table and each index | Walked the database's internal index structure, reading only the "signpost" pages, never the data pages | **Exact** (see check below) |
| Row counts on huge tables | Read the row-count field in the header of 600 randomly chosen data pages, scaled up | **Estimated, ±0.5%** (see check below) |
| Row counts on small tables | `count(*)` | **Exact** |

**Check on the size method.** The per-table page counts add up to within **6 pages out of
38,576,511** for the 147 GB file, and within 4–6 pages for the other three. That is an error of
0.00002%. The method is sound because none of the four databases uses auto-compaction and none has
any free space inside it (measured: free-page count is zero in all four).

**Check on the row-count method.** The sampled estimate was compared against a real `count(*)` on
four tables:

| Table | Sampled estimate | Exact `count(*)` | Error |
| --- | --- | --- | --- |
| `ohlc_futures_daily` | 14,289 | 14,289 | 0.00% |
| `ohlc_futures_5min` | 993,125 | 990,967 | +0.22% |
| `ohlc_futures_1min` | 4,879,844 | 4,877,661 | +0.04% |
| `ohlc_daily` | 2,628,145 | 2,620,201 | +0.30% |

So a sampled row count in this document is good to better than half a percent. One exception is
noted where it was worse: see section 5.1.

**What could not be measured, and why.** The earliest and latest price in `ohlc_1min` and
`ohlc_5min` could not be read by a single query — SQLite has to read all 765 million rows to answer
it, and the query was still running after ten minutes when it was stopped. The date range in the
table below was obtained a different way: by asking for the first and last price of each of the
1,564 stocks individually through the index, which took under a second. That result is **exact**.

### 1.2 `db\kanida.db` — 147.16 GB

**Measured file size: 158,009,389,056 bytes = 147.16 GB.** 38,576,511 pages of 4 KB. No free space
inside. Last written 2 Aug 2026.

Sizes below include the table **and** its indexes, because that is what actually occupies the disk.

| Group | Rows | Size | Share of file | Stocks | Dates covered |
| --- | --- | --- | --- | --- | --- |
| `ohlc_1min` | 765,342,938 *(est ±0.5%)* | **120.05 GB** | 81.6% | 1,564 | 2 Feb 2015 – 31 Jul 2026 |
| `ohlc_5min` | 162,283,110 *(est ±0.5%)* | **25.35 GB** | 17.2% | 1,564 | 2 Feb 2015 – 31 Jul 2026 |
| `ohlc_daily` | 2,620,201 *(exact)* | **422.5 MB** | 0.3% | 1,562 | 1 Jan 2013 – 31 Jul 2026 |
| `ohlc_futures_1min` | 4,877,661 *(exact)* | **1,139.4 MB** | 0.8% | 208 | 27 May 2026 – 30 Jul 2026 |
| `ohlc_futures_5min` | 990,967 *(exact)* | **231.3 MB** | 0.2% | 208 | 27 May 2026 – 30 Jul 2026 |
| `ohlc_futures_daily` | 14,289 *(exact)* | **3.3 MB** | 0.0% | 208 | 27 May 2026 – 30 Jul 2026 |
| `event_markers` | 13,919 *(exact)* | 1.4 MB | 0.0% | | |
| `corp_actions` | 4,296 *(exact)* | 0.7 MB | 0.0% | | |
| `instrument_labels` | 1,567 *(exact)* | **0.2 MB** | 0.0% | | the stock name/sector list |
| `schema_meta` | 1 *(exact)* | 0.0 MB | | | |
| 11 empty product tables | **0** *(exact)* | 0.1 MB | | | `users`, `orders`, `signals`, … |
| **Total** | | **147.16 GB** | 100% | | |

**Price history is 99.998% of this file.** Everything that is not price history adds up to 2.4 MB.

**A free 36.79 GB is sitting in duplicated indexes.** Each `ohlc_*` table has a primary key on
`(symbol, bar_time)`, which SQLite indexes automatically. Each table *also* has a hand-made index
on exactly the same two columns. They are byte-for-byte the same work done twice — confirmed by
their page counts being identical to the digit.

| Duplicate index | Size | Duplicates |
| --- | --- | --- |
| `idx_ohlc_1min_sym_dt` | **30.02 GB** | the `ohlc_1min` primary key |
| `idx_ohlc_5min_sym_dt` | **6.38 GB** | the `ohlc_5min` primary key |
| `idx_ohlc_daily_sym_dt` | 107.6 MB | the `ohlc_daily` primary key |
| `idx_ohlc_futures_1min_ts_dt` | 235.3 MB | the futures primary key |
| `idx_ohlc_futures_5min_ts_dt` | 47.9 MB | the futures primary key |
| `idx_ohlc_futures_daily_ts_dt` | 0.7 MB | the futures primary key |
| **Total wasted** | **36.79 GB** | |

If the file were kept for any reason, dropping these six would free 36.79 GB with zero effect on
anything. This is noted for completeness; the plan below drops the whole file, so it is moot.

### 1.3 `db\market15.db` — 18.17 GB

**Measured file size: 19,512,483,840 bytes = 18.17 GB.** Plus a 45.6 MB write-ahead log. This is
the live store. It is current to 18 Sep 2026 15:15.

| Group | Rows | Size | Share | Notes |
| --- | --- | --- | --- | --- |
| `candles_15m` | 33,735,543 *(est ±0.5%)* | **10.55 GB** | 58.0% | 513 stocks, 2 Feb 2015 – 18 Sep 2026 |
| `snapshot_members` | 56,956,498 *(exact)* | **7.44 GB** | 41.0% | see below — it is **not** price data |
| `daily_bars` | 551,867 *(est)* | 126.9 MB | 0.7% | 495 stocks, 5 Jul 2021 – 18 Sep 2026 |
| `quality_findings` | 108,783 *(exact)* | 54.4 MB | 0.3% | repair evidence |
| `raw_archive` | 6,344 *(exact)* | 2.6 MB | 0.0% | pointers to the raw vendor payloads on disk |
| `corrections` | 2,336 *(exact)* | 0.8 MB | 0.0% | every value ever changed, with its reason |
| everything else | | 0.2 MB | | `quarantine` 7, `ingest_runs` 154, `meta`, `session_regimes` 201 |
| **Total** | | **18.17 GB** | 100% | |

**`snapshot_members` deserves attention: 7.44 GB, and it holds no prices.** It is a *list of which
candles belonged to which frozen snapshot* — five short columns, no open/high/low/close. There are
three snapshots:

| Snapshot | Universe | Stocks | Rows it points at | Frozen |
| --- | --- | --- | --- | --- |
| `snap_20260916T040035_c10c7ebc` | pilot-PIIND | 1 | 70,824 | 16 Sep 04:00 — a smoke test |
| `snap_20260916T044534_ecc59915` | NIFTY500 | 513 | 29,018,297 | 16 Sep 04:53 |
| **`snap_20260916T055228_f4bab0e9`** | NIFTY500 | **495** | **27,867,377** | **16 Sep 06:11 — the research snapshot** |

**This is the single most important safety fact in this document, and it is in section 4.**
The frozen snapshot is a *pointer list*. It does not contain the candles. If the candles are
trimmed, the pointers dangle and the frozen snapshot can no longer be reconstructed.

**A free 3.45 GB.** `idx_snapmem_snapshot` indexes `snapshot_id`, which is already the first column
of that table's primary key. SQLite can answer every query it serves from the primary key. It is
entirely redundant, and it measures **3.45 GB**.

### 1.4 `db\derivatives.db` — 622 MB and growing

**Measured file size: 652,603,392 bytes = 622.4 MB.** Two capture days so far (17 and 18 Sep).

| Group | Rows | Size |
| --- | --- | --- |
| `candles_15m` | 2,675,883 *(exact)* | **411.9 MB** |
| `metrics` | 269,578 *(exact)* | **120.0 MB** |
| `snapshots` | 265,601 *(exact)* | **78.7 MB** |
| `contracts` | 35,922 *(exact)* | 9.0 MB |
| `backfill_progress` | 15,337 *(est)* | 2.3 MB |
| `underlying_snapshots` | 2,013 *(exact)* | 0.4 MB |
| `captures` 26, `runs` 12, `meta` 3, `daily_rollups` 0, `candles_day` 0 | | 0.1 MB |
| **Total** | | **622.4 MB** |

Growth is covered in section 5.

### 1.5 `db\KANIDA_SNR.db` — 490 MB

**Measured file size: 514,260,992 bytes = 490.4 MB.** Last written 2 Aug 2026. This is old
research output, not on any live path.

| Group | Rows | Size |
| --- | --- | --- |
| `mined_patterns` | 404,340 *(est)* | **141.0 MB** |
| `unified_patterns` + 5 seed copies | ~1,248,556 *(est)* | **345.0 MB** |
| `trade_log_rev` | 20,672 *(est)* | 4.0 MB |
| everything else (10 small tables) | ~3,250 | 0.4 MB |
| **Total** | | **490.4 MB** |

The six `unified_patterns*` tables are the same shape repeated (`unified_patterns`,
`_event`, `_seed13`, `_seed23`, `_seed37`, `_seed51`) — a seed-robustness check. They are kept.

### 1.6 Other measured sizes

| Location | Size | What it is |
| --- | --- | --- |
| `db\frame_cache\` | 831 MB | a cache — regenerable |
| `db\raw_archive\` | 200 MB | the raw vendor payloads the repair evidence points at |
| `db\derivatives_cache\` | 8.1 MB | a cache — regenerable |
| `kanida-app\var\pattern_history.sqlite3` | 174.9 MB | pattern history served to the app |
| `kanida-app\var\research_index.sqlite3` | 154.3 MB | the research index served to the app |
| `kanida-app\var\last_good.sqlite3` | 8.9 MB | last-good copies of app reads |
| `kanida-app\var\pilot.sqlite3` | **1.25 MB** | **the real live product state** |

**Free space on C:** 392.3 GB free of 930.4 GB (measured). There is room to do the work safely.

---

## 2. What must be kept, and why

Four different kinds of thing are tangled together today. They have nothing in common and should
not share a file.

### 2.1 Things that cannot be re-fetched, or are expensive to re-fetch

**How expensive is a re-fetch, exactly?** Two things were measured.

**Kite's per-request limits**, taken from the code where they were established by live testing, not
copied from a manual (`market_data\kite_provider.py`, lines 94–103: *"Measured live … not copied
from docs. `15minute` was confirmed to accept a 200-day span and to reject 201."*):

| Interval | Calendar days per request |
| --- | --- |
| 1-minute | 60 |
| 5-minute | 100 |
| 15-minute | 200 |
| daily | 2,000 |

**The real request rate.** Kite publishes 3 requests per second. The *actual achieved* rate was
measured across all 154 recorded ingest runs: **38,620 requests in 13,831 seconds = 2.79 requests
per second.** Steady-state runs sit at 2.85–2.92.

Putting the two together — the number of requests is exact (first and last date of every stock,
divided by the per-request cap), the hours are that divided by the rate:

| To re-fetch | Requests *(exact)* | Hours at 3.00/s | Hours at 2.79/s *(measured rate)* |
| --- | --- | --- | --- |
| `ohlc_1min`, 1,564 stocks, 11.5 years | 56,426 | 5.2 | **5.6** |
| `ohlc_5min`, 1,564 stocks, 11.5 years | 34,130 | 3.2 | **3.4** |
| `ohlc_daily`, 1,562 stocks, 13.6 years | 2,806 | 0.3 | **0.3** |
| **All of `kanida.db` price history** | **93,362** | **8.6** | **9.3** |
| Rebuild `market15` 15-minute store, 513 stocks | 11,286 | 1.0 | **1.1** |
| Same, widened to all 1,431 stocks | 31,482 | 2.9 | **3.1** |

**These are floors, not forecasts.** They assume every request succeeds first time and nothing goes
wrong. A real run has failures, retries, token refreshes and overnight gaps. Treat 9.3 hours as
"one long overnight run if everything behaves", not a promise.

**What genuinely cannot be re-fetched at all:**

| Thing | Why it is gone forever |
| --- | --- |
| Expired option and futures contracts | Kite stops serving a contract after expiry. The backlog already records this (item 2b: *"options data cannot be recovered once contracts expire"*). |
| The exact bytes a research run read | Vendors silently restate history. A re-fetch today would not reproduce the file the research was computed from. This is the whole point of the frozen snapshot. |
| The repair evidence | `corrections` (2,336 rows) records every value that was changed and why. That reasoning cannot be recreated. |

### 2.2 The research output — the owner's "backtest DB"

This is what the 107 patterns were built on and what the app serves as evidence. It must survive
intact and, critically, must become **standalone** — able to answer every question without reaching
back into the raw candles.

| Item | Where | Measured size | Standalone today? |
| --- | --- | --- | --- |
| The 107 pattern definitions | in code (`market_scanner\pattern_research\`) | — | yes |
| The pinned research run `4b33a5249562631524d6` | `market_scanner\output\expanded_research\` | **not measured — see open questions** | **not confirmed** |
| The warm-up proof `live_warmup.json` | `market_scanner\output\` | 12,652 bytes | yes |
| The frozen snapshot `snap_20260916T055228_f4bab0e9` | `market15.db` | 27,867,377 **pointers**, ~3.6 GB of the 7.44 GB | **NO — it is pointers only** |
| Repair evidence: `corrections` | `market15.db` | 0.8 MB | yes |
| Repair evidence: `quality_findings` | `market15.db` | 54.4 MB | yes |
| Repair evidence: `quarantine`, `ingest_runs`, `raw_archive` | `market15.db` | 2.6 MB | pointers into `db\raw_archive\` |
| The raw vendor payloads | `db\raw_archive\` | 200 MB | yes |
| Older mined research | `KANIDA_SNR.db` | 490.4 MB | yes |
| Pattern history served to the app | `kanida-app\var\pattern_history.sqlite3` | 174.9 MB | believed yes |
| Research index served to the app | `kanida-app\var\research_index.sqlite3` | 154.3 MB | believed yes |
| `signal_outcomes` | `kanida.db` | **0 rows — the table is empty** | n/a |

Two entries need saying plainly.

**`signal_outcomes` is empty.** The brief listed it as research output to protect. It has zero
rows. Whatever fills it has not run, or fills something else. Nothing is lost by moving it.

**The frozen snapshot is not standalone.** `snapshot_members` stores five columns —
snapshot id, instrument id, symbol, bar start, revision. No prices. It says *"the research read
these 27,867,377 candles"*, and the candles themselves sit in `candles_15m`. Trim `candles_15m` and
the snapshot becomes a list of pointers to nothing. **This is the thing that must be fixed before a
single row is deleted**, and section 4 is built around it.

### 2.3 Live product state

Small, irreplaceable, and it must never share a file with bulk data — because a bulk job holding
the write lock is a bulk job blocking a customer (section 6).

| Table | Rows | Where it lives today |
| --- | --- | --- |
| Everything the app actually uses | | `kanida-app\var\pilot.sqlite3`, **1.25 MB** |
| `users`, `orders`, `positions`, `signals`, `signal_outcomes`, `virtual_portfolios`, `virtual_positions`, `virtual_trades`, `autotrade_sessions`, `broker_accounts`, `instruments` | **all 0** | `kanida.db` — empty shells, no code reads them |

The product state is **already** in its own small file. Nothing needs moving. The 11 empty tables
in `kanida.db` exist only in `db\schema.sql` and are referenced by no code anywhere.

### 2.4 The rolling window — the key number

**The question.** If price history is fetched live rather than stored, how much recent history must
still be kept on hand for the 107 live detectors to produce exactly the same answers?

**This was not guessed. It is already measured, and the measurement is on disk.**
`market_scanner\output\live_warmup.json`, produced 16 Sep 2026. The scanner *refuses to run the
107-pattern set at all* until this file exists and passes — the gate is in
`market_scanner\pattern_live.py`.

How the measurement was done, in the module's own terms: run every detector over a stock's whole
history; then re-run it on progressively shorter recent windows; find the shortest window that
produces an identical list of events, at identical timestamps, at several different simulated
"now" moments. Then raise that to the detector family's own declared lookback, because *"a sampled
tail that happened to contain no 150-bar chart formation is not evidence that a shorter window
suffices."*

| The measurement run | Value |
| --- | --- |
| Result | **pass** — no failed cells, no coverage gaps |
| Stocks tested | 8 (including a suspension gap and a late-listing case, chosen before results were seen) |
| Window sizes tried | 60, 80, 100, 130, 170, 220, 280, 360, 460, 600, 800 |
| Comparisons made | 6,005 |
| Detector events compared | 53,284 |
| Disagreements that survived the largest window | 4, a rate of 0.0075% — arithmetic rounding, not missing history |

The per-family result. "Measured" is where disagreements first hit zero; "declared" is the family's
own stated lookback; the requirement is the larger of the two:

| Detector family | Timeframe | Measured enough | Family declares | **Required** |
| --- | --- | --- | --- | --- |
| Chart patterns | 1H | 220 | 260 | **260** |
| Chart patterns | 4H | 220 | 260 | **260** |
| Chart patterns | 1D | 170 | 260 | **260** |
| Chart patterns | 1W | 170 | 260 | **260** |
| Harmonic | 1H / 4H / 1D / 1W | 100 / 130 / 100 / 80 | 163 | **163** |
| Candlestick | all four | 100 | 25 | **100** |
| Price action | all four | 100 | 30 | **100** |

**The binding number is 260 candles, on every one of the four timeframes.** The chart-pattern
family sets it.

**From 260 candles to candles on disk.** The scanner already does this arithmetic itself, in
`market_scanner\engine.py`, function `history_window`. Reading that code with 260 plugged in:

```
sessions    = max( ceil(260 / 6),  ceil(260 / 2) )  = max(44, 130) = 130   + 10 slack = 140
              (a trading day holds 6 one-hour buckets and 2 four-hour buckets)

daily_bars  = max( 260 x 1,  260 x 5 )              = max(260, 1300) = 1300 + 10 slack = 1310
              (a weekly candle needs a full Mon-Fri of daily bars)
```

The +10 is for public holidays inside the window. The code's comment is explicit that the slack is
*"never for the detector"*.

**15-minute candles per trading day: measured, not assumed.** Counted directly over the last 142
sessions for five stocks: 24.8 per session on average — 24 since the 3 Aug 2026 close moved to
15:15, 25 before that. **25 is used below as the safe upper bound.**

**Bytes per candle: measured.** Table plus every index, divided by rows:

| | Bytes per row |
| --- | --- |
| `candles_15m`, table only | 212.2 |
| `candles_15m`, **table + all 4 indexes** | **335.7** |
| `daily_bars`, table + index | **241.1** |

**The arithmetic, in full:**

```
Per stock, 15-minute candles :  140 sessions  x  25 candles   =   3,500 candles
Per stock, daily bars        :                                =   1,310 bars

For 1,431 stocks:
  15-minute :  1,431  x  3,500  =  5,008,500 candles  x 335.7 B  =  1.57 GB
  daily     :  1,431  x  1,310  =  1,874,610 bars     x 241.1 B  =  0.42 GB
                                                       TOTAL     =  1.99 GB
```

**The rolling window is about 2.0 GB.** That replaces 147.16 GB + 10.55 GB of stored candles.

Three variations, so the size can be judged rather than taken on faith:

| Scenario | 15-minute | Daily | **Total** |
| --- | --- | --- | --- |
| 513 stocks — what the scanner actually has prices for today | 0.56 GB | 0.15 GB | **0.71 GB** |
| **1,431 stocks — the full universe the app lists** | 1.57 GB | 0.42 GB | **1.99 GB** |
| 2,000 stocks — head-room for growth | 2.19 GB | 0.59 GB | **2.78 GB** |
| 1,431 stocks, keeping every revision as the store does today | 2.75 GB | 0.42 GB | **3.17 GB** |

That last row matters. `candles_15m` keeps *revisions* — a corrected candle is stored beside the
original rather than overwriting it. Measured: up to 1.75 rows per distinct candle (BEL, CIPLA and
INFY each store 6,166 rows for 3,516 distinct candles over 142 sessions). A rolling window should
keep the current revision only, which is the 1.99 GB line. If revision history is kept inside the
window too, budget 3.17 GB. Either number is small.

**One honest warning: the daily window cannot be shrunk.** The requirement is 1,310 daily bars per
stock. Measured, `daily_bars` holds **an average of 1,164 and a maximum of 1,293** bars per stock.
The store is *already slightly short* of what weekly chart patterns need. Cutting the daily window
below 1,310 would silently disable weekly chart detection. It must be treated as a floor, not a
target.

### 2.5 Where the owner's instruction cannot be followed literally

| The instruction | What is possible | Why |
| --- | --- | --- |
| "we should not use the OHLCV data" | True for **stored history**: 147 GB can go. Not true for **all** OHLCV: a 2.0 GB rolling window must stay on disk. | A 1-hour chart pattern needs 260 one-hour candles. Fetching 260 candles for each of 1,431 stocks on every scan would be 1,431 requests per scan at 2.79 per second — **8.5 minutes per scan** before any detection work. The scan currently takes 183 seconds for everything. Live fetching for detection is not viable at 3 requests a second. |
| "fetch online from kite now" | Already true for new prices. `market15.db` is fed live every 15 minutes. | Nothing to change. |
| "and with vendor later" | Already designed for. The provider is swappable and there is a conformance test suite. | Backlog item 6. |
| "only the backtest which is a separate DB we should keep" | Achievable, **but not yet true**. The research snapshot is pointers into the live candles, not a separate self-contained thing. | Section 4 step 2 makes it true. |
| "for derivative … store only what we need and overwrite periodically" | Fully achievable. Measured proposal in section 5. | |

---

## 3. What gets dropped, and the size after

### 3.1 `db\kanida.db` — 147.16 GB becomes about 2.4 MB

| Table | Now | After | Decision |
| --- | --- | --- | --- |
| `ohlc_1min` | 120.05 GB | **0** | Drop. Nothing reads it. 5.6 hours to re-fetch if ever needed. |
| `ohlc_5min` | 25.35 GB | **0** | Drop. Nothing reads it. 3.4 hours to re-fetch. |
| `ohlc_daily` | 422.5 MB | **0** | Drop. 0.3 hours to re-fetch. Superseded by `market15.daily_bars`. |
| `ohlc_futures_1min` | 1,139.4 MB | **0** | Drop. Only 2 months of data (27 May – 30 Jul 2026) and no product reads it. |
| `ohlc_futures_5min` | 231.3 MB | **0** | Drop, same reason. |
| `ohlc_futures_daily` | 3.3 MB | **0** | Drop, same reason. |
| `instrument_labels` | 0.2 MB | **0.2 MB** | **Keep.** This is the one thing the live product needs. |
| `event_markers` | 1.4 MB | 1.4 MB | Keep. Small, and used by research scripts. |
| `corp_actions` | 0.7 MB | 0.7 MB | Keep. Corporate actions cannot be reconstructed cheaply. |
| 11 empty product tables | 0.1 MB | 0.1 MB | Keep the definitions; they cost nothing. |
| **Total** | **147.16 GB** | **≈ 2.4 MB** | |

The file becomes a small reference list. Renaming it to something like `reference.db` would make
that obvious, but that is a naming decision, not a storage one.

### 3.2 `db\market15.db` — 18.17 GB becomes about 2.3 GB

| Table | Now | After | Decision |
| --- | --- | --- | --- |
| `candles_15m` | 10.55 GB | **1.57 GB** | Trim to the 140-session rolling window, all 1,431 stocks. |
| `daily_bars` | 126.9 MB | **0.42 GB** | *Grows.* Today it covers 495 stocks; the window needs 1,310 bars for all 1,431. |
| `snapshot_members` | 7.44 GB | **0** in this file | Moves out — see section 4 step 2. Also, 3.45 GB of it is a redundant index that can go immediately. |
| `quality_findings` | 54.4 MB | 54.4 MB | Keep. Repair evidence. |
| `raw_archive` | 2.6 MB | 2.6 MB | Keep. Points at the 200 MB payload archive. |
| `corrections` | 0.8 MB | 0.8 MB | Keep. Every changed value and its reason. |
| everything else | 0.2 MB | 0.2 MB | Keep. |
| **Total** | **18.17 GB** | **≈ 2.05 GB** | |

### 3.3 `db\derivatives.db` — a growth rule, not a one-off cut

Detail in section 5. Headline: **223 MB per trading day becomes about 130 MB per trading day**, and
the file settles at roughly **23 GB** instead of roughly **47 GB**.

### 3.4 `db\KANIDA_SNR.db` — unchanged at 490 MB

Research output. Not on a live path. Leave it alone.

### 3.5 The new file, and the overall before/after

A new database is created to hold the research evidence so it stops depending on the live candles.
Its size is the frozen snapshot's candles copied out in full, once:

```
27,867,377 candles  x  335.7 bytes  =  9.36 GB        (projection, from the measured bytes-per-row)
```

That is a **projection**, not a measurement, and it is a generous one: the copy can drop the
provenance columns and the indexes it will not need. A realistic figure is 5–7 GB. It is written
once, read rarely, never written again, and can be compressed or moved to cheap storage.

**The whole picture:**

| File | Now *(measured)* | After *(projected)* | Change |
| --- | --- | --- | --- |
| `kanida.db` | **147.16 GB** | 0.002 GB | **−147.16 GB** |
| `market15.db` | **18.17 GB** | 2.05 GB | **−16.12 GB** |
| `derivatives.db` | 0.62 GB | 0.62 GB *(growth rule changes, see §5)* | 0 today |
| `KANIDA_SNR.db` | 0.49 GB | 0.49 GB | 0 |
| **new** research evidence file | — | **9.36 GB** *(projected, likely 5–7)* | +9.36 GB |
| `db\raw_archive\` | 0.20 GB | 0.20 GB | 0 |
| **Total** | **166.64 GB** | **12.72 GB** | **−153.92 GB, a 92% reduction** |

If the frozen snapshot is instead archived to a compressed file outside the database — which is
what it is for, since nothing queries it day to day — the live footprint is **3.2 GB**, a 98%
reduction.

---

## 4. The order of operations — safety first

**The rule that governs everything below:** the raw candles are what the 107 patterns were
researched on. Once they are gone, re-running that research means re-fetching years of data at
roughly 3 requests a second. Measured: **9.3 hours for `kanida.db`, 1.1 hours to rebuild the
15-minute store for today's 513 stocks, 3.1 hours to widen it to all 1,431.**

Nothing is deleted until the thing that replaces it has been proved to stand on its own.

### Step 1 — Free 40.24 GB with no risk at all

Drop the six duplicate indexes in `kanida.db` (36.79 GB) and the redundant one in `market15.db`
(3.45 GB). These index columns that are *already* indexed by the primary key. Every query keeps
working.

| Before | After | Recovery if wrong |
| --- | --- | --- |
| 166.64 GB | 126.40 GB | Rebuild the index. Minutes. **Nothing becomes unrecoverable.** |

**Verification before doing it:** run the scanner's own test suite (432 tests, 186 subtests) and
the pilot's (270 tests), and confirm a full scan still returns 107 patterns over 1,431 stocks in
about 183 seconds.

### Step 2 — Make the research evidence standalone. Delete nothing.

Copy the frozen snapshot's 27,867,377 candles — the actual open/high/low/close/volume, not
pointers — into a new, separate, read-only research database, together with `corrections`,
`quality_findings`, `quarantine`, `raw_archive`, `ingest_runs` and the `snapshots` header row.

| Before | After | Recovery if wrong |
| --- | --- | --- |
| 126.40 GB | 135.76 GB *(it grows — this step only adds)* | Delete the new file and start over. **Nothing becomes unrecoverable.** |

**This step must pass every one of these before step 3 is even considered:**

1. The new file's row count equals **27,867,377** exactly.
2. The checksum recorded on the frozen snapshot —
   `580af23f585159081c62760eaf1265a2a7afd498c34652df59e6c3b894dd7b19` — recomputes from the new
   file and matches. The snapshot header records exactly this so a research run can prove which
   bytes it read.
3. Distinct stock count is **495**, and the first and last candle are `2015-02-02 09:15:00` and
   `2026-09-15 15:15:00`.
4. The 107 detectors, run against the new file, reproduce the pinned research run
   `4b33a5249562631524d6` event for event — same detector spec hash
   `e88131bc87d3d5a9c0945a6669c78e8ddec50d122733655e92d62a5eb92414b5`, same timestamps.
5. The warm-up gate is re-run against the new file and still reports **pass** with 260 required
   bars on all four timeframes.
6. Every evidence card the app can show is served correctly with the live candle store
   disconnected. If anything still reaches into `market15.db` for evidence, it is not standalone
   and step 3 does not happen.
7. The new file is copied somewhere off this machine. One disk is not a backup.

**Point-in-time rules still apply to all of it.** Any evidence quoted from this file must be the
same strategy that is traded — the exact stop, target, trail and horizon — with costs and slippage
applied to every simulated trade, judged on expectancy rather than win rate, and with any small
sample labelled as small. Copying the data does not change the standard it is held to.

### Step 3 — Split, still deleting nothing

Create the new, small reference database with `instrument_labels`, `corp_actions` and
`event_markers`. Point the scanner and the pilot at it. Leave the 147 GB file in place, untouched,
as its own backup.

| Before | After | Recovery if wrong |
| --- | --- | --- |
| 135.76 GB | 135.76 GB | Point the config back. Seconds. **Nothing becomes unrecoverable.** |

**Verification before step 4:** the scanner boots, reports universe 1,431, and completes a full
scan; the pilot's 270 tests pass; the app's Discover acceptance suite runs clean at 107 checks.
One code change is required first — see risk R1 in section 7.

### Step 4 — Run for two full weeks with the old file still there

Ten trading sessions with nothing reading `kanida.db`. Confirm it by leaving it in place and
watching that nothing opens it.

| Before | After | Recovery if wrong |
| --- | --- | --- |
| 135.76 GB | 135.76 GB | **Nothing becomes unrecoverable.** This step exists purely to catch the job nobody remembered. |

### Step 5 — Delete the price history. **This is the first irreversible step.**

| Before | After | What becomes unrecoverable |
| --- | --- | --- |
| 135.76 GB | 13.79 GB | **The 1-minute history.** Re-fetch costs **5.6 hours**, and only back as far as Kite still serves. The 5-minute history: **3.4 hours**. Daily: **0.3 hours**. The two months of futures history (27 May – 30 Jul 2026): **re-fetchable only while those contracts are still served — for expired contracts, never.** |

Delete the file rather than emptying it. Emptying a SQLite file does not return the space to
Windows without a rebuild, and a rebuild of a 147 GB file needs 147 GB of free space (392.3 GB
available, measured, so it would work — but deleting is simpler and faster).

### Step 6 — Trim the live candle store to the rolling window. **Also irreversible.**

| Before | After | What becomes unrecoverable |
| --- | --- | --- |
| 13.79 GB | **3.20 GB** | Anything in `candles_15m` older than **140 sessions** and outside the frozen snapshot. Re-fetchable at **1.1 hours for 513 stocks, 3.1 hours for 1,431** — but the *exact bytes* are not recoverable, because a vendor re-fetch returns today's restated history, not the history as it stood. That is precisely why step 2 comes first. |

Do not trim `daily_bars`. It is already slightly short of the 1,310-bar requirement (section 2.4).

### Step 7 — Turn on the derivatives rules

Apply the slimmed capture and the retention schedule from section 5. Reversible for the columns
still being written; irreversible for the columns no longer captured — though none of them is read
by anything today.

### Summary of the sequence

| Step | Action | Total after | Reversible? |
| --- | --- | --- | --- |
| 1 | Drop 7 duplicate indexes | 126.40 GB | **Yes** |
| 2 | Copy research evidence to its own file | 135.76 GB | **Yes** |
| 3 | Split out the reference tables | 135.76 GB | **Yes** |
| 4 | Two weeks of observation | 135.76 GB | **Yes** |
| 5 | **Delete `kanida.db` price history** | 13.79 GB | **NO** — 9.3 h to re-fetch |
| 6 | **Trim to the 140-session window** | **3.20 GB** | **NO** — 1.1–3.1 h, and never byte-identical |
| 7 | Derivatives slimming + retention | ongoing | Partly |

Steps 1 to 4 are safe. Steps 5 and 6 are the ones that need the owner's explicit go-ahead, and they
should only be asked for once every check in step 2 has passed.

---

## 5. Derivatives — store only what we need, overwrite periodically

### 5.1 What is being captured now — measured

The capture runs **26 times a day**: every 15 minutes from 09:30 to 15:30, plus one after the close
at 15:45. Scope is the front two expiries per underlying, which resolved on 18 Sep to **27,260
contracts** (13,458 calls, 13,370 puts, 432 futures) across **216 underlyings**. About 27,238 rows
are written per mark.

Measured on 18 Sep 2026, the first full-scope day:

| Table | Bytes per row *(measured)* | Rows that day *(exact)* | **Per trading day** |
| --- | --- | --- | --- |
| `snapshots` | 310.7 | 249,408 | **73.9 MB** |
| `metrics` | 466.8 | 253,330 | **112.8 MB** |
| `candles_15m` | 161.4 | 237,793 | **36.6 MB** |
| **Total** | | | **223.3 MB per day** |

That matches the "about 220 MB a day" figure in the brief.

*One measurement caveat, stated rather than hidden:* the sampled row-count estimate for
`snapshots` came out 4.8% low against the exact count (252,976 versus 265,601). The exact count was
used in the table above. The 0.5% accuracy claim in section 1.1 holds for the large tables it was
validated on; this small, recently-written table was the one place it did worse.

**Where the retention rules land today.** The rules are real and in the code
(`market_data\derivatives\config.py`: `RAW_SNAPSHOT_DAYS = 90`, `METRICS_DAYS = 365`), and the
specification says the same. Projecting the measured daily figures forward:

| Table | Retention | **Steady-state size** *(projected)* |
| --- | --- | --- |
| `snapshots` | 90 days | 6.5 GB |
| `metrics` | **365 days** | **40.2 GB** |
| `candles_15m` | **never pruned** | ~8.9 GB per year |
| **Total at one year** | | **≈ 55 GB** |

**This corrects the brief.** The retention was described as "90 days, about 20 GB". The 90-day rule
only covers `snapshots`. `metrics` is kept for a *year* and is the largest single item at 40 GB,
and the 15-minute candle table is never pruned at all. Left as designed, this database reaches
about 55 GB in a year, not 20 GB.

**And the prune is not scheduled.** The code to do it exists
(`market_data\derivatives\store.py`, `prune()`, driven by `cli.py`) but no scheduler, task or
service calls it. Today it only runs if somebody runs it by hand.

### 5.2 What the Derivative tab actually reads

Every column was traced through the code to the places that read it.

**On `snapshots`, the tab reads exactly five columns:** `instrument_token`, `captured_at`,
`mark_kind`, `last_price`, `oi`.

Seven more are read, but only to compute something that gets stored elsewhere:

| Column | Read by | To produce |
| --- | --- | --- |
| `average_price` | metrics | premium traded in ₹ |
| `volume` | metrics | volume, premium, volume/OI ratios |
| `day_open`, `day_high`, `day_low` | the daily roll-up | `daily_rollups` open/high/low |
| `prev_close` | the backfill screen | a price fallback |
| `source` | the backfill screen | tells a real capture from a candle-seeded one |

**Two premises in the brief turned out to be wrong, and correcting them changes the answer.**

1. **There is no 5-level order book in the database.** Only the best bid and best ask are stored —
   four columns. The capture code takes level zero of the depth and discards the rest. The "5
   levels" phrase in the specification describes what the *vendor sends*, not what is kept.
2. **There are no per-bar open/high/low/close columns on `snapshots`.** `day_open`, `day_high`,
   `day_low` and `prev_close` are the session-to-date figures and the previous session's close.
   Per-bar candles live in `candles_15m`.

So the proposal has to be sharper than "drop the depth and the OHLC". It is column by column, and
every byte below was measured.

### 5.3 The proposal for `snapshots`

The record measures **201.1 bytes**. Here is where those bytes go, and the verdict on each:

| Column | Bytes/row *(measured)* | Verdict |
| --- | --- | --- |
| `captured_at` | 20.00 | **Keep** — part of the key |
| `exchange_time` | 20.00 | **Drop** — written, read by nothing |
| `fetched_at` | 20.00 | **Move** to the capture record, one row per mark instead of 27,238 |
| `snapshot_id` | 18.00 | **Move**, same reason |
| `source` | 11.01 | Keep — the backfill screen needs it |
| `mark_kind` | 10.00 | Keep — served |
| `last_price` | 9.00 | Keep — served |
| `day_open` / `day_high` / `day_low` | 9.00 each | Keep — the daily roll-up needs them |
| `prev_close` | 9.00 | Keep — backfill fallback |
| `average_price` | 8.99 | Keep — premium in ₹ |
| `last_trade_time` | 8.52 | **Drop** — read by nothing |
| `bid` | 5.93 | **Drop** — loaded into memory, never used in any calculation |
| `ask` | 5.91 | **Drop** — same |
| `instrument_token` | 5.00 | Keep — part of the key |
| `vendor_id` | 5.00 | Keep — provenance |
| `buy_quantity` | 2.50 | **Drop** — loaded, never used |
| `sell_quantity` | 2.49 | **Drop** — same |
| `bid_quantity` | 2.11 | **Drop** — read by one test and nothing else |
| `ask_quantity` | 2.10 | **Drop** — read by nothing |
| `oi` | 1.98 | Keep — served, and it is the point of the tab |
| `oi_day_high` | 1.98 | **Drop** — read by nothing |
| `oi_day_low` | 1.98 | **Drop** — read by nothing |
| `volume` | 1.60 | Keep — feeds every ratio |
| `average_price_est` | 1.01 | **Drop** — written by the seeder, read by nothing |

Plus one index: **`ix_snapshots_snap` goes** — it indexes `snapshot_id`, which is being moved out.
It measures 13.4 MB of the table's 78.7 MB.

| | Bytes per row | Cut |
| --- | --- | --- |
| Record today | 201.1 | |
| Columns with no reader anywhere | −54.53 | **27%** |
| Provenance moved to the capture record | −38.00 | **19%** |
| **Record after** | **108.6** | **46% smaller** |

**Result for `snapshots`, on disk:** 310.7 → **154.1 bytes per row**, a 50% cut.
**73.9 MB per day becomes 36.7 MB per day.**

*(The per-row record sizes are measured. The on-disk figure is a projection: the measured record
shrinkage applied to the measured on-disk row size, with the dropped index removed.)*

### 5.4 The proposal for `metrics`

`metrics` is the bigger problem at 112.8 MB a day. The record measures **290.5 bytes**. Three
groups of columns are never served to the tab:

| Group | Columns | Bytes/row *(measured)* |
| --- | --- | --- |
| Copies of `contracts` — the tab already joins that table | `tradingsymbol`, `underlying`, `instrument_type`, `strike`, `expiry`, `lot_size`, and the key columns' duplication | **82.05** |
| Pre-rendered status text the app re-computes for itself | `vol_oi_status`, `vol_tod_status`, `floors_failed`, `premium_status`, `headline`, `unusual_reasons`, and 16 more | **56.08** |
| Rupee price moves the app explicitly excludes, plus unreachable aggregate rows | `price_change_15m`, `price_change_day`, the nine put/call-ratio and max-pain columns, provenance | **16.75** |
| **Total never served** | | **154.88 (53%)** |

The put/call-ratio and max-pain columns are worth a note: they are only ever filled on
underlying-level rows, and the tab's query joins through `contracts` on a contract id that those
rows do not have. They are structurally unreachable. The tab gets those figures from
`underlying_snapshots` instead.

Dropping the never-served columns also retires two of the four indexes.

**Result for `metrics`:** 466.8 → **232.6 bytes per row**, a 50% cut.
**112.8 MB per day becomes 56.2 MB per day.**

### 5.5 Retention — "overwrite periodically"

| Table | Today | **Proposed** | Why |
| --- | --- | --- | --- |
| `snapshots` | 90 days | **30 days** | It is the raw input. Once the day is rolled up, the tab never reads it again. 30 days is a month of re-computation head-room. |
| `metrics` | 365 days | **90 days** | This is what the tab actually serves. A year of per-contract, per-15-minutes metrics is 40 GB nobody looks at. |
| `candles_15m` | never pruned | **180 days** | Currently unbounded, which is the real long-term risk. |
| `daily_rollups` | forever | **forever** | Tiny, and it is the permanent record. |
| `contracts` | forever | **forever** | 35,922 rows, 9 MB. Needed to read expired contracts back. |
| `underlying_snapshots` | 90 days | **90 days** | 2,013 rows. Not worth touching. |

**And schedule the prune.** The code exists and is not being called. Whatever the retention numbers
end up being, they do nothing until something runs them daily.

### 5.6 Derivatives: before and after

| | Per trading day | At retention |
| --- | --- | --- |
| **Today** | **223.3 MB** | ≈ 55 GB after a year (6.5 + 40.2 + 8.9) |
| Slim the columns only | 129.5 MB | ≈ 32 GB after a year |
| **Slim the columns and shorten retention** | **129.5 MB** | **≈ 12 GB, and stable** |

Breakdown of the 129.5 MB: `snapshots` 36.7, `metrics` 56.2, `candles_15m` 36.6.
At the proposed retention: 30 × 36.7 = 1.1 GB, 90 × 56.2 = 5.1 GB, 180 × 36.6 = 6.6 GB — **12.8 GB,
and it stops growing.**

Two things that should not be traded away for space:

- **Expired contracts cannot be re-fetched.** Anything dropped from `daily_rollups` is gone for
  good. It is small. Keep it forever.
- **`captures`** records what was asked for and what came back on every mark. 26 rows a day. It is
  the only way to tell a quiet market from a failed capture. Keep it forever.

---

## 6. What this does not fix

**Shrinking the files does not make the product serve millions of users.** It removes 154 GB of
dead weight and makes backups and restores possible. That is all it does.

**SQLite lets one writer in at a time.** This is not a bug or a setting; it is how the format
works. Many readers are fine, but the second writer waits for the first. The codebase already knows
this and works around it: `market_data\store.py` states the constraint plainly and records an
actual failure —

> *"a maintenance pass (a full-history re-fetch, a 29-million-row snapshot) holds the write lock
> for minutes … which is exactly how the live ingest loop died mid-cycle on 2026-09-16 against
> `start_run`."*

There is also a hand-rolled lock file on disk today, `db\market15.db.writer.lock`, holding
`pid=15180 at=2026-09-18T13:27:11`, to stop two writers colliding.

**One correction, because guessing is not allowed here.** The brief said we hit this today — the
metrics job unable to write while the capture job held the file. Every log on this machine was
searched for a lock error: **there are none.** What actually happened at 09:31 on 18 Sep was a
`disk I/O error` while writing snapshots, immediately preceded by an out-of-memory error in JSON
encoding. The mark wrote zero rows, retried, and succeeded 33 seconds later. That is a different
failure. The single-writer limit is real and documented, and it did break the ingest loop on
16 September — but today's incident was not it.

**What the next stage actually is.** One writer at a time is survivable for one owner and a handful
of pilot users. It is not survivable for thousands of people placing orders. Two things are needed,
and they are separate:

| Need | What it means | Rough cost |
| --- | --- | --- |
| Many people writing at once — orders, positions, portfolios | A server-based database (PostgreSQL) that handles concurrent writers properly | A few hundred dollars a month for a managed instance, plus the work to move `pilot.sqlite3` across. It is 1.25 MB, so the data migration itself is trivial. |
| Many people reading prices at once | Prices are read-only and identical for everyone. They do not need a database at all — a cache in memory in front of the rolling window serves thousands of readers from one copy. | Mostly engineering time, not hosting cost. |

**This is the following stage, not this one, and it should not start until the files are small.**
Moving 166 GB into a managed database would be slow and expensive. Moving 3 GB is an afternoon.
Doing the cleanup first makes the migration cheaper, which is the main argument for doing the
cleanup at all.

---

## 7. Risks, and what could go wrong

Every file in the repository was searched for queries that assume these tables share one file. The
search patterns are recorded at the end of this section so the result can be checked.

### R1 — One live query joins the labels to the price history. **This is the one that matters.**

`market_scanner\data.py`, line 145, function `universe()`. It reads the stock list from
`instrument_labels` and, in the same statement, asks `ohlc_daily` and `ohlc_5min` for the latest
date per stock. Split the files and this statement fails.

**Severity: low, and it is nearly free to fix.** When the scanner runs on `market15` — which it is
running right now — `market_scanner\engine.py` throws both of those columns away and uses the
market15 coverage instead. The two subqueries are already dead weight on the live path. Removing
them is a cleanup, not a change in behaviour.

**Must be done before step 3. Verified by:** the scanner's 432 tests, plus a full scan reporting
universe 1,431 and 107 patterns.

### R2 — Four more places join the two groups, none of them live

| File | What it does |
| --- | --- |
| `market_scanner\inspect_data.py:18` | a diagnostic tool |
| `pathfinder_fresh\market.py:43` | research |
| `pathfinder_demo.py:18` | a demo |
| `pathfinder_theme.py:13` | a demo |

Each needs the same one-line fix. None affects the product.

### R3 — Four scripts write to both groups in one transaction

`scripts\nse_corp_actions.py`, `scripts\enrich.py`, `scripts\fetch_ipo_prices.py`,
`scripts\fetch_universe.py`. Each reads price history and writes labels or corporate actions
through one connection, with one commit covering both. Split the files and "both or neither" is no
longer guaranteed.

**Severity: low.** They are manual maintenance scripts, not services, and `corp_actions` and
`event_markers` are being kept in the same small file as `instrument_labels`, so three of the four
keep working unchanged.

### R4 — Nothing else is coupled. Stated explicitly so it can be checked.

- **Zero** queries join the price tables to `users`, `orders`, `positions`, `signals`,
  `virtual_*`, `autotrade_sessions` or `broker_accounts`. Those tables are empty and no code
  outside `db\schema.sql` mentions them.
- **Zero** views and **zero** triggers exist in any of the four databases. This was confirmed by
  reading the live files, not just the schema file.
- **Zero** foreign keys cross between the two groups. All 16 foreign keys in `db\schema.sql` point
  at other product tables.
- The app (`kanida-app`) never reads any `ohlc_*` table at all. Its only use of `kanida.db` is
  reading `instrument_labels`.

### R5 — The file path is written down in about 110 places

| How the path is found | Count |
| --- | --- |
| Hard-coded full path in a file | ~45 |
| Built from the project root | ~65 |
| Environment variable `KANIDA_OHLC_DB` | 1 |
| Scanner config file `market_scanner\config.json` | 1 |
| Pilot setting `pattern_labels_database` | 1 |

**Severity: medium — it is tedious, not dangerous.** Only three of those are on a live path: the
scanner config, the pilot setting, and the shared read-only helper. The other ~110 are research
scripts that will fail loudly with "no such table" rather than quietly return wrong answers.

**Mitigation:** keep the filename `kanida.db`. Everything keeps opening the same file; it just no
longer has price history in it. Scripts that need prices fail loudly, which is the desired
behaviour.

### R6 — The frozen research snapshot is pointers, not data

Covered in sections 2.2 and 4. It is repeated here because it is the one thing that could destroy
irreplaceable work: **trimming `candles_15m` before step 2 has passed its checks would break the
27,867,377-row frozen snapshot, and a re-fetch would not restore it** — the vendor returns today's
restated history, not the history as it stood on 16 September.

### R7 — The rolling window is measured against today's detectors, not tomorrow's

The 260-bar figure comes from a measurement of the 107 detectors as they exist now, with detector
spec hash `e88131bc87d3d5a9c0945a6669c78e8ddec50d122733655e92d62a5eb92414b5`. A new detector with a
longer lookback would need a bigger window.

**Mitigation, and it is already built.** The scanner refuses to run the research pattern set if the
warm-up measurement is missing, or was measured against a different detector spec hash, or under
different live rules. It cannot silently run on too little history. If the window ever needs to
double, that is 2 GB more disk.

### R8 — The gap between 1,431 and 513

The app lists 1,431 stocks. The scanner has prices for 513. Measured from the running scanner right
now: **918 stocks scanned with no data.** This is a product gap that exists today and has nothing
to do with this plan — but it must not be confused with one. Widening the live store to all 1,431
costs a measured **3.1 hours** of fetching and brings the rolling window from 0.71 GB to 1.99 GB.

### R9 — The measurements are a snapshot of one moment

Every figure here was taken on 18–19 September 2026. `derivatives.db` in particular has only two
capture days behind it, so its daily figures rest on one full day (18 September). They should be
re-checked after a fortnight before the retention numbers are fixed.

### Search patterns used, so R4 can be audited

812 files were scanned (`.py .ts .tsx .js .sql .mjs .cjs`), excluding `node_modules`, virtual
environments and caches. Three passes: every SQL string literal split on `;` and required to
mention both a price table and a product table in one fragment; a proximity pass for SQL assembled
across nearby lines; and a shared-connection pass matching the variable each query was executed
against. Targeted searches were also run for `ATTACH DATABASE`, `CREATE VIEW`, `CREATE TRIGGER`,
`FOREIGN KEY`, and `IN (SELECT … FROM ohlc_`.

**The honest limit:** SQL built from variables more than 25 lines apart would have been missed. No
code in this repository is written that way, but the claim is "none found by these methods", not
"none exists".

---

## 8. Open questions — things not confirmed

These are listed rather than guessed at.

| # | Question | Why it is open |
| --- | --- | --- |
| 1 | Is the pinned research run `4b33a5249562631524d6` fully self-contained, and how large is it? | Not measured. The work to inventory `market_scanner\output\` did not finish. It must be answered before step 2 is signed off, because if the run re-reads raw candles to serve evidence, step 2 has more to copy than the frozen snapshot. |
| 2 | Full size of `market_scanner\output\`, and which parts are regenerable | Not measured. |
| 3 | Do `pattern_history.sqlite3` (174.9 MB) and `research_index.sqlite3` (154.3 MB) reach back into the raw candles? | The app is known not to read any `ohlc_*` table, so the 147 GB file is safe. Whether they depend on the full `candles_15m` history was not confirmed. Must be checked before step 6. |
| 4 | Is there a written retention or reproducibility rule in `docs\pattern_research\` that this plan would violate? | Not read. The evidence-serving contract is referenced from the warm-up module but was not opened. |
| 5 | Earliest/latest bar in `ohlc_1min` and `ohlc_5min` as a single figure | The direct query would have read all 765 million rows and was stopped. The per-stock method gave the range in the table in §1.2, which is exact; only the single-query form was abandoned. |
| 6 | Whether `snapshot_members` for the two older snapshots can simply be deleted | `snap_20260916T040035_c10c7ebc` is labelled a smoke test (1 stock) and `snap_20260916T044534_ecc59915` is superseded by the 05:52 one. Together they are 29.1 million of the 57.0 million rows — roughly 3.8 GB. Probably deletable, but not confirmed, and it is not on the critical path. |

---

## 9. What this comes to

| | Measured today | After |
| --- | --- | --- |
| `kanida.db` | **147.16 GB** | 0.002 GB |
| `market15.db` | **18.17 GB** | 2.05 GB |
| `derivatives.db` | 0.62 GB | 12.8 GB steady *(instead of ~55 GB)* |
| `KANIDA_SNR.db` | 0.49 GB | 0.49 GB |
| Research evidence, new file | — | 9.36 GB *(projected; archivable)* |
| **Live footprint** | **166.4 GB** | **3.2 GB** *(with the research evidence archived off-machine)* |

**The rolling window — the number the decision rests on — is 140 trading sessions of 15-minute
candles and 1,310 daily bars per stock: 3,500 + 1,310 rows, which for all 1,431 stocks is 2.0 GB.**
It comes from a measurement that already exists on disk and that the scanner refuses to run
without: 260 candles on each of the four timeframes, set by the chart-pattern family, verified
across 6,005 comparisons and 53,284 detector events with zero failures.

**And the thing worth repeating:** the 147 GB file is not on the live path, has not been written to
since 2 August, and exists to serve a 0.2 MB list of stock names.

---

*Measurements taken 18–19 September 2026. Every database was opened read-only. Nothing was deleted,
moved, compacted or modified. No code was changed. No server was started or stopped.*
