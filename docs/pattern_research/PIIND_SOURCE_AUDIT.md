# PIIND source-data audit

## Finding

**The examined PIIND low of 65.35 is already present in the source database's 1-minute and 5-minute rows. It was not introduced by the expanded research or by the examined 4-hour aggregation.** It conflicts sharply with the separately stored daily range and surrounding intraday prices. The resulting 50.9148% short-trade return is not trustworthy performance evidence.

Scope: PIIND, principally 4–6 September 2019; expanded run `8ae6ddc251e80668239e`, source research run `0e15f954dc432754`. Database queries used SQLite `mode=ro`, `query_only=ON`, and indexed symbol/time predicates. No source data or code was modified; no research batch was rerun.

## Exact source evidence

All rows below carry `symbol='PIIND'` and `instrument_token=6191105` in `db/kanida.db`.

| Source table | Bar time | Open | High | Low | Close | Volume |
|---|---|---:|---:|---:|---:|---:|
| `ohlc_1min` | 2019-09-05 11:03:00 | 1191.30 | 1191.30 | 1191.30 | 1191.30 | 1 |
| `ohlc_1min` | 2019-09-05 11:04:00 | 66.00 | 66.05 | 65.85 | 65.90 | 3246 |
| `ohlc_1min` | 2019-09-05 11:05:00 | 1191.30 | 1191.30 | 1191.00 | 1191.00 | 29 |
| `ohlc_1min` | 2019-09-05 11:21:00 | 1192.20 | 1192.20 | 1192.20 | 1192.20 | 1 |
| `ohlc_1min` | 2019-09-05 11:22:00 | 65.35 | 65.55 | 65.35 | 65.40 | 639 |
| `ohlc_1min` | 2019-09-05 11:23:00 | 1192.95 | 1192.95 | 1192.20 | 1192.20 | 66 |
| `ohlc_5min` | 2019-09-05 11:00:00 | 1193.90 | 1193.90 | 65.85 | 65.90 | 3280 |
| `ohlc_5min` | 2019-09-05 11:20:00 | 1192.15 | 1192.95 | 65.35 | 1192.15 | 763 |
| `ohlc_daily` | 2019-09-05 00:00:00 | 1190.00 | 1218.00 | 1183.10 | 1199.10 | 229598 |

The previous session is also inconsistent: source `ohlc_1min` at **2019-09-04 10:51** has O=H=L=C=**66.70**, while that day's `ohlc_daily` low is **1146.75**. Additional 1-minute rows at 11:10, 11:14, 11:16, 11:17 and 11:33 trade around 67, interspersed with rows around 1150. On 6 September the sampled daily and frozen intraday minimum both equal **1182.75**.

The three-day queries returned 225 five-minute rows, 1125 one-minute rows and 3 daily rows. Query plans used `idx_ohlc_5min_sym_dt`, `idx_ohlc_1min_sym_dt` and `idx_ohlc_daily_sym_dt`; this was not a universe scan.

## Aggregation and snapshot provenance

Manually aggregating the **48 source five-minute rows** from `2019-09-05 09:15:00` inclusive to `13:15:00` exclusive gives:

`open=1190.0, high=1218.0, low=65.35, close=1213.0, volume=185490`.

Those five values exactly match the frozen 4H candle. The frozen 1H bars also expose the source defects:

- 2019-09-05 10:15–11:15: low **65.85**, open 1194.20, close 1192.15, `gap=false`.
- 2019-09-05 11:15–12:15: low **65.35**, open 1192.15, close 1197.70, `gap=false`.
- 2019-09-04 09:15–13:15 (4H): low **66.70**, open 1161.95, close 1174.70, `gap=false`.

The expanded PIIND frozen history and the older source-run frozen history have the same decompressed SHA-256, also matching the expanded manifest:

`8fc5a88936ef8513bbd1ddf13148d6a2d34ed0b540530babaf06bc0e4cb313a9`.

PIIND history filename: `62f495579989a3f79360631344654cf1eb76dd0fa54fbaac84f43365b494a8d2.json.gz`, beneath both `market_scanner/output/history/0e15f954dc432754/` and `market_scanner/output/expanded_research/8ae6ddc251e80668239e/history/`.

## Why the existing quality checks missed this

`market_scanner/data.py:125` checks finite, positive, internally ordered OHLC values. These anomalous bars still satisfy those checks. Aggregation at lines 180–182 correctly takes the minimum low from the source rows. The discontinuity check at lines 186–187 compares **the aggregated candle's open with the previous aggregated close**, plus missing-bucket timing. It does not examine discontinuities between source rows inside a bucket or reconcile intraday extremes against daily ranges. Consequently, merging an isolated 65-price source row between approximately 1192-price rows can leave `gap=false` at both 1H and 4H resolutions.

## Confirmed effect on research

The saved PIIND `CDLHANGINGMAN / canonical / 4H / short` walk-forward ledger includes:

- Entry: **2019-09-04 13:15**, price **1174.70**.
- Rule: `setup:24:1:1`; signal ATR **602.795**.
- Target/exit price: **571.905**; target marked hit in the **2019-09-05 09:15–13:15** candle.
- Reported net return: **50.91480377968843%**, including the assumed 0.40% round-trip cost.

The low 65.35 makes that target appear touched, although the separately stored daily minimum is 1183.10. The large signal ATR also shows why removing one conspicuous winning trade alone is insufficient: erroneous prices can affect prior detector geometry, volatility measures, rule selection and subsequent trades. The inspected ledger does not establish the full set of affected periods/cells.

## Attribution limits and next action

The ingestion scripts call Kite historical data and store returned numeric OHLCV fields (`scripts/fetch_ohlc.py:95`, 123–126; `scripts/backfill_maxlookback.py:58`, 125–127). However, these tables have no per-row download timestamp, raw vendor-response archive, source-request ID or correction history. All examined rows have the same stored instrument token. Therefore this audit cannot determine whether the original cause was upstream data, an earlier instrument mapping/import mistake, a later modification, or another historical process. It does not independently establish the true exchange prices.

Treat affected intraday research as **data-quality flagged and unsuitable for performance claims** until the anomalous source intervals are reconciled against an authoritative replacement feed. Preserve this run as the reproducible result of its original inputs. Any cleaned research should use a new, explicitly versioned snapshot and should rerun detector/ATR/training paths affected by corrected data. Do not silently replace a suspicious low with the daily low or merely remove profitable anomalous trades.
