# Data repair report

> **Superseded in two places by the follow-up pass of 2026-09-16 (W7).** Read this note before section 5.
>
> 1. **Caveat 1 (mixed adjustment basis) is resolved.** Eight symbols -- BATAINDIA, CASTROLIND,
>    CESC, CHENNPETRO, GICRE, HINDPETRO, INDUSTOWER, ZEEL -- had their **full** 15-minute history
>    re-fetched (`python -m market_data.repair.unify_basis`, run `basis-unify-20260916`, 176 requests,
>    557,173 bars as new revisions, 205 `corrections` rows with reason `basis_unification`).
>    Zero bars now resolve to `legacy_unknown` for any of them. The other two symbols the
>    `adjustment_basis` class named, GUJENERGY and JMA, were already fully on the vendor's basis from
>    their own flagged full-history fetch, and 463 of the 513 symbols carry both *labels* with
>    identical *prices* -- a label difference is not a basis difference. The affected set is derived
>    from the store by `market_data.basis`, not from this list.
> 2. **The snapshot in section 5 is not the one to read.** The rerun must use
>    `snap_20260916T055228_f4bab0e9` (495 symbols, 27,867,377 rows, 2015-02-02 09:15 .. 2026-09-15
>    15:15, checksum `580af23f585159081c62760eaf1265a2a7afd498c34652df59e6c3b894dd7b19`), which is
>    NIFTY 500 minus the six quarantined symbols and has the `vendor_zero_print` / `vendor_bad_print`
>    / `wrong_instrument` / `unresolved` rows excluded by label (45,642 rows, itemised in
>    `snapshot_exclusions`). Caveat 4 is handled there; caveats 2 and 3 still stand.
>
> Also new: **LTIM, GSPL, GUJGASLTD, JBCHEPHARM, HEG, HFCL** (and AKI, outside the index) are now a
> persisted `quarantine` list, re-probed once a day, skipped by the live ingest loop. That is why
> `/api/state.data_status.last_run.errors` is 0 rather than 6.

Generated 2026-09-15T22:14:11.318542-07:00. Workflow: `market_data/repair/` (diagnose -> plan -> refresh -> reconcile -> report), contract `docs/DATA_PIPELINE_CONTRACT.md` section 3.

`db/kanida.db` was opened read-only for every step. Every write went to `db/market15.db` through `market_data/store.py`. No row was edited in place, no suspicious low was replaced with a daily low, and nothing unresolved was guessed at.

## 1. What was flagged

Read-only sweep of 501 NIFTY 500 symbols in `C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db`, 629.3s. The contract's section-2 checks are W2's, not a second set: 498 symbols by `market_data.validate.validate_symbol`, 3 symbols by `n/a`.

| check | candidate rows | of which explained by adjustment basis |
|---|---|---|
| cross_tf_close_diff | 31892 |  |
| non_positive | 7812 |  |
| cross_tf_close_divergence | 5449 |  |
| session_bar_count | 5098 |  |
| zero_volume_price_move | 5009 |  |
| intraday_below_daily_low | 3998 | 2499 |
| orphan_intraday_session | 3445 |  |
| intrabucket_discontinuity | 3259 |  |
| cross_tf_volume_gap | 3021 |  |
| cross_tf_range_shortfall | 1908 |  |
| missing_intraday_session | 1319 |  |
| outside_regular_session | 1156 |  |
| intraday_above_daily_high | 629 |  |
| cross_tf_range_break | 477 |  |
| session_truncated | 394 |  |
| daily_reconcile_missing | 372 |  |
| bar_outside_session | 195 |  |
| session_excess_bars | 110 |  |
| ohlc_order | 61 |  |
| adjustment_basis_segment | 5 |  |

- symbols with at least one finding: **498**
- symbols with no intraday rows at all: **3** (GSPL, GUJGASLTD, LTIM)
- symbols where `ohlc_daily` and `ohlc_5min` sit on **different adjustment bases**: **5**
- special (Muhurat) sessions identified and excluded from the off-session check: 11

Flagged by the withheld source-quality screen: **37** symbols (`docs/pattern_research/SOURCE_QUALITY_SCREEN.md`).

## 2. What was fetched

Provider `kite`, global rate limit 3 req/s, per-request caps `{'minute': 60, '3minute': 100, '5minute': 100, '10minute': 100, '15minute': 200, '30minute': 200, '60minute': 400, 'day': 2000}`.

| purpose | requests | symbols | calendar days |
|---|---|---|---|
| listing_probe | 476 | 476 | 95200 |
| visibility_probe | 4 | 4 | 20 |
| gap_fill | 476 | 476 | 22848 |
| flagged_full_history | 814 | 37 | 158212 |
| disputed_window | 2816 | 459 | 149117 |
| daily_crosscheck | 1539 | 513 | 2568078 |
| audit_sample | 100 | 100 | 13900 |

Planned total: **6225** requests, estimated **34.6 min** at the rate-limit floor.

Measured execution:

| measure | value |
|---|---|
| requests planned | 6225 |
| requests sent (all passes) | 6168 |
| requests that never succeeded | 57 (AKI, GSPL, GUJGASLTD, HEG, HFCL, JBCHEPHARM, LTIM) |
| empty responses | 214 |
| bars fetched | 8422740 |
| bars written to candles_15m | 7109720 |
| symbols written | 510 |
| passes (resumed) | 2 |
| elapsed | 2195.1s |
| mean seconds per request | 0.3526 |

Run id `repair-20260915`. Every payload is gzipped on disk and recorded in `raw_archive` with its sha256 before any candle derived from it is stored.
Adjustment basis recorded on the new rows: `kite-eod-adjusted`.

## 3. What was corrected, and why

| class | bars | share |
|---|---|---|
| agree | 6067006 | 96.81% |
| source_error | 1196 | 0.02% |
| adjustment_basis | 174518 | 2.78% |
| genuine_extreme | 0 | 0.00% |
| session_regime_cas | 0 | 0.00% |
| vendor_zero_print | 4299 | 0.07% |
| vendor_bad_print | 4 | 0.00% |
| wrong_instrument | 17804 | 0.28% |
| unresolved | 2380 | 0.04% |

6267207 bars compared across 510 symbols; **2124** rows written to `corrections`, each carrying the `raw_archive` request id that justifies it. `adjustment_basis` is recorded once per contiguous segment (ratio, span, bar count, matched ex-date) rather than four rows per bar, so the handful of real price corrections stay findable.

`session_regime_cas` reads 0 in the table above and that is correct: it counts bars we *hold* that the closing-auction regime explains, and our stored history stops at 2026-07-29, before CAS began. The regime's real exposure is in the refreshed window, below.

### Session regime -- NSE Closing Auction Session (from 2026-08-03, contract section 2A)

From that date continuous trading in F&O cash stocks ends at 15:00:00 and the official close is set by an auction at 15:30-15:35, so those sessions hold 24 15-minute bars instead of 25 ending 15:15:00. This is the expected shape, not a defect: no bar is synthesised, the buckets are labelled `SESSION_REGIME_CAS`, and 1D/close values come from the provider's daily series.

| measure | value |
|---|---|
| freshly fetched sessions examined | 270217 |
| symbols on the CAS regime | 208 |
| symbol-sessions on the CAS regime | 6436 |
| distinct trading dates affected | 31 |
| of those, instrument_labels.is_fno = 1 | 6436 |
| of those, is_fno != 1 (regime vs label disagreement) | 0 |
| short sessions CAS does NOT explain | 446 |

Example: **360ONE 2026-08-03** -- 24 bars, last bar start 15:00:00, is_fno=1, 0.0271 of the daily volume traded outside the continuous session.

### Worked examples -- `source_error`

- **ITC 2015-04-24 09:15:00** -- `source_error`
  - held  : O=125.6 H=189.05 L=125.2 C=189.05 V=1966435
  - fresh : O=189.05 H=189.05 L=189.05 C=189.05 V=0
  - fresh daily: O=188.05 H=192.4 L=187.55 C=191.35
  - held/fresh level ratio: 1.000000
  - why: held low 125.2000 < 0.8 x freshly fetched daily low 187.5500; the vendor's 15-minute bar for the same slot stays inside its own daily range
  - evidence: `raw_archive.request_id = c6dc3fdd530541123b8527c0a6bbd60d`

- **PIIND 2015-03-16 09:30:00** -- `source_error`
  - held  : O=232.35 H=654.95 L=232.35 C=652.35 V=115
  - fresh : O=654.95 H=654.95 L=652.05 C=652.35 V=115
  - fresh daily: O=660.5 H=660.5 L=630.4 C=647.1
  - held/fresh level ratio: 1.000000
  - why: held low 232.3500 < 0.8 x freshly fetched daily low 630.4000; the vendor's 15-minute bar for the same slot stays inside its own daily range
  - evidence: `raw_archive.request_id = 13aec711e3825bef5e3794a8b23f27b8`

- **PIIND 2015-06-29 11:45:00** -- `source_error`
  - held  : O=636.7 H=637.5 L=165.75 C=637.0 V=2288
  - fresh : O=636.7 H=637.5 L=635.65 C=637.0 V=2013
  - fresh daily: O=646.0 H=648.7 L=631.25 C=639.2
  - held/fresh level ratio: 1.000000
  - why: held low 165.7500 < 0.8 x freshly fetched daily low 631.2500; the vendor's 15-minute bar for the same slot stays inside its own daily range
  - evidence: `raw_archive.request_id = 13aec711e3825bef5e3794a8b23f27b8`

- **PIIND 2015-07-02 12:15:00** -- `source_error`
  - held  : O=664.7 H=665.0 L=173.2 C=665.0 V=4138
  - fresh : O=664.7 H=665.0 L=663.0 C=665.0 V=2413
  - fresh daily: O=660.0 H=670.0 L=658.7 C=662.55
  - held/fresh level ratio: 1.000000
  - why: held low 173.2000 < 0.8 x freshly fetched daily low 658.7000; the vendor's 15-minute bar for the same slot stays inside its own daily range
  - evidence: `raw_archive.request_id = 13aec711e3825bef5e3794a8b23f27b8`

### Worked examples -- `adjustment_basis`

- **BATAINDIA 2015-02-02 09:15:00** -- `adjustment_basis`
  - held  : O=711.5 H=711.5 L=699.3 C=702.68 V=11782
  - fresh : O=687.9 H=687.9 L=676.1 C=679.35 V=11782
  - fresh daily: O=687.9 H=687.9 L=667.15 C=671.05
  - held/fresh level ratio: 1.034331
  - why: held/fresh = 1.034331 across the whole session (spread 0.01%); no matching corporate action on file
  - evidence: `raw_archive.request_id = c6500296d8164f9f5d89f90caefbee7c`

- **CASTROLIND 2015-02-02 09:15:00** -- `adjustment_basis`
  - held  : O=230.87 H=231.75 L=230.48 C=231.6 V=15048
  - fresh : O=223.41 H=224.26 L=223.03 C=224.12 V=15048
  - fresh daily: O=224.07 H=226.63 L=220.57 C=221.27
  - held/fresh level ratio: 1.033387
  - why: held/fresh = 1.033387 across the whole session (spread 0.00%); no matching corporate action on file
  - evidence: `raw_archive.request_id = 9e234c70eb9ec6997e90ea7814c87fda`

- **CESC 2015-02-02 09:15:00** -- `adjustment_basis`
  - held  : O=59.88 H=60.3 L=59.65 C=60.16 V=192500
  - fresh : O=57.74 H=58.15 L=57.52 C=58.01 V=192500
  - fresh daily: O=57.74 H=59.55 L=57.52 C=58.11
  - held/fresh level ratio: 1.037031
  - why: held/fresh = 1.037031 across the whole session (spread 0.02%); no matching corporate action on file
  - evidence: `raw_archive.request_id = 7cd48bbcef8387dab5c6a14d31f67f6b`

- **CHENNPETRO 2015-02-02 09:15:00** -- `adjustment_basis`
  - held  : O=78.2 H=79.5 L=78.0 C=78.7 V=62887
  - fresh : O=74.9 H=76.2 L=74.7 C=75.4 V=62887
  - fresh daily: O=75.0 H=78.6 L=74.3 C=77.9
  - held/fresh level ratio: 1.043664
  - why: held/fresh = 1.043664 across the whole session (spread 0.13%); no matching corporate action on file
  - evidence: `raw_archive.request_id = bdb4bdfc14edbe39d83442a8cb85c653`

### Worked examples -- `vendor_bad_print`

- **BALKRISIND 2015-07-22 09:30:00** -- `vendor_bad_print`
  - held  : O=335.98 H=668.5 L=335.98 C=668.5 V=86344
  - fresh : O=668.5 H=668.5 L=668.5 C=668.5 V=86000
  - fresh daily: O=333.0 H=342.98 L=333.0 C=340.93
  - held/fresh level ratio: 1.000000
  - why: held high 668.5000 > 1.2 x freshly fetched daily high 342.9800; and so does the vendor's own bar (vendor high 668.5000 > 1.2 x freshly fetched daily high 342.9800). The defect is in the vendor's series, not introduced by us, and a re-fetch reproduces it. Quarantined as an unusable price; nothing is substituted.
  - evidence: `raw_archive.request_id = 092903dac029fd08c1f53349634b30c5`

- **GREENLAM 2015-08-04 09:15:00** -- `vendor_bad_print`
  - held  : O=36.7 H=182.5 L=36.0 C=37.0 V=3880
  - fresh : O=182.5 H=182.5 L=182.5 C=182.5 V=0
  - fresh daily: O=36.7 H=40.7 L=36.0 C=40.3
  - held/fresh level ratio: 1.000000
  - why: held high 182.5000 > 1.2 x freshly fetched daily high 40.7000; and so does the vendor's own bar (vendor high 182.5000 > 1.2 x freshly fetched daily high 40.7000). The defect is in the vendor's series, not introduced by us, and a re-fetch reproduces it. Quarantined as an unusable price; nothing is substituted.
  - evidence: `raw_archive.request_id = 3a4efc4572261e6a0b9688acd80c413e`

- **INFY 2015-04-24 09:15:00** -- `vendor_bad_print`
  - held  : O=523.9 H=2090.9 L=520.2 C=2090.9 V=1617880
  - fresh : O=2090.9 H=2090.9 L=2090.9 C=2090.9 V=0
  - fresh daily: O=522.7 H=526.2 L=484.5 C=488.2
  - held/fresh level ratio: 1.000000
  - why: held high 2090.9000 > 1.2 x freshly fetched daily high 526.2000; and so does the vendor's own bar (vendor high 2090.9000 > 1.2 x freshly fetched daily high 526.2000). The defect is in the vendor's series, not introduced by us, and a re-fetch reproduces it. Quarantined as an unusable price; nothing is substituted.
  - evidence: `raw_archive.request_id = d5e0dacc1c52bc3b4fea40396c6453bd`

- **YESBANK 2015-08-12 09:15:00** -- `vendor_bad_print`
  - held  : O=158.8 H=798.65 L=157.41 C=798.65 V=527355
  - fresh : O=798.65 H=798.65 L=798.65 C=798.65 V=0
  - fresh daily: O=158.96 H=159.0 L=153.34 C=154.0
  - held/fresh level ratio: 1.000000
  - why: held high 798.6500 > 1.2 x freshly fetched daily high 159.0000; and so does the vendor's own bar (vendor high 798.6500 > 1.2 x freshly fetched daily high 159.0000). The defect is in the vendor's series, not introduced by us, and a re-fetch reproduces it. Quarantined as an unusable price; nothing is substituted.
  - evidence: `raw_archive.request_id = 700538a5fe21de4306f229e943298092`


## 4. What remains unresolved

### Reused instrument tokens -- `wrong_instrument` (10 symbols)

These symbols carry intraday history from years before the vendor's own **daily** series for them begins, at price levels that belong to a different instrument. The vendor serves the same rows under the same token, so a re-fetch cannot repair it: the token had an earlier life. The rows are quarantined with the evidence, never deleted.

| symbol | our first intraday session | price there | vendor's first daily bar | lead (days) | sessions probed | sessions inferred | bars |
|---|---|---|---|---|---|---|---|
| IRCTC | 2015-02-02 | 38.1 | 2019-10-14 | 1715 | 139 | 139 | 3278 |
| PTCIL | 2015-02-02 | 40.0 | 2023-06-09 | 3049 | 139 | 139 | 3468 |
| INDIAMART | 2015-10-07 | 81.7 | 2019-07-04 | 1366 | 131 | 131 | 3229 |
| AFFLE | 2015-02-02 | 3.3 | 2019-08-08 | 1648 | 108 | 108 | 2698 |
| STARHEALTH | 2015-03-13 | 1000.0 | 2016-01-04 | 297 | 73 | 102 | 1708 |
| FIVESTAR | 2015-02-02 | 3.15 | 2022-11-21 | 2849 | 44 | 44 | 1097 |
| IRFC | 2018-03-23 | 996.0 | 2021-01-29 | 1043 | 37 | 37 | 437 |
| DELHIVERY | 2015-06-11 | 11.8 | 2016-01-18 | 221 | 29 | 75 | 695 |
| GICRE | 2015-02-02 | 793.1 | 2017-10-25 | 996 | 27 | 27 | 656 |
| HDFCLIFE | 2015-02-11 | 10.95 | 2017-11-17 | 1010 | 22 | 22 | 538 |

Totals: 749 sessions / 17804 bars proved inside a window we requested; 824 sessions / 19089 bars quarantined by inference up to the vendor's first daily bar. Median lead 1204.5 days.

- **4 `vendor_bad_print` bars** -- our row breaks out of the vendor's own daily range for that session **and so does the vendor's own 15-minute bar** (INFY 2015-04-24 prints 2090.90 against a daily high of 526.20, then and now). A re-fetch reproduces it, so there is nothing to correct against; quarantined as an unusable price.
- **4299 `vendor_zero_print` bars** -- we and the vendor hold the same O=H=L=C=0 row (INFY 2015-04-27..05-13 is the clearest case). The defect is the vendor's, not ours; there is nothing to correct, and the rows are labelled unusable rather than counted as agreement.
- **LTIM, GSPL, GUJGASLTD, JBCHEPHARM** -- invisible to this Kite account. Probed explicitly rather than assumed; see the `visibility_probe` results below.
- **FORCEMOT 2023-10-26 .. 2024-02-13** -- genuine trading suspension. Labelled, not filled.

| symbol | probe result |
|---|---|
| GSPL | error: ProviderError: symbol 'GSPL' is not in the Kite NSE instrument list (invisible to this account, delisted, or renamed) |
| GUJGASLTD | error: ProviderError: symbol 'GUJGASLTD' is not in the Kite NSE instrument list (invisible to this account, delisted, or renamed) |
| JBCHEPHARM | error: ProviderError: symbol 'JBCHEPHARM' is not in the Kite NSE instrument list (invisible to this account, delisted, or renamed) |
| LTIM | error: ProviderError: symbol 'LTIM' is not in the Kite NSE instrument list (invisible to this account, delisted, or renamed) |

| quarantine label | sessions |
|---|---|
| special_session_not_returned | 12 |
| wrong_instrument | 10 |

First 20 quarantined sessions:

| symbol | session | reason |
|---|---|---|
| AARTIIND | 2022-10-24 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| AFFLE | 2015-02-02 .. 2015-07-08 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| AIAENG | 2018-11-07 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| BANKBARODA | 2015-11-11 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| BOSCHLTD | 2018-11-07 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| DELHIVERY | 2015-06-11 .. 2015-07-21 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| FIVESTAR | 2015-02-02 .. 2015-04-07 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| GICRE | 2015-02-02 .. 2015-07-31 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| HDFCLIFE | 2015-02-11 .. 2015-03-13 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| IGL | 2018-11-07 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| INDIAMART | 2015-10-07 .. 2016-04-22 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| IRCON | 2020-11-14 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| IRCTC | 2015-02-02 .. 2015-08-20 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| IRFC | 2018-03-23 .. 2018-12-14 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| LODHA | 2021-11-04 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| LTTS | 2019-10-27 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| NEULANDLAB | 2023-11-12 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| PTCIL | 2015-02-02 .. 2015-08-20 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |
| REDINGTON | 2019-10-27 | special (Muhurat) session: the vendor returned no bars for a plain calendar-day request; the session is outside the regular window, not missing |
| STARHEALTH | 2015-03-13 .. 2015-07-07 | we hold intraday bars for sessions inside a window we did request that precede the vendor's own first daily bar for this symbol; the vendor serves the same pre-listing rows under the same instrument token, so this is a reused token in the legacy mapping, not something a re-fetch can repair |

**2380** bars are classed `unresolved`: held and fresh disagree, no constant level shift explains it, and no freshly fetched daily bar settles it. They keep their old revision and are listed in `market_data/repair/artifacts/reconcile/verdicts.jsonl`. They are **not** corrected and **not** deleted.

## 5. Frozen snapshot for the research rerun

| field | value |
|---|---|
| snapshot_id | snap_20260916T044534_ecc59915 |
| created_at | 2026-09-16T04:45:34 |
| universe | NIFTY500 |
| adjustment_basis_id | kite-eod-adjusted |
| provider | kite |
| first_bar | 2015-02-02 09:15:00 |
| last_bar | 2026-09-15 15:15:00 |
| symbol_count | 513 |
| row_count | 29018297 |
| checksum | 88bf43f0ce53b89a0c68307c393d6e00c444492b28bd65f50220353d4e32b090 |
| status | frozen |
| frozen_at | 2026-09-16T04:53:01 |
| notes | frozen by market_data/repair after diagnose+refresh+reconcile |
| reused | True |

The research rerun must read `snapshot_id = snap_20260916T044534_ecc59915`. A withheld source-quality release must never become the active evidence release (contract section 6).

### Caveats the rerun must carry

1. **Mixed adjustment basis.** Re-fetched windows are on the vendor's current basis; windows that were not re-fetched keep the legacy basis they were seeded with. 174518 bars were measured as a constant level shift between the two. Every one is recorded in `corrections` at segment level with its ratio and span. A symbol whose history spans both bases must not have returns computed across the boundary without re-basing; the clean fix is a full-history re-fetch of the affected symbols.
2. **Parity checks use 1H and 4H, not 1D/1W.** The frozen research built daily and weekly bars from `ohlc_daily`, while this store aggregates sessions and takes the 1D/1W close from the provider's daily bar (which after 2026-08-03 is a closing-auction price). 1H and 4H are directly comparable; 1D and 1W are not, by design.
3. **`quality_findings` accumulates per run without dedupe** -- filter by `run_id` when counting.
4. **`vendor_zero_print` and `wrong_instrument` rows are still in the store**, labelled, because deleting them would be a silent edit. The rerun must exclude them by label, not assume they are gone.

---

Artifacts: `market_data/repair/artifacts/{diagnose,plan,refresh,reconcile}/`. Raw payloads: `db/raw_archive/`. Provenance tables: `raw_archive`, `corrections`, `snapshots`, `ingest_runs` in `db/market15.db`.
