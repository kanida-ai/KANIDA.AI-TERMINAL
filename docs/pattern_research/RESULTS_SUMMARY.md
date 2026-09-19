# Expanded pattern research: final findings

**Research implementation, full-universe computation and reproducibility audit are complete. Publication is withheld because the source review found material OHLCV inconsistencies.** This is an analysis of the existing stored history; it is not a corrected-data release or live integration.

Run: `8ae6ddc251e80668239e`. Prepared and executed 15 September 2026.

## 1. What Codex and Claude Code completed

- Claude Code and its workers implemented the expanded detectors and fixtures; Codex implemented the isolated research pipeline and independently reviewed integration, causality, costs, chronology and saved evidence.
- 107 catalogue IDs: 28 chart patterns, 61 TA-Lib candlestick definitions, 8 price-action families and 10 harmonic families. These register as 262 direction/variant combinations.
- All 1,431 stocks and four timeframes (1H, 4H, 1D, 1W): **1,499,688 / 1,499,688 study cells stored; zero computation errors**. Missing data and insufficient evidence remain explicit result statuses.
- 135 detector tests and 22 evaluator/storage tests passed; 27 existing focused engine tests also passed. Independent real-data prefix checks covered the full registry before the code freeze.
- The final audit verified 46 frozen code/configuration files, dependencies, exact specifications, all 1,431 input-history checksums, all 1,431 result archives and every expected study key.

The catalogue index states implemented definitions, directions, variants and setup/confirmation capabilities. Deferred variants include custom gap-free candle adaptations, NR ties, gap hold/fill/retest strategies and distinct Adam/Eve backtest variants. Research coverage of the 107 registered IDs does not mean every optional textbook variant or live state is implemented.

## 2. Main source-data finding

**Do not use the raw highest-return ranking as a strategy recommendation.** PIIND dominates 10 of the top 12 displayed cells. The independently inspected original DB includes one-minute prices near 65 between approximately 1,192-price rows. On 5 September 2019, the 4H low is 65.35 while the separately stored daily low is 1183.10. The source defect creates an apparent +50.9148% short trade and distorts ATR/training. It predates this expansion.

A coarse diagnostic screen of 19,371,366 bars flags **37 stocks / 75 stock-timeframe datasets**. Flags require reconciliation; they are not all independently proven errors. Unflagged histories are not certified clean. We preserved the original data and experiment, and recorded `withheld_source_quality_review` in the publication review. No new results were activated in the application.

## 3. Raw historical results, before source correction

**62,440 cells (4.16% of all cells) have at least 20 selected walk-forward trades.** Of these, 25,499 have positive mean net returns and 36,941 have nonpositive means. These are overlapping study cells, not independent strategies or a combined portfolio. Positive means are exploratory, uncorrected results.

| Result status | Cells |
|---|---:|
| No usable candles | 7,074 |
| Too little history for recognition | 95,076 |
| No occurrences | 587,375 |
| Occurrences, insufficient rolling history | 193,918 |
| No selected walk-forward trades | 500,793 |
| 1–19 walk-forward trades | 53,012 |
| At least 20 trades, nonpositive mean | 36,941 |
| At least 20 trades, positive mean | 25,499 |

| Family | Catalogue IDs | Study cells | Cells with WF trades | Cells with ≥20 WF trades | Raw positive, ≥20 trades |
|---|---:|---:|---:|---:|---:|
| Charts | 28 | 257,580 | 2,006 | 425 | 155 |
| Candlesticks | 61 | 944,460 | 76,284 | 41,650 | 16,825 |
| Price action | 8 | 148,824 | 37,162 | 20,365 | 8,519 |
| Harmonics | 10 | 148,824 | 0 | 0 | 0 |

Every registered pattern ID occurred somewhere in the source universe. Harmonic definitions produced descriptive historical trades, but no selected walk-forward trades under this protocol. They therefore have no selected out-of-sample performance to display. Rare-pattern cells need an honest insufficient-evidence label; pooling stocks would be a new, separately defined experiment.

Protocol: rolling 36-calendar-month training and 6-month test windows; at least 20 eligible training trades; earlier-data rule selection; next eligible open; 0.40% round-trip entry-notional costs; conservative stop priority; explicit gap handling and fold carry-over. Baseline fixed-horizon results are separate from selected walk-forward results. The original ten-pattern batch used a different validation protocol, so its tested counts cannot be compared directly with these counts.

Limitations: source adjustment and historical universe membership are not certified; source gaps/aggregation stubs remain; short borrowing/contract availability is not modeled; many dependent definitions/rules were compared; nominal intervals and the test-ranked shortlist are not multiple-testing-adjusted release evidence.

## 4. Database decision

The original source and research were already separate: about **158.01 GB** in `db/kanida.db` and **0.833 GB** in the old backtest DB. Moving backtests alone will not make the source DB small.

The new shared research index is **3.914 GB** including pilots. This full run adds **17.328 GB** of compressed occurrence/trade/fold archives and **0.499 GB** of frozen input snapshots. Keep these outside the live application database.

Recommended boundaries:

1. Market history: immutable normalized snapshots in file/object storage, plus a recent-candle cache.
2. Research: versioned definitions, indexed summaries, run manifests and detailed archives.
3. Published evidence: compact compatible card records and a coverage index.
4. Application: current detections, user state, active release pointers and a bounded card cache.

The future cycle is vendor API → normalized completed candles → same versioned detector → current forming/confirmed state → compatible precomputed historical card. Research refresh runs separately. Preserve exact historical inputs because future API revisions may not reproduce an old experiment.

## 5. What remains before live use

1. Reconcile flagged source history and corporate-action/adjustment conventions against the new vendor. Add row-level discontinuity and comparable timeframe checks before aggregation.
2. Freeze corrected snapshots and rerun affected detector/ATR/training histories. Do not patch only suspicious winning trades.
3. Validate that the chosen live fetch window and retained state reproduce full-history detection; prefix causality alone does not prove this.
4. Implement the vendor adapter, live setup lifecycle, compact evidence publisher/API, compatibility checks, freshness and atomic activation/rollback.

These integration steps are designed in `EVIDENCE_SERVING_CONTRACT.md`; they have not been deployed.

## Files

- [Implemented catalogue](C:/Users/SPS/Documents/Kanida_Falcon/docs/pattern_research/IMPLEMENTED_CATALOGUE.md)
- [Exact registry JSON](C:/Users/SPS/Documents/Kanida_Falcon/docs/pattern_research/IMPLEMENTED_CATALOGUE.json)
- [Full raw research tables](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/output/expanded_research/8ae6ddc251e80668239e/ANALYSIS.md)
- [Reproducibility verification](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/output/expanded_research/8ae6ddc251e80668239e/final_verification.json)
- [Source-quality screen](C:/Users/SPS/Documents/Kanida_Falcon/market_scanner/output/expanded_research/8ae6ddc251e80668239e/SOURCE_QUALITY_SCREEN.md)
- [PIIND source audit](C:/Users/SPS/Documents/Kanida_Falcon/docs/pattern_research/PIIND_SOURCE_AUDIT.md)
- [Storage and workflow design](C:/Users/SPS/Documents/Kanida_Falcon/docs/pattern_research/STORAGE_AND_LIVE_WORKFLOW.md)
- [Future evidence-serving contract](C:/Users/SPS/Documents/Kanida_Falcon/docs/pattern_research/EVIDENCE_SERVING_CONTRACT.md)
