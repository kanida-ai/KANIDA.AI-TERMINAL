# Kanida.AI Quant Engine Due-Diligence Memo

Date: 2026-05-02  
Scope: engine math, data integrity, backtest pipeline only. Frontend/router UI was not modified.

## 1. Verdict

**Verdict: broken for the Turbo/Super performance claim; needs-work as a research engine.**

The 99%+ Turbo/Super win rate is not an ex-ante trading edge. It is primarily an artifact of **post-outcome bucket assignment**: `turbo` and `super` are assigned only after the trade has already hit take-profit and after realized speed, MAE, MFE, and post-exit continuation are known.

There may be a useful pattern-mining research layer underneath, but the current headline “engine performance” is not pitchable to an institutional LP / IB quant DD desk.

## 2. Headline Metrics Recomputed From Local DB

Local DB: `data/db/kanida_quant.db`

Counts:

- `ohlc_daily`: 223,206 rows
- `trade_log`: 21,515 rows
- `execution_log`: 21,507 rows
- `pattern_library`: 14,435 rows
- Closed backtest trades analyzed: 21,507

Effective P&L used the same logic as the backend display: if `trade_taken=1` and `smart_pnl_pct` exists, use smart P&L; else use blind P&L / `trade_log.pnl_pct`.

| Segment | Trades | TP Win Rate | Effective Win Rate | Avg Effective P&L |
|---|---:|---:|---:|---:|
| All | 21,507 | 30.18% | 37.84% | +0.106% |
| Turbo | 1,894 | 100.00% | 99.52% | +4.220% |
| Super | 2,190 | 100.00% | 99.86% | +5.086% |
| Turbo+Super | 4,084 | 100.00% | 99.71% | +4.684% |
| Standard | 13,616 | 17.67% | 27.47% | -0.571% |

This split is mechanically suspect because Turbo/Super require `exit_reason == "tp"` in code.

## 3. Red Flags

### RF-1: Turbo/Super buckets are post-outcome labels, not entry-time engines

Severity: **Critical**  
Fix difficulty: **Medium**

Evidence:

- `engine/backtest/run_backtest.py:59-67` classifies `turbo` and `super` only inside `if exit_reason == "tp"`.
- `engine/backtest/run_backtest.py:63-66` uses `days_to_exit`, continuation (`mpi`), and `mae`.
- `engine/backtest/run_backtest.py:107-118` computes `mpi` and `post5d` from bars after exit.
- `engine/backtest/run_backtest.py:286-296` computes post-exit stats and then assigns the bucket.

Institutional interpretation:

Turbo/Super are not predictive strategy buckets. They are realized winners sorted after the fact. The 99% WR is expected by construction.

### RF-2: Smart entry leaks same-day information

Severity: **Critical**  
Fix difficulty: **High**

Evidence:

- `engine/backtest/run_execution_analysis.py:184-193` loads entry-day `open/high/low/close`.
- `engine/backtest/run_execution_analysis.py:200-210` passes full entry-day OHLC into `execution_engine.analyze`.
- `engine/backtest/execution_engine.py:205-207` computes day move and range using entry-day close/high/low.
- `engine/backtest/execution_engine.py:256-257` estimates entry using entry-day close/low/high.
- `engine/backtest/execution_engine.py:284-362` makes long-entry decisions based on `day_move_pct`, recovery, and close-vs-open information that is not known at 9:15/9:30/10:00.
- `engine/backtest/run_execution_analysis.py:225-227` then computes smart P&L off the hindsight-derived entry.

Institutional interpretation:

The smart-entry layer is not a valid intraday execution model with daily candles. It uses full-day data to decide an intraday action. This invalidates `smart_pnl_pct` as a live-realistic estimate.

### RF-3: Pattern library is trained on the same period that is backtested

Severity: **Critical**  
Fix difficulty: **Medium**

Evidence:

- `engine/jobs/run_learning.py` loads all OHLCV rows and writes `pattern_library`.
- `engine/outcome_first/learner.py:35` computes forward outcomes from the same row set.
- `engine/outcome_first/learner.py:122-148` mines patterns and scores probabilities on all available dates.
- `engine/backtest/run_backtest.py:222` loads `SELECT * FROM pattern_library`.
- `engine/backtest/run_backtest.py:150-179` applies those patterns back across `BACKTEST_YEARS = {"2024", "2025", "2026"}`.

Institutional interpretation:

The backtest is in-sample. Patterns discovered using 2026 outcomes are eligible to generate 2026 “historical” trades. This is classic look-ahead / data-snooping unless a dated pattern version table is used.

### RF-4: Candidate dedup selects highest opportunity score after full-sample training

Severity: **High**  
Fix difficulty: **Medium**

Evidence:

- `engine/backtest/run_backtest.py:186-201` keeps the highest `opportunity_score` candidate per ticker/date/direction.
- The opportunity score itself comes from the full-sample learned `pattern_library`.

Institutional interpretation:

Even if behavior atoms are entry-time available, the selected pattern ranking is contaminated by later sample outcomes unless the ranking was known at that date.

### RF-5: TP/SL ordering is biased toward take-profit on same daily bar

Severity: **Medium**  
Fix difficulty: **Low**

Evidence:

- `engine/backtest/run_backtest.py:90-93` checks TP before SL for long trades.
- `engine/backtest/run_backtest.py:97-100` checks TP before SL for short trades.

Observed impact:

- 80 trades had both TP and SL reachable before/at exit using daily high/low.
- 79 of those were recorded as TP.
- This is not the main source of 99% WR, but it is still optimistic.

### RF-6: Data source and corporate-action quality are not institutional grade

Severity: **High**  
Fix difficulty: **Medium**

Evidence:

- `ohlc_daily` source mix:
  - `yfinance`: 217,894 rows, 146 tickers
  - `kite`: 5,312 rows, 8 tickers
- Backtest trade entry-source mix:
  - `yfinance`: 20,502 trades
  - `kite`: 1,005 trades
- Local engine DB has no `universe` table; the seeded production universe is not represented in this local engine DB.
- `data/ingest/fetch_fno_kite.py:135-149` rejects single-day moves over 25%, but does not adjust split/dividend events.
- `data/ingest/fetch_fno_kite.py:186-194` writes raw OHLCV directly.

Observed extreme moves:

- ABFRL: -66.59% close-to-close on 2025-05-22.
- OFSS: +30.11% on 2024-01-18.
- IEX: -29.58% on 2025-07-24.
- Multiple 25%+ daily moves exist in the DB.

The memory note flagged VEDL -64.76% on 2026-04-30. The local DB only had VEDL rows through 2026-04-27 in this audit slice and those rows were `source='yfinance'`, so the specific 2026-04-30 VEDL row could not be verified locally.

## 4. Statistical Evidence

### In-sample effective performance

The whole closed backtest set is weak after including all trades:

- All trades: +0.106% average effective P&L
- Standard: -0.571% average effective P&L
- Turbo+Super: +4.684% average effective P&L, but post-outcome classified

Year split:

| Year | Bucket | Trades | Effective WR | Avg Effective P&L |
|---|---|---:|---:|---:|
| 2024 | Turbo | 896 | 99.7% | +4.237% |
| 2024 | Super | 1,032 | 99.9% | +5.166% |
| 2024 | Standard | 5,758 | 25.8% | -0.642% |
| 2025 | Turbo | 685 | 99.4% | +4.121% |
| 2025 | Super | 818 | 99.9% | +4.768% |
| 2025 | Standard | 6,038 | 29.2% | -0.460% |
| 2026 | Turbo | 313 | 99.4% | +4.388% |
| 2026 | Super | 340 | 99.7% | +5.609% |
| 2026 | Standard | 1,820 | 26.9% | -0.713% |

This looks stable because the bucket definition itself encodes the outcome.

### Out-of-sample replication

Strict train-on-2024 / test-on-2026 under current config:

- 2024 had 0 stocks with >=260 bars (`max=249`, median 246).
- Current config requires `min_history_bars = 260`.
- Result: **0 patterns, 0 test trades**.

Relaxed train-on-2024 (`min_history_bars=180`, `min_occurrences=8`) / test-on-2026:

- Patterns: 13,854
- Test trades: 3,137
- All trades: 29.33% TP WR, -0.010% avg P&L
- Turbo+Super: 578 trades, 100.00% TP WR, +5.209% avg P&L
- Standard: 2,048 trades, 16.70% TP WR, -0.845% avg P&L

Train-on-2024+2025 / test-on-2026:

- Patterns: 14,148
- Test trades: 3,123
- All trades: 31.44% TP WR, +0.092% avg P&L
- Turbo+Super: 649 trades, 100.00% TP WR, +4.884% avg P&L
- Standard: 1,962 trades, 16.97% TP WR, -0.815% avg P&L

Interpretation:

The overall predictive model is roughly flat OOS before costs. Turbo/Super remain perfect because they are still assigned after the test outcome. That is not ex-ante evidence.

## 5. Risk Metrics

These are computed from realized effective per-trade returns as a diagnostic, not a valid portfolio simulation. The Turbo/Super risk metrics are invalid as investment evidence because bucket membership is post-outcome.

| Segment | Trades | Mean / Trade | Sharpe | Sortino | Max DD | Longest Losing Streak |
|---|---:|---:|---:|---:|---:|---:|
| All | 21,507 | +0.106% | 0.41 | 0.70 | -99.99% | 18 |
| Turbo+Super | 4,084 | +4.684% | 31.80 | 523.52 | -8.10% | 1 |
| Standard | 13,616 | -0.571% | -2.33 | -3.58 | -100.00% | 30 |

The “best-ever” Sharpe for Turbo/Super is itself a red flag because it is generated by selecting realized winners.

## 6. Benchmark Comparison

Backtest period from closed trades:

- First entry: 2024-01-02
- Last exit: 2026-04-27

Benchmarks:

- NIFTY50 buy-and-hold: +11.20%
- Equal-weight traded-universe buy-and-hold: +27.64% average, +20.37% median

The full model’s average per-trade edge of +0.106% before costs is not sufficient evidence of outperformance versus these benchmarks, especially with high turnover, slippage, and in-sample pattern selection.

## 7. Capacity / Liquidity Estimate

Using 20-day average traded value before signal dates:

| Participation | Median Capacity / Signal | 10th Percentile Capacity / Signal |
|---:|---:|---:|
| 0.5% ADV | ₹1.08 cr | ₹0.35 cr |
| 1.0% ADV | ₹2.17 cr | ₹0.71 cr |
| 2.0% ADV | ₹4.33 cr | ₹1.41 cr |
| 5.0% ADV | ₹10.83 cr | ₹3.53 cr |

Practical interpretation:

If live signals are only 5-20 names/day, gross capacity may be viable at small prop-book scale, but not institutional AUM unless signals are diversified, holding periods lengthen, and slippage/impact are explicitly modeled. For smaller ADV names, capacity collapses quickly.

## 8. Slippage / Cost Model

No credible slippage model exists in the audited backtest.

Issues:

- Blind entry assumes next open fill.
- Smart entry assumes intra-day proxy fills derived from daily OHLC.
- No brokerage, taxes, STT, bid/ask spread, impact, auction/opening slippage, or failed fills.
- No participation cap or volume constraint in backtest.

For NSE cash/F&O liquid names, a first institutional haircut should test at least:

- 10-25 bps per side for liquid index/F&O names
- 25-75 bps per side for less liquid names or gap/open entries
- additional impact as a function of participation rate

Given the full-system average is only +0.106% per trade, realistic costs likely erase the aggregate edge.

## 9. Robustness / Sensitivity

Re-simulating existing trades with target move shifted +/-20%:

| Target Shift | Segment | Trades | TP WR | Avg P&L |
|---:|---|---:|---:|---:|
| 0.8x | All | 21,507 | 33.20% | +0.136% |
| 0.8x | Original Turbo+Super | 4,084 | 92.97% | +3.458% |
| 1.0x | All | 21,507 | 30.17% | +0.143% |
| 1.0x | Original Turbo+Super | 4,084 | 100.00% | +4.817% |
| 1.2x | All | 21,507 | 14.56% | +0.118% |
| 1.2x | Original Turbo+Super | 4,084 | 58.01% | +5.117% |

Conservative same-bar TP/SL ordering:

- All trades: TP WR falls from 30.17% to 29.79%; avg P&L from +0.143% to +0.123%.
- Original Turbo+Super: TP WR falls from 100.00% to 99.00%; avg P&L from +4.817% to +4.766%.

Interpretation:

TP/SL ordering matters but is not the primary killer. The primary killer is post-outcome bucketing.

## 10. Answers To The Six Questions

1. **99.6% Turbo+Super WR: real or artifact?**  
   Artifact. Turbo/Super are assigned after TP hit and after realized speed/continuation/MAE. Not investable as an ex-ante engine.

2. **Bucketing logic uses unavailable information?**  
   Yes. It uses exit reason, days to exit, MAE/MFE, MPI, and post-exit 5-day stats.

3. **Smart P&L leaks post-entry data?**  
   Yes. It uses entry-day high/low/close and same-day NIFTY close to choose/price intraday entries.

4. **TP/SL ordering biased?**  
   Yes. TP is checked before SL on daily bars. Observed effect is smaller than bucketing leakage but still optimistic.

5. **Universe survivorship?**  
   Likely unresolved/high risk. The local engine DB has no `universe` table and most OHLCV comes from today’s yfinance/Kite available symbols. Need point-in-time historical membership before pitching.

6. **Corporate actions adjusted?**  
   Not adequately controlled. Extreme daily moves exist and raw OHLCV is written. The specific VEDL 2026-04-30 row was not available locally, but the ABFRL -66.59% row demonstrates the class of problem.

## 11. Recommended Fixes Ranked

1. **Replace Turbo/Super/Standard with ex-ante buckets.**  
   Bucket must be computed at signal time from pattern/tier/probability/liquidity/volatility only. Realized exit speed can be reported later as outcome analytics, not used as engine identity.

2. **Build a dated pattern store.**  
   `pattern_library` must have `as_of_date` / `trained_through_date`. Backtests must only use patterns trained before the signal date.

3. **Remove daily-bar smart-entry leakage.**  
   Either use only entry open for daily data, or ingest intraday bars and simulate decisions using only bars available up to each proposed entry time.

4. **Add conservative TP/SL policy.**  
   If both TP and SL occur on the same daily bar, use SL-first or mark ambiguous and exclude from headline stats unless intraday data resolves ordering.

5. **Point-in-time universe.**  
   Add historical universe membership by date. Do not backtest 2024 using symbols selected in 2026.

6. **Corporate-action-adjusted data.**  
   Choose one institutional source and adjustment convention. Maintain split/dividend adjustment audit columns. Quarantine >25% moves unless explained by verified corporate action.

7. **Costs and capacity.**  
   Add slippage, brokerage/taxes, participation caps, and ADV-based fill rejection.

8. **Walk-forward validation.**  
   Train on rolling windows, freeze patterns, test forward. Report all trades, not post-hoc winner buckets.

9. **Portfolio simulation.**  
   Convert signals into position sizing, max concurrent positions, capital constraints, compounding, drawdowns by date, and benchmark-relative returns.

## 12. Fix Before Pitching To LP / IB

Do not pitch the 99.6% Turbo/Super number.

Before any serious institutional discussion:

- Rebuild engine labels as ex-ante classifications.
- Produce walk-forward OOS metrics where every signal was generated using only prior data.
- Remove smart-entry leakage or back it with intraday data.
- Add transaction costs and capacity constraints.
- Produce benchmark-relative performance against NIFTY and equal-weight universe.
- Produce an audit trail showing data source, corporate-action adjustments, and point-in-time universe membership.

Until then, the honest institutional statement is:

> Kanida has a promising pattern-mining research prototype, but the current Turbo/Super/Standard performance claims are not yet valid investment evidence.
