# Expanded per-cell evaluation

Engine 1.0.1 fixes the baseline trigger from declared capabilities: `setup` when supported, otherwise `confirmed`. It never chooses that trigger by inspecting whether a future setup occurs. The legacy adapter declares horizontal breakout as confirmed-only and channel as setup-only, matching their existing implementations.

API: `market_scanner.pattern_research.evaluation.evaluate_cell(bars, events, timeframe, side, states)`.

Returns JSON-safe native values: engine version, status, bar/event counts, fixed-baseline rule/statistics/trades, rolling walk-forward statistics/trades, fold records, candidate count, exclusions, and explicit assumptions. Each trade includes signal/entry/exit indexes and timestamps, entry/exit prices, rule, fold, return/cost percentages, exit reason, and same-bar stop/target ambiguity. The caller supplies stock/pattern/variant identity and owns persistence.

Saved trades also carry favorable/adverse price excursion lower and upper bounds. `mfe_pct` and `mae_pct` alias their known lower bounds. Intrabar extrema after a barrier exit are not treated as known pre-exit excursions. A missing-data exit has unknown (`null`) upper bounds. These are price excursions before transaction costs. Statistics include breakeven trades and a Wilson 95% interval for win rate; these intervals do not adjust for candidate selection or temporal dependence.

## Fixed protocol

- Reference: setup if present, otherwise confirmed; next open; hold 6 candles on 1H/4H, 10 on 1D, 4 on 1W. No optimized stops. Full-history descriptive result.
- Walk-forward: first test starts 36 calendar months after the first available candle; subsequent tests advance six calendar months. Train over preceding 36 months. Candidates use supported triggers and the legacy hold/ATR/target grid (28 per trigger). Select a rule only with at least 20 independent training trades and positive mean net return minus one standard error. Equal scores prefer lexicographically smaller rule IDs. A rolling test does not have a separate validation optimization pass.
- Purge: candidate's entire potential holding horizon plus the maximum candidate holding horizon must end strictly before test start. Actual early exits cannot make an otherwise ineligible training observation eligible.
- Costs: 30 bps round-trip fees plus 10 bps round-trip slippage deducted as fixed 0.40% of entry notional. Fills use raw OHLC; costs are not also charged to prices.
- Execution: next observed open, conservative stop before target on ambiguous candles; opening price gaps fill at the open. Signal or entry candles marked with a data-quality gap disqualify entry. A data-quality gap discovered during a held position exits at the first available open and retains its gain/loss.
- Final sample: a trade is admitted only if its rule's full potential holding horizon exists in the supplied data, even when a future early target might have made it close sooner. This is an explicit final-sample eligibility convention, independent of realized returns. Interior fold ends do not truncate positions.
- Nonoverlap: entries must occur after the previous accepted exit candle. One admission stream persists across all test folds. A trade keeps the rule selected at entry and can finish in a later fold. The next fold cannot admit a competing position while it is still open.

## Interpretation

Fold statistics are **entry-cohort results** and include each trade's full eventual outcome. They are not calendar-period profit and loss. No shared capital, equity curve, portfolio sizing, compounding, calendar P&L, or executable short-product claim is made. Summed trade percentages are descriptive arithmetic sums. `tested` is a sample-count status (at least 20 total out-of-sample trades), not proof of profitability or multiple-testing-adjusted significance.

Short results are hypothetical price studies. Universe and corporate-action adjustments require separate data verification. More candidate families increase selection risk; preserve failed folds and candidate counts, and do not relax thresholds after looking at results.

## Efficiency and caller contract

OHLC arrays are cached by the most recent input list identity; callers must treat supplied candle lists as immutable. A Numba matrix calculates each candidate/event outcome once and reuses it in every fold. Training selection also runs in Numba. Only the selected and reference trade ledgers are returned, not every candidate ledger. All-price coherence is checked once per cached list. Events must supply valid actual signal indexes, supported states, and finite positive causal ATR.

## Verification

Focused synthetic tests cover next-open long/short costs, ambiguous barriers, opening and data-quality gaps, full-horizon eligibility, independent occurrence/trade identities, purging despite early exits, first-fold future-price invariance, and cross-fold rule/position carry-over. No full research run is performed by this module's implementation task.
