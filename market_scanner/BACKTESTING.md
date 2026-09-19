# KANIDA stock-specific historical studies

The user delegated rule design and rule mining. `backtest_rules.json` records
the assumptions used in the first research version. No statistics or chosen
rules are pooled across stocks. Every cell is identified by stock, pattern,
timeframe and trade direction. All ten detectors and four timeframes are replayed.

## Three distinct outputs

1. **Historical behavior:** a descriptive, fixed-horizon baseline on the full
   available history. Enter after the first qualified setup in an episode; use
   confirmations for a cell that has no setups. Enter at the next candle open.
   Hold 6 candles on 1H/4H, 10 on 1D, or 4 on 1W, then exit at the last close.
   These values are declared in advance and are not optimized for returns.
2. **Mined rule:** choose independently for the stock/pattern/timeframe/side,
   using early training data and subsequent validation data only.
3. **Held-out results:** evaluate the chosen rule on the final test segment.
   At least 20 eligible test trades are required before showing an estimate
   labeled as next-trade historical expectancy or historical win probability.
   The estimate can be negative. Below the threshold, raw test statistics and
   their sample size remain visible, with “insufficient evidence.”

A failed rule search is a valid result. The program does not lower thresholds
after viewing outcomes or replace missing stock history with a market average.

## Replay and duplicate opportunities

Replay sees at most 260 completed candles, exactly like the scanner. The same
Detector methods make the final decisions. Seven-bar pivots become visible only
after three subsequent candles close. A compiled conservative rejection filter
improves speed; it cannot qualify a pattern. Tests compare it with unfiltered
replay, including full TITAN history on all four timeframes and positive fixtures.

Record one setup and one confirmed breakout per pattern episode per side.
Require three consecutive absent closes before rearming. A neutral pattern is
studied separately in both directions. Each candidate allows one open trade per
cell, without overlapping positions. Other cells can overlap; these results are
individual studies, not a portfolio backtest or an aggregate return.

## Candidate rules and selection

All entries use the following candle's open. The signal candle is never an
executable entry. This assumes the signal is available at the close and does not
model scanner batch/settlement latency; these fills are research assumptions,
not a claim that the running scanner can execute them. The candidate grid has
56 combinations per cell:

- Setup versus confirmed-breakout trigger.
- Four maximum holding periods: 3/6/12/24 candles on 1H and 4H,
  5/10/20/40 on 1D, and 2/4/8/13 on 1W.
- A time exit alone, or a 1/2 ATR stop combined with a 1/2/3R target.
  ATR is the scanner's 20 prior true ranges, known at signal time.

The first 60% of candles form training, the next 20% validation and the last 20%
testing. A maximum-horizon embargo follows each boundary. Trades cannot cross
the boundary, and every entry must have its entire maximum holding horizon
available inside the segment, regardless of whether it could exit early.

Training needs 20 trades and a positive selection score, defined as mean net
return minus one standard error. Shortlist the top five candidates. Validation
needs eight trades and a positive score; choose the best validation score with a
deterministic rule-ID tie break. Freeze that rule before evaluating the test.
Never use test performance to pick a different rule. This reduces leakage and
overfitting; a single chronological holdout does not eliminate either selection
uncertainty or regime risk. No claim of statistical significance is made.

## Fills, costs, excursions and exclusions

Returns are unlevered percentages of entry notional. Assume 30 basis points in
round-trip fees plus 10 basis points in round-trip slippage (0.40% total), deducted
once per trade. These are configurable research assumptions, not verified broker
tariffs or an exact tax calculation. Cash dividends, borrowing and funding costs
are not supplied. Position-size and liquidity impact are not modeled. Bearish
results are hypothetical price studies; multiday
equity-short availability and derivative basis are not established.

Stops and targets remain fixed after entry. A gap through a barrier exits at the
open. If a candle touches both stop and target, exit at the stop and mark the
ambiguity. Otherwise exit at the touched barrier or the holding-period close.
An intrabar exit has a candle timestamp and explicitly unknown exact exit time.

Win rate is the fraction with net return strictly above zero. Expectancy is mean
net return, equivalent to win probability times average win plus loss probability
times average loss. Breakeven trades are counted separately. Wilson 95% intervals
accompany WR; expectancy uses a normal approximation based on the sample standard
error (unavailable for one trade). These intervals do not correct for serial
dependence, searching many candidates, or comparing many stocks.

MFE is the best favorable price excursion during a trade; MAE is the largest
adverse excursion. Excursions are percentages of entry, before costs. A time-exit
trade includes the complete final candle. A barrier-exit candle has unknown
ordering: record guaranteed/possible excursion bounds. The headline MFE uses its
lower bound and headline MAE its conservative upper bound. Full bounds and an
uncertainty flag remain in the ledger. “Max FAV” and “Max adverse” are maxima
across individual trades, not maximum portfolio drawdown. Average values are also
reported.

Signals cannot span missing candles, invalid OHLC or >35% discontinuities. A
trade encountering a marked gap while held is censored and counted separately.
This does not repair unadjusted data and can introduce exclusion bias; inspect
the quality counts. Smaller corporate actions may remain unmarked. The active
metadata universe excludes unavailable/delisted stocks, causing survivorship
bias. Actual data coverage varies by stock. Zero source bars are not evidence
that a pattern never occurs.

The initial observed calendar begins on 1 January 2013. Weekly buckets whose
earlier weekdays predate that calendar are excluded: absence from the calendar
cannot prove a market holiday. This correction removed one initial weekly bucket
from 299 stock histories and recalculated their weekly studies. All other
timeframe inputs remained unchanged.

## Reproducibility and operation

The source opens read-only. Full aggregated history is frozen per stock in
`output/history/<run-id>/`, with a SHA-256 checksum. Results and full trade ledgers
live separately in `output/backtests.sqlite3`. The run ID fingerprints the rules,
scanner configuration and detection/simulation code. Completed stocks survive
interruption and can resume with the same code/rules. Each stock is read in one
source transaction. Source changes during a run are flagged in the UI; this is
not a single long market-wide source transaction.

Start or resume from the workspace root:

```powershell
./market_scanner/backtest.ps1
```

After deliberately changing OHLC or wanting a new snapshot:

```powershell
./market_scanner/backtest.ps1 -NewSnapshot
```

The worker runs independently of the web server. Closing the browser does not
stop it. Results become available as each complete stock is committed; the UI
reports partial progress until the full universe finishes. Backtesting is an
explicit batch research run; live candle scanning remains automatic. New source
candles do not silently retune the selected rules.

The local **Pattern backtests** view filters by stock, pattern, timeframe, side
and evidence status. Select a row for the rule, training/validation/test results,
selection audit and trade ledger. Select a signal to reproduce its frozen
historical chart. Download a complete cell, all candidate training statistics
and every trade as JSON.

APIs: GET `/api/backtests/state`, `/api/backtests`, `/api/backtests/cell`, and
`/api/backtests/chart`. List queries accept `symbol`, `pattern`, `timeframe`,
`side`, `status`, `sort`, `offset` and `limit`. Cell queries use the exact four
identity fields; chart queries use stock, timeframe and absolute signal index.

Metric references: [CME on mathematical expectancy](https://www.cmegroup.com/education/courses/trading-psychology/the-mathematics-of-trading-success)
and [Tradervue trade statistics](https://app.tradervue.com/help/trade_stats).
The rule grid, thresholds, assumptions and conservative OHLC conventions above
are KANIDA research choices, not claims of universally profitable rules.

## Virtual capital (accounting version 1.0.0)

The original research engine simulates percentage returns and does not choose
rules using a cash account. The capital view replays its **saved trade fills**;
adding this view does not change historical pattern detections, rule selection,
trade eligibility, stops, targets or the train/validation/test boundaries.

Every stock × pattern × timeframe × direction has an independent account. The
whole-history baseline, training, validation and held-out test each restart with
the requested starting capital. Training and validation returns are never spliced
into a selected rule's held-out curve. Results across overlapping studies cannot
be summed into a portfolio return.

Choose ₹10,000, ₹30,000, ₹50,000 or a custom amount. Auto chooses the smallest of
these presets that affords one share at the first eligible entry plus its assumed
round-trip costs. If more than ₹50,000 is needed, it rounds that initial requirement
up to ₹1,000. Auto uses no future entry prices or returns, defaults to ₹10,000 for
an empty ledger, and never adds deposits later to rescue an unaffordable trade.

For each non-overlapping trade, let B be available cash, P the frozen entry price
and c the saved round-trip cost fraction. Buy `floor(B / (P × (1+c)))` whole shares,
reserve the entry notional and round-trip costs, and keep the remainder as cash.
Deduct costs once. Gross P&L is `shares × (exit − entry)` for longs, and its negative
for shorts. Final balance is `B + gross P&L − costs`. Cash amounts round to paise;
the next trade sizes from that final balance. Zero-share trades are skipped and
reported. No leverage, deposits or cash interest are assumed.

Hypothetical shorts reserve 100% of entry notional as collateral, with sale
proceeds locked. A gap can produce a deficit; a depleted account takes no further
trades. Borrowing, funding, actual margin requirements and derivative lot sizes
are not modeled. The F&O filter selects labeled cash equities, not futures trades.

The cash curve and its explicitly labeled closed-trade drawdown use balances after
trade settlement, omit intra-trade fluctuations, and space eligible trades evenly
rather than by elapsed calendar time. MFE and MAE remain the original price-based
excursions. Cash-account win rate can differ from percentage-study win rate when
trades cannot be afforded or paise rounding changes a tiny P&L. Fees/slippage remain
the research assumptions; liquidity, taxes beyond assumed costs, dividends and
unadjusted corporate actions are not newly modeled.

GET `/api/backtests/capital` uses the four cell identity fields plus
`segment=reference|train|validation|test` and `capital=auto|<rupees>`. Optional `run`
guards against displaying cash from a different active snapshot (HTTP 409).
Capital JSON includes run ID, accounting version, segment, selected/baseline rule,
original assumptions, cash ledger and curve. Invalid segments, capital amounts
and attempts to use an unvalidated rule's test segment return HTTP 400.


## Return bands and adjustable history count

The browsing minimum now defaults to five historical trades and can be changed
independently of the selected return range. Zero includes any sample size. This
is a display filter over existing trade records: it does not rerun or lower the
frozen rule search's training, validation or final-test requirements. Every row
shows its actual count, and the main average is historical, not a prediction.

The simple view uses past average net return per trade and keeps detailed win
rate, loss, excursion, rule and cash-account metrics behind View history. Return
bands use the displayed average rounded to two decimals; backend SQL, APIs and
scanner filtering share the same decimal rounding rule.

Holding time counts market candles rather than elapsed wall-clock hours. In the
normal 1H schedule, a six-candle entry at Monday 13:15 consumes three Monday
candles (including the 15-minute closing candle), then three Tuesday candles,
exiting Tuesday 12:15. Holidays can move that exit to the next trading day.
Six 4H bars span three to four trading dates; ten daily bars mean ten trading
sessions; four weekly bars mean about four weeks. Selected stop/target rules
can exit before their maximum candle count. The simple result labels these
holds explicitly as potentially overnight, with detailed candle counts available.
