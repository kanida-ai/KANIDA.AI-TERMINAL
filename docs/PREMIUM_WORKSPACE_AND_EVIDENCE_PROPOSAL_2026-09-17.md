# Premium workspace and historical evidence proposal

Status: design and research proposal, not an implementation change. Source checked during 16 September Pacific / 17 September IST; Claude is working concurrently, so findings are a point-in-time inspection. All numbers in the accompanying visual concept are illustrative.

## Product direction

Preserve the user's modular workspace: each strategy family opens a linked block containing two screeners, a chart and evidence. The repeatable product journey is **discover a setup → understand its historical behavior → evaluate an explicit trade rule → prepare a trade**.

The quality of the product should come from fast, coherent decisions, legible charts and trustworthy evidence. Positive historical performance must not be a requirement for a result to appear. A clear finding of no historical advantage is useful; an insufficient sample is a different finding.

## 1. Visual and interaction design

### Block structure

- Family-level navigation: Charts & Candlesticks, Quant, Options, Events. Allow subcategories within a family. Retain the catalogue's distinct pattern identities even when the visual grouping combines families.
- Keep a compact collapsed summary for each block; expand the active block. Avoid rendering many full-height empty charts and tables simultaneously.
- Each block owns its universe, two screener configurations and selected setup. Selecting a symbol updates that block's chart and evidence together. Other blocks retain their selections.
- On a wide desktop, starting proportions: screeners 18% + 18%, chart 38%, evidence 26%. Make panels resizable and offer a focus view. On narrower screens use two rows rather than crushing chart labels.
- Keep scan results in place while refreshing. Stamp the base data, detection time and evidence refresh separately. Show pending/reconnecting states without clearing the chart.
- On first use select the first eligible result with a visible selection highlight. Afterwards preserve the user's selection; never jump to a newly ranked stock during refresh. Show a clear changed/expired state when its eligibility changes.

### Visual hierarchy

- Neutral canvas and slightly raised opaque panels; a single brand accent for selection and actions. Reserve red/green for direction/outcomes and pair them with text.
- Clear heading scale, 13–14 px readable table text on desktop, aligned numeric columns, restrained 1 px borders and consistent spacing. Provide Compact and Comfortable density.
- Give the chart the largest visual area. Keep pattern geometry visible and put less-used studies behind controls.
- Use meaningful titles: “Triangle breakout · 1D”, “What happened next?”, “Trade rule”. Generic CHART/BACKTEST headings consume space without explaining the selected state.
- Replace implementation language such as `very_small`, detector identifiers and release filenames with plain product states. Put detailed provenance in an expandable “Method & data” view. Material qualification such as unreviewed evidence must remain visible.
- Consolidate repeated universe/freshness controls. Move the superseded legacy block to an archive/admin view after migration, rather than putting its migration notice at the top of the trader workspace.
- Empty states should offer a useful next step: view researched history, use a broader configured scan or wait for the next completed candle. They should not occupy most of an otherwise blank workspace.

### What to take from TrendSpider

Its official dashboard model has named sections with linked widgets: choosing a symbol updates charts in that section. Adopt the clarity of that relationship, while adding KANIDA's evidence alongside the chart. The supplied screenshot is the visual reference; the authenticated dashboard URL was not independently accessible via web retrieval.

Source: https://help.trendspider.com/kb/workspaces/dashboards

## 2. Distinguish three products in the evidence system

| Product | Question | Correct outputs |
|---|---|---|
| Pattern history / event study | What followed this objectively identified pattern? | Forward-return distribution by horizon; positive rate; favorable/adverse excursion; target/stop sequence; benchmark comparison |
| Trade-rule backtest | What happened if a particular executable rule was followed? | Exact entry/exit/cost assumptions; net expectancy; realized outcomes; trade ledger; capital-aware portfolio metrics only when simulated |
| Walk-forward validation | Did the selected rule work on later unseen periods? | Train/test dates, selected rules by fold, out-of-sample counts/returns, stability and uncertainty |

These share events and market data but have different meanings and denominators. A favorable excursion is not an executable profit; a positive horizon return is not proof of a profitable target/stop strategy; a selected portfolio equity curve cannot be inferred by compounding overlapping pattern occurrences.

## 3. Existing implementation to preserve

- `market_scanner/pattern_research/evaluation.py`: actual strategy evaluation, next-open entry, total 0.40% entry-notional cost assumption, ATR stop/target and time exits. Rule candidates include 1H/4H 3/6/12/24 bars, 1D 5/10/20/40 bars and 1W 2/4/8/13 bars.
- Rolling 36-month training and 6-month test folds, minimum training count 20, positive mean-minus-standard-error selection, horizon purge and maximum-hold embargo, non-overlapping test admission across fold boundaries.
- `outcomes.py`: separate event study through 30 hourly, 20 four-hour, 10 daily and 8 weekly bars. Returns, distributions, MFE/MAE, first-touch barriers, stability and walk-forward horizon selection already exist.
- Existing ambiguity handling includes stop-first for same-bar touches and opening prices for gaps through barriers. Incomplete future horizons remain unavailable. These are modeling policies, not knowledge of the unobserved intrabar path.
- Baseline, block-bootstrap comparisons and multiple-testing correction are implemented, with caveats below.
- Recent-five exact-cell occurrences already exist. They are same-stock chronological cases, not historical similarity search across stocks.

The user's proposed grids mostly fit the outcome engine. A 12-week outcome requires extending the current eight-week outcome limit and recomputing affected research; the legacy trade-rule engine's 13-week candidate is a separate result.

## 4. Corrections and research refinements

### First: repair semantics and known integration faults

1. Separate all-history descriptive metrics from out-of-sample metrics in the API and UI. Current `cards.py:122` can combine OOS win/mean with all-history median/MFE/MAE and a differently scoped baseline. Give every section a coherent scope, dates, horizon and denominator.
2. Reconcile the stored baseline-window mismatch flags and coverage with card text. Do not describe unmatched windows as matched. For an OOS headline, use the same test dates and rule/horizon policy in the baseline.
3. Reconcile refined significance sidecars with the stored serving result. Do not silently choose whichever q-value looks better. Version and publish the reviewed result atomically.
4. Fix barrier accounting: `cards.py` currently maps undetermined count to neither count and omits the separate gap-exit category. Every percentage must declare its eligible cohort and observation window. Unresolved recent cases are not “neither within a completed window”.
5. Separate fixed-horizon rows from target/stop rows. Current recent rows use one horizon for net return/MFE/MAE but a separate +2%/−1% barrier for outcome/bars held. Avoid one WIN/LOSS label spanning both.
6. Connect exact occurrence identities to historical chart replay. Current card rows lack occurrence IDs and replay URLs, and omit stored entry index.
7. Preserve the earlier confirmed fix requirement: CAS daily/weekly freshness expects 15:15 while official bars end at 15:30. This makes valid Discover setups vanish on the chart. Claude may already be addressing it.

### Then: improve inference without manufacturing a positive result

- Detect using only information available at that time. Keep a frozen as-of geometry, detection version, state and data snapshot. Do not let later pivots redefine the earlier signal.
- Count one occurrence per defined formation/state transition; do not count each refresh of the same forming pattern as a new independent sample. Descriptive overlapping events may remain available with an explicit label.
- Measure forming setups from when they were detectable, including those that never confirm. Add confirmation/failure rates and time-to-confirmation. Confirmed outcomes cannot be used as the predicted history of every forming setup.
- Use a common eligible cohort when comparing horizons, or display the changing n at each point. Do not treat recent, incomplete outcomes as zero or drop them without reporting the pending count.
- Use costs appropriate to venue, liquidity and order assumptions. The present 0.40% allowance is a simplifying assumption; it is not an observed spread/fill model. Retain it as a reproducible reference, add realistic sensitivity scenarios and explicit short eligibility/borrow limitations.
- Separate per-stock history from pooled history across eligible stocks. Thin per-stock data can coexist with useful pooled evidence, but must not inherit its certainty. Specify stock weighting and benchmark construction; use resampling robust to shared dates/stocks when drawing pooled inference.
- Add true index/sector conditions using point-in-time data. Current “regime” is the stock's own 200-bar trend because the production outcome call supplies no index series. Do not call it a bull/bear market condition.
- Potential similarity features: relative volume, volatility/ATR regime, liquidity, sector/index relative strength, normalized pattern geometry, breakout strength, age and event proximity. All must be observable at signal time. Define them before evaluating outcomes.
- Existing score tertiles and condition buckets are exploratory. Select thresholds/interactions on training data only, then test on later data. Searching more conditions creates more chances to find a flattering result by luck.
- Address current-membership survivorship bias using point-in-time universes where available; otherwise disclose that the historical study concerns today's covered stocks. Market-cap/sector conditions also need historical membership/classifications.
- Preserve walk-forward selection and an untouched chronological holdout. Freeze a release before forward monitoring. If a test set has repeatedly guided decisions, it is no longer untouched.
- If a rule has no advantage, show “No demonstrated advantage after costs”; if uncertain, show “Limited evidence”. Do not recast a negative result as bullish or bearish opportunity without a separately validated rule.

Sources supporting the validation principles:

- CFA Institute, Backtesting & Simulation: https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/backtesting-and-simulation
- Bailey et al., The Probability of Backtest Overfitting: https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf

## 5. Trader-facing card contract

### Compact view

1. **Identity:** symbol, timeframe, pattern, direction, forming/confirmed state.
2. **Scope:** “This stock” or “Across studied stocks”; historical dates and eligible count.
3. **Evidence state:** historical description, held-out validation, limited evidence or requires review. Do not turn statistical confidence into a “probability this trade wins” badge.
4. **Primary numbers:** positive net outcomes at the declared horizon, median net return, and the matched baseline comparison. Use uncertainty in expanded view and in the compact view when it changes the interpretation.
5. **Actions:** Explore evidence, Watch setup, Test a rule. Trade preparation depends on the actual validated rule and execution eligibility.

For a bearish setup, label returns as hypothetical short-side returns, and say “favorable/adverse move” rather than price upside/downside. A 64% positive rate alone cannot answer whether the pattern works: the baseline may be 70%, losses may be much larger than gains, or uncertainty may be large.

### Expanded view

- **Pattern history:** forward curve with horizon n, median and interval/distribution; mean and comparator when evaluating incremental effect; excursion distributions; first-touch outcomes for an explicit target/stop/time cap.
- **Trade rule:** entry, exit, costs, sizing/eligibility; realized net expectancy and trade outcomes; separate OOS tab/scope; drawdown only for a correctly constructed equity path.
- **Recent occurrences:** chronological examples with one consistent measurement policy, plus historical chart replay.
- **Conditions:** only conditions with sufficient evidence, show sample and comparator. Exploratory findings labeled as exploratory.
- **Method & data:** dates, source, last refresh, assumptions, exclusions and research/detector version.

Avoid “Best holding window” for a peak found on the same data being summarized. Use “Historical return profile” for descriptive analysis or “Holding period selected on training data” for a validated rule. MFE flattening is not an optimal exit: maximum favorable excursion grows mechanically as the observation window expands.

## 6. Last-five design and replay

Default: **Latest five completed occurrences in the selected cohort**. Offer “This stock” and “Across stocks” as explicit scopes once both are implemented. “Similar occurrences” requires an implemented, explained similarity definition. Do not select the five best outcomes.

For the fixed-horizon table:

`Date/time | Stock | Net return at 8 candles | Best favorable move within 8 | Worst adverse move within 8 | Replay`

Use Positive/Negative/Flat only when a label adds clarity. Display pending or fewer-than-five completed cases honestly. A separate target/stop view can show target-first, stop-first, timeout, ambiguous or incomplete, together with net execution return under its declared policy.

Replay shows frozen as-of pattern geometry, signal close, next-open entry reference, subsequent candles, holding-period end and actual excursion locations. If an explicit rule is selected, show that rule's stop/target/exit separately. A reveal-future control would let traders inspect what was known at the time. Do not redraw a retrospective idealized pattern.

## 7. Make instant evidence fast

Standardized evidence should be a cached lookup, not a fresh multi-year backtest for every click:

1. Precompute versioned event records and outcome rows from frozen history.
2. Index compact aggregates by pattern/variant/side/state/timeframe, universe/cohort, measurement policy, costs and data/detector versions.
3. Detect new setups from completed live candles, then look up the matching cohort's saved evidence.
4. As future candles arrive, mature each event's horizons and update incremental aggregates; preserve revisions when data is corrected.
5. Serve recent rows and frozen replay from occurrence IDs.
6. Arbitrary user-created rules become separately queued tests with progress and an exact parameter hash. Never substitute a nearby cached rule and label it an exact result.

Maintain separate market data, research/event artifacts and product/serving stores. There is no need for a separate database per strategy. Bulky reproducible event history can be partitioned; the app-facing store can hold indices and compact aggregates. Actual options results require historical contract/quote/expiry data and realistic execution assumptions, beyond underlying OHLCV.

## 8. Suggested implementation order and acceptance criteria

1. **Evidence contract and correctness.** Every displayed metric shares or explicitly declares its cohort, horizon, dates and costs. Recent rows match the selected measurement; baseline mismatches and incomplete outcomes have test coverage. Exact selected setups resolve on charts.
2. **Premium block and evidence redesign.** One linked selection updates chart and card without flicker; blocks retain state; no clipping at agreed desktop/tablet widths; critical qualifications remain visible; normal refresh does not move the selection.
3. **Occurrence replay.** Five latest completed eligible rows open the exact frozen historical setup. Returns can be independently recalculated from the shown entry/exit bars and declared costs.
4. **Pooled/similar history and richer conditions.** Explicit point-in-time feature definitions, cohort filters, baseline and dependency-aware validation. No future features or retrospective success filters.
5. **Custom rule and broker integration.** Catalogue parity across scanning/research/testing, versioned API contracts and lifecycle alerts, execution gates, monitored forward results, and then broker-specific capabilities.

Broker adoption remains a business outcome to validate. Consistent setup identity, transparent assumptions, reliable APIs, reproducible evidence and responsive UX are concrete engineering deliverables that support that goal.
