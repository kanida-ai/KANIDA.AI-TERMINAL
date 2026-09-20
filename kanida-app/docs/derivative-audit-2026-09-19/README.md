# Derivatives: product, workflow and logic audit

Audit date: 19 September 2026 (Pacific). Stored session inspected: 18 September 2026, IST.

## My assessment

KANIDA already has useful ingredients: one instrument selection links the page; real stored data drives the charts; missing values generally remain missing; filters have server-side validation; and the reusable block frame gives the product a recognizable identity.

The main obstacle to charging a premium is **confidence and speed of understanding**. A trader currently has to reconcile different times, interpret dense tables, and scroll through repeated panels. Some signals and labels also have correctness defects. A more attractive theme alone would leave those problems intact.

The proposition I recommend is: **Understand what changed in the derivatives market, inspect the evidence, and monitor the conditions that matter to you.** Build repeat use around saved work, useful changes, reliable alerts and reviewable history. Virality and willingness to pay remain hypotheses to validate with customers; neither follows automatically from adding AI or a polished interface.

No application logic or production data was changed during this audit. The deliverables are this assessment, backend evidence and an implementation prompt pack.

## What was inspected

| Workflow | Verification | Result |
|---|---|---|
| Default arrival and market status | Live desktop/mobile, source, read-only DB | Page says 15:45; screener falls back to 11:30. Important distinction is buried in explanatory text. |
| Index selection | Clicked NIFTY screener row | Linked analysis updates, but it is not a concise index overview. |
| Stock search and selection | Searched and selected TCS | Search and symbol propagation worked; search is limited to returned eligible rows. |
| Option chain → option detail | Clicked NIFTY 23,300 CE | Contract chart updates; chart and chain show different times/values. “Edit this chart” routes to underlying daily chart in source. |
| Filter draft/add/change/cancel | Live builder and source | Draft cancellation works. Build-up dropdown displays six dashes. |
| All 13 filter fields | Frontend mapping + backend validation | Operator, instrument-universe and filter-scope issues below. Exhaustive interactive combinations were not exercised. |
| Reading selector | Selected 15:45 | Screener becomes empty; chain loses spot/ATM while other blocks retain independent contexts. |
| OI by strike | Live chart/table + source | Bars render; table starts far from ATM. Units and colors need consistency. |
| Ten-strike OI grid and futures candles | Live + source | Renders actual series. At desktop width, calls wrapped 4+1 and puts were below the fold. Daily futures history unavailable in the inspected case. |
| Signal table | Live, source, synthetic reproduction | Historical labels repaint; rule ignores price direction when choosing bullish/bearish. |
| PCR / max pain / IV / futures session panels | Live content, screenshots, source, backend responses | Useful data, but stale/latest mixing and sparse-data wording need correction. |
| Index dashboard and futures market list | Live content + backend | Index key values blank; futures list empty despite futures data in other panels. |
| Expand, close, restore, refresh, hover | Source review; partial live interaction | Implementation exists. Browser connection failed during expand checks; these controls are not certified by this audit. Persistent hide settings were not changed. |
| Responsive layout | Temporary 390×844 viewport; restored | Introductory text dominates first screen; wide table still requires horizontal scrolling. |
| Empty/loading/error/stale handling | Source and observed missing-data paths | Distinctions exist, but several messages conflate unavailable measurements with no market activity. Deliberate server outages were not injected. |

The existing `DERIVATIVE_TAB_AUDIT.md` was read as prior context. Findings here were independently checked where stated, and extend it with backend and calculation evidence.

## Critical findings before visual polish

### 1. Historical signal labels can change after later data arrives — P0

`src/derivative/logic.ts:signalRows` normalizes each historical row against the largest change over the **whole available session**, including later readings. A synthetic append-only reproduction changed the same time=4 call row from `Very strong` to `Stable`, and its market signal from `bearish` to `bullish`. This is a correctness problem for replay, alerts and any historical evidence.

Also, `marketSignal` uses OI direction without the price-direction fields. A put-buying state can still produce `strong_bullish`. Price-plus-OI labels are interpretations, not observations of who initiated trades.

**Prompts:** P03 and P04.

### 2. The screen combines incompatible readings — P0

Default screener: 18 Sep 11:30, 214 instruments, 902 eligible contracts. Newest stored reading: 15:45. The chain follows the screener's reading; OI grid, session panels, selected series and cross-market lists follow their own paths without one shared `as_of`.

NIFTY max pain illustrates the consequence:

| Reading | Max-pain strike | Spot | Valid distance |
|---|---:|---:|---:|
| 11:30 | 23,300 | 23,302 | −2 points |
| 15:45 | 23,350 | missing | unavailable |

The headline combined **23,350**, **23,302**, and **“2 below”**. The distance itself is valid for the older pair; the combined sentence is not. Do not fix this by subtracting prices from different times.

DTE also mixes captured-date and current-date calculations: the same 22 Sep option appeared as both 4 and 2 days to expiry. `indices()` uses a spot-minus-strike distance convention while the session panel specifies strike-minus-spot.

**Prompt:** P01.

### 3. Missing capture is being interpreted as no eligible market activity — P0

Read-only metrics counts at 11:30: 27,239 contract rows, all with spot and premium. At 15:45: 10,552 contract rows, **zero with spot and zero with premium**. These rebuilt rows cannot pass a premium floor. The fallback is disclosed, but “No contract cleared the floors” does not explain the measurement outage. The global healthy-looking cash/pattern status does not describe derivatives coverage.

**Prompt:** P02.

### 4. Build-up labels do not match the stored vocabulary — P0

The database contains `Long build-up`, `Short build-up`, `Long unwinding`, `Short covering`, `Flat`, `no data`. The frontend `buildupLabel` recognizes snake_case keys. The builder passes the human-readable strings into it; all six choices visibly become dashes. Related table cells also lose their labels.

**Prompt:** P05.

### 5. “3 conditions” is not a count of three condition types — P0

NIFTY's aggregate had **124 distinct reason strings**, including different numerical multiples of the same two reason families. Backend grouping counts complete strings and sorts by their count. Frontend clamps that count to three and labels it “3 conditions”. This makes the badge and ranking misleading, and produces enormous hidden/accessible cell text.

**Prompt:** P06.

### 6. Filter semantics and scope are inconsistent — P1

- “Greater than” and “less than” send inclusive bounds for volume/OI fields.
- Premium `> V` is approximated by `>= V+0.01` **crore**, not a general strict comparison. The source comment calling this paise is incorrect.
- “Type is not CE” becomes PE-only even though the screener now contains futures too.
- Watchlist does not filter the screener; numeric rules do not uniformly filter analysis panels. Several Customize buttons open the same globally labelled sheet.
- An expiry filter only propagates to linked analysis when an underlying filter is also set.
- OI-change value menus only offer positive presets; no negative thresholds/ranges. Backend capabilities are broader than the editor.
- Source picks the default symbol from row one of an unusual-ranked list but calls it “Busiest by premium”. That description is not guaranteed by the ordering.

**Prompts:** P07 and P12.

### 7. IV's “latest” claim can be false — P1

`IvCell` finds the last non-null point, then says “Solved at the latest reading” whenever one exists. Inspected tiles had 9 solved points out of 26, ending at 11:30, while the session reached 15:45. The 34 rejected observations in the ATM pair can be two legs × 17 missing observations; labelling them simply as 34 “readings” beside 26 session slots is confusing, not necessarily a wrong total.

**Prompt:** P08.

### 8. Units and activity measures need a clear contract — P1

The pipeline documents Kite OI/volume in underlying units; some UI text calls these contracts. The screener adds option premium turnover and futures turnover under “Premium”. Those are different economic measures. A 782× or 1,445× volume ratio may be mathematically real but needs its baseline size, sample count and liquidity context before it deserves visual prominence.

**Prompt:** P09.

### 9. Alternate readers produce an inconsistent product — P1

The index dashboard reads `underlying_snapshots` and shows blank PCR/max-pain values while the selected-index session routes have values from metrics. The futures list returns zero and says no contracts have been captured, while the selected NIFTY futures chart and OI series are populated. This needs source/eligibility reconciliation, not invented fallback numbers.

**Prompt:** P10.

## What the trader should see after clicking an index

Keep one instrument row per screener entry and the reusable block design. Add a compact persistent context bar, not a permanently pinned full-height screener.

| Order | Information | Question answered |
|---|---|---|
| 1 | Index identity, spot and change, selected expiry, session/as-of, feed quality | “What exactly am I looking at?” |
| 2 | Three-sentence market brief + changes since my last visit | “What changed, and why should I care?” |
| 3 | Spot/futures chart with synchronized OI context and a few labelled concentration zones | “Where is the activity relative to price?” |
| 4 | Three to five evidence cards: OI additions/removals, unusual volume with baseline, ATM IV change, futures positioning | “What supports or contradicts this reading?” |
| 5 | Compact option chain at ATM, with expiry and strike-window controls | “Which actual contracts should I inspect?” |
| 6 | Analysis blocks: OI map, volatility, PCR, max pain, futures | “Let me investigate the details.” |
| 7 | Save view, alert on a condition, add to journal, compare scenarios, share a snapshot | “How do I make this useful in my workflow?” |

Max pain should be secondary context, not the main directional signal. Show mixed evidence explicitly. For stock derivatives, preserve the sequence but include underlying events when a dependable event feed is available.

### Suggested first viewport

```text
NIFTY ▾   22 Sep expiry ▾   Session / as-of ▾   Data quality
Spot + change | Session range | ATM IV | activity coverage

MARKET BRIEF                         [Why?] [Changes since last visit]
What changed • evidence • contradiction or missing data

[ Underlying chart + OI context ]     [ Most relevant changes / saved alerts ]

Overview | Options | Positioning | Volatility | Futures | Replay
```

The brief should occupy a small predictable area. It should not become a chat window that pushes the market data below the fold.

## Premium design changes

- Preserve the dark palette and modular block language. Use typography, spacing and emphasis to establish hierarchy.
- Move routine methodology behind an explanation drawer. Keep stale data and incomplete coverage visible beside the affected value.
- Use approximately 13–14 px readable table text, 11–12 px metadata, and 24–32 px primary numbers in overview mode; retain an optional dense mode.
- Give each color one stable purpose. Calls currently change between green and red across components; option side should not masquerade as bullish/bearish direction.
- Keep symbol, expiry and time context visible while scrolling. Add section navigation and a Focus action on each block.
- Avoid equal-height empty metric panels. Preserve common headers/padding, but allow a compact summary or expanded analysis density.
- Provide labelled chart scales, dates across sessions, a crosshair, and a clear distinction between selected-contract and underlying charts.
- Make mobile an overview/detail experience. A horizontally scrolling desktop table should be an optional advanced view.

**Prompts:** P11–P18.

## Features that can earn retention, sharing and premium value

| Feature | Why someone returns or pays | What to measure |
|---|---|---|
| Saved workspaces and named screens | Avoid rebuilding daily research | Time to resume a saved workflow; weekly reuse |
| Relevant change alerts with evidence | Know when a chosen condition changes without constant checking | Useful-alert feedback, mute rate, duplicate rate, alert-to-review |
| “Since your last visit” briefing | A clear daily reason to return | Time to identify the three relevant changes |
| Historical replay and journal | Review an interpretation and its later outcome | Repeat review sessions; correction rate; retention |
| Shareable evidence snapshots | A useful object to discuss with another trader | Shared-view opens, saves/forks, retained referred users |
| Scenario comparison | Understand payoff and exposure in one place | Completed comparisons and user comprehension |
| Broker embedding/API | Consistent data and workflows inside a broker product | Integration time, reliability, adoption by broker cohorts |

Start with individual traders' daily workflow, then validate broker requirements. Do not use profit screenshots, trading streaks or trade-count incentives as the growth mechanism. Premium value should come from time saved, trusted evidence, retained work and dependable monitoring.

Pricing tiers and prices need customer research. A reasonable hypothesis is: free exploration and limited saved work; paid monitoring, replay, workspace capacity and export; broker plans for embedding, entitlements and service commitments. Data quality labels and honest missing-data handling belong in every tier.

**Prompts:** P19–P26.

## Delivery order

1. **Trust gate:** P01–P10. Fix the signal, time, units, filter and data-state defects before amplifying them with AI or notifications.
2. **Premium core:** P11–P18. Build the index overview, a usable chain and charts, and responsive navigation. Validate with traders before broad rollout.
3. **Repeat-use loop:** P19–P23. Grounded summaries, saved work, change alerts, replay and sharing.
4. **Commercial expansion:** P24–P26. Plans, broker readiness and measured release quality.

These are dependency stages, not calendar estimates. Agree acceptance criteria and inspect the existing capture ownership before scheduling pipeline changes.

## Validation performed and limits

- 159 backend tests passed: `server/tests/test_derivatives.py` and `test_derivatives_series.py`.
- 359 existing frontend logic checks passed: `scripts/check-derivative.cjs`.
- Those passing tests did **not** catch the reproduced label, condition-count or historical-repainting defects. Additional tests are specified in the prompts.
- Read-only production-store samples are saved in `backend-evidence.json`. No scan, data backfill, trade or notification was triggered.
- Visual inspection covered the observed desktop and mobile layout and workflow families above, not every contract, expiry, filter combination, viewport or production-failure scenario.
- UI findings distinguish current observations from proposed functionality. No conversion lift, willingness-to-pay or profitability claim has been measured.

## External product references

Use these as examples of expected workflow completeness, not proof of growth or pricing:

- [TradingView watchlist alerts](https://www.tradingview.com/support/solutions/43000739708-watchlist-alerts-your-trading-edge/): condition monitoring across a list is an established workflow.
- [TradingView watchlist management](https://www.tradingview.com/support/solutions/43000745825-mastering-the-tradingview-watchlists/): organizing saved instruments is part of the daily workspace.
- [Sensibull strategy-builder detail](https://blog.sensibull.com/2023/06/13/strategy-builder-feature-update-see-delta-iv-more-on-leg-picker/): IV, delta and lot-aware display are relevant examples for a contract-to-scenario flow.
- [OIC open-interest FAQ](https://www.optionseducation.org/referencelibrary/faq/general-information): interpreting changes in open interest requires attention to opening/closing on both sides; aggregate OI alone does not identify buyer or seller intent.

See `IMPLEMENTATION_PROMPTS.md` for a separate, actionable prompt for every proposed workstream.
