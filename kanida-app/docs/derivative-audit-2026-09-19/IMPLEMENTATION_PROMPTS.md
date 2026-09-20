# Derivatives implementation prompt pack

Audit: 19 September 2026. Companion: [assessment](README.md) and [read-only backend evidence](backend-evidence.json).

These are **26 separate implementation workstreams**, not changes already made. P01–P10 repair trust; P11–P18 improve the core experience; P19–P23 add repeat-use and sharing; P24–P26 cover commercial validation and release quality. Run dependency-aware, bounded changes rather than one giant rewrite.

## Shared instructions to include with every prompt

> Work in the existing KANIDA application. Read applicable AGENTS.md and exact versioned framework documentation before code changes. Read this audit, then verify the current implementation because other development may have progressed. Preserve unrelated work. Reuse the modular block design and existing components where appropriate. Use actual stored data and explicit unavailable states; never fabricate market values, silently mix instruments/times/expiries, or represent an inference as an observed fact. Implement the specified scope with meaningful regression checks. Report changed files, behavior, validation and remaining limits. Do not initiate a production backfill, external notification campaign or order execution as part of a UI/data correctness change.

Each section below is a copy-ready task when combined with these shared instructions. Paths are relative to the application unless they refer to the repository-level market-data package. Source names are starting points, not a substitute for tracing current behavior.

## P01 — One coherent instrument, expiry and time context

**Order:** Critical · first dependency.

**Why:** The inspected max-pain headline combined a 15:45 strike with an 11:30 spot and distance. Chain, charts and session panels do not share one as-of context.

**Inspect:** src/derivative/index.tsx; model.ts; server/kanida_pilot/derivatives.py (maxpain_series, indices).

**Implementation prompt:**

> Define a typed analysis context containing instrument identity, expiry, session date, as-of timestamp/timezone and live versus historical mode. Pass it through every dependent request. Resolve each metric at or before that boundary with its actual observation time; do not silently forward-fill missing values. Calculate distances only from compatible observations. Standardize distance as max-pain strike minus spot and label its direction. Compute historical DTE relative to the selected session using a documented day convention. Preserve stale-request guards and cancel obsolete transport requests where supported.
>
> Acceptance criteria: Reproduce the 11:30/15:45 mismatch: the newer missing-spot pair must show unavailable distance. Rapid symbol/expiry changes cannot mix panels. Test dates, timezones, expiry-day DTE and opposite distance signs. Show actual observation time whenever it differs from the selected boundary.

## P02 — Derivative capture health and truthful empty states

**Order:** Critical · alongside P01.

**Why:** 15:45 contract metrics lacked spot and premium; the resulting empty screener looked like no contracts qualified.

**Inspect:** server/kanida_pilot/derivatives.py; market_data/derivatives; src/derivative/index.tsx and frame.tsx.

**Implementation prompt:**

> Return capture source, latest attempted/available/complete readings, required-field coverage and an explicit eligibility reason. Distinguish missing capture, partial capture, valid zero results, filters excluding all rows and request failure. Show a compact derivative-specific status beside affected panels. Offer a clearly labelled latest-complete reading without silently substituting it. Keep partial records visible where their available fields support a valid analysis.
>
> Acceptance criteria: Use the recorded 11:30 and 15:45 cases. Verify partial data cannot receive a healthy-complete badge or imply quiet trading. A valid complete reading with no eligible rows must still say no matches. Add failure/retry and recovery tests without initiating an unrequested production backfill.

## P03 — Make historical signals stable as data arrives

**Order:** Critical · before replay or alerts.

**Why:** Appending later observations changed an earlier signal from bearish to bullish because normalization used the whole available session.

**Inspect:** src/derivative/logic.ts (signalRows, sideChange); scripts/check-derivative.cjs.

**Implementation prompt:**

> Calculate each historical signal only from information available at its timestamp. Use fixed documented thresholds or an explicitly causal prior-data baseline. A one-hour comparison must use actual timestamps and sufficient elapsed history, not merely up to four rows. For replay, select the strike basket using information available then, or label a current-basket retrospective view distinctly. Version calculation rules.
>
> Acceptance criteria: Add append-invariance tests: adding an extreme future observation must leave every earlier row unchanged. Test gaps, early-session insufficient history, duplicate timestamps, expiry changes and historical basket membership. Preserve a reproduction demonstrating the old failure and the corrected result.

## P04 — Explain positioning without claiming knowledge of trader intent

**Order:** Critical · after P03.

**Why:** marketSignal ignores price direction; flow copy presents inferred buyer/seller activity as fact.

**Inspect:** src/derivative/logic.ts (marketSignal, FLOW_LABELS, strength calculations); signal table UI.

**Implementation prompt:**

> Separate observed price/OI changes from conventional positioning interpretations. Review the full price-up/down/flat and OI-up/down/flat truth table for calls, puts and futures. Use neutral wording where evidence is mixed; do not infer initiating buyer/seller identity from aggregate OHLC/OI. Show calculation window and rule explanation. Treat strength as a defined magnitude category, not predictive confidence or win probability. Avoid summing unrelated option prices as if they formed a meaningful tradable price series.
>
> Acceptance criteria: Test every truth-table combination including put price rising with rising OI. Contradictory evidence must remain visible. Verify every directional label has an inspectable rule and that no percentage is presented as forecast accuracy without measured validation.

## P05 — Normalize build-up values end to end

**Order:** Critical · small independent fix.

**Why:** All six build-up filter choices rendered as dashes because human labels were passed to a snake_case lookup.

**Inspect:** src/derivative/logic.ts (BUILDUP_LABELS, BUILDUP_VALUES); FilterDialog.tsx; server derivative serializers.

**Implementation prompt:**

> Introduce one canonical enum with separate display labels. Normalize existing stored human-readable values at a boundary without destructive database rewriting. Distinguish flat, missing and unknown values. Use the same IDs in filters, API responses, table cells and selected-futures summaries.
>
> Acceptance criteria: Every existing stored value renders correctly. All six filter choices are readable and round-trip to the intended server condition. Unknown values display a useful fallback and are observable; they must not silently become flat.

## P06 — Count real unusual-activity conditions and explain ranking

**Order:** Critical · before AI summaries.

**Why:** 124 distinct numeric reason strings represented only two rule families, yet the UI advertised three conditions.

**Inspect:** market_data/derivatives/metrics.py; server/kanida_pilot/derivatives.py (_screener_groups); src/derivative/logic.ts (unusualDegree).

**Implementation prompt:**

> Represent a trigger as rule ID, version, actual value, comparator, threshold, baseline and sample count. Count distinct rule IDs separately from triggered contracts and observation count. Rank using explicitly documented criteria and display the actual active sort. Replace concatenated reason prose with concise badges and an evidence drawer listing affected contracts.
>
> Acceptance criteria: Numeric variants of one rule count once as a condition family. The inspected two-family case cannot display three conditions. Ranking is deterministic with tie-breakers; accessible cell text remains concise and full evidence remains available on demand.

## P07 — Make every filter mean exactly what it says

**Order:** High · depends on P05.

**Why:** Strict operators become inclusive, excluding CE becomes PE-only, and scope/expiry propagation is inconsistent.

**Inspect:** src/derivative/logic.ts (FILTER_COLUMNS, screenerParam, rulesToFilters); FilterDialog.tsx; server filter validation.

**Implementation prompt:**

> Define one filter schema covering type, operators, unit, applicability and scope. Implement gt/gte/lt/lte explicitly without arbitrary currency increments. Implement not-CE across the actual instrument universe. Support negative OI thresholds and ranges where the API supports them. Propagate expiry independently of an underlying filter. Clearly separate screener eligibility rules from selected-instrument analysis context and watchlist scope. Keep draft/apply/cancel behavior.
>
> Acceptance criteria: Build a frontend/backend contract matrix for all 13 fields. Test exact boundary values, futures in exclusions, negative values, contradictory rules, expiry-only filters and watchlist scope. Applied chips accurately describe affected panels and removing a rule restores the expected universe.

## P08 — Make IV freshness and model quality visible

**Order:** High · depends on P01/P02.

**Why:** A tile said solved at the latest reading despite its last valid value ending at 11:30; rejected leg observations were labelled ambiguously.

**Inspect:** src/derivative/IvGridSection.tsx; SessionBlocks.tsx; server IV series implementation.

**Implementation prompt:**

> Return selected reading, last valid reading, valid/total slots and rejection reasons per leg. A last-valid value must carry its age rather than a latest claim. Aggregate the ten-strike/ATM requests where sensible to avoid repeated equivalent computation. Document model inputs and assumptions behind an expandable explanation. Evaluate a forward/dividend-aware model only when dependable inputs exist; never invent them or silently change the model.
>
> Acceptance criteria: Test trailing missing observations, one missing leg, invalid prices, expiry boundary and a fully valid series. In the inspected case show 9/26 valid slots and distinguish rejected leg observations from session slots. Verify batched and existing numerical results agree for identical inputs.

## P09 — Correct units, turnover and baseline context

**Order:** High · required for credible comparisons.

**Why:** OI/volume units were called contracts; options premium and futures turnover were mixed; huge ratios lacked denominator context.

**Inspect:** market_data/derivatives/config.py and metrics.py; server derivative serializers; src/derivative formatters and tables.

**Implementation prompt:**

> Create explicit numeric field metadata: unit, lot size, scale and semantic meaning. Convert units to lots/contracts only using the correct instrument metadata and disclose rounding. Separate options premium turnover from futures notional turnover. Show baseline value, sampling window and sample count for unusual-volume ratios. Introduce reliability flags for weak baselines using documented criteria validated on actual data. Add spread/quote age/depth only when available, with unavailable states otherwise.
>
> Acceptance criteria: Verify sample instruments with different lot sizes and lot-size changes. Monetary formatting and exports agree. No mixed aggregate is labelled option premium. A ratio with a tiny denominator can be inspected and is not automatically advertised as strong evidence.

## P10 — Reconcile index and futures data readers

**Order:** High · before redesigned overview.

**Why:** Index fields were empty while session routes had values, and the cross-market futures list was empty while selected futures charts were populated.

**Inspect:** server/kanida_pilot/derivatives.py (indices, futures and selected series); underlying snapshot and metrics readers.

**Implementation prompt:**

> Trace source selection, session/expiry resolution and eligibility rules across these routes. Define canonical metric readers and documented eligibility policies. Reuse coherent available measurements or explain why a view excludes them. Return source and exclusion reasons; do not patch gaps with zero or unrelated snapshots.
>
> Acceptance criteria: For the same context, shared metrics agree across overview, selected panels and lists. Test an available future excluded by a list policy separately from no captured future. Add reconciliation fixtures based on the captured audit case.

## P11 — Build an index overview with an obvious reading order

**Order:** Premium core · after trust fixes.

**Why:** Selecting an index opens a long collection of equally prominent blocks instead of a clear answer to what changed.

**Inspect:** src/derivative/index.tsx; frame.tsx; existing linked selection state.

**Implementation prompt:**

> Preserve modular blocks and one screener row per instrument. Add a compact persistent context bar with index/stock, expiry, session/as-of and data quality. Order the overview: identity/spot; short evidence brief; underlying chart; three-to-five relevant changes; ATM option chain; detailed analysis tabs. Default to the selected underlying chart. Preserve context in a shareable URL with validation. Keep max pain as secondary context. Use a deterministic brief until P19 is ready.
>
> Acceptance criteria: A trader can identify instrument, expiry, time, data quality and key change in the first viewport. Selecting another index updates every section coherently. Back/forward and direct links restore context. The full-height screener does not consume the persistent header.

## P12 — Make screening fast and explain every result

**Order:** Premium core · P06/P07/P09.

**Why:** Dense columns and opaque ranking make it difficult to understand why an instrument appears.

**Inspect:** src/derivative/index.tsx; Table.tsx; FilterDialog.tsx; screener endpoint.

**Implementation prompt:**

> Provide a compact default column set, optional dense mode, stable user-selected sort and named column presets. Show why each row matched as a short evidence summary with contract drilldown. Label the real ranking. Distinguish searching eligible results from looking up any supported symbol; provide an explicit lookup path if needed. Keep instrument-level grouping with counts for matching contracts.
>
> Acceptance criteria: Users can explain a row's inclusion without reading long hidden prose. Sort labels match results. An unsupported symbol, a supported symbol outside current filters and a genuine matching result have distinct states. Saved column choices survive reload.

## P13 — Turn option-chain clicks into a complete inspection flow

**Order:** Premium core · P01/P09/P11.

**Why:** The chain can select a contract, but time, chart destination and execution context are not sufficiently clear.

**Inspect:** src/derivative/ChainWidget.tsx; ChartTile.tsx; contract model and API.

**Implementation prompt:**

> Center the chain around ATM when spot exists, provide expiry and strike-window controls, and preserve selection on sensible context changes. On click show an inspection drawer with exact contract identity, price/time, spread and liquidity if available, IV/Greeks with provenance, lot size and synchronized chart. Add a scenario entry point with explicit legs, quantities, expiry payoff and cost assumptions. Inventory missing backend inputs before implementing calculations.
>
> Acceptance criteria: Inspect CE and PE contracts across expiries without confusing the underlying chart with a contract chart. Missing spot must not claim an ATM strike. Scenario outputs reconcile with tested pricing/payoff fixtures; no order is placed by the inspection flow.

## P14 — Make charts readable and instrument-correct

**Order:** Premium core · P01.

**Why:** Price/OI lines lack clear scale interpretation, labels omit dates across sessions, and Edit this chart routes to the underlying daily chart.

**Inspect:** src/derivative/ChartTile.tsx; SessionPanel.tsx; chart routing helpers.

**Implementation prompt:**

> Label price and OI axes with units, distinguish series visually, show full date/time on crosshair, and mark session boundaries and missing data. Give every chart an exact instrument/timeframe/expiry header. Route contract and underlying actions correctly and name each action for its destination. Synchronize hover/context where comparisons are intended without implying identical scales.
>
> Acceptance criteria: Test multi-session series, gaps, extreme scale differences and selected option versus underlying routes. A viewer can identify which line and scale belongs to price or OI. Keyboard and touch users can access the values exposed by hover.

## P15 — Redesign strike OI for symmetric comparison

**Order:** Premium core · P01/P09/P17.

**Why:** Calls wrapped 4+1 while puts fell below the fold; strike tables started far from ATM and side colors reversed between widgets.

**Inspect:** src/derivative/OiGridSection.tsx; OI-by-strike chart/table; grid layout helpers.

**Implementation prompt:**

> Use a symmetric call/strike/put comparison centered around ATM, with a selectable strike window. Offer compact heatmap/table and expanded small-multiple views using the same data. Distinguish total OI, session change and interval change. Provide numeric scales, units, as-of and hover detail. Use stable side identity colors separately from signed change colors. Allow a manual center when spot is unavailable.
>
> Acceptance criteria: No orphan 4+1 layout at target desktop widths. Calls and puts for a strike are visible together. ATM centering and manual centering work. Heatmap, table and expanded chart reconcile on value, interval and sign.

## P16 — Give PCR, max pain and futures useful context

**Order:** Premium core · P01/P04/P10.

**Why:** Large sparse panels and small metric cards require users to infer windows and meaning; last-hour and day changes can appear contradictory.

**Inspect:** src/derivative/SessionBlocks.tsx; SessionPanel.tsx; futures/index sections.

**Implementation prompt:**

> Create compact summaries with value, unit, observation time and explicitly named comparison window. Expand into a chart and methodology only when requested. For PCR specify OI/volume basis and included contracts; for max pain show coherent distance and limitations; for futures separate observed price/OI from interpretation. Add historical percentile/range only with a sufficient comparable history and clear sample definition.
>
> Acceptance criteria: Every change number states its window. A flat last hour and negative day change read as compatible statements. Missing benchmark history stays unavailable. No metric alone is described as a validated forecast.

## P17 — Create a cohesive premium visual system

**Order:** Premium core · can prototype independently.

**Why:** Tiny typography, repeated explanatory text and equal-weight panels obscure useful data despite a consistent dark palette.

**Inspect:** src/derivative/frame.tsx; Table.tsx; shared typography/color/spacing components.

**Implementation prompt:**

> Define reusable tokens for typography, spacing, contrast, borders, surfaces and semantic colors. Target readable 13–14 px table text and 11–12 px metadata in normal mode, with optional density controls. Keep common block headers and modular identity while permitting compact and expanded bodies. Move routine methodology into contextual drawers; keep warnings beside affected facts. Replace internal pilot/worker wording with user-facing language. Apply the system across all derivative panels.
>
> Acceptance criteria: Review populated, sparse, loading and error states at several widths. Side colors and direction colors have consistent meanings. Critical values do not depend on color alone. Remove duplicated prose without hiding time, quality or methodological caveats needed to interpret a number.

## P18 — Complete mobile and accessible interaction

**Order:** Premium core · P11/P17.

**Why:** Mobile's first screen is dominated by prose and a wide table; expand/close/restore and hover need further live verification.

**Inspect:** src/derivative/frame.tsx; Table.tsx; FilterDialog.tsx; hover and widget visibility state.

**Implementation prompt:**

> Design mobile as overview then focused detail, with an optional full table. Reduce competing nested scroll areas. Implement accessible names, focus order, keyboard chart inspection, visible focus, modal focus return and touch-sized controls. Verify expand, close, per-block restoration and restore-all across reload. Respect reduced motion. Preserve useful state through rotation and resizing.
>
> Acceptance criteria: Exercise 390px mobile and representative tablet/desktop widths, keyboard-only navigation and a screen reader. Verify all widget controls interactively because the audit's browser failure left them unverified. No essential action relies only on hover or color; dialog cancellation preserves applied filters.

## P19 — Add an evidence-grounded AI summary box

**Order:** Retention · after P01–P10.

**Why:** A concise brief could reduce interpretation effort, but summarizing inconsistent data would amplify current defects.

**Inspect:** New derivative brief service/component integrated with the canonical analysis context.

**Implementation prompt:**

> Build a small What changed / Evidence / Conflicting evidence / Watch next brief. Generate a structured deterministic fact packet with metric IDs, values, comparison windows, timestamps, coverage and rule versions. Let the model verbalize only this packet, linking each numeric claim to an evidence drawer. Require uncertainty and mixed evidence where appropriate. Cache by context and fact version; provide a deterministic fallback and feedback controls. Exclude imperative trade recommendations and unsupported claims about who is buying or selling.
>
> Acceptance criteria: Evaluate complete, stale, partial and contradictory fixtures. Every number must match an input fact; no omitted critical freshness warning. Compare trader comprehension and time-to-insight against the non-AI brief. Define latency/cost targets from actual measurements and fail safely to the factual fallback.

## P20 — Save workspaces and make returning effortless

**Order:** Retention · P07/P11.

**Why:** Users should resume a useful research workflow rather than rebuild filters and blocks every session.

**Inspect:** Workspace persistence, screener presets, widget visibility and analysis context.

**Implementation prompt:**

> Add named screens and workspaces with filters, column settings, preferred instruments, block arrangement and density. Use a short first-use setup around the trader's actual market and workflow. Separate a saved live view from a pinned historical snapshot. Provide clear reset/restore and conflict handling for local/server sync. Add a since-last-visit entry point that respects available data.
>
> Acceptance criteria: Reload and cross-device restoration preserve supported settings. Historical snapshots do not silently become live. A returning user resumes a chosen workspace in one action. Measure resume time and repeated weekly use rather than configuration clicks.

## P21 — Add useful change monitoring and alerts

**Order:** Retention · P01–P07/P20.

**Why:** Saved conditions become valuable when users can learn about meaningful changes without repeatedly checking the page.

**Inspect:** New derivative change feed and alert service using canonical filter/rule schemas.

**Implementation prompt:**

> Provide opt-in alerts for explicit conditions, crossings and meaningful changes on a saved universe. Store condition version, context and triggering evidence. Deduplicate, handle expiry rollover explicitly, support cooldown/quiet hours and stop directional alerts on inadequate data. Present an in-app change feed first; make external channels separately configurable. Include why it triggered and one-click review of the exact snapshot.
>
> Acceptance criteria: Test repeated readings, missing/recovered capture, out-of-order events, session boundaries, expired contracts and edited conditions. One event must not create a notification storm. Track useful-alert feedback, mute rate and alert-to-review, with deliverability and latency measured separately.

## P22 — Build causal replay and a research journal

**Order:** Retention · P01/P03/P04.

**Why:** Reviewable historical evidence can make the product useful beyond the current market snapshot.

**Inspect:** Historical derivative APIs, event snapshots and new replay/journal UI.

**Implementation prompt:**

> Allow a user to inspect what was known at a historical timestamp and record an interpretation. Store immutable context, rules and evidence versions. Measure later underlying returns and option outcomes separately over explicitly chosen horizons; option results require available contract prices, costs and expiry handling. Show incomplete horizons and missing data. Keep the latest five qualifying events chronological, with outcome distributions and sample counts when aggregating.
>
> Acceptance criteria: Replay never uses future-normalized signals or today's basket as if known then. Verify entry/reference and exit timing, gaps, expired contracts, overlapping events and costs. Clicking an event reproduces its evidence. Do not label exploratory event outcomes a tradable backtest without defined execution rules.

## P23 — Make evidence easy to share and inspect

**Order:** Growth · P01/P06/P19/P20.

**Why:** A useful market snapshot is a stronger sharing object than an unexplained screenshot.

**Inspect:** New snapshot serializer, public/private share route and preview card.

**Implementation prompt:**

> Create a shareable snapshot with instrument, expiry, timestamp, key facts, chart and methodology link. Make the receiver able to inspect evidence and open a copy of the view. Clearly distinguish historical snapshot from live data. Keep account information, private notes, vendor credentials and private workspace details out of the payload. Enforce access and data-display entitlements; support revocation where applicable.
>
> Acceptance criteria: Test private-to-public boundaries and tampered snapshot identifiers. Preview images match the stored snapshot. Track opens, saved/forked views and retained referred users; do not optimize only raw shares. A stale snapshot cannot look like current market data.

## P24 — Validate premium packaging and willingness to pay

**Order:** Commercial · after a usable core.

**Why:** Attractive screens alone do not establish premium value or an appropriate price.

**Inspect:** Product research plan, entitlement model and plan UI.

**Implementation prompt:**

> Propose testable packages around saved-work capacity, monitoring, replay, exports and team/broker workflows. Keep honest quality labels and basic interpretation clarity available to all users. Interview target trader segments around time saved and current alternatives, then test offers with explicit success criteria. Implement entitlements centrally with clear limits and graceful downgrade behavior. Do not invent a validated price point.
>
> Acceptance criteria: Deliver a feature-to-customer-value matrix, research script and pricing experiment plan before claiming demand. Verify entitlements in API and UI, export permissions and downgrade retention of user work. Measure retained paid use and cancellations alongside conversion.

## P25 — Prepare a dependable broker integration surface

**Order:** Commercial · after canonical contracts.

**Why:** Broker distribution requires consistent semantics and operational reliability as well as attractive widgets.

**Inspect:** Derivative API contracts, embeddable views, authentication/entitlements and observability.

**Implementation prompt:**

> Define a versioned read API and embeddable overview/chain widgets with tenant styling, exact instrument identifiers, time/quality fields and documented error states. Review data redistribution rights and partner authentication requirements as concrete integration dependencies. Add tenant isolation, rate policies, observability and measured service objectives. Scope trading/order execution as a separate explicitly specified integration, not an implicit consequence of embedding research.
>
> Acceptance criteria: A reference integration reproduces the same numbers as the product for the same context. Test tenant separation, revoked entitlements, partial data, unavailable service and schema evolution. Measure integration time and reliability in a partner pilot before promising an SLA.

## P26 — Release with correctness and usability evidence

**Order:** Release gate · applies across all stages.

**Why:** Existing 159 backend tests and 359 frontend checks passed despite the reproduced defects; adoption claims remain unmeasured.

**Inspect:** server/tests/test_derivatives*.py; scripts/check-derivative.cjs; UI workflow tests and product telemetry.

**Implementation prompt:**

> Add contract and regression tests from P01–P10 plus targeted end-to-end tests for instrument/expiry/time/filter selection, chart inspection, widget restoration and responsive layouts. Use representative captured fixtures with no secrets. Benchmark first useful render, context changes, request volume and summary generation under realistic load. Run trader task studies for overview comprehension and evidence inspection. Instrument saved-view reuse, useful-alert rate, replay review, referred-user retention and paid retention without recording sensitive notes.
>
> Acceptance criteria: Publish a release checklist with measured baselines, proposed targets and owners. Block release on mixed-context values, repainting or fabricated missing data. Separate correctness, usability, performance and commercial results. Keep a known-limitations list and recheck previously source-only controls before calling the workflow fully verified.

## Practical first batch

Start with P01, P02, P03, P05 and P06, then complete the remaining trust fixes. Prototype P11/P17 visually in parallel if useful, but connect AI summaries and notifications only after their underlying evidence is reliable. Validate the first complete index workflow before applying the design across stock derivatives and all advanced panels.

The audit did not execute these prompts or certify every UI control. The workflow matrix in the assessment identifies live-verified versus source-reviewed areas.

