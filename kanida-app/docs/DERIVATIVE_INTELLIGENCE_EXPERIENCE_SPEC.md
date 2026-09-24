# KANIDA derivative intelligence experience

Date: 2026-09-20. Status: proposed product and architecture specification, not implemented or accepted. Read with DERIVATIVE_PRODUCT_CONTEXT.md and the original Derivative detail questions and answers.docx. The user's latest clarification establishes 15-minute snapshots as the fundamental refresh mechanism.

## Product contract

One connected workspace answers four questions in sequence: where attention belongs, what changed in the latest interval, how it changes the session story, and why the conclusion is supported. Maintain three distinct intelligence outputs: market attention, interval events, session episodes. A weakening interval may coexist with a persistent session pattern. Do not flatten these into one bullish/bearish verdict.

Desktop uses a compact market list beside a larger selected-instrument workspace. Latest interval and session story are separate, vertically related sections in that workspace. Evidence expands beside or below the selected claim. Mobile shows the selected instrument, latest interval and session conclusion in sequence; a market chooser opens a full-screen list. No required horizontal table scrolling or swipe-only access to essential conclusions.

## Market attention

Default row fields: Instrument, Why it stands out, Where. Use a short observational reason, such as call activity extending to another strike or unusually concentrated put turnover, rather than full interpretation. Identify expiry when it differs from the selected/default context. Avoid a persistence column here; duration belongs with the episode. Price and change can be optional professional columns, off by default, and remain available for the selected instrument.

Move contract counts, aggregate OI changes, turnover, maximum volume ratios and rule counts into evidence. Do not combine option premium turnover with futures notional and call it premium. Use a documented attention policy incorporating materiality relative to the instrument, new events, changes to active episodes, coverage and liquidity. Internal ranking weights are not user-facing intelligence. Calibrate by product/regime; do not let the name with the largest chain or smallest denominator dominate automatically.

Show a viewport-appropriate shortlist with search and all-market access. Measured quiet, unavailable and unsupported names are separate states. Selected and watched names remain reachable. Missing evidence must not silently drop an active story. Freeze row order during interaction; offer new results without shifting click targets. A selected instrument never changes automatically because ranks changed.

## Latest interval

Display the actual comparison interval, one leading event headline, two concise sentences describing behavior and evidence, a compact calls/puts relationship, and the relevant strike cluster. Support secondary events on expansion. No fixed requirement to populate a certain number of events when evidence is quiet.

Compare consecutive valid scheduled endpoints of the same contracts. Never call a comparison spanning a missing endpoint a 15-minute change. First snapshot establishes a baseline unless a true opening snapshot exists; previous-close comparisons are explicitly overnight/day comparisons. Preserve source timestamps, quote freshness and market time separately from processing time.

Generate position additions/reductions, buying/writing/covering/unwinding hypotheses when supported, concentration, broadening/narrowing, leader changes, spatial shifts, strengthening/weakening, volatility repricing and call/put/spot/futures disagreement. PCR and Max Pain changes are conditional context, not mandatory tiles or independent votes proving direction.

OI plus price does not establish the initiating participant. Every open contract has a buyer and seller. Price-derived IV is not an independent third confirmation of the same price observation. Evaluate spot movement, time decay, volatility and quote quality before naming participant behavior. With weak evidence, report positions added or reduced and what remains unconfirmed. No claim of trader intent, hedging, institutional identity or strategy from snapshots alone.

## Session intelligence

Maintain multiple episodes per instrument and expiry, not only one winning side. Each episode has stable identity, first observed time, last confirmation, original and current strike membership, additions/exits, leadership history, activity direction, participation, coverage, evidence and invalidation reason. Breadth, pace and lifecycle are separate dimensions: a pattern can broaden while slowing.

Show one main session conclusion plus a secondary competing story when consequential. Preserve origin, major transitions and opening-to-current difference. Render meaningful milestones rather than every refresh. Collapse no-change intervals but keep full snapshots inspectable. A durable open-to-now reference remains available even if an older episode becomes less relevant. End-of-day narrative is a projection of these same episodes.

Persistence is recurring evidence across comparable observations, not elapsed time alone. Distinguish positions remaining outstanding from renewed position additions. Stable OI is not proof that buying or writing continued. Keep elapsed time since first detection, valid supporting observations, covered intervals and gaps separate. Missing observation preserves an unresolved episode but does not confirm continuity. A later recurrence can reference the prior episode without pretending a gap was observed. An absence from a thresholded screener is not evidence that positions closed.

Use product/regime-calibrated noise tolerance and separate criteria for entering, continuing and ending a state to avoid label oscillation. Price moves, expiry effects and coverage changes must not create false reversals. Invalidate only the affected episode; an unwinding event at a former lead strike need not reverse the entire instrument story. A new opposite event is not automatically a reversal of the earlier same-side episode.

## Calls, puts and strike clusters

Analyze contract identity first, clusters second, side relationships third, cross-market context last. Cover both calls and puts on both sides of ATM; the existing call-above/put-below basket is a viewport, not an analytical universe. Use common measured coverage when comparing breadth. Newly captured strikes are not newly participating strikes. Keep historical contract identities when ATM moves.

Show a shared strike axis with separate call/put tracks, current ATM marker, relevant cluster spans and a clearly identified lead where supported. Use discrete nodes for non-contiguous membership so ranges do not imply missing intermediate participation. On mobile, show two compact labeled cluster rows and expand to the full ladder. Cluster selection highlights the same contracts and interval in evidence/chart. Leadership is behavior-specific; allow tied or unclear leadership rather than forcing a winner.

## Evidence

One claim opens three evidence levels: plain-language explanation, compact before/after table of relevant measurements, and professional diagnostics. Every measurement carries contract/expiry, time window, unit, source, raw versus derived status and missingness. Retain contradictory evidence next to supporting evidence. Link claim IDs to evidence IDs and charts. Details can add Greeks, spread/depth, baseline cohort, calculation/model version and source timestamps.

Volume is typically cumulative within a session; derive interval volume from compatible counters and handle resets/corrections. Snapshot OI is a stock; net OI change is not gross opening volume. Prices require last-trade age or usable quotes; stale LTP should not determine activity. Use matched expiries/baskets for PCR comparisons. Preserve actual expiry calendars, corporate actions and lot-size versions. Historical context needs comparable time of day, expiry regime, instrument/moneyness and adequate sample size; never create analogous-event statistics before a valid cohort exists.

## Copy examples

All examples below are illustrative, not observations of current market data.

Market: NIFTY | Call activity extends higher | 23,350–23,450 CE.

Interval: Call activity reaches another strike. Positions increased at 23,450 CE while nearby calls also added interest. Premium behavior is consistent with fresh buying, although the underlying rise explains part of the move.

Session: The morning call buildup is broader, but slowing. First seen at 09:45 around 23,350 CE, it now includes three strikes. New additions eased this interval; the earlier cluster remains intact.

Conflict: Positions increased, but buying is unconfirmed. Premiums rose alongside the underlying, while implied volatility eased. The evidence does not yet distinguish fresh demand from repricing.

No material change: No material change in the latest interval. The earlier call cluster remains in place; new additions are limited.

Coverage gap: The earlier buildup remains the last confirmed state. One interval is missing, so continuity cannot be confirmed.

Leadership: Activity now leads at 23,450 CE; 23,350 CE still participates. Do not imply the same traders moved positions.

## Interaction and motion

Persist one selection tuple: instrument, expiry, session, snapshot version, episode/event, cluster or contract. Selecting a historical event puts all dependent views into the same historical boundary and makes return-to-current explicit. New publications never force a user out of history or close evidence.

Headline and facts appear immediately. Optional sentence-level reveal introduces only newly published explanatory text; never delay the conclusion with character typing. One brief highlight marks a material event, not every refresh. Newly detected activity uses a transient New label; ongoing episodes show Since plus plain-language lifecycle. Animate leadership or cluster changes only between comparable snapshots with stable strike coordinates. No looping pulse implying live tick data.

Keep order stable during reading, cancel obsolete animations on selection, respect reduced motion, and make status distinguishable without color. Screen readers receive one concise update rather than per-character announcements. Background/resumed clients receive the latest coherent state, not a replay of every pulse. Updates must not move focus or scroll position. These are proposed behaviors, not validated timing constants.

## Responsive behavior

Wide desktop: persistent market selector, latest interval above session story, contextual evidence alongside when space permits. Small desktop/tablet: compact selector; evidence replaces a secondary region or opens a sheet instead of squeezing three panes. Phone: market chooser, selected context, latest event, session conclusion, expandable milestones and full-screen evidence with a clear return path. Evidence and interpretation remain identical across device sizes. No separate simplified truth for beginners; advanced users expand the same claims.

## Architecture

Versioned snapshot ingestion -> normalization/quality -> interval feature computation -> event detection -> session episode reducer -> intelligence bundle -> grounded language -> shared publication/cache -> responsive clients.

Use immutable raw records and versioned corrections; deduplicate by source/instrument/contract/snapshot identity. Partition computation by session, instrument and expiry while supporting related cross-expiry events. Finalize with an explicit completeness policy; late/corrected data creates a revision and deterministic replay from the affected boundary. Preserve both as-published history and corrected research history. Backfilled candles cannot silently become original full option snapshots.

Publish market attention, interval event, session story and evidence with a consistent snapshot/version boundary. The client must not combine a newer market panel with an older unlabelled narrative. Historical views exclude all future observations and future-derived baselines. Retain an earlier readable bundle during failures with explicit stale status; never substitute invented explanations.

Compute intelligence once per instrument/expiry/snapshot/rule version and language once per canonical output/locale, then distribute to many users. Personalization selects/ranks existing facts, rather than recomputing market truth. Use asynchronous workers, durable queues, idempotency, bounded retries, precomputed read models, caching and subscription fan-out; coalesce updates for slow clients and avoid a synchronized million-client refresh burst. Serve deterministic template copy if generation fails. Apply market-data entitlements to both raw evidence and derived distribution. Provider contracts and rate limits are a business dependency, not solved by adding servers.

Persist structured claims with supporting and contradicting evidence, uncertainty, source quality, event/episode IDs, baseline definitions, model/rule versions and lifecycle transitions. The language layer cannot select unsupported strikes, fabricate durations or alter numeric facts. Validate generated claims against the bundle. Observe ingestion completeness, snapshot age, processing lag, revision rate, contradiction/unsupported-claim rates and cache/serving performance. Establish capacity and latency budgets using measured universe size and load tests rather than arbitrary infrastructure claims.

## Current gaps and delivery order

Reviewed frontend shows SIGNAL_WINDOW_MINUTES=60 and maximum baseline gap 120; this is not a true consecutive 15-minute event layer. summary.ts uses OI and price, takes lead by absolute OI change, applies 1.25/0.75 pace ratios and retains first-to-last elapsed duration across resumed gaps. The ten-slot grid limits the analytical view. Live preview previously showed 15:45 screener and 11:30 narrative. These are observed implementation limitations, not a complete backend audit.

First establish snapshot boundaries, provenance, missingness and consecutive interval features. Next implement episode tracking and calibrated event definitions with realistic fixtures. Then deliver the connected responsive interface and evidence linking. Add historical comparisons after comparable history and validation exist. Validate widening plus slowing, ATM migration without false spread, missing then resumed observations, conflicting sides, stable OI without renewed flow, expiry changes, low-base spikes, revisions, stale quotes, historical no-lookahead and large fan-out. Do not approve cosmetic completion while the evidence contract remains incorrect.

## Primary sources for quantitative boundaries

OIC general information explains that OI counts outstanding contracts with both buyer and seller and does not itself establish direction: https://www.optionseducation.org/referencelibrary/faq/general-information . Its US reporting cadence is not a statement about NSE intraday feeds.

NSE option chain reference: https://www.nseindia.com/option-chain . Verify actual licensed provider field definitions and timing before implementation; a public option-chain page is not the production feed contract.
