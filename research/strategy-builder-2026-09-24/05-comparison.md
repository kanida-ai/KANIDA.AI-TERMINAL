# Consolidated comparison — evidence before feature claims

Read after the individual reports. Source references such as **S06**, **R05** and **P09** point to screen IDs in those reports. These are qualitative expert-review findings, not moderated usability research or a representative user study.

## Research coverage

| App | Direct access and completion boundary |
|---|---|
| Sensibull | Broad strategy-path inspection; template/custom analysis, discovery, existing definitions, drafts and broker review. Live orders and destructive/persistent mutations not completed. Some expired-state and availability failures observed. |
| TrendSpider | Chart/options workspace inspected. Options chain blocked by exchange agreement; moved on immediately per user instruction. Downstream builder/backtest/alerts unverified. |
| Rupeezy/Astha | Rupeezy-branded supplied app inspected: store, prediction builder, analysis, order review, baskets, portfolio auto-exit and expert cards. Recommendation rationale terms-gated. No separate Astha interface claimed. |
| 5paisa | Template/custom builder, scenario test, analytics, order pad, basket/VTT entry, premium charts and idea/history discovery inspected. No active expert basket at inspection time. |

No competitor mobile UI was inspected. KANIDA mobile behavior is a proposed design, not reverse-engineered evidence. No full competitor backtest, live fill, dedicated roll, or strategy-alert delivery was completed. A control's existence and its end-to-end success are separate claims.

## What each does best in the observed evidence

**Sensibull: deepest connected research-to-tracking experience.** Its strongest combination is thesis discovery, editable leg analysis, detailed scenarios and draft position/order tracking (S04–S09, S13–S18). Easy Options explains why a structure suits a view; Wizard offers granular filtering. It is the strongest inspected reference for KANIDA's analysis depth and paper-ledger concept.

**TrendSpider: chart-context composition, with limited confidence.** The visible workspace keeps chart, market list, expiry/DTE and option data together (T01). Strategy Tester and Alerts/Bots are visible entry points, but this session cannot substantiate that it is best at F&O backtesting or automated strategies. Borrow the contextual composition only; specify testing and monitoring independently.

**Rupeezy: shortest observed family-to-order-review progression.** Store recipe → concrete variants → multiplier → compact analysis → review is coherent (R02–R06). Explicit funds shortage and the separate P&L-exit scope are useful. Its prediction “Builder” should inspire discovery, not the name or the limits of KANIDA's custom editor.

**5paisa: broker-terminal construction plus adjacent premium/idea context.** It supports direct legs, separate entry/LTP, scenario P&L and a detailed native order pad (P03–P09). Dedicated straddle/strangle charts and expandable historical multi-leg recommendations add context (P12–P15). Its budget/exit customization is useful if KANIDA clearly separates planned stop-loss from structural maximum loss.

## Feature evidence matrix

**T** tested transition/result; **V** visible control only; **G** access/data gated; **U** unverified/not located in inspected scope; **E** observed unavailable/error state. U is not a product-wide absence claim.

| Capability | Sensibull | TrendSpider | Rupeezy | 5paisa |
|---|---|---|---|---|
| Custom option-chain legs | T | G | U; basket search V | T |
| CE/PE multi-leg combination | T | G | T generated spread | T |
| Template/default catalog | T | U | T, 12 families | T |
| Thesis-based discovery | T Wizard/Easy | U | T prediction | T Quick Option Trade |
| Expert recommendation route | V home/community; not equivalent to advice | U | T cards; G rationale | T cards/history; E active baskets |
| Expiry selector | V | V/G | V | V dropdown inspected |
| Manual strike and entry edits | V plus chain T | G | U in strategy cards | V plus chain T |
| Ratio-preserving size multiplier | V | U | T | V |
| Payoff graph | T | G | T | T |
| Leg P&L table | T | U | U | T |
| Leg Greeks | T | G | U | T |
| Aggregate Greeks | T | U | T | T totals |
| Margin/funds | T | U | T | T |
| Max gain/loss/breakeven | T | U | T | T |
| POP | T | U | U | T |
| Spot/time/IV scenarios | V controls; leg recalculation T | U | T DTE only | T spot; V time/IV |
| Saved strategy definitions | T load/dialog; E old definition | U | U | U |
| Duplicate definition | V Save As | U | U | U |
| Saved order baskets | V broker basket, separate save not established | U | T existing/creation form | T empty/creation form |
| Virtual position/order ledger | T draft inspection | U | U | U; VTT not assumed virtual |
| Premium history | T Strategy Chart | V underlying chart | U | T straddle chart |
| Rule-based options backtest | U | V tester entry, U options scope | U | U |
| Edit/adjust existing tracked legs | T draft form | U | U no positions | U |
| Dedicated coordinated roll | U | U | U | U |
| Execution preparation | T Zerodha basket | V trading entry | T native review | T native order pad |
| Live submit/fill/reject | U | U | U | U |
| Strategy-scoped alerts | U | V alerts entry, scope U | U; account P&L exit V | U; VTT entry V |
| Mobile layout | U | U | U | U |

## Common features and meaningful differences

Across the **three accessible Indian-options workflows**, underlying/expiry context, ready-made structures, buy/sell multi-leg candidates, quantity sizing, payoff, maximum gain/loss, breakeven, margin/funds and an execution-review route were present. Across **all four**, only underlying market context and options expiry/chain entry are established; the gated TrendSpider session prevents a stronger all-four claim.

| Design dimension | Differences | KANIDA decision |
|---|---|---|
| Entry mental model | Sensibull has multiple beginner/advanced routes; Rupeezy separates store/prediction; 5paisa separates builder/ideas | One entry with Build from scratch, Use a template, Find a strategy |
| Meaning of builder | Free-form legs in Sensibull/5paisa; generated suggestions in Rupeezy | Use Build for contract editing, Discover for candidate generation |
| Persistence | Sensibull definitions versus drafts; Rupeezy/5paisa baskets separate from strategy research | One strategy identity, explicit revisions and Research/Paper/Live contexts |
| Scenario depth | Spot/time/per-strike IV in Sensibull/5paisa; DTE modal in Rupeezy | Progressive disclosure: basic spot/time, expandable volatility assumptions |
| Execution product | Broker integration in Sensibull; native broker UI in Rupeezy/5paisa | Adapter-based broker review with consistent core semantics |
| Price presentation | LTP, entry and theoretical target mixed at different layers | Explicit quote source, as-of, execution estimate and user overrides |
| Chart/history | Premium charts and recommendation history exist, not established full backtests | Separate Scenario, Replay and Rule Backtest features and labels |
| Monitoring | Sensibull drafts; Rupeezy intraday account P&L exit; other entry points unverified | Explicit per-strategy alerts first; automated orders separately authorized |

## Missing capabilities in the observed workflows

These are opportunities or evidence gaps, not blanket claims that competitors do not support them:

1. Durable connection between thesis, exact research revision, paper fills, broker execution and later adjustments.
2. Consistent exchange-date/instrument identity across analysis and order review.
3. Transparent probability, Greek units, valuation assumptions, quote age and liquidity treatment.
4. First-class invalid/expired-contract recovery that preserves historical definitions instead of zeroing fields.
5. Confirmed full historical multi-leg testing with reproducible data, cost/slippage and fill assumptions.
6. Confirmed strategy-scoped monitoring with trigger history, notification states and clear automation boundaries.
7. A before/after adjustment or roll comparison including realized P&L, residual risk and execution ordering.
8. Visible handling of partial execution, unknown broker outcome and reconciliation.
9. Discover shortlist comparison using risk budget, liquidity and rationale rather than long undifferentiated lists.
10. A verified mobile editing/analysis/review journey; none was inspected here.

## UX problems grounded in the session

| Observation | User consequence | Requirement |
|---|---|---|
| Sensibull old definition loaded blank expiry/zero strikes and error (S10) | Saved work loses understandable identity | Keep original terms, show expired/unresolvable state, offer explicit remap |
| Date labels differed across analysis/review in Sensibull/5paisa (S12, P09) | User cannot confidently confirm selected contract | One canonical expiry display, broker-symbol preview, mismatch hard block |
| Rupeezy card/modal breakevens differed (R04–R05) | Analysis appears inconsistent | Revision-bound calculations with same quote snapshot |
| Loading looked like empty portfolios/orders (S17–S18) | False impression of missing data | Distinct loading versus genuine empty state |
| MultiStrike OI requests already-present selection (P08) | User stuck with no recovery path | Per-widget state contract and actionable retry/fix |
| Rupeezy generated hundreds of repeated cards (R08) | High comparison effort | Small ranked shortlist, discriminating labels, filters and compare |
| Settings or table switches obscure units (S07–S08, P07) | Per-unit and full-strategy exposures confused | Units on every metric, consistent scaling control |
| “Low Risk” or “Defensive” compresses different risks (S15, P13) | Lower premium can be confused with safer outcome | Maximum loss, probability/model assumptions and liquidity shown separately |
| Saved strategy/basket/draft vocabulary fragmented | User cannot tell what is saved or tracked | Clear object lifecycle and explicit status |
| Bulk market actions beside per-leg limit choices | User may submit different execution semantics | Exact order policy summary and separate confirmed action |

## Ideas to combine

- Sensibull's leg/scenario depth + Rupeezy's concise staged selection + 5paisa's explicit order fields.
- Sensibull's explanatory archetypes + Rupeezy's visible leg recipes + 5paisa's historical basket provenance.
- Sensibull's draft order ledger + KANIDA-owned version history and broker reconciliation.
- TrendSpider's visible chart-context arrangement + optional, collapsible KANIDA premium/OI context.
- Rupeezy's upfront funds check + explicit initial/peak/post-hedge margins and partial-fill handling specified for KANIDA.

## Features to simplify

- Replace multiple discovery names with one Discover workflow, with optional advanced filters.
- Replace separate saved-definition, draft and basket silos with a strategy library; keep execution and paper ledgers separate underneath.
- Show four primary risk facts first: maximum loss, maximum profit, breakeven(s), estimated required funds. Expose POP/Greeks/IV in Analysis without hiding essential liquidity warnings.
- Make target spot/time visible together; move per-leg IV, futures assumptions and model details into an advanced panel.
- Rank 3–6 useful candidates initially; explain why each differs. “Show all” remains optional.
- Offer one primary next action per stage: Use strategy → Analyze → Paper trade or Review order → Monitor.
- Keep market context optional; an empty watchlist must not occupy the editor by default.
- Treat Save as continuous draft persistence; named snapshots and Duplicate remain deliberate actions.

## Recommended product position

KANIDA should own the **strategy lifecycle**, from a clear thesis to an auditable research revision, risk-reviewed execution and ongoing monitoring. Deep analysis is necessary, but reliability across state transitions is the stronger differentiation demonstrated by this study.

The implementation blueprint in `06-kanida-blueprint.md` specifies new capabilities as KANIDA requirements. It does not pretend those capabilities were observed end to end in every competitor.
