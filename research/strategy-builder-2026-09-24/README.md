# KANIDA strategy research and build blueprint

Direct-interface study · 24 September 2026 · Desktop Chrome

## Deliverables

Read the blueprint for implementation and the app reports for observed behavior and provenance.

1. [Sensibull: 18 screens, flows, feature map, PRD, flowchart, strengths and weaknesses](01-sensibull.md)
2. [TrendSpider: observed workspace, access gate and limited findings](02-trendspider.md)
3. [Rupeezy / Astha: 14 screens, flows, feature map, PRD, flowchart, strengths and weaknesses](03-rupeezy-astha.md)
4. [5paisa: 15 screens, flows, feature map, PRD, flowchart, strengths and weaknesses](04-5paisa.md)
5. [Consolidated comparison and product decisions](05-comparison.md)
6. [KANIDA Strategy Builder: UX and engineering blueprint](06-kanida-blueprint.md)

The reports cover 48 numbered screens/surfaces, including dialogs and analysis tabs. TrendSpider accounts for one limited workspace observation; it is not a full fourth builder audit. Its options data required an exchange agreement, so research moved on as requested. The Rupeezy URL displayed Rupeezy branding; a separate Astha product was not independently inspected.

## How to read the evidence

- **Observed:** visible in the actual interface.
- **Tested:** action taken and resulting UI state inspected.
- **Unverified / gated:** outcome not exercised, not accessible, or not found within the inspected paths. This does not establish that a product lacks a capability.
- **Proposed:** a KANIDA requirement or inferred reverse-engineered requirement, not a verified competitor implementation detail.

Screens were examined sequentially: Sensibull, TrendSpider, Rupeezy, then 5paisa. Research used existing Chrome sessions, including temporary builder/scenario changes and review dialogs. Final live orders, destructive confirmations, permanent saves and agreement acceptance were not submitted. Mobile layouts, real fill behavior and complete historical backtests were not verified. Numeric examples capture transient interface values and should not be treated as current quotes or trading recommendations.

## Build blueprint coverage

| Requested area | Blueprint location |
|---|---|
| Product scope, user journey, recommended flow | Sections 1–3 |
| Screen-by-screen design and interaction states | Section 4: K01–K14 |
| Mobile/web and accessibility | Section 5 |
| Default strategies | Section 6 |
| Payoff, Greeks, margin and risk | Section 7 |
| Backend services and ownership | Section 8 |
| Data requirements and invariants | Section 9 |
| APIs and streaming | Section 10 |
| Execution, partial fills, adjustment and roll state | Section 11 |
| Paper trading, simulation, replay and backtest | Section 12 |
| Monitoring and alerts | Section 13 |
| Acceptance criteria, performance and verification | Section 14 |
| Delivery stages and launch dependencies | Section 15 |
| Competitor-to-design traceability | Section 16 |

The recommended product is one versioned strategy workspace with multiple entry paths: build manually, choose a template, or discover by thesis. Analysis, paper runs, live deployments and adjustments remain linked but retain separate assumptions and ledgers. The most important engineering requirements are consistent contract identity, synchronized analytics, explicit quote quality, immutable reviewed orders, and recovery from partial or ambiguous execution.
