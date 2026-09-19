# Engine-first development plan

## What the pilot proves

An explicit set of rules can now travel through a shared pipeline: contract validation → read-only data adapter → reusable numerical features → candidate masks → portfolio simulation → training-only selection → later-period evaluation → checksummed evidence. Existing engines are called through adapters and are not overwritten.

This proves a bounded integration. It does not establish that KANIDA understands any sentence, searches every possible strategy, has a universal profitability model, or meets a service-level latency target for thousands of users.

## Responsibility of each layer

| Layer | Current implementation | Next requirement |
|---|---|---|
| Conversation | Limited local phrase adapter; explicit JSON for research | Evaluate a low-cost language model against semantic tests, then compile its output through the same strict contract. Clarify unsupported/ambiguous conditions. |
| Strategy definition | Nested AND/OR, explicit engines/parameters, exits, cash/risk/cost rules | Extend typed operators and capability catalogue without changing existing saved definitions. |
| Numerical features | Core indicators, an inspected NDP subset and Stock Miner trend | Verify more feature formulas and integrate Falcon/state/per-stock learning adapters. |
| Candidate research | Predeclared candidate batches and training-only ranking | Generate bounded variations around the user's original idea; preserve originals; record all trials and failed proposals. |
| Validation | Purged training horizons, later test periods, causal signal checks and accounting checks | Untouched final holdout, parameter/cost sensitivity, multiple-testing controls and stability across regimes and stocks. |
| Data | Frozen local bars; actual Nifty price-index benchmark | Corporate actions, delistings, point-in-time constituents, exchange calendars and vendor entitlements/completion. Fundamentals require publication-time histories. |
| Evidence | Local versioned reports and exact-request reuse | Queryable strategy/data/version index, authenticated tenant isolation and a promotion policy. Large candles/features belong in efficient data/feature storage. |
| Autonomous research | Manually reproducible persona suite | Budgeted queue, incremental feature refresh, deduplicated experiments, private user feedback, and daily worker after resource/schedule decisions. |
| Execution | Research only; no orders from this CLI | Preserve the separate execution and regulatory gates. Research discoveries never silently change a live strategy. |

## How to work toward under one minute

1. Compute reusable features when finalized market bars arrive. Do not download years of history for every question.
2. Store the compiled strategy and evidence identity separately from market data. Retrieve exact validated evidence when its data/version matches.
3. Compile supported rules into vectorized masks; reuse feature arrays across related candidates. Run candidate search under a declared time and experiment budget.
4. Use an asynchronous queue for unfamiliar or larger requests. Return actual progress and completed evidence, not invented counts or unsupported predictions.
5. Benchmark cold reads, warm feature reuse and exact-evidence hits separately. Measure p50/p95 with declared universe, history, candidate count, hardware and concurrent users before promising a latency guarantee.

The first three-stock results demonstrate fast local execution, not the completed scaling design. A future API data vendor plugs into the data adapter; it does not replace the research engine. A conversational model proposes structured intent and explains verified evidence; it must not calculate or invent backtest performance.

## How Codex and Claude cooperate

Codex implements and runs controlled experiments. Claude Code independently reviews a bounded set of files in the user's signed-in desktop session. Codex verifies each finding, applies additive fixes and reruns affected checks. Source ownership and review scope prevent concurrent modifications to the same engines. No production paid-model integration is required to do this development work.
