# Research pilot decisions and activation boundary

Updated 13 September 2026. This records user decisions, not inferred approval of missing limits.

## Confirmed

- Preserve existing engines; integrate through adapters and add a common research layer.
- Start with cash-equity strategy families, including indicators, momentum, mean reversion, regimes, patterns and portfolio constraints.
- Target AWS computation plus hosted AI. No monthly spending cap has yet been supplied.
- Private user research by default. Sharing requires explicit permission.
- Begin with CLI evaluation and numerical tests before completing the UI.
- Pursue a daily autonomous researcher and a reusable evidence library; activation time and resource limits remain pending.

## Pending user inputs

1. Monthly incremental AWS cap and hosted-AI cap, including currency. Recommend separate allowances, job-cost reservations and admission stops rather than assuming cloud billing alerts enforce a hard ceiling.
2. Initial concurrency. Recommendation: one simultaneous research job until throughput and cost are measured.
3. Daily schedule. Recommendation offered: 19:00 Asia/Kolkata, checking source freshness first. The user may instead choose manual runs until acceptance tests pass.

## Proposed research behavior

- Objectives such as 50% annualized returns or 100% CAGR below 15% drawdown are hypotheses to evaluate, not promises. A negative answer says no qualifying result was found in the stated completed search, not that no strategy could ever exist.
- “Consistently” needs an explicit definition. Show CAGR, calendar-year returns, rolling 12-month outcomes and the proportion of periods meeting the target; do not silently treat all of them as equivalent.
- F&O equity membership and actual futures/options execution are different requests. Cash-equity scope does not authorize synthesizing derivative returns.
- Preserve the original strategy and version every proposed improvement. Evaluate additions individually and in combinations; retain removals and failed variants too.
- Keep proposal/selection data separate from later evaluation periods. New user prompts are research interests, never ground-truth performance labels.
- The scenario generator, research coordinator and independent evaluator are separate logical roles. They need not make separate hosted-model calls for every trivial operation.
- An experiment is not a validated strategy. Store statuses such as proposed, tested, insufficient evidence, rejected, supported candidate and paper observation. Preserve data/engine versions, exact rules, experiment family, costs, trades, folds and limitations.
- The five-trade browsing minimum remains distinct from promotion criteria. Strong claims require sufficient independent evidence for the actual strategy; no universal sample count guarantees validity.
- Research updates its candidate priorities and evidence library. It does not automatically rewrite engine source, merge code, override user constraints or deploy changed strategies.
- Live execution remains disabled. Research or test-agent prompts cannot authorize orders.

## Work completed in the additive layer

- `inventory.py`: fingerprints 12 existing source components without changing them.
- `evaluate.py` + `scenarios.json`: 16 ordinary, ambitious, expert and investor interpretation scenarios. Baseline: 9 pass, 7 have contract findings.
- `test_evaluation.py`: 5 harness checks pass.
- `feature_probe.py`: calls the existing Stock Miner and NDP indicator modules on the same frozen TITAN daily history. Observed 129 numeric feature columns and 134 signal variants; 526 comparisons across two historical prefixes passed after a 252-observation warm-up exclusion.
- `intent.py` + `check_integration.py`: additive local phrase adapter; 22 targeted contracts pass. This does not establish general-language coverage or hosted-model integration.
- `numerical_probe.py`: four fixed RSI variants across three stocks and three chronological train/test folds, using existing NDP, Stock Miner and portfolio accounting. Training-selected filters have sparse later evidence and are not promoted. Exact reports preserve all training trials and observed test outcomes.

No strategy qualification, completed model integration, cloud deployment or active daily schedule is implied by these diagnostic results. Numerical outputs are explicitly limited exploratory simulations. Existing published pilot remains unchanged.
