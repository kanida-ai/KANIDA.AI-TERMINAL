# KANIDA local research CLI

The reusable runner now evaluates explicit strategy batches across daily cash equities, using the existing core, NDP signals and Stock Miner features. The original engines remain unchanged. This is an executable research pilot, not a trained universal strategy model.

## Try it in PowerShell

From `C:\Users\SPS\Documents\Kanida_Falcon`:

```powershell
& '.\research_orchestrator\kanida.ps1' capabilities
& '.\research_orchestrator\kanida.ps1' demo
& '.\research_orchestrator\kanida.ps1' research --spec research_orchestrator/examples/pilot-strategies.json
```

`demo` executes ordinary and ambitious objectives plus an expert's original/modified strategies. It explicitly blocks the F&O-universe example because historical membership has not been verified, and asks what “consistent” returns means. Candidate rules and the three-stock scope are declared rather than inferred by a language model. Repeated identical requests reuse checksummed evidence after rechecking current data and source identities.

To use Anaconda Prompt, run the same project environment (no changes to Anaconda's base environment):

```bat
cd /d C:\Users\SPS\Documents\Kanida_Falcon
market_scanner\.venv\Scripts\python.exe -m research_orchestrator demo
```

Edit a copy of `examples/pilot-strategies.json` to try your own supported strategy. Each request can contain 1–64 explicit stock symbols, 1–32 candidates and 1–6 training/test folds. These are computation bounds, not a latency guarantee. `--json` prints a compact result; `--refresh` recomputes while preserving previous evidence. `ask` remains a limited phrase-interpretation test and does not automatically execute unrestricted language.

Execution settings must be explicit: account/cost fields; each strategy's ranking and schedule; every exit field (`hold`, `stop_kind`, `stop`, `target`, `trailing`, `disqualify`); and each numerical entry threshold. Booleans, numeric strings and null thresholds are rejected. Positive stop and target values are required by the current core; uncapped/no-stop strategies need a different execution adapter. A crossover or breakout cannot reuse its entry event as a continuing `disqualify` condition. In the expert demo, adding a trailing stop preserves the separately declared 7% stop and 15% target.

## What is connected

- Core daily price/volume rules: Wilder RSI, momentum, prior-high breakout, dip, moving averages, relative strength, volume, liquidity and volatility.
- Explicit NDP variants: RSI, EMA/SMA crossover **events**, price-versus-SMA states and Donchian breakout.
- Stock Miner's `d_sma200 > 0` feature; future-label columns are not permitted.
- Nested AND/OR expressions compiled into independent masks; features are reused within each stock/cutoff across all candidates.
- Existing next-open trade paths and chronological shared-cash accounting, with whole shares, costs, slippage, risk/position limits and bounded exits.
- Training-only selection before later outcomes are evaluated. Signal-prefix checks and account reconciliation run inside each experiment.

`adapters.LocalSnapshotProvider` reads the frozen OHLCV files and actual Nifty 50 price index through read-only database access. A future vendor adapter can return the same bars and provenance to `engine.run(provider=...)`; no vendor is connected yet. Keep market data ingestion/completion, symbol identity, adjustment policy and historical membership in the data layer rather than inside a language-model prompt.

## Evidence and interpretation

The CLI table shows **account** return/CAGR/drawdown separately from **average per-trade** return and win rate. Each test fold starts a fresh account. Full JSON stores each trade, costs, daily capital curve, per-stock outcomes, market-regime associations, training selections, sample counts, rules, source/data hashes and limitations. Do not compound independent folds or treat the best displayed test candidate as a training-selected strategy.

The main verdict now follows only the training-selected strategy in each later fold. `post_hoc_test_matches` is a descriptive comparison that cannot qualify a strategy. A positive annual-return objective requires at least 365 calendar days of testing and 730 days of training for selection; exploratory shorter tests are labelled and cannot establish that annual target. Data quality remains unverified, so numeric threshold matches are never labelled verified historical qualifications. The current NDP RSI uses its own exponential seeding; it is not silently substituted for core's Wilder-seeded RSI.

Both stock candles and the index candles actually read are archived under `library/<owner>/inputs/`. Use `research --spec <request.json> --data <inputs/hash.json.gz>` to replay that exact archived data. The input checksum is verified. `--refresh` compares outcomes with the prior identical request/data/code result and fails if they differ, then writes a separate audit artifact if they agree; the original identical cache entry remains canonical. Evidence files are published atomically. Required accounting checks remain active under `python -O`. Candle completion is checked against an aware UTC as-of time, interpreting local timestamps as Indian market time. Missing benchmark sessions and overnight price jumps are recorded as quality diagnostics, not assumed to be corporate actions.

The evidence library lives under `reports/research-orchestrator/library/local-pilot/`. Cache identity includes the normalized contract, objectives, costs, candle contents, engine sources, dependency versions and owner namespace. This is a private single-machine namespace, **not** production authentication, encryption or multi-tenant access control. No customer prompts or holdings are shared. These files are research evidence; large live market streams should not be stored inside strategy rows.

Objective failure means “none of these declared candidates met the criteria,” not “no such strategy exists.” Minimum trades defaults to five. Candidates never become deployable automatically. Untouched final holdout, corporate-action validation, delistings, historical constituents, stress/parameter tests and full-market benchmarks remain necessary. The present selector is an explicit training heuristic, not a proprietary trained profitability model.

## Working with Claude Code

The independent review assignment is in `CLAUDE_REVIEW_TASK.md`. It is scoped to relevant source files and excludes credentials, databases, shell execution and edits. The preferred current route is the user's signed-in Claude desktop Code session. `claude-review.ps1` is an optional CLI route only when a subscription login is configured; it blocks API-key fallback. Desktop login and CLI login are separate in this environment.

## Earlier diagnostic tools

This new package sits beside the existing engines. It does not overwrite them.

The first CLI tool probes whether the unpublished Strategy Lab interpreter preserves the user's actual requirements. It runs ordinary, ambitious, expert and investor scenarios, and writes an immutable baseline report. It does not perform financial research, start a scheduler, call a paid model, or place orders.

From `C:\Users\SPS\Documents\Kanida_Falcon`:

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator.evaluate
```

Use `--persona expert` for one group. Results belong under `reports/research-orchestrator/`.

Read-only source inventory and harness checks:

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator.inventory
& '.\market_scanner\.venv\Scripts\python.exe' -m unittest research_orchestrator.test_evaluation -v
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator.feature_probe
```

The inventory fingerprints source files without importing or executing the legacy engines. It is discovery, not a claim that their interfaces have already been integrated.

The feature probe executes the inspected Stock Miner and NDP feature libraries on one shared frozen stock history, excludes future-outcome label columns, and compares earlier outputs with and without later candles. It does not certify indicator definitions or investment performance.

## Current CLI integration

```powershell
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator ask 'Buy RSI below 30 OR stocks breaking a 52-week high'
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator check
& '.\market_scanner\.venv\Scripts\python.exe' -m research_orchestrator probe
& '.\market_scanner\.venv\Scripts\python.exe' -m unittest research_orchestrator.test_evaluation research_orchestrator.test_numerical_probe -v
```

`ask` uses a new conservative local phrase adapter. It preserves supported constraints, blocks identified ambiguities and unsupported requests, and does not run a backtest. It is not a general-language model. `check` evaluates 22 targeted intent contracts; passing these does not establish arbitrary-prompt correctness.

`probe` runs a fixed four-candidate RSI experiment on TITAN, ICICIBANK and MARUTI. It bridges NDP signals and Stock Miner features to the existing trade-path and shared-cash accounting functions. Three rolling three-year training periods select a candidate before their corresponding 2020, 2021 and 2022 test periods are evaluated. Accounts independently start at INR 30,000 each year. All trades, daily equity, costs, training evidence, selections, data fingerprints and limitations are recorded in a timestamped JSON report. Source engines are not edited.

Training entry horizons are purged at the fold boundary. Features receive only the history available through each cutoff. No private customer prompts or holdings are used. Passing cash/prefix checks is an integration result, not financial validation. The experiment does not execute arbitrary requests, certify source price adjustments, qualify deployment, or establish an under-one-minute service guarantee.

The intended later orchestration is: typed user intent → capability-aware engine adapters → bounded candidate experiments → exact accounting → time-separated validation → evidence registry → supported explanation. Existing versions and outcome definitions remain explicit. Experiments that fail, are unsupported or have insufficient evidence stay in the registry; they are not promoted to deployable strategies.

The daily research worker is not yet built or scheduled. Cash-equity scope, an AWS + hosted-AI target and private-by-default user learning are confirmed. Monthly spend, concurrency and execution schedule remain pending; see `DECISIONS.md`. Its task is to research and propose versioned improvements, not to rewrite engines or change live strategies automatically.
