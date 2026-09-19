# Pattern expansion: focused research-engine audit

Date: 2026-09-15. Scope: `historical.py`, `backtest.py`, `study_engine.py`, `studies.py`, and their focused tests. This is a source audit, not verification of the full saved database or completion of expanded research.

## Expansion blockers

### 1. New detector registration does not automatically reach historical replay

- Evidence: `market_scanner/historical.py:58` defaults to mask `63`; lines 82–87 explicitly dispatch only the original six detector groups/ten chart patterns. `market_scanner/backtest.py:48` also supplies `63`; `market_scanner/studies.py:114`–124 uses the old accelerator or the same fallback mask.
- Impact: adding names and live-scanner methods alone can produce silently empty historical results for new families. Optimized masking can discard otherwise valid new patterns.
- Required acceptance: for every enabled new pattern, positive and negative fixtures must match live detection, unfiltered replay, filtered replay, and prefix-only replay. Perturb all future candles and verify earlier outputs are identical. Test candles at the 260-bar window boundary and pivot confirmation boundaries.
- Recommendation: introduce an explicit replay registry/capability check. Unsupported historical patterns must fail clearly; never store them as `no_occurrences`.

### 2. Chart episode suppression is unsuitable as a universal candlestick occurrence definition

- Evidence: `market_scanner/backtest.py:39`–65 and `market_scanner/studies.py:125`–138 retain one observation per state until three absent closes rearm a pattern/side. The key contains pattern and side, not formation identity.
- Impact: separate engulfing/hammer/etc. occurrences on nearby candles can be collapsed into one old episode. Trade overlap and occurrence identity are separate decisions; occurrence counts should not inherit chart deduplication accidentally.
- Required acceptance: two distinct same-pattern candlestick formations on adjacent or nearby eligible closes both enter the occurrence ledger; repeated observations of one continuing chart formation remain one episode; setup-to-confirmation remains connected. Apply the same policy in batch and custom studies. Trade admission can still reject overlapping positions.
- Recommendation: declare per-family event identity/rearm policy in the catalogue implementation, with a shared event extractor.

### 3. Frozen research dependencies and selected run must cover the expanded detector library

- Evidence: `market_scanner/studies.py:83`–99 explicitly loads only frozen `data`, `detectors`, and `historical` modules. Lines 102–110 replay the detector/config for the selected historical run. Lines 175–177 select the active stored run. Lines 114–115 read a frozen accelerator without including it in the required-file check at line 85.
- Impact: new helper modules or external detector dependencies need a reproducible snapshot contract. Old research runs cannot gain new patterns simply because current `NAMES` accepts them. Missing accelerator files can fail a study instead of safely falling back.
- Required acceptance: a fresh expanded run reproduces every enabled detector after current-source files change; an original ten-pattern run still loads and reproduces old signals; requests for patterns absent from a run return an explicit unsupported status; missing optional accelerator uses unfiltered replay. Persist versions/hashes of detector dependencies and catalogue definitions.

## Methodology differences requiring explicit treatment

### 4. Batch and custom studies currently answer different questions about missing data and costs

- Evidence: `market_scanner/backtest.py:77` deducts fixed round-trip fee plus slippage from returns; lines 94–95 discard an open trade upon a data-gap flag, including at entry. `market_scanner/study_engine.py:32`–33 and 77–79 instead apply slippage to both fills plus a round-trip fee; lines 52–53 exit an existing trade at the first available open after a gap, and do not apply that condition to the entry candle.
- Impact: results from the two engines are not interchangeable. Batch censoring conditions the reported sample on later data availability; it is not an executable exit policy. Ignoring a gap at entry can execute a stale signal without a declared maximum signal age.
- Required acceptance: fixtures covering a gap between signal and entry, a gap during a losing and winning trade, same-bar stop/target, adverse opening gaps, and long/short fees. Version and label the execution model and report excluded/censored samples. If one model is chosen for expanded research, demonstrate ledger parity between engines under equivalent settings.

### 5. Rolling tests need explicit carry-over and evidence reporting at catalogue scale

- Evidence: `market_scanner/study_engine.py:102`–121 purges candidate horizons before a fold and selects by mean return minus one standard error. `market_scanner/studies.py:361`–388 assigns trades to folds by entry date, but outcomes use the full study end, allowing positions to cross a fold boundary. Lines 399–400 attribute full eventual P&L to the entry fold. `market_scanner/backtest.py:163`–198 implements a different train/validation/test workflow and minimum final-test sample gate. The study output at lines 394–397 explicitly discloses snapshot universe membership and absent corporate-action adjustments.
- Impact: fold P&L is cohort P&L, not necessarily equity earned inside that calendar interval. Screening 107 entries and many rules makes a positive training score insufficient evidence of a dependable strategy. Existing input caveats remain relevant to expanded results.
- Required acceptance: end-to-end two-fold test where a trade crosses the boundary; prove the old rule remains fixed for that trade, capital is not reset, later returns cannot alter earlier selected rules, and earlier positions correctly block later entries. Report both fold entry-cohort outcomes and calendar equity if both are displayed. Preserve candidate count, failed/insufficient folds, sample sizes, and uncertainty; predeclare the experiment before full runs. Verify adjustment/universe metadata and label limitations in result cards.

## Existing safeguards to preserve

- Strict completed-candle/pivot timing and bounded replay window: `historical.py:40`–44, 64–72.
- Next-open entry and conservative same-bar stop priority: `backtest.py:67`–73, 100–107; `study_engine.py:22`, 54–65.
- Training selection independent of final-test prices and explicit purging: `backtest.py:165`–183; `study_engine.py:107`–113.
- Cash/share accounting and outcome-independent admission ranking: `study_engine.py:126`–128, 185–208.

## Verification status

Read existing focused tests in `test_historical.py`, `test_backtest.py`, and `test_study_engine.py`; they cover the original registry, chart episode rearm, next-open execution, costs, stop ambiguity, basic purging, shared capital, and simple future-price perturbation. They do not by themselves establish coverage of the expanded catalogue or the full rolling-job integration.

Executed the focused suites with `market_scanner/.venv/Scripts/python.exe`: **27 tests passed** (4 historical replay, 11 backtest, 12 study engine). No full scan, source database writes, or implementation edits were performed during this audit.
