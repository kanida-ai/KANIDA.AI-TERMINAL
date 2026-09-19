# Claude architecture review: storage and live workflow

- **Date:** 2026-09-15
- **Scope:** `STORAGE_AND_LIVE_WORKFLOW.md`, checked against the goal of a later vendor API, a small application DB, and precomputed evidence cards.
- **Method:** a read-only review of `store.py`, `runner.py` (prepare/process/verify), `report.py`, the README and the detector modules, plus two small read-only checks noted below.
- **Untouched:** the running batch, source DB and code.

## Verdict

The direction is sound. Market history, research and live serving are kept separate. Runs are frozen and content-hashed (code, dependencies, histories, specifications). Closed candles are kept separate from provisional ones. Zero-occurrence cells are kept. Compatibility is required to go beyond the pattern name. Research reproducibility is strong.

What is **not yet specified** is the contract that turns a research run into something a small live application can safely query. The gaps below are requirements to settle before the evidence-serving step. None of them blocks the current batch.

## Material findings

| # | Finding | Evidence | Requirement |
|---|---|---|---|
| 1 | **The evidence-card contract is undefined.** The doc lists what a card should show but not its schema, identity key or status vocabulary. | `cells` primary key is (run, symbol, timeframe, pattern_id, variant, side). Cost, engine, definition and adjustment identity exist only in run-level JSON. | Define a published `evidence_card` schema: identity key (next row), statistics shown, uncertainty, sample and date range, status label, provenance (run id, manifest SHA-256), and an as-of date. |
| 2 | **`definition_version` cannot serve as a compatibility key.** | All 249 new specs report `1.0.0`, including harmonic cells whose gates changed this session (Codex finding 3, Q1). Legacy reports `1.0.1`. The frozen manifest still reproduces this run, because it stores definitions and code hashes. | The live lookup key must include a **spec hash**: SHA-256 of the spec JSON plus detector module and `common.py` source hashes. Add the engine/execution version, cost model, aggregation/calendar/config hash and data adjustment basis. The live scanner must run hash-identical detector code, or the card shows "incompatible evidence". |
| 3 | **Parity between live windows and research is not guaranteed by current tests.** Research runs on full stored history. A live cache is **start-truncated**. The detector suites and `validation.py` test **end** truncation (prefix) and future perturbation only. | A read-only check on one 3,000-bar walk found 0 mismatches between the last 60 bars of 260/600-bar tail windows and full history. That covered 274 candle, 156 PA and 5 chart events, and 0 harmonic events. It is too small to clear chart and harmonic logic that carries state: formation claims, the swing list, mother-bar clusters, hikkake linking. | Declare a minimum live warm-up per module and timeframe. Add a real-data gate: `detect(tail_W)` equals full history for signals in the last N bars, for every family. Alternatively, run live detection on the same stored full history. Record W in the evidence key. |
| 4 | **The vendor-normalized candle contract is missing.** The `gap` quality flag, session stubs and the 35% open-jump discontinuity rule come from `data.aggregate`. Adjusted vendor prices or corrections change gaps and detections. | `code_files()` hashes `config.json` and the local aggregation code. A vendor adapter would be new, unhashed logic. | Specify a normalized candle schema: instrument id, exchange, timeframe, start/end, OHLCV, completeness, `gap` rule version, adjustment basis/factor version, vendor source, fetch/revision time. Version the aggregation/calendar logic. Any vendor or adjustment change creates a new snapshot identity and makes older evidence non-matching. |
| 5 | **The application DB projection and size budget are unstated.** The full run produces about 1,431 stocks × 1,048 cells ≈ 1.5M `cells` rows, each with a JSON `summary`. That is research-scale, not app-scale. | `store.connect` schema, and `commit_stock` writing summaries per cell. | Define a publish step that projects an **active** version into a compact card table. Keep only needed fields, drop ledgers, and treat unsupported or insufficient statuses explicitly. Give it a row and size budget. The app reads only that table, never `research.sqlite3` or artifacts on the request path. |
| 6 | **Activation, rollback and freshness are unspecified.** | The doc says "change the active evidence version" after verification. | Use an atomic active-version pointer and a stored verification record (validation hash, report coverage). Provide rollback to the prior version and card-level provenance. Add a staleness rule for evidence as-of vs detection date. The research snapshot ends in July 2026 and must display as historical. |
| 7 | **Selection and multiple testing are not handled on cards.** The report's "highest average test return" shortlist is selected using test results. The status string `tested_positive` reads as validation in a UI. | `report.py` example ranking; the `runner.process_stock` status names. | Map research statuses to neutral UI labels. Do not publish a per-stock cell as "validated" without a predeclared selection rule and an untouched release holdout, or an explicit multiple-testing adjustment. |
| 8 | **Pooled evidence is undecided.** Many per-stock cells will be small samples, and cards likely want pattern-level evidence across the universe. | Cells are per-stock independent studies. The report warns they must not be summed. | Decide whether cards show per-stock, pooled or both. Pooling needs overlap, dependence and cluster-aware uncertainty, plus its own versioned computation. It must not be an ad-hoc sum. |

## Smaller reproducibility or interface items

- **Portable event identity.** Events carry bar indices, which don't carry across windows or vendors, plus only `pattern_start` as a timestamp. Live and card storage should key detections by timestamps: formation start, detected, signal and confirmed bar end times, plus episode start time and spec hash. Indices should not be keys.
- **Catalogue file.** The manifest records `catalogue_ids` but not a hash of `PATTERN_CATALOGUE_PROPOSAL.json`. Add one so catalogue wording changes are traceable.
- **Provisional candles.** The live detection record needs an explicit `candle_complete` field. Cards should never attach research evidence to a provisional detection.
- **History copies.** Each run duplicates frozen histories (acknowledged). Before cloud storage, adopt content-addressed history blobs referenced by hash from manifests.
- **Environment.** Dependencies record Python, NumPy, Numba and TA-Lib versions. The OS/CPU/BLAS are not recorded. That is acceptable, since results are integer-index and TA-Lib based, but note it as an assumption.

## Not reviewed

- The evaluator's statistical correctness (Codex-owned; see `EVALUATION_NOTES.md`).
- Distributed job-queue design.
- Licensing terms for vendor snapshot retention.
