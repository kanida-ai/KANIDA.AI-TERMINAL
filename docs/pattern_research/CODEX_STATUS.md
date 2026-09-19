# Codex integration status

## Current authoritative status

The complete-universe run `8ae6ddc251e80668239e` and final reproducibility audit have passed: 1,431 stocks, four timeframes, 107 catalogue IDs, 262 direction/variant combinations and all 1,499,688 expected study cells. There were zero computation or checksum/coverage failures. All frozen input histories and result archives were verified.

**Publication is withheld for source-quality review.** Final outlier inspection traced materially unreliable PIIND intraday returns to anomalous rows already present in the original DB. A full coarse screen flags 37 stocks / 75 stock-timeframe datasets; unflagged data is not certified clean. See `RESULTS_SUMMARY.md`, `PIIND_SOURCE_AUDIT.md` and the run's `PUBLICATION_REVIEW.json`. No new results were activated in the live scanner. The research is complete as an analysis of its original inputs; corrected-source research and live integration remain future work.

Preflight verification passed: 135 detector tests, 22 evaluator/storage tests, and independent real-data prefix checks covering the entire registry. The exact validation hashes match the frozen run. Claude Code's detector implementation and architecture review are complete. See `IMPLEMENTED_CATALOGUE.md` for tested variants and explicit deferrals, and `EVIDENCE_SERVING_CONTRACT.md` for the proposed future application integration.

The notes below describe earlier milestones; statements that the full run is awaiting detectors are superseded by this section.

Claude Code owns the new detector modules and detector fixtures per CLAUDE_HANDOFF.md. Codex owns the isolated runner, evaluator, artifact store, reporting and independent integration checks.

- Legacy TITAN/LTTS pipeline pilot `0817b9f10cd9aca46dcf`: 2 stocks, 4 intervals, 13 directional definitions, 104/104 stored cells. This is only a 10-pattern pilot.
- Saved ledgers independently reconciled: 1,553 observations, 1,320 descriptive baseline trades, 53 walk-forward trades. Selected cells overlap and are not a combined portfolio.
- Evaluator/storage suite: 21 focused tests passing after audit fixes.
- Snapshot execution checks and resume artifact verification implemented. Full expansion has not started; awaiting completed detectors and independent causal-prefix/coverage validation.
- Original production scanner, source database and old research outputs remain intact.

## Latest integration

- All 107 catalogue IDs register: 262 direction/variant combinations, 1,048 study cells per stock across four timeframes.
- Full real-data prefix validation passes after chart fixes. Repeat once all source edits finish to record the final hashes.
- Combined run `12a696f4a76e99e63320` completed 4/4 stocks (TITAN, LTTS, CEMPRO, RELIANCE), 4,192/4,192 cells, zero errors. Artifact integrity and selected coverage pass. Every additional chart ID CH11–CH28 has real occurrences; CH28 volume variants emit (bullish 5, bearish 13 pilot occurrences).
- Root evaluator/storage suite now 22 checks. Engine 1.0.1 baseline trigger fixed from capabilities. Valid emitted events take precedence over conservative maximum lookback metadata in status labeling.
- Storage checksum verification optimized to stream previously validated archives during resume/reporting; immutable identity and study-key coverage remain verified.
- Full-universe batch awaits Claude's chart fixture tests and final ready status. No new root changes are planned before the final validation freeze unless checks find issues.
