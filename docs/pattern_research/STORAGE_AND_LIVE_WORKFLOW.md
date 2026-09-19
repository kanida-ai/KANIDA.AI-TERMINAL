# Pattern research storage and future live workflow

## Decision

Keep market history, research evidence, and live serving data separate. The vendor API changes ingestion; it does not remove the need to preserve the exact history used for research.

The project already has separate source and research files. `db/kanida.db` is about 158 GB, while the original pattern backtests are in `market_scanner/output/backtests.sqlite3` (about 833 MB at inspection). Moving research out of the source database alone will therefore not make that source database small. The main saving comes from keeping historical candles and detailed research artifacts outside the application database.

## Proposed stores

| Store | Contents | Current implementation | Cloud direction |
|---|---|---|---|
| Historical market data | Normalized OHLCV, exchange/session metadata, revisions, adjustment basis, snapshot manifests | Existing source DB and frozen compressed histories | Partitioned files in object storage; short recent-data cache for scanning |
| Research | Versioned definitions, run manifests, summary statistics, occurrence and trade ledgers, folds | New isolated `market_scanner/output/expanded_research/research.sqlite3`; compressed stock artifacts beside it | Relational summary/index DB plus immutable detailed files in object storage |
| Live application | Latest completed-candle detections, freshness, active evidence version, user-facing cards | Existing scanner cache | Small application DB/cache; read-only references to approved research summaries |

Research compute workers read a declared snapshot and write per-stock artifacts. One coordinator writes SQLite summaries locally. A future distributed deployment needs a proper job queue and a database supporting multiple writers; sharing this SQLite file between distributed writers is not the design.

## Continuous cycle

1. Fetch the required candles through a vendor adapter. Normalize symbol/exchange identity, timezone, session boundaries, interval, adjustment basis and missing-data flags.
2. Cache recent normalized candles. Run the same versioned detector on completed candles. A partially built current candle needs a separate provisional state and must never be mixed with closed-candle research.
3. Save a detection with its pattern ID, variant, direction, timeframe, definition version, formation identity, signal timestamp and input quality.
4. Look up compatible precomputed research. Compatibility includes instrument/universe, interval/session rules, detector version, data adjustment basis, entry/exit policy, costs and test period. Matching only a pattern name is insufficient.
5. Display historical occurrence count, baseline results and genuinely later walk-forward results separately. Include sample size, date range, costs, insufficient-evidence status and data freshness. A recognition score measures geometric fit; it is not a win probability.
6. Refresh research in scheduled batches when new history or a versioned definition is available. Finish verification before changing the active evidence version. Serving a live detection should not trigger a full historical backtest.

The present implementation covers isolated historical research. The vendor adapter, live scanner integration, evidence-serving endpoint and production activation remain separate implementation steps.

## Retention and portability

- Preserve all current source data. No migration or deletion is required for this analysis.
- Retain exact research inputs or an immutable, licensed snapshot that can reconstruct them; a future API response can contain corrections and may not reproduce an old test.
- Deduplicate content-addressed candle snapshots and reuse them across runs after integrity validation. Current runs intentionally retain a local copy for reproducibility.
- Keep compact indexed summaries in the relational DB; load the detailed compressed ledger only when needed. Do not duplicate every candle into every strategy result.
- Version the catalogue, detector source, dependencies, execution model and selected universe. Preserve failed selections and zero-occurrence cells so apparent coverage is auditable.
- Audit corporate-action adjustment and historical universe membership before presenting results as production evidence. Current research records supplied prices and a snapshot universe, with these limitations disclosed.

## Current source freshness

The source-run metadata reports July 2026 as its latest market data. Actual usable first/last candles differ by stock and interval; for example, the TITAN/LTTS pilot ends on July 29 for intraday/daily and July 24 for weekly. These are historical inputs, not September live signals.

## Proposed serving contract

`EVIDENCE_SERVING_CONTRACT.md` specifies the proposed card schema, exact compatibility hashes, timestamp-based event identity, live-window parity tests, bounded application cache, publication/rollback and freshness policy. It incorporates `CLAUDE_ARCHITECTURE_REVIEW.md`. These are future integration requirements; they do not change the current historical batch.
