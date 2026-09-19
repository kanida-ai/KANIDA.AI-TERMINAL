# KANIDA app and backend: current-state review

Reviewed 16 September 2026 Pacific / 17 September IST. This is a handoff for subsequent feature work, based on the running app, current source, saved database metadata, focused checks and a parallel backend audit. It is not an exhaustive security, broker-execution or mobile-device certification.

## Summary

The project now has a functioning private-pilot application around the expanded pattern research: a registry-driven Discover page, live Kite data ingestion, lifecycle-based pattern detection, chart overlays, and precomputed historical evidence cards. Source repair and a clean research rerun are persisted. Several integration and evidence-publication gaps remain; completing a research run does not mean every downstream product flow supports it.

No application source, configuration or operational database was intentionally changed during this review. No studies, research batches, broker actions or ingestion jobs were initiated. Two review documents were written. The existing browser tab was returned to Discover.

## 1. What is implemented

| Area | Current implementation and observed behavior |
|---|---|
| Private pilot | Invitation-based authentication, account/onboarding gates, owner tools, billing and broker integration code. Runtime configuration has Kite enabled, Google and billing disabled, and live orders disabled. |
| Discover | NIFTY 500 selection; configurable blocks; two scanner cards, chart and evidence workflow; responsive card density; forming/confirmed detections; live data status. |
| Strategy registry | Owner UI loads successfully. Expanded blocks contain 180 chart, 660 candlestick, 104 price-action and 104 harmonic strategy rows: 1,048 total. Legacy Chart Strategies contains another 52 rows. Quant, Results & Events, and Options blocks currently have zero strategies. |
| Pattern research | 107 pattern IDs / 262 directional-definition variants across four timeframes. The current clean run covers 495 stocks. These are coverage counts, not counts of independently proven profitable strategies. |
| Evidence cards | Saved historical summaries, walk-forward status, horizon curves, barrier outcomes, favorable/adverse excursion, conditional samples, recent occurrences, costs and provenance. Small-sample and source-review warnings are visible. |
| Market data | Separate revisioned 15-minute market-data store, raw archive, source identity, quarantine/exclusions, frozen snapshots and session-aware aggregation. Kite is the operating provider. |
| Live detection | Expanded detector set with forming, confirmed, invalidated and expired states. Rescans are scheduled around completed timeframe candles. Chart geometry is generated from detector data. |
| Chart workspace | Stock/timeframe navigation, candle chart, pattern geometry, evidence and action controls. A confirmed integration bug affects some selected daily/weekly setups; see below. |
| Watchlist | Per-user saved setups and bullish/bearish collections; current reviewed account was empty. No watch mutations were tested. |
| AutoTrade | Synthetic workflow account, saved plans, simulated order lifecycle, capital reservation and loss-budget controls. Real execution is hard-disabled in server code. |
| Simulate | Custom study UI and backend exist, including backtest/walk-forward configuration and saved studies. This remains connected to the legacy pattern registry and research path. |
| Falcon | Main landing page is still a coming-soon placeholder. Additional Falcon code exists, but the full agent experience is not mounted as a working feature. |

The admin description says strategies are used by Falcon; that label should not be treated as evidence that the deferred Falcon experience is already available.

## 2. Current data and research identities

| Item | Selected value |
|---|---|
| Frozen market snapshot | `snap_20260916T055228_f4bab0e9` |
| Clean source export | `m15clean-20260916` |
| Pattern research run | `4b33a5249562631524d6` |
| Outcome run | `0b89eff7c36c01b28af0` |
| Outcome engine | `1.2.0` |
| Application research index | 1,048 strategies; 518,760 rows; 495 studied symbols |
| Publication status | **Unreviewed**; configured release-decision file is absent |

The earlier run `8ae6ddc251e80668239e` remains explicitly withheld for source-quality review. It is distinct from the selected clean run.

The repaired frozen snapshot contains 27,867,377 rows. The backend audit verified a sourced PIIND correction, preserved earlier revisions and the existence of its archived vendor payload. It did not recompute the checksum of the entire snapshot. Seven symbols are quarantined overall; six are in the selected research universe.

At the start of inspection the latest live 15-minute candle ended at 17 September 09:30 IST; by the final browser check the status advanced to 09:45. Daily/hourly research-detection timestamps through the prior completed session are not automatically stale simply because a new base candle has arrived.

The future vendor adapter is a configurable, fixture-tested integration scaffold. Real vendor schema, identifiers, adjustment basis and timestamp/pagination behavior still need verification against the selected vendor.

## 3. Architecture and ownership map

```text
Kite / future vendor API
    -> market_data: revisioned candles, raw archive, corrections, quarantine
    -> completed-timeframe aggregation
    -> market_scanner: live pattern lifecycle and chart geometry
    -> private-pilot API on :8082 (research service on :8765)
    -> Discover / chart / watchlist

Frozen data snapshot
    -> expanded pattern backtest and walk-forward research
    -> outcome summaries and research databases
    -> application research index and evidence-card transforms
    -> Discover historical evidence

Separate user/product database
    -> accounts, sessions, strategy registry, plans, watches,
       synthetic orders, wallets, billing and broker connection records
```

Useful source entry points, relative to the repository root:

- App shell/routing: `kanida-app/src/PilotShell.tsx`, `src/MainWorkspace.tsx`, `src/shell/routes.tsx`.
- Discover: `kanida-app/src/discover/index.tsx`, `Block.tsx`, `ChartCard.tsx`, `EvidenceCard.tsx`, `deeplink.ts`.
- Shared selection: `kanida-app/src/context.tsx`, `src/activeSymbol.tsx`, `src/workspace/DiscoverPanel.tsx`.
- Strategy administration: `kanida-app/src/admin/StrategyAdmin.tsx`.
- Study UI: `kanida-app/src/SimulationDesk.tsx`, `src/constants.ts`.
- Private API: `kanida-app/server/kanida_pilot/app.py`, `config.py`, `db.py`, `auth.py`, `billing.py`, `kite.py`.
- Research serving: same server package, `research_store.py`, `research_index.py`, `cards.py`, `strategies.py`, `detections.py`, `evidence.py`.
- Product execution gates: same server package, `product.py`, `simulation.py`, `live.py`.
- Market-data pipeline: `market_data/`; scanner scheduling/calendar: `market_scanner/engine.py`, `data.py`; live patterns: `pattern_live.py`, `pattern_lines.py`, `pattern_warmup.py`.
- Legacy custom studies: `market_scanner/studies.py`.

Storage separation is implemented, but cloud portability is not yet lightweight by default. At inspection, main files were approximately 19.5 GB market data, 5.3 GB expanded research, 38.5 GB outcomes, 162 MB application research index and 510 MB scanner cache, excluding WAL and archive/snapshot artifacts. Retention and deployment packaging remain separate decisions.

## 4. Confirmed gaps to carry into the next task

### A. Some valid Discover setups disappear when opened on the full chart

Reproduced with HINDZINC / Rising Wedge / 1D. Discover showed the confirmed setup and its geometry. Opening the full chart navigated with the exact match identity, but the chart reported “No stored setup” and disabled its evidence/watch actions.

Root cause confirmed by source, calendar calculation and read-only API/database inspection: HINDZINC's CAS session calendar uses 15:15 for the end of continuous trading. The scanner also uses that shortened time for daily/weekly freshness, while the official daily/weekly bars correctly end at 15:30. Consequently the real setup is marked `current:false` and removed by the app's `current=true` query. This is a broader CAS daily/weekly freshness problem, not a missing detection or a renamed pattern.

Separately, the shared chart resolver searches a capped display list (`RESEARCH_DISPLAY_LIMIT=500`) rather than directly resolving every selected identity. That is a structural limitation worth addressing, but it was not the cause of this reproduced case.

### B. Reported statistical refinements are not what the cards serve

The rerun report's refined significance calculation lives in sidecar results. Two checked cells still have the older 400-replicate values in the serving outcome database: q approximately 0.857 and no discovery, versus report refinement q approximately 0.0475. The app must not be described as serving the refined results yet.

### C. Known baseline-window mismatches are not disclosed correctly

The checked CGPOWER 1H and TTML 4H cells have `baseline_window_mismatch=1` and baseline coverage about 70.03% and 38.27%. Card transformation ignores that flag, while its explanatory text can assert a same-window comparison. This warrants correction before releasing the evidence.

### D. Expanded research is not fully connected to custom simulation/trade preparation

`market_scanner/studies.py` validates against the legacy `detectors.NAMES` and reads the legacy active backtest run. The Simulate page displayed a blank selected-pattern name while its default remains `cup_handle`. The expanded 107-pattern research and live scanner do not imply that custom studies support the same catalogue.

The trading evidence path also deliberately retains legacy exact-rule eligibility and fails closed for unsupported research records. Completing the expanded research-to-plan bridge requires explicit work; do not bypass its evidence gates.

### E. Remaining product/deployment work

- Record an explicit release decision for the clean evidence population after resolving evidence issues.
- Complete and verify the selected future vendor integration.
- Implement the deferred Falcon experience and populate other strategy blocks when requested.
- Configure Google/billing if they are part of the intended pilot launch.
- Reconcile documentation and QA scripts with the new authenticated routes and data model.
- Define storage retention and historical vendor-correction refresh policies; the normal live ingest loop primarily appends new bars.

## 5. Validation performed in this review

| Check | Result |
|---|---|
| Frontend TypeScript | `npm.cmd run typecheck` passed |
| Private-pilot backend suite | 161 passed; three dependency deprecation warnings |
| Evidence card logic checks | 131 passed |
| Discover checks | 38 passed |
| Data-status checks | 41 passed |
| Chart-window checks | 8 passed |
| Running service health | App :8082 and research :8765 responded successfully |
| Manual authenticated UI | Discover, selected chart, Watchlist, AutoTrade, Simulate, Falcon and owner strategy registry inspected |
| Database/source checks | Active research identities, source repair example, publication gates, live ledger and specific evidence/calendar discrepancies verified |

The legacy browser script `check-replay.cjs` failed waiting for “See evidence”: it starts without authentication on the old root workflow and now encounters the welcome page. This is an outdated test setup, not proof of a replay calculation defect. Its later companion scripts were not run. No full research rerun, statistical replication, live brokerage test or exhaustive responsive-device pass was performed.

## 6. Handoff notes

- Older README/private-pilot documentation predates several implemented additions. Current source, runtime and persisted identities take precedence over those descriptions.
- The app repository has substantial uncommitted work. Preserve it; do not reset or overwrite it while implementing follow-ups.
- The detailed parallel backend audit is in [BACKEND_PROGRESS_AUDIT_2026-09-16.md](pattern_research/BACKEND_PROGRESS_AUDIT_2026-09-16.md), including source locations and exact evidence examples. Its statement about not running a comprehensive suite refers to that bounded audit; the tests above were run by the primary reviewer.
- Next work can be scoped against this baseline without redoing the full familiarization.
