# Proposed evidence-serving contract

Status: design for the future vendor/API and application integration. The current batch produces research artifacts; this contract is not yet implemented in the live scanner.

## 1. Storage boundaries

- **Market history:** immutable normalized candle snapshots in content-addressed files/object storage; a recent-candle cache for live work.
- **Research store:** full run manifests, occurrence/trade/fold archives and indexed research summaries. Keep every attempted cell and its status.
- **Published evidence store:** compact, versioned card records plus a coverage index. Store shared definitions once in a registry, not in every card. Detailed ledgers stay in research storage.
- **Application cache:** only active/recent cards, detection lifecycle records and active-version pointers. Proposed initial cap: 50,000 cached cards and at most 1 KB of card payload each, approximately 50 MB before indexes and database overhead. Measure the physical size before deployment. Eviction removes cache entries, not historical research.

The publication job reads the verified research store. The application requests only compact published cards; it never decompresses a full stock ledger or starts research on a page request. Cache misses can fetch a published card. If no compatible published card exists, return an explicit unavailable state.

## 2. Compatibility identity

Names and version strings alone are insufficient. Resolve a card using:

```text
instrument_id + exchange + timeframe + side + pattern_id + variant
+ detector_spec_hash
+ aggregation_calendar_hash + adjustment_basis_id
+ execution_model_hash + cost_model_hash
+ evidence_population_id + evidence_release_id
```

`detector_spec_hash` covers canonical serialized specification JSON, detector source, shared helpers and detector dependency versions. Retain the human-readable definition version too. The run manifest already stores code/specifications/dependencies; its immutable identity is the research provenance. A published card also records the manifest digest and catalogue-document digest.

The live detector must match the published detector identity. A new vendor, price adjustment convention, aggregation implementation or candle-quality rule requires compatibility review and a new snapshot/release identity. Do not attach evidence through a pattern-name-only fallback.

## 3. Normalized candle contract

Required fields:

```text
instrument_id, symbol_at_time, exchange, timeframe
start_time, end_time, timezone, session_id
open, high, low, close, volume, candle_complete
quality_flags, quality_rule_version
adjustment_basis_id, adjustment_revision
vendor_id, vendor_revision, fetched_at, normalized_snapshot_id
```

Requirements: positive finite coherent prices; chronological nonoverlapping intervals; explicit missing-data flags; no fabricated bars; one declared session/calendar/aggregation policy; corporate-action adjustment and revisions identified rather than guessed. The existing `gap` flag is a data-quality discontinuity and is distinct from a genuine price gap. Vendor adapters must reproduce or explicitly version that distinction.

## 4. Live event identity and lifecycle

Use timestamps and instrument/spec identity as durable keys. Bar indices remain useful inside a particular snapshot, but are not portable keys across fetch windows.

```text
detection_id, instrument_id, detector_spec_hash, pattern_id, variant, side
formation_start_time, detected_at_bar_end, signal_at_bar_end
confirmed_at_bar_end (nullable), episode_id
state, candle_complete, input_snapshot_id, input_quality, geometry
```

Proposed lifecycle: `forming`, `confirmed`, `invalidated`, `expired`. A neutral shape remains neutral until a directional condition is met. A complete recognition is not necessarily a confirmed breakout. Some implemented patterns expose confirmed events only; do not invent an earlier forming state for them.

The historical detector currently emits occurrence events. A live adapter must explicitly maintain active setups and their invalidation/expiry; an old setup in a historical event list is not proof that the shape is still forming now. Provisional incomplete-candle previews, if added, are separately labeled and cannot receive closed-candle research evidence as a validated match.

## 5. Live/research parity gate

Existing prefix tests establish that later candles do not rewrite earlier events. They do not by themselves prove that a short live fetch window is equivalent to the full research history.

Before vendor integration:

1. Declare the required warm-up and retained detector state for each family/timeframe.
2. Compare the most recent event window from full-history detection with start-truncated live windows on real and synthetic data. Normalize indices to timestamps before comparison.
3. Cover pivot replacement, overlapping chart formations, mother-bar clusters, Hikkake confirmation and gap/session boundaries; require positive harmonic cases too.
4. Either pass those checks with a fixed documented warm-up or retain/incrementally update sufficient history and state. A blanket "fetch the last 260 candles" is not yet verified for all new families.

## 6. Card payload and interpretation

Required card fields:

```text
identity, evidence_release_id, run_id, manifest_sha256
population_scope, sample_start, sample_end, evidence_as_of
occurrences, baseline_rule, baseline_trade_count, baseline_mean_net_return
walkforward_trade_count, walkforward_win_rate, walkforward_win_rate_interval
walkforward_mean_net_return, walkforward_mean_interval
fold_count, selected_fold_count, cost_assumptions
coverage_status, sample_status, compatibility_status, freshness_status
research_limitations, detailed_evidence_link
```

Keep compatibility, sample sufficiency and freshness separate. For example, an exactly compatible card can still have too few trades or old data.

| Research result | Proposed user-facing label |
|---|---|
| missing data / insufficient history | Not enough historical data |
| no occurrences | No occurrences in this historical sample |
| no selected walk-forward trades | No selected walk-forward trades |
| fewer than 20 walk-forward trades | Limited historical sample |
| 20 or more, positive or negative mean | Historical walk-forward result |
| different detector/data/execution identity | Incompatible historical evidence |
| source quality unresolved / release withheld | Historical data requires review |

Show the actual positive/negative return next to its sample size. Neither `tested_positive` nor a high fit score becomes "validated", "high probability", or a calibrated forecast. The report's top-return shortlist uses test results to choose what to display and does not constitute an untouched release holdout.

## 7. Population and publication policy

Initial cards use the exact instrument/timeframe/pattern/variant/side population already studied. Insufficient per-stock evidence stays insufficient. Any later pooled universe/regime evidence must have a distinct population identity and label, a predeclared pooling method and dependence-aware uncertainty. Do not silently add overlapping cells together.

Research publication and strategy endorsement are separate decisions. Historical cards can disclose exploratory results. Any stronger strategy-quality claim requires a separately declared release-selection policy and untouched holdout or appropriate multiple-testing treatment.

## 8. Activation, refresh and rollback

Build each evidence release in a staging namespace. Verify artifact integrity, coverage, source/spec compatibility and card labels. Switch an atomic active-release pointer only after those checks, and retain the previous pointer for rollback. Every returned card retains its own provenance.

Proposed initial operations: ingest completed candles continuously, refresh historical evidence monthly, and mark evidence older than 30 calendar days as stale. Treat these as configurable defaults to settle against vendor availability and compute capacity before production. Live-data freshness uses the latest expected completed exchange bar plus the vendor's stated delivery delay, not wall-clock age across weekends or market closures.

## 9. Source-quality release gate

The completed run `8ae6ddc251e80668239e` is reproducible but has publication status `withheld_source_quality_review`. The PIIND audit traces unreliable simulated returns to inconsistent original one-minute rows; the full diagnostic screen flags 37 stocks / 75 stock-timeframe datasets. These findings are recorded in `PIIND_SOURCE_AUDIT.md` and the run's `PUBLICATION_REVIEW.json` / `SOURCE_QUALITY_SCREEN.json`.

The future publisher must require a separate source-quality release decision in addition to checksums and coverage. It must not infer publication eligibility from `coverage_complete` or `tested_positive`. Include `source_quality_status` and `publication_status` in release/card metadata. A withheld release must not become the active evidence release.

Validate source rows before aggregation, including intrabucket discontinuities and intraday/daily range consistency under a comparable session and adjustment basis. Investigate flags against authoritative replacement history; do not silently invent prices or delete only favorable/unfavorable trades. Corrections require a new snapshot and a rerun of affected full detector, ATR and training histories. The coarse diagnostic thresholds are not an exhaustive source certification.

A refresh appends later evidence to a new release. It does not silently rewrite old source snapshots, detector rules or previously published cards. Corrections produce a new explicitly identified revision.

The existing July 2026 snapshot must be labeled historical until newer vendor data are available.

## Review provenance

This design incorporates the independent findings in `CLAUDE_ARCHITECTURE_REVIEW.md`. None of the proposed publication, cache, live-state or vendor-interface work changes the running research batch.
