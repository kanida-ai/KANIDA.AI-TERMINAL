# Spec — Signal Tier + Signal-Day Data in the Picks Panel

Branch: `feat/portal-signal-tier` (off `main`). Status: **IMPLEMENTED 2026-06-18.**

## IMPLEMENTATION SUMMARY (what shipped)

Backend (100 power_user tests green; verified end-to-end on real 2026-06-18 data —
output matches `outputs/tier_classification.xlsx` exactly):
- NEW `backend/power_user/services/signal_tier.py` — `classify_signal_tier()` (ported
  from `scripts/export_tier_excel.classify`) + `enrich_picks()` (one batched
  `ohlc_daily` query; computes signal-day return, 2-day return, RVOL, 3d/20d, turnover
  %ile; assigns the tier in-place; never raises; factory-independent).
- `explainer.py` — `PICK_SCHEMA_VERSION 1→2`; 5 new OPTIONAL keys (`signal_tier`,
  `signal_tier_reason`, `signal_tier_color`, `signal_day_ret_pct`, `two_day_ret_pct`);
  validator extended with the signal-tier enum; `build_pick_payload` emits the fields.
- `picks_router.py` — enrich on `/picks/today(+preview)` and best-effort on `/picks/live`.
- `replay_cache.py` — enrich on replay compute + **stale-schema guard** (`_is_current_schema`)
  so pre-bump cache entries auto-recompute instead of failing validation.
- Tests: NEW `test_signal_tier.py` (11) + updated `test_explainer.py` / `test_picks_router.py`
  for v2.

Frontend (type-safe; `next build` to be run in deploy env — worktree has no node_modules):
- `power-api.ts` — `SignalTier` type, `TierColor` += `red`, optional fields on `Pick`+`LiveDecision`.
- `PickCard.tsx` — `SignalTierBadge` + `SignalDayStrip` (exported), rendered in the header.
- `ExpandablePickRow.tsx` — badge + strip in the compact list row (the main scan view).
- `TierBadge.tsx` — `red` color class.

Result on 6/18 (breakout day): 6 AVOID / 3 STANDARD / 1 GOLD-baseline — honest, no
over-promotion. PREMIUM-Pullback (~83-85% cell) is NOT marketed as certified-80%.

---


## 1. Requirements

Add to the picks panel the user already sees, **per pick**:
1. **Signal Tier** badge — the derived signal-quality tier: `GOLD / ENTERPRISE / PREMIUM-Pullback / PREMIUM-Compression / STANDARD / AVOID` (+ baseline/dry-up variants).
2. **Signal-day price numbers** — `signal_day_ret_pct` and `two_day_ret_pct`.

Locked decisions (operator, 2026-06-18):
- **Alongside**: keep the existing rank-conviction tier (ELITE/HIGH/MID… that drives ENTER/WAIT/SKIP); the Signal Tier is a *second* badge. Two orthogonal axes.
- **Panel shows only** signal-day return % and 2-day return %. Volume features (RVOL, turnover %ile, 3d/20d) are computed **internally** to assign the tier but are **not** displayed.
- Top-10 unchanged (no Top-20).

Tier rules = the OOS-validated derivation (v1 dry-up + v2 pullback), classifier ported from `scripts/export_tier_excel.py::classify`.

## 2. Design

### 2.1 Data source (grounded)
`falcon_features` has `range_pct`, `vol_vs_20d` (≈ rvol20) but **NOT** signal-day return or 2-day return. So enrichment computes from `ohlc_daily`:
- `signal_day_ret_pct = close/prev_close - 1`
- `two_day_ret_pct = close/prev_prev_close - 1`
- volume (for tier only): `turn_pct` (252d turnover %ile), `trend3_20` (3d/20d avg vol); `rvol20` reuse `feat_vals['vol_vs_20d']`; `range_pct` reuse `feat_vals['range_pct']`.

### 2.2 Backend
- **New module** `backend/power_user/services/signal_tier.py`:
  - `classify_signal_tier(sret, twoday, rng, avg_lift, trend3_20, turn_pct) -> (tier, reason)` (port of the verified classifier).
  - `enrich_picks(con, picks, signal_date)` — ONE batched `ohlc_daily` query for all pick symbols; computes the features above; writes `signal_tier`, `signal_tier_reason`, `signal_day_ret_pct`, `two_day_ret_pct` onto each pick dict. Sub-200ms (single query + vector compute).
- **Chokepoint** `explainer.build_pick_payload`: emit the 4 new fields (read from enriched pick; default `None` if absent — safe for callers that don't enrich).
- **Schema**: bump `PICK_SCHEMA_VERSION 1 → 2`; add the 4 keys to `PICK_OPTIONAL_KEYS` (not REQUIRED — avoids a hard contract break and keeps replay/live builders valid). Extend the tier-enum check to allow the new Signal-Tier enum on the new field only (existing `tier` enum unchanged).
- **Routers**: `_build_today_picks` calls `enrich_picks` after `compute_top_n`, before payload build → covers `/picks/today` + `/picks/today/preview`.
- **Replay cache**: schema bump auto-invalidates (router already raises CACHE_DRIFT → recompute). Enrich inside the replay compute path too.
- **Live panel** `/picks/live` (`live_tier.get_decisions`): compute Signal Tier on-read by joining `ohlc_daily` for the decision's `signal_date`+symbol (no `falcon_live_decisions` schema change). Lower priority — can ship in a 2nd commit.

### 2.3 Frontend
- `TierBadge.tsx`: add the Signal-Tier values + colors (GOLD=amber, ENTERPRISE=violet, PREMIUM=teal, STANDARD=slate, AVOID=red). Keep existing conviction-tier badge intact.
- `PickCard.tsx` / `ExpandablePickRow.tsx`: render the Signal-Tier badge next to the conviction badge + a one-line `sig-day +X.X% · 2-day +Y.Y%` strip. Guard on `null` (older payloads / live degraded).

## 3. Tasks (ordered, each testable)
1. `signal_tier.py` + unit test (classifier parity vs `export_tier_excel.classify` on a fixture).
2. Enrichment + `build_pick_payload` fields + schema bump; update `validate_pick_payload`.
3. Backend test: `/picks/today` payload contains the 4 fields, validator passes, enum correct.
4. Replay path enrichment + cache-recompute test.
5. Frontend badge + strip + null-guards.
6. (2nd commit) `/picks/live` on-read tier.
7. Regression: existing pick/explainer/validator tests green.

## 4. Risk & rollback
- Optional-key schema bump = additive; old clients ignore new fields. Risk: LOW.
- Cache recompute is automatic + already handled.
- Rollback = revert the branch; no DB migration, no live-path mutation in commit 1.
- Never touches the auto-trade execution path or the frozen `prod` tree.
