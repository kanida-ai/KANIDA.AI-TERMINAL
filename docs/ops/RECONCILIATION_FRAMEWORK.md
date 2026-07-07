# AutoTrade Reconciliation Framework

Canonical reference for panel↔broker reconciliation. Read this before ANY
reconciliation change. Visual: the artifact at
https://claude.ai/code/artifact/2d963c6a-98dd-4058-93d8-e88159b00719

## Why this exists
Panel↔broker mismatches kept recurring because each was patched as a single case.
Root cause is structural: reconciliation lived in five event-scoped surfaces
(entry-fill, GTT-fill, pre-exit, boot-resume, broker-position sync), none
authoritative, and the broker books positions **per (symbol, product), aggregated
across every session on the account** — so reconciling one session against the
account net is impossible. On 2026-07-07, three same-pick sessions had every
quantity overwritten to the account total by exactly that mistake.

## The four principles (the contract)
1. **Order-id is the atomic unit of truth.** A position is reconciled through the
   broker orders that opened/closed it (entry/exit/GTT order-ids owned by that
   session). The account net book is a CROSS-CHECK ONLY — never write a session's
   quantity from the aggregate.
2. **Positive evidence only.** Mutate on a confirmed order event (COMPLETE fill,
   REJECTED, triggered-GTT-whose-order-filled). Never infer from net-0 / absence /
   aggregate.
3. **A continuous invariant.** Each cycle, for every (symbol, product):
   `Σ open-position qty (all sessions) == broker net qty + holdings`.
   Violation → alert + order-id resolution; never a blind aggregate correction.
4. **Fail safe.** Unreachable / delayed / empty broker → strict no-op. Ambiguity
   is flagged, not guessed. A stale-but-flagged row beats a false close / phantom.

## 30-mode catalog (status: SAFEGUARDED / TO BUILD / HIT-LIVE)
- **A Entry** (A1 pending-not-filled, A2 partial, A3 reject-upfront, A4
  async-reject, A5 poll-timeout, A6 mark-vs-fill)
- **B Hold-intraday** (B1 RMS auto-square, B2 GTT fired, B3 manual exit,
  B4 multi-session same symbol)
- **C Hold-overnight** (C1 T+1 settlement, C2 overnight GTT, C3 corporate action,
  C4 multi-day carry)
- **D Exit** (D1 exit rejected, D2 confirm missed, D3 double-exit, D4 exit price,
  D5 partial exit)
- **E Infra** (E1 API error, E2 empty book, E3 restart, E4 postback missed,
  E5 token expired, E6 duplicate order)
- **F State/P&L** (F1 stale basis, F2 phantom, F3 orphan, F4 P&L gross/net,
  F5 GTT orphan / GTT-on-intraday)

Full scenario/safeguard per mode: see the artifact.

## Build plan (phased, reviewed)
- **P1** order-id tracking (`entry_order_id`/`exit_order_id` on positions) — foundation. DONE.
- **P2** order-driven reconcile engine (per-session, positive evidence). DONE (v2 reconciler).
- **P3** invariant checker + alerts; re-enable the broker sync SAFELY. DONE.
- **P4** GTT-fill confirmation (triggered GTT → its order-id → real fill → CLOSE). DONE (`get_gtt_fill`).
- **P5** guards: no GTT on intraday/MIS legs; token-expiry abort; corp-action alert; P&L clarity. BUILT (branch, awaiting review/deploy).
- **P6** full test matrix — one test per mode.

## P5 GUARDS (built — additive, paper byte-for-byte unchanged, no falcon_position_state)
- **G1 (F5) NO GTT on MIS legs** — choke-point `monitoring/gtt_manager.py:place_for_position`
  gated by `_gtt_allowed_for_product(product)` (CARRIED CNC/MTF/NRML → place;
  MIS → SUPPRESSED_INTRADAY, records levels only, gtt_id NULL). Product resolved
  via `_effective_product(prof_id)` (per-profile order_product → session
  order_product) normalised with the SAME EQ→CNC rule the reconciler's
  `_kite_product` uses. Belt-and-suspenders `sweep_intraday_gtts()` cancels any
  stray MIS gtt_id + clears it; wired on the MIS square-off path
  (`square_off_scheduler`, additive to the kill-switch full GTT-cancel sweep).
- **G2 (E5) token-expiry abort** — `broker/zerodha.py:_token_abort_reason()` gates
  `place_order` (entry) + `place_market_exit` (exit) AFTER `_live_allowed()` (paper
  bypassed). O(1), NO network on a valid token: reads `get_cached_token_status(60s)`
  (a status the admin widget / auth scheduler / a prior order already computed);
  if fresh+valid → proceed (never calls `profile()`); fresh+invalid → FAILED
  `TOKEN_EXPIRED`; no fresh cache → O(1) `token_present()` (DB/env read) → FAILED
  `TOKEN_MISSING` when absent. Bound per-account clients validate their own token
  in `_build_kite` (skipped here). New `services/kite_auth.py`:
  `get_cached_token_status` + `token_present` + a side-cache in `get_token_status`.
- **G3 (C3) corp-action alert** — `position_reconciler.py:_corp_action_ratio()` — a
  CLEAN split/bonus multiplier {2,3,5,1.5,2.5,10} (±2% tol) between broker_held and
  db_held_all → distinct `CORP_ACTION_SUSPECTED` alert (kind on
  autotrade_recon_alerts, carries the ratio) INSTEAD of the generic
  ORPHAN_AT_BROKER (surplus) / UNATTRIBUTED_CLOSE (deficit). Reverse split →
  reciprocal. NEVER mutates. Non-clean diff → stays generic.
- **G4 (F4) P&L gross/net** — pure `autotrade/charges.py:estimate_charges(product,
  buy_value, sell_value, legs)` → {brokerage, stt, exchange, gst, stamp, dp, total}
  (Zerodha equity rates in one `_RATES` block; ESTIMATE, no live API). Journal read
  path ADDS `realised_pnl_net` + `charges_estimate` + a `realised_pnl_gross` mirror
  per position AND session-level `total_realised_pnl_net`/`total_charges_estimate`
  — the existing GROSS `realised_pnl` / `total_realised_pnl` are UNCHANGED.

## P2+P3 reconciler (v2) — order-id-driven, invariant-based
`monitoring/position_reconciler.py` was REWRITTEN. The old aggregate qty-correction
(`set_qty` from the account net) and the close-on-net-0-without-order-evidence
paths are GONE. Each cycle, per (symbol, product):

    Σ open qty (ALL sessions on the account) == broker net qty + holdings

Decision table:
- `==` → in sync, NO action (this is what makes multi-session same-symbol safe:
  30+21==51 CNC and 326==326 MIS both hold; nothing is touched).
- `broker < db` → resolve PER POSITION on POSITIVE order-id evidence only, in a
  deterministic order (by row id), closing until the invariant is met:
  `gtt_id → get_gtt_fill` COMPLETE, then `exit_order_id → get_order_status`
  COMPLETE. An unresolved remainder → **UNATTRIBUTED_CLOSE** alert (nothing closed).
- `broker > db` → **ORPHAN_AT_BROKER** alert (never adopt / mutate).

Product is resolved per session from `config_json.order_product` (CNC/MIS/NRML/MTF,
EQ→CNC) and the broker net row's `product` field, so a same-symbol CNC leg and MIS
leg never cross-contaminate. Account-wide sum is scoped to the reconciling
session's broker profile(s). After any close → `refreeze_invested_basis()`.

Alerts are logged at WARNING and persisted to `autotrade_recon_alerts`
(id, ts, session_id, symbol, product, kind, detail). **Alerts NEVER mutate a
position** — a stale-but-flagged OPEN beats a false close.

## Operational note
`FALCON_AUTOTRADE_BROKER_RECONCILE` now gates the SAFE v2 reconciler and defaults
**on** (only `off`/`0`/`false`/`no` disables). The v2 reconciler is order-id-driven
and multi-session-safe — it never writes a session's qty from the account
aggregate. Fail-safe unchanged: unreachable/expired (net book None) / empty book /
paper → strict no-op. Entry-fill / GTT / pre-exit reconciles still run.
