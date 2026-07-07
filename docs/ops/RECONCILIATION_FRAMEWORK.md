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
- **P5** guards: no GTT on intraday/MIS legs; token-expiry abort; corp-action alert; P&L clarity.
- **P6** full test matrix — one test per mode.

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
