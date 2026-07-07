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
- **P1** order-id tracking (`entry_order_id`/`exit_order_id` on positions) — foundation.
- **P2** order-driven reconcile engine (PENDING→FILLED→CLOSED, per-session, positive evidence).
- **P3** invariant checker + alerts; re-enable the broker sync in CROSS-CHECK-ONLY mode.
- **P4** GTT-fill confirmation (triggered GTT → its order-id → real fill → CLOSE).
- **P5** guards: no GTT on intraday/MIS legs; token-expiry abort; corp-action alert; P&L clarity.
- **P6** full test matrix — one test per mode.

## Operational note
The old broker-position reconciler is DISABLED via
`FALCON_AUTOTRADE_BROKER_RECONCILE=off` (config/.env) because it corrupted
per-session quantities across same-symbol sessions. Keep it OFF until P3 makes it
multi-session-safe (cross-check-only). Entry-fill / GTT / pre-exit reconciles
still run.
