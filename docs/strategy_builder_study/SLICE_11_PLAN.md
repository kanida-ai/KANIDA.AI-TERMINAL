# Slice 11 — everything the GTM audit left after slice 10

Date: 26 Sep 2026 (IST).
- **Inputs:** the GTM audit (`research/gtm-audit-2026-09-25/02-pending-prd.md`) and `SLICE_10_PLAN.md` §2 (deferred list).
- **Scope (owner, 26 Sep):** all 13 remaining items in one slice.

**Owner decisions (26 Sep):**
- **Naming:** the top-nav scanner "Discover Strategies" is renamed **"Market scans"**. Options matching stays "Find a strategy".
- **BANKNIFTY/FINNIFTY Lab calendar:** parked again (slice 12 or later).
- **Live trading:** stays gated. From P15/P16 only the non-live parts are built (unknown-outcome state, reconcile by key). No broker call is added.

## Build order (backend first, then screens; each item gets a test, then a desktop and a 390 px check)

| # | GTM | Backend | Frontend |
|---|---|---|---|
| 11.1 | P22 durable Lab jobs | A bounded worker pool replaces one thread per run; queued/running/cancelled states; cancel; runs left `running` by a restart are recovered as failed ("server restarted"); a per-user limit on active runs; a run manifest (data range, rows, lot, fee version, model version, request hash). | Cancel button, queue position, manifest in "Data and assumptions" |
| 11.2 | P23 replay coverage | A missing leg blocks the replay; per-leg expected vs available bars; requested vs effective window and interval; the fallback honours the requested window; a P&L timeline. | Coverage table, a changed-request notice, a timeline chart |
| 11.3 | P18 records | Snapshot rename and notes (the checksum is unchanged); library sort; expired/unresolved badges; deployment summaries in the library. | Read-only snapshot detail (frozen terms and analysis), notes, sort, badges |
| 11.4 | P20 monitor | A deployment detail endpoint: last-known valuation with its age when marks fail; residual exposure (held vs planned). | `/strategies?view=deployment&id=` page |
| 11.5 | P21 alerts | Edit cooldown, session and channel; suppression shown on the rule; mutation errors keep the form. | Create alert from the strategy header; settings; retry |
| 11.6 | P15 (non-live) | The bridge stores a durable "submitting" record before the call; a transport error becomes `unknown` (not `refused`) and is reconciled by the idempotency key; no second basket. | An "Outcome unknown - reconciling" state |
| 11.7 | P17 start flow | — | Choose the underlying before a draft exists (no empty drafts); template search and filters; replacing legs shows a preview and offers undo |
| 11.8 | P05 safe edits | — | Shift/Width/Wings only where they fit, with a preview and no silent clamp; undo/redo; full expiry picker with a remap preview; repair-as-new for expired |
| 11.9 | P14 hierarchy | — | 4 primary numbers (max loss, max profit, breakeven, required funds); the rest expandable; Scenario labels |
| 11.10 | P12 mobile | — | Compact builder header with an actions menu; one-leg editor sheet; cards for spreads/compare/Lab tables |
| 11.11 | P08 states | — | Loading, error and Retry on every screen (library, templates, About, alerts, runs, paper) |
| 11.12 | P11 navigation | — | "Market scans" rename; Strategies root route; one scoped status banner |
| 11.13 | P13 a11y | — | Focus return, labelled fields and errors, semantic table labels, keyboard path through build → review |
| 11.14 | P24 evidence | Privacy-safe funnel and failure events (start → analyze → save → review → deploy); p50/p95 latency per route at `/api/sb/ops/metrics` (owner only). | — |

Out of scope: live broker lifecycle (P16 live parts), multi-expiry, share links, closed-tab push.

## Result (26 Sep 2026)

All 14 rows are built. After the independent review and quant audit fixes, the full suite is 692 passed (the 6 older derivatives failures predate this slice). Screens were checked at 1440 px and 390 px on the fake-live-quote harness.

**Still partial (stated in the workbook):**
- P12: real-device checks.
- P13: assistive-technology acceptance.
- P15: the engine needs a lookup-by-key endpoint.
- P24: isolation and backup drills.
