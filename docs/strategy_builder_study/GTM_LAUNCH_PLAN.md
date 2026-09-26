# GTM launch plan — Strategies, from pilot to cloud (26 Sep 2026)

Owner direction: make the product go-to-market ready, move it to the cloud and launch it at consumer scale. Not a
conservative beta.

Input: the fresh audit `research/gtm-audit-fresh-2026-09-25-2355/` (P01–P20). All 6 probes reproduced on `8881ca8`.

## CTO decisions (replacing the open questions)

| Question | Decision | Why |
|---|---|---|
| Calendars/diagonals | **Fix, don't hide** (P01–P03) | Most of the model already exists. The defects are identity and consistency bugs, not missing math. |
| Off-tick price | The field **snaps to the nearest tick when you leave it**, with a visible "rounded from 120.12". The API rejects off-tick input with a 400. | This meets the blueprint's tick-before-calculate rule without friction for millions of users. |
| Two paper systems | **Merge them into one ledger model with a `mode` field** (`practice_stored` / `paper_live`). One list, one detail, one set of actions; anything a mode can't do shows a stated reason. | Two systems cannot scale or be explained. |
| Scope | **Everything, P01–P19**. Live trading is built and sandbox-certified; arming stays human-gated (Shyam). P20 stays deferred. | GTM needs the whole lifecycle. The execution boundary is law, not conservatism. |

## Owner answers (26 Sep)
- Cloud: **AWS Mumbai (ap-south-1)**.
- Feed: **start with Kite**. Shyam is presenting to Zerodha directly for a redistribution/partner integration. Keep the quote layer vendor-swappable and fanned out through a shared cache, so each user never opens their own feed.
- Load test target: **10k concurrent**.
- Slice 13: **go**.

## Slice 13 — correctness (the P0s, plus the cheap P1s) — BUILT 26 Sep (see BUILD_RESULT_SLICES_1-3.md)
- **P01:** a canonical `contract_id` (exchange|underlying|expiry|strike|type) used by chain select/remove, the leg editor (fetching the selected expiry's chain), duplicate detection, transforms, delta orders, snapshots and review. Expiry appears on every leg label, mobile card and frozen view.
- **P02:** Adjust and roll value the current and proposed positions using `analytics.analyze`, at one horizon, with the same IV, cost and model context. No separate intrinsic profile.
- **P03:** risk labels come from the metric's own basis ("Model at near expiry — 29 Sep 15:30 IST", "modelled extrema at far-leg IV").
- **P04:** fail closed. Any unresolved included leg makes the strategy-wide metrics unavailable, with the missing contract IDs listed. Paper, review and Adjust refuse a body with a dropped leg.
- **P05:** one quote policy (`quotes.book`) for research, spreads, Adjust, fills and alerts. A crossed or invalid book falls back to a labelled indicative price, never "exec". Liquidity is checked per option side.
- **P06:** one probability reference, the chain ATM IV, everywhere (Discovery, spreads, builder, compare, snapshot). Every probability carries its `reference_iv`, source, time, horizon, input hash and model.
- **P07:** structured 400 validation: integer lots > 0, finite positive spot/strike, bounded IV, real expiries, at most 8 legs, on-tick prices.
- **P12:** one pending state keyed by `input_hash`. A draft shows loading instead of "No legs". Out-of-order responses are ignored.
- **P16:** the Paper-traded filter includes deployments.
- Fix the alerts worker's closed-database error on shutdown.
- **Proof:** the six audit probes copied into `server/tests/test_gtm_fresh.py`, plus Playwright checks for the wrong-expiry pick, the edited-expiry header and stale results.

## Slice 14 — lifecycle and UX
- **P08:** the unified paper ledger. `created_at`, `action_at` and `market_sample_at` are stored separately; capital is scoped to its ledger; every run gets Monitor, Alerts, Adjust and Close, or a stated reason.
- **P09:** gross/net with a named exit-cost assumption; per unit / per lot / whole-strategy Greeks and quantities; spread cards show point width, basis and age; rounding reconciles.
- **P10:** evidence badge wrapping (320/390/768 px and 200% text); a single risk cap; a compact grouped expiry picker; a curated beginner recipe list with advanced options behind it.
- **P11:** user-language data status ("Live options connection unavailable; showing 25 Sep 15:30 prices") plus a retry path. Diagnostics move to the operator view. Compact mobile header with persistent risk.
- **P13:** Lab disables unsupported structures before Run; replay copy follows the backend denominator.
- **P14:** alert setup shows the sample's age; versioned threshold edits.
- **P15:** discovery intent is saved on the draft; the draft→rule mapping preflight appears before Prove.

## Slice 15 — cloud and scale
- **Data:** SQLite → Postgres, using migrations (Alembic) with a rehearsed backup/restore.
- **API and workers:** a stateless API behind a load balancer. Lab, alerts, settlement and capture move to a job queue with separate worker pools; no in-process threads in the API.
- **Market data:** fan-out through a shared quote cache (Redis). Users never hit the vendor directly. **Needs a licensed redistribution feed**: a personal Kite session cannot serve other users.
- **Accounts:** real authentication (OTP/OAuth), per-user rate limits and quotas, tenant isolation tests (extending the slice 12 isolation suite).
- **Operations:** observability (metrics, traces, error tracking, SLO dashboards) and alerts on job health.
- **Web:** web build on a CDN; mobile builds for the stores.
- **Load test** to the agreed concurrency target; a security review (OWASP pass, dependency audit, secrets in a vault).

## Slice 16 — live certification
- **P17:** live-quote forward paper, end to end on the licensed feed: deploy → partial fill → monitor → alert → adjust → close → restart recovery.
- **P18/P19:** broker/account identity, capability flags, basket and transition margin bound to account, price and product; sandbox drills (partial hedge, rejected short, unknown submit, cancel/replace, restart, duplicate intent).
- A dated gate record per P17. **Arming live stays with Shyam**; agents emit intents only.
