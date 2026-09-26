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




## Cloud preview decisions (owner, 26 Sep)
- Only the NEW app (Strategies, kanida-app) goes to the cloud, at **strategies.kanida.ai**, in the same AWS account (Mumbai), as its own separate service with its own database.
- The OLD app (engine / Power User portal, api.kanida.ai, kanida-prod-svc) is NOT deployed with it. It keeps running untouched; we switch it off together later (backup first, step by step).
- The old app's parts are reused INSIDE the new app as its own features (copied/adapted, not linked): logo/branding, Google sign-in + invite codes + waitlist, admin panel (invites, waitlist, users), jobs panel, plus the AWS infrastructure pattern (ECS/ALB/ECR/CI). The two apps are never mixed.
- Users start fresh: the owner is the only admin and invites testers from the new admin panel.
- Market data: capture runs in the cloud (Kite credentials entered by the owner into AWS Secrets Manager; the Mac capture stops at switch-over, outside market hours).
- Mobile: native iOS + Android (Expo EAS; TestFlight + Play internal testing; owner creates the Apple/Google developer accounts).
- Sized for speed (CDN for web, fast storage, shared quote cache). Access restricted: invite-only.

Cloud preview steps: (1) sign-in/invites/admin/jobs panel/logo inside the new app → (2) container + AWS service at strategies.kanida.ai (private) → (3) cloud capture → (4) iOS/Android test builds → (5) owner iterates → Zerodha.

## REVISED ORDER (owner-approved 26 Sep) — the adjustment-evidence product

**Product:** an intelligent options strategy simulator (not automated trading). The core screen is for a trader already in a
position: compare **keep / reduce / hedge / roll / exit** on the same assumptions, with the original loss always visible.
It says what each choice helps, hurts, costs and still leaves at risk. Analysis, never instructions.

**Moat:** a growing, validated record of when adjustments help, hurt, or have insufficient evidence. It comes from
**branching experiments**: the same position is split into every choice and each branch follows the same future market.
History (the 10-year NSE bhavcopy, daily) gives years of conditions now; the live paper lab adds realistic fills going
forward. Principles are pre-registered, tested on later unseen periods, and promoted or retired only with human approval -
never rewritten nightly from that day's winners.

| Slice | Scope |
|---|---|
| 14 | Engine integrity (E03–E07, E10, E12) + Strategies lifecycle/UX (P08–P15) + **an immutable decision log** (every strategy, adjustment, paper fill and outcome, with consent flags), so the data clock starts now |
| 15 | Cloud move (AWS Mumbai): Postgres, queue workers, shared quote cache, auth, observability, 10k-concurrent load test; E01 identity/master, E02 immutable archive |
| 16 | **Adjustment Decision screen**: keep/reduce/hedge/roll/exit on one screen, common horizon and assumptions, original loss visible, assumption sliders ("does the improvement survive?"), plain-language trade-offs; comprehension test with users |
| 17 | **Live paper lab**: thousands of branching experiments daily across stocks, strategies and triggers (structured randomisation), realistic fills (spread, cost, partials), failures and non-fills kept |
| 18 | **Principles registry**: candidate principles tested out of sample on unseen periods; promote/weaken/retire with human approval; drives the "in comparable experiments…" evidence on the slice-16 screen |

Owner focus (26 Sep): engine fixes, the adjustment IP + live paper lab moat, and launch readiness for millions. The 10-year historical backtest is parked.
| later | **Historical branching lab on the 10-year bhavcopy (parked by owner 26 Sep)**, Thesis Continuity, Protection Frontier, live-trading certification (P17–P19/E08–E09, when the broker agreement lands), Pressure Transport / Liquidity Survival |

(This supersedes the earlier slice 16 = live certification and 17–19 = Atlas/Thesis/Frontier ordering above.)

## Bound in: Derivatives Intelligence study (research/derivatives-intelligence-2026-09-26), 26 Sep

Probes re-run on `7094f68`: **2 pass / 10 fail** (unchanged; slice 13 touched Strategies, not the engine).

### Engine integrity gates E01–E12 → where each lands
| Gate | What | Slice |
|---|---|---|
| E03 schema/reader | Builder crashes on a FRESH capture DB (`no such column: s.bid`); the cloud DB starts empty. Optional columns + reader contract tests on fresh and migrated schemas | **14 (first)** |
| E07 costs | Option sale STT 0.1% → 0.15% and exercise 0.125% → 0.15% (NSE, from 1 Apr 2026); futures sale 0.05%. Effective-dated schedule (the Lab also uses point-in-time rates back to 2016); bump EVIDENCE_VERSION; re-run grids, keeping superseded reports | **14** |
| E05 pricing | σ=0 with T>0 uses the discounted strike; NaN price → no IV; positive intraday time on expiry morning; corrected dividend wording | **14** |
| E04 time/population | Volume baseline cut at bar END; matched-cohort ΔOI (births/deaths separate); explicit coverage | **14** |
| E06 metrics/language | A missing PCR side is never 0; Max Pain help = payout minimisation (range + ties); price/OI labels say "consistent with writing", never who did it | **14** |
| E10 evidence validity | S/N control never uses itself; episode/session-level counts; 4 sessions labelled exploratory | **14** |
| E12 reproducibility | Fixed-clock fixtures (the 6 date-sensitive `test_derivatives.py` failures I've been carrying) | **14** |
| E01 identity/master | Permanent contract ID + effective-dated vendor-token alias (Kite reuses tokens); lot/tick versions | **15** (designed into the Postgres schema) |
| E02 immutable archive | Append-only observations with source revisions; cold checksummed archive before hot prune; as-known replay | **15** |
| Snapshot scope | Interpretation snapshot key gains expiry + rules version (two expiries coexist) | **15** (migration) |
| E08 paper realism | Depth-constrained partial fills, per-contract mark age, exit rules on NET P&L (realised + unrealised − costs), trailing/time rules | **14** (mark age, net basis) / **16** (depth fills) |
| E09 margin/settlement | = P19/P18 | **16** |
| E11 rule/hedge lifecycle | Typed EQ/FUT/OPT legs, versioned rules | **17–19** |

### IP build (after the integrity gate G0, in the study's dependency order)
| Slice | Product | Gate |
|---|---|---|
| 17 | **State Transition Atlas**, descriptive first (A1–A5): versioned episodes, matched-contract features, a comparable-history panel that says "history insufficient" rather than invent a confidence | G1 |
| 18 | **Thesis Continuity Engine** (T1–T6): immutable entry thesis with predicates → supported/contradicted/unknown/expired; incidents; review timeline. Builds on the slice-14 unified paper ledger | G2 |
| 19 | **Protection Frontier** (H1–H6): typed EQ/FUT/OPT holdings, five hedge templates with exact identities, Pareto frontier, transition funding | G3 |
| later | Pressure Transport, Liquidity Survival (need BBO/depth history), Demand Attribution (needs trade prints - not faked from OI) | research |
| gated | **G5 validated historical intelligence**: calibrated probabilities are released only after prospective validation on enough independent sessions. The data clock starts when capture starts | time |

Moat action that cannot wait: history cannot be reconstructed later, so capture of timestamped BBO/depth for a liquid universe must start as early as possible (owner decision on scope/cost).

## Slice 14 — engine integrity (E03–E07, E10, E12) + lifecycle and UX
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
