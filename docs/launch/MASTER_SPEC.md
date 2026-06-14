# Kanida.AI — Stage 1 Launch Master Spec

**Status:** DRAFT — awaiting operator approval
**Author:** Claude (orchestrator)
**Date:** 2026-06-13 (IST)
**Scope:** Stage 1 only — take the *existing* shared-content product to market, hosted, paid. No per-user broker connect (that's Stage 2).

> **You read this document ONCE.** After you approve it, you do not re-review requirements. You only receive audit reports per module. Nothing merges on a RED audit.

---

## 0. Commercial model (LOCKED 2026-06-13)

| Item | Decision |
|---|---|
| Currency | INR (₹) |
| **Access model** | **Invite code → free. No code → pay.** No trial. |
| Price (paid) | **₹999 / month** (no annual tier at launch) |
| Free via code | Existing power users + anyone you hand a code to → `comp`/`founding`, never charged, never gated |
| Payment provider | **Razorpay** (UPI / NetBanking / cards) |

**No trial machinery** — this removes the trial columns, the trial-expiry scheduler, and the trial emails. The existing `power_user_invite_codes` system *is* the free lever; Razorpay is the only new payment path.

## 0.1 Infra targets (LOCKED 2026-06-13)

| Item | Decision |
|---|---|
| App DB | **Supabase (managed Postgres)** — the one DB every user request hits |
| Research warehouse (14G) | **Stays on your machine** — offline, batch-only, publishes patterns to App DB weekly |
| Backend host | Hosted always-on (Railway/VPS) — off the laptop |

---

## 1. What we are launching (grounded in current code)

The product **already exists and works**. We are NOT building product features. We are wrapping the live product in the commercial + operational layer it lacks.

**Already built (do not touch the logic):**
- 6 Co-Trader portfolios (BTST, Daily, Weekly, Monthly, Patient, Falcon Top 10) — live equity curves, positions, trades, performance — `power_user/services/portfolio_*.py`, `persona_*.py`
- Daily Top-10 picks + 3-bucket explainability — `picks_router.py`, `falcon_top20_explainer.py`, `pattern_narrator.py`
- Live intraday tier, position sizing — `live_tier.py`, `portfolio_sizing.py`
- Auth (invite-code → JWT), admin, waitlist, replays — `auth.py`, `invites.py`, `replay_cache.py`
- Daily V7 pipeline that feeds all of the above — shared, one operator Kite token for market data (correct, permanent)

**What's missing (this spec builds exactly these, nothing more):**
1. **Infra** — backend off the laptop, onto always-on hosted infra
2. **Billing** — Razorpay subscriptions + paywall gate
3. **Signup** — open email signup + 7-day trial (no invite code needed)
4. **Legal** — Terms, Privacy, Refund, Risk Disclosure pages
5. **Email** — minimal lifecycle (welcome, trial-ending, expired)

**Explicitly OUT of scope (Stage 2, not now):** per-user broker OAuth, per-user holdings/P&L, per-user execution, referrals, multi-seat, usage-tiered rate limits, Long-Term engine (product roadmap, slide 2 "Building").

---

## 1.5 DB end-state architecture (the consolidation)

**Principle: two databases separated by workload, with a one-way publish wall. NOT one common DB.**

```
APP DB  (Supabase Postgres, hosted, multi-user)      RESEARCH WAREHOUSE (on your machine, offline)
  every user request hits ONLY this           ◄──── publishes small pattern set, weekly (B4)
  • power_user_* (users, billing, subs)               • ohlc_1min (87.8M), falcon_outcomes (827k)
  • signals, features, promoted_patterns              • pattern mining, backtests, winB experiments
  • 5 persona portfolios + equity/trades              • NEVER in a request path
  • kite_tokens, falcon_auth_log (folded in)
  • falcon_trade_* (operator), position_state
  • precomputed historical-evidence table
  • [Stage 2] user_broker_tokens, user_positions
```

**Current → end-state mapping:**

| Today | End state |
|---|---|
| `data/db/kanida_universe.db` (573M PROD) | → ported into **App DB (Supabase)** |
| `data/db/kanida_quant.db` (83M legacy) | `kite_tokens` + `auth_log` **fold into App DB**; `live_opportunities` **dropped**; file **retired** |
| `universe_engine/data/db/kanida_universe.db` (14G R&D) | **stays on machine** as research warehouse |
| `kanida_universe_winB.db`, `intraday_mining.db` | research scratch — stay on machine |
| `.claude/worktrees/**/kanida_quant.db` (×5 stale) | **delete** (worktree leftovers) |

**The one coupling to fix:** the "historical evidence" bucket currently reads the 14G R&D DB at request time (`POWER_RND_DB_PATH`, `falcon_outcomes`). End-state: precompute per-stock evidence into an App-DB table during the nightly job, so no user request reaches across the wall. (Tracked in M2 + a follow-up job.)

**Forward-compatibility check:** Stage 2 (per-user broker) adds `user_broker_tokens` + `user_positions` tables to the *same* App DB, keyed by `user_id`. No re-architecture. The 5th product (Long-Term) is new persona rows, not new schema.

---

## 2. Non-negotiable invariants (every agent obeys these)

| ID | Invariant |
|---|---|
| INV1 | **Existing power users are never disrupted.** Their invite-code login + JWT keep working unchanged. They are tagged `founding` → bypass all billing gates forever. |
| INV2 | **Auto-Trade (Falcon operator product) is never disturbed.** No changes to `backend/falcon/*` execution paths. |
| INV3 | **No mock data, ever.** All data from DB. (Repo rule.) |
| INV4 | **All time/date in IST.** `datetime.now(timezone(timedelta(hours=5,minutes=30)))`. |
| INV5 | **No secrets committed.** Razorpay keys, JWT secret, etc. → env vars only. `config/.env` never committed. |
| INV6 | **Power User code stays in `power_user/`.** Never mixed into legacy `routers/` or `falcon/`. |
| INV7 | **Additive DB migrations only.** New columns nullable / defaulted. No destructive ALTERs. Existing rows keep working. |

---

## 3. Module breakdown — one build agent + one audit agent each

Build order respects dependencies. `DB` is first (everyone depends on schema). `Frontend` is last (depends on all backend contracts).

```
M1 Infra ──────────────┐
M2 DB ─────┬───────────┤
           ├─ M3 Billing┤
           ├─ M4 Paywall├── M7 Frontend
           └─ M5 Signup ┤
M6 Legal ──────────────┘   (Legal + Email are independent leaves)
M8 Email ──────────────┘
```

### M1 — Infra (move off laptop, host it)
- **What:** Deploy backend to hosted always-on infra; stand up **Supabase Postgres** as the App DB; port the PROD `kanida_universe.db` into it; **fold** legacy `kite_tokens` + `falcon_auth_log` in and **retire** `kanida_quant.db`; leave the 14G R&D warehouse on your machine; verify daily pipeline + Kite token refresh fire on the host.
- **How:** `Dockerfile` + `railway.json` already exist; code already supports `DATABASE_URL`. Provision Supabase. Write a one-time porting script (SQLite → Postgres) for the App-DB tables. Zerodha auth refresh becomes a host-side scheduled job (replaces the Windows Scheduled Task) — verify Playwright works headless on Linux.
- **Where stored:** Hosting config in `deploy/`; porting script in `scripts/migrate_to_supabase.py` (one already exists — extend it); env manifest in `docs/launch/ENV.md`.
- **New env vars:** `DATABASE_URL` (Supabase), `POWER_JWT_SECRET` (CRITICAL — currently random per boot → must be fixed), `SITE_USER`/`SITE_PASS`, all `KITE_*` + `ZERODHA_*`.
- **Acceptance:** Backend reachable off-laptop; `/power/today` loads from Supabase; daily pipeline writes signals into Supabase; token refresh succeeds headless; `kanida_quant.db` no longer referenced; laptop can be closed without 503.
- **Risk flag:** Highest-uncertainty module (external infra, Playwright headless, SQLite→PG type/SQL differences). Audit agent verifies token refresh actually works headless and that no code still reads `kanida_quant.db`, not just that the container boots.

### M2 — DB (schema migration)
- **What:** Add billing columns to `power_user_users`; create `power_user_subscriptions` + `power_user_billing_events` tables.
- **How:** Additive migration (INV7), written for **Postgres** (App DB target). New columns: `billing_plan TEXT DEFAULT 'founding'` (values: `founding`/`comp`/`paid`/`blocked`), `razorpay_customer_id TEXT NULL`, `subscription_status TEXT DEFAULT 'active'`. **No trial column.** **Backfill: every existing row → `billing_plan='founding'`, `subscription_status='active'`** (INV1). Relax `google_sub NOT NULL` (open email signup writes synthetic `email:<sha256>` — coordinated with M5).
- **Where stored:** `backend/power_user/db_schema.sql` (append) + `backend/power_user/migrations/0001_billing.sql` (new).
- **Plan semantics:** `founding` = original power users (free forever) · `comp` = free via invite code · `paid` = active Razorpay sub · `blocked` = lapsed/cancelled.
- **Acceptance:** Migration runs idempotently on a copy of the ported DB; all existing users come out `founding`; rollback script exists; no existing query breaks.

### M3 — Billing (Razorpay)
- **What:** Razorpay subscription lifecycle — create customer, create subscription/checkout, handle webhooks (activated, charged, halted, cancelled).
- **How:** New `billing_service.py` (Razorpay SDK wrapper) + `billing_router.py`. Endpoints: `POST /api/power/billing/create-subscription`, `GET /api/power/billing/status`, `POST /api/power/billing/webhook` (signature-verified), `POST /api/power/billing/cancel`.
- **Where stored:** `backend/power_user/services/billing_service.py`, `backend/power_user/routers/billing_router.py`. Writes to `power_user_subscriptions` + `power_user_billing_events`.
- **New env vars:** `RAZORPAY_KEY_ID`, `RAZORPAY_KEY_SECRET`, `RAZORPAY_WEBHOOK_SECRET`, `RAZORPAY_PLAN_MONTHLY` (₹999/mo plan id).
- **Acceptance:** Test-mode subscription completes end-to-end; webhook updates `subscription_status`; signature verification rejects forged webhooks; idempotent on duplicate webhook delivery.

### M4 — Paywall (gate the product)
- **What:** A FastAPI dependency `current_paid_user_required` that allows: `founding`, `comp`, `admin`, or `paid` with `subscription_status='active'`. Blocks everyone else with a structured `PAYMENT_REQUIRED` (402) carrying the Razorpay checkout link.
- **How:** New dep in `dependencies.py` reusing existing `current_user_required` + a DB read of billing columns. Apply to `picks_router`, `portfolios_router`, `persona_backtest_router`, `falcon_top20_router`, `live_tier`. Leave public: `replay`, `credibility`, `waitlist`, `auth`, `billing`.
- **Where stored:** `backend/power_user/routers/dependencies.py` (extend); wire into the 5 routers.
- **Acceptance:** Founding/comp user → full access; paid active → access; no-code unpaid → 402; cancelled (`blocked`) → 402. Auto-Trade routes untouched. (No trial branch.)

### M5 — Signup (open the funnel)
- **What:** `POST /api/power/auth/signup` — email only. Two outcomes: **(a) with a valid invite code → free `comp` account** (existing invite flow, reused); **(b) no code → account created as `blocked`, redirected to Razorpay checkout; flips to `paid` on webhook.** No trial. Invite-code login for founding members unchanged.
- **How:** Extend `auth_router.py` + `auth.py`. Resolve `google_sub NOT NULL` by writing synthetic `email:<sha256(email)>` (coordinated with M2). Email-verification optional for v1 (flag for Stage 2).
- **Where stored:** `backend/power_user/routers/auth_router.py`, `backend/power_user/services/auth.py`.
- **Acceptance:** New email + valid code → `comp` account + JWT, full access; new email no code → account + JWT but 402 on product routes until paid; duplicate email → clean 409; existing founding login unchanged (regression test).

### M6 — Legal (launch blocker)
- **What:** Terms of Service, Privacy Policy, Refund Policy, Risk Disclosure (SEBI-aware "not investment advice" framing). Static content pages + footer links.
- **How:** Frontend route group `/legal/*`. Content drafted by me as templates **clearly marked "lawyer review required before public launch."** Not legal advice — placeholders you get reviewed.
- **Where stored:** `frontend/app/legal/{terms,privacy,refund,risk}/page.tsx`; source markdown in `docs/launch/legal/`.
- **Acceptance:** 4 pages render; linked from footer + signup; risk disclosure shown at signup with checkbox consent.

### M7 — Frontend (commercial surface)
- **What:** `/power/pricing` (single ₹999/mo plan + "have a code?" path), `/power/signup` (email + optional code), `/power/billing` (subscription status, manage/cancel), paywall state on product pages (402 → "Subscribe ₹999/mo" CTA).
- **How:** New pages under `frontend/app/power/*`. Razorpay Checkout via hosted/embedded flow (no card data touches our frontend — PCI stays out of scope). Extend `frontend/lib/power-api.ts` with billing calls.
- **Where stored:** `frontend/app/power/{pricing,signup,billing}/page.tsx`, `frontend/lib/power-api.ts`.
- **Acceptance:** Two paths work: (1) code → free account → product; (2) no code → pricing → Razorpay → paid → product. Founding/comp users see no paywall. Cancelled user is gated again.

### M8 — Email (minimal, transactional)
- **What:** 3 transactional emails: welcome (on signup), payment-success receipt, payment-failed/subscription-cancelled. No trial emails. Daily digest is Stage 2.
- **How:** New `email_service.py` using Resend or SES (provider chosen in M1). Triggered from signup + Razorpay webhook events.
- **Where stored:** `backend/power_user/services/email_service.py`; templates in `backend/power_user/email_templates/`.
- **New env vars:** `EMAIL_PROVIDER`, `RESEND_API_KEY` (or SES creds), `EMAIL_FROM`.
- **Acceptance:** Signup sends welcome; successful charge sends receipt; failed/cancelled sends recovery notice exactly once; no marketing email to founding members.

---

## 4. How the subagent workflow runs (the loop you asked for)

```
YOU ──approve spec──► ME (orchestrator)
                        │
                        ├─ for each module M1..M8, in dependency order:
                        │     1. I dispatch BuildAgent-Mx  (spec section + exact files + CONTRACT.md)
                        │     2. BuildAgent writes code + tests + a build-log
                        │     3. I dispatch AuditAgent-Mx  (spec section + the diff + tests)
                        │     4. AuditAgent returns GREEN / RED with findings
                        │        • RED  → I send fixes back to BuildAgent, re-audit
                        │        • GREEN→ module done, move to next
                        │
                        └─ I report to YOU in plain English after each module:
                              what was built · audit verdict · what's next
```

- **Interconnection:** all agents share `docs/launch/CONTRACT.md` — the single source of truth for table names, column names, API shapes, and env var names. No agent invents a name; they read it from the contract. This stops drift between, e.g., what Billing writes and what Paywall reads.
- **Audit agent checks, every module:** (a) matches spec? (b) honors INV1–INV7? (c) security (webhook signatures, no secret leaks, no SQL injection)? (d) regressions to founding-user flow or Auto-Trade? (e) tests present + passing.
- **You never re-review requirements.** You get verdicts. You can interrupt any time, but the default is: I drive, audits gate, you watch.

---

## 5. Documentation produced (so production is traceable)

| Doc | Purpose |
|---|---|
| `docs/launch/MASTER_SPEC.md` | This file — the contract you approved |
| `docs/launch/CONTRACT.md` | Shared names/shapes all agents read |
| `docs/launch/ENV.md` | Every env var, what it's for, where set |
| `docs/launch/builds/Mx-build-log.md` | Per-module: what the build agent did |
| `docs/launch/audits/Mx-audit.md` | Per-module: audit verdict + findings |
| `docs/launch/STATUS.md` | Live board: module → BUILDING / GREEN / RED |

---

## 6. What I do the moment you approve

1. Write `CONTRACT.md` (locked names/shapes) + `ENV.md` + `STATUS.md`.
2. Dispatch **BuildAgent-M1 (Infra: Supabase + porting)** and **BuildAgent-M2 (DB schema)** first — M2's Postgres migration lands on the Supabase instance M1 stands up.
3. Audit each module as it lands; report to you in plain English.
4. Proceed module-by-module per the dependency graph (§3), gating on GREEN audits.

---

**Commercial + infra decisions are LOCKED (§0, §0.1, §1.5).** Nothing left to decide.

**To proceed:** reply **"approved"** and I'll write `CONTRACT.md` and dispatch M1 + M2.
