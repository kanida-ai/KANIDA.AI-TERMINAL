# KANIDA.AI — Master Build Program
Turns the audit + architecture into **session-sized work packages**. Run ONE session per package, in order.

## How to use this (your multi-session model)
1. Open a **new Claude Code session in agent mode** at the spine repo (`KANIDA.AI-TERMINAL`), with `CLAUDE.md` (the brief), `docs/audit.md`, `docs/architecture.md`, and this file present.
2. Paste that package's block as the session's task. It is self-contained: goal · context files · steps · "done when" · hand-back.
3. Let it build + write the hand-back (`docs/handbacks/<id>.md`). **Verify "done when" yourself.** Then close/delete the session.
4. Move to the next package. Dependencies are listed — don't start a package before its "after" is done.

## Global rules every session must obey (from the brief — put in CLAUDE.md)
- No return promises/target prices. Every return shown with its drawdown. **Expectancy is the hero metric, never win-rate.** Losers shown first.
- Every number carries n + date range + data source + cost convention. n<50 flagged, n<20 greyed.
- **Point-in-time only** (every price read takes `as_of`; no look-ahead/survivorship). Backtests labelled "Simulated · Not traded · Not PaRRVA-verified".
- Signals/marks/approvals/corrections are **append-only**. Nothing publishes without an **RA-approval row**. Signals never reach the creator console.
- Agents **emit intents only**; **Kanida never places a customer order** (hand-off only). No secrets in code.
- Two codebases: **KANIDA.AI-TERMINAL** = product spine (git); **Kanida_Falcon** = R&D engines to port. Reuse before building.

---

# PHASE 0 — Consolidate the foundation

### S0.1 — Repo consolidation & branch hygiene
**Goal:** one clean spine on `main`. **After:** nothing.
**Context:** the ~35 worktrees of KANIDA.AI-TERMINAL; branch `feat/agent-platform` (Chart Agent).
**Steps:** 1) List all branches/worktrees; tag each active/stale/merged. 2) Merge `feat/agent-platform` → `main` (Chart Agent lands in main). 3) For each `_kanida_*` branch: merge if wanted, else document + close. 4) Write `docs/BRANCHES.md` (what's live, what's parked). 5) Confirm one build/deploy path from `main`.
**Done when:** `main` builds + deploys, carries the Chart Agent, and every open branch is accounted for in `docs/BRANCHES.md`.

### S0.2 — Single MarketData interface
**Goal:** one thin `MarketData` seam over Kite. **After:** S0.1.
**Context:** Kite adapters in TERMINAL (`data/adapters/kite_adapter.py`, `agents/chart/data.py`, `fetch_kite.py`); the `MarketData` iface idea in Kanida_Falcon.
**Steps:** 1) Define `MarketData` (methods: `bars(symbol, as_of, tf)`, `universe(as_of)`, adjusted, PIT). 2) Implement the Kite-backed adapter behind it. 3) Route every engine's price read through it. 4) Add a fixture/golden test proving `as_of` truncation (no look-ahead). 5) Document how a licensed vendor swaps in later.
**Done when:** all engines read prices only via `MarketData`; PIT test passes.

### S0.3 — Unify the book / track-record engine
**Goal:** ONE book engine. **After:** S0.2.
**Context:** `agents/chart/strategy.py` (TERMINAL) + `scripts/confirm_and_trade.py`, `kanida_engine/engine_trade.py` (Falcon).
**Steps:** 1) Pick the stronger core (confirm_and_trade has full ledger+MFE/MAE). 2) Merge into `backend/book/` service: next-open entry, rule exits, versioned costs+slippage, **expectancy + avg-win/avg-loss + win-rate + max & current drawdown**, closed-trade ledger. 3) Add **2× slippage sensitivity** run. 4) Make the signal ledger **append-only + hash-chained**. 5) Golden-test vs a known historical result; kill the duplicate engines.
**Done when:** one book engine reproduces a golden backtest incl. expectancy + 2× slippage; duplicates deleted.

---

# PHASE 1 — Trader manager (the record clock starts here)

### S1.1 — Trader rulebook + nightly signals
**Goal:** deterministic Trader manager. **After:** S0.3.
**Context:** `agents/chart` (pattern rulebook), Falcon `signal_runner.py`, SELVI `SPS_V7/rulebook.py`. **Founder input needed:** the Trader rulebook (stub if absent).
**Steps:** 1) Define the deterministic 1–3d momentum+MR rulebook (named rules → features). 2) Nightly job (18:00 IST) → append-only signals via S0.3 ledger. 3) Attach each signal to its book (expectancy/DD/n). 4) Reproducibility test (same date → same signals + hash). 5) Manager manifest (mandate, 7 disciplines, risk budget).
**Done when:** nightly Trader signals are reproducible, append-only, each with a live book.

### S1.2 — Data-quality gate + "no signal today"
**Goal:** bad data → no signals, publicly. **After:** S0.2.
**Context:** `screener.py` freshness guard; `daily_data_refresh.py`.
**Steps:** 1) 17:30 IST check: completeness, interior gaps, corporate actions, staleness. 2) On failure → block signal gen + set a public `no_signal_today` state with reason. 3) Founder alert. 4) Test with a deliberately broken feed.
**Done when:** a broken feed produces zero signals + the public "no signal today" state.

---

# PHASE 2 — Investor + Pathfinder + base rates

### S2.1 — Investor manager
**After:** S0.3. **Context:** `Kanida_Falcon/scripts/momentum_port.py` (rel-strength, top-N, monthly rebalance). **Founder input:** Investor rulebook.
**Steps:** 1) Productize: rank → hold 10–15 names → monthly rebalance, PIT, cost-modeled. 2) Its own book (expectancy/DD/n, thesis per name). 3) Manager manifest + risk budget. 4) Test rebalance + leak-free.
**Done when:** Investor produces a monthly-rebalanced virtual portfolio with a real track record.

### S2.2 — Pathfinder runner
**After:** S0.3. **Context:** `Kanida_Falcon/arena` + `scripts/agent_arena.py` (challenger pipeline). **Founder input:** first 5 hypotheses.
**Steps:** 1) Model each hypothesis as its own virtual book. 2) Status machine: queued→running→passed/died→promoted. 3) Mandatory post-mortem on death; graveyard store. 4) Weekly digest data. 5) Seed the 5 hypotheses; test lifecycle.
**Done when:** hypotheses run as books, transition status, and dead ones have post-mortems.
**STATUS: DELIVERED by session P1** (`docs/handbacks/P1.md`). All five steps are done except (5) —
the five hypotheses are still a founder input, so the engine seeded six of its own from a
pre-registered grid. Beyond the brief: the go/no-go gauntlet, the L1–L4 change-log, the Constitution,
the point-in-time seal, and the `pathfinder_llm` gateway. Six experiments ran, six died, none was
promoted; the graveyard is populated with real post-mortems.

### S2.3 — Base-rate + regime engine
**After:** S0.2. **Context:** `Kanida_Falcon/engine/state_engine/baseline.py`, `scripts/regime.py`.
**Steps:** 1) Base-rate engine: founder's 10–15 observation rules → daily count/base-rate/n. 2) Regime detector: breadth + India-VIX bucket + expiry proximity → RISK_ON/NEUTRAL/OFF. 3) Wire both into the product path (not research-only). 4) Test n + regime tags populate.
**Done when:** daily base-rate cards + a populated regime tag are available to the API.

---

# PHASE 3 — Compliance layer (the gate to selling — mostly net-new)

### S3.1 — RA review queue + audit log
**Context:** admin patterns in `power_user/routers/admin_router.py`. **Founder input:** RA name/number.
**Steps:** 1) `compliance/` module: every signal enters a queue (pending). 2) RA approve/reject with a note; **publish only approved** (20:00). 3) Append-only audit log. 4) RBAC `RA` role. 5) Test: unapproved signal never reaches the API.
**Done when:** no signal is publicly visible without an RA-approval row.

### S3.2 — Auto-disclosure line
**Steps:** 1) Per-idea disclosure generator (RA name/number, holdings/conflict, market-risk). 2) Attach to every published idea + content piece. **Done when:** every idea carries a correct auto-disclosure.

### S3.3 — Content compliance classifier
**After:** S3.1. **Founder input:** 50 labelled examples.
**Steps:** 1) Classifier PUBLISHABLE vs KANIDA-ONLY. 2) Hard blocks: ticker+directional verb; any price/target/stop; any Kanida performance figure; banned phrases; any manager position. 3) Founder-review queue at 100% for 8 weeks. 4) Test against the 50 examples.
**Done when:** the classifier blocks all hard-block cases and routes correctly.

### S3.4 — PaRRVA client + badge
**Steps:** 1) Submit approved recommendations (API when granted; file-upload fallback). 2) Show the verified badge only when verified. **Done when:** approved recs submit + badge reflects verification.

### S3.5 — Public corrections log
**Steps:** 1) Append-only corrections store + public feed. 2) "What I got wrong" surfaces from it. **Done when:** a correction is publicly visible and immutable.

---

# PHASE 4 — The Expo app (biggest net-new; API already exists)

### S4.1 — Expo scaffold + API client + nav
**Steps:** 1) One Expo app (Android/iOS/web/tablet). 2) API client to `/api/*`. 3) Auth shell + bottom nav (Home/Agents/AutoTrade/Insights/Profile). **Done when:** the app boots on all four targets against the real API.
### S4.2 — Home: 3-manager cards + market overview (expectancy+DD+n+live-since+today).
### S4.3 — Manager detail screens (Trader/Investor/Pathfinder sub-tabs) on real data.
### S4.4 — Idea card (rule · invalidation · base-rate w/ n · sizing to user capital · disclosure · "Send basket").
### S4.5 — Track record (losers-first ledger · distribution · "how we count" · PaRRVA badge) + methodology page + corrections feed.
*(Each S4.x: After S4.1; Done when the screen renders real backend data with the global rules honored — losers-first, n flags, no promises.)*

---

# PHASE 5 — Execution hand-off (never place a customer order)

### S5.1 — Kite Publisher basket hand-off
**Steps:** 1) Build the basket → hand to Kite Publisher; user confirms in their own Kite. 2) **Log the hand-off, never an order.** 3) Test: no order is ever placed by Kanida. **Done when:** "Send basket" hands off + logs, zero orders placed.
### S5.2 — Per-user Login-with-Zerodha (read holdings/funds for sizing + optional mirror; per-user session; never store tick data). **Done when:** a user connects and sizing uses their real capital.

---

# PHASE 6 — Creator console (net-new; signals never reach it)

### S6.1 — Discover feed + content generation (script/thread/carousel/infographic) from PUBLISHABLE facts only.
### S6.2 — Compliance-classifier integration + multi-platform publish + auto-disclaimer + content IDs.
### S6.3 — Referral system (handle links + promo codes, 25%→10% commission, 2% TDS, clawback, self-referral blocks) + Earn dashboard. Enforce at API: **creators get no access to ideas/positions/performance.**
*(Done when: a creator generates a compliant piece, publishes with a referral link, and earnings track — with zero access to signals.)*

---

# PHASE 7 — Platform (make it sellable)

### S7.1 — Auth (phone OTP) + e-KYC before first payment. **Founder input:** e-KYC vendor.
### S7.2 — Razorpay tiers (Free=1-day-delayed / Pro ₹299 / Intelligence ₹599) + **wire the paywall (currently off)** + GST invoices + entitlements. AutoTrade tier in schema, disabled.
### S7.3 — Analytics events (view-3-managers, send-basket, trial, pay, cancel, referral, creator-publish) + admin gate dashboard.
### S7.4 — Security: RBAC (customer/creator/RA/admin), Secrets Manager, backups + restore test, ap-south-1, `docs/security_policy.md`; **move prod off the laptop to ECS**; runbook automation (17:30/18:00/20:00/08:45/09:10/09:15/15:35 jobs + health checks + failure drill).

---

# PHASE 8 — Harden & the OMS decision

### S8.1 — Wall off the OMS. **Founder decision required.** Gate the live multi-broker OMS entirely off the customer path; it becomes the disabled Phase-2 AutoTrade tier. **Done when:** no customer path can reach `place_order`; only the Kite hand-off exists.
### S8.2 — Load test + gate dashboard + payout export + final hardening. **Done when:** launch gates green.

---

## Founder inputs to gather (stub these; each session flags what's missing)
Trader rulebook · Investor rulebook · 5 Pathfinder hypotheses · 10–15 observation rules · cost values · Kite credentials · RA name/number + holdings + disclaimers · PaRRVA access · e-KYC vendor · Razorpay + GSTIN · legal copy · 50 classifier examples.

## Sequencing at a glance
Phase 0 → 1 → 2 → 3 (compliance can run parallel to 4 once 0–2 done) → 4 → 5 → 6 → 7 → 8.
Compliance (Phase 3) gates any public launch. The app (Phase 4) needs Phases 0–2 producing real data.
