# Kanida.AI — Product & Vision Audit

> Principal product + engineering review. Brutally honest, evidence-grounded.
> Date: 2026-06-19 IST. Auditor: Claude (Opus 4.8, 1M).
> Every claim cites a file path or a memory artifact. Where I could not verify, I say "unverified".

---

## 1. EXECUTIVE VERDICT

**Are we close? Partly — to a *content product*, not to the stated vision.**

What actually exists and runs today is **one** thing done genuinely well: a single EOD swing-trading engine ("Falcon Top 10") that emits ~10 ranked Indian-equity picks per day, each wrapped in a real, data-grounded pattern narrative, served through a clean Next.js portal at `www.kanida.ai`. That engine has survived a real out-of-sample walk-forward gate (memory: `falcon_v7_validation.md`, `falcon_top10_persona_locked.md`) — which is more than most retail "AI trading" products can say. The frontend is well above average for a solo build. The auto-trade execution rail to Zerodha is real and has placed real orders (`falcon_phase2_live.md`, `backend/falcon/trade/services/`).

**Is the vision real or partly delusion? Mixed — one pillar is delusional as currently framed, the rest are real but earlier-stage than the founder believes.**

- The **"smartest AI trading system on earth" / self-learning** claim is the delusional part *as worded*. The "learner" that exists (`scripts/tier_weekly_learner.py`) does **not** learn. It re-grades the *same fixed rules* on fresh data and writes a shadow "challenger" row — `tier_self_learning_loop.md` says so verbatim: *"the learner still only RE-GRADES the same rules ... it does NOT yet SEARCH new thresholds."* There is no search over new thresholds, no champion/challenger that can actually win, no model that improves. That is a metrics-refresh cron dressed as machine learning. Ship the honest version of the claim.
- The **multi-persona** vision is *defined* (6 personas in `backend/power_user/services/portfolio_defs.py`) but they are all **the same EOD swing engine with different hold/cadence knobs**. There is no DAY trader, no F&O trader, no INDEX trader. Memory is explicit that futures data is only ~2 months deep so F&O personas are *not even backtestable* (`self_improving_engine_progress.md`). The "9 personas" framing is, today, marketing over a single strategy family.
- **Automated trading**, **tiering**, and a **clean product UI** are real and live — see scorecard.
- **GTM/billing** is built (`1f4b79f`) but **the paywall is currently un-wired in production** (commit `3cb4412` "un-wire paywall from live endpoints"). Verified: `backend/power_user/routers/falcon_top20_router.py` (the live Falcon Top 10 surface) has **no** `current_paid_user_required` dependency. **The product collects ₹0 today.** It is a free beta, not a business.

**Pivot or stay course?** **Stay course on the engine; pivot hard on the narrative and the surface area.** Do NOT build 9 personas or "self-learning" next. The single most valuable asset is the *validated EOD signal + the explainability layer*. The fastest path to a real business is: (1) turn the paywall back on for a sharply-scoped single product, (2) get off the laptop, (3) replace "smartest AI on earth / self-learning / one bot per stock" with claims you can actually defend. The vision is reachable in *spirit* (an intelligent, explainable, semi-automated co-trader) but the literal version (many live persona-bots, a system that teaches itself, "smartest on earth") is 12–24 months and a team away — not a solo-on-a-laptop quarter.

**One-line verdict:** *You have a real, validated single-strategy signal product with an excellent UI and a non-functioning business model, marketed as something an order of magnitude more advanced than it is. Close the gap by shrinking the claim and turning on the revenue — not by building more.*

---

## 2. CURRENT-STATE MAP

Tags: **LIVE** (runs in prod, serves users) · **BUILT-NOT-DEPLOYED** (code exists, not in the live path) · **PROTOTYPE** (research/one-off scripts) · **MISSING** (vision only).

### Workflow A — Daily Market Cycle (canonical blueprint A1–A7)
| Component | File | State |
|---|---|---|
| A2 OHLC ingestion | `data/ingest/fetch_fno_kite.py` (via `backend/falcon/jobs/daily_data_refresh.py`) | **LIVE** |
| A3 Feature engineering | `backend/falcon/jobs/daily_features.py` | **LIVE** |
| A4 Signal generation | `backend/falcon/services/signal_runner.py`, `backend/falcon/jobs/daily_signals.py` | **LIVE** |
| A5 EOD orchestrator | `backend/falcon/trade/services/eod_orchestrator.py` | **LIVE** (auto-trade operator only) |
| A6 Pre-market review UI | `/falcon/premarket` | **LIVE** (operator only) |
| A7 Order deployment (09:15) | `backend/falcon/trade/services/premarket_deployer.py` | **LIVE** (operator only) |
| Pipeline auto-kick on token refresh | `backend/falcon/jobs/_pipeline.py` | **LIVE** |

### Workflow B — Weekly Research (mine → validate → publish)
| Component | File | State |
|---|---|---|
| B1 Pattern mining | `universe_engine/engine/falcon_miner.py` | **PROTOTYPE** (R&D DB only, manual cadence) |
| B2 Outcome labeling | `universe_engine/engine/falcon_outcomes.py` | **PROTOTYPE** (R&D only) |
| B3 Validate + promote | `universe_engine/engine/falcon_validator.py` | **PROTOTYPE** (R&D only) |
| B4 Publish R&D→prod | `scripts/publish_patterns.py`, `backend/falcon/jobs/weekly_remine.py` | **LIVE** (manual/operator) |
| Self-improving layer (Steps 0–8) | `universe_engine/self_improving/` | **MISSING in prod tree** — lives only on `kanida-dev` worktree (`self_improving_engine_progress.md`). Empty here. |

### Workflow C — Real-Time monitor (09:15–15:30)
| Component | File | State |
|---|---|---|
| C1 Position monitor (60s) | `backend/falcon/trade/services/position_monitor.py` | **LIVE** (operator account) |
| C2 Trail SL | `backend/falcon/trade/services/trail_manager.py` | **LIVE** |
| C3 Stop-out (Kite-side) | Kite + `kite_reconcile.py` | **LIVE** |
| C5 EOD reconcile | `backend/falcon/trade/services/kite_reconcile.py` | **LIVE** |
| KiteTicker WebSocket | `backend/falcon/trade/services/kite_ticker.py` | **LIVE** (`falcon_phase3_live.md`) |

### Workflow D — On-user-action
| Component | File | State |
|---|---|---|
| D1 Trade panel | `backend/falcon/trade/routers/trade_router.py`, `/falcon/trade` | **LIVE** (operator only) |
| D3 Engine playbook config | `/falcon/config` | **LIVE** (operator) |
| Power User trade panel | — | **MISSING** (Stage 2; users cannot trade from `/power/*`) |

### Product surface (Power User portal — what paying customers would see)
| Component | File | State |
|---|---|---|
| `/power/today` Falcon Top 10 | `frontend/app/power/today/page.tsx` + `falcon_top20_router.py` | **LIVE** |
| 3-bucket explainability | `backend/power_user/services/falcon_top20_explainer.py`, `pattern_narrator.py` | **LIVE** |
| Signal Tier badge (GOLD/ENTERPRISE/PREMIUM/STANDARD/AVOID) | `backend/power_user/services/signal_tier.py`, `components/power/Top20Card.tsx` | **LIVE** (deployed `aaecb72`/`4f5d2e6`) |
| Replay / proof (anti-cherry-pick) | `frontend/app/power/replay/`, `replay_cache.py` | **LIVE** |
| 6 Co-Trader personas | `portfolio_defs.py`, `persona_simulator.py`, `/power/portfolios` | **LIVE but variants of one engine** |
| Position sizing assistant | `/power/sizing`, `portfolio_sizing.py` | **LIVE** |
| Auth (invite → JWT, Google) | `auth_router.py`, `middleware.ts` | **LIVE** |
| Billing (Razorpay, ₹999/mo) | `billing_router.py`, `/power/billing`, M3 | **BUILT-NOT-DEPLOYED** (paywall un-wired `3cb4412`) |
| Paywall gate | `dependencies.py::current_paid_user_required` | **BUILT-NOT-ENFORCED** on live product endpoints |
| Open signup / pricing pages | `/power/signup`, `/power/pricing` | **BUILT** (invite-only beta in practice) |
| Legal (Terms/Privacy/Refund/Risk) | `docs/launch/legal/`, `/legal` | **DRAFT** — needs SEBI-savvy lawyer (`launch_stage1_build.md`) |

### Self-learning / tiering brain
| Component | File | State |
|---|---|---|
| Tier rulebook as data | `falcon_tier_rules` table, `scripts/tier_rulebook_migrate.py` | **LIVE** (Phase 0) |
| Weekly shadow learner | `scripts/tier_weekly_learner.py` (Windows task `KanidaTierWeeklyLearner`) | **LIVE but re-grades only — does NOT search new rules** |
| Promotion review/approve UI | `/power/admin` TierReviewPanel, `admin_router.py` | **LIVE** (Phase 2) but **inert** until the learner can actually diverge |
| Multi-persona self-improving engine (Steps 2–7) | `kanida-dev` worktree | **PROTOTYPE** (R&D DB, not in prod) |

---

## 3. VISION SCORECARD

| # | Vision element | % complete | Hard evidence | Gap to "done" |
|---|---|---:|---|---|
| 1 | **Multi-persona platform** (day/F&O/index/swing) | **25%** | 6 personas in `portfolio_defs.py`, but all = one EOD swing engine with hold/cadence knobs. Futures data ~2 mo → F&O un-backtestable (`self_improving_engine_progress.md`). No DAY/INDEX engine exists. | A *genuinely different* engine per persona (intraday/F&O/index), each with its own data + validation. Real net-new work, not config. |
| 2 | **Smartest AI / self-learning** | **15%** | Engine survived OOS (`falcon_v7_validation.md`) = legit. But "learner" only re-grades fixed rules (`tier_self_learning_loop.md`); no threshold search, no winning challenger. "One bot per stock" is false (`self_improving_engine_progress.md` Step 0). | A real search/optimization loop with no-lookahead validation + auto-promotion gates. Today it's a metrics-refresh cron. |
| 3 | **Fully automated Zerodha trading** | **70%** | Live order placement, SL/trail, monitor, reconcile, premarket staging (`falcon_phase2_live.md`, `falcon_phase3_live.md`, `backend/falcon/trade/`). 13-check preflight (`falcon_gtm_reliability.md`). | Runs on the **operator's own account only**; no per-user broker connect (Stage 2). Reliability gate (30 consecutive GREEN days) not yet met. |
| 4 | **Signal tiering (GOLD/ENTERPRISE/PREMIUM/AVOID)** | **80%** | Live on `/power/today` (`signal_tier.py`, `Top20Card.tsx`, prod `aaecb72`). Derived + OOS-tested (`tier_price_volume_derivation.md`, `tier_pullback_breakthrough.md`). | PREMIUM ~83–85% is **not OOS-certifiable** (thin 2026 N). Needs leave-one-year-out CV before it can be marketed as a number. |
| 5 | **AI-product UX like Claude.ai** | **40%** | Strong landing + Top20Card explainability (`frontend/app/power/`). But it's a multi-tab portal (today/portfolios/replay/sizing/credibility/billing), not one-job-per-screen. No conversational entry. | Collapse to one primary surface + one primary action. See §6/§7. |
| 6 | **Commercial GTM (₹999/mo)** | **55%** | Razorpay service, paywall predicate, pricing/billing pages, email all built + audited GREEN (`launch_stage1_build.md`, `docs/launch/STATUS.md`). | **Paywall un-wired in prod (`3cb4412`)** → ₹0 revenue. Legal is DRAFT. Still on laptop. Turn it on. |

**Weighted reality:** the product is roughly **a strong v0.6 of a single-strategy signal subscription**, mislabeled as a v1 of a multi-persona self-learning platform.

---

## 4. PERSONA-BASED TRADING

**What exists** (`backend/power_user/services/portfolio_defs.py`, lines ~123–479):

| Persona | Cadence | Hold | Engine |
|---|---|---|---|
| P1 The Daily Trader | every day 9:15 | 7d | EOD swing, top-14 |
| P2 The Patient Trader | every day 9:30 | 7d | same engine, +15min confirm |
| P3 The Weekly Trader | Tue only | 5d | same engine, top-10 |
| P4 The Monthly Trader | 1st & 16th TD | 15d | same engine, top-14 |
| P5 The BTST Trader | every day 9:15 | 2d | same engine, top-15 (−54% worst yr ⚠️) |
| P6 Falcon Top 10 | every day 9:15 | 7d | same engine, avg_lift ranker (the locked flagship) |

**The honest reading:** these are six **schedules and hold-times over one signal source**, not six trading styles. They differ in *when you enter and how long you hold*, not in *what the engine looks at*. A real "Day Trader" persona needs intraday signals; a real "F&O Trader" needs an options/futures engine; an "Index Trader" needs index-level models. None of those engines exist, and the data to build F&O isn't there yet (~2 months of futures, `self_improving_engine_progress.md`).

**Desired vs reality gap:**
- DAY trader → **MISSING** (intraday mining is a research breakthrough in `engine_intraday_smart_entry_breakthrough.md` but not a productized engine; needs deployer moved to 10:45 IST and a live intraday feed).
- F&O trader → **MISSING + un-buildable today** (data gap).
- INDEX trader → **MISSING**.
- Swing/positional → **EXISTS** (this is what all 6 personas actually are).

**Architecture work to make personas first-class + extensible:**
1. Define a `Persona` as `(engine_id, universe, ranker, entry_rule, exit_rule, sizing)` where **`engine_id` is the discriminator** — today every persona hardcodes `engine_id = falcon_top10_eod`. Until two real engines exist, "persona" is a presentation concept, and you should *say so*.
2. Make the engine an interface: `signal_source.generate(date, universe) -> ranked_picks`. The swing engine implements it; a future intraday engine implements it; the persona layer composes on top. This is the clean seam `persona_engine_core.py` half-implements (it reuses `simulate_year` for parity, which is good — keep parity sacred per memory).
3. **Do not stub 9 personas now.** Step 8 of the self-improving plan ("stub all persona configs") is the *lowest*-leverage remaining work. Build the *second real engine* (intraday) before adding any persona chrome — one more genuine strategy beats eight more config rows.

---

## 5. "SMARTEST AI / SELF-LEARNING" — HONEST ASSESSMENT

**How intelligent is it really?** It is a **well-validated static pattern engine** with a **metrics-refresh loop bolted on**, not an adaptive system.

What's genuinely good (don't undersell this):
- Patterns are *mined* (shallow decision trees, `falcon_miner.py`), *outcome-labeled*, and *promoted only if they survive OOS validation* (`falcon_validator.py`, 846/2,283 promoted per `falcon_v7_validation.md`). That is real ML discipline — most "AI trading" apps have none.
- The explainability is real: `falcon_top20_explainer.py` evaluates patterns for a stock and `pattern_narrator.py` grounds the narrative in the stock's *actual* feature values (the narrative-accuracy fix in `power_user_portal_ops.md` is exactly the kind of honesty that builds trust).

What is **not** intelligent / not self-learning (the marketing gap):
- The weekly "learner" (`scripts/tier_weekly_learner.py`) does `DELETE ... WHERE status='challenger'` then re-inserts the **same `conditions_json`** with fresh metrics. `tier_self_learning_loop.md` states plainly: *"the learner still only RE-GRADES the same rules (challenger conditions == active); it does NOT yet SEARCH new thresholds — so until that learner upgrade, 'approve' just refreshes metrics."* There is no learning. The Phase-2 approve button is inert by construction ("No divergence yet").
- The Phase-6 self-improving re-ranking experiment was **honestly negative**: re-ranking by `improved_score` added only **+19pp cumulative over 5.5yr and was regime-dependent** (`self_improving_engine_progress.md` Step 6). The data itself says the engine's avg_lift selection is *already near-optimal* — the real levers are exit timing and entry promptness, **not** smarter ranking. So even the *attempt* at self-improvement found that "smarter selection" isn't where the edge is.
- "**500+ AI agents. One per stock.**" appears verbatim in the live landing hero (`frontend/app/power/page.tsx:157`). The Step-0 audit (`self_improving_engine_progress.md`) explicitly corrected: *"NO 'one bot per stock' — mining is per (year × target × scope)."* **This headline is false and is the single most legally/credibility-risky line on the site.** Change it.

**Is "smartest on earth" attainable? No — and chasing it is a trap.** You are one person on a laptop competing rhetorically with firms that have co-located infra and PhD teams. The claim is unfalsifiable, un-defensible, and invites exactly the scrutiny that kills small fintech credibility. **Reframe to what is true and rare:** *"Every pick comes with its evidence — the historical pattern, its out-of-sample hit rate, and what happened the last time this setup fired on this stock."* That is a real, differentiating, defensible claim. Lead with **explainability and honesty**, not "smartest."

---

## 6. FRONTEND / PRODUCT-UX CRITIQUE

**Does it look like an AI product or a dashboard?** It's *trending toward* an AI product (the landing is genuinely good — `frontend/app/power/page.tsx`) but the logged-in experience is **a multi-tab dashboard**. Surfaces present: `today`, `portfolios`, `replay`, `sizing`, `credibility`, `live`, `billing`, `pricing`, `signup`, `redeem`, `waitlist`, `admin` (`frontend/app/power/`). That is **12 routes** for what is fundamentally *one daily action*: "show me today's picks with their evidence."

Specific problems (cite + fix):
1. **No single primary action.** The landing's primary CTA points at `/power/portfolios` (`page.tsx:305`), but the actual product value is `/power/today`. The hero says "Co-Trading," the gate card says "today's top 14 picks," `/power/today` says "Falcon Top 10," and `/power/portfolios` shows 6 personas. **Four different mental models on four screens.** Pick one: *Today's picks* is the product. Everything else is supporting evidence.
2. **Naming drift confuses.** "Falcon Top 10" (today page H1, `today/page.tsx:151`) vs "top 14 picks" (landing gate, `page.tsx:480`) vs "all 100 picks" (conversion CTA, `page.tsx:541`) vs 6 named personas. A new user cannot tell what they're buying. **Standardize on one product name and one pick count.**
3. **6 personas is choice-overload for retail.** `/power/portfolios` lists Daily/Patient/Weekly/Monthly/BTST/Falcon-Top-10 — five of which are near-identical and one (BTST) has a −54% worst year (`portfolio_defs.py:420`). Showing a −54% persona to a retail user, next to five look-alikes, erodes trust and decision-confidence. **Default to ONE (Falcon Top 10); make the rest an "advanced" reveal.**
4. **"500+ AI agents. One per stock." (`page.tsx:157`)** — false (see §5). Fix immediately.
5. **Tiering is shown without a defensible number.** Good that you avoided marketing "80%" (memory is consistent on this). Keep the badge qualitative (GOLD/PREMIUM) until LOYO CV certifies a number (`tier_pullback_breakthrough.md` v3).

Specific simplifications:
- **Collapse to 3 surfaces:** (a) **Today** (the one job), (b) **Proof/Replay** (why trust it), (c) **Account/Billing**. Move sizing into the Today pick detail; move personas behind an "Advanced" toggle; retire `/live`, `/redeem`, `/waitlist` into the auth flow.
- **One primary action per screen.** Today = "See today's picks." Pick detail = "Understand this call." Account = "Manage subscription." Nothing else competes for the eye.
- The `Top20Card` progressive-disclosure (top 3 expanded, rest collapsed — `today/page.tsx:116`) is **the right pattern** — extend that discipline everywhere.

---

## 7. CLAUDE.AI BENCHMARK

### (a) Architecture mapping

| Claude.ai layer | Kanida analogue | State | Structural weakness vs Claude.ai |
|---|---|---|---|
| **Foundation model** (the asset) | The validated pattern set + signal engine (`falcon_miner.py` → `falcon_promoted_patterns`) | Real, OOS-gated | Kanida's "model" is a **static rule set re-published weekly**, not a continuously trained artifact. It improves only when the operator re-mines. Fine for now — but it's the asset, so protect/version it. |
| **Training / scaling** (mining → walk-forward) | `falcon_outcomes.py` + `falcon_validator.py` + walk-forward sims | Real, but **R&D-only, manual, on a 14G laptop DB** | No reproducible training pipeline off the laptop. "Training data" (futures, intraday) is thin/stale (`self_improving_engine_progress.md` data limits). Single-machine bottleneck. |
| **Alignment / safety** (constitution) | Tiering + AVOID + falling-knife guard + 13-check preflight + risk controls (`signal_tier.py`, `falcon_gtm_reliability.md`, `engine_kaynes_regime_failure.md`) | Real and thoughtful | The "safety layer" is the strongest analogue — AVOID isolates the worst ~21% (WR 46% vs 70%, `self_improving_engine_progress.md` Step 7). Weakness: regime tags are 0%-populated (no regime data), so the falling-knife guard is partial. |
| **Inference / serving** (daily signals) | `signal_runner.py` → `falcon_signals_live` → `/power/today` | LIVE | Serving runs on **one laptop, Pacific-clocked, cloudflared-tunneled, in-memory 24h cache wiped on restart** (`power_user_portal_ops.md`). No redundancy. This is the single biggest structural gap vs Claude.ai's serving backend. |
| **Clients** (thin frontend) | Next.js portal on Vercel (`frontend/app/power/`) | LIVE, good | The client is the *most* Claude.ai-like part — clean, on a real CDN. Ironically the front door is solid and the engine room is a laptop. |

**Where Kanida is structurally weakest vs Claude.ai:** the **serving + training substrate**. Claude.ai separates a hardened inference backend from training; Kanida runs *both* training (R&D) and serving on the same personal machine, with the live business depending on it not sleeping (`laptop_sleep_watchdog.md`). The "model" can't be retrained reproducibly off-box, and inference has no failover. **This is the #1 thing to fix that no UI polish can substitute for.**

### (b) Product/UX mapping

Claude.ai's genius is **one job per surface, one obvious primary action, progressive disclosure of power**: Chat = "say something"; Cowork/Code = "give it a task." There is always exactly one thing to do.

| Claude.ai principle | Kanida's equivalent / how to apply |
|---|---|
| **One primary action** | Kanida's primary action should be: **"See today's picks."** Today it's diffused across portfolios/today/landing (§6). Make `/power/today` *the* app; everything else is secondary. |
| **One job per screen** | Today = the call. Pick detail = the why. Replay = the proof. Account = billing. Kill the 12-route sprawl. |
| **Progressive disclosure** | Already half-done: `Top20Card` expands top-3, collapses the rest. Extend: show *tier + 1-line reason* by default; reveal full 3-bucket evidence on demand. |
| **Conversational entry** (directly leverageable!) | Kanida's killer adaptation: a **"Ask about this pick"** box on each card — "Why is RELIANCE here?", "What's the risk?", "What happened last 3 times?" The pattern data + narrator already exist (`pattern_narrator.py`); wiring an LLM Q&A over the per-pick evidence is the most on-brand "AI product" move available and turns a dashboard into a co-trader. **This is the single highest-leverage UX bet** and directly mirrors Claude.ai's chat-over-context. |
| **Explainability as the product** | Claude shows reasoning; Kanida should make *evidence* the hero, not metrics. The "3 things every pick answers" pillars (`page.tsx:230`) are exactly right — make the logged-in product deliver on them as cleanly as the landing promises. |

---

## 8. GAPS, RISKS, TECH DEBT

**Single points of failure (all verified in memory):**
- **The whole business runs on one personal laptop.** Backend = `uvicorn` from the working tree, restarted daily ~14:30 IST by Task Scheduler (`power_user_portal_ops.md`). Laptop sleep → backend 503 + auth task pauses + Kite token expires (`laptop_sleep_watchdog.md` — the two fixes are **NOT YET APPLIED**).
- **Machine runs Pacific time, not IST** (`portal_signal_tier_feature.md`) — every scheduled task does IST math against a Pacific clock. One DST/anchor slip and signals/deploys mis-fire. Brittle.
- **SQLite, single-writer**, served + mined on the same file/box (`dependencies.py:48`). No PITR backups, no failover. (The 680 GiB Codex git-bloat incident, `codex_git_bloat_incident.md`, shows how close the disk has come to filling.)
- **Cloudflared named tunnel** is the only ingress to `localhost:8001` (`power_user_portal_ops.md`). Tunnel down or laptop offline = site down.
- **Kite token expiry** breaks the nightly OHLC fetch silently if auth task is stale (`zerodha_auth_standalone_task.md`, the 2026-06-18 EOD-gap incident).
- **In-memory 24h cache wiped on every restart** → cold-cache 100–200s persona-sim → portal timeout if the warmer doesn't run (the 2026-06-14 paywall-broke-the-warmer incident, `power_user_portal_ops.md`).

**Product / credibility / regulatory risks:**
- **"500+ AI agents. One per stock."** on the live landing is **factually false** (§5). Highest-priority fix.
- **"Smartest AI trading system on earth"** is un-defensible marketing (§5).
- **SEBI exposure:** legal pages are **DRAFT**, need a SEBI-savvy lawyer (`launch_stage1_build.md`). In India, signal distribution + "Live" persona labels flirt with research-analyst / investment-adviser registration. The highest-risk claim per memory: *"'Live' persona labels are model/simulated, not real client capital."* Must be unambiguous before charging money.
- **Tiering numbers (80–85% WR) are NOT OOS-certified** (`tier_pullback_breakthrough.md`). Memory repeatedly self-warns "Don't market 80%." Keep that discipline; one screenshot of an uncertified WR in an ad is a credibility (and possibly regulatory) landmine.
- **Auto-trade reliability gate not met:** 30 consecutive GREEN days required (`falcon_gtm_reliability.md`); there were 4 days of auto-trade failures that prompted the reliability layer. Per-user auto-trade (Stage 2) on this foundation would be reckless.

**Tech debt:**
- Postgres artifacts built but shelved (`migrate_to_supabase.py`) — fine, but the codebase uses raw `sqlite3` everywhere, so the cloud move is a *refactor*, not a copy (`launch_stage1_build.md`).
- `kite_tokens` read via raw sqlite3 bypassing `DATABASE_URL` (R1/C2 punch-list) — will break the token on any Postgres host.
- Legacy code on disk (`app/_legacy/`, `backend/_archive/`, a separate `KANIDA.AI_TERMINAL` project that runs a *decoy* scheduled task) — cleanup debt + operational confusion (`power_user_portal_ops.md` "Legacy decoy").

---

## 9. STRATEGIC SHIFTS / PIVOTS

**DROP (stop spending time on):**
1. **"Self-learning engine" build (Steps 7–8, persona stubs).** The honest result is that re-ranking barely helps (Step 6, +19pp regime-dependent) and the learner can't yet learn. Park it. It's a research curiosity, not a revenue driver.
2. **9 personas.** Five of the six are the same engine. Building more config-personas adds confusion, not value.
3. **"Smartest AI on earth" / "500+ agents one-per-stock" messaging.** Delete. Replace with explainability-led, defensible copy.
4. **Per-user auto-trade (Stage 2) for now.** Don't put customer money through a one-laptop rail that hasn't passed its own 30-day reliability gate.

**SIMPLIFY:**
1. **Collapse 12 routes → 3** (Today / Proof / Account). One job per screen.
2. **One product, one name, one pick count.** "Kanida Daily 10" (or similar). Kill the Falcon-Top-10 / top-14 / top-100 naming drift.
3. **Default to one persona;** hide the rest behind "Advanced."

**DOUBLE DOWN:**
1. **The validated EOD signal + explainability.** This is the only true asset. Make it the entire product.
2. **Honesty as the brand.** The replay/anti-cherry-pick proof, the AVOID tier, the "we don't market 80%" discipline — *that* is your moat against the tip-seller competition. Lean all the way in.
3. **Conversational "Ask about this pick"** — the one new feature that turns a dashboard into the "AI co-trader" the vision actually wants (§7b). The data and narrator already exist.
4. **Get off the laptop.** Phase 1 (SQLite-on-a-volume) is already designed and tooled (`docs/launch/STATUS.md`, P1BUNDLE/P1PUB GREEN). Execute it.

---

## 10. RECIPE FOR SUCCESS (prioritized, phased)

Effort: **S** = days, **M** = 1–2 weeks, **L** = a month+.

### Phase 0 — Credibility hygiene (do this week) — effort **S**
- **Fix/replace "500+ AI agents. One per stock."** and "smartest on earth" → defensible explainability copy. (`frontend/app/power/page.tsx`) **[S, do today]**
- **Get the legal pages lawyer-reviewed** (SEBI angle) before any charge. **[S to commission, blocks revenue]**
- **Apply the laptop-sleep fixes** (`powercfg`, WakeToRun) — stop the most common outage. (`laptop_sleep_watchdog.md`) **[S]**

### Phase 1 — Turn on the business — effort **M**
- **Re-wire the paywall** for ONE clearly-scoped product (re-add `current_paid_user_required` to `falcon_top20_router.py`; reverse the spirit of `3cb4412`), with the authenticated cache-warmer fix already in place (`6a7512a`). **[M — this is the difference between a hobby and a company.]**
- **Execute Phase-1 cloud lift-and-shift** (SQLite-on-a-volume, already tooled). Laptop loss ≠ product down. **[M]**

### Phase 2 — Sharpen the one product — effort **M**
- **Collapse the UI to 3 surfaces**, one job each; default to one persona. **[M]**
- **Certify tiering via leave-one-year-out CV** so you can finally show a defensible WR number (`tier_pullback_breakthrough.md` v4 plan). **[M]**

### Phase 3 — The differentiating bet — effort **M–L**
- **"Ask about this pick" conversational layer** over the existing per-pick evidence (`pattern_narrator.py` + an LLM). This is the feature that makes the "AI co-trader" claim *true* and is the most defensible upgrade. **[M]**

### Phase 4 — The only real "more engine" worth doing — effort **L**
- **Build the second genuine engine (intraday/Day Trader)** off the validated `engine_intraday_smart_entry_breakthrough.md` research, then — and only then — promote "personas" to a first-class, multi-engine concept. **[L]**

### THE SINGLE MOST IMPORTANT NEXT MOVE
**Turn the paywall back on for one sharply-named product, after a one-day credibility-copy fix — i.e., make the thing you already have into a business before building anything new.** You have a validated signal, a clean UI, and a built billing stack sitting un-wired. The gap between "impressive free beta" and "company" is a config change and a sentence of honest copy — not another engine.

---

*Appendix — verification basis:* every workflow claim cross-checked against `backend/falcon/`, `backend/power_user/`, `frontend/app/power/`, `universe_engine/engine/`, `backend/main.py`, and the memory corpus. Paywall-off confirmed by absence of `current_paid_user_required` in `falcon_top20_router.py` + commit `3cb4412`. Learner-doesn't-learn confirmed in `scripts/tier_weekly_learner.py` + `tier_self_learning_loop.md`. "One bot per stock" falseness confirmed in `self_improving_engine_progress.md` (Step 0) vs live `frontend/app/power/page.tsx:157`.
