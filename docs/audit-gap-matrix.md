# Mockup → Code Gap Matrix (what exists vs. what to build)

Complements `docs/audit-report.md`. Verdict of the three-codebase audit (KANIDA.AI-TERMINAL product +
Kanida_Falcon R&D). Legend: ✅ built · 🟡 partial · ❌ missing.

## Reuse — the engines are real (port/wrap, don't rebuild)
- ✅ **Book / track-record engine** (expectancy, costs, DD, ledger) — `agents/chart/strategy.py`,
  `Kanida_Falcon/scripts/confirm_and_trade.py`. *(unify the two; add 2× slippage + append-only ledger)*
- ✅ **Pattern mining** — `Kanida_Falcon`: mine_phase1, state_engine, pat, SELVI.
- ✅ **Chart Agent** (point-in-time detectors + screener) = Trader Strategy #1.
- 🟡 **Market data** over Kite (daily ✅, 1-min 🟡, PIT-universe 🟡 survivorship-flagged) — needs one
  `MarketData(as_of)` seam.
- 🟡 **Base-rate + regime** — exist in research, not wired into the product path.
- ✅ **Arena / challenger** (Kanida_Falcon) = Pathfinder's research core.
- ✅ **Razorpay billing · auth (Google/JWT) · scheduler · admin gate · ap-south-1 infra** — exist.
- ✅ **Agent Builder** (no-code strategy backtester) — product-grade bonus.

## Build — the product/compliance/app layers
- 🟡 **Trader manager** — engines exist; build the unified rulebook + ensembler + regime gate (~2–4 pieces).
- 🟡→❌ **Investor manager** — research momentum-portfolio only; productize (10–15 names, monthly rebalance).
- 🟡→❌ **Pathfinder** — engine exists; build the product lifecycle (queued→running→passed/died→promoted +
  post-mortem + graveyard) + forward-tracking.
- ❌ **Compliance layer** — RA queue, classifier (Publishable/Kanida-only), PaRRVA, corrections log,
  auto-disclosure. **~0% built. The gate to selling — biggest true build.**
- 🟡 **App** — today Next.js **web**; the new product is **Expo mobile** = fresh build against the API.
- ❌ **Kite Publisher basket hand-off** · 🟡 per-user Login-with-Zerodha.
- ❌ **Creator console** (content-gen, referral/TDS, monetization dashboard) — entirely unbuilt.
- 🟡 **Payments paywall un-wired** · ❌ OTP + e-KYC · 🟡 analytics events · RBAC lacks RA/creator roles.

## Two flags
1. **⚠️ A live 6-broker OMS that has placed real orders exists** (`backend/autotrade/`) — the brief says
   *never place a customer order*. Decision: **wall it off the customer path; Phase-2 tier only.**
2. **One repo, ~35 worktree branches** (not competing products). Consolidate: merge the Chart Agent branch
   to main; rationalize branches.
