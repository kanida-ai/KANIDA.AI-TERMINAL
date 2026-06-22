# Spec — Kanida.AI Co‑Trading (virtual capital, guided, bridge to AutoTrade)

Status: PROPOSAL for build. Owner agent: `falcon-cotrading`. Theme: mint/F2 (same
as Ask‑Falcon). Route: `/power/co-trading`. Entry: the "Start Co‑Trading" CTA in
the Ask‑Falcon right column. HARD RULE: no mock/static — real signals + real
prices where available; anything not yet served = honest empty/"coming soon" +
listed under Backend needs. Never fabricate P&L or prices.

## Purpose
Let a user FOLLOW Falcon with **virtual capital** — see exactly how Falcon would
allocate, enter, manage (SL/trailing), and exit — before connecting a broker for
AutoTrade. Simple, guided, explainable.

## User flow
1. **Setup** (one guided screen): choose trading style (only Falcon Top 10 Swing
   LIVE; others Launch‑Pending), virtual capital (₹ input/presets), start date
   (default today; or a past date for replay), risk profile (Conservative /
   Balanced / Aggressive → affects position sizing + max positions).
2. **Allocation** — Falcon allocates the capital across the selected signals per
   the engine rules (even spread or score‑weighted per risk profile). Per stock:
   entry price, current LTP, quantity, capital allocated, stop‑loss, expected
   holding period, Falcon tier, and **why selected + which rule triggered + exit/SL
   logic**.
3. **Virtual portfolio** — summary: starting capital, current value, total P&L,
   return %, open positions, closed positions, cash available, max drawdown.
4. **Falcon Actions feed** — plain‑language log: entries, exits, trailing‑stop
   moves, skipped trades, capital rotation. Each item links to the trade's reason.
5. **Controls** — remove a stock, reduce allocation, pause new entries, open
   deeper analysis (reuse the Ask‑Falcon right‑column analysis) before committing.
6. **Replay mode** — pick a past start date; simulate how Falcon would have managed
   the chosen capital using ONLY point‑in‑time info (walk‑forward, no look‑ahead).
7. **AutoTrade bridge** — once performance is shown, a contextual CTA: "Connect
   your broker to automate eligible Falcon trades" → AutoTrade (Launch‑Pending).

## Data sources / Backend (what's real vs needed)
- Signals + tier + why + rule + SL/target/hold: REAL via `falconTop20` /
  `falcon_signal_day_study` / pattern taxonomy + the existing risk rules.
- Allocation math (qty = floor(alloc / entry); spread/score‑weight; per risk
  profile): computable client‑side from real picks + a real price.
- **Backend needs (don't fake):**
  - `POST /api/power/cotrade/simulate` `{ style, capital, start_date, risk }` →
    full virtual portfolio: positions[], summary{...}, actions[] — a point‑in‑time
    walk‑forward simulation (reuse universe_engine pnl_simulator/falcon_portfolio).
  - `GET /api/power/cotrade/portfolio` (persist a user's virtual portfolio + track
    over time) — open/closed positions, P&L, drawdown, cash.
  - Live/last price per symbol (current LTP, entry fill) — quote feed.
  - Replay = the simulate endpoint with a past start_date (point‑in‑time only).

## Phase plan
- **Phase 1 (this build):** the dedicated `/power/co-trading` page — Setup flow +
  Allocation PREVIEW computed from the REAL current Top‑10 (entry from last close,
  qty, SL, tier, why/rule) + portfolio summary SHELL (starting capital + allocation;
  live P&L/tracking shown as "starts when you begin / market open pending") +
  Falcon Actions feed SCAFFOLD (explains the allocation) + controls (client state)
  + AutoTrade bridge CTA. Replay + live tracking + persistence = Backend needs.
- **Phase 2:** wire `/cotrade/simulate` (real walk‑forward portfolio + actions) +
  replay mode.
- **Phase 3:** persistence + live tracking + the AutoTrade bridge handoff.

## Honesty
Only Swing is live. No fabricated P&L, prices, or "80%+". Tiers = real bands.
Replay uses point‑in‑time data only (no look‑ahead). Virtual ≠ advice.
