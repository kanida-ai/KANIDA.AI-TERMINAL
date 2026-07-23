# Falcon — AI‑Native Front‑End Product Plan

Status: PROPOSAL for operator review. Author: Claude. Date: 2026‑06‑19.
Design system: keep the locked Power‑User dark theme + mint `#3FE3A4` accent.
Benchmark: Claude.ai (Chat / Cowork / Code) — left nav switches *modes*, each
mode has ONE primary action, progressive disclosure, conversational entry.

---

## 0. Recommended path (me vs Claude Design)

**Recommendation: I own the spec + build directly; use Claude Design for ONE thing only.**

- I write every spec/flow/requirement (this doc) and implement directly — I hold the
  codebase + engine + design‑system context that Claude Design does not.
- Use Claude Design as a *visual accelerator for the two net‑new, design‑sensitive
  surfaces only*: (a) the **Home / Ask‑Falcon hero** and (b) the **guided‑prompt
  composer**. These are new interaction patterns where seeing beats describing. I'll
  hand Claude Design a precise brief (colors, tokens, the composer states). Everything
  else (Signals, Co‑Trading, AutoTrade, Proof, Plans) is an *evolution of existing
  pages/components* — no design round‑trip needed; I build them directly.
- Net: no full‑app design detour (slow), but the novel hero is de‑risked visually
  before code. If you'd rather skip even that, I can build the hero directly too.

---

## 1. Information architecture + left navigation

Today there are 17 disconnected routes. Collapse to a **single shell** (persistent left
nav + main work area, exactly like the screenshots) with **6 primary modes**:

| Mode (nav) | Icon | One primary action | Replaces today's routes |
|---|---|---|---|
| **Ask Falcon** (home) | spark | "Ask about a stock / sector / today's picks" | new (home `/power`) |
| **Signals** | signal | "See today's Top 10 for my style" | today, replay/[date] |
| **Co‑Trading** | handshake | "Understand & plan a trade" | (new; uses live data) |
| **AutoTrade** | bolt | "Check my readiness / what's live" | live, premarket (operator) |
| **Performance** | chart | "Track my picks & proof" | portfolios, sizing, credibility, replay/proof |
| **Plans** | lock | "Upgrade to unlock" | pricing, billing |

Footer/account (not primary): **Account** (profile, broker, persona), **Learn Falcon**,
**Admin** (operator‑only; existing). `login/signup/redeem/waitlist` stay as pre‑auth.

Principles: one job per screen; each nav click swaps the main area (no page‑hop feel);
kill naming drift (Falcon‑10 / top‑14 / top‑100 → just **"Today's Top 10"**).

---

## 2. Home / Ask‑Falcon landing experience

Layout (recommended — **AI prompt is the hero, signals are one peek below**):

```
  ✴ Good morning, Shyam
  <rotating market‑pulse line>            ← 1 line, calm, e.g. regime + top sector

  ┌─────────────────────────────────────────────┐
  │  Ask Falcon…           [intent ▾] [Ask →]    │   ← guided composer (Section 3)
  └─────────────────────────────────────────────┘
  [Today's Top 10] [My Style] [Find more] [Sector strength] [Performance]   ← action chips

  ── Today's Top 10 (peek) ───────────────────────  "see all →"
  #1 NETWEB  ·  GOLD  ·  +3.7%   …  (3–4 compact rows, tap → Signals)
```

Why prompt‑central: it makes the product *feel* like an AI product (the core gap), it
supports the future conversational/AutoTrade roadmap, and the Top‑10 peek keeps the
trader's daily job one tap away. The peek is a *strip*, not a dashboard.

- **Greeting**: `Good {morning|afternoon|evening}, {first_name}` (IST‑aware).
- **Market‑pulse line** (rotating, 1 at a time): market regime (from the regime engine),
  strongest sector today, or "the most important Falcon observation of the day." Clean,
  never more than one line.
- Premium, calm, lots of whitespace — not a data wall.

---

## 3. Guided prompt design (structured, not open‑ended)

No raw free‑text to an LLM. A **composer** = intent → context → Ask. Each intent maps to a
deterministic backend query over existing engine data; an optional narrator layer
(reuse `explainer.generate_story`) phrases the result in trader language.

| Intent (dropdown) | Second control | Backend source |
|---|---|---|
| Analyze a stock | stock search ▾ | falcon_features + signal_day_context + taxonomy + tiering |
| Analyze a sector | sector ▾ | sector_stats (top20 explainer) |
| Explain today's Top 10 | — | today's picks + per‑pick why |
| Find setups beyond Top 10 | universe ▾ | compute_top_n ranks 11–50 |
| Compare two stocks | 2× stock ▾ | features diff |
| Review my portfolio | — | portfolio service |
| Market & sector strength | — | regime + sector ranks |
| Why is X Gold / Enterprise | stock ▾ | signal_tier + reasons |
| Check AutoTrade readiness | — | preflight checks |

Output = a **focused result panel** (structured card, not a chat dump): trend, multi‑day
price behavior, volume/unusual activity, extended vs consolidating vs strong, what the
pattern engine sees, sector context, tier (Gold/Gold+/Enterprise‑candidate/Enterprise),
and risk warnings (overextension, weak‑volume). Trader‑friendly language, not TA jargon.
Guardrails: invalid/empty selections are impossible (everything is a chosen entity).

---

## 4. Trading‑persona selection + personalized flow

First‑run: a **persona picker** (reuse `CompassPersonaOrbit`). Honest LIVE vs Launch‑Pending:

| Persona | State | Holding | Notes |
|---|---|---|---|
| Falcon Top 10 Swing | **LIVE** | ~7–30d | the validated engine |
| Weekly Swing | LIVE (variant) | ~weekly | persona knob on same engine |
| BTST | **Launch Pending** | 1 day | needs intraday engine |
| Intraday | **Launch Pending** | minutes–hours | needs intraday engine + data |
| Long‑Term Investor | **Launch Pending** | months | needs different ranker |

Selected persona adapts Signals (which picks, ranks, holding, risk controls, expected
behavior). Stored on the user profile. Don't fake personas that aren't real — mark them
Launch Pending (Section 8) so the product feels complete without lying.

---

## 5. Signals / Co‑Trading / AutoTrade workflows

**Signals** (persona‑aware): Today's Top 10 with rank + confidence, Gold/Gold+/Enterprise
tags, why‑selected, suggested entry context, stop‑loss/risk, and the **signal tracker** —
what happened to signals from 7/14/20/30d ago, historical performance of similar setups,
and whether the current setup is **improving or weakening after generation**. Reuse
`Top20Card`; add an expandable "track" panel. Each row opens a focused panel (no page‑hop).

**Co‑Trading** (semi‑auto, human in loop): understand *why* Falcon likes a stock → review
supporting signal logic → see the trade plan (entry/SL/target/size) → track outcome after
entry. Reuse the existing live/premarket plumbing; present as one focused flow.

**AutoTrade** (full auto): readiness check → pick persona → configure risk → view eligible
signals → clear **LIVE vs Launch‑Pending** split. Honest today: auto‑trade is
operator‑account‑only; per‑user broker connect is Launch Pending. Show the readiness rail
(token, funds, risk config) and what *will* happen when it goes live.

---

## 6. Past‑signal proof for free users

Free users build confidence on history before paying:
- Past Falcon signals from selected prior dates (reuse `replay/[date]`, `replay/proof`).
- Outcomes after 7/14/20/30 days; historical performance by persona.
- Limited previews of Gold/Enterprise *logic* (not today's live picks).
- Clear "what Falcon identified + how it performed" narrative.
- Prominent **"Unlock Live Signals → Upgrade"** CTA.

This is the conversion engine: prove the edge on the past, gate the present.

---

## 7. Subscription, paywall, tiering placement

Re‑wire the dormant paywall (built + audited GREEN; currently un‑wired). Plans:

| Plan | Gets |
|---|---|
| Free | history/proof only (Section 6) |
| Retail / Basic | Standard live signals |
| Pro | Standard + **Gold**, entry insights, deeper analysis |
| Enterprise | + **Enterprise** signals, full rulebook evidence, advanced analytics, AutoTrade (when live) |

Placement (don't clutter home): tier badges + lock states **inside signal cards**, the
**Plans** page, and the **analysis detail** view. Reuse `PaywallNotice` + `TierBadge`.
Gate live `/api/power/...today` + Falcon‑AI analysis + Gold/Enterprise + AutoTrade by plan.

---

## 8. Launch‑Pending pattern (no dead ends)

Every future feature gets a built page now with a premium **Launch Pending** state:
a `LaunchPending` component — title, what it will do, what to expect, an honest
"why not yet," and a "Notify me / Join waitlist" CTA. Applies to: per‑user AutoTrade,
BTST/Intraday/Long‑Term personas, F&O/Index modes. Never an empty nav item or 404.

---

## 9. Component structure + reusable patterns

```
app/power/(shell)/layout.tsx        ← persistent left nav + main; mode routing
  ask/        page.tsx              ← Home / Ask Falcon (hero + composer + peek + chips)
  signals/    page.tsx              ← persona‑aware Top 10 + tracker
  co-trading/ page.tsx
  autotrade/  page.tsx
  performance/page.tsx
  plans/      page.tsx

components/power/ (reuse + new)
  REUSE:  PickCard, Top20Card, TierBadge, PaywallNotice, EquitySparkline,
          CompassPersonaOrbit, Top20Filters, ExpandablePickRow, UserMenu
  NEW:    AppShell (nav), GreetingHero, MarketPulseLine, FalconComposer (intent+context),
          ActionChips, AnalysisResultPanel, SignalTrackerPanel, PersonaPicker,
          LaunchPending, PlanGate (wraps gated content), AskFalconResult
```
Patterns: one‑job‑per‑screen, progressive disclosure (peek → expand → focused panel),
plan‑gating via a single `<PlanGate requires="pro">` wrapper, persona context provider.

---

## 10. Phased implementation plan (highest value first)

| Phase | Scope | Why first |
|---|---|---|
| **1. Shell + Home + Signals + Paywall** | AppShell nav; Ask‑Falcon home (greeting, composer UI, Top‑10 peek, chips); persona‑aware Signals with tracker; **re‑wire paywall**; `LaunchPending` component | Turns the dashboard into an AI product + turns on revenue — the two audit priorities |
| **2. Ask Falcon analysis** | Wire the guided‑prompt intents to backend endpoints + `AnalysisResultPanel` (stock/sector/compare/why‑tier/beyond‑Top‑10) | Makes the "AI co‑trader" claim true |
| **3. Performance + Proof** | Unified Performance mode; free‑user proof + Unlock CTA | Conversion engine |
| **4. Co‑Trading + AutoTrade (readiness)** | Co‑Trading focused flow; AutoTrade readiness + config + Launch‑Pending per‑user | Bridges to automation honestly |
| **5. Persona expansion** | BTST/Intraday/Long‑Term — real engines (big, later) | Only after the product shell + revenue exist |

**Start = Phase 1.** Recommended first screen: the **AppShell + Ask‑Falcon home**, because
it sets the whole product's feel; then **Signals** (the daily job) and the **paywall**.
```
```
