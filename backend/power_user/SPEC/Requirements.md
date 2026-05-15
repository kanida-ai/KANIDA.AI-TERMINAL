# KANIDA.AI Power User Portal — Requirements

**Version:** 1.1 (locked — explainer-first MVP)
**Date:** 2026-05-14 IST
**Status:** Decisions locked. Moving to Design.

---

## 1. Why this product exists

KANIDA's Falcon V7.1 engine has produced ₹30L → ₹1.05Cr over a 3.3-year walk-forward (all 4 years positive, max DD −10%). The Power User Portal exposes the **signals + reasoning** — never order routing — to a skilled retail audience that already pays for TradingView / Chartink / Screener / Trendlyne but lacks "why this stock, why today, what's its history."

## 2. Phase 1 (this build) = explainer-first MVP, 1 week, production-grade

Three tabs, three personas covered, real auth, real DB, real caching:

| Tab | Endpoint | Public preview | Power-user (auth) |
|---|---|---|---|
| **Today's Picks** | `/falcon/picks/today` | Top 5 with full stories | Top 100 + filters + watchlist |
| **Historical Replay** | `/falcon/picks/replay?date=YYYY-MM-DD` | 3 featured dates + 🎲 Random Replay (any date, last 2yr) | Any custom date with deeper drill-down |
| **Live Decisions** | `/falcon/picks/live` | — (auth required) | 9:30 / 9:45 / 10:00 IST tier decisions |

Phase 1B (clean second sprint, 2 weeks behind): custom backtest, per-stock failure filter, email digest, watchlist deep features.

## 3. Personas covered in Phase 1

- **Night Scanner** (A) — opens at 9 PM, scans tomorrow's shortlist ✓
- **Tool Collector** (B) — single page replacing 4 tools ✓
- **Tip Buyer** (C) — gets the *reason* per pick, not just ticker ✓
- **Busy Professional** (F) — sees the digest content on the today page (email delivery is Phase 1B) ✓ (partial)
- **Strategy Tester** (D) — Phase 1B (custom backtest)
- **Breakout Victim** (E) — Phase 1B (per-stock fail filter)

## 4. Locked decisions

| # | Decision | Locked value |
|---|---|---|
| 1 | Scope phasing | Option A: explainer-first MVP (1 week production-grade) |
| 2 | Live tier data source | Admin Kite token, centralized; scheduled job at 09:30:30 / 09:45 / 10:00 IST writes to `falcon_live_decisions`; users read pre-computed |
| 3 | Replay outcomes | Hybrid — 3 featured dates pre-computed into `falcon_replay_cache` at deploy; arbitrary dates on-the-fly from `ohlc_daily` + 24h API cache |
| 4 | Featured dates | 2026-04-15 ("The Blowout" — 92% D+5 WR), 2024-11-04 ("Year-Ago Proof" — 96% D+5 WR), 2025-12-15 ("Steady Day" — 78% D+5 WR) + 🎲 Random Replay |
| 5 | Auth gate | Option B: public preview (3 featured + Random + Today's top 5) → Google sign-in + invite gate for power features |
| 6 | Cache infrastructure | SQLite tables (no Redis dep) — already sub-50ms on this codebase |
| 7 | Domain | `app.kanida.ai` (production); localhost during dev |
| 8 | Beta cohort size | 12-18 invite codes, admin-issued |

## 5. Performance contract (binding)

| Page / endpoint | Target | Strategy |
|---|---|---|
| Landing (3 featured cards) | < 500ms first paint | Cards = SSR-prerendered with `falcon_replay_cache` payload |
| Today's top-100 | < 1s | Single SQL on `falcon_signals_live` + in-memory enrichment from `falcon_pattern_candidates` |
| Random replay (cache miss) | < 2s | On-the-fly: features → patterns → outcomes; ~1.5s observed in worktree script |
| Random replay (warm) | < 200ms | API-layer SQLite cache, 24h TTL |
| Live decisions page (post 09:45 IST) | < 200ms | Pre-computed in `falcon_live_decisions` table |
| Per-pick detail expansion | instant | Detail data shipped with top-N payload |

## 6. Engine output contract (READ-ONLY for this build)

| Table | Owner | Power-user code | Notes |
|---|---|---|---|
| `falcon_signals_live` | operator pipeline | READ | top-100 per signal_date, with sample_rules JSON |
| `falcon_pattern_candidates` | operator pipeline | READ | 2,283 rules; needed for FULL rule text (sample_rules only has 5) |
| `falcon_promoted_patterns` | operator pipeline | READ | 865 patterns; classification + avg_oos_year_lift_pp |
| `falcon_features` | operator pipeline | READ | 432K rows; needed to recompute top-N on arbitrary historical dates |
| `falcon_sectors` | operator pipeline | READ | symbol → sector mapping |
| `ohlc_daily` | operator pipeline | READ | for actual-outcomes (replay) |
| `kite_tokens` (kanida_quant.db) | operator | READ-ONLY (scheduler only) | admin Kite token for live tier |

**Power-user code WRITES only to power_user_* tables and `falcon_live_decisions` + `falcon_replay_cache`.** Never touches operator engine state.

## 7. Auth model

- Anonymous (no JWT): can read public preview endpoints; landing page, 3 featured replays, 🎲 Random Replay, today's top 5
- Authenticated (Google sign-in + valid invite): full top-100, custom historical replay, live decisions, watchlist (Phase 1B), backtest (Phase 1B)
- Admin (operator behind ADMIN_SECRET): invite code issuance, beta-user management, metrics

Invite flow:
```
Google sign-in → backend checks email
  in power_user_users with is_active=1 → issue 24h JWT → redirect to /signals
  not in users → /redeem-invite page → user enters one-time code
    valid code → create user, mark code used → issue JWT
    invalid → /waitlist (capture email)
```

## 8. Non-goals (explicit OUT of scope for Phase 1)

| Out | Why |
|---|---|
| Custom backtest UI | Phase 1B |
| Email digest delivery | Phase 1B (content is in Today's page) |
| Watchlist save/load | Phase 1B (schema created now for forward-compat) |
| Per-stock failure filter | Phase 1B |
| Auto-trade / order placement | Phase 2 (never on public side) |
| Per-user Kite integration | Never (users execute via own broker) |
| Real-money execution | Not in this product |
| Payment integration | Beta is free |
| Mobile native app | Web-responsive only |
| Operator-only pages | Strict separation; lives at `/falcon/*` |

## 9. Polish items (locked, must be in MVP)

1. **Sector word naturalization** — `"A telecommunication in..."` → `"A telecom stock in..."`; map all 22 sectors to natural words
2. **Sentence capitalization** — `"... 18% gain. just confirmed"` → `"... 18% gain. Just confirmed"`
3. **Story joining** — no double periods; first letter of each sentence capitalized

All three fix `generate_story` in `explain_picks_v3.py` during port.

## 10. Success metrics (90 days post-launch)

- 12-18 invites issued, 70%+ activation (10-12 active users)
- 50%+ DAU/MAU month 2
- 3+ min median session time
- Zero auto-trade incidents from public surface (code-path separation works)
- Investor demo: 🎲 Random Replay button passes the "you didn't cherry-pick" test in a live call
