# Falcon Front-End — Design Decisions & Feedback Log

The `falcon-ui` agent reads this every run and APPENDS a dated entry after each
change. Operator feedback goes here too. This is how design iteration compounds.
Spec lives in `docs/specs/FALCON_AI_FRONTEND_PLAN.md`.

## Locked decisions (as of 2026-06-19)
- **Theme:** dark + mint `#3FE3A4` ("green = profit"). No new accent colors.
- **Benchmark:** Claude.ai — left nav switches modes; one primary action/surface;
  progressive disclosure; conversational (guided) entry. Feel = AI product, not dashboard.
- **IA — 6 modes:** Ask Falcon (home) · Signals · Co-Trading · AutoTrade ·
  Performance · Plans. Account/Learn/Admin in footer. (Collapses the 17 legacy routes.)
- **Home layout:** prompt is the HERO; Today's Top 10 is a compact one-tap "peek"
  strip below it (not a dashboard). Greeting "Good {morning…}, {first_name}" +
  one rotating market-pulse line.
- **Prompts are GUIDED only:** intent dropdown -> entity (stock/sector) picker -> Ask
  -> focused result panel. No open free-text to an LLM.
- **Personas:** Falcon Top 10 Swing (+ Weekly) = LIVE. BTST / Intraday / Long-Term =
  Launch-Pending. Per-user AutoTrade = Launch-Pending (operator-only today).
- **Paywall:** re-wire (built+audited, currently dormant). Free = history/proof;
  Basic = Standard; Pro = +Gold; Enterprise = +Enterprise + rulebook + AutoTrade.
  Gating via a single `<PlanGate>`; tier badges/locks inside cards, not on home.
- **Honesty:** no "certified 80%", no "smartest on earth", no "500 agents/stock".
  Explainability-led copy. Tiers = qualitative quality bands.
- **Build discipline:** dev branch/worktree off main; never touch prod tree or the
  auto-trade execution path.
- **Path:** build directly in the mint theme (no static-design detour); hero-first
  as a clickable prototype, operator reacts, then continue.

## Phase order
1a. AppShell (6-mode nav) + Ask-Falcon home (greeting, pulse line, guided composer,
    chips, Top-10 peek) + LaunchPending component.   <-- START HERE
1b. Persona-aware Signals + 7/14/20/30d tracker + paywall re-wire.
2.  Ask-Falcon analysis endpoints + result panels.
3.  Performance + free-user proof.
4.  Co-Trading + AutoTrade readiness.
5.  Persona expansion (real BTST/Intraday/Long-Term engines).

## Feedback / change entries
<!-- newest first; falcon-ui appends here -->
- 2026-06-29 — AutoTrade Phase-2 frontend shipped (worktree: _kanida_falconui, branch feat/falcon-ai-shell). Three features: (1) Universe filter chip row added to PortfolioAutoTrade.tsx CONFIG phase — five chips (All 500 / Nifty 50 / Nifty 100 / Nifty 200 / F&O) styled identically to Top20Filters.tsx (rounded-full, mint active state, tracking-wide uppercase). Sends `universe_filter` in SessionConfig on create + preview; defaults to 'all500' which is current backend behaviour = graceful degradation. (2) Manual stock picker added below universe chips: loads ranked picks via new `AutoTradeAPI.sessionPicks(universe, topN)` endpoint (GET /api/autotrade/session/picks); shows skeleton while loading; on 404/error shows "Universe preview unavailable — top N picks will be used" inline note (no crash, session still creates normally). Picks list shows rank badge + symbol + sector chip + score. Top N rows pre-checked; user can check/uncheck freely. If selection matches default top-N, no `symbol_whitelist` is sent. If it differs, sends `symbol_whitelist: string[]`. Clicking a different Top N chip re-syncs the checked set. (3) Phase-2 uncommitted files committed: BrokerAccountsPanel.tsx (new), TradeJournalPanel.tsx (new), AutoTradePanel.tsx (journal+broker tabs), page.tsx (user.id threading), autotrade-api.ts (broker/journal/picks types + sessionPicks method). `npx tsc --noEmit` and `npm run build` both pass with zero errors. Design decisions: universe filter placed in a dedicated card outside the 2-col config grid (full width, consistent with the kill-switch card pattern); manual picker uses accent-mint checkbox; rank badges are mint for top-N, faint for extras (secondary refinement made visually clear).
- 2026-06-28 — Daily Trade Journal tab added to the AutoTrade console (worktree: _kanida_falconui, branch feat/falcon-ai-shell). Three files changed: (1) `frontend/lib/autotrade-api.ts` — added `TradeJournal`, `JournalPosition`, `JournalSessionSummary`, `JournalReviewItem` types + `AutoTradeAPI.journal(sessionId)` method hitting `GET /api/falcon-proxy/api/autotrade/session/{id}/journal`; (2) `frontend/components/power/autotrade/TradeJournalPanel.tsx` — new self-contained panel (session selector, stat cards row, positions table with expandable detail rows, review flags section, Copy/CSV/Excel export toolbar, loading skeleton, error+retry state); (3) `frontend/components/power/autotrade/AutoTradePanel.tsx` — added "Journal" tab (index 2, after Sessions) + `activeSessionId` state + `PortfolioAutoTrade` receives new `onSessionChange` callback. `PortfolioAutoTrade.tsx` updated: `onSessionChange` prop fires on `onResume` and `onCreate`, dependency arrays updated. `npx tsc --noEmit` passes with zero errors. Design decisions: flat surfaces/no gradients per system rules; review-flag colours per spec (DEEP_LOSS=red, LARGE_WIN=mint, STOP_RECOVERED=amber, CONTINUED_DECLINE_OPEN=orange, WINNER_OPEN=mint); Excel downloads as CSV with .xlsx extension (no xlsx library in project) per the spec's approved fallback.
- 2026-06-19 — Log + falcon-ui agent created. No screens built yet. Next: Phase 1a.
