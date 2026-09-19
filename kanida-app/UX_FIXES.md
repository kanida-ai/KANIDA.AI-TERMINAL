# KANIDA pilot — UX fix tracker

Started 2026-09-13. Source backup taken before any edit (scratchpad `kanida-app-backup-before-ux-fixes`).
Status: ✅ fixed (struck through) · ⚠️ partial / awaiting verification · ❌ not fixed

## Final verification (2026-09-13)
- **Typecheck:** 0 errors (baseline had 2).
- **Server tests:** **65/65 pass** (baseline 32).
- **Web build and pilot:** web export succeeds; pilot restarted with the new server code.
- **Source hygiene:** longest source line 1,479 chars (was 4,455); 0 text-glyph checkboxes.
- **QA scripts:** `check-model` and `check-chart-window` pass. `check-shapes.cjs` fails, but it is pre-existing and stale (byte-identical to the backup; calls `/api/chart` unauthenticated and drives the old pre-pilot UI).
- **Code diffs:** `live.py` gained only refusal checks before any intent is recorded (no broker/order/lock changes; live stays locked); `app.py` only passes evidence/settings into the services.
- **Browser (signed-in Chrome), checked on the running pilot:**
  - Discover: ranking by "95% low of avg", caveat, verdict chips, sample badges, data age, rule labels, Prepare trade blocked with reason, separate Simulate link, Watch tab.
  - AutoTrade: capital excludes the ₹224 scenario, loss-limit line, stale-data card, ILLUSTRATIVE/legacy badges, no simulate on illustrative.
  - Activity: IST dates.
  - Redirects: `/portfolios` → `/watch`, `/agent` → `/`.
  - Simulate hand-off: back link, full history from 2017-04-10 matching the replay, and the return restores APCL with filters kept.

## Workers
- Wave 1 — all ✅: A Discover · B Chart · C AutoTrade/Account · D Simulate/Replay · E Evidence · F Shell/nav
- Wave 2 — all ✅: 2-1 Discover/plans · 2-2 Simulate/Replay/PlanReview · 2-3 dead code · quant audit
- Wave 3 / final — all ✅: W3 server gate hardening · F1 quant labels/errors · F2 ui a11y, breakpoints, archive, constants · orchestrator touch-ups (new error codes, loss-limit display, last constants)

## 1. Trust in the numbers
| ID | Issue | Status |
|---|---|---|
| 1.1 | ~~Stats shown aren't for the rule you'd trade; Prepare trade sizes an untested 1:2 rule~~ — see below | ✅ |
| 1.2 | ~~Sorted by pattern fit, so losers come first~~ — positive setups rank by the server's 95% lower bound (then average); positive-only default; zero/negative in a separate section; always-visible "partly luck, not corrected for comparing many stocks" caveat | ✅ |
| 1.3 | ~~Small samples look solid~~ — Small/Moderate/Larger sample badge on every row; minimum 10 trades by default everywhere; rows show the 95% low | ✅ |
| 1.4 | ~~Virtual scenario results feed cash balance~~ — available capital excludes scenario outcomes; "Chosen-scenario outcome (synthetic) · NOT CAPITAL"; funds checked on capital; 2% loss limit counts losses only and is shown | ✅ |
| 1.5 | ~~Data is six weeks old, with no warning~~ — data age everywhere; Prepare trade blocked when over 3 days old; server refuses simulate/live with `DATA_STALE` (`PILOT_MAX_DATA_AGE_DAYS`, default 3); explanatory stale-data card | ✅ |

**1.1 details.**
- Headline stats and row averages are labelled "hold-period avg · not this exit rule".
- `tradable_evidence` is true only for the frozen tested rule when all hold: ≥20 later-test trades, a positive later-test average, an exact rule match, scanner `screen=='review'`, and a directional side.
- Plan save is rejected without it (409), unless saved as illustrative, which can't be simulated.
- At simulate and live, the server re-checks evidence, the saved rule and data age.
- Legacy plans read as illustrative.
- Readable messages for every refusal code.
- Tested evidence carries a "selected from many stocks and rules" warning.

**Quant audit (dev-quant-auditor) — all addressed.** Today no cell passes the gate: 1 of 74,412 has ≥20 later-test trades, and it isn't positive.
| ID | Finding | Status |
|---|---|---|
| A1 | Legacy plans could be simulated or taken live | ✅ refused 409 `EXIT_EVIDENCE`; resume → illustrative |
| A2 | Evidence/staleness only checked at save; staleness UI-only | ✅ server re-checks at simulate + live (`EXIT_EVIDENCE_CHANGED`, `DATA_STALE`, `PLAN_CHANGED`, `EVIDENCE_UNAVAILABLE`) |
| A3 | Discover best-of-many ranking without warning | ✅ 95% lower-bound ranking + caveat + labelled row metric |
| A4 | Winner's curse on gate-passing evidence | ✅ warning label |
| A5 | Unadjusted-price disclosure hidden in compact brief | ✅ shown in compact and expanded brief |
| A6 | Gate ignored neutral direction / screen state | ✅ requires `review` + directional side |
| A7 | Scenario gains delayed the loss limit | ✅ losses-only limit, displayed |
| A8 | "Same rule" check always passed by construction | ✅ commented; re-check compares against the saved rule |

## 2. Workflow
| ID | Issue | Status |
|---|---|---|
| 2.1 | ~~Evidence path loses context~~ — "See evidence" opens replay; separate "Test these rules in Simulate →"; "← Back to chart" restores the setup and filters; full-history start matches the replay (browser-verified) | ✅ |
| 2.2 | ~~Decision step gone~~ — For review / Watch / Pass chips + filter counts; `/agent` → Discover | ✅ |
| 2.3 | ~~My watch hidden; /portfolios duplicate~~ — Watch tab; `/portfolios` → `/watch` (browser-verified) | ✅ |
| 2.4 | Buttons respond slowly while the chart animates | ⚠️ See below |
| 2.5 | ~~Opening evidence silently clears filters~~ — filters kept; out-of-filter setups labelled | ✅ |
| 2.6 | ~~Onboarding timeframes unused~~ — they become Discover's default timeframe filter | ✅ |

**2.4 details.** Code fix done (per-frame state removed, staged animated values, memoized layers, click-to-finish); **browser measurement pending**.
- The Chrome tab reported `visibilityState: hidden` (window behind another), which pauses rendering and throttles timers, so responsiveness can't be measured there.
- It is also the likely cause of the screenshot timeouts seen earlier.
- Needs one check with the Chrome window in front.

## 3. Layout and polish
| ID | Issue | Status |
|---|---|---|
| 3.1 | ~~Replay chart only ~140px tall on short screens~~ — 240–420px sized from space; equity stacks on short windows | ✅ |
| 3.2 | ~~Inconsistent date format / no time zone~~ — "13 Sep 2026 · 19:55 IST" (browser-verified) | ✅ |
| 3.3 | ~~Breakpoints disagree~~ — shared `BREAKPOINTS` + `useLayoutMode()` across all screens | ✅ |
| 3.4 | ~~Research failure shown as "no matches"; Billing "awaiting setup" while loading~~ — real error + retry; loading states | ✅ |
| 3.5 | ~~Missing confirmations~~ — grant 14d, recovery link, cancel study, remove watch, pause entries all confirm | ✅ |
| 3.6 | ~~Accessibility~~ — accessible Checkbox, labelled dialog Sheet, tab roles, Chip `aria-pressed`, Button state/hint, Pressable labels, chart text alternative (not yet tested with a real screen reader) | ✅ |

**3.3 intentional shifts.** Phone cutoff 700 → 760; Welcome rows 800 → 760; Workspace 1080 → 1050; Screens padding 1100 → 1050.

## 4. Code cleanup
| ID | Issue | Status |
|---|---|---|
| 4.1 | ~~Unused screens/files~~ — archived (not deleted) to `_archive/` with README; tsconfig excludes it | ✅ |
| 4.2 | ~~Very long single lines~~ — longest now 1,479 chars | ✅ |
| 4.3 | ~~Hardcoded values repeated~~ — `src/constants.ts` adopted across screens (model.ts keeps commented literals for `check-model.cjs`) | ✅ |
| 4.4 | ~~evidence.py:21 refuses studies when research folder set~~ — intentional offline snapshot mode; comment + test | ✅ |

**4.1 archived:**
- StrategyFlow and StrategyEvidence
- dead parts of Screens and TraderShell
- Chart.tsx
- root index.ts + App.tsx
- ExecutionDesk removed
- unused imports removed

## Follow-ups (not blocking)
- Run the 2.4 responsiveness check with Chrome in front.
- Add a "prepare again" action for illustrative/legacy plans (currently archive only).
- Rewrite `scripts/check-shapes.cjs` for the signed-in pilot UI.
- Allow `./constants` in `scripts/check-model.cjs` so model.ts can adopt shared constants.
- Screen-reader test (NVDA/VoiceOver).

## Outside this app's scope (market_scanner / data)
- **Unadjusted prices.** No corporate-action/dividend adjustment (`backtest_rules.json:21`); gaps over 35% are dropped (`data.py:186`). This breaks the quant rule and must be fixed in the data; the app discloses it in every exit brief.
- **Stale data.** Stored market ends 31 Jul 2026; nothing can be prepared or simulated until prices are refreshed. The app and server now say so.
- **Structural stops.** No point-in-time engine replays pattern-geometry stops, so the 1:2 benchmark can't earn tradable evidence yet.
- **Study engine start date.** It falls back to 2020-01-01; a real "full history" option would allow multi-stock full-history studies.
