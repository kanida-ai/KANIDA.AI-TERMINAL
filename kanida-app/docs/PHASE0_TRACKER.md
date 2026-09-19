# KANIDA redesign — Phase 0 (Foundations) tracker

Started 2026-09-14. User approved TRENDSPIDER_STUDY.md §10 ("go with your recommendations").
Baseline: typecheck 0 errors · server tests 65/65 · backup `scratchpad/kanida-app-backup-before-phase0` (91 files).
Status: ⏳ in progress · ✅ done (struck through, verified) · ⚠️ partial · ❌ not done

## Decisions (§12A, recommended options applied)
| # | Decision | Choice |
|---|---|---|
| 1 | Five-region layout | ✅ Approved: top bar · left tool rail · chart centre · right widget sidebar · bottom dock |
| 2 | Full-page routes | Keep `/simulate` `/watch` `/autotrade` `/activity` as deep links that open the matching dock tab full screen |
| 3 | Dock tabs | Discover · Evidence/Replay · Simulate · Plans/AutoTrade · Activity (Events, Market Map arrive in Phase 3) |
| 4 | Sidebar defaults | Watch + Evidence summary. Seasonality/Corporate actions/Notes wait for data; no placeholder widgets with fake content |
| 5 | Study overlay lifetime | Markers stay until unpinned |
| 6 | Phone | Bottom sheet over chart (peek / half / full) |
| 7 | Pinned study re-run | "Follow symbol" toggle, default ON |
| 8 | Legend density | Full evidence badges on the selected pattern; compact rows for other patterns |
| 9 | Dashboard | Same tab (Phase 4) |
| 10 | Condition grammar Phase 1 | Patterns + evidence + verdict + price + universe |
| 11 | Alerts | In-app (paper) only first |
| 12 | Event data priority | Corporate actions + results calendar → SEBI PIT/SAST → bulk/block deals |
| 13 | Data prerequisites | Corporate-action adjustment + fresh OHLC before Phase 1 evidence work (Phase 0 UI proceeds) |
| 14 | Market Map colour | Expectancy 95% low |
| 15 | Theme | Keep mint/dark |
| 16 | Options chain / OI heatmap | Out of scope for now (NSE data licence) |
| 17 | AI assistant | "Ask Falcon", model disclosed |

## Phase 0 work
Wave 0A runs three parallel workers with separate file ownership. Wave 0B is the shell. Wave 0C is QA.

| ID | Item | Owner | Status |
|---|---|---|---|
| 0.1 | ~~Active-symbol store~~ — see below | S | ✅ |
| 0.2 | ~~Layout primitives~~ (browser behaviour checked in 0C) | P | ✅ |

**0.2 details** (verified: typecheck 0 errors; `src/layout/` present with all exports)
- **Primitives in `src/layout/`:** `Dock` · `Sidebar` · `Widget`/`WidgetError` · `Popover`/`MenuList`/`useDismiss` · `BottomSheet` · `TopBar`/`DataAgePill` · `ToolRail` · helpers (`IconButton`, `useDragResize`, `useRoving`, `readStore`/`writeStore`).
- **Dock:** tablist/tab/tabpanel with arrow and Home/End keys; inactive panels stay mounted, so drafts survive; resize separator is keyboard-accessible; size commits on release; persists `kanida.layout.dock.<key>`; `restoreActive={false}` for deep links.
- **Popover:** Escape closes only the topmost popover; a backdrop catches outside clicks, so a second click on the trigger can't select an item underneath; closes on route change via `routeKey`.
- **DataAgePill:** mint when fresh, amber "STALE" past `DATA_STALE_DAYS`, red past 5×.
- **Sheet:** already closes on Escape through RN-web `Modal` `onRequestClose`.
- **Phone:** Dock tabs scroll; Sidebar becomes swipe cards; ToolRail horizontal; BottomSheet snaps.
- **Shell needs:** pass `usePathname()` as `routeKey`; put the BottomSheet in a full-screen container with the chart behind; hide the chart while the Dock is full screen.
| 0.3 | ~~Legend control centre + hover readout~~ — see below (browser behaviour checked in 0.13) | L | ✅ |
| 0.4 | Region shell composing 0.1–0.3; existing screens mounted as-is in dock tabs; Watch + Evidence summary widgets; top bar data-age pill | B1 + B2 + B3 | ⚠️ See below |

**0.4 progress**
- ✅ **Widgets part (B2), verified:** typecheck 0 errors; check-model passes; exports present.
  - `src/widgets/`:
    - `WatchWidget` + `watchWidget()` Sidebar entry (menu: Open full view · Discover setups).
    - `WatchRow`/`watchStatus`/`useWatchRemoval`, shared with the `/watch` WatchDesk (same markup; route untouched).
    - `EvidenceSummaryWidget` + `evidenceSummaryWidget()`.
  - `AgentCase.tsx` exports `useCaseEvidence(match, side)`; AgentCase renders as before.
  - **Evidence widget keeps every label and gate:**
    - hold-period label and 95% range
    - n + sample label
    - costs 0.40% included, next-open entry
    - data-age/STALE badge
    - "Picked from many" caveat and hypothetical-short pill
    - compact ExitBrief with tradable/illustrative reason and the unadjusted-prices note
    - `prepareBlock` gating
  - **Key guard:** remounts per match; numbers only when `evidenceFromWorkspace` confirms history/symbol/TF/match/side and the exit request for this key completed. During an A→B switch it shows B's header + "Loading evidence…" with no numbers.
  - **Symbol-only selection:** "No stored setup selected for SYMBOL" + up to 4 setups to pick (no auto-pick).
- ✅ **Chart/Discover/Evidence split (B1), verified:** typecheck 0 errors; check-model 21+108 and check-chart-window 8 pass; web export exits 0.
  - `src/workspace/index.ts` exports `ChartCentre({phone,compactLegend,onOpenEvidence,onPrepare})`, `DiscoverPanel({layout:'dock'|'sheet',onSelect?})`, `EvidencePanel()` and `SetupSummary`, plus helpers `useDiscoverRows`, `useWorkspaceMatch`, `resolveSymbolMatch`, `historyFor`, `discoverSort`, `matchKey`, `openSimulate`.
  - **DiscoverPanel:** data age, refresh/research error + Retry, search, Filters, TF chips, verdict chips + counts, 95%-low sort, positive-only, min trades, "partly luck" caveat, hold-period rows, sample badges, zero/negative section. The row writes `setActive(source 'discover')`; the highlight is full vs a faint marker; ↑/↓ on web.
  - **ChartCentre:** header (age · fresh prices needed · outside filters) · Prev/Next · Watch · legend (compact one-liner on phone) · unchanged `prepareBlock` Prepare trade · See evidence · "Test these rules in Simulate →" with origin.
  - **EvidencePanel:** StockReplay (markers still inside the replay's own chart; the main-chart markers are Phase 1).
  - **Symbol-only rule:** keep the store's matchId if it's still valid → otherwise the first same symbol/TF match in Discover order → otherwise rank all stored matches → otherwise "No stored setup for SYMBOL on TF" (no evidence, Prepare disabled).
  - **Keyed loading:** chart/replay keyed by `evidenceKey(symbol,pattern,TF,side,run,candle_end)`; `useExitPlan` cache key verified as key-specific with late results dropped.
  - **PatternCanvas:** `hiddenPatterns?:string[]` (opacity 0, animation unchanged); only the selected pattern is drawn, so 👁 on other rows dims the row only.
  - **Import gotcha:** import from `./workspace/index`, not `./workspace` (Windows resolves that to `Workspace.tsx`).
- ✅ **Shell composition (B3), code verified:** typecheck 0 errors; check-model 21+108 and check-chart-window 8 pass; server tests 65/65; web export exit 0. **Browser verification is pending (0C).**
  - **Files:**
    - `src/MainWorkspace.tsx` (desktop/tablet/phone composition, dock-tab ↔ URL sync)
    - `src/shell/routes.tsx` (route↔tab maps)
    - `src/shell/SymbolSearch.tsx` (top-bar search → `setActive` source 'search')
    - `src/shell/parts.tsx` (mark, Watch toggle, Account, research banner)
    - `src/PilotShell.tsx` (gating kept; one persistent MainWorkspace on workspace routes)
    - `app/index|simulate|watch|autotrade|activity` re-export `WorkspaceRoute` (originals copied to `scratchpad/app-before-b3`)
  - **Desktop:** TopBar (mark · search · data-age pill · Watch · Account) · ToolRail (Prev stock · Next stock · See evidence) · ChartCentre · Sidebar (Watch, Evidence summary) · Dock tabs Discover · Evidence/Replay · Simulate · Plans/AutoTrade · Activity · Watch.
  - **Tool rail:** the chart-window, replay-drawing, scenarios and inner-swings controls stay inside PatternCanvas (they're internal state), so the rail holds only Prev, Next and See evidence.
  - **Tablet:** "Widgets" button → right Sheet with the two widgets.
  - **Phone:** compact top bar; chart about 45% + widget swipe cards; BottomSheet with Discover · Watch · Simulate · AutoTrade · Activity (+Evidence via "See evidence").
  - **Edits outside the shell:** SimulationDesk `onBack` prop (Back to chart switches the dock); TraderDesk `DeskEmbedded` padding context.
  - **Known gaps to test in 0C:**
    - Watch and Prev/Next each appear twice (top bar/rail and chart header).
    - AutoTrade keeps polling every 5s once its tab has mounted.
    - Simulate remount on a new origin.
    - Auto-selection of the top Discover result on every breakpoint.
    - The "Your agents" sheet isn't in the new shell (moved to a later phase).
    - Stock sheet / Watch "Review" / Welcome example hand-offs; back/forward; fast tab switching; 1366×543; phone sheet drag.
| 0.5 | Deep links: `/simulate` `/watch` `/autotrade` `/activity` open dock tab full screen; `/agent` `/portfolios` redirects kept | Shell | ⏳ |
| 0.6 | Phone adaptation: chart first + one-line legend + tabs as bottom sheets + swipe cards | Shell | ⏳ |
| 0.7 | Tablet: chart + dock, widgets in slide-over | Shell | ⏳ |
| 0.8 | QA: typecheck, check-model, check-chart-window; **new pilot acceptance script** (see note) | QA (0C) | ⏳ |
| 0.9 | Screens: 1440×1100, 768×1024, 390×844, 1366×543 short screen | QA | ⏳ |
| 0.10 | Symbol change from each source updates chart, legend, widgets | QA | ⚠️ See below |
| 0.11 | Evidence never shown for a different key (quant audit) | QA | ⚠️ See below |
| 0.12 | Every §10.6 mapped feature reachable; evidence gates unchanged | QA | ⚠️ See below |

**QA1 Playwright acceptance** (8083 QA server, build `entry-440be66f`): **75 checks · 65 pass · 9 info · 1 fail.** Script `scripts/check-phase0.cjs`; artifacts `qa/phase0/` (55 screenshots, results.json, rapid-switch-samples.json). No page or console errors at any viewport.
- **Pass:** login; regions at 4 viewports; tablet widgets slide-over + Escape; phone sheet snap buttons + touch drag + Watch tab; no horizontal overflow ×4.
- **Pass:** all 6 dock tabs + URL map; 4 deep links open full screen; dock resize, dock collapse and sidebar collapse persist across reload; back/forward consistent in 8 states.
- **Pass:** symbol propagation from **all 6 sources** (Discover row, ↓, Next/Prev, search, Watch widget, deep link). Chart header, legend, Evidence widget, title and `?s=` all update.
- **Pass:** legend found rows, evidence badges, hover readout; **rapid switching**: 120 samples, 0 with evidence for a symbol other than the header; gates 6.1–6.4 (Prepare disabled with the stale reason ×2, NOT CAPITAL, caveat and verdicts, Simulate origin round trip).
- **Pass:** responsiveness during the pencil reveal: click → panel 21–30ms in-page (61–97ms wall), 0 long tasks.
- **FAIL 2.10:** short 1366×543 chart visible height **0px**. The inner ScrollView between header (~95px) and footer (~90px) is 76px and filled by the legend; the plot starts at y 381. This is the same issue as F1, and the layout fix is running. Chart heights: desktop 320 · tablet 226 · phone 52.
- **Info:**
  - **F6:** 0 Discover rows are visible at the default dock height (desktop 300px dock, and short), because the Discover header controls fill the panel.
  - **Phone:** plot visible 52/210px.
  - **BottomSheet mouse-drag** doesn't snap on narrow desktop (touch works).
  - **AutoTrade** keeps polling `/api/trading` after leaving its tab (2 requests in 11s).
  - **Legend not-found rows** untested (they need pattern filters).
  - **Tab clicks use replace,** so Back skips intermediate tabs (acceptable).

**QA run 3** (build `entry-5f82e9cb`, after fix2 + the accessible-name hooks; updated script with stricter gates): **77 checks · 76 pass · 1 fail.**
- **Pass (all viewports):**
  - regions
  - symbol propagation from all 6 sources (heading "Chart header for …" resolved)
  - legend found rows, badges and hover readout (5.3)
  - rapid switching, no key violations
  - gates 6.1–6.4 (Prepare disabled with reason; NOT CAPITAL; caveat/verdicts; Simulate origin round-trip)
  - deep links, dock persistence, back/forward
  - tablet slide-over; phone sheet snaps + touch drag + Watch tab
  - chart height gates (2.10) and no duplicate controls (2.12)
  - no horizontal overflow, clean console
- **FAIL:** short 1366×543 check 2.11. Discover rows at default dock height = 0 full (2 partial); dock capped at 168px; row height 46px. → fix3 running: chart min 190px on short windows, one-line toolbar with an inline "partly luck ⓘ" chip, Discover default ~34% at normal heights.
- **Owner's pilot visual pass (1707×748):**
  - one-line heading header, 220px chart with pattern + scenarios
  - collapsed legend overlay with the hold-period one-liner
  - compact Discover toolbar with 4 rows visible
  - no page scroll
- **Note:** a real-mouse hover on the owner's (hidden) Chrome window didn't move the readout because the coordinate mapping is unreliable there. Hover is verified by QA 5.3 in Playwright.

**Backend test ROOT CAUSE (2026-09-14): real billing race, NOT a Phase 0 regression. Owner decision needed.**
- **Where:** `server/kanida_pilot/billing.py` `Billing.apply()` uses `time.time_ns()` as a monotonic event sequence (`request_stamp=value.get('_kanida_request_stamp') or time.time_ns()`; `if request_stamp<=sub['last_event']:return`).
- **What happens:** on Windows (Python 3.12) the clock advances in 0.3–15.6 ms steps that vary with system load. Two `apply()` calls in the same tick get identical stamps, and the later state is silently dropped as "stale".
- **Evidence (diagnosis worker):**
  - A captured run showed the "active" and "halted" updates 6.26 ms apart with the identical stamp 1789399049357614800, so "halted" was dropped.
  - Emulating a 15.6 ms tick makes the test fail even alone; forcing monotonic stamps makes it pass.
  - It failed alone 7/25, billing-only 2/15, full suite 2/6 plus our 3/3.
- **Scope:** test isolation is fine (fresh Settings and temp SQLite per test; no conftest; no access to 8082/8083). server/ is unchanged since 2026-09-13 21:37, so today's failures come from the machine's current timer granularity.
- **Real-world impact:** a Razorpay webhook and a status sync in the same tick could leave the newer subscription state ignored until the next fetch; a backwards clock step would also drop updates.
- **Proposed fix (not applied; billing/access is owner-gated):** a process-wide `next_stamp()` helper, `max(time.time_ns(), last+1)` under a lock, used in `call()` and the `apply()` fallback. Low risk; the existing ordering test still holds. The limit is uniqueness per process only (multi-worker deployments would need a DB sequence).
- **Alternative (not recommended):** pass explicit increasing stamps in the test. That makes the test reliable but hides the bug.

**Backend test update (2026-09-14, later): not a timing flake.**
- Quiet-machine reruns: **3/3 full-suite runs fail** the same test (64 passed / 1 failed, 11.8–15.0s each); the test **passes alone**. That means it's **order-dependent** (state leaked from an earlier test, or time/state conditions that only occur in the full run).
- The full suite passed 65/65 earlier today, before and after B3, with no backend file changes known.
- A diagnosis worker is running (backend only; test-isolation fixes allowed; billing/access code changes will be proposed, not applied).

**Backend test note (2026-09-14, earlier): suspected flaky test.**
- `server/tests/test_pilot.py::test_billing_requires_paid_period_and_matching_plan` failed once in a full run (64 passed / 1 failed) while a web export and a Playwright run were loading the machine. It **passes when run alone** (1 passed, 1.3s).
- No backend file changed in Phase 0 (server files unchanged since 2026-09-13 21:37). An earlier worker saw the same single failure and a pass on rerun.
- The test compares `current_end=now()+3600` via billing status, so it's likely timing/load-sensitive.
- **Action:** rerun the full suite 3× on a quiet machine after the QA run, and record the flake rate. If it recurs, stabilise the test (freeze time) as a separate backend task.

**0.10 progress** (screen QA on the owner's pilot, build `entry-440be66f`, read-only)
- **Passed:**
  - ToolRail Next/Prev (GESHIP↔APCL: title and legend follow).
  - Top-bar search → TCS (title "TCS · 1D — KANIDA", legend "TCS · 1D · NSE", URL `?s=TCS&tf=1D`, honest "No stored setup" state).
  - Deep-link reload `/?s=GESHIP&tf=1D&m=…` restores the setup.
- **Pending:** Discover row, ↓ key, Watch widget row, legend hover readout (QA1 Playwright on 8083).

**0.11 progress:**
- **Audit (QA2):** no path shows a previous key's numbers; all gates intact with file:line proof.
- **No-setup guard (TCS) passed live:** zero evidence numbers, no verdict, Prepare disabled.
- **Follow-ups Q1–Q6 queued for a gate-fix worker after the layout fix:**
  - Q1: silent setup substitution outside the filters for Watch/search.
  - Q2: SetupSummary caveats not mounted on the chart surface.
  - Q3: phone legend lacks the hold-period and sample labels.
  - Q4: cursor boundary values use hindsight (label it).
  - Q5: cost and next-open badges hard-coded.
  - Q6: widget keyed by match id instead of the full key.
- **Pending:** QA1's rapid-switch sampling.

**0.12 progress:**
- **Passed:** all 6 dock tabs select correctly and show the right content, with URLs `/`, `/simulate`, `/autotrade`, `/activity`, `/watch`. Panels stay mounted. Dock full screen 184→550px hides the chart; exit restores it.
- **Gates visible:** LIVE ORDERS DISABLED, NOT CAPITAL wording, Prepare trade unavailable + stale reason, "partly luck" caveat, verdict chips, sample badges, hold-period labels.
- **Layout issues found, fix worker running:**
  - **F1 (HIGH):** chart squeezed off-screen on short screens (legend/header/action bar stacked above a 210px chart in a scrolling centre).
  - **F2:** duplicate Watch and Prev/Next controls.
  - **F4:** nested scrolling.
  - **F5 (HIGH):** dock panel only 184px.
- Full findings: `scratchpad/phase0-screen-qa-findings.md`.
| 0.13 | UX 2.4 responsiveness check (chart animation + dock) with a visible browser | QA | ⏳ |

**0.1 details** (verified: typecheck 0 errors; 46 pure store checks pass)
- **New file:** `src/activeSymbol.tsx`.
- **Exports:**
  - `ActiveSymbolProvider`
  - `useActiveSymbol()` → `{active, history, setActive(next,{replace?}), back, forward, canBack, canForward, isActive, reset}`
  - `useActiveNavigation(list)` → next/prev without wrap
  - `evidenceKey({symbol,pattern,timeframe,side,rule,dataEnd})`: returns `''` if any part is missing, so no load happens
  - `useKeyedValue(key, loader)`: value is `undefined` on key change; late results are dropped
- **History:** capped at 50; identical writes are deduped.
- **Stale match protection:** a `matchId` from another symbol or timeframe is rejected; side, asOf and matchId never carry across symbols.
- **URL:** `?s=&tf=&m=` synced only on `/`, using replace semantics; native builds skip URL sync.
- **Title:** "SYMBOL · TF — KANIDA" on workspace routes.
- **Context:** `ProductProvider` wraps the store. `workspace.selected` derives from it and `setWorkspace` delegates to it (source 'discover'). Filters and the study `origin` are untouched; the store resets only on account switch or sign-out.
- **Wave 0B must:**
  - Call `setActive` with the real source from search, Watch, Simulate and similar surfaces.
  - Make Workspace follow `active.symbol`/timeframe when there's no `matchId`.
  - Set `side` when it's known.
  - Load evidence through `useKeyedValue(evidenceKey(...))`.
  - Use `setActive` for in-app links instead of `router.push('/?s=…')`.

**0.3 details** (verified: typecheck 0 errors; check-chart-window 8 and check-model 21+108 pass; L's scratch helper test 78 passed)
- **`src/PatternCanvas.tsx`**
  - The component is now `memo`, so existing callers are unchanged.
  - New optional props: `cursor`, `onCursor(i|null)`, `onGeometry(g)`, `hideHeader`.
  - Web hover (mouse/pen) is throttled to one update per animation frame. Tap, Prev/Next and hover all share one cursor.
- **`src/patternGeometry.ts`:** `lineValueAt(line,i)` and `boundaryValuesAt(found,i)` (returns null outside a line's span; "Handle low" from the handle bar onward).
- **`src/ChartLegend.tsx`:** `ChartLegend` plus the types `LegendPatternRow` and `LegendEvidence` (with `side`; a short trade gets a "hypothetical short study" pill). Helpers: `legendRowsFromChart`, `evidenceFromWorkspace`, `chartKeyMatches`, `barReadout`, `HOLD_PERIOD_HISTORY_LABEL`.
- **Key guard:** no rows when the chart symbol/timeframe differs. `keyMatches:false` → "Loading evidence…" and no numbers, unless every one of these matches:
  - the history is this match's;
  - the legend key;
  - the chart snapshot, including `last_candle === match.candle_end`;
  - the exit side;
  - the exit request has finished.
- **Evidence placement:** evidence renders only under the selected, found row. The badges "next-open entry" and "costs 0.40% incl." were confirmed against backtest.py.
- **Performance:**
  - The cursor lives in a `useSyncExternalStore` store read only by `CursorMarks` and `CursorReadout`.
  - The SVG memo no longer depends on the cursor, so candle and ink layers don't re-render on hover.
  - The pencil reveal and click-to-finish are unchanged.
- **Wave 0B wiring (Workspace/StockWorkspace):**
  - Keep the `onSnapshot` data plus `cursor` state; pass `onCursor` + `hideHeader`.
  - Rows: `legendRowsFromChart(snapshot, match, {enabledPatterns, catalogue: state.patterns, cursor, hidden})`.
  - Evidence: `evidenceFromWorkspace({match, history, exit, chartData, legendKey, age, stale})`.
  - `bars = chartKeyMatches(...) ? snapshot.bars : []`.
  - `onOpenEvidence` → Evidence/Replay tab.
- **Gap:** PatternCanvas draws only the selected pattern, so the 👁 per-row visibility toggle can't hide individual outlines yet. Deferred: needs a canvas prop (0B or Phase 1).

**QA note (0.8)**
- `scripts/check-agent.cjs`, `scripts/check-shapes.cjs` and `scripts/check_product.py`/`preview-check.cjs` target the **pre-pilot product UI** (old "Tell your agent what to find" Discover, unauthenticated `/api/*` on the old `market_scanner.product` gateway). They can't pass against the pilot even before Phase 0, and are superseded rather than "fixed".
- Wave 0C writes a new Playwright acceptance script against the built-in local UI-QA server (`scripts/qa_server.py`: port 8083, isolated `var/ui-qa/qa.sqlite3`, fictional fixture account; never deployed). It covers the Phase 0 checks: regions render, dock tabs/deep links, symbol propagation from each source, legend key guard, and the four viewport sizes.
- **QA server start** (from kanida-app, uses the same factory pattern as `server/kanida_pilot/__main__.py`):
  `$env:PYTHONPATH='server;scripts'; .\.pilot-venv\Scripts\python.exe -c "import uvicorn; uvicorn.run('qa_server:application', factory=True, host='127.0.0.1', port=8083)"`
  It serves `dist-pilot` (rebuild first) and uses the research scanner on 8765. The fixture account is created on first start.
- **QA server started 2026-09-14 (background):** `/health`, `/api/pilot/config` and `/welcome` return 200; clean uvicorn startup. It stays running for Wave 0C.
