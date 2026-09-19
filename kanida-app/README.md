# KANIDA product preview

A universal Expo SDK 57 / React Native / TypeScript application for iPhone, Android, tablet and web. It reuses the existing scanner and frozen stock-level research; the source OHLC database is never written by this product.

## Open it

- Desktop: http://127.0.0.1:8082/
- iPhone connection QR: http://127.0.0.1:8082/connect
- Expo Go on the current Wi-Fi: `exp://10.0.0.192:8081`
- Safari on the current Wi-Fi: http://10.0.0.192:8082/

Install/update Expo Go on the iPhone, connect the phone and computer to the same Wi-Fi, and scan the QR with the iPhone camera. Keep the computer awake and both preview services running. A physical iPhone has not been tested from this Windows workstation.

From the repository root, start or restart with:

```powershell
.\kanida-app\start-preview.ps1
```

The script refreshes the LAN address, exports the web application, starts the read-only research scanner if needed, starts the product gateway, and starts Expo Go's Metro server. `-SkipBuild` reuses the last web export; use the full command after changing Wi-Fi or frontend code. `stop-preview.ps1` stops only the two product preview processes. It leaves the original scanner on port 8765 running.

If the QR does not connect, first verify that Safari on the phone can open the LAN address printed by the script. Network isolation or a local firewall can prevent devices on the same Wi-Fi from reaching each other. No public tunnel, cloud deployment, broker login, or live account is configured.

## Product flow

1. **Trader Agent:** a compact decision feed, with For review / Watch / Pass screening. Desktop uses a feed beside the selected chart; mobile opens a focused stock sheet. Guided commands, independent timeframes, sector/index filters, past-return bands and adjustable minimum historical trades (default 10) remain available.
2. **The chart explains itself:** opening a stock starts a 3.3-second pencil-style reveal of its detected boundaries and internal price swings, then its levels and conditional long/short paths. Converging outlines can extend with dotted lines to a geometric apex; these extensions are not predicted prices. Replay the drawing, hide scenarios, or tap a candle for its full date and closing price. The drawing respects the device’s reduced-motion preference. Dates and price labels retain readable sizes as the chart resizes.
3. **Stock evidence:** exact stock × pattern × timeframe × direction history: net average, win rate, average winner/loser, best/worst trade, favourable/adverse excursions, tested holding time and whole-share capital replay.
4. **My watch:** saved setups persist locally and are compared with refreshed scans while KANIDA is open. Bullish and bearish baskets retain each stock’s own statistics. No background notifications or portfolio-level return/drawdown are implied.
5. **AutoTrade:** choose capital and account risk, inspect sizing, then review entry, stop, target, holding limit and costs before saving a paper draft. Detailed settings are optional. Save/reload/pause/resume/archive persist, with an activity trail. Plans reserve no cash and never enable execution.

The agent’s decisions and search commands use transparent rules, not a general language-model agent. A directional pattern with a positive past average and sufficient history enters For review. Unresolved direction or insufficient evidence enters Watch. Non-positive averages enter Pass. These are research screens, not live trade approvals.

Discover opens on safe defaults: sorted by average net return per trade (expectancy, not win rate), positive averages only, at least 10 historical trades (`MIN_TRADES_DEFAULT` in `src/decision.ts`) and your onboarding timeframes. Each row carries a For review / Watch / Pass verdict, a sample-size label and the stored data's age; data older than 3 days is marked stale. Its headline stats are labelled as pattern hold-period history, not the exit rule. **Prepare trade** stays disabled unless prices are fresh and the server reports tested evidence for the exact exit rule that would be traded; otherwise a plan can only be saved as illustrative, which cannot be simulated.

## What is real, and what is planned

The historical results, candles, detector geometry, cash replays, filters, watches and saved plans use real local data. The source currently ends in July 2026, so the current qualified setup count is zero. Historical cards are labelled accordingly. Pattern-fit scores are not win probabilities.

The chart and planner share one server-generated **exit plan** per stock, pattern, timeframe and direction. The frozen selected rule is used only when its later test passes the configured sample requirement, a search-adjusted lower return bound, and positive results in both chronological test halves. Its stop, target multiple, entry trigger and holding limit are preserved together, along with its own later-test statistics. The implementation does not choose another winner using test data.

An unsupported study uses a clearly labelled **1:2 paper benchmark** with a structural stop and volatility buffer, rather than a universal percentage stop. For cups this is below the handle; for boundary patterns beyond the opposing boundary; for shoulders beyond the right shoulder; otherwise a recent observed swing is used. A minimum of one recent average candle range avoids a stop inside ordinary noise. These structural benchmarks are untested and never inherit baseline win rates. Existing frozen research tests volatility-based stops; it did not test these new structural benchmark rules. A timed-exit selection has no tested stop/target ratio and is not silently converted into one.

The policy exposes Default benchmark, Limited evidence, History supported and Exit test failed. A tested stop that is inside the current structure cannot lend its evidence to a newly widened stop. Drawn paths remain conditional illustrations, not model forecasts or future timestamps. The minimum-trades browsing filter (default 10) is independent of research support.

Saving the suggested plan preserves its rule identity and evidence snapshot, checked again by the server. Changing a stop, target or holding period produces **Custom · untested** and removes the evidence association; changing capital or account risk preserves it. Restoring suggested exits restores the original rule. A new candle or research run invalidates an old suggestion before saving.

AutoTrade currently creates reviewable paper **plans**. Neither a forward paper-fill engine nor a live broker is connected. There are no fabricated fills, open positions, portfolio returns or live P&L. A stop/target edited in a plan is not covered by the baseline's time-exit statistics; the UI explicitly calls for a separate historical test of the modified rule.

The current research assumes 0.40% round-trip fees and slippage. The sizing preview reserves those costs and uses the smaller of the capital allocation and account-risk budgets. Gaps can exceed the planned stop. Prices, short borrow, corporate actions, active-universe selection and other original research limitations remain applicable.

Before live execution, implement shared-account reservations and portfolio replay; fresh-price and market-calendar validation; broker/instrument eligibility; idempotent order submission, reconciliation and partial fills; enforceable risk/exposure limits and a kill switch; authenticated accounts and an explicit live activation flow. These are future execution features, not implied by the current paper-planning interface.

## Code and data

- `app/`: Expo Router routes and responsive navigation shell.
- `src/TraderShell.tsx`: three-destination desktop/mobile navigation and agent mark.
- `src/TraderDesk.tsx`: decision feed and saved watches.
- `src/AgentCase.tsx`, `src/decision.ts`: evidence briefing and transparent screening rules.
- `src/PatternCanvas.tsx`, `src/patternGeometry.ts`: responsive candles, staged boundaries and swing tracing, geometric projections, conditional zones and candle inspection.
- `src/ExitPlan.tsx`, `../market_scanner/exit_plan.py`: shared evidence-linked exit policy and customer explanations.
- `src/PlanReview.tsx`: progressive paper-plan preparation and review.
- `src/Screens.tsx`: phone pairing route; former dashboard exports are no longer routed.
- `src/Sheets.tsx`: filters, stock overview, evidence and plan review. Evidence and planning share one native modal to avoid overlapping iOS presentations.
- `src/constants.ts`: shared values (capital choices, timeframes, study defaults, cost assumption, minimum trades, data-staleness limit).
- `_archive/`: unused screens and boilerplate moved out of the build (including the former `src/Chart.tsx`); see `_archive/README.md`.
- `src/model.ts`: exact filter logic, types and sizing.
- `../market_scanner/product.py`: LAN web host, explicit read-only research API proxy, isolated SQLite plan/watch/event persistence. Same-origin JSON browser writes; no scan or broker write endpoint.
- `../market_scanner/output/product.sqlite3`: local draft plans, saved watches and their event trail.
- `../market_scanner/output/backtests.sqlite3`: existing frozen research, preserved.

This is a local, single-workspace preview intended for a trusted LAN. It has no multi-user authentication. Source and research data are proxied to the LAN preview; no cloud data upload is performed.

## Verification

```powershell
# From kanida-app
npm run typecheck
npx expo-doctor
npm run export:web
node scripts/check-model.cjs
node scripts/check-agent.cjs
node scripts/check-shapes.cjs

# From repository root
.\market_scanner\.venv\Scripts\python.exe -m unittest discover -s market_scanner/tests
.\market_scanner\.venv\Scripts\python.exe kanida-app/scripts/check_product.py
```

`check_product.py` creates a temporary plan database and a loopback-only HTTP server. Headless Edge tests desktop (1440×1100), iPhone-size (390×844) and tablet (768×1024) layouts, actual SVG drawing progress, candle inspection, scenario visibility, watch save/reload/removal, evidence, historical cash replay, filters, stock overview, plan save/reload/pause/resume/archive, short-side selection and the QR screen. It does not create test plans in the real product database. Screenshots and the browser check record are in `qa/`.

The backend suite passes 66 tests, including selected-rule identity, failed/limited evidence, structural-risk conflicts, custom edits and forged/stale suggestion rejection. The agent checks cover decision thresholds, selected-side evidence, repeated-stock drawing, reduced motion and 4H/1W charts; the full browser flow also exercises 1H and 1D. Expo Doctor passes all 21 checks. The iOS Expo Go manifest and its 6.2 MB development bundle have been retrieved successfully from the LAN server. That verifies delivery/build, not physical-device native rendering. Web screenshots use Chromium/Edge, not mobile Safari. The web JS bundle is approximately 1.3 MB plus four text fonts and the Feather icon font.

The current Expo dependency tree reports moderate upstream advisories involving `decode-uri-component` and build-time `uuid`/`xcode`. No high/critical advisory was reported. npm's suggested automatic fix would downgrade major Expo packages; that incompatible downgrade was not applied. Reassess dependencies before production distribution.
