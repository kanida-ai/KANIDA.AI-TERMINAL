# Falcon Home + Discover Strategies — product spec (v1)

Status: **approved direction from the owner (2026-09-14)**. Build in progress. This spec supersedes the Phase 0 "one workspace" layout as the main screen; the Phase 0 workspace is kept as the full-chart view.

**Owner priority (2026-09-14, later):** complete ONE block (Chart Strategies) end to end with the block structure ready for more blocks. **Falcon (§2) is deferred** — `/` shows a placeholder linking to Discover Strategies. Backtest content and strategies are iterated after the block is proven.

Scope of v1: **prove Chart Strategies and the UX.** Quant, Results, Options and other blocks come later through the admin registry without code changes to the page.

Benchmark: TrendSpider Dashboard (live screenshots from the owner, live notes 17, docs inventory M16). KANIDA keeps its own identity and honesty rules.

---

## 1. Navigation

Top bar, left to right:

| Item | Route | What it is |
|---|---|---|
| KANIDA logo | `/` | Goes to Falcon |
| **Falcon** | `/` | The AI home: one button, "Scan the market in 30 sec" |
| **Discover Strategies** | `/discover` | Blocks of strategy cards (§3) |
| **Watchlist** | `/watch` | Existing watch list |
| **AutoTrade** | `/autotrade` | Existing plans/AutoTrade, with Simulate and Activity inside it |
| Stock search | top bar | Opens the stock on the full chart |
| Data-age pill, account | right | Existing |

- The Phase 0 workspace (chart + legend + dock) moves to **`/chart?s=&tf=&m=`** and is opened by "Open full chart →" from any chart card. Old links `/?s=…` redirect to `/chart?s=…`. `/simulate` and `/activity` keep working.
- Admin (owner role only): **`/admin/strategies`** (§6). Not in the top bar; reached from the account menu.
- Phone (<760 px): the four items become a bottom tab bar; search stays in the top bar.

---

## 2. Falcon — the AI home (`/`)

"Jarvis for the share market", named **Falcon**. No Marvel names or look-alike marks.

### 2.1 Idle state
- Centre of the screen: Falcon mark, one line: "Falcon reads every strategy across NIFTY 500 and tells you what matters today."
- One primary button: **Scan the market in 30 sec**.
- Under it, small: "Last scan 09:12 IST · data to 31 Jul 2026 · View results" (only if a scan exists).
- If prices are stale (older than `PILOT_MAX_DATA_AGE_DAYS`): an amber line above the button: "Prices are 45 days old — insights will be research only, not current." The button still works.

### 2.2 Scanning state (the animation)
- A full-width neural-network animation: layered nodes, pulses travelling along edges, nodes lighting as each stage runs (mint on dark, matching the theme). SVG/Canvas, 60 fps target, no external assets.
- Beside/below it, a **stage list**. Each stage shows: status (waiting · running · done), title, and a **reference line with real numbers from the real pipeline**.

| # | Stage title | Reference line (real values only) |
|---|---|---|
| 1 | Boot the data | "NIFTY 500 · 501 stocks · prices to 31 Jul 2026 (45 days old)" |
| 2 | Pull market sentiment | Breadth from our own prices: "% above 50-day average 58% · advancers 312 / decliners 189 · NIFTY trend up". News/FII sentiment is not connected yet and is not claimed. |
| 3 | Isolate noise | "1,240 stored setups → 312 kept · removed: 610 small sample, 244 negative after costs, 74 not directional" |
| 4 | Quant engineering & strategy scanning | "19 chart strategies × 3 timeframes · 312 setups" with strategy names flickering past |
| 5 | Run walk-forward simulation | "Loaded later-test (walk-forward) results for 312 setups · last run <date>" — **stored results, not re-run**, and it says so |
| 6 | Calculate win probability & edge after costs | "Ranked by 95% low after 0.40% costs · win rate shown as context only" |
| 7 | Compose insights | "Top 5 for traders · Top 5 for investors" |

- Honesty rules for the animation:
  - Every number comes from the server job. If a stage has no data it shows "not available" — it is never invented.
  - Stage pacing: each stage shows for at least ~2.5 s so it can be read; total target ≤30 s. Pacing is presentation only; numbers are real.
  - "Skip animation" appears after the first completed scan. "Cancel" is always available.
  - `prefers-reduced-motion`: no pulses, just the stage list with a progress bar.
- Overall progress bar + elapsed seconds.

### 2.3 Results state
- Top: market context strip (from stage 2) + "Scanned 09:12 IST · data to 31 Jul 2026" + **Rescan**.
- Two columns (stacked on phone):
  - **For traders** — timeframes 1H/4H/1D, holds up to ~10 trading days.
  - **For investors** — 1W timeframe or holds of 20+ trading days.
- Each column shows up to **5 insight cards ranked by 95% low** (owner decision; iterate later). Never pad with invented items.
- **Insight kinds** (the data today: only 3 of 567 stored setups have a positive 95% low; only 1 backtest cell passes the later-test gate — so most useful insights are observations, not "buy this"):
  1. `setup` — a stored setup with 95% low > 0 and n ≥ 10, ranked by 95% low. Labelled "pattern hold-period history, not a tested exit rule" unless its cell is `tested`.
  2. `caution` — a real negative finding, e.g. "Channel 1D: 70 found on NIFTY 500, none with a positive 95% low after costs."
  3. `cluster` — a real concentration, e.g. "15 NIFTY 500 stocks formed a Falling Wedge on 1D; 6 are in Capital Goods."
  4. `context` — breadth/trend from stage 2.
  Order: setups first (by 95% low), then cautions/clusters by the size of the group. If there is nothing true to say, the column says so.
- Trader column = 1H/4H/1D strategies; investor column = 1W strategies.
- **Insight card:**
  - Rank · headline, e.g. "GESHIP — Falling Wedge, 1D, bullish"
  - Why: "later-test avg +4.83% after costs · 95% low +0.74% · n 10 Moderate sample · win rate 70% (context)"
  - Chips: side · timeframe · verdict (For review / Watch) · data age
  - **Open in Discover Strategies →** deep link (§3.7): block, strategy preselected in scanner card A, stock selected, chart + backtest showing.
- Stale data: every card carries "Research only — prices 45 days old"; trading actions stay blocked by the existing gate.

---

## 3. Discover Strategies (`/discover`)

### 3.1 Page layout
- Page header: title "Discover Strategies" · universe chip **NIFTY 500 ▾** (default) · data-age pill · **Columns ▾** (density) · Refresh all ↻.
- Body: a vertical stack of **blocks** from the strategy registry (§6). v1 ships the **Chart Strategies** block enabled; other blocks exist in the registry as disabled.
- Each block:
  - Header: block title, one-line description, a colour dot for its link group, ⋮ (Reset block, Collapse).
  - A row of four cards:
    ```
    [ Scanner card A ▾ ] [ Scanner card B ▾ ] [ Chart card ] [ Backtest card ]
    ```
  - Grid: ≥1280 px: `1fr 1fr 1.35fr 1.25fr`, 12 px gaps, card height ~420 px. 760–1279 px: 2×2. <760 px: scanners stacked; tapping a stock opens chart + backtest in a bottom sheet.

### 3.2 Linking rules
- **Each block is its own link group.** A click in Chart Strategies never changes another block.
- Click a stock row in scanner A or B → that block's Chart card and Backtest card switch to **that stock + that card's strategy + timeframe**. The last click wins.
- The link key is `strategy + symbol + timeframe + side + data end` (reuse `matchKey`/`evidenceKey`). A card never shows data for a different key; stale responses are dropped.
- The Chart card header names the source: "from: Falling Wedge 1D (A)".
- The click also writes the global active symbol (source `discover`) so Watchlist and the full chart follow.

### 3.3 Scanner card
Anatomy (from TrendSpider, adapted):
- **Title = strategy name ▾** → opens the strategy picker (§3.4). ⋮ menu: Refresh · Sort by (95% low / avg / n / symbol) · Open full list · Clear card. ✕ clears the card to its empty state ("Choose a strategy ▾").
- **Status line** under the title: ⓘ (what this strategy is: rule, timeframe, universe, exit, costs) · ↻ re-run · text.
  - Loading: thin progress bar under the title + "Scanning NIFTY 500… 8 found" counting up + ✋ Stop.
  - Done: "Scanned NIFTY 500 · data to 31 Jul 2026 · 17 found". If results are stored (pre-computed), the text says "Stored scan · data to 31 Jul 2026 · 17 found" — no fake scanning.
  - Error: "⚠ Strategy results are unavailable right now. Retry" (never an empty list pretending to be zero).
  - Empty: "No NIFTY 500 stock matches this strategy on data to 31 Jul 2026."
- **Columns:** Stock (symbol + short company, 2 lines) · 95% low · Avg · n. Sorted by 95% low desc. Small-sample rows carry the sample label.
- **Rows appear one by one** (staggered reveal ~25 ms per row, count ticking up) as results arrive.
- Row states: hover tint · keyboard focus ring · **selected** = mint border + tint (TrendSpider uses amber). ↑/↓ moves selection and updates the chart; Enter = open full chart.
- Default strategies for A and B come from the registry (e.g. A = Falling Wedge breakout 1D, B = best other bullish 1D). User changes persist per user.

### 3.4 Strategy picker (the dropdown)
- Opens **inside the workspace**, anchored under the card title (Popover), ~480 px wide, max height ~70% of the window; the rest of the page stays visible.
- Header "CHOOSE A STRATEGY" · search box focused immediately.
- Filter chips: **All · Chart patterns · Bullish · Bearish · 1H · 1D · 1W · Recently used** (chips come from registry tags, so admin additions appear automatically).
- "Recently used" section first.
- Each row: strategy name (+ tags) on the left; on the right a green tag: "chart strategy · NIFTY 500 · 17 found · best 95% low +0.74%". Strategies with 0 found today are dimmed, still selectable.
- Keyboard: ↓ from search into the list, ↑/↓, Enter selects, Esc closes. Outside click closes.
- Choosing a strategy closes the picker and runs the card (§3.3 loading state).

### 3.5 Chart card
- Header: "SYMBOL · Pattern · TF" · "from: <card> (A/B)" chip · **Open full chart →** (`/chart?s=&tf=&m=`).
- Body: the existing animated `PatternCanvas` (fill, hideHeader) drawing the pattern for that strategy's match; last price tag; date axis adapts to width; data date + "Your time zone: IST".
- Empty: "Click a stock in a scanner card to see its chart." Loading: skeleton, then the draw animation.

### 3.6 Backtest card (clicked stock only)
- Header: "Backtest · SYMBOL · Pattern TF".
- **Same strategy that is traded:** the replay under that strategy's exact entry/stop/target/max-hold rule, next-open entry, costs + slippage 0.40% round trip.
- Top: verdict chip (For review / Watch / Pass) · sample label.
- Metrics: trades n · later-test avg after costs · 95% low–high · win rate (context) · worst trade · avg hold days.
- Mini equity curve of the later-test trades; last 5 trades list (date, entry, exit, return) with "All trades".
- Honesty lines always visible: "Costs 0.40% included · next-open entry · later-test (out-of-sample) only". If the exact rule is not tested: "Not tested for this exit rule — showing pattern hold-period history" labelled as such (existing wording).
- Header link: **Evidence →** (full chart, Evidence tab; accessible name "Open evidence and replay for SYMBOL TF"). Moved from the footer to save a line (fix round 1).
- Body scrolls inside the card; header and honesty footer stay fixed.
- Empty/loading/error states as for the chart card.

### 3.7 Deep links
`/discover?b=<blockKey>&a=<strategyKey>&bb=<strategyKey>&sel=<A|B>&s=<SYMBOL>` — Falcon insights use this. Loading the link: block scrolled into view, strategies set, stock selected, chart + backtest shown.

---

## 4. Micro-interactions checklist (must all exist in v1)
1. Picker opens in place, search focused, chips filter instantly, recent first.
2. Rows reveal one by one with a ticking count; progress bar; Stop.
3. Status line with universe · data date · count; ⓘ explains; ↻ re-runs.
4. Selected row highlight; hover tint; keyboard ↑/↓/Enter/Esc.
5. Chart + backtest switch together; header names the source card.
6. Card ⋮ menus; ✕ clears to an empty state with a call to action.
7. Every card has loading, empty, error and stale states with real text.
8. Density selector reflows the grid; responsive 4 → 2×2 → stacked + bottom sheet.
9. Falcon: animated stages with real reference numbers, skip, cancel, reduced motion, last-scan resume.
10. Deep links from Falcon insight → exact Discover state.

---

## 5. Data and API contract (v1)

Details of existing endpoints: see §8 (filled from the code map). New endpoints on the pilot server:

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/strategies/catalog` | Enabled blocks → strategies (key, name, tags, timeframe, side, audience, found count, best 95% low, data end) |
| GET | `/api/strategies/{key}/results?universe=nifty500` | Rows: symbol, company, match id, timeframe, side, 95% low, avg, n, sample label, verdict, data end. Sorted by 95% low. |
| POST | `/api/falcon/scan` | Starts a scan job → `{id}`. Cached per data-end date and registry version. |
| GET | `/api/falcon/scan/{id}` | Stage events with real counts + final insights `{traders:[…≤5], investors:[…≤5], context, data_end, scanned_at}` |
| GET/POST/PATCH | `/api/admin/blocks`, `/api/admin/strategies` | Owner-only registry management (§6) |
| POST | `/api/admin/strategies/{key}/preview` | Runs the strategy query without publishing: count + top 5 |

Rules: point-in-time (stored matches only, data end stated); evidence gate reused (`tradable_evidence`); ranking = 95% low after costs, tie-break n; audience = trader/investor per §2.3; stale data flagged on every response.

---

## 6. Admin strategy registry (add strategies after launch without code)

Tables (pilot DB):
- **blocks**: id, key, title, description, kind (`chart` · `quant` · `results` · `options` · …), order, enabled, created, updated.
- **strategies**: id, key, block_id, name, description, tags[], timeframe(s), side, audience (`trader` · `investor` · `both`), source_type (`stored_pattern` in v1; later `quant_rule`, `scanner_endpoint`), source_config JSON (e.g. `{pattern:"falling_wedge", timeframe:"1D", side:"long"}`), min_trades, default_slot (`A` · `B` · null), order, enabled, created_by, updated.

Admin page `/admin/strategies` (owner only):
- List by block; drag to reorder; enable/disable toggles.
- **Add strategy** form: block · name · description · source type · pattern (select from patterns that exist in the data) · timeframe · side · audience · tags · minimum trades · default slot.
- **Preview** before enabling: found count, top 5 by 95% low, evidence coverage. A strategy with no tested evidence can be saved but shows "not tested" everywhere — it is never presented as evidence.
- Every change is recorded in the activity log.
- v1 seed: one strategy per stored pattern × timeframe × side in the Chart Strategies block; disabled placeholder blocks for Quant Strategies, Results, Options.

---

## 7. Build plan (parallel workers, disjoint files)

| Worker | Owns | Delivers |
|---|---|---|
| W1 backend | `server/kanida_pilot/strategies.py`, `falcon.py`, new tables + seed, routes, tests | §5 + §6 endpoints, scan job with real stage counts |
| W2 Discover | `app/discover.tsx`, `src/discover/*` | §3 page, cards, picker, linking, deep links |
| W3 Falcon | `app/index.tsx` (Falcon), `src/falcon/*` | §2 idle/scanning/results, animation |
| W4 Shell + admin | `src/PilotShell.tsx`, `src/shell/*`, `app/chart.tsx`, `app/admin/*`, `src/admin/*` | §1 nav, route move + redirects, §6 admin UI |

Then: typecheck, backend tests, web export, Playwright checks on the QA server (8083) for every item in §4, and a screen-by-screen review against TrendSpider.

---

## 8. Existing data sources (from the code map, 2026-09-14)

- **Contract for new endpoints:** `src/strategies/types.ts` (shared by backend and frontend).
- **Stored scan** (market_scanner `output/scanner.sqlite3`, finished 12 Sep, candles to 31 Jul 2026 on 1H/4H/1D/1W): 567 matches on 441 stocks; NIFTY 500: 239 matches on 175 stocks. Patterns: channel, falling_wedge, rising_wedge, symmetrical_triangle, descending_triangle, flag_pole, horizontal_breakout, inverse_head_shoulders, cup_handle, head_shoulders.
- **Evidence per match:** `/api/matches?min_trades=0&universe=nifty500` rows carry `history[]` = `{run, side, status, reference{n, win_rate, expectancy_pct, expectancy_ci95,…}, test{…}}`. **"95% low" = `reference.expectancy_ci95[0]`** — pattern hold-period history after costs, *not* the tested exit rule (server label: "Pattern hold-period history, not this exit rule").
- **Backtest cells** (`/api/backtests/cell?symbol&timeframe&pattern&side`): `status` tested / small_test_sample / no_validated_rule / no_occurrences; `rule`, `splits{train,validation,test}`, `reference`, `trades[]` (with `split`), `reference_trades[]`. No stored equity curve. Active run: 74,412 cells — **tested 1, small_test_sample 33**; the rest have no validated rule.
- **What this means for v1 UI:** the Backtest card normally shows the **hold-period history** (reference stats + reference trades), clearly labelled; it shows the tested rule's later-test trades only when `status=='tested'`. Only 3 stored setups have a positive 95% low today, so Falcon insights lean on cautions and clusters (§2.3).
- **Chart:** `/api/chart?symbol&timeframe` (bars ≤260 + matches with `lines`) — `PatternCanvas` fetches it itself.
- **Freshness:** `/api/state` `source_latest`, `source_stale`, `schedule`; `/api/backtests/state` run id and dates. `DATA_STALE` after 3 days.
- **Gates:** `tradable_evidence` (exit-plan response), `DATA_STALE`, `prepareBlock` — unchanged.
- **Admin:** roles `owner`/`member` only; `owner()` guard in `app.py`; SQLAlchemy Core tables in `db.py` created by `create_all` (new tables need no migration).

## 8A. Build status (2026-09-14)

| Item | Status | Verified by |
|---|---|---|
| §1 Nav (Falcon · Discover Strategies · Watchlist · AutoTrade), workspace moved to `/chart`, legacy `/?s=` redirect | ✅ built | e2e checks 1.x, 2.x; Phase 0 suite 77/77 on `/chart` |
| §2 Falcon | ⏸ deferred — `/` placeholder with "Open Discover Strategies →" (full UI code kept unused in `src/falcon/`) | e2e 1.x |
| §3 Discover Strategies — Chart Strategies block (cards, picker, linking, backtest, deep links, phone sheet) | ✅ built | e2e `scripts/check-discover.e2e.cjs` (100 checks, 4 viewports) |
| §5 catalog + results endpoints | ✅ built | `server/tests/test_strategies.py` |
| §6 Admin registry + page (add/edit/enable/reorder/preview) | ✅ built | pytest + e2e 13.x |
| Billing monotonic stamp fix | ✅ applied | pytest 74/74 |

## 9. Open items
- Prices end 31 Jul 2026. Until OHLC is refreshed, Falcon and Discover show research-only results with a stale label.
- Investor column may be empty if no 1W / long-hold strategies pass the gate; shown honestly.
- Ranking by 95% low is v1; iterate later.
