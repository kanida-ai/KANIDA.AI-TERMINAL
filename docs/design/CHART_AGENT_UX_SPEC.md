# Chart Agent — UX Spec v1 (SOURCE OF TRUTH)

This is the single source of truth for the `/power/agents` web experience. The build implements
**exactly** this; any deviation or blocker is written back here before it's called done. No
reinterpreting or redesigning. Reference mockups: the user's mobile pair + the dark desktop
"expand-in-place" mockup + v5 canvas (all show the SAME dark+green identity).

## 0. Principles (non-negotiable)
- **ONE design system, everywhere.** The existing Kanida **dark + green agentic identity** — the
  same tokens/components the rest of `/power` uses. NO second palette (delete the navy/cyan v4). ONE
  logo (the compass), never a second "K" mark.
- **Robinhood-simple.** Clean, obvious, fast, beautiful, consistent. Fewer elements, done perfectly.
- **No decorative controls.** Every visible control works and is wired to real state. If it has no
  real function yet, it is NOT shown. (Kills the dead Filters/View/Experienced buttons.)
- **The agentic feel comes from BEHAVIOUR, not ornaments.** No waveforms, no fake "typing/thinking"
  theatre, no LIVE ornament. A clean feed of real one-line intelligence.
- **Every number/line/chart is real backend output.** Nothing hardcoded. With our sparse precedents
  most setups are honestly **WATCH** (n<20); "Qualified" is rare — never faked to fill a section.
- **The chart is the hero.** Large, pattern-accurate from the real detector geometry, expandable.

## 1. Layout — LEFT nav · TOP tabs · MAIN expand-in-place feed (NO separate right panel)
This replaces the 3-column-with-sticky-right. The evidence **expands in place** in the feed, which
removes the scroll-disconnection bug entirely.

- **LEFT (slim, collapsible):** compass logo + KANIDA.AI · CHART AGENT. Agent roster (Chart Pattern
  = Active green dot; the rest = honest "Soon"/"Queued", never faked Active). Pattern categories with
  counts (Breakouts / Triangles / Channels / Wedges + "View all 9 detectors"). A COLLAPSE control.
- **TOP bar:** "Post-Market Analysis Complete" status + the real as-of date/time. Tabs **ALL INSIGHTS
  N · QUALIFIED N · WATCH N · NO TRADE N** with REAL counts (from the scan) that filter the feed. A
  real search/filter only if wired; else omit.
- **MAIN feed:** the scan summary strip (Stocks Scanned / Patterns Found / Meaningful / Qualified),
  then a short AI intro (2-3 real one-liners derived from the scan counts), then the timestamped
  one-line insight rows + market-story lines woven in. Progressive "View next 20 of N".

## 2. The feed row (collapsed) and the expand-in-place interaction
Collapsed row: `timestamp · status-glyph · **TICKER** — {Pattern} {one-line insight} · {win% · Δ} ·
STATUS badge · chevron`. Status badge + glyph: QUALIFIED (green ⚡) · WATCH (amber ◉) · NO TRADE
(red △). One-line insight from the real `hook`.

**Interaction:** click a row → it **expands in place** (smooth) to the full evidence below the line,
with a green highlight border. Click again (or the chevron) → collapses. **One open at a time**
(opening a row collapses the previously open one). The feed keeps scrolling; open/close any row; you
never lose your place and nothing goes blank. On expand, fetch that setup's bundle (already
precomputed → instant) and render the sections in §3.

## 3. The expanded evidence (inline) — sections top-to-bottom
1. **Header:** TICKER — {Pattern} · STATUS · "{sub-headline, e.g. Breakout confirmed after 37-day
   contraction}" + star (watchlist) + share.
2. **CHART — THE HERO (large):** candlesticks from `/bars` (green up / red down, volume beneath),
   the **actual detected pattern drawn from `/setup.geometry`**: the trendline(s) through the real
   swing-high/low anchors, the circled touches, the breakout candle, right-side price axis with the
   current price highlighted, "Vol N× Avg". **Pattern label MUST match geometry** (falling wedge →
   falling wedge; horizontal → flat level). Clicking/expanding the chart makes it much larger.
3. **WHY I IDENTIFIED THIS:** real bullet reasons from the detector (touches, contraction, breakout %
   past level, volume ×) — from `/setup` geometry/quality context.
4. **HISTORICALLY:** Cases (`evidence.n`) · Win Rate T+5 (`evidence.horizons["5"].win`) · Avg Return
   T+5 (`horizons["5"].etv`). If n is tiny → show the honest "insufficient precedents (n=X)" state,
   NOT a fabricated win rate.
5. **WHAT USUALLY HAPPENS NEXT:** Winning path (green) + Losing path (red) from `/setup.paths`
   (winners/losers T0→T+10), with the share % and stage labels. Honest empty state if no precedents.
6. **MY DECISION:** the verdict (`decision.verdict` — QUALIFIED/WATCH/NO TRADE, the source of truth)
   in the status colour + the real one-line reason.
7. **TOMORROW I AM WATCHING:** Hold (green) / Warning (amber) / Invalidates (red) from
   `/setup.watch_plan` {confirmation, warning, invalidation}.
8. **ACTIONS:** "Add to Watchlist" + "AutoTrade this Setup" (disabled with a Launch-Pending tooltip
   until eligible; AutoTrade only offered on QUALIFIED). Educational-purposes footnote.

## 4. Data contract (real endpoints — map, don't invent)
- `GET /api/agents/chart/scan?date=` → summary counts (scanned, count, statistically_meaningful,
  qualified) + per-row {stock, pattern, stage, direction, level, distance_pct, volume_x, touches,
  tier, quality_score, evidence_summary, hook, sector}. Drives the feed + tabs + categories.
- `GET /api/agents/chart/setup?symbol=&pattern=&date=` → {geometry, quality, evidence, paths,
  decision, watch_plan} + embedded bars (precomputed → instant). Drives the expanded evidence.
- `GET /api/agents/chart/bars?...` → OHLC (fallback if bundle lacks bars).
- `GET /api/agents/chart-v1` → the 9 patterns for the categories/roster.
Default date = the freshest precomputed screen (findFreshestScan). Fields not in the backend
(marketCap, marketAlign) → honestly omit, never fabricate.

## 5. Design tokens (match the mockup; reuse the existing Kanida green tokens where they exist)
- **Background** near-black; **cards** a touch lighter; **borders** hairline. **Green** = Kanida
  accent (positive/qualified/active). **Amber** = watch. **Red** = no-trade/negative. Text white →
  grey → dim. Candles green/red.
- ONE font family (the existing Kanida sans; numbers tabular). ONE card style, ONE button style, ONE
  icon language, ONE chart style. Soft radii. Subtle, premium, dark.
- Use the existing frontend's tokens/components as the base so the agents page is visually identical
  in language to the rest of `/power` — do NOT introduce a parallel token set.

## 6. Validation checklist (the build must pass ALL before it is presented)
- One logo (compass) only; zero second "K". One font/colour/card/button system; no white-heavy or
  blue v4 remnants.
- No decorative/dead controls — every visible control is wired.
- Every feed line is clickable; click expands **in place**; one-open-at-a-time; collapse works;
  scrolling never blanks the layout; you never jump to the top.
- Each expanded chart draws the **correct pattern for that row's label** (spot-check ≥4 patterns:
  horizontal, a triangle, a wedge, a channel) from real geometry; chart renders; reasoning matches
  the selected setup.
- Tabs (ALL/QUALIFIED/WATCH/NO TRADE) show real counts and filter correctly.
- No hardcoded values presented as real; honest WATCH/insufficient-n states where the data is thin.
- Desktop layout correct; interactions understandable without explanation.

## 7. Delete
Remove the v4 navy/cyan build: `components/power/agents/v4/*` and revert `app/layout.tsx`'s v4 fonts
if unused elsewhere. This spec fully replaces it.
