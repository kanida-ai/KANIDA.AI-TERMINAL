# Handoff: Falcon — "Ask Falcon" Home (Version F2)

## Overview
The **Ask Falcon home** is the landing screen of Falcon, an AI-native Indian-equity
trading product. It is a three-column **master–detail** layout: a labeled left nav,
a middle panel that lets the user pick a **trader persona** and shows that persona's
**Falcon Top 10** signals (with a guided prompt at the bottom), and a right panel that
shows a plain-English explanation of whichever signal is selected
(**Why we're picking this · Historical track record · What the sector is doing**).

Tone is confident, intelligent, explainability-led — **never hype**
(no "smartest AI", no "win rate" boasts beyond the factual track record).

## About the Design Files
The files in this bundle are **design references created in HTML/React-via-Babel** —
a prototype showing the intended look, layout, and behavior. They are **not production
code to copy verbatim**. The task is to **recreate this design in the target codebase's
environment** (React, Vue, Svelte, SwiftUI, etc.) using its established components,
state patterns, and data layer. If no front-end environment exists yet, pick the most
appropriate framework and implement the design there.

The prototype uses in-browser Babel and a global `window` handoff between files purely
for zero-build previewing — **do not replicate that pattern** in production. Use real
modules/components.

## Fidelity
**High-fidelity (hifi).** Colors, typography, spacing, radii, and interactions are final.
Recreate the UI pixel-accurately using the codebase's libraries, then wire it to real
signal data. All exact values are in **Design Tokens** below.

## Files
- `Falcon F2 — Ask Falcon Home (standalone).html` — open this in a browser to see the
  design running by itself (fills the viewport). It supplies the brand CSS tokens, the
  `Tier` badge component, and mounts `ScreenF2`.
- `falcon-f2.jsx` — the actual screen: data model, persona logic, all sub-components
  (`PersonaSelect`, `F2Detail`, `ScreenF2`), and an injected `<style id="f2-css">` block
  with every layout rule. **This is the primary reference.**

> Note: in the prototype, the `Tier` component and brand tokens live in the standalone
> HTML; in `falcon-f2.jsx` they're read via `window.Tier`. In production, make `Tier` a
> normal shared component and the tokens normal CSS variables / theme values.

---

## Layout (three columns, left → right)

Overall: a full-height flex row. Background `--canvas` (#0a100e).

### 1. Left nav rail — fixed **210px**, `--panel` bg, 1px right border `--line`
- **Brand** (top): 30×30 rounded-9px mint-tinted tile with a `✴` glyph + "Falcon" wordmark (16px/600).
- **"WORKSPACE"** section label (10px, uppercase, 0.12em tracking, `--faint`).
- **Nav items** (icon + label, 13.5px): Ask Falcon *(active)*, Signals, Co-Trading,
  AutoTrade *(with a "SOON" pill)*, Performance, Plans.
  - Default: `--muted` text, transparent bg. Hover: bg `rgba(255,255,255,0.035)`, text `--ink`.
  - **Active**: bg `--mint-dim`, text `--mint`, weight 500.
  - Icons 17×17, 1.6 stroke, `currentColor`.
- **Account footer** (bottom, pushed with margin-top:auto, 1px top border): 30×30 mint
  avatar "S" + "Shyam Verma" (12.5px/500) / "Pro · NSE" (11px `--faint`).

### 2. Middle panel — fixed **336px**, `--panel` bg, 1px right border
Vertical stack: persona selector → list header → scrollable Top 10 → guided prompt.

- **Persona selector** (`.f2-pwrap`, 1px bottom border):
  - "TRADER PERSONA" label (10px uppercase `--faint`).
  - A clickable field (`.f2-pselect`): 28×28 mint-dim icon tile + name (13px/600) +
    "Top 10 · {holding period}" (11px `--faint`) + chevron on the right. 1px `--line-2`
    border, radius 11px, bg `rgba(255,255,255,0.04)`. Hover border → `rgba(63,227,164,0.35)`.
  - **Dropdown** (`.f2-pmenu`, absolute, bg `#0e1713`): the 5 personas, each a row with
    icon tile + name + holding period. Selected/hover row bg `rgba(63,227,164,0.08)`.
- **List header** (`.f2-lh`): "Falcon Top 10" (12px/600) on the left; on the right a
  mint dot (6px, glowing) + "today" (11px `--muted`).
- **Top 10 list** (`.f2-scroll`, flex:1, scrolls, scrollbar hidden): each row (`.f2-item`)
  is a 3-col grid `[18px rank][1fr name][auto tier+%]`, 9×11px padding, radius 10px.
  - Rank: 11px mono `--faint` (mint when selected).
  - Name: symbol 13.5px/550 + company name 11px `--faint` underneath.
  - Right: `Tier` badge + day % (12.5px/600 mono, `--mint`).
  - Hover bg `rgba(255,255,255,0.035)`; **selected** bg `--mint-dim`.
  - Clicking a row selects it and updates the right panel.
- **Guided prompt** (`.f2-cmd`, 1px top border) — the "search bar", a **single horizontal line**:
  a container (`.f2-gp`, radius 12px, 1px `--line-2`, 6px padding) holding three items in a row:
  1. **Intent** field (`flex: 1.3`) — tiny "INTENT" label (9px uppercase `--faint`) over
     "Analyze a stock" (11.5px/500) + chevron. Value truncates with ellipsis if needed.
  2. **Stock** field (`flex: 1`) — "STOCK" label over the selected symbol (e.g. "TATAMOTORS").
  3. **Ask button** (`.f2-ask`, fixed width) — mint bg, near-black text (#06130c), 600,
     "Ask" + arrow icon, radius 9px, soft mint shadow. Hover → `--mint-hi`.
  - Inset fields: bg `rgba(255,255,255,0.03)`, 1px `--line`, radius 9px.

### 3. Right panel — flex:1, scrolls (`.f2-right`)
Content padded 26×32px, max-width 760px (`.f2-rpad`).
- **Greeting** (`.f2-greet`, 24px/600, -0.03em): mint `✴` + "Good morning, Shyam".
- **Subline** (`.f2-sub`, 14px `--muted`): "Markets opened firm — IT & financials are leading today."
  *(The regime/breadth pills that earlier sat top-right were intentionally removed.)*
- **Explanation card** (`.f2-card`, gradient `--card → --card-2`, 1px `--line-2`,
  radius 18px, 22×24px padding, soft shadow):
  - **Header row**: `#{rank}` (15px/700 mono mint) + symbol (16px/600) + "{name} · {sector}" (13px `--faint`).
  - **Badges row**: an action badge (`.f2-badge`: "STRONG BUY" / "BUY" / "ACCUMULATE" —
    mint text on `--mint-dim`, inset mint ring) + the `Tier` badge +
    "sig {day%} · 30d {30-day%}" (12px mono `--muted`).
  - **"Why we're picking this"** (`.f2-h`, 14px/600) + paragraph (13.5px/1.6 `--ink-2`),
    ending with "**{N} signals** are firing on this stock right now…".
  - 1px divider (`.f2-rule`).
  - **"Historical track record"** + paragraph: "When **{SYM}** has appeared in our Falcon
    Top 10 before (2023–2026), it worked **{wins} times out of {total}** ({rate} win rate)."
    Then a bulleted list (`.f2-bul`, faint round bullets):
    - "When it worked → **+X%** (mint) per trade. When it didn't → **−Y%** (red) per trade."
    - "Worst single trade: **−Z%** (red) — it hit the stop, exactly what the stop is there to do."
  - 1px divider.
  - **"What the sector is doing"** + a one-paragraph sector note.

---

## Interactions & Behavior
- **Select a signal**: clicking any row in the Top 10 sets the selected symbol; the right
  panel re-renders for that stock. The selected row is highlighted (`--mint-dim`), and the
  guided prompt's Stock field reflects the current symbol.
- **Switch persona**: opening the persona dropdown and choosing a persona **re-ranks the
  Top 10** (see ordering logic below) and auto-selects the new #1.
- **Guided prompt**: structured, not free-text. Intent is a fixed set ("Analyze a stock",
  etc.); Stock is chosen from Falcon's covered universe. In the prototype the fields are
  display-only; in production wire them to real dropdowns and have **Ask** trigger the
  analysis for the chosen intent + stock.
- No page navigation — everything is one screen; selection swaps the right panel only.
- Transitions: subtle 0.12–0.14s background/border transitions on hover; no large motion.
- The detail card re-mounts per symbol (React `key={sym}`), giving a clean swap.

## State Management
Local component state in `ScreenF2`:
- `persona` (string id, default `"swing"`) — current trader persona.
- `sel` (string symbol, default `"TATAMOTORS"`) — currently selected signal.
- `q` (string) — search/filter text (used to filter the list; the visible field is the
  guided prompt, but the filter hook remains for when search is wired up).
- Derived: `ordered = orderFor(persona)`, `current = ordered.find(sel) || ordered[0]`,
  `pos = index of current in ordered + 1` (the "#" shown in the detail header).
- `PersonaSelect` holds its own `open` boolean for the dropdown.

**Persona → Top 10 ordering** (`orderFor`):
- `swing` (Falcon Top 10 Swing): by intrinsic rank.
- `btst`: by day % descending.
- `intraday`: by conviction (`conf`) descending.
- `weekly`: by 30-day return descending.
- `longterm`: by tier rank (enterprise > goldplus/entcand > gold > premium > standard), then conf.

**Data fetching (production):** replace the static `F2DATA` array with the live
Top-10 signal feed; replace `SEC[sector]` with real sector context; compute the track
record from historical signal outcomes. Each signal needs: rank, symbol, name, sector,
tier, action, conviction (0–100), day %, 30-day %, "why" text, signal count, and a track
record `{ wins, total, rate, avgWin, avgLoss, worst }`.

## Design Tokens

**Colors**
| Token | Hex / value | Use |
|---|---|---|
| `--canvas` | `#0a100e` | App background (near-black, green-tinted) |
| `--panel` | `#0c1310` | Rail / middle-panel background |
| `--card` | `#111b16` | Card base |
| `--card-2` | `#0f1814` | Card gradient end |
| `--mint` | `#3FE3A4` | Single brand accent ("green = profit") |
| `--mint-hi` | `#5AECB5` | Accent hover |
| `--mint-dim` | `rgba(63,227,164,0.10)` | Active/selected tint |
| `--ink` | `#e9f2ec` | Primary text |
| `--ink-2` | `#c2d0c9` | Body text |
| `--muted` | `#859990` | Secondary text |
| `--faint` | `#5b6c64` | Labels / tertiary |
| `--line` | `rgba(255,255,255,0.065)` | Hairline borders |
| `--line-2` | `rgba(255,255,255,0.10)` | Stronger borders |
| `--amber` | `#E6B450` | Gold / Gold+ tier |
| `--teal` | `#4BCBE0` | Premium tier |
| `--tier-green` | `#3FE3A4` | Enterprise / Ent. Candidate tier |
| `--slate` | `#8595a0` | Standard tier |
| `--red` | `#E8736B` | Losses / stop-loss only |

Tier badge label map: `gold→Gold`, `goldplus→Gold+`, `premium→Premium`,
`enterprise→Enterprise`, `entcand→Ent. Candidate`, `standard→Standard`.

**Typography** — Geist (UI) + Geist Mono (all numbers/tickers/prices; enable tabular nums).
Scale used: 24/600 (greeting), 16/600 (card symbol), 15/700 mono (#rank), 14/600 (section
headings), 13.5/550 (list symbol, body), 12.5 (day %), 11.5 (prompt values), 11 (captions),
10 (labels), 9–9.5 (badge/label uppercase, ~0.05–0.12em tracking). Base letter-spacing
-0.01em on UI text; 0 on mono.

**Spacing** — panel widths: rail 210 / middle 336 / right flex. Common paddings 6/8/9/11/
14/16/18/22px. Gaps 6–12px.

**Radii** — 6px (badges), 8–9px (nav items, inset fields, buttons), 10px (list rows),
11–12px (selectors, prompt), 14px (inner blocks), 18px (main card).

**Shadows** — card: `0 24px 70px -34px rgba(0,0,0,0.55)`. Ask button:
`0 8px 22px -10px rgba(63,227,164,0.7)`. Mint "glow" dots: `0 0 8px var(--mint)`.

## Assets
- **Fonts**: Geist + Geist Mono (Google Fonts). Substitute the codebase's equivalent
  if Geist isn't available, but prefer Geist for fidelity.
- **Icons**: all inline SVG (1.6 stroke, `currentColor`), defined in `f2Ico` inside
  `falcon-f2.jsx` — ask/sparkle, signals, co-trade, autotrade, performance, plans, search,
  chevron, flame, clock, bolt, trend, shield, arrow. Swap for the codebase's icon set
  (keep them line-style, ~1.6 stroke, monochrome).
- No raster images. No external image assets.

## Related (optional)
An enhanced variant **F3** also exists in the design project (`falcon-f2.jsx` exports its
data so F3 can reuse it). F3 adds, in the right panel: a 30-day **sparkline**, a
**signal-strength breakdown** (Trend / Momentum / Volume / Rel. strength bars), and
**collapsible** sections. Treat those as optional enhancements on top of F2.
