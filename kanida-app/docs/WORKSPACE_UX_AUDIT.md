# Workspace UX: benchmark, audit and rebuild (22 Sep 2026)

This benchmarks the KANIDA workspace against TrendSpider's dashboard (charts.trendspider.com/dashboard), which I
inspected live in the owner's Chrome by DOM measurement plus screenshots, changing nothing. The goal was to study
interaction and layout quality, not to copy the look. KANIDA keeps its own dark, mint language.

## 1. What TrendSpider does well (measured)
- **Rows, not a free grid.** The dashboard is a stack of named sections, each one row of panels in a non-wrapping
  flex line. A row never wraps, so adding a panel makes its neighbours give up width; nothing drops onto a new line
  of its own.
- **Width by content class, not by pixels.**

  | Class | Flex | Result |
  |---|---|---|
  | List (scanners, movers) | `0 1 auto` | stays compact, about 278 px |
  | Chart | `1 1 25%` | weight 1 |
  | Table or map (flow, heatmap) | `2 1 50%` | weight 2 |

  Measured rows: 4 × 305 · 2 × 620 · 278 / 278 / 385 / 278 · 825 / 414. The screen is always full.
- **One height.** Every panel on the page is 402 px, so rows line up and the page reads as a grid, not a collage.
- **One chrome.** A 10 px gap everywhere, 10 px radius, and a 1 px #444 border. Headers hold only a title and one
  or two controls, and the extra controls appear on hover.
- **Three type steps, one family.** Body 14/21, panel title 16/24 bold, section title 21/31.5 bold. Hierarchy comes
  from weight and position, not from many sizes.
- **Insertion is local and obvious.** A "+" sits at each row's left and right edges to add beside, and "+ Add
  content" under a row. A "columns" control (4–10) sets density globally, rather than per widget.

## 2. Where our workspace differed
| | TrendSpider | KANIDA before |
|---|---|---|
| Model | rows with flexible shares | a wrapping 12-column grid with fixed spans |
| Adding a widget | neighbours shrink | appended; wraps onto a new line, leaving empty space to the right |
| Heights | one height | two fixed heights (380 / 600), never fitted to the screen |
| Type | 3 steps, 1 family | about 9 sizes across two families, plus the reused blocks' own scale |
| Panel chrome | one | tile frame + the block's own frame + the panel's own frame (double titles and borders) |
| Resize | by class and density | corner snap between spans |

## 3. Exact problems found
1. **A third widget dropped to a new row** (M + M left 4 of 12 columns empty). The owner saw this.
2. **Rows ignored the screen height,** so 3–4 widgets already scrolled.
3. **Double titles.** The tile said "PCR", then the block inside said "PCR through the session" at 19 px Manrope
   with a paragraph.
4. **Headline figures were 26 px display type** inside 270 px tiles, so values dominated for no reason.
5. **Widget titles were 13 px while inner block titles were 19 px:** a sub-part louder than its container.
6. **Borders inside borders:** the tile, then the WidgetFrame, then the Headline box.
7. **Narrow tiles truncated their own titles** ("Max p…") because the follow chip took the room.
8. **The option chain at 300 px** collided its columns.
9. **Sub-pixel overflow** wrapped a "full" row. This was fixed earlier and replaced by the flex model.

## 4. The layout behaviour now implemented (`src/workbench/layout.ts`)
- **Rows share the width by weight:** S 1 · M 2 · L 3, F takes a row to itself. Each widget has a minimum width:
  its class minimum (S 250 · M 300 · L 400) or its own, if its content needs more (option chain 420, OI through
  the session 520, and others; `registry.MIN_WIDTH`).
- **Balanced partition.** Order is preserved. The engine picks the fewest rows that respect every minimum and the
  per-row cap, then the most even split of weight (an exact linear partition).
- **Results at 1440 × 900:**

  | Widgets | Layout |
  |---|---|
  | 1 | fills the screen |
  | 2 | halves |
  | 3 | one row |
  | 4 | one row of 4, or 2 + 2 when wide |
  | 6 | 3 + 3 |
  | 8 | 4 + 4, both rows on one screen |

  Nine or more scroll at one standard row height (440; 520 for widgets that ask for tall).
- **Screen-fit heights.** When every row can have at least 320 px, the rows share the visible height, so adding a
  widget doesn't create a scroll.
- **Density:** "Per row: Auto · 2 · 3 · 4 · 5" in the top bar, saved with the workspace.
- **Checked by** `node scripts/check-workbench-layout.cjs`, 21/21: the cases above, Full, minimums, the cap, narrow
  screens, phone, resize snapping, and order preservation.

## 5. The typography and chrome system (`src/workbench/tokens.ts`)
| Role | Face | Size | Used for |
|---|---|---|---|
| page | Manrope 700 | 20/28 | the workspace name, once |
| title | Inter 600 | 13/18 | every widget title; list-row titles |
| label | Inter 500, caps, +0.6 | 10/14 | section labels (it is the Derivative tab's own `head`) |
| body | Inter 400 | 12/18 | reading text, chips |
| helper | Inter 400, muted | 11/16 | as-of lines, captions, counts |
| metric | Inter 600, tabular | 15/20 | a figure that is the point of its row |

**Chrome:** header 40 · padding 12 · inner gap 8 · radius 12 · 1 px `C.line` border · gutter 12 · controls 28 px ·
icons 14. It's the same on every widget. All workspace text now uses these roles: 56 ad-hoc styles were mapped.

The reused Derivative blocks are brought onto the same scale inside a tile by `BlockBareContext`. That is the one
after-close change to `src/derivative/frame.tsx` (`scripts/apply-workspace-hooks.py`). Inside a tile, it:
- drops the block's own title row
- turns the 26 px headline into the metric token
- removes the inner frame's border and its duplicate name

The Derivative tab itself is unchanged.

## 5b. "A widget fits its box" (the owner's observation, then measured)
**TrendSpider, measured per panel:**
- Chart and map panels never scroll (11 of 11).
- List panels scroll with a 5 px hairline. Table panels scroll with the bar hidden (`scrollbar-width: none`).
- No panel shows a horizontal bar. Horizontal overflow in lists is 6 px.

**KANIDA now:**
- **Fit:** PCR, max pain, IV, futures build-up, OI by strike and OI through the session (`widgets.tsx FIT`) are laid
  out to their tile. They show both panels side by side when there's room (900 px or more) and the chart alone when
  there isn't; the headline above keeps the latest figure. Panels are exactly as tall as the space under the
  headline, in a frame that never scrolls. "Show: Fit to size" is the default, and the user's own choice wins.
- **Lists and reading text:** Screener results, option chain and AI summary may scroll vertically only. There is
  no track, and a 4 px thumb appears on hover (`scroll.ts`, one stylesheet scoped to the workspace, which also
  covers the reused blocks without touching their files). There is no horizontal bar anywhere.
- **Measured in the preview, 8 widgets at 1440 px:**

  | | Widgets |
  |---|---|
  | fit, no scroll | PCR, Max pain, Greeks, Key strikes, 15-min signal |
  | vertical scroll, invisible bar | Screener results, Option chain, AI summary |
  | horizontal bars | none |

## 6. Add, remove, resize, reorder
- **Add:** the top-bar button or "+ Add widget" after the last row appends. Hovering the gap between two rows shows
  "+ Add widget here", which inserts at that point. Every add re-composes the rows, and the new widget scrolls into
  view with one green pulse of its border.
- **Remove:** the rows re-compose at once (8 → 4 + 4). An Undo toast lasts 6 s.
- **Resize:** drag the edge between two neighbours. The pair snaps live to the nearest S / M / L split and the rest
  of the row reflows. The size chips in ⚙ do the same by keyboard or tap.
- **Reorder:** drag the header grip. A green marker shows before or after, depending on which half of the target is
  under the pointer. Dropping re-composes the rows.
- **Expand:** full screen, with Esc to return. **Duplicate:** placed right after the original.

## 7. Desktop vs mobile
- **Desktop:** the composed rows above, screen-fit heights, the density control, the edge resize and the drag grip.
- **Phone (under 760 px):** one ordered column in the saved order. Widget height by class is 420, or 560 for L / F /
  tall. Each widget collapses. Move up / down replaces dragging. The instrument chip is sticky. There is no density
  control or grip, and no horizontal scroll (measured at 375 px).

## 8. Frontend changes made
- **New:** `tokens.ts`, `layout.ts`, `scripts/check-workbench-layout.cjs`.
- **Rewritten:**
  - `Tile.tsx`: flex share and min width from the engine, the shared resize edge, the drop marker, the add pulse,
    token chrome, and title priority on narrow tiles.
  - `index.tsx`: composed rows, screen-fit heights, the density control, before/after drops, between-row inserts,
    and positional add.
- **Restyled:** `widgets.tsx` and `Sheets.tsx`, onto the type roles. `Builder` got a `dense` mode for the widget.
- **Server:**
  - `layout.columns` is added to the workspace definition, with validation and a store migration.
  - The registry gets size classes and per-widget minimum widths.
  - Templates carry order and class only.
- **Hook script:** four more anchored edits to `frame.tsx`, making ten edits in total across the three files. The
  dry run matches every anchor exactly once.

## Verification
- The browser at 1440 × 900 matched the table in §4 at every step, within the screen. Measured bottom edge: 807 px
  for 1–4 widgets, and 875 px for 6 and for 8 after a remove.
- The edge drag took PCR from 269 to 504 px (S → L), and the row reflowed.
- A drop onto PCR's left half placed Greeks before it.
- The phone view at 375 px is one column with no sideways scroll.
- **After-close preview:** the same source with the hook applied to a copy only, built into `dist-preview/` and
  served on :8093. It shows the single-scale result: block titles gone, headline figures at the 15 px metric.
- **Checks:** layout 21/21 · screener parity 61/61 · derivative 453 · server suite 545 passed, 1 skipped. `tsc` is
  clean.

## Honest limits
- **The reused blocks keep their own scale on :8092** until the after-close hook is applied. :8093 shows the
  result.
- **Not every mix fits on one screen.** Eight widgets that include an option chain (420 px minimum) and two L
  widgets need three rows at 1440 px. The engine then scrolls at a steady height rather than crushing the chain.
- **The OI and IV session charts still don't shade the match's time window.** That would need a change inside
  their `src/derivative` components.
