# KANIDA Workspace: build result (22 Sep 2026)

This is the build against `WORKSPACE_SPEC.md`, using the layout the owner approved in `WORKSPACE_LAYOUT.md`, with
these decisions: a new /workspace tab, a deterministic AI summary, a snap grid, and futures + spot for the price
chart.

Guardrails held throughout:
- Nothing was committed or deployed, and nothing was installed.
- Ports 8082 and 8765 and the KANIDA tasks were not touched.
- `app.py` and `src/derivative/*` were not edited.
- `db/derivatives.db` and `var/intelligence.db` were only read, with `mode=ro`.

## Try it now (dev, port 8092)
Open `http://127.0.0.1:8092/__dev/session?as=owner&to=/workspace`. A second account is `as=member`.

- It runs on the live session: today's readings from 09:30.
- Dev accounts and stores live in `var/dev-screener/`, and the cookie is isolated from 8082.
- Restart with `scripts/start-screener-dev.ps1`, or add `-Build` to re-export `dist-screener/` first.

## After 15:30: one command, then a rebuild (owner, with OK)
```
.pilot-venv\Scripts\python.exe scripts\apply-workspace-hooks.py            (dry run; lists every change)
.pilot-venv\Scripts\python.exe scripts\apply-workspace-hooks.py --apply
scripts\start-pilot.ps1 -Build
scripts\register-screener-task.ps1                                          (alerts job, optional)
```

The script makes three changes. Each anchor must match exactly once, and each change is skipped if it's already
applied. I dry-ran it today: all anchors match. I also compiled patched copies: `app.py` compiles, and the TSX has
no syntax errors.

1. **`app.py`:** mount the screener, then the workspace, at the end of `create_app()`.
2. **`src/shell/routes.tsx`:** add Workspace and Options Screener to the nav, as normal tabs, with their active
   state. If you'd rather have only one of the two new tabs, delete the other entry.
3. **`src/derivative/frame.tsx`:** add `BlockBareContext`. It's one React context. A reused block inside a
   workspace tile drops its own title row, because the tile header already names it. The Derivative tab never sets
   it, so nothing there changes. Until this lands, reused blocks show their title twice.

The production stores are `var/screener.db` and `var/workspace.db`, both created on first start.

## What was built
**Server: `server/kanida_pilot/workspace/`**

| Module | What it does |
|---|---|
| `registry.py` | 22 widget types, each backed by a live route, with their allowed settings. |
| `model.py` | Validates the definition: each widget against the registry, settings a widget doesn't have are dropped, a pinned widget needs an instrument, and the focus carries the scanner and match. |
| `store.py` | `var/workspace.db`, with a version check on autosave that returns 409 plus the newer copy. |
| `templates.py` | 5 templates. Every row fills the 12 columns, and a test enforces that. |
| `summary.py` | The deterministic AI summary. |
| `alignment.py` | Checks the user's scanner against the 15-min signal. |
| `extras.py` | Greeks (the screener's BSM IV cache, with the expiry-day roll) and volume per 15-min interval. |
| `routes.py`, `__init__.py` | The routes and the mount. |

**Web: `src/workbench/` and `app/workspace.tsx`**
- **Grid:** 12 columns with S / M / L / Full sizes in CSS `calc`, so a row never overflows. Drag the header grip to
  reorder. Drag the corner to resize; it snaps to a size and shows its name. There is a Short / Tall height and
  full-screen expand. Removing a widget offers Undo.
- **Library:** a searchable Add-widget sheet in 5 categories. Each item has a one-line description and a Follows or
  Sets-the-instrument tag.
- **Settings (⚙):** Follow the workspace or pin an instrument, expiry (only the two captured ones), the widget's
  own options, size, height, duplicate, move up / down and remove.
- **Workspaces:** switch, create from a template, rename, duplicate, delete, and autosave about 0.8 s after a change.
  The last-opened workspace reopens.
- **One selection:** picking a scanner match, an index row, a futures row, a watchlist symbol, an alert, or the
  instrument chip sets the instrument. A scanner match also sets a **focus**: its strikes, its window, the scanner
  and the match.
  - The option chain, OI and IV widgets highlight the focus strike.
  - The strike under the pointer highlights across widgets.
  - The focus ribbon in the top bar can be cleared.
- **Your scanner, everywhere** (your mid-build question): the AI summary opens with the scanner. It shows the
  scanner's name and "Reads as", the match's live lifecycle, its "Matched because" lines, and, condition by
  condition, whether the 15-min signal agrees or differs at this reading. The 15-min signal widget shows the same
  strip. The server only reads a scanner that is the caller's own or a KANIDA default; anyone else's is ignored.
- **Hub:** in-flight and per-reading dedupe under every read. Measured in the browser: **no duplicate widget
  requests** with the signal, option chain and summary open on one instrument. There is one status check a minute
  and a single re-read when a reading lands. Widgets off screen render their header only.
- **Phone:** an ordered full-width feed, with the instrument chip sticky at the top. Each widget collapses, and
  Move up / down replaces dragging. No sideways scroll at 375 px.

## Verification
- **Server tests:**
  - 544 passed and 1 skipped in the full suite, including 14 new workspace tests.
  - The screener and alignment tests all pass.
- **Consistency checks:** `check-screener.cjs` passes 61/61 and `check-derivative.cjs` passes 453. `tsc` is clean.
- **Browser, dev server, live 22 Sep session, walked screen by screen:**
  - The first visit gets a working workspace.
  - Switching the results widget to another scanner, then clicking a match, moves every connected widget and the
    AI summary to that instrument.
  - Add PCR, pin it to BANKNIFTY, drag it before the AI summary, and snap-resize it from Small to Large. After a
    reload, the order, size, pin, selection and focus all survive.
  - Full-screen expand, a new workspace from each template, and the phone feed at 375 px.
- **Found and fixed in testing:**
  - Rows wrapped from sub-pixel overflow; widths now use CSS `calc`.
  - Two templates didn't fill 12 columns; they're repacked, and a test now enforces it.
  - IV didn't roll on expiry day like Greeks; it now rolls, with a note.
  - The settings sheet offered 18 NIFTY expiries when only 2 are captured; it now offers 2.
  - The summary's IV line contradicted the engine's "IV was unchanged"; the engine now wins.
  - The screener's "ended because" text was confusing for "continuously"; it now names the interval that broke.
  - The summary heading said "Your scanner" for KANIDA defaults.

## Final tests
- **Beginner, under a minute.** The first visit already opens "Options Trader": results, chain, signal and
  summary. Click any match and the workspace investigates it. No setting is needed.
- **Experienced trader.** 22 widget types, any number of duplicates, pinning per widget (NIFTY and BANKNIFTY side
  by side), per-widget expiry and views, their own scanners and the builder in place, 4 sizes, and 20 workspaces.
- **One system.** There is one selection and one focus, and the summary is built over exactly the widgets on this
  instrument. It names each line's source, and says which sources it didn't read because no widget for them is on
  the workspace.

## Known limits (honest)
- **The OI and IV charts highlight the focus strike but don't shade the match's time window.** That would need a
  prop inside `src/derivative`'s session charts, which I didn't edit. The top bar states the focus window.
- **Price chart:** futures only, as decided. There is no underlying equity candle chart.
- **The Signal-to-noise widget is empty on the dev server,** because the dev server's pilot snapshot store is a
  throwaway file. It reads the real report once mounted in the pilot.
- **Drag to reorder is desktop / web.** The phone uses Move up / down.
- **The option chain opens on the newest reading.** It isn't pinned to the reading the Derivative screener
  resolved.
