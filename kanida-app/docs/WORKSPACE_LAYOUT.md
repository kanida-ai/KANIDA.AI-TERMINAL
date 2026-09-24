# KANIDA Workspace: layout sketch and data map (APPROVED 22 Sep 2026; built, see WORKSPACE_RESULT.md)

Brief and decisions: `WORKSPACE_SPEC.md`. No code has been written yet.

## (1) Desktop workspace

```
┌ KANIDA  Falcon  Discover  Derivative  Workspace  Watchlist  AutoTrade ─────────────────────────────────┐
│ [My Intraday Options ▾]   ◉ NIFTY · 29 Sep · focus 24,400–24,600 CE     as of 11:15 · next ~11:30      │
│                                                    [✦ AI summary]  [+ Add widget]  [⋯ workspace]       │
│ ┌ Screener results ────── ⚙ ⤢ ✕ ┐┌ Option chain  ⛓ follows ─── ⚙ ⤢ ✕ ┐┌ 15-min signal ⛓ ── ⚙ ⤢ ✕ ┐  │
│ │ Call OI building ▾   Active 29 ││ NIFTY 29 Sep · ATM 24,400          ││ BUILDING  Call writing     │  │
│ │ ● NIFTY 24,500 CE   NEW 11:15  ││  … rows, focus band 24,400–24,600  ││ key strikes 24,500 CE …    │  │
│ │ ■ RELIANCE 2,960 CE  STILL     ││    highlighted                     ││                            │  │
│ │ ▲ HDFCBANK 1,720 CE  STRONGER  ││                                    ││                            │  │
│ └─────────────── M (4 cols) ─────┘└────────────── M (4) ───────────────┘└──────────── M (4) ───────────┘  │
│ ┌ AI summary ⛓ ──────────────────────────── ⚙ ⤢ ✕ ┐┌ OI through the session ⛓ ─────────── ⚙ ⤢ ✕ ┐  │
│ │ What changed · Where · Persistent · Conflicting  ││ ΔOI tiles, 10:15–11:15 shaded (the match's   │  │
│ │ Key strikes · Unusual — each line cites its widget││ episode)                                      │  │
│ └────────────────────────── L (6) ──────────────────┘└──────────────────── L (6) ────────────────────┘  │
│ ┌ PCR 📌 BANKNIFTY ─ ⚙ ⤢ ✕ ┐┌ Max pain ⛓ ─ ⚙ ⤢ ✕ ┐     (empty space is fine)                           │
│ └────────── S (3) ─────────┘└─────── S (3) ──────┘                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**The top bar**
- The workspace switcher.
- The **selected instrument** chip, with an optional **focus** (strikes and a time window) carried over from the
  screener match.
- The as-of time and the next reading.
- **+ Add widget.**
- **⋯** opens rename, duplicate, delete and "Start from template".

**Widget header.** A title, then ⛓ *follows* or 📌 *pinned to X*, then ⚙ settings, ⤢ expand, ✕ remove. Nothing
else. Dragging the header moves the widget. A corner handle snaps between sizes.

**Grid**
- 12 columns. **S = 3 · M = 4 · L = 6 · Full = 12.** Two heights: Short and Tall.
- Widgets pack left to right and wrap. Dropping a widget reflows the rest; nothing floats free.
- **⤢ Expand** opens the widget full-screen as an overlay, and Esc returns.

## (2) Add widget library (searchable)

```
┌ Add widget ───────────── [ search: "iv" ] ┐
│ SCREENERS    Screener results · Screener   │
│              builder · Scanners (KANIDA +  │
│              mine) · Market scan           │
│ OPTIONS      Option chain · OI by strike · │
│              IV · PCR · Max pain · Greeks ·│
│              Key strikes                   │
│ CHARTS       Price chart · OI through the  │
│              session · IV chart · Volume   │
│ INTELLIGENCE 15-min signal · AI summary ·  │
│              Session history · Signal-to-  │
│              noise                         │
│ MARKET       Futures build-up · Index      │
│              dashboard · Watchlist · Alerts│
│ each row: one-line "what it shows" + size  │
└────────────────────────────────────────────┘
```

**Widget settings (⚙).** Only the widget's own settings, as chips, never number boxes. The first row is always the
mode: `Follow workspace` or `Pin to …` (instrument, then expiry). Below it, only what that widget has: timeframe,
calls / puts / both, ATM ±, view, or scanner.

## (3) Every widget mapped to real data

| Widget | Data (existing route) | Reuses | Settings |
|---|---|---|---|
| Screener results | `/api/screener/scanners/{id}/results` | screener `ResultsPanel` | scanner, view |
| Screener builder | `/api/screener/parse` · `describe` · `run` | screener `Builder` | none (runs into its own results) |
| Scanners | `/api/screener/scanners` | the list rows | KANIDA / mine |
| Market scan | `/api/derivatives/screener` + `snapshots` | "What's happening" list | filters |
| Option chain | `/api/derivatives/chain` | ChainWidget | expiry, ATM ±, calls / puts / both |
| OI by strike | `/api/derivatives/oi-by-strike` | OiByStrikeSection | expiry |
| OI through the session | `/api/derivatives/oi-grid` | OiGridSection | expiry |
| IV / IV chart | `/api/derivatives/iv-series` | IvGridSection / SessionPanel | view: ATM line / by strike |
| PCR | `/api/derivatives/pcr-series` | SessionPanel | OI / volume |
| Max pain | `/api/derivatives/maxpain-series` | SessionPanel | none |
| Greeks | **new** `/api/workspace/greeks`, the screener's IV cache (BSM, COMPUTED) | new | expiry, ATM ± |
| Key strikes | `signal.ts` `standing()` + `key_strikes` from `/snapshots` | SignalPanel parts | none |
| Price chart | `/api/derivatives/futures-chart` + spot from `/series` | FuturesChartPanel | 15-min / daily |
| Volume | `futures-chart` volume + call / put volume per reading (metrics) | new, small | futures / options |
| 15-min signal | `/api/derivatives/snapshots` | SignalPanel | none |
| Session history | `/api/derivatives/events` / snapshots | SessionBlocks | none |
| Signal-to-noise | `/api/derivatives/signal-noise` | existing report | session |
| AI summary | **new** `/api/workspace/summary` (deterministic, below) | new | which widgets to read |
| Futures build-up | `/api/derivatives/futures-buildup` | FuturesWidget | none |
| Index dashboard | `/api/derivatives/indices` | IndexWidget | none |
| Watchlist | the user's watchlists (the same source as /watch) | list | watchlist |
| Alerts | `/api/screener/alerts` | alerts list | none |

**Merged rather than padded.** "OI" and "OI chart" are one widget type with a view setting, and so are "IV" and
"IV chart". The spec's four screener widgets become three: KANIDA and mine are one list, with a tab.

**Not built.** An underlying *equity* candle chart. The owner chose futures + spot.

## (4) How the widgets act as one system
- **The selection.** `{underlying, expiry, focus?: {side, strikes[], window {from, to}, source: scanner-match}}`
  lives on the workspace.
  - Clicking a Screener result sets it: the instrument, the match's lead strikes, and its episode window
    (first matched → now).
  - Clicking a row in Market scan, Watchlist or a chain row sets it the same way.
- **Focus rendering.**
  - The option chain scrolls to the strikes and bands them.
  - OI and IV charts shade the window.
  - Key strikes and the signal show the current state.
  - Pinned widgets ignore the selection and say so in the header (📌).
- **AI summary** is deterministic and on the server. `POST /api/workspace/summary {underlying, expiry, focus,
  sources[]}` reads the same stored data those widgets draw, at the same reading. It returns six short sections,
  and each line names its source ("from OI · 24,500 CE"):
  - **What changed:** this reading against the last.
  - **Where:** strikes and range against ATM.
  - **Persistent:** behaviours held across readings (the signal engine's episodes).
  - **Conflicting:** for example, price up while put OI builds, or IV not confirming. The same `conflicting` rule
    as `signal.ts`.
  - **Key strikes:** `standing()` and `key_strikes`.
  - **Unusual:** the stored `unusual` flags and volume ratios.

  It is only what the data says, with no forecast and no buy / sell. It reads only the sources whose widgets are on
  the workspace. When there is too little evidence, it says so.
- **Mobile (<760 px).**
  - An ordered vertical feed in the saved order, with sizes ignored and a sticky selected-instrument chip.
  - Each widget collapses to its header. Reorder with "Move up / down" in ⚙ instead of dragging.

## (5) Performance
- **One data hub per page.** `useShared(url)` dedupes requests in flight and caches by URL until the reading
  changes. Five widgets on NIFTY's snapshot make one request.
- **One refresh per page.** A single `status` poll runs once a minute. When a new 15-minute reading lands, only the
  cached URLs are invalidated. Widgets never poll on their own.
- **Lazy rendering.** A widget off screen renders its header only, and fetches when it scrolls into view. On web
  this uses IntersectionObserver.
- Expanding a widget reuses the cached data.

## (6) Backend: `server/kanida_pilot/workspace/` + `var/workspace.db`
It is mounted the same way as the screener: one line in app.py, applied after close.

```
workspaces(id, user_id, name, position, template, selected JSON {underlying, expiry, focus},
           widgets JSON [ {widget_id, widget_type, position, size: S|M|L|Full, height: short|tall,
             follow_workspace, instrument, expiry, strike_range, timeframe, scanner_id, settings{}} ],
           version, created_at, updated_at, deleted)
```

- **Structure.** Each widget is validated against a server-side registry of widget types and their allowed
  settings, so the definition is structured, not a bag of coordinates. Scanners are referenced by `scanner_id` and
  stay in `var/screener.db`.
- **Autosave.** The page debounces a save about 800 ms after a change. The server checks `version`, so an edit in
  a second tab cannot silently overwrite the first.
- **Routes.** `/api/workspace/workspaces` (list / create), `…/{id}` (get / save / rename), `…/{id}/duplicate`,
  `…/{id}/delete`, `/api/workspace/templates`, `/api/workspace/summary`, `/api/workspace/greeks`.
- **Privacy.** Workspaces are owner-scoped, and another user's workspace returns 404.
- **Templates.** The spec's five are seeded as data.

## (7) The three final tests, and how the design meets them
1. **Beginner, under a minute.** Pick the "Options Trader" template, which already works. Then click a result.
2. **Experienced trader.**
   - Duplicate any widget, pin copies to different instruments or expiries (NIFTY and BANKNIFTY side by side).
   - Use several scanners, the builder in-place, and S / M / L / Full sizes.
   - Keep several saved workspaces.
3. **One system.** There is one selection, a shared focus from the match, and a summary built over exactly the
   widgets on screen.

## Open questions (in the chat reply)
