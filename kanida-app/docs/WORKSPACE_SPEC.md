# KANIDA Workspace: owner brief (saved 22 Sep 2026)

Requirements record. The design is in `WORKSPACE_LAYOUT.md`. The reference screenshot, a TradingView-style widget
board, shows the interaction concept only: modular widgets, "+" to add, a per-widget menu and close. KANIDA's own
dark / green visual language is kept.

## Owner decisions (22 Sep)
- **Placement.** The workspace is a new `/workspace` page, and the Derivative tab stays. Existing blocks are reused
  through thin wrappers. Small prop changes in `src/derivative` are made only after 15:30 IST, with the owner's OK.
- **AI Summary.** A deterministic synthesis on the server over the connected widgets' structured data. There is no
  LLM, no key and no cost.
- **Grid.** An in-house 12-column grid with size presets (Small / Medium / Large / Full). Drag the header to move;
  widgets snap into place. No package installs.
- **Price chart.** Front-month futures candles (15-min / daily) plus the captured 15-min spot line.

## Objective
A user-configurable workspace made of modular widgets. It should not feel like a fixed dashboard.

The default flow: **the screener discovers → the user selects an instrument → connected widgets investigate →
AI explains the combined evidence.** Keep it simple, not a Bloomberg terminal.

## Workspace
Users can:
- add, remove, drag and rearrange, resize, expand full-screen, and configure widgets
- save the workspace to their profile
- keep several workspaces

Layout, size, position, configuration and settings persist per user.

## Screener as the starting point
The screener can be a KANIDA default, a user-created scanner, or a natural-language scanner. Selecting a result
updates every connected widget: option chain, price chart, OI, IV, PCR, max pain, 15-min signal, AI summary, key
strikes and futures. The instrument is never re-selected per widget.

## Widget library
The library is searchable and has these categories:

| Category | Widgets |
|---|---|
| Screeners | Builder, Results, My saved, KANIDA |
| Options | Option chain, OI, IV, PCR, Max pain, Greeks, Key strikes |
| Charts | Price, OI, IV, Volume |
| Intelligence | 15-min signal, AI summary, Session history, Signal-to-noise |
| Market | Futures, Underlying, Watchlist, Alerts |

Only widgets backed by real product data are included. There are no placeholders.

## Widget behaviour
- **Two modes.** Follow workspace, or Independent (pinned to its own instrument, expiry, timeframe and settings).
- **Settings.** Each widget shows only its relevant settings (instrument, expiry, strike range / ATM±, timeframe,
  calls / puts / both, view, scanner) behind a small settings control.
- **Header.** A clear title and minimal controls: settings, expand, remove.

## Layout
- A responsive grid with Small / Medium / Large / Full sizes. Widgets snap into place; nothing floats freely.
- **Templates:**
  - **Options Trader:** Results, Option chain, 15-min signal, AI summary
  - **Index Derivatives:** NIFTY screener, OI, IV, PCR, Max pain, Option chain
  - **Intraday Scanner:** Screener, Results, Price chart, 15-min signal, Alerts
  - **Volatility Watch:** IV scanner, IV chart, Option chain, AI summary
  - **Blank**
- Templates are starting points only.

## AI connects the workspace
The AI Summary uses the structured data of the widgets on the same instrument. It covers:
- what changed
- where
- what is persistent
- what is conflicting
- which strikes matter
- what is unusual

It gives no buy / sell recommendations.

## Cross-widget
Selecting an instrument from a match with "Call OI increasing continuously" does the following:
- the OI chart highlights the period
- the option chain focuses on the active strikes
- the signal shows the current state
- the summary explains the combined evidence

The widgets act as one system.

## Duplicates
Any widget can be added more than once, and each copy keeps its own configuration. Examples: two charts on
different timeframes, two chains on different expiries, NIFTY and BANKNIFTY side by side.

## Saved workspaces
Create, name, rename, duplicate and delete. Saving is automatic, and workspaces are private by default.

## Desktop and mobile
Desktop first, with the grid. On mobile, the workspace becomes an ordered vertical feed that keeps priority,
settings, the selected instrument and the order.

## Performance
- Shared data subscriptions, with no duplicate calls for the same instrument.
- Lazy rendering of widgets off screen.
- Efficient 15-minute refresh and caching.

## Backend model
The workspace is a structured definition:
- `workspace_id`, `user_id`, `workspace_name`
- `widgets[]`: `widget_id`, `widget_type`, `position`, `size`, `follow_workspace`, `instrument`, `expiry`,
  `strike_range`, `timeframe`, `settings`
- `workspace_selected_instrument`, `created_at`, `updated_at`

Saved scanners stay separate, reusable objects that widgets reference.

## Final tests (all three must be true)
1. A beginner builds a useful workspace in under a minute.
2. An experienced trader builds a sophisticated multi-widget setup without constraint.
3. When one instrument is selected, the widgets behave like one intelligent system.
