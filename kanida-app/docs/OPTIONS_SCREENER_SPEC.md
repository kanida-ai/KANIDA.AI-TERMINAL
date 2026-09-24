# KANIDA Options Screener — owner spec (saved 2026-09-22)

Source: owner brief, 22 Sep 2026. This file is the requirements record. The layout and field map live
in `OPTIONS_SCREENER_LAYOUT.md`. No code is written until the owner approves that layout.

## Build constraints (22 Sep 2026, while markets are open)
- Don't restart, rebuild into, or stop the pilot on :8082, the scanner on :8765, or the scheduled tasks
  "KANIDA F&O capture", "KANIDA F&O metrics" and "KANIDA Equity prices". Develop on a separate port
  (8092) with its own dist folder. The 8082 restart happens only after 15:30 IST, with the owner's OK.
- Don't connect to Global Datafeeds (`market_data/gdf_*`). Another session holds the only allowed connection.
- Don't edit `src/derivative/*` or `server/kanida_pilot/{derivatives,snapshots,signal_noise,session_events,app}.py`.
  Add new modules instead. List any app.py hook for the owner to apply after close.
- `db/derivatives.db` and `var/intelligence.db` are read-only. The screener's own store is `var/screener.db`.
- No commits, pushes, deploys or package installs.
- State words must reuse the interpretation logic in `src/derivative/signal.ts`, `logic.ts` and `summary.ts`.
  Don't invent new thresholds.

## Summary
An AI-native, user-configurable options screener for Indian index options and NSE F&O stock options.
Users describe market behaviour in simple words, and KANIDA interprets the numbers underneath.
A condition reads as metric → (side) → state → (strike range) → time window, never as "IV change > 5%".

## Coverage
- Indian indices and NSE F&O stocks, all available expiries, calls and puts simultaneously.
- Default strike universe is ATM ±5 strikes. The user can customise it.

## Data and time
- A new snapshot arrives every 15 minutes. All conditions are evaluated on those snapshots only.
- Windows: last 15, 30, 45 or 60 min, since market open, or a custom intraday window.
  Also a count of readings (for example, 3 readings). Never claim finer resolution than 15 minutes.

## Condition builder
Flexible shapes:
- Metric → State → Time
- Metric → Side → State → Time
- Metric → State → Strike range → Time

Examples:
- OI → Calls → Increasing continuously → 45 min
- IV → Puts → Expanding rapidly → 30 min
- Max Pain → Shifting higher → Since open
- Premium → Calls → Rising continuously → 3 readings
- PCR → Increasing → 45 min

Conditions combine with AND / OR, kept visually simple. There is no programming-style rule builder in the default UX.

## Metrics (only useful and available ones; no padding)
Premium, IV, change in IV, OI, change in OI, volume, delta, gamma, PCR, change in PCR, max pain,
max-pain shift, underlying movement, and other relevant fields already in the KANIDA data model.

## State vocabulary (words always visible; arrows optional)
Increasing, decreasing, increasing continuously, decreasing continuously, expanding rapidly, contracting
rapidly, stable, reversing higher, reversing lower, unusually active, quiet, broadening across strikes,
concentrating at fewer strikes, shifting higher, shifting lower.
Examples: ↑ Increasing · ↓↓ Decreasing rapidly · → Stable.

## Interpretation
No user thresholds. States are read contextually from the 15-minute readings, considering:
- the change from the previous reading
- consecutive readings
- the move since open
- the instrument's own intraday behaviour
- neighbouring strikes
- call versus put behaviour

For example, "OI increasing continuously for 45 min" means the OI rose across the three applicable readings.

## Natural language
The user types a request, for example: "Show me F&O stocks where call OI has been increasing for the
last 45 minutes and call IV is expanding." It becomes visible, editable conditions.
Natural language and the visual builder produce the same underlying scanner definition.

## Results
- Return matches only. Never label them Buy, Sell, Good, Bad or Best trade.
- Explain why each one matched. For example:
  "NIFTY · 24,500 CE — Matched because call OI has increased across the last three readings, IV is
  expanding, and activity remains concentrated around ATM to ATM+2. First matched: 10:15.
  Status: Still matching."

## Match lifecycle
New match → Still matching → Strengthening / Weakening → Condition ended.
Don't re-show a continuing match as new. Keep the first-matched and ended times.

## Default scanners (at least 10; data-defined, not hardcoded)
1. Call OI building
2. Put OI building
3. Call IV expanding
4. Put IV expanding
5. Call premium surge
6. Put premium surge
7. Max pain moving higher
8. Max pain moving lower
9. PCR building
10. Broad OI build-up: activity across several nearby strikes, not isolated to one strike

The owner also asked for scanners such as:
- OI increasing continuously on the call or put side for the last 45 min
- Something happening in PCR
- Option premium on the call or put side up about 100% over the last two 15-min readings, or over any
  consecutive 15-min readings

## Alerts
Available on any scanner:
- notify on a new match
- notify when a match ends
- notify on a material state change

Never send the same notification every 15 minutes when nothing has changed.

## Saved profiles
Save, name, edit, duplicate and delete scanners. Each scanner belongs to the user and is private until
sharing is added later. Scanners built manually and scanners built from natural language are both persisted.

## Intraday history (current session only)
For each match: when it first matched, how long it matched, how many readings matched, whether it is
still active, and when it ended. There is no multi-year backtest in this version.

## UX standard
Beginner-simple, powerful underneath. Visual, clean and conversational.
- No numeric boxes, formulas, query languages, excess dropdowns, dense tables, or dozens of visible parameters.
- Show complexity only on request.

## Backend
Every scanner is a structured definition with:
- instrument universe
- expiry
- strike range
- metric
- side
- state
- time window
- AND/OR relationship
- notification settings
- profile owner

The one definition serves the visual builder, natural-language creation, the 15-minute evaluation,
notifications, saved profiles and the APIs.

## Final design test (both must be true)
1. A beginner can build a meaningful scanner without knowing a single numeric IV/OI threshold.
2. An experienced trader can express a useful multi-condition behaviour without feeling restricted.

The product should feel like "Tell KANIDA the market behaviour you want to find", not "Program a
traditional screener".

## Next step (owner)
A visual mockup of the builder and results screen. The idea succeeds or fails on how simple
condition-building feels.
