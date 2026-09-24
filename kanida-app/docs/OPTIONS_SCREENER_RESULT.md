# Options Screener: build result (22 Sep 2026)

This is the result of the build against `OPTIONS_SCREENER_SPEC.md`. It uses the layout approved in
`OPTIONS_SCREENER_LAYOUT.md`, with the owner's recommendations on Q1–Q6.

Nothing was committed or deployed, and no package was installed. Ports 8082 and 8765 and the three KANIDA tasks
were not touched. No file under `src/derivative/` was edited, and neither were `derivatives`, `snapshots`,
`signal_noise`, `session_events` or `app.py`. `db/derivatives.db` and `var/intelligence.db` were only read.

## Try it now (dev, port 8092)
Open `http://127.0.0.1:8092/__dev/session?as=owner`. A second account is at `?as=member`, for testing privacy.

- The dev server uses throwaway dev accounts in `var/dev-screener/`. It never sees the real pilot users.
- The browser cookie is renamed to `kanida_dev_session`, so signing in on 8092 cannot log you out of 8082. I
  checked this with curl.
- Restart it with `scripts/start-screener-dev.ps1`, or add `-Build` to re-export `dist-screener/` first.
- It reads `db/derivatives.db` live, so today's readings appear from 09:30.

## Hooks to apply after 15:30 (owner)
1. **`server/kanida_pilot/app.py`**: add these two lines at the end of `create_app()`, just before `return app`:
   ```python
   from .screener import mount as mount_screener
   mount_screener(app,settings)
   ```
   `mount` places the screener's routes ahead of the `/api/{path}` and `/{path}` catch-alls. If the screener fails
   to start, the pilot logs it and keeps running without it.
2. **`src/shell/routes.tsx`**:
   - Add `{route:'/screener',label:'Options Screener',short:'Screener',icon:'filter'}` to `NAV_ITEMS`.
   - Add `path==='/screener'` to the list in `navActive`.
   - Until then, the page is reachable at `/screener`, because `app/screener.tsx` is a new route file.
3. **Rebuild and restart the pilot**: `scripts/start-pilot.ps1 -Build`, which rebuilds `dist-pilot`.
4. **Register the alerts job**: `scripts/register-screener-task.ps1` creates "KANIDA Screener eval".
   - It repeats every 5 minutes, like the F&O tasks. This machine runs on Pacific time, so there is no IST clock
     trigger.
   - A run with no new reading exits after one query. Remove it with `-Remove`.
5. The production store is `var/screener.db`. It is created on first start and seeded with the 14 defaults.

## What was built
**Server: `server/kanida_pilot/screener/`**

| Module | What it does |
|---|---|
| `vocab.py` | The closed vocabulary: 12 metrics, 19 state words, windows, strike ranges and delta bands. It is served to the builder. |
| `states.py` | Ports of `logic.ts` `gridDirection`, `gridPriceDirection` and `sessionDirection`. The words compose them with the tab's own 5% flat bands and `PACE_UP`/`PACE_DOWN` 1.25 / 0.75. There are no new thresholds. |
| `definition.py` | The one structured definition. It covers validation with plain-sentence errors, AND binding tighter than OR, grain, the digest and "Reads as". |
| `nl.py` | A deterministic sentence parser. It reports what it understood, what it assumed, and what it could not map. |
| `data.py` | A read-only session loader. It works point in time, and a missing reading is stored as NaN, never carried forward. |
| `greeks.py` | IV comes from `implied_vol.solve`, exactly as the IV route calls it. Delta and gamma use BSM at the solved IV. Results are cached. |
| `evaluate.py` | Evaluates every reading, contract grain or book grain. It re-reads the strike range at each reading, applies the expiry-day roll, and writes "Matched because" and "Ended because". |
| `lifecycle.py` | The states are New match, Still matching, Strengthening, Weakening and Condition ended. A re-match starts a new episode on the same card. A gap in the data neither extends nor ends a match. |
| `store.py`, `service.py`, `routes.py`, `__main__.py` | `var/screener.db`, result caching, deduplicated alerts, the API (owner-scoped, returning 404 for other users' scanners), and the job. |

**Web: `src/screener/` and `app/screener.tsx`**
- **Builder:** the sentence box, the scope chips, and condition chips (metric › side › state › time) that open word
  lists. AND/OR switches with a tap. "More options" holds strike steppers, delta bands and the liquid-only switch.
  "Reads as" comes from the server.
- **Results:** Active / Ended today / All views. Each card has a status badge, "Matched because", "Ended because"
  and today's timeline.
- **Scanner list:** KANIDA scanners and My scanners, with save, rename, edit, duplicate and delete.
- **Notifications:** three notify switches per scanner, and an alerts panel.
- **Refresh:** the page reads again when a new reading lands.

**Defaults (14):** these were seeded as data in `defaults.py`.
- Call / put OI building
- Call / put IV expanding
- Call / put premium surge
- Max pain higher / lower
- PCR building / falling
- Broad OI build-up
- Unusual activity near ATM
- Call / put writing near ATM

## Decisions applied
1. **Q1, premium surge.** It uses the pace rule: "Expanding rapidly" means rising at every reading and the latest move
   is more than 1.25× the one before. **Worth knowing:** on 21 Sep the matches this produced were accelerations, not
   big moves (BEL 380 CE +4% over 30 min). The owner's example of "+100% in two readings" would need a size rule,
   and that would be a new threshold. The owner should decide whether to add one.
2. **Q2, delta and gamma.** Both are computed and tagged COMPUTED, with only Increasing, Decreasing and Stable.
   Delta bands are available as a strike-range filter.
3. **Q3, Quiet.** It uses summary.ts `quiet`: premium flat and OI flat at every reading of the window.
4. **Q4, natural language.** The parser is deterministic. The owner's example sentence parses exactly (tested).
5. **Q5, alerts.** Alerts are in-app. They are raised only on transitions after the moment the user switched
   alerts on. The store's UNIQUE key means a transition can never notify twice.
6. **Q6, expiry day.** On the nearest expiry's own expiry day, IV, delta and gamma conditions read the next
   expiry, and a note says so. I made this call because I hadn't marked a recommendation on Q6.

## Found and fixed while building
- A contract with no row at a reading was being ended as "left the strike range". It is now a gap, per RULE 2.
- Pace compared a 30-minute move that spanned a missing reading with a 15-minute move, and so reported
  "strengthening". Pace now refuses to compare across a gap.
- "Reads as" lower-cased acronyms ("pCR"). It also printed a strike range for PCR and max-pain scanners, which
  don't use one.
- On phone, the scanner strip stretched to fill the screen, and Enter didn't submit the sentence box.

## Verification
- **Screener tests:** 74, in `tests/test_screener_*.py`. They include:
  - the fast engine checked against the reference rules at every reading of random series
  - a **point-in-time replay**: the status at reading i equals the status from a run that only has readings up to i
  - owner scoping, CSRF, and alerts that are deduplicated and not replayed
- **Full server suite:** 530 passed and 1 skipped. Nothing existing broke.
- **Parity:** `node scripts/check-screener.cjs` passes 61 of 61. It runs the TypeScript originals in `logic.ts` on
  the same fixture, and checks the constants against `logic.ts` and `summary.ts`.
- **Derivative tab:** `check-derivative.cjs` passes 453. `tsc --noEmit` is clean.
- **Real data (21 Sep, 216 underlyings, 25 readings):**
  - A full-session replay takes about 0.5–2 s per scanner.
  - IV scanners take about 20 s cold, while IVs are solved and cached, and are instant afterwards.
  - The job runs all definitions in about 30 s.
- **Browser, desktop 1400 px and phone 375 px, no horizontal overflow:**
  - sentence → chips → Run → Save as → edit a chip → AND to OR → save
  - duplicate a default → delete with confirmation
  - notify switches, alerts panel, and the timeline
  - a second account sees none of the first account's scanners

## Final design test
- **Can a beginner build a meaningful scanner without a number?** Yes. Picking a default and pressing Run, or typing
  "call OI building for 45 min", gives a working scanner. Every chip is a word, and no box asks for a threshold.
  The only numbers are strike offsets and delta bands, and they sit under "More options".
- **Can an experienced trader express real multi-condition behaviour?** Yes, within these limits:
  - up to 6 conditions, with AND/OR in one level of grouping
  - cross-side books ("call OI building AND put OI unwinding")
  - book conditions combined with contract conditions, and custom windows
  - ATM / OTM / ITM / delta strike ranges, liquid-only, and chosen symbols

  **Where they will feel restricted:**
  - no nested grouping beyond (A AND B) OR C
  - no size-based "big move" state (see Q1)
  - only the two captured expiries

## Known limits (honest)
- **Liquidity.** Many stock-option matches sit below the tab's ₹2 cr liquidity floors. Their cards say "Below
  liquidity floors", and "Liquid strikes only" removes them.
- **Default lists are long.** Contract-grain defaults over a whole day produce hundreds of ended episodes. The
  default view is Active, and the API pages results at 100.
- **Index list (out of scope, not edited).** `derivatives.INDEX_UNDERLYINGS` lists 3 indices, but the capture scope
  holds 5 (MIDCPNIFTY and NIFTYNXT50 as well). The screener uses all 5.
- **Parser limits.** The parser is rule-based. Phrasing it doesn't know is shown as "Didn't understand", never
  guessed.
