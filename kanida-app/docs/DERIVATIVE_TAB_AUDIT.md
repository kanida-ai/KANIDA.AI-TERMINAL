# Derivative tab — UX/UI audit

Walked block by block in a browser, signed in, with a real instrument selected (TCS, a mid-liquidity
stock — deliberately not NIFTY, because NIFTY hides thin-data problems). Build: bundle 16:24,
19 Sep 2026. Everything below was seen on screen, not read from source.

---

## The map — what is on the tab, in order

| # | Block | Left panel | Right panel | Drives |
|---|---|---|---|---|
| 1 | **Screener** | 214 instruments, one row each | Signal by 15-min reading | Click a row → every block below |
| 2 | **Option chain** | Linked chart | Calls / strike / puts ladder | Click a strike → the chart |
| 3 | **OI by strike** | Call/put OI bars | OI + build-up table | Hover a strike → lights tab-wide |
| 4 | **ΔOI through the session** | Futures candles (15-min / daily) | 2×5 grid, ten at-the-money contracts | Click a tile → the chart |
| 5 | **PCR through the session** | PCR by OI and by volume | Latest reading + deltas | — |
| 6 | **Max pain through the session** | Max pain against spot, one scale | Latest reading + deltas | — |
| 7 | **IV through the session** | ATM implied volatility | 2×5 grid, same ten contracts | Click a tile → the chart |
| 8 | **Futures build-up** | OI vs 20-day average, and basis | Latest reading | — |

**Workflows that work.** Clicking a screener row repoints all eight blocks (verified with TCS —
chart, signal table, chain, OI-by-strike, ΔOI, PCR, max pain, IV and futures all followed). The
reading control moves the screener and the chain together. "At the money" returns the chain to spot.
Search narrows the rows. Sorting works on all ten columns. Every empty state explains itself.

---

## Findings, worst first

### 1. The Unusual cell carries thousands of characters of hidden text — FIX FIRST
NIFTY's Unusual cell contains **every reason for every one of its 80 flagged contracts**, concatenated:
*"volume 206.0x its own time-of-day median 1 · day volume 28.3x yesterday's OI 1 · volume 781.9x …"*
— roughly 4,000 characters in one table cell. On screen it is clamped to two lines, so it looks fine.
It is not fine: a screen reader reads the whole thing, the accessible name is unusable, and every row
carries this weight in the DOM.
**Fix:** summarise. "3 conditions across 80 contracts — highest: volume 782× its own median
(NIFTY2692223300CE)". Put the full list behind the row's own detail, not in the cell.

### 2. Scrolling over an inner table traps the page
With the pointer over the OI-by-strike table, the wheel scrolls that table and the page does not
move. A trader scrolling down the tab stops dead and has to move the mouse to continue. Reproduced
twice.
**Fix:** let the inner table hand the scroll back at its ends, or drop its own scroller and let the
block grow.

### 3. A direction chip can contradict its own delta
TCS PCR reads **0.52 · ↓ FALLING** beside **"No previous-session close is stored for this figure, so
there is nothing to measure against."** Max pain does the same: **2,200 · → STABLE** beside the same
sentence. Both are true — the chip measures movement *within* the session, the delta measures change
*since the previous close* — but nothing on screen says they measure different things, so it reads as
the screen arguing with itself.
**Fix:** say what the chip is measuring ("falling through the session"), or suppress it when there is
no baseline for the figure beside it.

### 4. "Refused readings — 34 readings" on a 26-reading session
The IV puts row reports 34 refusals when the session has 26 readings. Either it is summing across
contracts and labelling it per-contract, or it is counting something else. A count larger than the
number of readings is wrong on its face.
**Verify before changing:** it may be an aggregate across the five put tiles, in which case the label
is wrong rather than the number.

### 5. The IV grid is mostly gaps and says so quietly
Each tile reads **"9 of 26 readings solved"** — two-thirds of the line is missing, because the
previous session's readings carry no spot. The tiles draw a short line and the count sits in small
grey text.
**Fix:** when under half the readings solved, say so where the eye lands, not in the caption.

### 6. Five identical tiles carry no information
All five call tiles on TCS read *"Call writing increasing / Sellers are building resistance"*. It may
be true, but five identical labels side by side tell the reader nothing about WHERE it is happening.
**Fix:** let magnitude differentiate them — the strongest tile should be visibly the strongest.

### 7. Two different "as of" times on one screen
Screener and chain sit on **11:30**; PCR, max pain, IV and futures on **15:45**. Each is labelled and
each is honest — but the reader sees two timestamps on one page and has to work out why.
**Fix:** one line at the top of the tab explaining the two clocks, or bring them together.

### 8. Smaller things
- The Option chain block opens with an empty **"CONTRACT ON THE CHART — "** placeholder above the
  chart; it is a prompt occupying a header slot.
- Open interest renders as **3Cr** while premium renders as **₹14768 cr**. Same suffix, different
  meaning (contracts vs rupees), sitting near each other.
- The futures chart's "Daily candles" control is disabled on every symbol, because daily history has
  not been fetched for any contract. Correct, and it looks broken.

---

## What is good, and should not be lost in a redesign
- Every empty state explains itself instead of showing a blank.
- The unusual mark carries its reason rather than being a colour.
- IV is marked COMPUTED on every tile, with the rate stated as a code constant with no feed behind it.
- A figure with no baseline says so instead of showing a zero.
- The chain opens at the money and can be returned there.
- Colour is never the only carrier of meaning.
