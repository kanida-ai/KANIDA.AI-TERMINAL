# CLAUDE_UX_LIVE_RESULT — authenticated Chrome walkthrough

Performed 20 Sep 2026 against the signed-in app in the owner's Chrome profile
(`http://127.0.0.1:8082/derivative`). No production code, filter, database or credential was changed.
Starting state (NIFTY · 15:45 IST · scroll top · 0 filters) was restored and verified before exit.

## How the session was reached

`list_connected_browsers` → one local Chrome (`e31ea5d7…`, in use). The connector exposes only its own
MCP tab group, and none existed, so a tab was opened in **that same Chrome profile** and navigated to the
URL; the profile's session cookie authenticated it immediately. **Codex was right and my UX01 note was
wrong**: the built-in browser's `SIGN_IN_REQUIRED` was an artifact of a separate browser profile, not
evidence the app was inaccessible.

**Viewport actually available: 1517 × 665 CSS px** (physical 1366 × 768, devicePixelRatio 0.9).
Not 1440 × 900 — wider, and 235px shorter, so the fold findings below are stricter than the target.

## Correction to CLAUDE_UX_01_RESULT

UX01 said the context bar's Reading "is false for six of nine blocks". **On the default view that is not
what the app shows.** At NIFTY · 15:45 the block headers read:

| Block | as-of |
|---|---|
| Screener, OI by strike, PCR, Max pain, IV, Futures build-up, Index dashboard, Futures OI build-up | 15:45 IST |
| ΔOI by strike (and the signal table) | **11:30 IST** |
| Option chain | none until a contract is picked |

The divergence appears **on interaction, not on arrival** — see finding 1. The source defect UX01
identified is real; its live symptom was described wrongly.

## Findings — verified by interaction

1. **Changing the reading desynchronises the page.** Selecting 11:30 from the reading control moved
   Screener, Option chain, OI by strike and ΔOI to 11:30. PCR, Max pain, IV, Futures build-up, Index
   dashboard and Futures OI build-up **stayed at 15:45** — a 4h15m gap, with the context bar asserting
   11:30 over all of it. On HDFCBANK at the default reading the ΔOI block sat at 15:15 against 15:45
   elsewhere, unexplained.
2. **The page is 6,856px tall in a 608px scroller — 11.3 screens.** Measured block origins are 231, 699,
   1381, 2063, 2745, 3428, 4110, 4792, 5474, 6156: **every block is exactly 682px regardless of content.**
   The PCR readings panel fills roughly 60% of its box; the rest is empty.
3. **Scroll trap reproduced, with a control.** Inner signal-table scroller at its maximum (120 of 120);
   five wheel ticks over it moved the page 0px. The identical gesture 400px away moved the page 500px.
4. **The signal table contradicts itself, verbatim from the live page:**
   - 15:00 — "Both sides building similarly — no clear directional edge." → **Strong Bullish**
   - 14:45 — same sentence → **Strong Bullish**
   - 14:15 — "Resistance strengthening" → **Bullish**
   - 15:15 — "Resistance increasing + support weakening" → **Strong Bearish**
   "More resistance" maps to Bullish in one row and Bearish in another, and "no directional edge" is
   labelled Strong Bullish. This is the prior audit's P03/P04 still live.
5. **Calls are red in one block and green in every other.** `OiByStrikeSection.tsx:20` sets
   CE `#F17D87` / PE `#39E5A3`; `frame.tsx:23` sets call green / put red. Green and red simultaneously
   carry bullish/bearish in the signal table. Two colours, three meanings.
6. **The screener needs sideways scrolling on a full desktop:** clientWidth 893 vs scrollWidth 1485
   (592px of overflow), and the instrument-name column is not pinned above 760px — scroll right and you
   lose which row you are reading. About 3–4 of 216 instruments are visible at once.
7. **The reading control eats the panel.** It opens as an inline strip of ~26 time chips inside the
   screener, pushing the rows down to roughly two visible.
8. **Placeholders occupy prime space.** Option chain opens with a full-width headline reading
   "CONTRACT ON THE CHART —" plus a prompt, beside an empty CHART pane.
9. **Five identical ΔOI tiles.** All five NIFTY call tiles read "Call writing increasing / Sellers are
   building resistance"; they wrap 4 + 1 and the put band sits below the panel's own fold.
10. **Rounding hides the ratio.** PCR 1.13 is shown above "TOTAL CALL OPEN INTEREST 18.1Cr" and
    "TOTAL PUT OPEN INTEREST 18.1Cr". `Cr` also means rupees-crore in the premium column two blocks up.
11. **Data gaps, stated honestly but placed quietly.** At 15:45 the whole PREMIUM ₹CR column is dashes and
    every row reads "Nothing flagged"; at 11:30 the same screener is fully populated (NIFTY "2 conditions
    · 80 contracts", ₹14768 cr). IV reports "9 of 26 readings carried a value". Put OI-change and
    build-up are dashes across the whole option chain while populated in OI by strike.

## Working — preserve in any redesign

- **Symbol propagation is solid.** Clicking HDFCBANK repointed all eight linked blocks in one pass.
- **Expand/restore works:** page collapsed 6,856 → 1,142px with a "Show every widget" chip back.
- **Disclosures are accessible:** opens as a dialog, Escape closes it, focus returns to the trigger.
- **The filter sheet is honest** — server-validated, and it says so rather than hiding rows client-side.
- **Capture health is truthful and reactive:** amber "Partial capture" at 15:45, green "Capture complete"
  at 11:30, with an amber line naming the chosen reading and the newest one held.
- **The unusual cell is fixed** — now "2 conditions · 80 contracts", not the old text bomb.

## Not verified

- **Mobile layout of the real app.** The tab sits in the owner's maximised 1366×768 window;
  `resize_window` reports success but `outerWidth` stays 1366, so no narrow viewport could be produced
  without disturbing the owner's window. Mobile is therefore **untested in the live app** and was
  exercised only in the standalone prototype.
- Watchlist and expiry filters, every column sort, deliberate error injection, and the 1440×900 fold
  exactly (the available viewport was 665px tall).
- Screenshots were reviewed in-session; none were written into the repository.
