# CLAUDE_UX_02_RESULT — market story approval prototype

## Entry path

`kanida-app/docs/prototypes/market-story/index.html` — open directly in a browser. Standalone: no
network, no fonts, no build, no pilot authentication. Assets: `assets/story.css`, `assets/scenarios.js`
(fixtures), `assets/story.js`.

## What it implements

**Context** — instrument, expiry (+DTE), session, reading and mode on one line, with a capture chip
that turns amber on the partial-capture scenario. All times labelled IST. A persistent dashed
`Illustrative design preview — not live market data` tag sits in the header. The only live controls are
the scenario selector, the timeline, the quiet-motion toggle, the evidence disclosure and the chain
drawer; the three unbuilt inspect destinations are `aria-disabled` and read "Not in this preview".

**What matters now** — behaviour kicker, breadth chip, headline, two plain sentences, a persistence
line, and an always-visible amber inference caveat. No bullish/bearish vocabulary appears anywhere in
the prototype (verified by text scan across all 18 readings).

**Calls and puts** — independent cards (behaviour, location, leading strike, breadth, first detection,
duration, consecutive readings, and a note), with **Combined — read last** third. Conflicts are carried
into the combined card rather than resolved. Calls are mint, puts periwinkle, amber is uncertainty only:
one meaning per colour, fixing the live app's call-red/call-green contradiction.

**Where it happened** — SVG spot line with a rupee axis (the strike ladder doubles as its ticks) and an
IST time axis. Premium, OI and IV are never on that axis. The line stops at the selected reading and
breaks at a capture gap rather than bridging it. A compact strike-participation grid shows which strikes
joined at which reading, with the ΔOI for the selected reading beside each.

**Timeline** — every reading selectable; selection re-renders narrative, sides, chart annotations,
participation and evidence from one object. Readings after the selected one show their time and "not
read yet" — no later state leaks backwards.

**Why KANIDA said this** — collapsible facts / inference / baseline / uncertainty / numbers table.

**Motion** — `Replay explanation` reveals the sentences in word groups, traces the segment from first
detection with a travelling point, then draws a hand-drawn bracket around the strike cluster and an
underline under the leading strike. Measured 3.3s end to end. `Skip` ends it instantly; changing reading
or scenario cancels it; the quiet toggle and `prefers-reduced-motion` skip it entirely. A screen reader
gets the completed explanation once via a polite live region, never word by word.

## Verification (Claude's built-in browser, real measurements)

| Check | Result |
|---|---|
| 1440×900 | context 0–76, story 127–555, chart 127–625, **calls 676–935, combined 676–792** — story and both side summaries in the first viewport; page 1,421px total |
| 390×844 | no horizontal overflow, **zero nested scrollers**, sides collapse to Calls/Puts tabs, timeline shows latest 3 with "Show the full day", context not sticky |
| Scenarios | all 4 × all 18 readings render; conflicts appear only where the fixture defines them |
| Replay | word groups → trace → bracket in 3.3s; skip, cancel-on-change, quiet motion all verified |
| Evidence / drawer | 5 blocks; drawer opens with 5 ladder rows, moves focus, closes on Escape |
| Clipping / console | 0 overflowing nodes, 0 console errors |

**Defect found and fixed during testing:** the participation grid and timeline originally revealed later
readings' state while an earlier reading was selected. Both now withhold it.

## Known limitations

- Fixtures only, and deliberately so: no claim is made that this output matches any API. Per the review
  correction, `market_intelligence.py` is treated as unfinished and unapproved, and bid/ask, Greeks,
  historical baselines, freshness and coverage are **not** assumed available.
- Desktop verified by DOM measurement plus a scaled screenshot; the preview pane is 391px wide, so the
  1440px layout was not seen at 1:1. Mobile was seen at native resolution.
- One instrument, one expiry, four illustrative sessions. No search, filters, saved views or alerts.

## Files changed

Added: `docs/prototypes/market-story/index.html`, `assets/story.css`, `assets/story.js`,
`assets/scenarios.js`; `docs/CLAUDE_UX_LIVE_RESULT.md`; this file. No application or backend code
touched.

See `CLAUDE_UX_LIVE_RESULT.md` for the authenticated walkthrough that shaped this design — in
particular the reading-desynchronisation, the 6,856px uniform-block page, the reproduced scroll trap and
the self-contradicting signal rows.
