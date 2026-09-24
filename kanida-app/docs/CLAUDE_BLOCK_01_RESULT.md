# CLAUDE_BLOCK_01_RESULT — Block 1, built to the agreed claim policy

Live at `http://127.0.0.1:8082/derivative`. The screener (left) and the Signal by 15-min reading table
(middle) are untouched. The explanation is the third pane. No backend change, no deployment, no commit.

## The claim policy, and where it lives

Three tiers, each with a data precondition, written into `summary.ts` and enforced by
`scripts/check-derivative.cjs` so it fails the build rather than relying on memory.

| Tier | Precondition | Allowed |
|---|---|---|
| **1 · Observed** — "Call positions build at 2 more strikes" | field measured at both readings | headlines, ladder, session, everywhere |
| **2 · Qualified** — "Consistent with call writing, though spot also fell 3 points…" | supporting fields present **and** the competing explanation in the **same sentence** | body only |
| **3 · Attributed** — "Call writing at 23,450" | validated price attribution | **closed. Nothing emits one.** |

Tier 3 is closed because the rule does not control for the underlying's own move: a call's premium falls
when spot falls, whoever is trading. A delta-adjusted residual will improve that and will not close it —
Greeks are estimates, and an unexplained residual still does not say who initiated a trade.

One exception, made explicit: the ΔOI tile's own label is **quoted and attributed** in the evidence
("The ΔOI tile labels this "Call writing increasing""), because reporting what another surface says is not
saying it. The test allows a behaviour word only with a competing explanation **or** that attribution.

## Live output, 18 Sep 2026 session

**NIFTY · 25 Sep expiry · 11:15 → 11:30 IST**

> **Call positions keep building at the same strikes**
> Open interest rose and premium fell across 23,300–23,500 CE, measured against the 11:15 IST reading.
> Consistent with call writing, though spot also fell 3 points over the same interval, which may explain
> some of the premium change.
> Put strikes did not change materially this interval.
> Call positions began changing at 10:30 IST, 1 hour ago. 23,300 CE carries the largest change at this
> reading; the leading strike has changed during the episode.
>
> **OPEN INTEREST ADDED · 11:15 → 11:30 IST**
> 23,500 ▇ since 10:30 · 23,450 – no change · 23,400 ▇ since 10:30 · 23,350 ▇ since 10:30
> ── spot 23,302 ── · 23,300 ▇ since 10:30
>
> **CROSS-MARKET CONTEXT · 11:15 → 11:30 IST**
> Puts — Little change at the put strikes this interval.
> PCR — 0.86, fell from 0.88 — driven by call open interest rising, not the put side.
> Futures — Front contract open interest rose and basis widened this interval.

**TCS · 29 Sep expiry · 15:00 → 15:15 IST**

> **Call positions build at 2 more strikes**
> Open interest rose and premium fell across 2,100–2,140 CE, measured against the 15:00 IST reading.
> 2,120 and 2,140 CE are new this interval.
> Consistent with call writing, though spot also fell 9 points over the same interval…
> Put strikes separately show open interest rising at 2,020 and 2,060 PE.
> Call positions began changing at 14:45 IST, 30 minutes ago.

Three scans (14:45, 15:00, 15:15) report **30 minutes**.

**Session history**, every reading, no window: 11:30 keeps building · 11:15 largest addition moves ·
11:00 keeps building · 10:45 largest addition moves · 10:30 begins building · 10:15 and earlier
"Not enough readings yet to compare" — the 60-minute baseline, visible rather than hidden.

## What changed in this round

| Change | Why |
|---|---|
| Headlines are Tier 1 | The shipped panel was writing "Call writing holds…" — a bare behaviour claim |
| Interval baseline (t vs t−1) alongside the hour-wide episode, both ends named | A 15-minute product that only knows an hour cannot say what changed in the last fifteen minutes |
| **Interval gets its own flat-band yardstick** | Judged against the hour's peak, nearly every interval read as flat and the ladder was empty |
| Ladder: OI added over the named interval; spot between the strikes it falls between; three distinct marks for added / measured-no-change / not captured | A bar with no unit is unreadable, and one mark cannot mean both kinds of nothing |
| Cross-market rows, no ticks, each scoped | They are not independent votes on the strike story |
| PCR row names **which side moved** | A falling PCR does not establish puts thinning — it falls when calls rise |
| Futures row reports, never declares a conflict | Declaring disagreement needs the disagreeing behaviour named |
| "led throughout" gated on `lead_stable` | Otherwise it is a claim about intervals nobody checked |
| Quiet copy: outstanding OI is a total | Flat OI is consistent with equal closing and opening — not "the same positions remain" |
| "Last confirmed" on an unobserved reading | So held history cannot read as a current measurement |
| Max pain removed from the permanent row | Weakest of the three; it stays available in the blocks below |
| Session history expandable, whole session | No arbitrary window |
| Evidence dots removed | Strong evidence of OI rising is not strong evidence of writing |

**Bug found and fixed:** `Number(null)` is `0` and finite, so a missing measurement was reading as a
captured zero — the exact distinction the ladder exists to make. Absence is now checked before coercion.

## Tests

- `scripts/check-derivative.cjs` — **422 pass** (404 before this work). New assertions enforce: no behaviour
  word in any headline; every behaviour word carries its competing explanation or an attribution; both ends
  of the interval named; Tier 2 refused when spot is uncaptured; "led throughout" gated; quiet copy makes no
  claim about whose positions remain; three ladder marks kept distinct; PCR credits the right leg; futures
  declares no conflict; no forecast or directional vocabulary anywhere.
- `pytest server/tests` — **426 pass**, unchanged.
- `npx tsc --noEmit` — clean.
- `scripts/check-shapes.cjs` still fails — pre-existing, verified identical with changes stashed.

## Files changed

`src/derivative/summary.ts` (claim policy, interval baseline, ladder, cross-market),
`src/derivative/SummaryPanel.tsx` (ladder, cross-market, session history),
`src/derivative/ScreenerSection.tsx` and `index.tsx` (two existing series wired through),
`scripts/check-derivative.cjs` (policy assertions).

Two additional reads — `/api/derivatives/futures-buildup` and `/api/derivatives/pcr-series` — both routes
the blocks below already use. No new endpoint, no schema change.

## Known limits

- **Spot is only captured for 9 of 26 readings on this session.** Where it is absent the interpretation says
  so and does not stand bare. That is the honest path firing, not a fallback.
- **IV is still absent from the language.** Unchanged from before; needs solved IV stored at capture.
- **Real mouse input does not reach the browser tab in this session** — including pre-existing controls.
  Interactions were driven by dispatched pointer events on the same React handlers.
- **Mobile untested.** The app sits in a maximised window that will not resize.
- The attention list (events instead of 216 names) is **not built** — that replaces the screener and was
  not approved.

## Round 3 — the three panes the owner asked for

```
[ WHAT'S HAPPENING ][ SIGNAL BY 15-MIN READING ][ WHAT IS HAPPENING ]
       368px                   556px                    368px          at a 1366px window
```

**Left pane is new.** It answers *what is happening*, not *what exists*: one row per instrument with a
Tier-1 description, the busiest strike and how long it has run. The 216-name screener is the OTHER MODE of
the same pane — "All instruments" — with every column, filter, sort, reading control and Customize it has
today. Nothing was removed from it.

Live at the 15:45 reading of 18 Sep:

> NIFTY — Call positions keep building at the same strikes · since 10:30 · 23,300 CE largest
> HDFCBANK — Call positions begin reducing · since 15:15 · 730 CE largest
> TMPV — Call positions begin building · since 15:15 · 305 CE largest
> TATASTEEL — Call positions begin reducing · since 15:15 · 188 CE largest
> RELIANCE — No further call position change this interval
> ETERNAL — Call position reduction narrows to 2 strikes · since 14:15 · 330 CE largest
> 208 more instruments — open the full table

**What it costs, and it says so on screen.** Each instrument needs its own ΔOI grid, so it evaluates the
first 8 of the server’s own ranking and prints "Read from the first 8 of 216 instruments in the server’s
own order. The rest were not read, which is not the same as nothing happening in them." Evaluating the
whole book is a server-side events pass, computed once per reading and shared by every reader — not
something a browser should do once per user. That remains the first architecture item.

### Also in this round

| Change | Why |
|---|---|
| **Headline verb follows the open interest** | Live on TATASTEEL: "Call positions begin building" over a body reading "open interest fell". A reduction is now headlined as a reduction, and a check fails the build if the two ever disagree again. |
| Block height 320 → 430 | At 320 the screener showed 2 rows of 216, the signal table 5 readings of 25, and the explanation had 210px of body for ~530px of content |
| Even width split, floors at 350 | 1366 − gutters − the fixed 556 signal table − gaps leaves 750 for the other two. At 375 they wrapped on the exact tie |
| Reading strip removed from the explanation | It duplicated the screener’s own reading control, two panes to its left, and cost two rows |
| Controls on one row: Replay · Skip · Quiet · Why this? | Two rows of chrome in a 320px panel |
| **Search opens the instrument table** | The search sits in the block header and only filters the table; with the events list in front, typing appeared to do nothing |

Verified live: picking TATASTEEL from the events list repointed all eight linked blocks; the
"All instruments" toggle shows all 216 rows with the reading control and Customize intact; search switches
to the table and clears back. **423 frontend checks, 206 backend derivative tests, typecheck clean.**

## Round 4 — every instrument, computed on the server

The events pane read 8 instruments, because each one cost a ΔOI request from the browser. It now reads
**all 216**, because the walk moved to the server: one pass per 15-minute reading, handed to every reader.

> WHAT’S HAPPENING · **178 of 216 instruments are reading a behaviour**
> NIFTY — Call positions keep building at the same strikes · since 10:30 · 23,300 CE largest
> KPITTECH — Put positions begin building · since 15:15 · 500 PE largest
> ASHOKLEY — Call positions begin reducing · since 15:15 · 163 CE largest

### What was added

| File | Change |
|---|---|
| `server/kanida_pilot/session_events.py` | **new.** The walk, server side. A port of the verified client rule — no new threshold, no new vocabulary. |
| `server/kanida_pilot/derivatives.py` | `events()`: the whole book at one reading, in **five queries** rather than four per instrument, held for 60s because a captured reading never changes. |
| `server/kanida_pilot/app.py` | `GET /api/derivatives/events?at=&limit=` |
| `server/tests/test_session_events.py` | **the drift guard** — see below |
| `scripts/emit-session-walk-fixture.cjs` | regenerates the shared fixture from the TypeScript side |
| `src/derivative/WhatsHappening.tsx` | one request for the book, instead of N for a few |

**Measured:** 216 instruments, 2,160 contracts — **842ms cold, 0ms cached**. Was ~2.8s before the query
batching, and would have been 216 browser requests before that.

### The drift guard

The walk now exists twice: `summary.ts` for the selected instrument, `session_events.py` for the book.
That is a real risk, and it is held the way this repository already holds it elsewhere —
`server/tests/test_session_events.py` runs BOTH over one shared fixture
(`server/tests/fixtures/session_walk.json`, generated from the TypeScript side) and fails naming the
reading, the side and the field if they disagree on any of 13 fields at any reading. It also asserts the
four thresholds and the rule version are byte-identical to `logic.ts`.

Verified on live data: the list and the story produce the **same sentence** for the same instrument —
NIFTY "Call positions keep building at the same strikes", KPITTECH "Put positions begin building".

### A defect this surfaced

The events pass read every point up to the reading; the ΔOI block stops at the newest reading that carried
a **spot**. On 18 Sep, where NIFTY’s capture died at 11:30 and the rest was rebuilt from candles without a
spot, that put the list on 15:30 and the story on 11:30 — four hours apart, same instrument, one screen.
The pass now uses the block’s own anchor, per instrument.

**438 backend tests** (was 426), **423 frontend checks**, typecheck clean. Server restarted via
`scripts/refresh-pilot.ps1`, which refuses to replace a working pilot with a failed build.

## Round 5 — the middle pane: Simple / Detailed, Change / Level

Only the middle pane changed. Left and right panes untouched; the layout is the same three panes.

### Three conflicts settled before building

| The mockup wanted | Built | Why |
|---|---|---|
| Market Signal verdict (Strong Bullish/Bearish) as row colour | **dropped** | It contradicted its own row on this data — "no clear directional edge" labelled Strong Bullish; "more resistance" Bullish in one row, Bearish in another. Colour now carries the call-vs-put balance; the 15-Min Read carries the meaning. |
| `Price +38%` as the first pill | **the leading contract’s own premium** | At side level that is the summed premium of five strikes, which `logic.ts` forbids showing as a price — "nobody can trade their sum". The pill now reads a real contract, and its note names which. |
| An IV pill on every row | **kept, dashes when unsolved** | IV is solved on demand and was available for 9 of 26 readings on this session. The pill stays in place and reads — with the reason, rather than a zero. |

### What was built

| File | Change |
|---|---|
| `src/derivative/signal.ts` | **new.** One structured snapshot per reading — timestamp, both activity labels, all five metrics in change AND level, leading-strike detail, the 15-minute read, and the expansion text. All four presentations read this one object. |
| `src/derivative/SignalPanel.tsx` | **new.** Simple/Detailed, Change/Level, metric chips, inline row expansion. |
| `src/derivative/summary.ts` | ladder now covers both sides and carries the LEVEL behind each change. |
| `index.tsx`, `ScreenerSection.tsx` | max-pain and IV series wired at page level (existing routes); `SignalTable` → `SignalPanel`. |

**The window is fifteen minutes.** Every row is measured against the reading immediately before it, both
ends named. The hour-wide window the tab also keeps is what the right-hand pane uses for the EPISODE — two
windows, two questions, each saying which it is.

### Live, on the 18 Sep session

> **11:30 · Latest** · Strong ↑↑ Call writing increasing · Mild ↑ Put buying increasing
> More resistance than support; calls stronger
> Price −₹3.20 (−3%) · OI +22.6L · IV 0.0 · PCR −0.02 · Max Pain → 23,300

**Level** on the same row: Price ₹106.15 · OI 7.8Cr · IV 10.3% · PCR 0.86 · Max Pain 23,300.

**Detailed**, same row expanded: Option price Call −₹3.20 / Put +₹1.95 — "calls moved more than puts on
premium"; Open interest Call +22.6L / Put +4.8L; Implied volatility Call −0.1 / Put 0.0; Put/call ratio
0.88 → 0.86 (−0.02) — "the ratio moved on the call side; the put leg changed less"; Max pain 23,300 →
23,300 (unchanged); Strikes that moved, ranked.

### Nothing below the table

Verified: the pane ends on its last data row. Every colour, every strength word, every metric definition,
the Change/Level difference, the Simple/Detailed difference and the comparison window are the seven
sections of **How to read this** — including the sentence that no colour here means bullish or bearish.

**434 frontend checks** (was 423). 11 new: metric order and the word "Price"; newest-first and one latest
row; the 15-minute window on every row; the price pill traceable to a contract and never a basket; IV
dashing with its reason and splitting into legs where solved; PCR and max pain labelled whole-book;
unchanged max pain reading as an arrow not a zero; no direction word anywhere; an uncovered reading not
reading as quiet; and the guide containing all four colours and the not-a-direction statement.

**A bug this surfaced, for the third time:** `Number(null)` is `0` and finite, so an unsolved IV was
reading as a `0.0` change rather than a dash. `num()` in both `signal.ts` and `summary.ts` now checks
absence before the coercion, with the reason written above it.

---

## Round 6 — the middle pane rebuilt as market intelligence

**What was wrong with what I built in Round 5.** The pane printed one row per reading. On the 18 Sep
NIFTY session that was twenty-six rows in which the read column said *"More resistance than support;
calls stronger"* four times running, Max Pain said `→ 23,300` on every line, and PCR moved by two
hundredths. About 85% of the cells repeated the row above them. I had already computed *what changed
this interval* — `observe()` knows which strikes joined, left, shifted, slowed or broadened — and used
none of it here. A row per reading is the wrong unit when most readings are unremarkable.

**What the pane is now.** The state is computed first and explained second. `signal.ts` no longer emits
a display row; it emits one intelligence object per reading, and the screen, the sentences and anything
downstream all read the same fields.

### 1. The object (`SignalState`, `src/derivative/signal.ts`)

Per reading: `state` and `state_change`; `side` / `row` / `location` / `strike_range` /
`leading_strike`; `persistence_since` / `persistence_minutes` / `consecutive_readings`;
`breadth_previous` / `breadth_current` / `breadth_direction` / `breadth`; `price_change`,
`price_change_pct`, `price_level`, `price_contract`; `call_oi_change` / `put_oi_change` and their
levels; `call_iv_change` / `put_iv_change` / `iv_change` / `iv_level` / `iv_reason`; `pcr_*`;
`max_pain_*`; `supporting_evidence[]`; `conflicting_evidence[]`; `opposing_side_state`; `regime`;
`tags[]`; `key_strikes[]`; `plain_language_read`; `plain_language_detail`.

**Two windows, and each says which it is.** Every Δ on screen is the INTERVAL — this reading against the
one immediately before. Persistence is the EPISODE — the tab's existing hour-wide rule. Mixing them is
how a panel comes to claim an hour of persistence from a fifteen-minute move; they are computed
separately and a test holds `10:15 → 10:30 → 10:45` at **30 minutes**, not 45.

**The closed vocabulary.** Appeared · Building · Held · Strengthened · Broadened · Concentrated ·
Shifted · Slowed · Fading · Reversing · Balanced · Mixed — plus two that describe the DATA rather than
the market: **Not observed** (the reading was taken but carried nothing comparable) and **No baseline
yet** (the hour-wide comparison is not full). A test fails the build if any state outside the list
reaches a reader, and a second one fails if the guide does not explain every word in it.

### 2. The collapse (`periods()`)

Consecutive readings in the same state become one period carrying the number of readings it covers.
Nothing is deleted: the period holds every reading it spans, and opening it lists them newest first with
the full breakdown under each. A test asserts the reading count survives the collapse exactly.

On the live 18 Sep NIFTY session, nine readings became **five periods**: SHIFTED 11:15 · HELD 11:00 ·
SHIFTED 10:45 · MIXED 10:30 · NO BASELINE YET 09:30→10:15 (4 readings · 60 min). A quiet session is
allowed to look quiet, and this one mostly does.

### 3. The screen (`SignalPanel.tsx`)

The latest reading is the card: state badge, `11:15 → 11:30 · 15 min`, persistence and regime on the
right, a 16px headline, a two-sentence read, at most four qualifying tags, the key strikes, and the five
metric boxes in the order Price → Δ Open interest → Δ IV → Δ PCR → Max pain. Deltas by default; levels
live in the inline expansion, so the permanent Change/Level toggle is gone. So are the Simple/Detailed
tabs — the detail is the same object printed under the line that summarised it, not a second dashboard.
Nothing sits below the timeline; every explanation is behind **How to read this**.

Colour: green = increasing/building, red = decreasing/weakening, grey = unchanged, amber = slowing or
conflicting. A test asserts the guide states, in those words, that none of it means bullish or bearish.

### 4. What it may claim, and what it must say against itself

This is the one place I have moved from the Round 4 position, and it is worth stating plainly. That
policy said a Tier-1 headline may never carry a behaviour word. The approved design does carry one —
*"Call writing held across 23,300–23,500 CE"* — so the qualification now binds at CARD scope rather than
sentence scope: **a reading that names a regime must carry `conflicting_evidence`, and a test fails the
build if one is printed with nothing against it.** Live, that reads:

> **Call writing held across 23,300–23,500 CE**
> Call open interest rose 22.6L over the interval. Premium at 23,300 CE fell −₹3.20. *Implied volatility
> is unchanged, so it is not confirming the move. Put activity remains weak over the same interval.*

The conflict is not a footnote — supporting and conflicting evidence render in the same list, same
weight, and the expansion shows both for every reading.

### 5. Four contradictions the live walkthrough caught, all mine

These were in my build, found by reading the real page rather than the fixture:

1. **A value printed above the words "Not captured."** The Δ Open interest box showed `+8.1L` and
   captioned it *Not captured*, because the caption keyed off a side that no-baseline readings do not
   have. The caption now always names which side the figure is and what the other did; the value dashes
   on its own when nothing was measured.
2. **Max pain did the same** — `23,300` above *Not captured*. The level was there; the COMPARISON was
   missing. It now reads *No earlier reading to compare*.
3. **"No comparison" sat above measured interval figures.** The badge is about the hour-wide window, the
   numbers are about the fifteen minutes, and both were true — but together they read as a bug. The
   label is now **No baseline yet**, the headline says *the hour-wide comparison is not available yet*,
   and the guide explains that the two windows answer different questions.
4. **The wrong side, captioned confidently.** At a no-baseline reading the box defaulted to Calls and
   called a LARGER put move "the other side". It now follows whichever side actually moved more.

I also retitled the expansion's fact list from *"What each figure stood at"* to *"Behind this reading"*:
two of its rows are per-leg changes, and a heading promising levels over a list holding both is the
panel misdescribing its own numbers.

### 6. Verification

| | |
|---|---|
| Frontend logic | **438 checks passed** (`scripts/check-derivative.cjs`) |
| Backend | **438 passed** (`server/tests`, run from `server/`) |
| Typecheck | clean |
| Live | authenticated Chrome tab, 18 Sep NIFTY, three panes at 1366 CSS px |

New checks cover: the closed vocabulary; persistence as time between stamps and never a score; breadth
counted with its direction; the price figure naming one real contract; absence dashing with a reason
rather than reading as zero; PCR and max pain as whole-book figures over the same interval; a regime
never printed without its conflicts; no banned direction word anywhere in the object; the headline
agreeing with the open interest; the collapse preserving every reading; a hole not reading as quiet; and
the guide explaining every state word and all four colours.

Pre-existing failures, unchanged and unrelated to any file touched here: `check-model.cjs`,
`check-agent.cjs`, `check-shapes.cjs` (none of their subjects are modified in this branch).

### 7. Not done, and why

- **Mobile/narrow was not exercised.** The owner's window is maximised and does not resize; the pane
  wraps its metric boxes and tags, but that is unverified on a real narrow viewport.
- **The left and right panes are untouched**, as the brief required.
- **Persistence tops out near an hour** because the episode window is 60 minutes (max 120). "Persistent
  · 1 hr" therefore means *at least the whole window*, not *exactly one hour*. That limit is the tab's,
  not this pane's, and the guide states how the count works.

---

## Round 7 — the first hour of a live session

**The defect.** Simulating readings arriving one at a time showed the pane stalling for the first hour of
every trading day:

```
1 reading  -> no_baseline  | "The hour-wide comparison is not available yet"
2 readings -> no_baseline  | "The hour-wide comparison is not available yet"
3 readings -> no_baseline  | "The hour-wide comparison is not available yet"
4 readings -> no_baseline  | "The hour-wide comparison is not available yet"
5 readings -> appeared     | "Call buying appeared at 23,350 CE"
```

09:30, 09:45, 10:00 and 10:15 IST all read the same placeholder; the hero only became useful at 10:30.
It was not false — the hour-wide window genuinely is not full — but the FIFTEEN MINUTES are fully
measured, and the pane was withholding a reading it already had.

**The fix, and the line it does not cross.** Where the episode has no baseline but the interval carries a
directional measurement, the INTERVAL supplies the state, the side, the strikes and the leading strike.
Persistence is **withheld, not borrowed**: `persistence_since`, `persistence_minutes` and
`consecutive_readings` stay null, a new `persistence_known:false` rides on the object, the card prints
*"Too early to judge persistence"* in amber, and the missing window is named in `conflicting_evidence` —
*"This is the last fifteen minutes only. The hour-wide window is not full yet, so how long this has been
running cannot be judged."* A fifteen-minute move may describe itself; it may never claim an hour it has
not run for.

**Three further defects the live walkthrough caught, all in this round's own work:**

1. **The opening reading claimed an interval it does not have.** At 09:30 there is no previous reading at
   all — a different absence from an hour-wide window that is merely not full. It now reads *"First
   reading of the session — nothing to compare it against yet."*
2. **It called the far side "weak" with nothing measured.** `otherOi==null` fired the weakness claim and
   the `Puts weak` tag at the opening reading, stating a comparison that was never made. Both now require
   a measured subject side.
3. **The same book gave two answers.** On the real 18 Sep data at 09:45, calls +64.7L against puts +56.3L
   — 1.15× — headlined as *"Call writing"*, while the episode path calls that shape *"Both sides
   active"*. The fallback now applies the episode's own 1.25× balance test, returns `mixed`, and names no
   behaviour. Verified live: the card now reads **MIXED · Both sides active across 23,300–23,500 CE**.

### Verified on real data, not a fixture

Driven through the authenticated app at the 09:45 reading of 18 Sep:

```
MIXED   09:30 → 09:45 · 15 min          Too early to judge persistence
Both sides active across 23,300–23,500 CE
Call open interest rose 64.7L over the interval. Premium at 23,300 CE fell −₹1.35.
This is the last fifteen minutes only. The hour-wide window is not full yet, so how
long this has been running cannot be judged.
[Broader activity]   KEY STRIKES 23,300 CE Highest activity · 23,350/23,400/23,450 Increasing

EARLIER TODAY · 1 state before this reading
09:30  NO BASELINE YET   First reading of the session — nothing to compare it against yet
```

| | |
|---|---|
| Frontend logic | **444 checks passed** (was 438; six new blocks) |
| Backend | 88 passed on the drift guard + derivatives suite; unchanged by this round |
| Typecheck | clean |

New checks: reading one claims no interval and no weak far side; readings two to four read off the
interval with `persistence_known:false`, null elapsed time and the missing window named; the episode
takes over at reading five with 0 then 15 minutes; a genuinely flat opening stays `no_baseline` and
prints no persistence line; positions being closed in the first hour read as `reversing`, not a build;
and a balanced book is `mixed` with no regime inside the first hour exactly as outside it.
