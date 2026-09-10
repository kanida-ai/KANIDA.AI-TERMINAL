# Pathfinder — Build Session S3: THE SWIPEABLE FEED (Instagram-grade, clarity-first)

> Fresh session in this repo. Read `CLAUDE.md`, **`docs/sessions/PATHFINDER.md`** (LOCKED spec — the feed
> principle: *front-load clarity, preserve depth, keep discovery alive*), **`docs/handbacks/PF-S1.md` +
> `PF-S2.md`** (the real feed + experiment contracts you consume), **`docs/FRONTEND_SPEC.md`** (KANIDA design
> DNA), and **`docs/design/mockups/PLACE_MOCKUPS_HERE.md`** (the elevation direction). If PNGs exist in
> `docs/design/mockups/`, they are the baseline visual identity. **The mockup is the baseline; Instagram
> is the bar.**

## Goal
Rebuild the Pathfinder surface in the existing Expo app (`kanida-app/`) as a **full-screen, vertically
swipeable story/reel feed** — one finding per screen, immersive, 5–10 second reads — that delivers
**clarity in the first 2–3 stories** and **endless optional depth** after. Ship on **one Expo codebase → iOS
+ Android + responsive web.**

## The interaction model
- **Swipe up** → next discovery (the feed, ranked by S1/S2: what matters now first, then more).
- **Swipe / tap deeper** on a finding → its evidence (provenance chips: same stock / peer / sector / market ·
  n · period · regime · cost hurdle), related stocks, experiment history (versions, trial counts,
  expected-vs-actual), and Pathfinder's follow-up questions.
- **Clarity contract**: leave after 30 seconds → you have "what matters now". Everything after is chosen depth.
- **No padding**: render exactly what the engine published (a 4-story day is a 4-story day).

## Story types (all from real S1/S2 data)
Theme-in-play (proof + watchlist) · stock behaviour (dip/surge) · volume anomaly · relationship · debunk ·
**experiment story** (*I noticed → I tested → history showed → I decided to test with virtual money → what
happened → what I learned and changed → what I'm testing next*) · **verdict / scoreboard** (Right · Wrong ·
Inconclusive · n) · watchlist theme · learnings.

## Honest-attractive, in the UI (enforce)
Every number is a computed `{{fact}}` with tappable provenance · died/failed experiments shown as fully as
wins · small samples labelled · no promises · **NO user-facing entry / target / stop / execution on the
research surface** (compliance) · experiment baskets show sector/theme + evidence publicly, constituents only
under RA review.

## Craft — raise to Instagram's bar
Smooth vertical paging, progress indicators, tasteful micro-motion, premium tabular typography, KANIDA's dark
ground + violet-Pathfinder identity (per FRONTEND_SPEC), addictive but calm — **never a quant terminal**.
Loading skeletons, honest empty states, error states naming the base URL.

## Scope — OUT
Trader/Investor screens (agent-agnostic shell only) · auth · payments · analytics.

## Reuse
`kanida-app/` shell (routing, theme tokens, `facts.tsx` token resolver, `honesty.ts`, API client with the
single base-URL seam) · S1 `/api/pathfinder/feed` · S2 experiment endpoints.

## Done when
- The swipeable feed renders **real S1/S2 data** on iOS, Android, and web; first 2–3 stories give clarity;
  swipe-deeper works; story types above render; honesty rules enforced in the UI.
- `npm run typecheck`, `npx expo lint`, `npm test`, and the contract tests pass against the live API.
- **UX verification performed**: drive it in a browser at phone size (375px) and desktop, dark theme, and
  confirm the flow end-to-end — do not claim it works without looking.
- Committed locally on `feat/product-build` (never push/main). Hand-back `docs/handbacks/PF-S3.md`,
  including what could not be pixel-matched because mockup PNGs were absent (if so).
