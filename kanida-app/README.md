# `kanida-app` — the KANIDA.AI client

**ONE Expo codebase → iOS app · Android app · responsive web (desktop web + mobile web).**
There is no `.web.tsx`, no `.native.tsx` and no platform branch on a screen anywhere in `src/`.
The same routes, components and tokens serve all three targets.

Session **S3** rebuilt the Pathfinder surface as a **full-screen, vertically swipeable story feed**
against the real S1/S2 research contract (`GET /api/pathfinder/feed`, `/experiments`,
`/experiment/{id}`): one finding per screen, clarity in the first two or three stories, endless
optional depth after, nothing padded.

---

## Run it

```bash
# 1. the research API (S1 feed + S2 experiments, served from the append-only stores in var/)
cd ../backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010
#   (PowerShell:  $env:KANIDA_PATHFINDER_SOURCE="research"; python -m uvicorn pathfinder.mock_app:app --port 8010)

# 2. the app
cd ../kanida-app && npm install && npm start
#   press w → web        (also: npm run web)
#   press i → iOS        (also: npm run ios)
#   press a → Android    (also: npm run android)
#   scan the QR in Expo Go on a phone
```

## The base URL is the only seam

`EXPO_PUBLIC_API_BASE_URL` is the **only** environment coupling in the app. It is read in exactly
one file, [`src/api/client.ts`](src/api/client.ts).

```bash
EXPO_PUBLIC_API_BASE_URL=http://127.0.0.1:8010   # the research engine, locally (default)
EXPO_PUBLIC_API_BASE_URL=https://api.kanida.ai   # the same contract, deployed
```

Leave it unset and the client infers the Metro host with port 8010, so scanning the QR code on a
phone reaches the engine running on your laptop without extra configuration.

## Verify

```bash
npm run typecheck        # tsc --noEmit, app + tests
npx expo lint
npm test                 # unit rows — token resolution, sign policy, the feed composition, the compliance lint
npm run test:contract    # rows against the LIVE research API on EXPO_PUBLIC_API_BASE_URL
npm run test:contract:p0 # the older P0 rows, for the mock source only (KANIDA_PATHFINDER_SOURCE=mock)
npx expo export --platform all   # proves iOS + Android + web bundle from this one source
```

## Layout

```
src/
  api/        client.ts (the ONLY file that knows a hostname) · types.ts (contract mirror) · useResource.ts
  design/     tokens.ts (palette, type scale, spacing) · theme.tsx · responsive.ts
  lib/        format.ts (all figures, IST dates) · honesty.ts (the product rules + the compliance lint)
              tokens.ts ({{fact:…}} parsing) · stories.ts (the feed as stories — pure)
  components/ ui.tsx · facts.tsx (tappable facts + provenance sheet) · chips.tsx · narrative.tsx
              feed/StoryPager.tsx (the vertical pager) · feed/StorySlide.tsx (one story per screen)
              experiment.tsx (versions · periods · trials · gates · basket · proposal · post-mortem)
              ChangeLog.tsx · states.tsx · Page.tsx · agent.tsx
  app/        expo-router routes
    (tabs)/                        Home · Agents · AutoTrade · Insights · Profile
    pathfinder/index               THE FEED  (?date= for an edition, ?story= to land on one)
    pathfinder/story/[id]          a finding's depth: evidence, every fact, the frozen rule, the grade
    pathfinder/experiment/[id]     an experiment's record: versions, trials, expected vs actual
    pathfinder/experiments         the registry, and everything the gate declined
    pathfinder/scoreboard          Right · Wrong · Inconclusive · n, forward vs backfilled
```

## The rules this app renders

Enforced in `src/lib/honesty.ts`, `src/lib/stories.ts`, `src/components/facts.tsx` and the tests:

- **Every number is a computed fact with tappable provenance** (n, window, source, cost convention,
  evidence level). A figure the engine reports without a fact card (a count, a scoreboard tally)
  is labelled as such and still tappable — it never pretends to be a Fact.
- **The narrative is digit-free**; every `{{fact:…}}` is resolved against `facts[]`. An unresolvable
  id renders a visible `[missing figure]`, never the raw token.
- **A backfill is labelled a backfill** on the edition, on every graded card and on the scoreboard —
  the engine's string, verbatim. Forward n and backfilled n are shown side by side.
- **Nothing is padded.** A four-finding edition is four stories; an edition that published nothing
  says so; when no experiment cleared the gate the feed shows the closest one and the gate it failed.
- **Died, buried, Wrong and void are shown as fully as Right.**
- **n < 50 flagged, n < 20 greyed** — the server derives the flag, the app renders it.
- **No entry, target, stop or execution on the research surface.** Experiment text is linted against
  the same banned words the server refuses; research cards against the shape of an order. A line
  that fails is withheld visibly. Constituents appear only when the API marks them RA-reviewed.
- **No promises, no target prices.** Every return sits next to its drawdown; no `+` on a drawdown.
