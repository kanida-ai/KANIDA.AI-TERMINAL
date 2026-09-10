# `kanida-app` — the KANIDA.AI client

**ONE Expo codebase → iOS app · Android app · responsive web (desktop web + mobile web).**
There is no `.web.tsx`, no `.native.tsx` and no platform branch on a screen anywhere in `src/`.
The same routes, components and tokens serve all three targets.

Session **P2** built the Pathfinder slice: **Home card · Overview · Experiments ·
Experiment detail · Learnings · Strategy Lab**, against the P0 mock
(`backend/pathfinder/mock_app.py`).

---

## Run it

```bash
# 1. the API (session P0's honest mock)
cd ../backend && uvicorn pathfinder.mock_app:app --port 8010

# 2. the app
cd ../kanida-app && npm install && npm start
#   press w → web        (also: npm run web)
#   press i → iOS        (also: npm run ios)
#   press a → Android    (also: npm run android)
#   scan the QR in Expo Go on a phone
```

## Swapping the mock for the real engine is a base-URL change

`EXPO_PUBLIC_API_BASE_URL` is the **only** environment coupling in the app. It is read in
exactly one file, [`src/api/client.ts`](src/api/client.ts).

```bash
EXPO_PUBLIC_API_BASE_URL=http://127.0.0.1:8010   # P0 mock (default)
EXPO_PUBLIC_API_BASE_URL=https://api.kanida.ai   # P1 engine
```

Set it in `.env` (see `.env.example`) or in the EAS build profile. Paths, types, parsing and
every screen are identical either way — P0 and P1 serve the same contract
(`docs/openapi.yaml`).

Leave it unset and the client infers the Metro host with port 8010, so scanning the QR code on a
phone reaches the mock running on your laptop without extra configuration.

## Verify

```bash
npm run typecheck      # tsc --noEmit, app + tests
npm test               # 15 unit tests — token resolution, sign policy, n-flag copy
npm run test:contract  # 16 tests against a LIVE server on EXPO_PUBLIC_API_BASE_URL
npm run test:all
npx expo export --platform all   # proves iOS + Android + web bundle from this one source
```

`npm run test:contract` is also the **acceptance suite for the P1 swap**: point it at the real
engine, and if it passes, every screen renders honestly against real data with no code change.

## Layout

```
src/
  api/        client.ts (the ONLY file that knows a hostname) · types.ts (contract mirror) · useResource.ts
  design/     tokens.ts (palette, type scale, spacing) · theme.tsx · responsive.ts
  lib/        format.ts (all figures, IST dates) · honesty.ts (the product rules) · tokens.ts ({{fact:…}} parsing)
  components/ ui.tsx · facts.tsx · Story.tsx · Performance.tsx · ExperimentCard.tsx ·
              ChangeLog.tsx · Ledger.tsx · Evidence.tsx · Pipeline.tsx · Sparkline.tsx · states.tsx
  app/        expo-router routes
    (tabs)/                     Home · Agents · AutoTrade · Insights · Profile
    pathfinder/(agent)/         Overview · Experiments · Learnings · Strategy Lab
    pathfinder/experiment/[id]  the full loop story
```

## The rules this app renders

Enforced in `src/lib/honesty.ts`, `src/components/Performance.tsx` and the contract tests:

- **Expectancy is the hero**, never win rate. Win rate appears once, labelled "supporting".
- **Every return is shown with its drawdown.** The types make a naked return unconstructible.
- **Losers first.** The ledger and the list render in the order the engine sent. There is no
  sort-by-best control anywhere, on purpose.
- **Died experiments are shown**, with their post-mortem and what was kept.
- **n < 50 flagged, n < 20 greyed** — the server derives the flag, the app renders it.
- **No promises, no target prices.** A rulebook has an invalidation instead.
- **The LLM never calculates.** Narrative arrives with no numerals in it; every figure is a
  `{{fact:…}}` reference resolved against the engine's `facts[]`, tappable for its n, window,
  data source, cost convention and the deterministic component that computed it.
