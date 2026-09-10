# ADR 0005 — One Expo codebase, and the base URL is the only environment seam

**Status:** accepted · **Date:** 2026-09-08 (IST) · **Session:** Pathfinder P2 (frontend)
**Supersedes nothing. Implements:** `docs/sessions/PATHFINDER.md` § Session P2, `docs/FRONTEND_SPEC.md` § Platforms.

## Context

The brief requires the product to ship as **one Expo codebase → iOS app + Android app +
responsive web** (which is simultaneously the web app and mobile web), and requires that swapping
the P0 mock for the P1 engine "needs no code change".

Two ways to get this wrong are common enough to be worth writing down:

1. **Platform forks.** `Screen.web.tsx` beside `Screen.native.tsx` starts as one file and becomes
   three products with three sets of bugs. The claim "one codebase" then means only "one repo".
2. **Environment coupling scattered through the app.** A hostname in a screen, a second one in a
   config, a third in a build script — and "point it at the engine" becomes a migration.

## Decision

**1. No platform forks, and no styling layer that has a web dialect.**
Screens are plain React Native (`View`, `Text`, `StyleSheet`, `Pressable`), which
`react-native-web` renders on the web target unchanged. There is no `.web.tsx` / `.native.tsx`
pair, no `Platform.OS` branch on a screen, and no NativeWind/Tailwind (whose class semantics
differ between the targets). `Platform.select` is used in exactly two places, both for a
*presentational* detail with no behavioural difference: the shadow recipe and the system font
stack.

Responsiveness is a **data** concern, not a platform concern: `useLayout()` returns a breakpoint
from the window width, and the same components read it. The bottom tab bar and the desktop top
rail are one component with one set of tokens.

**2. `EXPO_PUBLIC_API_BASE_URL` is the only environment coupling, and it is read in exactly one
file** — `kanida-app/src/api/client.ts`. Every screen calls `api.loop()` / `api.experiments()` /
`api.experiment(id)` / `api.learnings()`. Nothing else in the app knows a hostname exists.

**3. The contract tests are the acceptance suite for the swap.**
`kanida-app/tests/contract.test.ts` runs against whatever `EXPO_PUBLIC_API_BASE_URL` points at and
asserts the assumptions the screens make — token resolution, no numerals in model prose,
losers-first ordering, expectancy-and-drawdown on every block, entry-after-signal, guarded errors.
Point it at P1; if it passes, the swap is done.

## Consequences

- The "one codebase" claim is checkable, and it was checked: `npx expo export --platform all`
  bundles iOS (1238 modules), Android (1387) and web (832) from this source, exit 0.
- Anyone adding a `.web.tsx` file, a `Platform.OS` branch in a screen, or a second base URL is
  making an ADR-level change and should say so.
- We give up the convenience of web-only CSS features (real CSS grid, `:hover` media queries,
  container queries). The responsive system is flexbox + a breakpoint hook instead. This is the
  price of the guarantee and it is worth it.
- A native-only capability (push, biometrics, deep OS integration) will eventually need a
  platform-specific module. That is fine — the rule is about *screens*, not about native modules,
  which belong behind a shared interface in `src/api` or a new `src/native`.
