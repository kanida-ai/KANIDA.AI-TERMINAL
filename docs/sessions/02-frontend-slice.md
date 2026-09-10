# Session 02 — Frontend Slice (Expo, against the mock; runs PARALLEL to the engine)

> Paste this as the task for a fresh agent-mode session. First read `CLAUDE.md`, `docs/DOC_MAP.md`,
> `docs/sessions/01-api-contract.md` (the contract you build against), and the EXISTING
> `docs/specs/FALCON_AI_FRONTEND_PLAN.md` + `docs/FRONTEND_SPEC.md`. **Requires Session 01 done** (the
> mock server + openapi.yaml must exist).

## Goal
Prove the **customer-facing look** on **honest data**: scaffold the new mobile-first **Expo** app and build
the first real screens against the **mock** from Session 01. When the engine's real endpoint is ready, you
swap the base URL — no rework. This is the new UX (per the mockups); it does **not** touch the old
`/power/agents` web screen.

## Scope — IN
1. **Expo scaffold** — one codebase → iOS / Android / web. Navigation shell: Home · Agents · AutoTrade
   (disabled/soon) · Insights · Profile. **Design tokens = the terminal-dark system** from the mockups;
   record them in `docs/FRONTEND_SPEC.md`.
2. **API client generated from `docs/openapi.yaml`**, pointed at the mock server (base URL from env).
3. **Screens (to the mockups):**
   - **Home** — three manager cards (Trader/Investor/Pathfinder): each shows **expectancy + drawdown + n +
     live-since + today's count**. Investor/Pathfinder can read stub data for now.
   - **Trader detail** — **idea card** (strategy · direction · entry · **invalidation** · base-rate w/ n ·
     sizing to user capital · disclosure · "Send basket" button, non-functional stub) and **track record**
     (losers-first ledger · outcome distribution · "how we count" · decision badge).
4. **Honest rendering (enforce the non-negotiables in the UI):** WATCH/NO_TRADE badges, **n<20 greyed**,
   **expectancy as the hero number**, every return paired with drawdown, **losers first**, **no promises /
   no target prices**. Build the UI to make the *honest* WATCH-heavy data look good — that's what ships.

## Scope — OUT
The real API (use the mock) · other agents' full detail screens (cards only) · payments · auth · KYC ·
creator console · the old Next.js `/power/agents`.

## Done when
- The Expo app boots on iOS, Android, and web against the mock.
- Home + Trader-detail render the mock's honest data correctly.
- A **UX check** (drive it, or the `falcon-ui`/a UX agent) confirms: losers-first, n-flags, expectancy
  hero, drawdown paired, zero promises.
- Swapping the env base URL to a real endpoint requires **no code change**.
- `docs/handbacks/02-frontend-slice.md` records the screens built, the token set, and any contract gaps
  found (feed them back to Session 01 / `openapi.yaml`).

## Founder decision referenced
Expo-fresh vs. porting the web app — this brief assumes **Expo-fresh** (the agreed direction). Confirm.
