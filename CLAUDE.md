# KANIDA.AI — Build Root (read fully before touching code)

This worktree (`feat/product-build`) is the home for building the KANIDA.AI product per `docs/PRD.md`.
Owner: Shyam (Founder). You are one of several **single-responsibility sessions** — build your assigned
package to its "done when", write a hand-back, stop. Load the docs your task's "reads" column names
(`docs/DOC_MAP.md`), and **update the docs you change**.

## What Kanida is
Mobile-first AI research app for Indian retail. Three transparent managers — **Trader** (1–3 day),
**Investor** (3–12 mo), **Pathfinder** (hypothesis lab) — each with a rulebook, risk budget, virtual
money, and a public **losers-first, cost-adjusted** track record. Plus a base-rate feed, a creator
console, and a **broker hand-off** (Kanida never places a customer order).

## Non-negotiables (enforce in code — content lint + review)
- **No return promises or target prices anywhere.** Every return shown **with its drawdown**.
  **Expectancy is the hero metric, never win-rate.** **Losers shown first.**
- Every number carries **n + date range + data source + cost convention**. n<50 flagged, n<20 greyed.
- **Point-in-time is law** — every price read takes an `as_of`; no look-ahead, no survivorship.
- Backtests only on methodology pages, labelled "Simulated · Not traded · Not PaRRVA-verified".
- Signals / marks / approvals / corrections are **append-only**. **Nothing reaches a customer without an
  RA-approval row.** Signals never reach the creator console.
- Agents **emit intents only**; execution is a **Kite Publisher hand-off** (user confirms in their own
  broker). The existing live OMS (`backend/autotrade/`) is **walled off the customer path** (Phase-2 tier).
- No secrets in code.

## The two codebases (reuse before building)
- **This repo, KANIDA.AI-TERMINAL** = the product spine (one FastAPI backend + Next.js web + the ~35
  feature worktrees). **Build here.**
- **`C:\Users\SPS\Documents\Kanida_Falcon`** = R&D engine-room (pattern mining, book/expectancy engines,
  arena/challenger, regime + base-rate). **Port engines from here; don't rebuild them.**

## How the build runs (contract-first, parallel)
The **API contract (`docs/openapi.yaml`) is the seam.** Define it first; then the engine track builds to
*produce* it and the frontend track builds to *consume* it (against a mock) — in parallel, no rework.
**Golden thread:** every PRD requirement `Rn` → tables (DATA_MODEL) → endpoints (openapi.yaml) →
screens (FRONTEND_SPEC) → test rows (TEST_PLAN). *No code without a requirement id; no requirement
without a test row.*

## Start here
1. `docs/DOC_MAP.md` — the 12 specs, their owner-session, what each generates + reads.
2. `docs/BUILD_PROGRAM.md` — the ordered, session-sized work packages.
3. `docs/sessions/` — paste-ready session briefs. **Run `01-api-contract` first**, then `02-frontend-slice`
   and the engine slice in parallel.
4. `docs/architecture-proposal-v1.md` — the proposed shape. **Reconcile with the existing
   `docs/architecture.md`, `docs/audit-report.md`, `docs/api-map.md`, and `docs/specs/` already on main —
   do not duplicate them; extend them.**

Each session writes `docs/handbacks/<id>.md`: what was built · how to run · results/evidence · missing
founder inputs (stubbed) · risks · next step.
