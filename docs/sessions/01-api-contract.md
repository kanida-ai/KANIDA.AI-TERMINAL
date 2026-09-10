# Session 01 — API Contract + Mock (the linchpin; run FIRST)

> Paste this as the task for a fresh agent-mode session in this repo. First read `CLAUDE.md`,
> `docs/DOC_MAP.md`, `docs/architecture-proposal-v1.md`, and the EXISTING `docs/api-map.md` +
> `docs/openapi.yaml` (reconcile with them — don't duplicate).

## Goal
Define the **honest API contract** for the Trader slice and stand up a **mock server** that serves
honest-shaped sample data. This single artifact unblocks BOTH the engine track and the frontend track,
so they build in parallel with zero rework.

## Scope — IN
1. **Design the payload** for `GET /api/trader/slice?date=` (extend, don't fork, `docs/api-map.md`):
   a **scan summary** (scanned, found, statistically_meaningful, qualified) + a list of **setups**, each:
   `{ symbol, sector, strategy, direction, stage, entry, invalidation, horizon_days,
      expectancy, expectancy_2x_slippage, n, win_rate, avg_win, avg_loss, max_drawdown, current_drawdown,
      ledger_losers_first[], decision: TRADE|WATCH|NO_TRADE, decision_reason, disclosure, as_of }`.
   Honor the non-negotiables **in the schema**: expectancy is required; every return field pairs with a
   drawdown field; no "target price" field; `n` is required; `ledger_losers_first` is losers-first.
2. **Write it as OpenAPI 3.1** in `docs/openapi.yaml` (merge into the existing file). Guarded error shapes
   (400/404/500 never leak internals). This is what generates the server stubs AND the app's client.
3. **Honest sample fixtures** (3–4): a **WATCH** setup (n=8, small expectancy), a **TRADE** setup (n≥60,
   positive expectancy net of 2× slippage), a **NO_TRADE** (expectancy ≤ 0 after costs), one SHORT.
   These MUST look like reality — mostly WATCH, small n, losers present. No fantasy winners.
4. **Mock server**: serve the fixtures at `/api/trader/slice` (Prism off the openapi.yaml, or a tiny
   FastAPI stub). Document how to run it and how to point a client at it.

## Scope — OUT
The real engine, real data, the app, other agents (Investor/Pathfinder can be stubbed in the schema only),
auth, payments, compliance module.

## Done when
- `docs/openapi.yaml` validates and describes `/api/trader/slice` with the honest schema above.
- The mock server returns the 3–4 honest fixtures; a client can call it.
- The fixtures pass a self-check: expectancy present, returns paired with drawdown, losers-first, n flags,
  zero target prices / promises.
- `docs/handbacks/01-api-contract.md` records the schema decisions + how to run the mock + ADR if any
  contract decision was made (`docs/adr/`).

## Founder input (stub if absent)
Exact field names you want surfaced on the idea card — proceed with the list above if unspecified.
