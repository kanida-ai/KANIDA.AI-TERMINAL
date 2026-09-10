# API Map

Every endpoint, grouped by product. Mount points are in `backend/main.py`.

> **Two layers, don't confuse them.** Everything below the "Product" heading is the **new
> KANIDA.AI product contract**, specified in `docs/openapi.yaml` and built contract-first. Everything
> from "Power User" down is the **existing** operator/Phase-2 surface, documented as-is. New product
> work extends `docs/openapi.yaml`; it does not fork these.

---

# Product — the contract-first surface (`docs/openapi.yaml`)

## Pathfinder — `/api/pathfinder/*` (read-only)

The autonomous research loop. **Read-only by construction** — Pathfinder emits research, never an
order, and has no write surface on the customer path.

| Endpoint | Purpose |
|---|---|
| `GET /api/pathfinder/loop` | The live loop as a story (six beats) + the deterministic `facts[]` every sentence references. |
| `GET /api/pathfinder/experiments?status=` | The edge-discovery pipeline. Ordered **losers first**; dead experiments are listed, not hidden. |
| `GET /api/pathfinder/experiment/{id}` | One experiment's full journey: rulebook, story, facts, evidence, losers-first virtual ledger, the append-only L1–L3 change-log, and the post-mortem when it died. |
| `GET /api/pathfinder/learnings` | What was learned (with level + confidence + n) and what is being tested next, including what is blocked and why. |
| `GET /api/pathfinder/feed?date=` | **S1.** The clarity-first after-close edition: findings ranked by usefulness (`what_matters_now` = the first 2–3, then `discoveries`), each with a digit-free `narrative` (numbers are `{{fact:…}}` refs into `facts[]`), full `provenance` (level · n · period · regime · comparison group · cost hurdle), a `grading_rule` frozen at publication, its `grading` state, plus the running `scoreboard` (Right · Wrong · Inconclusive · n). No minimum count — never padded. |

- **Router:** `backend/pathfinder/router.py` · **Contract:** `backend/pathfinder/schemas.py`
  (`docs/openapi.yaml` is **generated** from it — `python scripts/gen_openapi.py`).
- **Mount:** `backend/main.py`, **default OFF**. Ships disabled unless
  `KANIDA_PATHFINDER_ENABLED=true`, because P0 serves hand-authored mock fixtures.
- **Mock server (what the frontend builds against):**
  `cd backend && uvicorn pathfinder.mock_app:app --port 8010`.
- **Data source:** `KANIDA_PATHFINDER_SOURCE=mock` (P0 fixtures) → `postgres` (P1 engine). Swapping
  is an env var, not a code change; `postgres` raises until P1 lands rather than silently serving
  fixtures as engine output.
- **Feed source (S1):** `/feed` reads the research store at `KANIDA_PATHFINDER_RESEARCH_DB`
  (default `var/pathfinder_research.db`), written by `python scripts/run_pathfinder_scan.py`
  (`--date`, `--backfill N`, `--llm none|auto|recorded|live`; price warehouse via `KANIDA_DB`). No
  store → a guarded 404 `no_edition`, never fixtures.
- Errors are guarded: `{"error": {"code", "message", "request_id"}}` on 400/404/500. Nothing internal
  is ever disclosed.

## Trader slice — `/api/trader/*`

*Not yet built — owned by `docs/sessions/01-api-contract.md`.*

---

# Existing surfaces

## Power User — `/api/power/*` (8 routers, invite-gated)

| Router file | Mount | Purpose |
|-------------|-------|---------|
| `power_user/routers/auth_router.py` | `/api/power/auth` | Google + invite sign-in, `/me`, logout |
| `power_user/routers/invites_router.py` | `/api/power/invites` | Public redeem + waitlist |
| `power_user/routers/admin_router.py` | `/api/power/admin` | Invite issue/revoke, user list/deactivate, metrics, Zerodha auth status, jobs control, push subscribe, replay-warm |
| `power_user/routers/picks_router.py` | `/api/power/picks` | Live-tier intraday picks |
| `power_user/routers/auth_refresh_router.py` | `/api/power/auth-refresh` | Magic-link token-refresh flow |
| `power_user/routers/portfolios_router.py` | `/api/power/portfolios` | Co-Trader personas — listings, positions, equity, trades |
| `power_user/routers/persona_backtest_router.py` | `/api/power/personas` | Persona simulator output (yearly/monthly/trades/reconciliation) |
| `power_user/routers/falcon_top20_router.py` | `/api/power/today` | Falcon Top-10 + 3-bucket explainability (the `/power/today` data) |

## Falcon Auto-Trade — `/api/falcon/*` (5 routers, operator-only)

| Mount | Purpose |
|-------|---------|
| `/api/falcon/signals` | EOD signal management |
| `/api/falcon/portfolio` | Active Kite positions |
| `/api/falcon/patterns` | Pattern taxonomy + audit |
| `/api/falcon/admin` | Engine playbook config, pipeline trigger, freshness |
| `/api/falcon/trade` | Premarket staging, deploy queue, position monitor |

## Legacy — `/api/*` (11 routers, status mixed)

Mounted in `main.py` but mostly pre-Falcon. **Audit consumers before removing.**

| Mount | Likely status |
|-------|---------------|
| `/api/jobs` | **Still used** (pipeline trigger) |
| `/api/universe` | **Still used** (stock lookups) |
| `/api/ai` | Used by `/terminal/chat` |
| `/api/swing` | May power `/analysis` |
| `/api/admin` | OG admin — used by legacy `/admin`, `/analysis` |
| `/api/quant`, `/api/backtest`, `/api/live`, `/api/execution`, `/api/orders`, `/api/strategy` | Likely deprecated — verify no live consumer before unmounting |

## Frontend routes

**Active:** `/power/*` (Power User), `/falcon/*` (operator).
**Legacy (candidates for archive after usage audit):** `/admin`, `/analysis`,
`/analysis-v2`, `/analysis-v3`, `/dashboard`, `/engine`, `/terminal*`, `/welcome`,
`/login`.

Next.js rewrites (`next.config.ts`) proxy `/api/power/*` and friends to
`BACKEND_ORIGIN` (api.kanida.ai). `middleware.ts` applies HTTP Basic Auth to
everything except `/power/*`.


### Pathfinder data source (P1)

`KANIDA_PATHFINDER_SOURCE` selects what the four `/api/pathfinder/*` endpoints serve. The router,
the schemas and the tests do not change between them — that is the point of the seam.

| Value | Serves | Notes |
|---|---|---|
| `mock` *(default)* | P0's hand-authored fixtures | Labelled as fixtures on every response. |
| `engine` | the real loop's authoritative tables | SQLite mirror at `KANIDA_PATHFINDER_DB` (default `var/pathfinder.db`). **Raises** if the database is absent rather than falling back to fixtures. |
| `postgres` | the same tables on Postgres | **Raises** — needs a driver and credentials P1 did not have. |

Produce the engine data with `python scripts/run_pathfinder_loop.py --fresh --llm recorded`.
`KANIDA_PATHFINDER_LLM` selects the gateway provider: `auto` (default) · `live` · `recorded` · `none`.
