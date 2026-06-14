# M4 — Paywall — Build Log

**Agent:** BuildAgent-M4
**Date:** 2026-06-13 (IST)
**Scope:** Add `current_paid_user_required` dependency + wire it into the 5 gated routers per CONTRACT §4.
**Constraint:** No Python execution available — correctness by careful reading against CONTRACT.md / MASTER_SPEC.md.

---

## 1. New dependency — `backend/power_user/routers/dependencies.py`

Added (M4-owned file, extend-only — no existing behaviour changed):

- `import os` (for the `POWER_CHECKOUT_URL` env override).
- `_checkout_url()` — returns `os.environ.get("POWER_CHECKOUT_URL", "/power/billing")`. CONTRACT §3.3 default is `/power/billing`; env override supported "unless ENV defines otherwise".
- `_is_allowed_billing(role, billing_plan, subscription_status)` — pure, DB-free function implementing the **exact** CONTRACT §2 predicate:
  `role=='admin' OR billing_plan IN ('founding','comp') OR (billing_plan=='paid' AND subscription_status=='active')`.
- `current_paid_user_required(user=Depends(current_user_required), con=Depends(get_db))`:
  1. `current_user_required` runs first → **401** if not logged in (behaviour unchanged; only depended upon).
  2. Reads `role, billing_plan, subscription_status` from `power_user_users WHERE id = user.user_id` (DB is source of truth — JWT may not carry billing; also a cancelled sub must bite on the next request, not at 24h token refresh).
  3. If user row missing → **401 USER_NOT_FOUND** (JWT points at a deleted/migrated row; not a payment problem).
  4. Applies `_is_allowed_billing`. Role taken from the live DB column, falling back to JWT `role` only if the column is NULL (defence-in-depth so admins are never paywalled).
  5. On allow → returns the same `JWTPayload` (handlers keep using `user.user_id`).
  6. On deny → **HTTP 402** `{code:"PAYMENT_REQUIRED", message, checkout_url}` per CONTRACT §3.3.

`current_user_required` and `current_user_optional` were **not modified** (only a new dependency added).

**INV1 verification:** founding + comp short-circuit to allow regardless of `subscription_status`; admin short-circuits first. Existing power users (backfilled to `billing_plan='founding'`, `subscription_status='active'` per M2) pass unconditionally.

---

## 2. Gated endpoints (file:line of the gate dependency)

### `backend/power_user/routers/picks_router.py`
- `import` updated to include `current_paid_user_required` (line ~36).
- `GET /api/power/picks/today` — `today_full` — `user = Depends(current_paid_user_required)` (line ~124). *(was `current_user_required`)*
- `GET /api/power/picks/live` — `live_decisions` (the **live-tier route**, reads `falcon_live_decisions`) — `user = Depends(current_paid_user_required)` (line ~248). *(was `current_user_required`)*

> Left PUBLIC in this router (CONTRACT §4 "replay routes" + preview funnel): `GET /picks/today/preview`, `GET /replay/featured`, `GET /picks/replay/{date}`, `POST /picks/replay/random`. Untouched.

### `backend/power_user/routers/portfolios_router.py`
- Router-level gate: `APIRouter(..., dependencies=[Depends(current_paid_user_required)])` (line ~36). All routes are product data (no health/meta routes in this file), so a single router-level dependency gates them uniformly:
  - `GET /api/power/portfolios`
  - `GET /api/power/portfolios/sizing`
  - `GET /api/power/portfolios/{slug}`
  - `GET /api/power/portfolios/{slug}/positions`
  - `GET /api/power/portfolios/{slug}/trades`
  - `GET /api/power/portfolios/{slug}/performance`
  - `GET /api/power/portfolios/{slug}/equity`

### `backend/power_user/routers/persona_backtest_router.py`
- `import` updated to include `current_paid_user_required`.
- Gated per-endpoint (NOT router-level, to leave the admin `/refresh` routes on their own admin-secret axis):
  - `GET /api/power/personas` — `get_persona_list` — `_paid=Depends(...)` (line ~42)
  - `GET /api/power/personas/{slug}` — `get_persona_backtest` (line ~52)
  - `GET /api/power/personas/{slug}/yearly` — `get_persona_yearly` (line ~79)
  - `GET /api/power/personas/{slug}/monthly/{year}` — `get_persona_monthly_for_year` (line ~92)
  - `GET /api/power/personas/{slug}/trades` — `get_persona_trades` (line ~108)
  - `GET /api/power/personas/{slug}/reconciliation` — `get_persona_reconciliation` (line ~133)

> Left UNGATED on purpose: `POST /{slug}/refresh` and `POST /refresh-all` — admin-only ops routes already protected by `_require_admin` (X-Admin-Secret). Adding the paywall would force admin/ops scripts to also carry a paid user JWT, which is wrong. These are meta/ops, not product-data routes.
> Note: `get_persona_yearly/monthly/trades/reconciliation` call `get_persona_backtest(slug)` as a plain Python function (not an HTTP request) — the inner `_paid` parameter defaults to its `Depends` sentinel and is unused there, so no double-gating and no behaviour change.

### `backend/power_user/routers/falcon_top20_router.py`
- `import` updated to include `current_paid_user_required`.
- `GET /api/power/today/falcon-top-20` — `falcon_top_20` — `_paid=Depends(current_paid_user_required)` (line ~64).
- Stale module docstring ("Public endpoint (NOT auth-gated)") updated to describe the gate.

---

## 3. Public routes deliberately LEFT UNGATED (CONTRACT §4)

| Router / route | File | Reason |
|---|---|---|
| `auth_router` (all) | `auth_router.py` | CONTRACT §4 public |
| `billing_router` (all) | (M3) | CONTRACT §4 public — you cannot pay if the pay routes are paywalled |
| `invites_router` (all) | `invites_router.py` | CONTRACT §4 public |
| `replay` routes | `picks_router.py` (`/picks/replay/{date}`, `/picks/replay/random`) | CONTRACT §4 "replay routes" public |
| `/picks/today/preview`, `/replay/featured` | `picks_router.py` | Conversion funnel — public preview |
| credibility | (M-owned elsewhere) | CONTRACT §4 public — not present in the 5 gated routers; nothing to change |
| waitlist | (waitlist route) | CONTRACT §4 public |
| persona `/refresh`, `/refresh-all` | `persona_backtest_router.py` | Admin ops (X-Admin-Secret), not product data |
| `admin_router`, `auth_refresh_router` | resp. files | Admin/ops; outside the §4 gate set; untouched |

---

## 4. Invariants

- **INV1** existing founding/comp users keep full access — predicate short-circuits on `founding`/`comp`/`admin`. ✅
- **INV2** `backend/falcon/*` never touched. ✅ (no edits outside `power_user/`)
- **INV6** all edits inside `backend/power_user/`. ✅
- Existing `current_user_required` / `current_user_optional` unchanged. ✅

## 5. Deviations / notes for audit

- **402 placement nuance:** `current_paid_user_required` depends on `current_user_required`, so an anonymous caller gets **401** (not 402). Only an authenticated-but-unpaid caller gets **402 PAYMENT_REQUIRED**. This matches CONTRACT/MASTER_SPEC ("no-code unpaid → 402", "logged-in but blocked → 402").
- **`POWER_CHECKOUT_URL` env var** is read directly via `os.environ` inside `dependencies.py` (M4-owned). It is NOT added to `config.py` (M-owned elsewhere) and is NOT in CONTRACT §5's canonical list — it is an optional override with a safe default (`/power/billing`). Flagged for the orchestrator in case ENV.md should record it.
- **`role` source:** read from the live DB column with JWT fallback. The DB is authoritative; the JWT fallback only triggers if `power_user_users.role` is NULL.
- **No tests added** — Python cannot be executed in this environment, and the test files are not listed as M4-owned in CONTRACT §6. Audit/M-test should add coverage for: founding allow, comp allow, admin allow, paid+active allow, paid+inactive deny(402), blocked deny(402), anon deny(401), missing-row deny(401).
- **No files outside M4 ownership were edited.** Only `dependencies.py` (extend) + the 5 gated routers (wire), all M4's wiring responsibility per CONTRACT §4/§6.
