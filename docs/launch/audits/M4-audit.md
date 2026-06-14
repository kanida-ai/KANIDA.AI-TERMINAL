# M4 — Paywall — Audit

**Agent:** AuditAgent-M4
**Date:** 2026-06-13 (IST)
**Scope:** `current_paid_user_required` + gating of `picks_router`, `portfolios_router`, `persona_backtest_router`, `falcon_top20_router`, live-tier.
**Method:** READ-ONLY. Static review against CONTRACT.md §2/§3.3/§4/§7 + MASTER_SPEC "M4". Independent grep of each router + sibling routers.

---

## VERDICT: GREEN

No predicate bug, no INV1 lockout, no ungated product endpoint, no wrongly-gated public route, no invariant breach. Cleared to merge.

---

## Checklist results (cited)

### 1. Predicate correctness — PASS
`_is_allowed_billing` (`dependencies.py:112-131`) implements CONTRACT §2 exactly:
- `role=='admin'` → allow (`:125`)
- `billing_plan IN ('founding','comp')` → allow (`:127`)
- `billing_plan=='paid' AND subscription_status=='active'` → allow (`:129`)
- else deny (`:131`)

Read from **DB, not stale JWT**: `current_paid_user_required` (`:154-159`) does a live `SELECT role, billing_plan, subscription_status FROM power_user_users WHERE id=?` every request. Build-log rationale is correct — a cancelled/halted sub flips `subscription_status` away from `'active'` in the DB and bites on the very next request, not at 24h token refresh. A `blocked` user (or `paid` + non-active) hits the deny branch immediately. Confirmed.

### 2. INV1 (founding + comp full access) — PASS (the #1 risk is clear)
Traced end-to-end:
- M2 backfills every existing row → `billing_plan='founding'`, `subscription_status='active'` (CONTRACT §1.1, MASTER_SPEC M2).
- `_is_allowed_billing` short-circuits `founding` at `:127` **before** any `subscription_status` check — so even if status were stale/NULL, founding still passes. Same for `comp`.
- `admin` short-circuits first at `:125`, and `role` is read from the live DB column with JWT fallback (`:169`) so an admin is never paywalled even if the column is NULL.
Existing power users (all founding) are NOT locked out.

### 3. Gate coverage (CONTRACT §4) — PASS
Independently grepped all 11 files in `backend/power_user/routers/`. `current_paid_user_required` appears in exactly the 5 target routers and nowhere else.

Product-data endpoints — all gated:
- `picks_router.py`: `/picks/today` (`:124`), `/picks/live` (live-tier, `:248`). Public-by-design left ungated: `/picks/today/preview`, `/replay/featured`, `/picks/replay/{date}`, `/picks/replay/random` — all conversion-funnel / replay routes per §4. Correct.
- `portfolios_router.py`: router-level gate (`:41`) covers all 7 data routes (list, sizing, {slug}, positions, trades, performance, equity). No health/meta routes in the file, so blanket gating is safe. Correct.
- `persona_backtest_router.py`: all 6 read endpoints carry per-endpoint `_paid=Depends(...)` (`:43,56,86,102,124,147`). Admin ops `/refresh` (`:175`) + `/refresh-all` (`:192`) left on the X-Admin-Secret axis (`_require_admin`) — correctly NOT paywalled (admin/ops scripts must not need a paid JWT). Stale module docstring at `:14` ("all public EXCEPT /refresh") is now inaccurate but is a comment-only nit, not a gate defect.
- `falcon_top20_router.py`: sole route `/today/falcon-top-20` gated (`:65`).

No product endpoint slipped through ungated.

### 4. 402 shape (§3.3) — PASS
Deny raises `HTTPException(402, {code:"PAYMENT_REQUIRED", message, checkout_url})` (`:178-182`) — exact §3.3 shape. Anonymous caller correctly gets **401** (not 402): `current_paid_user_required` depends on `current_user_required` (`:135`), which raises 401 before billing is ever read. Missing user row → 401 USER_NOT_FOUND (`:164`), correctly treated as auth failure not payment failure.

### 5. No collateral change — PASS
`current_user_required` (`:71`) and `current_user_optional` (`:85`) are byte-for-byte unchanged; M4 only added a new function. INV2 (no `backend/falcon/*` edits) and INV6 (all edits in `power_user/`) hold — grep confirms the gate symbol exists only under `power_user/routers/`. Legacy `backend/routers/*` credibility/swing routers untouched.

### 6. POWER_CHECKOUT_URL — PASS
`_checkout_url()` (`:108-109`) reads `os.environ.get("POWER_CHECKOUT_URL", "/power/billing")` — safe default matches §3.3. Documented in `docs/launch/ENV.md:15` (optional, default `/power/billing`).

---

## Notes (non-blocking, no fix required)

- **N1 (cosmetic):** `persona_backtest_router.py:14` docstring still says "All endpoints public + read-only EXCEPT /refresh" — now stale (6 reads are paywalled). Comment-only; optional cleanup.
- **N2 (verified safe):** Internal calls `get_persona_backtest(slug)` from `/yearly|/monthly|/trades|/reconciliation` pass no `_paid`, so the parameter keeps its `Depends(...)` sentinel default — never evaluated, never crashes. The *outer* HTTP endpoints each carry their own `_paid` gate, so HTTP-level gating is intact and there is no double-gating. Correct as built.
- **N3:** No unit tests added (build-log §5; Python not executable in build env, test files not M4-owned per CONTRACT §6). Recommend the orchestrator add coverage at integration time: founding/comp/admin/paid-active allow; paid-inactive + blocked → 402; anon → 401; missing-row → 401.

---

## Explicit confirmations requested

- **Founding/comp users keep access:** CONFIRMED. `_is_allowed_billing` short-circuits `founding`/`comp` at `dependencies.py:127` independent of `subscription_status`; admin short-circuits at `:125`. M2 backfill puts all existing users on `founding`. No lockout path exists for them.
- **No product endpoint slipped ungated:** CONFIRMED. Independent grep across all 5 routers; every product-data route is gated (router-level or per-endpoint); only funnel/replay/admin-ops routes remain public, exactly per CONTRACT §4.
- **No public route wrongly gated:** CONFIRMED. Gate symbol absent from `auth_router`, `billing_router`, `invites_router`, `admin_router`, `auth_refresh_router`; credibility/waitlist live in those public/admin routers and are untouched.
