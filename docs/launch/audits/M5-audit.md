# M5 — Open Email Signup — Audit

**Auditor:** AuditAgent-M5
**Date:** 2026-06-13 (IST)
**Module:** M5 (Signup) · BuildAgent-M5
**Artifacts audited:**
- `backend/power_user/routers/auth_router.py` (`/signup` additions)
- `backend/power_user/services/auth.py` (`signup_user`, `SignupError`, `_synthetic_sub_for_email_hashed`)
- `backend/power_user/services/invites.py` (reuse confirmation — read-only)
- Verified against `git diff HEAD` (artifacts uncommitted; diffed against last commit `453b60d`).

---

## VERDICT: GREEN

All checklist items pass. No contract violation, no login-flow regression, no constraint/uniqueness bug, no duplicate-create race, no invariant breach.

---

## Checklist findings (cited)

### 1. Contract §3.1 fidelity — PASS

| Case | Contract | Implementation | Cite |
|---|---|---|---|
| Valid code | `comp` + `access:'full'` + `checkout_url:null` | returns `billing_plan='comp', access='full', checkout_url=None` | `auth.py:601-607` |
| No/invalid code | `blocked` + `access:'payment_required'` + `checkout_url:'/power/billing'` | returns exactly that | `auth.py:640-646` |
| Duplicate email | 409 `{code:'EMAIL_EXISTS'}` | `SignupError('EMAIL_EXISTS')` → `HTTPException(409, {"code": e.code})` (body is `{code}` only, no message — exact) | `auth.py:572-574`, `auth_router.py:222-228` |
| 200 shape | `{token, user_id, billing_plan, access, checkout_url}` | both return dicts carry exactly these 5 keys; router passes through unchanged | `auth.py:601-607,640-646`, `auth_router.py:237-241` |

Response shapes are exact. EMAIL_INVALID maps to 400 `{code, message}` (`auth_router.py:229-235`) — not contract-specified but a reasonable, non-conflicting addition.

### 2. Regression INV1 (critical) — PASS, existing login flow UNTOUCHED

`git diff HEAD` proves every change is additive:
- **auth.py**: `+172` lines, the lone `-1` is the pre-existing no-newline-at-EOF marker converting to a newline when the appended block was added — **no existing line was deleted or modified**. New content only: `import hashlib` (line 29), `_synthetic_sub_for_email_hashed` (added *alongside* the untouched `_synthetic_sub_for_email`, lines 317-334), and the M5 block (`SignupError` + `signup_user`, lines 499-646).
- `sign_in_with_email_and_code()` (`auth.py:391-495`) — body byte-for-byte unchanged in the diff.
- `invite_login()` handler (`auth_router.py:147-182`) — unchanged; diff only inserts the new `/signup` route *after* it.
- `redeem_with_email()` (`invites.py:371-475`) — `invites.py` does **not appear in the diff at all** (`git diff --stat` lists only the two M5 files). Confirmed only *called*, never edited.
- `_synthetic_sub_for_email()` (`auth.py:313-314`) — unchanged.

Router import block adds `SignupError`, `signup_user`, and widens `typing` to include `Optional` (`auth_router.py:17,25,29`) — additive, no existing symbol removed.

### 3. Invite reuse + fall-through — PASS

- Comp path calls `invites.redeem_with_email(con, email=email, code=code)` (`auth.py:579`) — the existing atomic validate→consume→insert (`invites.py:371-475`, `BEGIN IMMEDIATE`). **No redemption logic reimplemented.**
- Present-but-invalid code: `redeem_with_email` raises `InviteError` (NOT_FOUND/ALREADY_USED/EXPIRED/FORMAT_INVALID), caught at `auth.py:580-586`, logged, and **falls through** to branch (b). No double-insert: on `InviteError` the redeem txn ROLLBACKs (`invites.py:448-450`), so no user row and no consumed code leak; branch (b) then creates the single blocked user. Verified the fall-through cannot double-insert — the comp insert only happens inside the `else` (success) clause (`auth.py:587-607`); the `except` path does nothing but log.
- No half-consumed code: `redeem_with_email` consumes the code and inserts the user in **one** transaction with a `rowcount != 1` RACE guard (`invites.py:443-445`); partial state is impossible.

### 4. Synthetic google_sub — PASS, deviation is safe

- Blocked branch: `_synthetic_sub_for_email_hashed` = `'email:' + sha256(lower(strip(email)))` (`auth.py:332-334`) — exactly the contract §3.1 / `db_schema.sql:224-234` form. Deterministic per-email → satisfies `google_sub NOT NULL UNIQUE` (`db_schema.sql:28`).
- Deviation (documented, build-log §Deviations): comp branch keeps `redeem_with_email`'s raw `email:<raw>` sub. **Safe** — analysis:
  - No uniqueness problem: each scheme is injective on lowercased email; both write to the same UNIQUE column but a given email is created through exactly ONE path (signup-blocked OR invite-redeem), never both.
  - No cross-format collision: raw form is `email:<address>` (contains `@`); hashed form is `email:<64-hex>` (no `@`). The two value spaces are disjoint — a raw sub can never equal a hashed sub, so even hypothetical same-email-via-both-paths could not collide on `google_sub` (it would collide first on the `email` UNIQUE, correctly yielding EMAIL_EXISTS). The contract's sha256 mandate is honoured on the branch M5 controls the insert; reuse is honoured where the contract mandates it.

### 5. Duplicate / race — PASS (defence in depth)

- Pre-check: `find_user_by_email` → `SignupError('EMAIL_EXISTS')` before either branch (`auth.py:572-574`).
- Blocked-branch insert wrapped in `try/except sqlite3.IntegrityError` → `rollback()` → `SignupError('EMAIL_EXISTS')` (`auth.py:614-632`), catching a lost race on either UNIQUE(email) or UNIQUE(google_sub) (`db_schema.sql:27-28`). 409 holds under concurrency.
- Comp-branch race handled by `redeem_with_email`'s own in-txn `USER_ALREADY_EXISTS` check (`invites.py:420-425`) under `BEGIN IMMEDIATE`; if hit it raises InviteError → falls through to branch (b), where the insert's IntegrityError guard returns 409. Cannot double-create.

### 6. Invariants — PASS

- **INV2 (no falcon/*):** No `backend/falcon/*` code touched. Only `falcon` occurrences in M5 files are inert prose comments (`auth.py:231` docstring; `config.py` comments). Clean.
- **INV6 (power_user/ only):** Both edited files live under `backend/power_user/`. Clean.
- **Rate-limiting on the open endpoint:** Applied — `check_anon_rate_limit(con, ip_hash, route_key, config.POWER_INVITE_LOGIN_LIMIT_PER_HOUR)` (`auth_router.py:217-218`), reusing the verified helpers in `dependencies.py:264,272,300` and the real config default 5/hr (`config.py:86-87`). Mass-create is gated per IP-hash.

---

## Notes (non-blocking)

- **Cross-module column dependency:** `signup_user` writes `billing_plan`/`subscription_status`, columns created by M2's `db_init.py` ALTERs. M2 init runs at boot before any request, so this is satisfied operationally. Not an M5 defect.
- **No CAPTCHA**, IP-hash rate-limit only — acceptable for launch cohort per build-log; revisit if abused.
- **No unit test added** — M5 owns no test file per CONTRACT §6; contract conformance verified by read + diff here. Recommend the orchestrator add a `/signup` case to the shared `tests/test_routers.py` (comp / blocked / duplicate-409) before merge, but this is not a RED condition.
- **Email verification** deferred to Stage 2 (flagged in `signup_user` docstring, `auth.py:558-561`) — consistent with MASTER_SPEC M5.

---

## Existing login flow — explicit confirmation

**The existing invite-code LOGIN flow is UNTOUCHED.** `git diff HEAD` shows `invites.py` not in the changeset at all, and `sign_in_with_email_and_code`, `invite_login`, `redeem_with_email`, and `_synthetic_sub_for_email` are byte-for-byte unchanged. All M5 changes are purely additive (new imports, one request model, one route, one service fn, one helper, one exception class). The build-log's "byte-for-byte" claim is verified.
