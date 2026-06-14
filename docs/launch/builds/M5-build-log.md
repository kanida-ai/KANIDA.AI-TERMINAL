# M5 — Open Email Signup — Build Log

**Module:** M5 (Signup)
**Agent:** BuildAgent-M5
**Date:** 2026-06-13 (IST)
**Contract refs:** CONTRACT §1.1, §2, §3.1, §6, §7 · MASTER_SPEC M5 + §2
**Files owned + edited (CONTRACT §6):**
- `backend/power_user/services/auth.py`
- `backend/power_user/routers/auth_router.py`

No files outside M5 ownership were touched. No DB schema files, no M3/M4 files, no `backend/falcon/*`.

---

## What was built

### 1. `POST /api/power/auth/signup` (auth_router.py)
New public funnel endpoint, distinct from the existing `/invite-login`.

- Request model `SignupRequest{ email: str, invite_code: Optional[str]=None }`.
  `Optional[str]` (not `""` default) so the frontend may omit the key entirely.
- Rate-limited per IP-hash with the same budget as invite-login
  (`config.POWER_INVITE_LOGIN_LIMIT_PER_HOUR`, default 5/hr) so the open funnel
  can't mass-create rows. Uses the existing `check_anon_rate_limit` +
  `hash_ip_ua` + `log_request` helpers from `dependencies.py`.
- Delegates all logic to `auth.signup_user(con, email, invite_code)`.
- Error mapping:
  - `SignupError('EMAIL_EXISTS')` → **409** `{code:'EMAIL_EXISTS'}` (exact contract body).
  - `AuthError('EMAIL_INVALID')` → **400** `{code, message}`.
- Success → **200** `{token, user_id, billing_plan, access, checkout_url}` (passed straight through from the service).

### 2. `signup_user(...)` service helper (auth.py)
Holds the branch logic so the router stays thin.

**Branch (a) — valid invite_code → `comp`:**
1. Duplicate-email guard via `find_user_by_email` → `SignupError('EMAIL_EXISTS')`.
2. Reuse `invites.redeem_with_email(con, email, code)` — the EXISTING atomic
   validate→consume→insert path. No redemption logic reimplemented.
3. `UPDATE power_user_users SET billing_plan='comp' WHERE id=?` on the new row
   (redeem_with_email creates it as a plain `role='user'`; M5 stamps the plan).
4. Return `{token, user_id, billing_plan:'comp', access:'full', checkout_url:None}`.
   - If the code is **present but invalid** (not found / used / expired / bad
     shape → `InviteError`), per CONTRACT §3.1 this is NOT an error — it falls
     through to branch (b). Internal reason logged, user treated as code-less.

**Branch (b) — no/invalid code → `blocked`:**
1. Same duplicate-email guard already ran.
2. Insert the user directly: `role='user'`, `is_active=1`,
   `billing_plan='blocked'`, `subscription_status='active'` (column default),
   synthetic hashed `google_sub`.
3. `IntegrityError` on the insert (UNIQUE race after the guard) → mapped to
   `SignupError('EMAIL_EXISTS')` so the contract's 409 still holds under a race.
4. Mint JWT via `issue_jwt`, bump `touch_last_seen`.
5. Return `{token, user_id, billing_plan:'blocked', access:'payment_required',
   checkout_url:'/power/billing'}`.
   (User flips to `paid` later via the Razorpay webhook — M3.)

### 3. Synthetic `google_sub` (auth.py)
New helper `_synthetic_sub_for_email_hashed(email)`:
```
return f"email:{sha256(email.lower().strip())}"   # hex digest
```
Per CONTRACT §3.1 + db_schema.sql comment (lines 224–234). Satisfies the dev
SQLite `google_sub NOT NULL UNIQUE` constraint without storing the raw email.

**Two synthetic-sub forms now coexist, deliberately:**
- `_synthetic_sub_for_email()` → `email:<raw>` — legacy invite-login /
  `redeem_with_email` (UNCHANGED).
- `_synthetic_sub_for_email_hashed()` → `email:<sha256>` — open-signup blocked
  branch (M5, new).

Each is a pure function of the lowercased email → exactly one sub per email
within its scheme, no intra-scheme collision. A given email is created through
only ONE path (signup OR invite redeem), so no single user needs both. If such
a user later signs in via Google with the same email, the existing
`sign_in_with_google()` email-fallback rotates `google_sub` to the real Google
sub regardless of which synthetic form was stored.

> Consequence (documented deviation, see below): the **comp** branch keeps the
> `email:<raw>` sub that `redeem_with_email` writes; only the **blocked** branch
> uses the hashed sub. Both satisfy NOT-NULL/UNIQUE; the contract's sha256
> requirement is met on the branch where M5 controls the insert.

### 4. Email normalization
Reuses the existing `_normalize_email()` (lowercase + trim + structural check;
raises `AuthError('EMAIL_INVALID')`). No new normalization logic.

---

## Reuse summary (no duplicated logic)
| Need | Reused from |
|---|---|
| Atomic invite validate + consume + user insert (comp branch) | `invites.redeem_with_email` |
| JWT minting | `auth.issue_jwt` |
| last_seen bump | `auth.touch_last_seen` |
| Email normalize + EMAIL_INVALID | `auth._normalize_email` |
| Duplicate detection | `auth.find_user_by_email` |
| Rate limit / IP hash / request log | `dependencies.check_anon_rate_limit`, `hash_ip_ua`, `log_request` |
| Billing columns (`billing_plan`, `subscription_status`) | M2 (db_init.py ALTERs + backfill) — read/written, not defined here |

---

## Login flow regression check (INV1)
The existing invite-code LOGIN path is **byte-for-byte behaviourally unchanged**:
- `sign_in_with_email_and_code()` — untouched (admin bypass, existing-user
  re-login, new-user redeem all intact).
- `invite_login()` router handler — untouched.
- `redeem_with_email()` — untouched (only *called* by the new comp branch).
- `_synthetic_sub_for_email()` — untouched; new hashed helper added alongside it.

All M5 changes are **purely additive**: new imports, one new request model, one
new endpoint, one new service function, one new helper, one new exception class.
No existing function body was modified.

---

## Out of scope (v1) — flagged for Stage 2
**Email verification.** v1 trusts the supplied email with no confirm round-trip
(no verify-link, no `email_verified` column). Marked with a
`TODO(Stage 2)` in `signup_user`'s docstring. When Stage 2 adds it, gate JWT
issuance (or product access) on verification.

---

## Deviations
1. **Comp-branch synthetic sub is `email:<raw>`, not `email:<sha256>`.** Because
   the contract also requires reusing `invites.py` for the code path, and
   `redeem_with_email` (owned by invites.py — not an M5 file) writes the raw
   form. Reimplementing redemption to force the hashed sub would duplicate the
   atomic-redeem logic the contract told me to reuse, and would diverge from
   how an invite-created user is later matched. The hashed sub is applied on the
   blocked branch, which M5 fully controls. Both forms satisfy the dev
   NOT-NULL/UNIQUE constraint identically. Net: requirement honoured where M5
   owns the insert; reuse honoured where the contract mandates it.

## Risks for audit
- **Cross-module column dependency:** the comp branch issues
  `UPDATE ... SET billing_plan='comp'` and the blocked branch INSERTs
  `billing_plan`/`subscription_status`. These columns are created by M2
  (`db_init.py` guarded ALTERs). If M5 runs against a DB where M2's ALTERs
  never ran, the INSERT/UPDATE raises `OperationalError`. Mitigated in practice
  because `db_init` runs at every boot before any request. Auditor should
  confirm M2 init precedes signup traffic.
- **Race on duplicate email:** guarded twice — pre-check `find_user_by_email`
  AND `IntegrityError`→`EMAIL_EXISTS` on the blocked-branch insert. The comp
  branch relies on `redeem_with_email`'s own in-transaction
  `USER_ALREADY_EXISTS` check; if hit, it surfaces as `InviteError` and falls
  through to the blocked branch, where the duplicate guard then returns 409.
  Worth an auditor read to confirm that fall-through can't double-insert (it
  can't: the pre-check already returned 409 before either branch ran).
- **No CAPTCHA on the open funnel** — only IP-hash rate limiting (5/hr). Fine
  for launch cohort; revisit if abused.
- **Cannot run Python here** — endpoint contract matched by reading only. No
  unit test added (M5 owns no test file per CONTRACT §6; existing
  `tests/test_routers.py` is shared and not in M5's ownership list).
