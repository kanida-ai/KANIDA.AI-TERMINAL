# Kanida.AI Launch — CONTRACT (single source of truth)

**Every build agent reads this. No agent invents a table, column, endpoint, or env-var name — it comes from here.** If a build needs a name not listed, the agent stops and asks the orchestrator to amend this file first.

**Target DB dialect:** PostgreSQL (Supabase). Migrations are Postgres SQL. SQLite remains only for local dev.
**All timestamps:** `TIMESTAMPTZ`, written in IST (`+05:30`).

---

## 1. Tables

### 1.1 `power_user_users` — EXISTING, additive columns only (INV7)

Add these columns (all nullable or defaulted so existing rows are valid):

| Column | Type | Default | Notes |
|---|---|---|---|
| `billing_plan` | TEXT | `'founding'` | one of `founding` / `comp` / `paid` / `blocked` |
| `razorpay_customer_id` | TEXT | NULL | Razorpay customer handle |
| `subscription_status` | TEXT | `'active'` | mirror of latest sub status; `active` for founding/comp |

Also: **relax `google_sub` to nullable** (open email signup has no Google sub).

**Backfill on migrate:** every existing row → `billing_plan='founding'`, `subscription_status='active'`.

### 1.2 `power_user_subscriptions` — NEW

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `user_id` | BIGINT NOT NULL | FK → `power_user_users(id)` |
| `razorpay_subscription_id` | TEXT UNIQUE | |
| `plan_code` | TEXT NOT NULL | `'monthly_999'` for launch |
| `status` | TEXT NOT NULL | Razorpay states: `created`/`authenticated`/`active`/`halted`/`cancelled`/`completed`/`expired` |
| `current_start` | TIMESTAMPTZ | from Razorpay |
| `current_end` | TIMESTAMPTZ | access valid until this |
| `created_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | |
| `updated_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | |

Index: `(user_id)`, `(razorpay_subscription_id)`.

### 1.3 `power_user_billing_events` — NEW (webhook audit + idempotency)

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `user_id` | BIGINT NULL | FK → `power_user_users(id)`, nullable (event may arrive pre-link) |
| `razorpay_event_id` | TEXT UNIQUE NOT NULL | **idempotency key** — dedupe duplicate webhook delivery |
| `event_type` | TEXT NOT NULL | e.g. `subscription.activated`, `subscription.charged`, `subscription.halted` |
| `payload` | JSONB NOT NULL | raw event |
| `received_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | |

---

## 2. Plan semantics (the gate logic — M4 reads this exactly)

| `billing_plan` | Meaning | Product access? |
|---|---|---|
| `founding` | Original power users | ✅ always |
| `comp` | Free via invite code | ✅ always |
| `paid` | Active Razorpay sub | ✅ only if `subscription_status='active'` |
| `blocked` | No code, unpaid, or lapsed/cancelled | ❌ → 402 |

`role='admin'` (existing column) → ✅ always, regardless of plan.

**Allow predicate:** `role=='admin' OR billing_plan IN ('founding','comp') OR (billing_plan=='paid' AND subscription_status=='active')`

---

## 3. API endpoints

### 3.1 Signup — `POST /api/power/auth/signup`
Request: `{ "email": str, "invite_code": str | null }`
- valid `invite_code` → create user `billing_plan='comp'`, return token + `access:"full"`
- no/invalid code → create user `billing_plan='blocked'`, return token + `access:"payment_required"` + `checkout_url`
Response 200: `{ "token": str, "user_id": int, "billing_plan": str, "access": "full"|"payment_required", "checkout_url": str|null }`
Errors: 409 `{code:"EMAIL_EXISTS"}`

### 3.2 Billing
- `POST /api/power/billing/create-subscription` (auth) → `{ "razorpay_subscription_id": str, "short_url": str }`
- `GET  /api/power/billing/status` (auth) → `{ "billing_plan": str, "subscription_status": str, "current_end": str|null }`
- `POST /api/power/billing/webhook` (Razorpay `X-Razorpay-Signature` header, **no JWT**) → 200; verifies HMAC against `RAZORPAY_WEBHOOK_SECRET`; dedupes via `razorpay_event_id`
- `POST /api/power/billing/cancel` (auth) → `{ "status": "cancelled" }`

### 3.3 Paywall rejection shape (M4)
HTTP 402: `{ "code": "PAYMENT_REQUIRED", "message": str, "checkout_url": str }`

---

## 4. Paywall application set (M4)

**Gate (require `current_paid_user_required`):** `picks_router`, `portfolios_router`, `persona_backtest_router`, `falcon_top20_router`, and the live-tier routes.
**Leave public (no gate):** `auth_router`, `billing_router`, `invites_router`, replay routes, credibility, waitlist.
**Never touch:** anything under `backend/falcon/*` (Auto-Trade — INV2).

---

## 5. Env var names (canonical — see ENV.md for full detail)

`DATABASE_URL` · `POWER_JWT_SECRET` · `POWER_DB_PATH` · `POWER_RND_DB_PATH` ·
`RAZORPAY_KEY_ID` · `RAZORPAY_KEY_SECRET` · `RAZORPAY_WEBHOOK_SECRET` · `RAZORPAY_PLAN_MONTHLY` ·
`EMAIL_PROVIDER` · `RESEND_API_KEY` · `EMAIL_FROM` ·
`SITE_USER` · `SITE_PASS` · `KITE_API_KEY` · `KITE_API_SECRET` · `ZERODHA_USERNAME` · `ZERODHA_PASSWORD` · `ZERODHA_PIN`

---

## 6. File ownership (which module writes which file)

| File | Module |
|---|---|
| `backend/power_user/migrations/0001_billing.sql` (+rollback) | M2 |
| `backend/power_user/db_schema.sql` (append) | M2 |
| `backend/power_user/db_init.py` (guarded SQLite ALTERs + backfill — amended 2026-06-13, see note) | M2 |
| `scripts/migrate_to_supabase.py` (extend) | M1 |
| `deploy/*`, `docs/launch/ENV.md` | M1 |
| `backend/power_user/services/billing_service.py` | M3 |
| `backend/power_user/routers/billing_router.py` | M3 |
| `backend/power_user/routers/dependencies.py` (extend: `current_paid_user_required`) | M4 |
| `backend/power_user/routers/auth_router.py`, `services/auth.py` (extend: signup) | M5 |
| `frontend/app/legal/*`, `docs/launch/legal/*` | M6 |
| `frontend/app/power/{pricing,signup,billing}/page.tsx`, `frontend/lib/power-api.ts` | M7 |
| `backend/power_user/services/email_service.py`, `email_templates/*` | M8 |

---

## 7. Invariants every agent obeys (from MASTER_SPEC §2)

INV1 existing users never disrupted · INV2 Auto-Trade never touched · INV3 no mock data · INV4 IST everywhere · INV5 no secrets committed · INV6 power_user code stays in `power_user/` · INV7 additive migrations only.
