# M3 — Billing (Razorpay) — Build Log

**Agent:** BuildAgent-M3
**Date:** 2026-06-13 (IST)
**Status:** BUILT — awaiting audit
**Depends on:** M2 (schema: billing columns + `power_user_subscriptions` + `power_user_billing_events`) — confirmed already landed.

---

## Files written / changed

| File | Action | Owner (CONTRACT §6) |
|---|---|---|
| `backend/power_user/services/billing_service.py` | **new** — Razorpay SDK wrapper | M3 ✅ |
| `backend/power_user/routers/billing_router.py` | **new** — 4 endpoints | M3 ✅ |
| `backend/main.py` | register `power_billing_router` (import + include_router) | allowed (registration) ✅ |
| `requirements.txt` | add `razorpay>=1.4.1` | allowed ✅ |
| `docs/launch/builds/M3-build-log.md` | this log | — |

No other files touched. `backend/falcon/*` untouched (INV2). All new code under `power_user/` (INV6). No DB schema written by M3 (that is M2's; verified present).

---

## Endpoints (CONTRACT §3.2)

| Method | Path | Auth | Returns |
|---|---|---|---|
| POST | `/api/power/billing/create-subscription` | `current_user_required` | `{razorpay_subscription_id, short_url}` |
| GET  | `/api/power/billing/status` | `current_user_required` | `{billing_plan, subscription_status, current_end}` |
| POST | `/api/power/billing/webhook` | **none** (HMAC sig) | `200 {status, deduped, applied}` |
| POST | `/api/power/billing/cancel` | `current_user_required` | `{status: "cancelled"}` |

Router self-prefixes `/api/power/billing` and is registered exactly like the other power_user routers (no `prefix=` on `include_router`, `tags=["Power-User"]`).

---

## Webhook event → DB state mapping (CONTRACT §2)

`apply_webhook_event` applies `_EVENT_PLAN_MAP` in `billing_service.py`:

| Razorpay `event` | `power_user_users.subscription_status` | `power_user_users.billing_plan` | Access? |
|---|---|---|---|
| `subscription.activated` | `active` | `paid` | ✅ grants |
| `subscription.charged` | `active` | `paid` | ✅ grants |
| `subscription.authenticated` | `authenticated` | *(unchanged)* | not yet |
| `subscription.pending` | `halted` | *(unchanged)* | retrying |
| `subscription.halted` | `halted` | `blocked` | ❌ ends |
| `subscription.cancelled` | `cancelled` | `blocked` | ❌ ends |
| `subscription.completed` | `completed` | `blocked` | ❌ ends |
| `subscription.expired` | `expired` | `blocked` | ❌ ends |
| *(any other event)* | *(unchanged)* | *(unchanged)* | recorded for audit, no-op |

This matches CONTRACT §2: `paid` + `active` ⇒ access; lapsed/cancelled ⇒ `blocked` ⇒ M4 returns 402.
`subscription.activated`/`charged` set `billing_plan='paid'`; `halted`/`cancelled`/`completed`/`expired` set the status and flip `billing_plan='blocked'`.

The `power_user_subscriptions` row is upserted alongside (status + `current_start`/`current_end` from the Razorpay entity, converted epoch→IST ISO).

---

## Security measures

1. **Signature verification is a hard gate.** `verify_webhook_signature` recomputes `HMAC-SHA256(raw_body, RAZORPAY_WEBHOOK_SECRET)` over the **raw request bytes** (not re-serialized JSON) and compares with `hmac.compare_digest` (constant-time). Implemented with stdlib `hmac`/`hashlib` — works even if the `razorpay` SDK is absent. Missing signature / missing secret / mismatch ⇒ **400, no DB write**. The forged-webhook path never reaches state mutation.
2. **Idempotency via UNIQUE `razorpay_event_id`.** `apply_webhook_event` INSERTs the audit row first; an `sqlite3.IntegrityError` (duplicate) ⇒ `rollback()` + return `{deduped:True}` **without re-applying** state. The router still returns 200 so Razorpay stops retrying. Idempotency key = `X-Razorpay-Event-Id` header, with a deterministic `body:<sha256(raw_body)>` fallback so duplicates still collide if the header is absent.
3. **No secrets in code (INV5).** All Razorpay keys/plan id read from env (`RAZORPAY_KEY_ID/SECRET/WEBHOOK_SECRET/PLAN_MONTHLY`). Nothing hard-coded.
4. **Lazy SDK import / fail-graceful.** `import razorpay` happens inside the functions that need it; absence ⇒ `BillingConfigError` ⇒ **503** (`BILLING_NOT_CONFIGURED`), never an import crash at boot. This env has no SDK and no creds, so module import must stay clean — it does.
5. **No client-trust for access grants.** `create-subscription` records the sub but does **not** set `billing_plan='paid'`. Access flips to paid only on a signature-verified `activated`/`charged` webhook.
6. **Parameterized SQL** everywhere (no string interpolation) — no SQL injection surface.
7. **PII redaction** in logs via `redact_email`, consistent with auth_router.
8. **IST timestamps** (INV4) for all `created_at`/`updated_at`/`received_at` and epoch conversions.

---

## CONTRACT deviations / judgment calls

- **None to the locked names/shapes.** Tables, columns, endpoints, env vars, and the §2 plan/state mapping are used verbatim.
- **Added event types beyond the spec's headline list.** CONTRACT §3.3/§2 name `activated/charged` (grant) and `halted/cancelled/completed/expired` (end). I additionally mapped `authenticated` and `pending` to status-only updates (no `billing_plan` change) because Razorpay emits them and ignoring them would leave `subscription_status` stale. These never grant access; they only refine the status mirror. Flagging for audit confirmation.
- **`cancel` flips DB immediately** (status `cancelled`, `billing_plan='blocked'`) rather than waiting for the webhook, so the gate closes at once. The later `subscription.cancelled` webhook re-affirms the same state idempotently. Judgment call — surfaced for audit.
- **`total_count=120`** on subscription create (Razorpay requires a finite cycle count; ~10 years ≈ "until cancelled"). Not in the contract; a Razorpay API requirement.
- **Customer creation** uses `fail_existing=0` so a returning email reuses the Razorpay customer instead of erroring; `razorpay_customer_id` is cached on the user row (idempotent).

---

## Open risks for the audit agent

1. **Cannot run Python here** (no interpreter in this env) — code is matched to the contract by reading, not executed. No unit tests were run. Audit should run the suite / a smoke import once Razorpay is installed.
2. **No tests written.** Brief listed deliverables as files + build log; M2/M1 logs suggest tests may be a separate concern, but the spec's M3 acceptance criteria (sig rejects forged webhook, idempotent on dup delivery, status updates) are untested here. **Recommend the audit add/req tests** for: (a) forged signature → 400, (b) valid sig + duplicate event_id → single apply + 200, (c) `charged` → user `paid`/`active`, (d) `cancelled` → `blocked`.
3. **Webhook `user_id` resolution** relies on either the `power_user_subscriptions` row existing or the `notes.power_user_id` we stamp at create time. If a webhook for an unknown sub with no notes arrives, the event is recorded with `user_id=NULL` (audit row kept) and no user state changes — acceptable, but audit should confirm this is the desired pre-link behaviour (CONTRACT §1.3 explicitly allows nullable `user_id`).
4. **Postgres parity:** code uses `sqlite3.IntegrityError` for the dedupe path (dev DB). On Supabase/Postgres the driver raises `psycopg2.IntegrityError`/`UniqueViolation`, which this `except sqlite3.IntegrityError` will NOT catch. **This is the one real portability gap** — when the app runs on Postgres the dedupe will surface as a 500 (→ Razorpay retries → still eventually consistent, but not clean). The whole codebase currently uses the `sqlite3` connection pattern (`get_db` opens sqlite3), so this matches existing convention, but M1/M2's Postgres cutover will need the exception broadened. Flagging explicitly.
5. **Cancel selects "most recent non-terminal" subscription** — if a user somehow has multiple live subs (shouldn't happen with single-plan launch), only the newest is cancelled.
