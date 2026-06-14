# M8 — Email (transactional) — Build Log

**Agent:** BuildAgent-M8
**Date:** 2026-06-13 (IST)
**Module:** M8 — minimal transactional email + 3 triggers
**Status:** BUILT (awaiting audit)

---

## Scope delivered

A provider-abstracted, **best-effort** transactional email service plus the three
lifecycle triggers from MASTER_SPEC M8 / CONTRACT §5. The overriding rule —
**email must NEVER raise into signup or the webhook** — is enforced at three
independent layers (see "Best-effort proof" below).

---

## Files created

| File | Purpose |
|---|---|
| `backend/power_user/services/email_service.py` | Provider-abstracted sender. `EMAIL_PROVIDER` → `resend` (RESEND_API_KEY) or `ses` (boto3). Lazy SDK import; log-and-noop when unconfigured. Public API: `send_welcome`, `send_payment_receipt`, `send_payment_failed`. |
| `backend/power_user/email_templates/welcome.html` / `.txt` | Welcome email (HTML + text). |
| `backend/power_user/email_templates/receipt.html` / `.txt` | Payment-success receipt (amount, access-valid-until, account). |
| `backend/power_user/email_templates/payment_failed.html` / `.txt` | Payment-failed / cancelled recovery notice. |

## Files edited (cross-module hooks — flagged per constraints)

| File | Module | Edit | Lines |
|---|---|---|---|
| `backend/power_user/services/auth.py` | **M5** | Welcome hook: one local guarded helper `_send_welcome_best_effort()` + a single call at each of the two signup-success return sites. | helper at ~571; calls at ~601 (comp) and ~639 (blocked) |
| `backend/power_user/services/billing_service.py` | **M3** | Receipt / payment-failed hook: one guarded helper `_send_billing_email_best_effort()` + a single call at the end of `apply_webhook_event`, AFTER `con.commit()`. | helper at ~395; call at ~530 |
| `requirements.txt` | (root) | Added `resend>=2.0.0` (lazy-imported; SES uses boto3 if chosen). | email section |

**No other files touched.** INV2 (no `backend/falcon/*`), INV6 (stayed in `power_user/`) respected.

---

## Service design

- **Provider abstraction.** `EMAIL_PROVIDER` env selects the path: `resend`
  (`_send_via_resend`, uses `RESEND_API_KEY`) or `ses` (`_send_via_ses`, boto3 +
  standard AWS cred chain). `EMAIL_FROM` is the sender on both. SDKs are
  **lazy-imported inside the send path** — importing `email_service` never fails
  in an env without the package (mirrors `billing_service._client()`).
- **Single choke point.** All three public functions funnel through `_dispatch()`,
  so the "render → pick provider → send → swallow" logic (and the never-raise
  guarantee) lives in exactly one place.
- **Unconfigured = log-and-noop.** `is_configured()` checks provider + sender +
  key WITHOUT importing any SDK. If false, `_dispatch` logs at INFO and returns
  `False` — no exception.
- **Templates** are paired `.html` + `.txt`, rendered with `str.format(**fields)`.
  A missing template file or a placeholder mismatch logs a warning and noops
  (returns `None`/`False`) — never raises.
- **Subjects** owned in `_SUBJECTS` dict (one place), not in the template files.
- **IST (INV4):** `_format_period_end` renders the receipt date via
  `Asia/Kolkata` (`datetime.now/astimezone` with the `+05:30` tz).
- **PII:** every log line masks the recipient via `redact_email` (imported from
  `services/auth.py`) — matches house convention.
- **No secrets (INV5):** `RESEND_API_KEY` / `EMAIL_FROM` read from env only.
- **No marketing:** purely transactional copy; welcome only fires on a fresh
  open signup (never founding members); receipts only on real Razorpay charges.

---

## The 3 triggers — exactly where hooked

1. **Welcome → `auth.py::signup_user` success.**
   Local helper `_send_welcome_best_effort(email)` defined once at the top of the
   function. Called at **both** signup-success return sites:
   - comp-via-code branch (right after `log.info("...comp via code...")`)
   - blocked/paywall branch (right after `log.info("...blocked / payment_required...")`)
   Both new-signup outcomes get a welcome; the EMAIL_EXISTS / invalid-code paths
   do not reach a success return, so no email there. Founding members never hit
   `signup_user` (they log in via invite-login), so they never get a welcome.

2. **Receipt → billing webhook apply path.**
   `apply_webhook_event` (in `billing_service.py`) calls
   `_send_billing_email_best_effort(...)` once, **after `con.commit()`** (the DB
   state change) and before the function's final return. For
   `subscription.charged` / `subscription.activated` it sends
   `send_payment_receipt(email, amount, period_end)`:
   - `amount` extracted from `payload.payload.payment.entity.amount` (paise → ₹).
   - `period_end` from the subscription entity's `current_end` (epoch → IST ISO).
   - `email` resolved by `user_id` lookup on `power_user_users`.

3. **Payment-failed → same webhook apply path.**
   Same single call: for `subscription.halted` / `subscription.cancelled` it
   sends `send_payment_failed(email)`. All other event types (authenticated /
   pending / completed / expired / unmapped) send nothing.

---

## Best-effort proof (the #1 M8 rule)

Email cannot break signup or the webhook. Verified by construction, three layers:

1. **Service layer:** every public `send_*` routes through `_dispatch`, whose
   provider-send is wrapped in `try/except Exception` (plus a dedicated
   `ImportError` branch). It returns `True`/`False`, **never raises**. Unconfigured
   provider, missing key, absent SDK, render failure, and provider API errors all
   return `False`.

2. **Hook layer (signup):** `_send_welcome_best_effort` wraps the whole call —
   including the `from .email_service import send_welcome` — in `try/except
   Exception` + `log.warning`. Even a broken/unimportable email module cannot
   propagate into the signup response.

3. **Hook layer (webhook):** `_send_billing_email_best_effort` likewise wraps the
   import + send in `try/except Exception` + `log.warning`. It runs **after** the
   commit, so the DB state and the 200 returned to Razorpay are already locked in;
   an email failure cannot change either.

**Ordering guarantees:**
- Signup: email fires only on a *success* return → never sent on a 409/validation
  failure, and never before the user row + JWT exist.
- Webhook: email fires only on a **fresh apply** (the function returns early on
  `deduped` and on unmapped events before reaching the hook) → each receipt /
  failed-notice is sent **at most once per real event**, matching the webhook's
  idempotency contract (CONTRACT §3.2). The `received_at` dedup gate
  (`UNIQUE razorpay_event_id`) guarantees a duplicate Razorpay delivery does not
  re-email.

---

## Deviations / notes for audit

- **Two welcome call-sites, one helper.** `signup_user` has two distinct
  success returns (comp branch returns inside an `else`; blocked branch returns
  at function tail). To send a welcome on both *without* restructuring M5 logic
  (which I'm not authorized to do), I added **one** guarded helper and invoked it
  at each success return. This is the minimal, single-mechanism hook; it does not
  alter any M5 control flow or response shape.
- **Double-guard is intentional.** `send_welcome`/`send_*` already swallow all
  errors internally, yet the hooks wrap them again. The outer wrap exists to
  catch a failure of the `import` statement itself (e.g. a syntax error
  introduced into `email_service.py` later) — defense-in-depth so a future break
  in M8 can never regress signup/webhook.
- **Receipt amount may be absent.** For `subscription.activated` (and any charged
  event whose payload lacks a `payment` entity), `_payment_amount_rupees` returns
  `None`; the template then renders the "₹999 (monthly plan)" label rather than a
  precise figure. Acceptable for a transactional receipt; flagged for awareness.
- **SES creds not validated here.** For `EMAIL_PROVIDER=ses`, `is_configured()`
  only requires provider + sender; boto3 resolves AWS creds via its standard
  chain at send time (a missing-cred failure is swallowed by `_dispatch`). If SES
  is chosen, the operator must also add `boto3` to requirements (noted in the
  requirements.txt comment). Launch default is Resend.
- **Cannot run Python in this env** (no interpreter). Code matches existing
  conventions (lazy import, `from __future__ import annotations`, IST tz,
  `redact_email` logging) read from `auth.py`, `billing_service.py`, `web_push.py`.
  Audit should byte-compile + run a quick best-effort unit test
  (unconfigured-noop, send-raises-is-swallowed) where Python is available.

---

## Env vars (CONTRACT §5 / ENV.md — already documented by M1, none added)

`EMAIL_PROVIDER` (`resend`|`ses`) · `RESEND_API_KEY` (secret) · `EMAIL_FROM`.
No new env var names introduced.
