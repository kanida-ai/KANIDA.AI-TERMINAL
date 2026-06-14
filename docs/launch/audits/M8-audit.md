# M8 — Email (transactional) — Audit

**Auditor:** AuditAgent-M8
**Date:** 2026-06-13 (IST)
**Scope:** `email_service.py`, `email_templates/*`, the guarded email hooks in `auth.py` (signup) + `billing_service.py` (webhook apply), `requirements.txt`.
**Method:** READ-ONLY. Source inspection + git diff. (No Python interpreter in env — byte-compile not run; code verified by inspection, consistent with build-log note.)

---

## VERDICT: GREEN

No path can let an email failure break signup or the webhook; no email fires on a failed/duplicate event; founding/comp members get nothing; all invariants hold.

---

## Checklist findings (cited)

### 1. Best-effort guarantee (THE critical rule) — PASS
Three independent guard layers, every failure mode swallowed:

- **Service `_dispatch` choke point** (`email_service.py:147-194`): all three public `send_*` funnel through `_dispatch`. Empty recipient → return False (`:158-160`); unconfigured → return False (`:162-165`); render failure → return False (`_render` catches `OSError/UnicodeDecodeError` at `:100` and `KeyError/IndexError/ValueError` at `:105`, returns None → `:168-169`); provider send wrapped in `except ImportError` (`:184`) **and** bare `except Exception` (`:188`). `_dispatch` returns bool, **never raises**.
- **Signup hook site** (`auth.py:565-571`): `_send_welcome_best_effort` wraps the `from .email_service import send_welcome` import **and** the call in `try / except Exception` + `log.warning`. A broken/unimportable `email_service` cannot propagate into `signup_user`. Calls are at `:623` (comp branch) and `:657` (blocked branch), both at success returns.
- **Webhook hook site** (`billing_service.py:436-465`): `_send_billing_email_best_effort` wraps event-type filtering, email lookup, the `from .email_service import ...` import, and the send — all in `try / except Exception` + `log.warning`. Invoked at `:607-610`, **after `con.commit()` at `:599`**. Router (`billing_router.py:218`) calls `apply_webhook_event` inside its own try/except; the email hook runs after the committed state and the to-be-returned 200 are already locked in, so it cannot influence either.

No circular-import risk: `email_service` imports `redact_email` from `.auth` at module level, but `auth.py` imports `email_service` only *inside* the hook function (lazy), never at module load.

### 2. Hook placement / ordering — PASS
- **Webhook:** email fires only after the function reaches the fresh-apply tail. It returns early on dedupe (`:529-534`, IntegrityError on UNIQUE `razorpay_event_id`) and on unmapped events (`:537-544`) — both *before* the hook at `:607`. So a duplicate Razorpay delivery and any unmapped event send nothing. Fires after `con.commit()`.
- **Signup:** both `_send_welcome_best_effort` calls sit at success returns (`:623`, `:657`), after the user row + JWT exist. The EMAIL_EXISTS duplicate guard (`:617-619`) and the IntegrityError race (`:649-654`) raise `SignupError` *before* reaching either return → no welcome on a failed/duplicate signup.

### 3. No emails to founding — PASS
- Welcome only fires inside `signup_user` (the open-signup path). Founding members authenticate via invite-login (`sign_in_with_email_and_code`), which has no welcome hook. Confirmed founding never enters `signup_user`.
- Receipt/failed only fire from Razorpay webhook events. Founding/comp users generate no Razorpay subscriptions, so the webhook path never resolves to them. Hook docstring (`:447-448`) states this; code path confirms it.

### 4. State mapping — PASS, matches M3
`_EMAIL_RECEIPT_EVENTS = {subscription.charged, subscription.activated}` (`:398`); `_EMAIL_FAILED_EVENTS = {subscription.halted, subscription.cancelled}` (`:399`). All other types (authenticated/pending/completed/expired/unmapped) → `_send_billing_email_best_effort` returns at `:451-452` with no send. The receipt set aligns with M3's `_EVENT_PLAN_MAP` access-granting rows (`activated/charged → paid`, `:370-371`); the failed set aligns with two of the access-ending rows. `completed`/`expired` deliberately send no email (access-ending but not user-actionable as "payment failed") — acceptable and consistent with the documented intent.

### 5. Cross-module edits minimal — PASS
`git diff HEAD` on `auth.py`, `billing_service.py`, `requirements.txt` reviewed.
- **auth.py:** the diff includes the whole `signup_user` function because M5 is uncommitted (all M1–M8 work sits on top of `d68c6f6`), but the M8-attributable additions are exactly: the `_send_welcome_best_effort` helper (`:565-571`) and its two call lines (`:623`, `:657`). No M5 control flow or response shape is altered by these — they are pure side-effect calls at existing return points. (`hashlib` import + `_synthetic_sub_for_email_hashed` + `SignupError`/`signup_user` body are M5's, not M8's.)
- **billing_service.py:** M8 additions are `_EMAIL_*` sets, `_payment_amount_rupees`, `_email_for_user`, `_send_billing_email_best_effort`, and the single hook call at `:607-610`. The hook call is appended after the existing commit and before the existing return — no change to M3 mapping, upsert, or return shape.
- **requirements.txt:** adds `resend>=2.0.0` (M8) plus `razorpay`/`playwright`/`pyotp` (M3/M1 — not M8's, but additive and harmless). No other files in `git status` belong to M8.

### 6. Invariants — PASS
- **INV2 (no falcon/*):** no edits under `backend/falcon/*`. ✅
- **INV4 (IST):** `email_service._format_period_end` (`:257-271`) renders via `Asia/Kolkata` `IST = timezone(+05:30)`; `billing_service._epoch_to_ist_iso` (`:127-138`) feeds the period_end in IST. ✅
- **INV5 (no secrets):** `RESEND_API_KEY`, `EMAIL_FROM`, `EMAIL_PROVIDER` read from `os.environ` only (`:59-84`, `:119`); nothing hard-coded. ✅
- **INV6 (power_user/ + requirements only):** all artifacts under `backend/power_user/` plus root `requirements.txt`. ✅
- **PII redaction:** every email log line masks via `redact_email` (`email_service.py:163,190,193`; `auth.py` hook `:570`). The billing hook logs `user_id` (not the address) at `:455,464`. ✅

---

## Confirmation (explicit)

**Email failure cannot break signup or the webhook.** Verified at all three layers: (1) `_dispatch` returns a bool and swallows every unconfigured/import/render/provider error; (2) the signup hook wraps the import + call in try/except, so even an unimportable `email_service` cannot reach the signup response; (3) the webhook hook wraps import + send in try/except and runs only *after* `con.commit()`, so the committed DB state and the 200 to Razorpay are immune. The webhook email fires only on a fresh, mapped apply (not on dedupe or unmapped events); the welcome fires only on a successful new signup. Founding/comp members receive nothing.

## Must-fix list
None (GREEN).

## Non-blocking observations (advisory only — not gating)
1. `requirements.txt` carries M1/M3 deps (`razorpay`, `playwright`, `pyotp`) alongside M8's `resend`. Additive and correct, just not M8-owned — noted so the orchestrator doesn't attribute them to this module.
2. Byte-compile / unit smoke-test (unconfigured-noop, send-raises-is-swallowed) could not be run here (no interpreter). Recommend running it in CI where Python is available, per the build-log note. Code is syntactically sound by inspection.
3. `subscription.completed` / `subscription.expired` intentionally send no email despite ending access. This matches the documented design (only charged/activated → receipt, halted/cancelled → failed). Confirm with product that a lapsed-on-completion user needs no notice; not a defect.
