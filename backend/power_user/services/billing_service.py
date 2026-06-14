"""Razorpay subscription lifecycle — SDK wrapper + webhook handling.

This module is the ONLY place that talks to Razorpay. The router calls these
functions; everything Razorpay-shaped stays here.

Responsibilities:
  create_customer            — create (or reuse) a Razorpay customer for a user
  create_subscription        — create a Razorpay subscription on RAZORPAY_PLAN_MONTHLY
  verify_webhook_signature   — HMAC-SHA256 verify against RAZORPAY_WEBHOOK_SECRET
  apply_webhook_event        — map a verified Razorpay event → DB state changes
  cancel_subscription        — cancel a subscription at Razorpay

Design notes / invariants:
  * INV5 (no secrets in code): all keys come from env (see config / os.environ).
    Nothing is hard-coded.
  * The `razorpay` SDK is LAZILY imported inside each function that needs it,
    so that importing this module never fails in an environment without the SDK
    or without credentials (this build/test env has neither). Callers get a
    clear BillingConfigError instead of an ImportError at startup.
  * Webhook signature verification is a SECURITY BOUNDARY. It is implemented with
    the stdlib `hmac` + `hashlib` (no SDK dependency) so it works even if the
    Razorpay SDK is absent, and uses hmac.compare_digest (constant-time) to
    avoid timing oracles. A forged/unsigned webhook is rejected.
  * Plan/state mapping obeys CONTRACT §2 exactly (see _EVENT_PLAN_MAP below).
  * All timestamps written in IST ISO-8601 (INV4).

Tables written (CONTRACT §1):
  power_user_users            — billing_plan, subscription_status, razorpay_customer_id
  power_user_subscriptions    — one row per Razorpay subscription
  power_user_billing_events   — webhook audit + idempotency (UNIQUE razorpay_event_id)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("kanida.power_user.billing")
IST = timezone(timedelta(hours=5, minutes=30))

# Launch plan code stored in power_user_subscriptions.plan_code (CONTRACT §1.2).
PLAN_CODE_MONTHLY = "monthly_999"


# ──────────────────────────────────────────────────────────────────────────
#  Errors
# ──────────────────────────────────────────────────────────────────────────

class BillingError(Exception):
    """Base billing error. Router maps `.code` to an HTTP status."""

    def __init__(self, code: str, detail: str = "", http_status: int = 502):
        self.code = code
        self.detail = detail
        self.http_status = http_status
        super().__init__(f"{code}: {detail}")


class BillingConfigError(BillingError):
    """Razorpay SDK or credentials missing/misconfigured.

    Surfaced as 503 (service unavailable) — the operator hasn't finished wiring
    Razorpay env vars yet. Distinct from a user/API error so the audit + ops can
    tell "not configured" apart from "Razorpay rejected the request".
    """

    def __init__(self, detail: str = ""):
        super().__init__("BILLING_NOT_CONFIGURED", detail, http_status=503)


# ──────────────────────────────────────────────────────────────────────────
#  Env / SDK access (lazy, fail-graceful)
# ──────────────────────────────────────────────────────────────────────────

def _env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        raise BillingConfigError(f"{name} is not set")
    return val


def _webhook_secret() -> str:
    """RAZORPAY_WEBHOOK_SECRET — required to verify webhook authenticity."""
    return _env("RAZORPAY_WEBHOOK_SECRET")


def _plan_id() -> str:
    """RAZORPAY_PLAN_MONTHLY — the ₹999/mo plan id created in the dashboard."""
    return _env("RAZORPAY_PLAN_MONTHLY")


def _client():
    """Build a Razorpay client from env credentials.

    Lazy-imports the `razorpay` SDK so this module imports cleanly in an
    environment without the package (this build env). Raises BillingConfigError
    — never a bare ImportError — when the SDK or keys are absent.
    """
    try:
        import razorpay  # type: ignore
    except ImportError as e:
        raise BillingConfigError(
            "razorpay SDK not installed (pip install razorpay)"
        ) from e

    key_id = _env("RAZORPAY_KEY_ID")
    key_secret = _env("RAZORPAY_KEY_SECRET")
    try:
        return razorpay.Client(auth=(key_id, key_secret))
    except Exception as e:  # pragma: no cover - defensive
        raise BillingConfigError(f"failed to init Razorpay client: {e}") from e


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


# ──────────────────────────────────────────────────────────────────────────
#  Razorpay timestamp → IST ISO
# ──────────────────────────────────────────────────────────────────────────

def _epoch_to_ist_iso(epoch: Optional[Any]) -> Optional[str]:
    """Razorpay sends current_start / current_end as unix epoch seconds.

    Convert to IST ISO-8601 (INV4). Returns None on missing/garbage input.
    """
    if epoch in (None, "", 0):
        return None
    try:
        return datetime.fromtimestamp(int(epoch), IST).isoformat()
    except (ValueError, TypeError, OverflowError, OSError):
        return None


# ──────────────────────────────────────────────────────────────────────────
#  Customer
# ──────────────────────────────────────────────────────────────────────────

def create_customer(con: sqlite3.Connection, *, user_id: int, email: str,
                    display_name: Optional[str] = None) -> str:
    """Create (or reuse) a Razorpay customer for this user.

    Returns the razorpay_customer_id and persists it on power_user_users.
    If the user already has a razorpay_customer_id, returns it without a second
    Razorpay call (idempotent).
    """
    row = con.execute(
        "SELECT razorpay_customer_id FROM power_user_users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if row is None:
        raise BillingError("USER_NOT_FOUND", f"no power_user_users row {user_id}",
                           http_status=404)
    existing = row["razorpay_customer_id"] if not isinstance(row, tuple) else row[0]
    if existing:
        return existing

    client = _client()
    try:
        customer = client.customer.create({
            "email": email,
            "name": display_name or email,
            # fail_existing=0 → return the existing customer instead of erroring
            # if Razorpay already has one for this email.
            "fail_existing": 0,
        })
    except BillingError:
        raise
    except Exception as e:
        raise BillingError("RAZORPAY_CUSTOMER_FAILED", str(e)) from e

    customer_id = customer.get("id")
    if not customer_id:
        raise BillingError("RAZORPAY_CUSTOMER_FAILED",
                           "Razorpay returned no customer id")

    con.execute(
        "UPDATE power_user_users SET razorpay_customer_id = ? WHERE id = ?",
        (customer_id, user_id),
    )
    con.commit()
    return customer_id


# ──────────────────────────────────────────────────────────────────────────
#  Subscription create
# ──────────────────────────────────────────────────────────────────────────

def create_subscription(con: sqlite3.Connection, *, user_id: int, email: str,
                        display_name: Optional[str] = None,
                        total_count: int = 120) -> Dict[str, Any]:
    """Create a Razorpay subscription on the monthly plan and record it.

    Steps:
      1. ensure a Razorpay customer exists for the user
      2. create a Razorpay subscription on RAZORPAY_PLAN_MONTHLY
      3. INSERT a power_user_subscriptions row (status from Razorpay, usually
         'created'); plan_code = 'monthly_999'

    Returns {razorpay_subscription_id, short_url, status}.

    `total_count` is the number of billing cycles Razorpay will attempt; 120
    months (~10y) is effectively "until cancelled" — Razorpay requires a finite
    count for subscriptions.

    NOTE: This does NOT flip the user to 'paid'. Access is granted only when the
    `subscription.activated` / `subscription.charged` webhook arrives and is
    signature-verified (CONTRACT §2 — never trust the client / pre-payment).
    """
    customer_id = create_customer(con, user_id=user_id, email=email,
                                  display_name=display_name)

    client = _client()
    plan_id = _plan_id()
    try:
        sub = client.subscription.create({
            "plan_id": plan_id,
            "customer_id": customer_id,
            "customer_notify": 1,
            "total_count": total_count,
            "notes": {"power_user_id": str(user_id)},
        })
    except BillingError:
        raise
    except Exception as e:
        raise BillingError("RAZORPAY_SUBSCRIPTION_FAILED", str(e)) from e

    sub_id = sub.get("id")
    short_url = sub.get("short_url") or ""
    status = sub.get("status") or "created"
    if not sub_id:
        raise BillingError("RAZORPAY_SUBSCRIPTION_FAILED",
                           "Razorpay returned no subscription id")

    now = _now_ist_iso()
    con.execute(
        """
        INSERT INTO power_user_subscriptions
            (user_id, razorpay_subscription_id, plan_code, status,
             current_start, current_end, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id, sub_id, PLAN_CODE_MONTHLY, status,
            _epoch_to_ist_iso(sub.get("current_start")),
            _epoch_to_ist_iso(sub.get("current_end")),
            now, now,
        ),
    )
    con.commit()

    return {
        "razorpay_subscription_id": sub_id,
        "short_url": short_url,
        "status": status,
    }


# ──────────────────────────────────────────────────────────────────────────
#  Subscription cancel
# ──────────────────────────────────────────────────────────────────────────

def cancel_subscription(con: sqlite3.Connection, *, user_id: int) -> Dict[str, Any]:
    """Cancel this user's most recent active-ish Razorpay subscription.

    Reflects the cancellation in the DB immediately (status='cancelled',
    user.subscription_status='cancelled', billing_plan='blocked') so the gate
    closes without waiting for the webhook. The webhook (subscription.cancelled)
    is still the source of truth and will re-affirm this state idempotently.
    """
    row = con.execute(
        """
        SELECT razorpay_subscription_id
          FROM power_user_subscriptions
         WHERE user_id = ?
           AND razorpay_subscription_id IS NOT NULL
           AND status NOT IN ('cancelled', 'completed', 'expired')
         ORDER BY id DESC
         LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    if row is None:
        raise BillingError("NO_ACTIVE_SUBSCRIPTION",
                           "no cancellable subscription for this user",
                           http_status=404)
    sub_id = row["razorpay_subscription_id"] if not isinstance(row, tuple) else row[0]

    client = _client()
    try:
        # cancel_at_cycle_end=0 → cancel immediately.
        client.subscription.cancel(sub_id, {"cancel_at_cycle_end": 0})
    except BillingError:
        raise
    except Exception as e:
        raise BillingError("RAZORPAY_CANCEL_FAILED", str(e)) from e

    now = _now_ist_iso()
    con.execute(
        """
        UPDATE power_user_subscriptions
           SET status = 'cancelled', updated_at = ?
         WHERE razorpay_subscription_id = ?
        """,
        (now, sub_id),
    )
    con.execute(
        """
        UPDATE power_user_users
           SET subscription_status = 'cancelled', billing_plan = 'blocked'
         WHERE id = ?
        """,
        (user_id,),
    )
    con.commit()
    return {"status": "cancelled", "razorpay_subscription_id": sub_id}


# ──────────────────────────────────────────────────────────────────────────
#  Webhook signature verification — SECURITY BOUNDARY
# ──────────────────────────────────────────────────────────────────────────

def verify_webhook_signature(raw_body: bytes, signature: Optional[str]) -> bool:
    """Verify a Razorpay webhook's X-Razorpay-Signature header.

    Razorpay computes HMAC-SHA256(raw_request_body, webhook_secret) and sends it
    hex-encoded in X-Razorpay-Signature. We recompute over the EXACT raw bytes
    (never the re-serialized JSON — re-serialization changes whitespace/key order
    and breaks the HMAC) and compare in constant time.

    Returns True only if the signature is present, well-formed, and matches.
    Any forged or unsigned webhook returns False → router rejects with 400.

    Implemented with stdlib hmac/hashlib (no SDK dependency) so signature
    verification works even when the razorpay package is absent.
    """
    if not signature:
        return False
    try:
        secret = _webhook_secret().encode("utf-8")
    except BillingConfigError:
        # No secret configured → cannot verify → reject (fail-closed).
        log.error("billing: RAZORPAY_WEBHOOK_SECRET not set; rejecting webhook")
        return False

    expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(expected, signature.strip())
    except (TypeError, ValueError):
        return False


# ──────────────────────────────────────────────────────────────────────────
#  Event → DB state mapping (CONTRACT §2)
# ──────────────────────────────────────────────────────────────────────────
#
# Maps a Razorpay subscription event_type → (subscription_status, billing_plan).
#   billing_plan = None  →  do NOT change billing_plan (leave as-is)
#
# Access ends (billing_plan='blocked') on halted/cancelled/completed/expired.
# Access granted (billing_plan='paid', status 'active') on activated/charged.
#
_EVENT_PLAN_MAP: Dict[str, Tuple[str, Optional[str]]] = {
    # (subscription_status, billing_plan-or-None)
    "subscription.activated":      ("active",    "paid"),
    "subscription.charged":        ("active",    "paid"),
    "subscription.authenticated":  ("authenticated", None),   # auth done, not yet paid
    "subscription.halted":         ("halted",    "blocked"),  # retries exhausted
    "subscription.cancelled":      ("cancelled", "blocked"),
    "subscription.completed":      ("completed", "blocked"),  # plan finished
    "subscription.expired":        ("expired",   "blocked"),
    "subscription.pending":        ("halted",    None),       # payment failed, retrying
}


def _extract_subscription_entity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dig the subscription entity out of a Razorpay webhook payload.

    Shape: payload['payload']['subscription']['entity'] = {id, status, ...}.
    Returns {} if absent (e.g. a non-subscription event we ignore).
    """
    try:
        return (payload.get("payload", {})
                       .get("subscription", {})
                       .get("entity", {})) or {}
    except AttributeError:
        return {}


# M8 email hook — which webhook events trigger which transactional email.
# Receipt on access-granting events; failed/recovery on access-ending events.
# Anything else (authenticated/pending/completed/expired/unmapped) sends nothing.
_EMAIL_RECEIPT_EVENTS = frozenset({"subscription.charged", "subscription.activated"})
_EMAIL_FAILED_EVENTS = frozenset({"subscription.halted", "subscription.cancelled"})


def _payment_amount_rupees(payload: Dict[str, Any]) -> Optional[float]:
    """Best-effort extract the charged amount (in RUPEES) from a webhook payload.

    Razorpay sends amounts in PAISE. For subscription.charged the payment entity
    lives at payload['payload']['payment']['entity']['amount']. Returns None if
    absent/garbage (the receipt template then shows the plan-price label).
    """
    try:
        amt = (payload.get("payload", {})
                      .get("payment", {})
                      .get("entity", {})
                      .get("amount"))
    except AttributeError:
        return None
    if amt in (None, ""):
        return None
    try:
        return int(amt) / 100.0
    except (ValueError, TypeError):
        return None


def _email_for_user(con: sqlite3.Connection, user_id: Optional[int]) -> Optional[str]:
    """Look up a user's email for the transactional-email hook. None if unknown."""
    if user_id is None:
        return None
    row = con.execute(
        "SELECT email FROM power_user_users WHERE id = ?", (user_id,)
    ).fetchone()
    if row is None:
        return None
    return row["email"] if not isinstance(row, tuple) else row[0]


def _send_billing_email_best_effort(con: sqlite3.Connection, *,
                                    user_id: Optional[int], event_type: str,
                                    payload: Dict[str, Any],
                                    period_end: Optional[str]) -> None:
    """M8 hook — fire receipt / payment-failed email after a webhook applies.

    BEST-EFFORT: every failure (no email on file, unconfigured provider, SDK
    absent, provider error) is swallowed + logged. This NEVER raises and NEVER
    affects the 200 the router returns to Razorpay. Only called on a FRESH apply
    (deduped events skip it, so each email fires at most once per real event).

    No email is sent to founding/comp users — they never generate Razorpay
    subscription events, so this path only ever runs for actual payers.
    """
    try:
        if event_type not in _EMAIL_RECEIPT_EVENTS and event_type not in _EMAIL_FAILED_EVENTS:
            return
        email = _email_for_user(con, user_id)
        if not email:
            log.info("billing: email hook skipped — no email for user_id=%s "
                     "(event=%s)", user_id, event_type)
            return
        from .email_service import send_payment_failed, send_payment_receipt
        if event_type in _EMAIL_RECEIPT_EVENTS:
            send_payment_receipt(email, _payment_amount_rupees(payload), period_end)
        else:
            send_payment_failed(email)
    except Exception as e:  # noqa: BLE001 — email is best-effort, never breaks the webhook
        log.warning("billing: email hook failed event=%s user_id=%s — %s",
                    event_type, user_id, e)


def _find_user_id_for_subscription(con: sqlite3.Connection,
                                   sub_entity: Dict[str, Any]) -> Optional[int]:
    """Resolve which power_user the event belongs to.

    Primary: match power_user_subscriptions.razorpay_subscription_id.
    Fallback: the notes.power_user_id we stamped at create_subscription time
    (covers the race where the webhook beats our own INSERT).
    """
    sub_id = sub_entity.get("id")
    if sub_id:
        row = con.execute(
            "SELECT user_id FROM power_user_subscriptions "
            "WHERE razorpay_subscription_id = ?",
            (sub_id,),
        ).fetchone()
        if row is not None:
            return row["user_id"] if not isinstance(row, tuple) else row[0]

    notes = sub_entity.get("notes") or {}
    if isinstance(notes, dict) and notes.get("power_user_id"):
        try:
            return int(notes["power_user_id"])
        except (ValueError, TypeError):
            return None
    return None


def apply_webhook_event(con: sqlite3.Connection, *, event_id: str,
                        event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Record + apply a verified Razorpay webhook event. IDEMPOTENT.

    Order of operations (idempotency is the contract):
      1. Try INSERT into power_user_billing_events with UNIQUE razorpay_event_id.
         If it already exists (IntegrityError), this is a duplicate delivery →
         return {deduped: True} WITHOUT re-applying state. The router still
         returns 200 to Razorpay.
      2. Resolve the user_id from the subscription entity.
      3. Upsert the matching power_user_subscriptions row (status, period).
      4. Apply the CONTRACT §2 plan/state mapping to power_user_users.

    Caller MUST have already verified the signature. This function trusts the
    payload.

    Returns {deduped, applied, user_id, event_type, subscription_status,
             billing_plan}.
    """
    sub_entity = _extract_subscription_entity(payload)
    user_id = _find_user_id_for_subscription(con, sub_entity)
    now = _now_ist_iso()

    # ── 1. Idempotency gate — UNIQUE razorpay_event_id ──────────────────────
    try:
        con.execute(
            """
            INSERT INTO power_user_billing_events
                (user_id, razorpay_event_id, event_type, payload, received_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, event_id, event_type, json.dumps(payload), now),
        )
        con.commit()
    except sqlite3.IntegrityError:
        # Duplicate delivery — already recorded + applied. Do NOT re-apply.
        con.rollback()
        log.info("billing: dedup webhook event_id=%s type=%s", event_id, event_type)
        return {"deduped": True, "applied": False, "user_id": user_id,
                "event_type": event_type}

    # ── 2. Map event → desired state ────────────────────────────────────────
    mapping = _EVENT_PLAN_MAP.get(event_type)
    if mapping is None:
        # Unmapped event (e.g. payment.captured, or a future event). Recorded
        # for audit but no state change. Still a success (200).
        log.info("billing: recorded unmapped event_id=%s type=%s (no-op)",
                 event_id, event_type)
        return {"deduped": False, "applied": False, "user_id": user_id,
                "event_type": event_type}

    new_status, new_plan = mapping
    sub_id = sub_entity.get("id")

    # ── 3. Upsert the subscription row ──────────────────────────────────────
    if sub_id:
        cur_start = _epoch_to_ist_iso(sub_entity.get("current_start"))
        cur_end = _epoch_to_ist_iso(sub_entity.get("current_end"))
        existing = con.execute(
            "SELECT id FROM power_user_subscriptions "
            "WHERE razorpay_subscription_id = ?",
            (sub_id,),
        ).fetchone()
        if existing is not None:
            con.execute(
                """
                UPDATE power_user_subscriptions
                   SET status = ?,
                       current_start = COALESCE(?, current_start),
                       current_end   = COALESCE(?, current_end),
                       updated_at = ?
                 WHERE razorpay_subscription_id = ?
                """,
                (new_status, cur_start, cur_end, now, sub_id),
            )
        elif user_id is not None:
            # Webhook arrived before our own create INSERT (race) — create the row.
            con.execute(
                """
                INSERT INTO power_user_subscriptions
                    (user_id, razorpay_subscription_id, plan_code, status,
                     current_start, current_end, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, sub_id, PLAN_CODE_MONTHLY, new_status,
                 cur_start, cur_end, now, now),
            )

    # ── 4. Apply plan/state to the user (CONTRACT §2) ───────────────────────
    if user_id is not None:
        if new_plan is not None:
            con.execute(
                """
                UPDATE power_user_users
                   SET subscription_status = ?, billing_plan = ?
                 WHERE id = ?
                """,
                (new_status, new_plan, user_id),
            )
        else:
            con.execute(
                "UPDATE power_user_users SET subscription_status = ? WHERE id = ?",
                (new_status, user_id),
            )
    con.commit()

    log.info("billing: applied event_id=%s type=%s user_id=%s status=%s plan=%s",
             event_id, event_type, user_id, new_status, new_plan)

    # ── M8 email hook — best-effort, AFTER the DB state change is committed.
    # Guarded so it never raises into the webhook / never affects the 200 to
    # Razorpay. Only runs here (fresh apply), so dedup'd events never re-email.
    _send_billing_email_best_effort(
        con, user_id=user_id, event_type=event_type, payload=payload,
        period_end=_epoch_to_ist_iso(sub_entity.get("current_end")),
    )

    return {
        "deduped": False,
        "applied": True,
        "user_id": user_id,
        "event_type": event_type,
        "subscription_status": new_status,
        "billing_plan": new_plan,
    }
