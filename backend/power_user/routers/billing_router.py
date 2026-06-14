"""/api/power/billing/* — Razorpay subscription endpoints (CONTRACT §3.2).

  POST /api/power/billing/create-subscription  (auth)  → {razorpay_subscription_id, short_url}
  GET  /api/power/billing/status               (auth)  → {billing_plan, subscription_status, current_end}
  POST /api/power/billing/webhook              (NO JWT) → 200; X-Razorpay-Signature verified
  POST /api/power/billing/cancel               (auth)  → {status: 'cancelled'}

Security model:
  * create-subscription / status / cancel are JWT-gated (current_user_required).
  * webhook is PUBLIC (Razorpay calls it server-to-server) but is authenticated
    by HMAC-SHA256 signature against RAZORPAY_WEBHOOK_SECRET — a forged or
    unsigned webhook is rejected with 400 BEFORE any DB state changes.
  * webhook is idempotent: dedupe via power_user_billing_events.razorpay_event_id
    (UNIQUE). Duplicate deliveries are recorded once, applied once, and still
    answered 200 so Razorpay stops retrying.

This router is PUBLIC (no paywall) per CONTRACT §4 — it is the path users take to
GET access, so gating it would be circular.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..services.auth import JWTPayload, redact_email
from ..services import billing_service
from .dependencies import current_user_required, get_db

log = logging.getLogger("kanida.power_user.billing_router")
router = APIRouter(prefix="/api/power/billing", tags=["power_user_billing"])


# ──────────────────────────────────────────────────────────────────────────
#  Response models
# ──────────────────────────────────────────────────────────────────────────

class CreateSubscriptionResponse(BaseModel):
    razorpay_subscription_id: str
    short_url: str


class BillingStatusResponse(BaseModel):
    billing_plan: str
    subscription_status: str
    current_end: Optional[str] = None


class CancelResponse(BaseModel):
    status: str


# ──────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────

def _user_email(con: sqlite3.Connection, user: JWTPayload) -> str:
    """Best email for the user: prefer the live DB row, fall back to JWT claim."""
    row = con.execute(
        "SELECT email, display_name FROM power_user_users WHERE id = ?",
        (user.user_id,),
    ).fetchone()
    if row is not None:
        email = row["email"] if not isinstance(row, tuple) else row[0]
        if email:
            return email
    return user.email


def _user_display_name(con: sqlite3.Connection, user_id: int) -> Optional[str]:
    row = con.execute(
        "SELECT display_name FROM power_user_users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if row is None:
        return None
    return row["display_name"] if not isinstance(row, tuple) else row[0]


# ──────────────────────────────────────────────────────────────────────────
#  POST /create-subscription  (auth)
# ──────────────────────────────────────────────────────────────────────────

@router.post("/create-subscription", response_model=CreateSubscriptionResponse)
def create_subscription(
    user: JWTPayload = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> CreateSubscriptionResponse:
    """Create a Razorpay subscription for the current user.

    Returns {razorpay_subscription_id, short_url}. The user is NOT flipped to
    'paid' here — that happens only when the activated/charged webhook lands.
    """
    email = _user_email(con, user)
    display_name = _user_display_name(con, user.user_id)
    try:
        result = billing_service.create_subscription(
            con, user_id=user.user_id, email=email, display_name=display_name,
        )
    except billing_service.BillingError as e:
        log.warning("billing: create-subscription failed for %s — %s",
                    redact_email(email), e.code)
        raise HTTPException(e.http_status, {"code": e.code, "message": e.detail})

    log.info("billing: subscription created for %s sub=%s",
             redact_email(email), result["razorpay_subscription_id"])
    return CreateSubscriptionResponse(
        razorpay_subscription_id=result["razorpay_subscription_id"],
        short_url=result["short_url"],
    )


# ──────────────────────────────────────────────────────────────────────────
#  GET /status  (auth)
# ──────────────────────────────────────────────────────────────────────────

@router.get("/status", response_model=BillingStatusResponse)
def billing_status(
    user: JWTPayload = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> BillingStatusResponse:
    """Current billing plan + subscription status + access-valid-until.

    `current_end` is the latest subscription's current_end (None if the user has
    no subscription — e.g. founding/comp users).
    """
    row = con.execute(
        "SELECT billing_plan, subscription_status FROM power_user_users WHERE id = ?",
        (user.user_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, {"code": "USER_NOT_FOUND",
                                   "message": "User not found"})
    billing_plan = (row["billing_plan"] if not isinstance(row, tuple) else row[0]) or "blocked"
    sub_status = (row["subscription_status"] if not isinstance(row, tuple) else row[1]) or "inactive"

    end_row = con.execute(
        """
        SELECT current_end
          FROM power_user_subscriptions
         WHERE user_id = ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (user.user_id,),
    ).fetchone()
    current_end = None
    if end_row is not None:
        current_end = end_row["current_end"] if not isinstance(end_row, tuple) else end_row[0]

    return BillingStatusResponse(
        billing_plan=billing_plan,
        subscription_status=sub_status,
        current_end=current_end,
    )


# ──────────────────────────────────────────────────────────────────────────
#  POST /webhook  (NO JWT — signature-verified)
# ──────────────────────────────────────────────────────────────────────────

@router.post("/webhook")
async def razorpay_webhook(
    request: Request,
    x_razorpay_signature: Optional[str] = Header(default=None),
    x_razorpay_event_id: Optional[str] = Header(default=None),
    con: sqlite3.Connection = Depends(get_db),
) -> JSONResponse:
    """Razorpay subscription webhook. PUBLIC but signature-authenticated.

    Flow:
      1. Read the RAW request body (bytes) — needed for HMAC; re-serialized JSON
         would not match Razorpay's signature.
      2. verify_webhook_signature(raw, X-Razorpay-Signature). Reject 400 on fail.
      3. Parse JSON. Derive a stable idempotency key:
           X-Razorpay-Event-Id header if present, else SHA-style fallback built
           from the raw body so duplicate deliveries still collide.
      4. apply_webhook_event(...) — records (UNIQUE event_id) + applies state.
      5. Always return 200 once recorded (even on dedupe) so Razorpay stops
         retrying.

    A forged/unsigned webhook never reaches step 4 — it is rejected at step 2
    with HTTP 400 and no DB write.
    """
    raw_body = await request.body()

    # ── Signature gate (SECURITY BOUNDARY) ──────────────────────────────────
    if not billing_service.verify_webhook_signature(raw_body, x_razorpay_signature):
        log.warning("billing: webhook signature verification FAILED — rejecting")
        raise HTTPException(400, {"code": "INVALID_SIGNATURE",
                                   "message": "Webhook signature verification failed"})

    # ── Parse ───────────────────────────────────────────────────────────────
    try:
        payload: Dict[str, Any] = json.loads(raw_body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        log.warning("billing: webhook body not valid JSON — %s", e)
        raise HTTPException(400, {"code": "INVALID_PAYLOAD",
                                   "message": "Webhook body is not valid JSON"})

    event_type = payload.get("event", "") or ""

    # Idempotency key: prefer Razorpay's per-delivery event id header; fall back
    # to a deterministic hash of the raw body so duplicate deliveries dedupe even
    # without the header. (UNIQUE constraint on razorpay_event_id enforces it.)
    event_id = x_razorpay_event_id
    if not event_id:
        import hashlib
        event_id = "body:" + hashlib.sha256(raw_body).hexdigest()

    # ── Record + apply (idempotent) ─────────────────────────────────────────
    try:
        result = billing_service.apply_webhook_event(
            con, event_id=event_id, event_type=event_type, payload=payload,
        )
    except Exception as e:  # never 500 to Razorpay if avoidable — but if the DB
        # write itself fails, returning non-200 makes Razorpay retry, which is
        # the correct behaviour, so we let it surface as 500.
        log.error("billing: apply_webhook_event failed event=%s — %s",
                  event_type, e)
        raise HTTPException(500, {"code": "WEBHOOK_APPLY_FAILED",
                                   "message": "Failed to process webhook"})

    # Always 200 to Razorpay after recording (even on dedupe) so it stops retrying.
    return JSONResponse(status_code=200, content={
        "status": "ok",
        "deduped": result.get("deduped", False),
        "applied": result.get("applied", False),
    })


# ──────────────────────────────────────────────────────────────────────────
#  POST /cancel  (auth)
# ──────────────────────────────────────────────────────────────────────────

@router.post("/cancel", response_model=CancelResponse)
def cancel(
    user: JWTPayload = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> CancelResponse:
    """Cancel the user's subscription at Razorpay + reflect in DB immediately."""
    try:
        result = billing_service.cancel_subscription(con, user_id=user.user_id)
    except billing_service.BillingError as e:
        log.warning("billing: cancel failed for user_id=%s — %s",
                    user.user_id, e.code)
        raise HTTPException(e.http_status, {"code": e.code, "message": e.detail})

    log.info("billing: cancelled subscription for user_id=%s sub=%s",
             user.user_id, result.get("razorpay_subscription_id"))
    return CancelResponse(status=result["status"])
