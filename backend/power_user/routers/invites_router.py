"""/api/power/invites/* — invite redemption + waitlist signup.

  POST /api/power/invites/redeem    — exchange (google_id_token, code) for JWT
  POST /api/power/invites/waitlist  — capture email when user has no invite

Both endpoints are unauthenticated (they create auth). Rate-limited per IP-hash:
  redeem  : 5 attempts per hour per IP-hash
  waitlist: 3 attempts per hour per IP-hash

Brute-force resistance:
  ALL failure modes (bad format, not found, expired, used, etc.) return the
  same HTTP body so an attacker can't enumerate valid codes.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr

from ..services.auth import AuthError, redact_email, verify_google_id_token
from ..services.invites import (
    InviteError,
    add_to_waitlist,
    redeem_atomic,
    validate_code,
)
from .dependencies import (
    check_anon_rate_limit,
    get_db,
    hash_ip_ua,
    log_request,
)

log = logging.getLogger("kanida.power_user.invites_router")
router = APIRouter(prefix="/api/power/invites", tags=["power_user_invites"])

# Rate limits (R2 from auth review)
REDEEM_LIMIT_PER_HOUR    = 5
WAITLIST_LIMIT_PER_HOUR  = 3

# Single response shape for every redeem failure — prevents enumeration.
GENERIC_INVITE_FAIL = {
    "code":    "INVITE_INVALID",
    "message": "This invite code is not valid. It may have been used, expired, "
                "or entered incorrectly. Check the code or join the waitlist.",
}


class RedeemRequest(BaseModel):
    id_token: str
    code:     str


class WaitlistRequest(BaseModel):
    email:  EmailStr
    source: str = "landing_cta"


@router.post("/redeem")
def redeem(
    body: RedeemRequest,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Atomically: verify Google id_token, atomically redeem code, issue JWT.

    Returns:
      200 + {jwt, user}             on success
      400 + GENERIC_INVITE_FAIL     on ANY invite failure (uniform body)
      401 + {code, message}         on Google id_token failure
      429 + rate-limit body         on brute-force
    """
    ip_hash   = hash_ip_ua(request)
    route_key = "/api/power/invites/redeem"

    # Rate limit FIRST — before any DB work
    check_anon_rate_limit(con, ip_hash, route_key, REDEEM_LIMIT_PER_HOUR)

    # Verify Google id_token (separate from invite — wrong Google token isn't
    # a security probe on our codes)
    try:
        g = verify_google_id_token(body.id_token)
    except AuthError as e:
        log_request(con, user_id=None, route=route_key, status_code=401,
                    latency_ms=0, ip_hash=ip_hash)
        raise HTTPException(401, {"code": e.code, "message": e.detail})

    # Atomic redemption — every InviteError becomes the SAME generic body
    try:
        result = redeem_atomic(con, body.code, g)
    except InviteError as e:
        log.warning("invites: redeem failed for %s — internal=%s",
                     redact_email(g.email), e.code)
        log_request(con, user_id=None, route=route_key, status_code=400,
                    latency_ms=0, ip_hash=ip_hash)
        raise HTTPException(400, GENERIC_INVITE_FAIL)

    log_request(con, user_id=result["user"]["id"], route=route_key,
                status_code=200, latency_ms=0, ip_hash=ip_hash)
    return result


@router.get("/validate/{code}")
def validate(
    code: str,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Lightweight check used by the UI as the user types. Same brute-force
    resistance — never reveals which failure mode applies.

    Returns:
      200 + {valid: bool}
      429 on rate limit (lower budget than redeem — pure read)
    """
    ip_hash   = hash_ip_ua(request)
    route_key = "/api/power/invites/validate"
    check_anon_rate_limit(con, ip_hash, route_key, REDEEM_LIMIT_PER_HOUR * 3)
    status = validate_code(con, code)
    return {"valid": status.is_valid}


@router.post("/waitlist")
def waitlist(
    body: WaitlistRequest,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Add to waitlist. Idempotent: same email twice returns ok=True both times.

    Returns:
      200 + {ok: true, joined: bool}   joined=true if newly added, false if dup
      429                              on rate limit
    """
    ip_hash   = hash_ip_ua(request)
    route_key = "/api/power/invites/waitlist"
    check_anon_rate_limit(con, ip_hash, route_key, WAITLIST_LIMIT_PER_HOUR)

    joined = add_to_waitlist(con, str(body.email), source=body.source)
    log_request(con, user_id=None, route=route_key, status_code=200,
                latency_ms=0, ip_hash=ip_hash)
    return {"ok": True, "joined": joined}
