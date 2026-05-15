"""Magic-link refresh route — Layer 2 endpoint.

Flow:
  1. All 4 morning auth attempts fail → web_push fires notification with
     URL `/power/admin/refresh-token?t=<one-time-token>`
  2. Admin taps notification → frontend page calls
     POST /api/power/auth-refresh/consume {token}
  3. This route validates the token (one-time, 15-min TTL), marks it used,
     triggers an immediate manual auth attempt
  4. Returns the attempt outcome

Security:
  - Token is the SOLE auth — no JWT required. Magic-link IS the auth.
  - Token is consumed atomically (UPDATE WHERE used_at IS NULL).
  - Rate-limited per IP-hash so an attacker can't grind tokens.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from .dependencies import check_anon_rate_limit, get_db, hash_ip_ua, log_request

log = logging.getLogger("kanida.power_user.auth_refresh_router")
router = APIRouter(prefix="/api/power/auth-refresh", tags=["power_user_auth_refresh"])

CONSUME_LIMIT_PER_HOUR = 10   # generous; a single token is one-shot anyway


class ConsumeRequest(BaseModel):
    token: str


@router.post("/consume")
def consume(
    body: ConsumeRequest,
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Validate magic-link, trigger auth attempt, return outcome."""
    ip_hash   = hash_ip_ua(request)
    route_key = "/api/power/auth-refresh/consume"

    # Anonymous rate limit (this endpoint has no JWT)
    check_anon_rate_limit(con, ip_hash, route_key, CONSUME_LIMIT_PER_HOUR)

    from ..services.web_push import consume_magic_link
    result = consume_magic_link(con, body.token,
                                  kind="admin_auth_refresh", ip_hash=ip_hash)
    if not result["ok"]:
        log.warning("magic-link: rejected (%s)", result["reason"])
        log_request(con, user_id=None, route=route_key, status_code=400,
                    latency_ms=0, ip_hash=ip_hash)
        # Same uniform message regardless of internal reason — prevent enumeration
        raise HTTPException(400, {
            "code":    "LINK_INVALID",
            "message": "This refresh link is no longer valid. It may have "
                        "expired (15 min) or been used already. Sign in to "
                        "/power/admin and refresh manually.",
        })

    # Token consumed → trigger immediate auth attempt
    from services.auth_scheduler import trigger_manual
    trigger_manual(trigger_kind="magic_link")

    log_request(con, user_id=None, route=route_key, status_code=200,
                latency_ms=0, ip_hash=ip_hash)
    return {
        "ok":      True,
        "message": "Refresh triggered. The page will reflect new token status "
                    "within ~30 seconds.",
    }
