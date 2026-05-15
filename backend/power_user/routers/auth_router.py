"""/api/power/auth/* — Google sign-in + JWT issue + /me + logout.

  POST /api/power/auth/google     — exchange Google id_token for our JWT
                                    Returns {status: 'ok', jwt, user}  OR
                                            {status: 'needs_invite', email, ...}
  GET  /api/power/auth/me         — current user from Bearer JWT
  POST /api/power/auth/logout     — clears the cookie (frontend; backend no-op)

Auth flow:
    Frontend gets Google id_token via NextAuth → POSTs it here →
    backend verifies + DB-lookup → returns JWT or NEEDS_INVITE indicator.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..services.auth import AuthError, JWTPayload, redact_email, sign_in_with_google, touch_last_seen
from .dependencies import current_user_required, get_db

log = logging.getLogger("kanida.power_user.auth_router")
router = APIRouter(prefix="/api/power/auth", tags=["power_user_auth"])


class GoogleSignInRequest(BaseModel):
    id_token: str


class LogoutResponse(BaseModel):
    status: str


@router.post("/google")
def sign_in_with_google_endpoint(
    body: GoogleSignInRequest,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Exchange a Google id_token for our internal JWT.

    Returns:
      200 + {status:'ok', jwt:'...', user:{...}}                    if user exists
      200 + {status:'needs_invite', email, google_sub, display_name} if user new
      401 + {code, message}                                         on bad Google token
    """
    try:
        result = sign_in_with_google(con, body.id_token)
    except AuthError as e:
        log.warning("auth: google sign-in failed (%s)", e.code)
        raise HTTPException(401, {"code": e.code, "message": e.detail})

    if result["status"] == "ok":
        log.info("auth: google sign-in OK for %s", redact_email(result["user"]["email"]))
    else:
        log.info("auth: needs_invite for %s", redact_email(result.get("email", "")))
    return result


@router.get("/me")
def get_current_user(
    user: JWTPayload = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Return the current user from JWT. Bumps last_seen_at."""
    row = con.execute(
        "SELECT id, email, display_name, picture_url, role, is_active, created_at "
        "FROM power_user_users WHERE id = ?", (user.user_id,)
    ).fetchone()
    if not row or not row["is_active"]:
        raise HTTPException(403, {"code": "USER_INACTIVE",
                                    "message": "Account deactivated"})
    touch_last_seen(con, user.user_id)
    return {
        "id":           row["id"],
        "email":        row["email"],
        "display_name": row["display_name"],
        "picture_url":  row["picture_url"],
        "role":         row["role"],
        "created_at":   row["created_at"],
    }


@router.post("/logout", response_model=LogoutResponse)
def logout() -> LogoutResponse:
    """Server-side no-op. Frontend clears the JWT cookie.

    To truly invalidate every JWT in flight, rotate POWER_JWT_SECRET on the
    backend — that's the kill switch. Per-user invalidation would need a
    token denylist (Phase 1B if we need it).
    """
    return LogoutResponse(status="ok")
