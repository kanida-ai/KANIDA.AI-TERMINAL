"""Shared FastAPI dependencies for power_user routers.

Three injectable dependencies:
  get_db()           — opens + closes a SQLite connection per request
  current_user_required(Authorization header) → JWTPayload   (401 if missing/invalid)
  current_user_optional(Authorization header) → JWTPayload | None  (no 401)
  require_admin(secret header) → True   (403 if not matching ADMIN_SECRET)

Plus IP-hash rate limiter:
  rate_limit_anon(request, route_key, per_hour) — uses power_user_request_log

All headers are case-insensitive (FastAPI handles via Header dependency).
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, Header, HTTPException, Request

from .. import config
from ..services.auth import AuthError, JWTPayload, verify_jwt

log = logging.getLogger("kanida.power_user.deps")
IST = timezone(timedelta(hours=5, minutes=30))


# ──────────────────────────────────────────────────────────────────────────
#  DB connection per request
# ──────────────────────────────────────────────────────────────────────────

def get_db():
    """Per-request SQLite connection. Closes automatically when request ends.

    `check_same_thread=False` is required because FastAPI evaluates the Depends
    in the request's async event loop thread, but sync handlers run inside
    `starlette.concurrency.run_in_threadpool` on a different worker thread.
    Without this, you get:
        sqlite3.ProgrammingError: SQLite objects created in a thread can only
        be used in that same thread.
    The per-request scope ensures only ONE thread touches each connection at
    a time, so this is safe.
    """
    con = sqlite3.connect(config.POWER_DB_PATH, timeout=30.0,
                           check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


# ──────────────────────────────────────────────────────────────────────────
#  JWT extraction
# ──────────────────────────────────────────────────────────────────────────

def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """Pull the token from 'Authorization: Bearer <jwt>'. Returns None if absent."""
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1]


def current_user_required(
    authorization: Optional[str] = Header(default=None),
) -> JWTPayload:
    """401 if JWT missing/invalid. Use for gated endpoints."""
    token = _extract_bearer(authorization)
    if not token:
        raise HTTPException(401, {"code": "AUTH_REQUIRED",
                                   "message": "Bearer token required"})
    try:
        return verify_jwt(token)
    except AuthError as e:
        raise HTTPException(401, {"code": e.code, "message": e.detail})


def current_user_optional(
    authorization: Optional[str] = Header(default=None),
) -> Optional[JWTPayload]:
    """Returns user if JWT present + valid; None otherwise. No 401.

    Use for public endpoints that want to know whether the caller is a
    logged-in user (e.g. to skip rate-limiting) but don't require it.
    """
    token = _extract_bearer(authorization)
    if not token:
        return None
    try:
        return verify_jwt(token)
    except AuthError:
        return None


# ──────────────────────────────────────────────────────────────────────────
#  Paywall gate — billing-aware (M4)
# ──────────────────────────────────────────────────────────────────────────

# Frontend billing page. Override via env (e.g. point at a Razorpay-hosted
# short-url) without touching code. CONTRACT §3.3 default = "/power/billing".
def _checkout_url() -> str:
    return os.environ.get("POWER_CHECKOUT_URL", "/power/billing")


def _is_allowed_billing(role: Optional[str],
                         billing_plan: Optional[str],
                         subscription_status: Optional[str]) -> bool:
    """The exact allow predicate from CONTRACT §2:

        role == 'admin'
        OR billing_plan IN ('founding', 'comp')
        OR (billing_plan == 'paid' AND subscription_status == 'active')

    Kept as a pure function so it's trivially unit-testable without a DB or
    a request context. Any caller passing the three raw column values gets
    the same verdict the dependency enforces.
    """
    if role == "admin":
        return True
    if billing_plan in ("founding", "comp"):
        return True
    if billing_plan == "paid" and subscription_status == "active":
        return True
    return False


def current_paid_user_required(
    user: JWTPayload = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> JWTPayload:
    """Gate product-data endpoints behind billing.

    Pipeline:
      1. `current_user_required` runs first → 401 if not logged in (unchanged
         behaviour; we only depend on it, never modify it).
      2. Read billing columns for THIS user from power_user_users. The JWT
         may not carry billing_plan / subscription_status (and even if it
         did, a cancelled sub must take effect on the next request, not at
         the next 24h token refresh) — so we always read the DB as the
         source of truth.
      3. Apply the CONTRACT §2 allow predicate.
      4. On deny → HTTP 402 with the CONTRACT §3.3 body.

    Returns the same JWTPayload on success, so handlers can keep using
    `user.user_id` exactly as they do with `current_user_required`.
    """
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT role, billing_plan, subscription_status "
        "FROM power_user_users WHERE id = ?",
        (user.user_id,),
    ).fetchone()

    if row is None:
        # JWT references a user row that no longer exists (deleted/migrated).
        # Treat as unauthenticated rather than payment-required.
        raise HTTPException(401, {"code": "USER_NOT_FOUND",
                                   "message": "Account no longer exists; sign in again"})

    # Prefer the live DB role; fall back to the JWT's role only if the column
    # is somehow NULL (defence in depth — admins must never be paywalled).
    role                = row["role"] or user.role
    billing_plan        = row["billing_plan"]
    subscription_status = row["subscription_status"]

    if _is_allowed_billing(role, billing_plan, subscription_status):
        return user

    log.info("paywall: deny user_id=%s plan=%s status=%s",
             user.user_id, billing_plan, subscription_status)
    raise HTTPException(402, {
        "code":         "PAYMENT_REQUIRED",
        "message":      "An active subscription is required to access this resource.",
        "checkout_url": _checkout_url(),
    })


# ──────────────────────────────────────────────────────────────────────────
#  Admin gate — ADMIN_SECRET header
# ──────────────────────────────────────────────────────────────────────────

def require_admin(
    x_admin_secret: Optional[str] = Header(default=None),
) -> bool:
    """403 if X-Admin-Secret doesn't match POWER_ADMIN_SECRET env var.

    Same gate the operator already uses for /falcon/admin/* routes — reused
    here for /api/power/admin/*.
    """
    expected = config.POWER_ADMIN_SECRET
    if not expected:
        # No admin secret configured at all → fail-closed in prod, fail-open in
        # dev (where it's an explicit choice not to set one)
        raise HTTPException(503, {"code": "ADMIN_NOT_CONFIGURED",
                                    "message": "POWER_ADMIN_SECRET / ADMIN_SECRET not set"})
    if not x_admin_secret or x_admin_secret != expected:
        raise HTTPException(403, {"code": "ADMIN_FORBIDDEN",
                                    "message": "Invalid admin secret"})
    return True


def require_admin_or_jwt(
    request:        Request,
    x_admin_secret: Optional[str] = Header(default=None),
    authorization:  Optional[str] = Header(default=None),
) -> bool:
    """Phase 1b: accept ANY of three admin auth modes. 403 otherwise.

      1. Authorization: Bearer <JWT>          with role='admin' (typical CLI)
      2. Cookie: power_jwt=<JWT>              with role='admin' (browser session)
      3. X-Admin-Secret: <secret>             (legacy ops-script path)

    Use this on admin endpoints that should be reachable from the admin's
    browser session AND from ops scripts. The cookie path is what unlocks
    the /power/admin frontend without re-typing the secret — once the user
    has a session via /auth/invite-login, fetch() with credentials:'include'
    auto-sends the cookie and this dep validates it.
    """
    # Path A — Bearer JWT with admin role
    token = _extract_bearer(authorization)
    if token:
        try:
            payload = verify_jwt(token)
            if payload.role == "admin":
                return True
        except AuthError:
            pass

    # Path B — Cookie JWT (browser session, HTTPOnly so the cookie name
    # matches what frontend/lib/power-auth.ts sets via the /api/power/session
    # route handler).
    cookie_token = request.cookies.get("power_jwt")
    if cookie_token:
        try:
            payload = verify_jwt(cookie_token)
            if payload.role == "admin":
                return True
        except AuthError:
            pass

    # Path C — legacy X-Admin-Secret
    expected = config.POWER_ADMIN_SECRET
    if expected and x_admin_secret and x_admin_secret == expected:
        return True

    if not expected and not token and not cookie_token:
        raise HTTPException(503, {"code": "ADMIN_NOT_CONFIGURED",
                                    "message": "Neither admin JWT nor POWER_ADMIN_SECRET available"})
    raise HTTPException(403, {"code": "ADMIN_FORBIDDEN",
                                "message": "Admin role required"})


# ──────────────────────────────────────────────────────────────────────────
#  IP-hash rate limit (anonymous random replay; brute-force invite)
# ──────────────────────────────────────────────────────────────────────────

def hash_ip_ua(request: Request) -> str:
    """SHA256(ip + user_agent). Never store the raw IP. 16-char hex prefix."""
    ip = (request.client.host if request.client else "") or ""
    ua = request.headers.get("user-agent", "") or ""
    h = hashlib.sha256((ip + "|" + ua).encode("utf-8")).hexdigest()
    return h[:16]


def check_anon_rate_limit(con: sqlite3.Connection,
                           ip_hash: str,
                           route_key: str,
                           per_hour: int,
                           ) -> None:
    """Raise HTTPException(429) if this IP-hash has exceeded `per_hour` calls
    to `route_key` in the last 60 minutes.

    Uses power_user_request_log as the counter — same table that logs every
    request. No separate Redis or cache needed.
    """
    one_hour_ago = (datetime.now(IST) - timedelta(hours=1)).isoformat()
    row = con.execute(
        "SELECT COUNT(*) FROM power_user_request_log "
        "WHERE ip_hash = ? AND route = ? AND created_at >= ?",
        (ip_hash, route_key, one_hour_ago)
    ).fetchone()
    n = row[0] if row else 0
    if n >= per_hour:
        retry_after = 3600   # one hour
        raise HTTPException(429, {
            "code":         "RATE_LIMITED",
            "message":      f"Anonymous users limited to {per_hour} {route_key} calls per hour. "
                            f"Sign in for unlimited access.",
            "retry_after":  retry_after,
        })


def log_request(con: sqlite3.Connection,
                 *,
                 user_id: Optional[int],
                 route: str,
                 status_code: int,
                 latency_ms: int,
                 ip_hash: str,
                 ) -> None:
    """Insert one row into power_user_request_log. Best-effort — never raises."""
    try:
        con.execute("""
            INSERT INTO power_user_request_log
              (user_id, route, status_code, latency_ms, created_at, ip_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, route, status_code, latency_ms,
              datetime.now(IST).isoformat(), ip_hash))
        con.commit()
    except sqlite3.Error as e:
        log.warning("log_request: insert failed: %s", e)
