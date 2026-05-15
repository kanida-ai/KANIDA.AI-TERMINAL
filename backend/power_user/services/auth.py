"""Google ID-token verification + internal JWT issuance.

Auth flow (full picture):
    Frontend  → NextAuth Google provider → receives id_token from Google
    Frontend  → POST /api/power/auth/google {id_token}
    Backend   → verify_google_id_token(id_token) → user_info dict
    Backend   → look up email in power_user_users
                ├─ found + is_active → mint internal JWT (24h) → return JWT
                └─ not found        → return {code: NEEDS_INVITE, email}
    Frontend  → if NEEDS_INVITE: route to /redeem-invite
                → POST /api/power/invites/redeem {id_token, code}
                → backend creates user + mints JWT

Why an internal JWT (not just passing Google's id_token through)?
    1. Google ID tokens expire in 1 hour; ours last 24h
    2. We need to embed our own user_id, role — Google's claims don't carry those
    3. Revocation: rotating POWER_JWT_SECRET invalidates every session (Phase 1B)

Security invariants:
    * Google id_token is verified against Google's PUBLIC KEYS (google-auth lib
      handles JWKS fetch + cert validation); never trust client-side parse
    * audience claim MUST match GOOGLE_CLIENT_ID (prevents token-for-other-app
      injection)
    * Internal JWT signed with HS256 using POWER_JWT_SECRET (256-bit random)
    * No id_token or refresh_token stored in DB — we only keep google_sub + email
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import jwt
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

from .. import config

log = logging.getLogger("kanida.power_user.auth")
IST = timezone(timedelta(hours=5, minutes=30))


# ──────────────────────────────────────────────────────────────────────────
#  PII helpers — use in every log line that touches user identity
# ──────────────────────────────────────────────────────────────────────────

def redact_email(e: Optional[str]) -> str:
    """Mask an email for log output. 'alice@example.com' → 'a***e@example.com'.

    Add this to ANY log call that references user identity. Logs are routed
    to file + (later) Sentry/Datadog; we don't want raw PII flowing there.
    """
    if not e or "@" not in e:
        return "***"
    name, dom = e.split("@", 1)
    if len(name) >= 2:
        return f"{name[0]}***{name[-1]}@{dom}"
    return f"***@{dom}"


def redact_sub(s: Optional[str]) -> str:
    """Mask Google sub for logs. Last 4 chars are stable enough for tracing."""
    if not s or len(s) < 6:
        return "***"
    return f"***{s[-4:]}"


# ──────────────────────────────────────────────────────────────────────────
#  Result types
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class GoogleUser:
    """Verified Google account info — never trust unless this came from
    verify_google_id_token()."""
    google_sub:    str       # stable Google account ID — survives email changes
    email:         str
    email_verified: bool
    display_name:  Optional[str]
    picture_url:   Optional[str]


@dataclass
class JWTPayload:
    """Decoded internal JWT — what every authed request gets in its `user`."""
    user_id:      int
    email:        str
    google_sub:   str
    role:         str
    issued_at:    int        # unix seconds
    expires_at:   int


class AuthError(Exception):
    """Raised on token verify / JWT problems. Caller maps to 401/403."""
    def __init__(self, code: str, detail: str = ""):
        self.code   = code   # INVALID_GOOGLE_TOKEN | EXPIRED | WRONG_AUD | JWT_DECODE_FAILED | USER_NOT_FOUND | USER_INACTIVE
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ──────────────────────────────────────────────────────────────────────────
#  Google ID-token verify
# ──────────────────────────────────────────────────────────────────────────

def verify_google_id_token(id_token_str: str,
                            audience: Optional[str] = None
                            ) -> GoogleUser:
    """Verify a Google-issued ID token against Google's JWKS + audience.

    Raises AuthError on any failure. Returns GoogleUser on success.

    Args:
        id_token_str — the raw JWT string from the frontend
        audience     — expected `aud` claim; defaults to config.GOOGLE_CLIENT_ID
    """
    audience = audience or config.GOOGLE_CLIENT_ID
    if not audience:
        raise AuthError("CONFIG_MISSING",
                         "GOOGLE_CLIENT_ID not configured; cannot verify audience")
    try:
        info = google_id_token.verify_oauth2_token(
            id_token_str,
            google_requests.Request(),
            audience,
        )
    except ValueError as e:
        # google-auth raises ValueError for any verification failure
        msg = str(e).lower()
        if "expired" in msg:
            raise AuthError("EXPIRED", str(e))
        if "audience" in msg or "aud" in msg:
            raise AuthError("WRONG_AUD", str(e))
        raise AuthError("INVALID_GOOGLE_TOKEN", str(e))

    # info contains: sub, email, email_verified, name, picture, iss, aud, exp, iat
    iss = info.get("iss", "")
    if iss not in ("accounts.google.com", "https://accounts.google.com"):
        raise AuthError("INVALID_GOOGLE_TOKEN", f"wrong issuer: {iss}")
    if not info.get("email_verified", False):
        raise AuthError("EMAIL_UNVERIFIED",
                         f"Google reports email_verified=false for {info.get('email')}")

    return GoogleUser(
        google_sub     = info["sub"],
        email          = info["email"].lower().strip(),
        email_verified = True,
        display_name   = info.get("name"),
        picture_url    = info.get("picture"),
    )


# ──────────────────────────────────────────────────────────────────────────
#  Internal JWT — issue + verify
# ──────────────────────────────────────────────────────────────────────────

def issue_jwt(user_id: int, email: str, google_sub: str, role: str = "user") -> str:
    """Mint a signed JWT for this user. TTL from config (24h default)."""
    now = int(time.time())
    payload = {
        "user_id":    user_id,
        "email":      email,
        "google_sub": google_sub,
        "role":       role,
        "iat":        now,
        "exp":        now + config.POWER_JWT_TTL_HR * 3600,
        "iss":        "kanida.power_user",
    }
    return jwt.encode(payload, config.POWER_JWT_SECRET, algorithm=config.POWER_JWT_ALG)


def verify_jwt(token: str) -> JWTPayload:
    """Decode + verify an internal JWT. Raises AuthError on failure."""
    if not token:
        raise AuthError("JWT_DECODE_FAILED", "empty token")
    try:
        decoded = jwt.decode(
            token,
            config.POWER_JWT_SECRET,
            algorithms=[config.POWER_JWT_ALG],
            options={"require": ["exp", "iat", "user_id", "email"]},
        )
    except jwt.ExpiredSignatureError:
        raise AuthError("EXPIRED", "JWT expired — sign in again")
    except jwt.InvalidTokenError as e:
        raise AuthError("JWT_DECODE_FAILED", str(e))

    return JWTPayload(
        user_id    = int(decoded["user_id"]),
        email      = decoded["email"],
        google_sub = decoded["google_sub"],
        role       = decoded.get("role", "user"),
        issued_at  = int(decoded["iat"]),
        expires_at = int(decoded["exp"]),
    )


# ──────────────────────────────────────────────────────────────────────────
#  DB lookups (separate from token math so tests can run without DB)
# ──────────────────────────────────────────────────────────────────────────

def find_user_by_google_sub(con: sqlite3.Connection, google_sub: str) -> Optional[Dict[str, Any]]:
    """Look up a user by Google's stable account ID. Returns None if not found."""
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT * FROM power_user_users WHERE google_sub = ?", (google_sub,)
    ).fetchone()
    return dict(row) if row else None


def find_user_by_email(con: sqlite3.Connection, email: str) -> Optional[Dict[str, Any]]:
    """Look up by email (lowercased). For migration-edge-case where google_sub
    changed but email is same — should be rare."""
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT * FROM power_user_users WHERE email = ?", (email.lower().strip(),)
    ).fetchone()
    return dict(row) if row else None


def touch_last_seen(con: sqlite3.Connection, user_id: int) -> None:
    """Update last_seen_at on every authed request. Cheap. Used by /auth/me.

    Scale note: this is one indexed-PK UPDATE per request. Fine for the
    12-18 beta cohort (<200 writes/min). At Phase 2 scale (DAU > 1K) move
    to a background queue that batches updates every 60s — see
    backend/falcon/preflight.py for the cache pattern we'd reuse.
    """
    con.execute(
        "UPDATE power_user_users SET last_seen_at = ? WHERE id = ?",
        (datetime.now(IST).isoformat(), user_id),
    )
    con.commit()


# ──────────────────────────────────────────────────────────────────────────
#  Convenience: full sign-in path (Google verify → DB lookup → JWT or 'needs invite')
# ──────────────────────────────────────────────────────────────────────────

def sign_in_with_google(con: sqlite3.Connection, id_token_str: str) -> Dict[str, Any]:
    """End-to-end sign-in: verify id_token, look up user, return JWT or NEEDS_INVITE.

    Returns one of:
        {"status": "ok", "jwt": "...", "user": {...}}
        {"status": "needs_invite", "email": "...", "display_name": "...", "google_sub": "..."}

    Raises AuthError on Google-side failures (caller maps to HTTP 401).
    """
    g = verify_google_id_token(id_token_str)
    existing = find_user_by_google_sub(con, g.google_sub)
    if existing is None:
        # Fall back to email match in case the user had a prior account.
        # Rare edge case: Google account ownership transfer (Workspace admin
        # reassigns an email from sub A to sub B). The user's email is the
        # same, their Google sub changes. Treat this as the same user —
        # update the row's google_sub instead of creating a duplicate.
        existing = find_user_by_email(con, g.email)
        if existing is not None:
            con.execute(
                "UPDATE power_user_users SET google_sub = ? WHERE id = ?",
                (g.google_sub, existing["id"]),
            )
            con.commit()
            existing["google_sub"] = g.google_sub
            log.info("auth: google_sub updated for %s (sub rotated)",
                     redact_email(existing["email"]))

    if existing is None:
        return {
            "status":       "needs_invite",
            "email":        g.email,
            "display_name": g.display_name,
            "google_sub":   g.google_sub,
            "picture_url":  g.picture_url,
        }

    if not existing.get("is_active"):
        raise AuthError("USER_INACTIVE",
                         f"account {g.email} is deactivated; contact admin")

    token = issue_jwt(
        user_id    = int(existing["id"]),
        email      = existing["email"],
        google_sub = existing["google_sub"],
        role       = existing.get("role") or "user",
    )
    touch_last_seen(con, int(existing["id"]))
    return {
        "status": "ok",
        "jwt":    token,
        "user": {
            "id":           existing["id"],
            "email":        existing["email"],
            "display_name": existing.get("display_name"),
            "picture_url":  existing.get("picture_url"),
            "role":         existing.get("role") or "user",
        },
    }
