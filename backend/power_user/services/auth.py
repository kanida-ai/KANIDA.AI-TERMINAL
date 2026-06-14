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

import hashlib
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


# ──────────────────────────────────────────────────────────────────────────
#  Phase 1b: invite-code (email-only, NO Google OAuth) sign-in path
# ──────────────────────────────────────────────────────────────────────────

# Synthetic google_sub for invite-code users — keeps the NOT-NULL constraint
# satisfied and stays uniquely tied to email. If a user later signs in via
# Google with the same email, sign_in_with_google() updates google_sub in
# place (see "google_sub rotated" path above) and the two flows reconcile.
def _synthetic_sub_for_email(email: str) -> str:
    return f"email:{email.lower().strip()}"


# Launch M5 (open email signup): the CONTRACT §3.1 mandates a HASHED synthetic
# sub of form  'email:' + sha256(lowercased email)  for users created via the
# open signup endpoint. This keeps the (dev) google_sub NOT-NULL + UNIQUE
# constraint satisfied without exposing the raw email inside the sub column.
#
# NOTE — two synthetic-sub forms now coexist, deliberately:
#   * _synthetic_sub_for_email()        → 'email:<raw>'    (legacy invite-login,
#                                          redeem_with_email; unchanged)
#   * _synthetic_sub_for_email_hashed() → 'email:<sha256>' (open signup, M5)
# Both are stable functions of the (lowercased) email, so each email maps to
# exactly ONE sub within each scheme — no collision risk inside a scheme. A
# given email is only ever created through ONE path (signup OR invite redeem),
# so a single user never needs both. If such a user later signs in via Google
# with the same email, sign_in_with_google()'s email-fallback rotates google_sub
# to the real Google sub regardless of which synthetic form was stored.
def _synthetic_sub_for_email_hashed(email: str) -> str:
    digest = hashlib.sha256(email.lower().strip().encode("utf-8")).hexdigest()
    return f"email:{digest}"


def _normalize_email(email: str) -> str:
    """Lowercase + strip. Raises AuthError if structurally invalid."""
    e = (email or "").lower().strip()
    if "@" not in e or len(e) < 5 or len(e) > 254:
        raise AuthError("EMAIL_INVALID", "Email format invalid")
    return e


def bootstrap_admin_user(con: sqlite3.Connection) -> Dict[str, Any]:
    """Idempotent: ensure the admin user row exists with role='admin'.

    Called at backend startup. The admin's email is operator-locked in
    config.POWER_ADMIN_EMAIL. If a row already exists for that email but with
    role != 'admin' (e.g. it was created as 'user' before the admin email
    was set), promote it. Returns the resulting user dict.
    """
    admin_email = config.POWER_ADMIN_EMAIL
    if not admin_email:
        log.warning("bootstrap_admin_user: POWER_ADMIN_EMAIL not set; skipping")
        return {}

    existing = find_user_by_email(con, admin_email)
    if existing is None:
        # Create fresh admin row. Synthetic google_sub so the NOT-NULL UNIQUE
        # constraint passes — gets overwritten if the admin later signs in
        # via Google with the same email.
        now_iso = datetime.now(IST).isoformat()
        con.execute("""
            INSERT INTO power_user_users
              (email, google_sub, display_name, picture_url,
               invite_code, role, is_active, created_at)
            VALUES (?, ?, ?, ?, NULL, 'admin', 1, ?)
        """, (admin_email, _synthetic_sub_for_email(admin_email),
              "Admin", None, now_iso))
        con.commit()
        log.info("bootstrap_admin_user: created admin row for %s",
                 redact_email(admin_email))
        return find_user_by_email(con, admin_email) or {}

    if existing.get("role") != "admin" or not existing.get("is_active"):
        con.execute(
            "UPDATE power_user_users SET role='admin', is_active=1 WHERE id = ?",
            (existing["id"],),
        )
        con.commit()
        log.info("bootstrap_admin_user: promoted %s to admin",
                 redact_email(admin_email))
        return find_user_by_email(con, admin_email) or {}

    log.debug("bootstrap_admin_user: %s already admin",
              redact_email(admin_email))
    return existing


def sign_in_with_email_and_code(
    con: sqlite3.Connection,
    email: str,
    code: str,
) -> Dict[str, Any]:
    """End-to-end invite-code sign-in (Phase 1b).

    Two paths:
      1. ADMIN: if email == POWER_ADMIN_EMAIL AND code == POWER_ADMIN_SECRET,
         bypass the invite_codes table entirely. The admin row is bootstrapped
         on startup; we just issue an admin JWT.

      2. REGULAR: invite_codes lookup + atomic redeem (or re-login if the
         user already exists). For existing users, we accept any unused valid
         code as "proof of identity continuity" — but they can also re-login
         with the same code they redeemed originally (idempotent on second
         use is fine since the user row already exists).

         Actually: existing users don't need a code at all on subsequent
         logins — they just type their email. Their existence in the DB IS
         their authentication for v1 (this is invite-only; the bar is "did
         the admin already create this row?"). New users need a fresh unused
         code, which gets consumed on first login.

    Returns:
        {"status": "ok", "jwt": "...", "user": {...}}

    Raises AuthError. The router maps all failures to the same generic body
    to prevent enumeration of valid email addresses.
    """
    from .invites import (
        InviteError,
        redeem_with_email,
    )

    email = _normalize_email(email)
    code = (code or "").strip()

    # ── Path 1: admin bypass ────────────────────────────────────────────
    if (config.POWER_ADMIN_EMAIL
            and email == config.POWER_ADMIN_EMAIL
            and code
            and config.POWER_ADMIN_SECRET
            and code == config.POWER_ADMIN_SECRET):
        # Make sure the admin row exists + is admin (defence in depth — startup
        # bootstrap should already have done this).
        admin = bootstrap_admin_user(con)
        if not admin:
            raise AuthError("ADMIN_BOOTSTRAP_FAILED",
                             "Could not provision admin row")
        token = issue_jwt(
            user_id    = int(admin["id"]),
            email      = admin["email"],
            google_sub = admin["google_sub"],
            role       = "admin",
        )
        touch_last_seen(con, int(admin["id"]))
        log.info("auth: admin login OK for %s", redact_email(email))
        return {
            "status": "ok",
            "jwt":    token,
            "user": {
                "id":           admin["id"],
                "email":        admin["email"],
                "display_name": admin.get("display_name") or "Admin",
                "picture_url":  admin.get("picture_url"),
                "role":         "admin",
            },
        }

    # ── Path 2: existing user re-login (no code needed) ─────────────────
    existing = find_user_by_email(con, email)
    if existing is not None:
        if not existing.get("is_active"):
            raise AuthError("USER_INACTIVE",
                             f"Account is deactivated; contact admin")
        token = issue_jwt(
            user_id    = int(existing["id"]),
            email      = existing["email"],
            google_sub = existing["google_sub"],
            role       = existing.get("role") or "user",
        )
        touch_last_seen(con, int(existing["id"]))
        log.info("auth: invite re-login OK for %s", redact_email(email))
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

    # ── Path 3: new user → atomic redeem of invite code ─────────────────
    if not code:
        raise AuthError("CODE_REQUIRED",
                         "Invite code required for first sign-in")
    try:
        return redeem_with_email(con, email=email, code=code)
    except InviteError as e:
        # Re-raise as AuthError so the router can map uniformly
        raise AuthError("INVITE_INVALID", e.code)


# ──────────────────────────────────────────────────────────────────────────
#  Launch M5: OPEN email signup (no invite required) — billing-aware
# ──────────────────────────────────────────────────────────────────────────

class SignupError(Exception):
    """Raised by signup_user for the duplicate-email case (and other hard
    failures the router maps to a specific HTTP status). Distinct from
    AuthError so the router can map EMAIL_EXISTS → 409 precisely while every
    other failure mode keeps the existing AuthError → 400/401 mapping.

    code values:
        EMAIL_EXISTS   → 409 {code:'EMAIL_EXISTS'}
    """
    def __init__(self, code: str, detail: str = ""):
        self.code   = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def signup_user(
    con: sqlite3.Connection,
    email: str,
    invite_code: Optional[str] = None,
) -> Dict[str, Any]:
    """Open email signup (CONTRACT §3.1 / MASTER_SPEC M5).

    Two branches, by whether a VALID, unused, unexpired invite code is supplied:

      (a) VALID invite_code  → create user with billing_plan='comp', redeem the
          code atomically via the EXISTING invites.py path (redeem_with_email),
          issue JWT, return access='full', checkout_url=None.

      (b) NO / INVALID code  → create user with billing_plan='blocked', issue
          JWT, return access='payment_required', checkout_url='/power/billing'.
          (User flips to 'paid' later when the Razorpay webhook lands — M3.)

    Duplicate email (a row already exists for this email) → SignupError(
    'EMAIL_EXISTS'); the router maps that to HTTP 409. We do NOT leak whether
    the existing account is founding/comp/paid/blocked — just "exists".

    Returns (shape consumed by the router, which adds nothing further):
        {
          "token":        str,          # the JWT
          "user_id":      int,
          "billing_plan": "comp"|"blocked",
          "access":       "full"|"payment_required",
          "checkout_url": str | None,
        }

    Raises:
        AuthError("EMAIL_INVALID")  — structurally bad email (router → 400)
        SignupError("EMAIL_EXISTS") — duplicate (router → 409)

    Reuse map (no logic duplicated):
        * invites.redeem_with_email  — atomic code validate + consume + user
          insert for the comp branch. We do NOT reimplement code redemption.
        * issue_jwt / touch_last_seen — JWT minting + last_seen bump (shared
          with every other auth path).
        * _synthetic_sub_for_email_hashed — google_sub NOT-NULL satisfier.

    TODO(Stage 2): email verification. v1 trusts the supplied email without a
    confirmation round-trip — there is no verify-link / email_verified column
    yet. When Stage 2 adds it, gate JWT issuance (or product access) on
    verification. Out of scope for v1 per MASTER_SPEC M5.
    """
    from .invites import InviteError, redeem_with_email

    # M8 email hook — best-effort welcome on a NEW signup. Wrapped so ANY email
    # failure (unconfigured provider, SDK absent, provider error) is swallowed +
    # logged and NEVER affects the signup response. Defined once, called at each
    # of the two signup-success return sites (comp-via-code + blocked/paywall).
    # No welcome is sent to founding members — this path only runs for fresh
    # open signups.
    def _send_welcome_best_effort(addr: str) -> None:
        try:
            from .email_service import send_welcome
            send_welcome(addr)
        except Exception as _e:  # noqa: BLE001 — email is best-effort, never breaks signup
            log.warning("auth: welcome email hook failed for %s — %s",
                        redact_email(addr), _e)

    email = _normalize_email(email)
    code = (invite_code or "").strip()

    # ── Duplicate-email guard (both branches share it) ───────────────────
    # Open signup creates a BRAND-NEW account only. An existing email — no
    # matter its plan or how it was created — is a 409. Re-login for existing
    # users goes through /auth/invite-login, not here.
    if find_user_by_email(con, email) is not None:
        log.info("auth: signup rejected — email exists for %s", redact_email(email))
        raise SignupError("EMAIL_EXISTS", "An account with this email already exists")

    # ── Branch (a): valid invite code → comp account ─────────────────────
    if code:
        try:
            result = redeem_with_email(con, email=email, code=code)
        except InviteError as e:
            # Code present but not valid (not found / used / expired / bad
            # shape). Per CONTRACT §3.1 an INVALID code is NOT an error — it
            # falls through to the blocked/paywall branch. We log the internal
            # reason but treat the user as code-less.
            log.info("auth: signup code invalid (%s) for %s — falling back to "
                     "blocked branch", e.code, redact_email(email))
        else:
            # redeem_with_email already created the user (role='user', synthetic
            # sub 'email:<raw>') and consumed the code atomically. Flip the new
            # row to billing_plan='comp' (free-forever via code). subscription_
            # status stays at its column default ('active'); comp is never
            # gated by sub status (CONTRACT §2).
            user_id = int(result["user"]["id"])
            con.execute(
                "UPDATE power_user_users SET billing_plan='comp' WHERE id = ?",
                (user_id,),
            )
            con.commit()
            log.info("auth: signup OK (comp via code) for %s (user_id=%d)",
                     redact_email(email), user_id)
            _send_welcome_best_effort(email)   # M8 hook — best-effort, post-success
            return {
                "token":        result["jwt"],
                "user_id":      user_id,
                "billing_plan": "comp",
                "access":       "full",
                "checkout_url": None,
            }

    # ── Branch (b): no / invalid code → blocked account + paywall ────────
    # Create the user ourselves (no invite code to consume). Synthetic HASHED
    # google_sub per CONTRACT §3.1 satisfies the dev NOT-NULL/UNIQUE constraint.
    google_sub = _synthetic_sub_for_email_hashed(email)
    now_iso = datetime.now(IST).isoformat()
    try:
        cur = con.execute(
            """
            INSERT INTO power_user_users
              (email, google_sub, display_name, picture_url,
               invite_code, role, is_active, created_at,
               billing_plan, subscription_status)
            VALUES (?, ?, NULL, NULL, NULL, 'user', 1, ?, 'blocked', 'active')
            """,
            (email, google_sub, now_iso),
        )
        con.commit()
    except sqlite3.IntegrityError as e:
        # Lost a race on the UNIQUE(email)/UNIQUE(google_sub) constraint between
        # the find_user_by_email check above and this insert. Treat as duplicate.
        con.rollback()
        log.info("auth: signup race → EMAIL_EXISTS for %s (%s)",
                 redact_email(email), e)
        raise SignupError("EMAIL_EXISTS", "An account with this email already exists")

    user_id = int(cur.lastrowid)
    token = issue_jwt(user_id=user_id, email=email,
                      google_sub=google_sub, role="user")
    touch_last_seen(con, user_id)
    log.info("auth: signup OK (blocked / payment_required) for %s (user_id=%d)",
             redact_email(email), user_id)
    _send_welcome_best_effort(email)   # M8 hook — best-effort, post-success
    return {
        "token":        token,
        "user_id":      user_id,
        "billing_plan": "blocked",
        "access":       "payment_required",
        "checkout_url": "/power/billing",
    }
