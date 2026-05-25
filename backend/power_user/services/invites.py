"""Invite code generation + atomic redemption.

The atomicity matters: a race condition would let two people redeem the same
single-use code if we read-then-write across separate statements. Every state
transition (validate → user create → mark used) happens in ONE SQLite
transaction with the code row locked via UPDATE ... WHERE used_by_user_id IS NULL.

Code format: `kn-2026-{6-hex}`  — short, human-pronounceable, year-stamped.

Brute-force resistance (R2 from review):
  Logic-layer: redeem returns the SAME generic error for {not found, expired,
  already used, format invalid} so an attacker can't probe for valid codes.
  Router-layer: applies per-IP-hash rate limit on /invites/redeem (T09).
"""
from __future__ import annotations

import logging
import re
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .auth import GoogleUser, issue_jwt, redact_email, touch_last_seen

log = logging.getLogger("kanida.power_user.invites")
IST = timezone(timedelta(hours=5, minutes=30))

CODE_PREFIX = "kn"
CODE_HEX_LEN = 6                                # 6 hex chars = 24 bits = 16.7M space
CODE_RE = re.compile(r"^kn-\d{4}-[a-f0-9]{6}$")  # validates shape, not validity


# ──────────────────────────────────────────────────────────────────────────
#  Result types
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class IssuedCode:
    code:       str
    issued_at:  str
    expires_at: Optional[str]
    note:       Optional[str]


@dataclass
class CodeStatus:
    """Lightweight status from validate_code (no DB write)."""
    exists:     bool
    is_valid:   bool          # True iff exists AND not expired AND not used
    used_by_id: Optional[int]
    expires_at: Optional[str]
    issued_at:  Optional[str]


class InviteError(Exception):
    """All errors are mapped to the SAME generic message at the router layer
    to prevent enumeration. The internal `code` lets us log which actually
    happened, while the user sees only 'INVITE_INVALID'."""
    def __init__(self, code: str, detail: str = ""):
        self.code   = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ──────────────────────────────────────────────────────────────────────────
#  Code generation (admin only)
# ──────────────────────────────────────────────────────────────────────────

def _new_code() -> str:
    """Generate one fresh code. Cryptographically random hex tail."""
    year = datetime.now(IST).year
    tail = secrets.token_hex(CODE_HEX_LEN // 2)   # 6 chars
    return f"{CODE_PREFIX}-{year}-{tail}"


def generate_codes(con: sqlite3.Connection,
                    n: int,
                    issued_by: str = "system",
                    expires_in_days: Optional[int] = None,
                    note: Optional[str] = None,
                    ) -> List[IssuedCode]:
    """Mint `n` fresh codes. Idempotent against accidental collision (retries on UNIQUE)."""
    if n <= 0 or n > 100:
        raise ValueError(f"n must be 1..100, got {n}")
    now = datetime.now(IST)
    expires_at = (now + timedelta(days=expires_in_days)).isoformat() if expires_in_days else None
    issued_at  = now.isoformat()
    issued: List[IssuedCode] = []

    for _ in range(n):
        # Retry on collision (1 in 16M but be defensive)
        for attempt in range(5):
            code = _new_code()
            try:
                con.execute("""
                    INSERT INTO power_user_invite_codes
                      (code, issued_by, issued_at, expires_at, note)
                    VALUES (?, ?, ?, ?, ?)
                """, (code, issued_by, issued_at, expires_at, note))
                issued.append(IssuedCode(code=code, issued_at=issued_at,
                                          expires_at=expires_at, note=note))
                break
            except sqlite3.IntegrityError:
                if attempt == 4:
                    raise
                continue
    con.commit()
    log.info("invites: issued %d codes (by=%s, expires_in_days=%s)",
             n, issued_by, expires_in_days)
    return issued


def list_codes(con: sqlite3.Connection,
                limit: int = 100,
                only_unused: bool = False,
                ) -> List[Dict[str, Any]]:
    """For the admin UI. Returns code rows ordered by most recent.

    LEFT JOINs power_user_users so the admin UI can show per-user
    activation state on consumed rows (added 2026-05-25 for the
    "deactivate user" admin button — let the operator block a
    misregistered account from logging in further).
    """
    sql = """
        SELECT c.*, u.is_active AS used_by_is_active
        FROM power_user_invite_codes c
        LEFT JOIN power_user_users u ON c.used_by_user_id = u.id
    """
    if only_unused:
        sql += " WHERE c.used_by_user_id IS NULL"
    sql += " ORDER BY c.issued_at DESC LIMIT ?"
    con.row_factory = sqlite3.Row
    rows = con.execute(sql, (limit,)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────────
#  Validation (read-only, used internally)
# ──────────────────────────────────────────────────────────────────────────

def _is_expired(expires_at: Optional[str]) -> bool:
    if not expires_at:
        return False
    try:
        exp = datetime.fromisoformat(expires_at)
    except ValueError:
        return True   # malformed → safer to treat as expired
    return datetime.now(IST) >= exp


def revoke_code(con: sqlite3.Connection, code: str) -> Dict[str, Any]:
    """Manually deactivate an unused invite code so it can no longer be
    redeemed. Reuses the existing _is_expired() check in the redeem path —
    revoking == setting expires_at to "now" (IST). No schema change, no new
    rejection logic; the next attempt to redeem the code rejects with
    CODE_EXPIRED. (2026-05-25 operator request: codes never auto-expire by
    default; only manual deactivation invalidates them.)

    Already-used codes are NEVER modified — they're consumed history, not
    revocable. Already-revoked codes are idempotent no-ops.

    Returns: {
      ok:                  True,
      code:                str,
      was_used:            bool,   # True if code is already consumed (no-op)
      was_already_revoked: bool,   # True if expires_at was already in the past
      expires_at:          str | None,
    }
    Raises InviteError("NOT_FOUND") if the code doesn't exist.
    """
    row = con.execute(
        "SELECT expires_at, used_by_user_id FROM power_user_invite_codes WHERE code = ?",
        (code,),
    ).fetchone()
    if not row:
        raise InviteError("NOT_FOUND", f"no invite code: {code}")
    old_expires_at, used_by = row[0], row[1]

    if used_by is not None:
        # Already consumed — don't touch the row. Caller decides whether to
        # surface a different message ("already used" vs "revoked").
        return {
            "ok":                  True,
            "code":                code,
            "was_used":            True,
            "was_already_revoked": False,
            "expires_at":          old_expires_at,
        }

    was_already_revoked = old_expires_at is not None and _is_expired(old_expires_at)
    if was_already_revoked:
        return {
            "ok":                  True,
            "code":                code,
            "was_used":            False,
            "was_already_revoked": True,
            "expires_at":          old_expires_at,
        }

    now_iso = datetime.now(IST).isoformat()
    con.execute(
        "UPDATE power_user_invite_codes SET expires_at = ? "
        "WHERE code = ? AND used_by_user_id IS NULL",
        (now_iso, code),
    )
    con.commit()
    return {
        "ok":                  True,
        "code":                code,
        "was_used":            False,
        "was_already_revoked": False,
        "expires_at":          now_iso,
    }


def validate_code(con: sqlite3.Connection, code: str) -> CodeStatus:
    """Read-only check. Does NOT mark the code as used. Returns CodeStatus.

    Use this in the UI (e.g. real-time validation as the user types) but use
    redeem_atomic for the actual sign-up — otherwise race conditions."""
    if not CODE_RE.match(code or ""):
        return CodeStatus(exists=False, is_valid=False, used_by_id=None,
                          expires_at=None, issued_at=None)
    row = con.execute(
        "SELECT issued_at, expires_at, used_by_user_id FROM power_user_invite_codes "
        "WHERE code = ?", (code,)
    ).fetchone()
    if not row:
        return CodeStatus(exists=False, is_valid=False, used_by_id=None,
                          expires_at=None, issued_at=None)
    issued_at, expires_at, used_by = row
    is_used = used_by is not None
    is_expired = _is_expired(expires_at)
    return CodeStatus(
        exists     = True,
        is_valid   = not (is_used or is_expired),
        used_by_id = used_by,
        expires_at = expires_at,
        issued_at  = issued_at,
    )


# ──────────────────────────────────────────────────────────────────────────
#  Atomic redemption — THE critical path
# ──────────────────────────────────────────────────────────────────────────

def redeem_atomic(con: sqlite3.Connection,
                   code: str,
                   google_user: GoogleUser,
                   ) -> Dict[str, Any]:
    """Atomically:
        1. Lock the invite_code row
        2. Check it's unused + not expired + format valid
        3. Insert new user record
        4. Mark the code as used (linking back to the new user)
       All in one BEGIN IMMEDIATE transaction. If any step fails, ROLLBACK.

    Returns:
        {"jwt": "...", "user": {...}}

    Raises:
        InviteError on any failure. The router layer maps every InviteError
        to the SAME generic HTTP 400 to prevent enumeration.

    Notes on race condition:
        SQLite BEGIN IMMEDIATE acquires the RESERVED lock immediately, so a
        concurrent transaction can read but not write the codes table until
        we commit. Two simultaneous redemptions of the same code will see
        one succeed and one fail with 'CODE_ALREADY_USED'.
    """
    if not CODE_RE.match(code or ""):
        raise InviteError("CODE_FORMAT_INVALID", f"bad shape: {code!r}")

    email = google_user.email
    google_sub = google_user.google_sub

    # SQLite-specific: 'BEGIN IMMEDIATE' acquires RESERVED lock right now.
    # The default 'BEGIN DEFERRED' would wait until first write, which leaves
    # the validate-then-write window open for races.
    con.isolation_level = None   # we manage transactions manually
    con.execute("BEGIN IMMEDIATE")
    try:
        # 1. Look up the code under lock
        row = con.execute(
            "SELECT code, expires_at, used_by_user_id FROM power_user_invite_codes "
            "WHERE code = ?", (code,)
        ).fetchone()
        if not row:
            raise InviteError("CODE_NOT_FOUND", "no such code")
        _code, expires_at, used_by = row
        if used_by is not None:
            raise InviteError("CODE_ALREADY_USED", f"used_by_user_id={used_by}")
        if _is_expired(expires_at):
            raise InviteError("CODE_EXPIRED", f"expires_at={expires_at}")

        # 2. Check if a user with this email or google_sub already exists
        existing = con.execute(
            "SELECT id, email FROM power_user_users "
            "WHERE google_sub = ? OR email = ?",
            (google_sub, email),
        ).fetchone()
        if existing:
            # User exists — DON'T let them re-claim a code. Their existing
            # row is the source of truth.
            raise InviteError("USER_ALREADY_EXISTS", f"user_id={existing[0]}")

        # 3. Insert the new user
        now_iso = datetime.now(IST).isoformat()
        cur = con.execute("""
            INSERT INTO power_user_users
              (email, google_sub, display_name, picture_url,
               invite_code, role, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, 'user', 1, ?)
        """, (email, google_sub, google_user.display_name,
              google_user.picture_url, code, now_iso))
        user_id = cur.lastrowid

        # 4. Mark code used (atomic — same transaction).
        # Defensive: under BEGIN IMMEDIATE this can't logically fail to update
        # exactly one row (we just verified used_by_user_id IS NULL above),
        # but assert it anyway so a future migration that removes the lock
        # or changes the WHERE clause breaks loudly here instead of silently
        # creating users whose code wasn't actually marked.
        cur = con.execute(
            "UPDATE power_user_invite_codes SET used_by_user_id = ?, used_at = ? "
            "WHERE code = ? AND used_by_user_id IS NULL",
            (user_id, now_iso, code),
        )
        if cur.rowcount != 1:
            raise InviteError("RACE_DETECTED",
                              f"expected 1 row updated, got {cur.rowcount} "
                              f"— BEGIN IMMEDIATE lock semantics may be broken")

        con.execute("COMMIT")

    except InviteError:
        con.execute("ROLLBACK")
        raise
    except Exception as e:
        con.execute("ROLLBACK")
        log.exception("invites: redeem_atomic crashed for code=%s", code)
        raise InviteError("INTERNAL_ERROR", str(e))
    finally:
        con.isolation_level = ""   # restore default for any later non-managed code

    # 5. Issue JWT (outside the txn — pure crypto)
    token = issue_jwt(user_id=user_id, email=email, google_sub=google_sub, role="user")
    touch_last_seen(con, user_id)

    log.info("invites: code %s redeemed by %s (user_id=%d)",
             code, redact_email(email), user_id)

    return {
        "jwt": token,
        "user": {
            "id":           user_id,
            "email":        email,
            "display_name": google_user.display_name,
            "picture_url":  google_user.picture_url,
            "role":         "user",
        },
    }


# ──────────────────────────────────────────────────────────────────────────
#  Phase 1b: email-only atomic redemption (no Google id_token)
# ──────────────────────────────────────────────────────────────────────────

def redeem_with_email(
    con: sqlite3.Connection,
    email: str,
    code: str,
    display_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Atomic email + code → user + JWT (Phase 1b invite-only auth).

    Same transactional safety as `redeem_atomic` but takes a raw email
    instead of a verified GoogleUser. Used by the new POST
    /api/power/auth/invite-login endpoint.

    For schema compat, persists a synthetic google_sub of form
    'email:{email}' so the NOT-NULL UNIQUE google_sub column is satisfied.
    If the user later signs in via Google with the same email, the existing
    google_sub-rotation path in sign_in_with_google() updates that column.

    Raises InviteError on every failure mode (same uniform-failure contract
    as `redeem_atomic`).
    """
    from .auth import issue_jwt, touch_last_seen, _synthetic_sub_for_email

    if not CODE_RE.match(code or ""):
        raise InviteError("CODE_FORMAT_INVALID", f"bad shape: {code!r}")

    email = (email or "").lower().strip()
    if "@" not in email or len(email) < 5:
        raise InviteError("EMAIL_INVALID", f"bad email: {email!r}")
    google_sub = _synthetic_sub_for_email(email)

    con.isolation_level = None
    con.execute("BEGIN IMMEDIATE")
    try:
        # 1. Lock + check the code row
        row = con.execute(
            "SELECT code, expires_at, used_by_user_id FROM power_user_invite_codes "
            "WHERE code = ?", (code,)
        ).fetchone()
        if not row:
            raise InviteError("CODE_NOT_FOUND", "no such code")
        _code, expires_at, used_by = row
        if used_by is not None:
            raise InviteError("CODE_ALREADY_USED", f"used_by_user_id={used_by}")
        if _is_expired(expires_at):
            raise InviteError("CODE_EXPIRED", f"expires_at={expires_at}")

        # 2. Reject if a user with this email already exists. Re-login goes
        #    through sign_in_with_email_and_code's existing-user branch, not
        #    through here. If they're here, they're brand new.
        existing = con.execute(
            "SELECT id, email FROM power_user_users WHERE email = ? OR google_sub = ?",
            (email, google_sub),
        ).fetchone()
        if existing:
            raise InviteError("USER_ALREADY_EXISTS", f"user_id={existing[0]}")

        # 3. Insert the new user row
        now_iso = datetime.now(IST).isoformat()
        cur = con.execute("""
            INSERT INTO power_user_users
              (email, google_sub, display_name, picture_url,
               invite_code, role, is_active, created_at)
            VALUES (?, ?, ?, NULL, ?, 'user', 1, ?)
        """, (email, google_sub, display_name, code, now_iso))
        user_id = cur.lastrowid

        # 4. Atomic code-consume
        cur = con.execute(
            "UPDATE power_user_invite_codes SET used_by_user_id = ?, used_at = ? "
            "WHERE code = ? AND used_by_user_id IS NULL",
            (user_id, now_iso, code),
        )
        if cur.rowcount != 1:
            raise InviteError("RACE_DETECTED",
                              f"expected 1 row updated, got {cur.rowcount}")

        con.execute("COMMIT")
    except InviteError:
        con.execute("ROLLBACK")
        raise
    except Exception as e:
        con.execute("ROLLBACK")
        log.exception("invites: redeem_with_email crashed for code=%s", code)
        raise InviteError("INTERNAL_ERROR", str(e))
    finally:
        con.isolation_level = ""

    token = issue_jwt(user_id=user_id, email=email,
                      google_sub=google_sub, role="user")
    touch_last_seen(con, user_id)

    log.info("invites: code %s redeemed (email-only) by %s (user_id=%d)",
             code, redact_email(email), user_id)

    return {
        "status": "ok",
        "jwt":    token,
        "user": {
            "id":           user_id,
            "email":        email,
            "display_name": display_name,
            "picture_url":  None,
            "role":         "user",
        },
    }


# ──────────────────────────────────────────────────────────────────────────
#  Waitlist (no auth required — captured when redeem fails or no invite)
# ──────────────────────────────────────────────────────────────────────────

def add_to_waitlist(con: sqlite3.Connection, email: str, source: str = "login_no_invite") -> bool:
    """Insert into power_user_waitlist. Returns True on insert, False if email already there.

    Idempotent — same email twice is a no-op.
    """
    email = email.lower().strip()
    if not email or "@" not in email:
        return False
    now_iso = datetime.now(IST).isoformat()
    try:
        con.execute(
            "INSERT INTO power_user_waitlist (email, joined_at, source) VALUES (?, ?, ?)",
            (email, now_iso, source),
        )
        con.commit()
        log.info("invites: waitlisted %s (source=%s)", redact_email(email), source)
        return True
    except sqlite3.IntegrityError:
        # UNIQUE(email) — already on the list
        return False
