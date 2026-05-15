"""Web Push fallback — Layer 2 of the Zerodha auto-auth system.

When all 4 morning Playwright attempts fail, this module fires a notification
to every active admin push subscription. Payload includes a one-time
magic-link URL so the admin can tap → page auto-runs refresh → no typing.

Stack:
  pywebpush (encrypts payload via VAPID)
  py-vapid  (signs the request to the push service)

Subscriptions:
  Admin visits /power/admin → browser prompts for notification permission →
  on grant, the page POSTs the PushSubscription to /api/power/admin/push/subscribe.
  We store endpoint + p256dh + auth in power_user_push_subscriptions.

Failure modes:
  410 GONE         — subscription expired (Chrome/Firefox rotate endpoints).
                       Mark is_active=0 so we stop trying.
  413 / 4xx        — payload too large / invalid. Log + mark error.
  network down     — log + retry on next push (we don't queue).

Security:
  VAPID keypair lives in env vars VAPID_PRIVATE_KEY + VAPID_PUBLIC_KEY.
  Generate ONCE via `vapid --gen` (py-vapid CLI). Same keys forever — losing
  them invalidates every existing subscription (users must re-subscribe).
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

log = logging.getLogger("kanida.power_user.web_push")
IST = timezone(timedelta(hours=5, minutes=30))


# ──────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────

def _vapid_private_key() -> Optional[str]:
    return os.environ.get("VAPID_PRIVATE_KEY", "").strip() or None


def _vapid_public_key() -> Optional[str]:
    return os.environ.get("VAPID_PUBLIC_KEY", "").strip() or None


def _vapid_claims() -> Dict[str, str]:
    """The 'sub' claim is required by push services. mailto: is conventional."""
    contact = os.environ.get("VAPID_CONTACT", "mailto:ops@kanida.ai").strip()
    return {"sub": contact}


def is_configured() -> bool:
    """True iff both VAPID keys are set."""
    return bool(_vapid_private_key() and _vapid_public_key())


def public_key() -> str:
    """Return the VAPID public key for the frontend to subscribe with."""
    return _vapid_public_key() or ""


# ──────────────────────────────────────────────────────────────────────────
# Subscription management
# ──────────────────────────────────────────────────────────────────────────

def save_subscription(con: sqlite3.Connection,
                       *,
                       user_id: Optional[int],
                       endpoint: str,
                       p256dh: str,
                       auth: str,
                       user_agent: Optional[str] = None,
                       ) -> int:
    """Insert (or reactivate) a push subscription. Idempotent on endpoint."""
    now = datetime.now(IST).isoformat()
    cur = con.execute("""
        INSERT INTO power_user_push_subscriptions
          (user_id, endpoint, p256dh, auth, user_agent, created_at, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(endpoint) DO UPDATE SET
          user_id    = excluded.user_id,
          p256dh     = excluded.p256dh,
          auth       = excluded.auth,
          user_agent = excluded.user_agent,
          is_active  = 1
    """, (user_id, endpoint, p256dh, auth, user_agent, now))
    con.commit()
    return cur.lastrowid or 0


def list_active_subscriptions(con: sqlite3.Connection) -> list[Dict[str, Any]]:
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT id, endpoint, p256dh, auth, user_agent, user_id
          FROM power_user_push_subscriptions
         WHERE is_active = 1
    """).fetchall()
    return [dict(r) for r in rows]


def deactivate_subscription(con: sqlite3.Connection, endpoint: str, reason: str) -> None:
    con.execute("""
        UPDATE power_user_push_subscriptions
           SET is_active = 0, last_send_error = ?
         WHERE endpoint = ?
    """, (reason[:200], endpoint))
    con.commit()


# ──────────────────────────────────────────────────────────────────────────
# Magic link (one-time, short-lived) for the admin notification CTA
# ──────────────────────────────────────────────────────────────────────────

MAGIC_LINK_TTL_MIN = 15


def mint_magic_link(con: sqlite3.Connection, *,
                     kind: str = "admin_auth_refresh",
                     issued_for: str = "admin") -> str:
    """Generate a one-time short-lived token. Returns the token string."""
    token = secrets.token_urlsafe(24)
    now = datetime.now(IST)
    expires = now + timedelta(minutes=MAGIC_LINK_TTL_MIN)
    con.execute("""
        INSERT INTO power_user_magic_links (token, kind, created_at, expires_at, issued_for)
        VALUES (?, ?, ?, ?, ?)
    """, (token, kind, now.isoformat(), expires.isoformat(), issued_for))
    con.commit()
    return token


def consume_magic_link(con: sqlite3.Connection, token: str,
                        kind: str, ip_hash: str = "") -> Dict[str, Any]:
    """Atomically validate + mark used. Returns {ok, reason}.

    Race-condition safe: UPDATE ... WHERE used_at IS NULL → at most one
    consumer wins.
    """
    con.isolation_level = None
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute("""
            SELECT kind, expires_at, used_at FROM power_user_magic_links
             WHERE token = ?
        """, (token,)).fetchone()
        if not row:
            con.execute("ROLLBACK")
            return {"ok": False, "reason": "NOT_FOUND"}
        row_kind, expires_at, used_at = row
        if used_at is not None:
            con.execute("ROLLBACK")
            return {"ok": False, "reason": "ALREADY_USED"}
        if row_kind != kind:
            con.execute("ROLLBACK")
            return {"ok": False, "reason": "WRONG_KIND"}
        try:
            exp = datetime.fromisoformat(expires_at)
            if datetime.now(IST) >= exp:
                con.execute("ROLLBACK")
                return {"ok": False, "reason": "EXPIRED"}
        except ValueError:
            con.execute("ROLLBACK")
            return {"ok": False, "reason": "EXPIRED"}

        now_iso = datetime.now(IST).isoformat()
        cur = con.execute("""
            UPDATE power_user_magic_links
               SET used_at = ?, used_ip_hash = ?
             WHERE token = ? AND used_at IS NULL
        """, (now_iso, ip_hash[:64], token))
        if cur.rowcount != 1:
            con.execute("ROLLBACK")
            return {"ok": False, "reason": "RACE_LOST"}
        con.execute("COMMIT")
        return {"ok": True, "reason": None}
    except Exception as e:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        return {"ok": False, "reason": f"INTERNAL_ERROR: {e}"}
    finally:
        con.isolation_level = ""


# ──────────────────────────────────────────────────────────────────────────
# Send — single subscription
# ──────────────────────────────────────────────────────────────────────────

def _send_one(subscription: Dict[str, Any], payload: Dict[str, Any]) -> tuple[bool, str]:
    """Push one notification. Returns (success, error_str)."""
    from pywebpush import webpush, WebPushException    # noqa: WPS433
    private_key = _vapid_private_key()
    if not private_key:
        return (False, "VAPID_PRIVATE_KEY missing")

    info = {
        "endpoint":         subscription["endpoint"],
        "keys": {
            "p256dh":  subscription["p256dh"],
            "auth":    subscription["auth"],
        },
    }
    try:
        webpush(
            subscription_info=info,
            data=json.dumps(payload),
            vapid_private_key=private_key,
            vapid_claims=_vapid_claims(),
        )
        return (True, "")
    except WebPushException as e:
        status = getattr(e.response, "status_code", None)
        body = ""
        try: body = (e.response.text or "")[:200]
        except Exception: body = ""
        return (False, f"WebPush {status}: {body}")
    except Exception as e:
        return (False, f"{type(e).__name__}: {str(e)[:200]}")


# ──────────────────────────────────────────────────────────────────────────
# Entry points used by the auth scheduler
# ──────────────────────────────────────────────────────────────────────────

def notify_auth_needed() -> Dict[str, Any]:
    """All 4 morning attempts failed → notify every active admin subscription
    with a magic link. Run from auth_scheduler after Layer 1 exhausts.

    Returns summary {sent, failed, magic_link, ...}.
    """
    if not is_configured():
        log.warning("web_push: VAPID not configured — Layer 2 disabled")
        return {"sent": 0, "failed": 0, "skipped": True,
                "reason": "VAPID_NOT_CONFIGURED"}

    from ..config import POWER_DB_PATH
    con = sqlite3.connect(POWER_DB_PATH, timeout=10.0, check_same_thread=False)
    try:
        subs = list_active_subscriptions(con)
        if not subs:
            log.warning("web_push: no active subscriptions — admin hasn't subscribed yet")
            return {"sent": 0, "failed": 0, "skipped": True,
                    "reason": "NO_SUBSCRIPTIONS"}

        token = mint_magic_link(con, kind="admin_auth_refresh", issued_for="admin")
        magic_url = f"/power/admin/refresh-token?t={token}"
        payload = {
            "title": "Kanida.AI — Zerodha auth needed",
            "body":  ("Auto-renewal failed 4× this morning. Tap to refresh now — "
                       "no typing required, link expires in 15 min."),
            "url":    magic_url,
            "tag":    "kanida-auth",       # browsers replace prior with same tag
        }
        sent = 0
        failed = 0
        for sub in subs:
            ok, err = _send_one(sub, payload)
            if ok:
                sent += 1
                con.execute(
                    "UPDATE power_user_push_subscriptions SET last_sent_at = ?, last_send_error = NULL WHERE endpoint = ?",
                    (datetime.now(IST).isoformat(), sub["endpoint"]),
                )
            else:
                failed += 1
                if "404" in err or "410" in err:
                    # Subscription is dead — deactivate
                    deactivate_subscription(con, sub["endpoint"], err)
                    log.info("web_push: deactivated dead subscription %s", sub["endpoint"][:40])
                else:
                    con.execute(
                        "UPDATE power_user_push_subscriptions SET last_send_error = ? WHERE endpoint = ?",
                        (err[:200], sub["endpoint"]),
                    )
                    log.warning("web_push: send failed for %s: %s",
                                  sub["endpoint"][:40], err)
        con.commit()
        log.info("web_push: notify_auth_needed sent=%d failed=%d", sent, failed)
        return {"sent": sent, "failed": failed, "magic_link": magic_url}
    finally:
        con.close()
