"""/api/power/admin/* — operator-only routes for invite + user management.

Auth: ADMIN_SECRET via X-Admin-Secret header (reuses operator's gate).

  POST /api/power/admin/invites/issue  — mint N new codes
  GET  /api/power/admin/invites/list   — list all codes
  GET  /api/power/admin/users          — list users + their stats
  GET  /api/power/admin/waitlist       — list waitlist
  GET  /api/power/admin/metrics        — DAU, retention, top routes
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..services.invites import generate_codes, list_codes
from .dependencies import get_db, require_admin

log = logging.getLogger("kanida.power_user.admin_router")
router = APIRouter(prefix="/api/power/admin", tags=["power_user_admin"])
IST = timezone(timedelta(hours=5, minutes=30))


class IssueCodesRequest(BaseModel):
    n:               int = Field(default=1,  ge=1, le=100)
    expires_in_days: Optional[int] = Field(default=None, ge=1, le=365)
    note:            Optional[str] = Field(default=None, max_length=200)


@router.post("/invites/issue")
def issue(
    body: IssueCodesRequest,
    _admin: bool = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Mint `n` codes. Returns the actual code strings (single point of capture)."""
    issued = generate_codes(con, n=body.n,
                             issued_by="admin",
                             expires_in_days=body.expires_in_days,
                             note=body.note)
    log.info("admin: issued %d codes (expires_in=%s, note=%s)",
             body.n, body.expires_in_days, body.note)
    return {
        "n_issued":   len(issued),
        "codes":      [c.code for c in issued],
        "expires_at": issued[0].expires_at if issued else None,
        "note":       body.note,
    }


@router.get("/invites/list")
def list_invites(
    only_unused: bool = Query(False),
    limit: int        = Query(100, ge=1, le=500),
    _admin: bool      = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    rows = list_codes(con, limit=limit, only_unused=only_unused)
    return {
        "n":     len(rows),
        "codes": rows,
    }


@router.get("/users")
def list_users(
    limit: int    = Query(200, ge=1, le=1000),
    _admin: bool  = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    rows = con.execute("""
        SELECT id, email, display_name, role, is_active, invite_code,
               created_at, last_seen_at
          FROM power_user_users
         ORDER BY created_at DESC
         LIMIT ?
    """, (limit,)).fetchall()
    return {"n": len(rows), "users": [dict(r) for r in rows]}


@router.get("/waitlist")
def list_waitlist(
    limit: int    = Query(500, ge=1, le=2000),
    _admin: bool  = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    rows = con.execute("""
        SELECT email, joined_at, source, invite_issued
          FROM power_user_waitlist
         ORDER BY joined_at DESC
         LIMIT ?
    """, (limit,)).fetchall()
    return {"n": len(rows), "waitlist": [dict(r) for r in rows]}


# ── Sprint 5c-1: Zerodha auto-auth admin surface ─────────────────────

@router.get("/auth/status")
def auth_status(
    _admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    """Full Zerodha auth health snapshot — for the admin widget."""
    from ..services.auth_status import get_status
    from ..config import POWER_DB_PATH
    return get_status(POWER_DB_PATH)


@router.get("/auth/log")
def auth_log(
    limit: int   = Query(20, ge=1, le=200),
    _admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    """Recent falcon_auth_log entries for the timeline view."""
    from ..services.auth_status import recent_log
    from ..config import POWER_DB_PATH
    return {"entries": recent_log(POWER_DB_PATH, limit=limit)}


@router.post("/auth/refresh-now")
def auth_refresh_now(
    _admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    """Operator clicked 'refresh now' in the admin widget. Triggers an
    immediate auth attempt (async daemon thread)."""
    from services.auth_scheduler import trigger_manual
    return trigger_manual(trigger_kind="manual")


class PushSubscriptionRequest(BaseModel):
    endpoint:   str
    p256dh:     str
    auth:       str
    user_agent: Optional[str] = None


@router.get("/push/vapid-public-key")
def vapid_public_key(_admin: bool = Depends(require_admin)) -> Dict[str, str]:
    """Frontend reads this once to call pushManager.subscribe()."""
    from ..services.web_push import public_key, is_configured
    if not is_configured():
        return {"key": "", "configured": "false"}
    return {"key": public_key(), "configured": "true"}


@router.post("/push/subscribe")
def push_subscribe(
    body: PushSubscriptionRequest,
    _admin: bool = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Frontend got a PushSubscription from the browser, sends it here."""
    from ..services.web_push import save_subscription
    sub_id = save_subscription(
        con, user_id=None,            # admin-tier subscription
        endpoint=body.endpoint,
        p256dh=body.p256dh,
        auth=body.auth,
        user_agent=body.user_agent,
    )
    return {"ok": True, "subscription_id": sub_id}


# ── Sprint 5c-2: Featured-replay pre-warmer surface ──────────────────

@router.get("/replay-warm/status")
def replay_warm_status(_admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    """Last-run summary of the featured-replay pre-warmer daemon."""
    from ..services.replay_warmer import status as _warmer_status
    return _warmer_status()


@router.post("/replay-warm/run")
def replay_warm_run(
    force: bool = Query(False),
    _admin: bool = Depends(require_admin),
) -> Dict[str, Any]:
    """Trigger an immediate warm pass. force=true recomputes even cached rows
    (use after a code change in the explainer)."""
    from ..services.replay_warmer import warm_once
    return warm_once(force=force)


# ─────────────────────────────────────────────────────────────────────


@router.get("/metrics")
def metrics(
    _admin: bool  = Depends(require_admin),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Beta-cohort observability:
      total_users / active_users / dau / 7d_retention / top_routes / wl_size"""
    now = datetime.now(IST)
    day_ago    = (now - timedelta(days=1)).isoformat()
    week_ago   = (now - timedelta(days=7)).isoformat()

    total_users  = con.execute("SELECT COUNT(*) FROM power_user_users WHERE is_active=1").fetchone()[0]
    dau          = con.execute(
        "SELECT COUNT(*) FROM power_user_users WHERE last_seen_at >= ?", (day_ago,)
    ).fetchone()[0]
    wau          = con.execute(
        "SELECT COUNT(*) FROM power_user_users WHERE last_seen_at >= ?", (week_ago,)
    ).fetchone()[0]
    waitlist_n   = con.execute("SELECT COUNT(*) FROM power_user_waitlist").fetchone()[0]
    codes_unused = con.execute(
        "SELECT COUNT(*) FROM power_user_invite_codes WHERE used_by_user_id IS NULL"
    ).fetchone()[0]
    codes_used   = con.execute(
        "SELECT COUNT(*) FROM power_user_invite_codes WHERE used_by_user_id IS NOT NULL"
    ).fetchone()[0]

    top_routes = con.execute("""
        SELECT route, COUNT(*) AS n, AVG(latency_ms) AS avg_ms
          FROM power_user_request_log
         WHERE created_at >= ?
         GROUP BY route ORDER BY n DESC LIMIT 10
    """, (week_ago,)).fetchall()

    return {
        "computed_at":  now.isoformat(),
        "users": {
            "total_active": total_users,
            "dau":          dau,
            "wau":          wau,
        },
        "invites": {
            "codes_used":   codes_used,
            "codes_unused": codes_unused,
            "waitlist":     waitlist_n,
        },
        "top_routes_7d": [
            {"route": r["route"], "n": r["n"],
              "avg_ms": round(r["avg_ms"] or 0, 1)}
            for r in top_routes
        ],
    }
