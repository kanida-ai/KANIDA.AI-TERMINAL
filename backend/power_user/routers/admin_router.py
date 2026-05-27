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

from ..services.invites import InviteError, generate_codes, list_codes, revoke_code
from .dependencies import get_db, require_admin_or_jwt as require_admin

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


@router.post("/invites/{code}/revoke")
def revoke_invite(
    code:   str,
    _admin: bool = Depends(require_admin),
    con:    sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Manually deactivate an unused invite code (2026-05-25 operator request).

    Codes do NOT auto-expire by default — they remain redeemable until the
    admin explicitly revokes them. This endpoint sets expires_at to "now"
    so the existing _is_expired() check in the redeem path rejects the next
    attempt as CODE_EXPIRED. Already-used codes are NEVER modified
    (consumed history is immutable). Already-revoked codes are no-op.

    Returns: {ok, code, was_used, was_already_revoked, expires_at}
      - was_used=True       → caller may want to surface "already redeemed"
      - was_already_revoked → idempotent no-op
    """
    try:
        result = revoke_code(con, code)
    except InviteError as e:
        if e.code == "NOT_FOUND":
            raise HTTPException(404, {"code": e.code, "message": "Invite code not found."})
        raise HTTPException(400, {"code": e.code, "message": str(e)})
    log.info("admin: revoked invite %s (was_used=%s was_already_revoked=%s)",
             code, result["was_used"], result["was_already_revoked"])
    return result


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


def _set_user_active(
    con: sqlite3.Connection, user_id: int, active: bool
) -> Dict[str, Any]:
    """Flip power_user_users.is_active for a single user.

    Safety rails (2026-05-25):
      - Can't touch admin role users (lock-out protection — there is no
        bootstrap-back mechanism if the only admin is deactivated by hand)
      - 404 if user doesn't exist
      - Idempotent: returns the current state with `changed=False` if the
        user is already in the requested state

    Returns: {ok, user_id, email, role, is_active, changed}
    """
    row = con.execute(
        "SELECT id, email, role, is_active FROM power_user_users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, {"code": "USER_NOT_FOUND",
                                    "message": f"No user with id={user_id}."})
    if row["role"] == "admin":
        raise HTTPException(400, {
            "code":    "CANNOT_TOUCH_ADMIN",
            "message": "Admin users cannot be deactivated/reactivated from "
                        "this endpoint (lock-out protection). Use SQL if you "
                        "really mean it.",
        })

    target = 1 if active else 0
    if int(row["is_active"]) == target:
        return {
            "ok":         True,
            "user_id":    row["id"],
            "email":      row["email"],
            "role":       row["role"],
            "is_active":  bool(target),
            "changed":    False,
        }
    con.execute(
        "UPDATE power_user_users SET is_active = ? WHERE id = ?",
        (target, user_id),
    )
    con.commit()
    log.info("admin: set is_active=%d on user_id=%s (%s)",
             target, user_id, row["email"])
    return {
        "ok":         True,
        "user_id":    row["id"],
        "email":      row["email"],
        "role":       row["role"],
        "is_active":  bool(target),
        "changed":    True,
    }


@router.post("/users/{user_id}/deactivate")
def deactivate_user(
    user_id: int,
    _admin:  bool = Depends(require_admin),
    con:     sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Block a registered user from logging in (sets is_active=0).

    The existing auth flow's `/me` endpoint already returns 403 for
    inactive users (auth_router.py line ~102), so no client-side
    enforcement needed — the next API call from this user's session
    rejects automatically. JWT cookie remains technically valid until
    its 24h TTL, but every protected route checks is_active.

    Cannot deactivate admin users. Idempotent (no-op if already inactive).
    """
    return _set_user_active(con, user_id, active=False)


@router.post("/users/{user_id}/reactivate")
def reactivate_user(
    user_id: int,
    _admin:  bool = Depends(require_admin),
    con:     sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Restore login access for a previously-deactivated user (is_active=1).

    Idempotent (no-op if already active). Cannot affect admin users.
    """
    return _set_user_active(con, user_id, active=True)


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


# ──────────────────────────────────────────────────────────────────────
#  Daily-pipeline jobs surface (Phase 1b — surfaces existing infra)
#
#  Wraps the existing /api/jobs/* endpoints with the power-user admin
#  auth path (JWT cookie / X-Admin-Secret), so the operator can drive
#  the engine from /power/admin without leaving the page or typing the
#  secret. NO new pipeline logic — these proxies the canonical implementations:
#    - data freshness:  services/data_freshness.get_freshness()
#    - pipeline state:  main._pipeline_status
#    - manual trigger:  main._run_pipeline_sync  (run in daemon thread)
# ──────────────────────────────────────────────────────────────────────

@router.get("/jobs/status")
def jobs_status(_admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    """Data freshness — last OHLCV trade_date, latest snapshot, pipeline log
    last-modified per step. Same shape as /api/jobs/status."""
    from services.data_freshness import get_freshness
    return get_freshness()


@router.get("/jobs/pipeline")
def jobs_pipeline(_admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    """Current Falcon V7 pipeline-run state.

    The "Run pipeline now" button fires the V7 3-step incremental pipeline
    (data_refresh → features → signals) which writes the falcon_signals_live
    table that /power/today reads. The legacy 5-step pipeline in main.py
    writes a separate set of tables (kanida_quant.db) and is NOT what
    refreshes the user-facing surface.

    Status fields come from:
      - Falcon V7 lock state (falcon.jobs._pipeline.is_pipeline_running)
      - Latest falcon_signal_runs row (last run + result)
    """
    from falcon.jobs._pipeline import is_pipeline_running
    running = bool(is_pipeline_running())
    last_run, last_result = None, None
    try:
        from falcon.db import falcon_conn
        with falcon_conn() as fc:
            row = fc.execute(
                "SELECT started_at, status FROM falcon_signal_runs "
                "WHERE job_name = 'daily_signals' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if row:
                last_run, last_result = row[0], row[1]
    except Exception:
        pass
    return {"running": running, "last_run": last_run, "last_result": last_result}


@router.post("/jobs/run")
def jobs_run(_admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    """Trigger the Falcon V7 incremental pipeline in a background daemon thread.

    Takes ~30-60s end-to-end (vs 30-90 minutes for the legacy full re-mine
    pipeline that this endpoint USED to fire). The V7 pipeline:
      1. daily_data_refresh — Kite OHLC delta (only new bars)
      2. daily_features     — feature computation for any gap dates
      3. daily_signals      — apply existing patterns from falcon_pattern_taxonomy
                              to today's features → write falcon_signals_live

    Mining a fresh set of patterns is SEPARATE (weekly_remine.py, operator-
    triggered) and not invoked daily.
    """
    from falcon.jobs._pipeline import kick_off_v7_pipeline_if_stale, is_pipeline_running
    if is_pipeline_running():
        return {"status": "already_running",
                "message": "Falcon V7 pipeline is already in progress."}
    result = kick_off_v7_pipeline_if_stale(reason="admin_manual")
    if result["kicked_off"]:
        return {"status": "started",
                "message": "Falcon V7 pipeline started in background (~30-60s). Poll /jobs/pipeline."}
    return {"status": "skipped",
            "message": f"Not kicked off: {result.get('reason_skipped')}"}


@router.get("/jobs/scheduled")
def jobs_scheduled(_admin: bool = Depends(require_admin)) -> Dict[str, Any]:
    """Roster of scheduled jobs known to this backend, in the order they run.

    Each entry: { name, cadence, description, status, next_eta_iso (optional) }.
    The runtime values come from the same _pipeline_status / auth_status
    dictionaries the panels already poll, so this is a read-only summary.
    """
    from datetime import datetime, timezone, timedelta
    IST_LOCAL = timezone(timedelta(hours=5, minutes=30))

    jobs: List[Dict[str, Any]] = []

    # ── Falcon V7 daily pipeline (THE one /power/today depends on) ─────
    try:
        from falcon.jobs._pipeline import is_pipeline_running
        from falcon.db import falcon_conn
        running = bool(is_pipeline_running())
        last_run, last_result = None, None
        with falcon_conn() as fc:
            row = fc.execute(
                "SELECT started_at, status FROM falcon_signal_runs "
                "WHERE job_name = 'daily_signals' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if row:
                last_run, last_result = row[0], row[1]
        jobs.append({
            "name":        "Falcon V7 daily pipeline",
            "cadence":     "16:05 IST · Mon–Fri  (also after every Kite token refresh)",
            "description": "INCREMENTAL: daily_data_refresh → daily_features → daily_signals. Applies the 865 frozen patterns from falcon_pattern_taxonomy to today's new feature rows → writes falcon_signals_live. ~30–60s end-to-end (no re-mining).",
            "status":      "running" if running else "idle",
            "last_run":    last_run,
            "last_result": last_result,
            "implemented": True,
            "trigger":     {"method": "POST", "path": "/api/power/admin/jobs/run"},
        })
    except Exception:
        pass

    # ── Daily Kite OHLC refresh (step 1 of V7) ─────────────────────────
    jobs.append({
        "name":        "Daily OHLC refresh (Falcon)",
        "cadence":     "Step 1 of Falcon V7 pipeline",
        "description": "Incremental Kite fetch of new bars per Falcon-tracked symbol. 24 workers @ 5 rps. Idempotent — noops if no new bars.",
        "status":      "implemented",
        "implemented": True,
    })

    # ── Legacy daily pipeline (LEGACY — full re-mine into kanida_quant.db, NOT what /power/today reads) ─
    try:
        import main as m
        lps = m._pipeline_status
        jobs.append({
            "name":        "Legacy daily pipeline (kanida_quant.db)",
            "cadence":     "16:05 IST · Mon–Fri  (auto-armed by main.py — separate from V7)",
            "description": "OLD 5-step pipeline (OHLCV → pattern_learning RE-MINE → backtest → execution → pending). Writes to legacy kanida_quant.db; does NOT refresh /power/today. Kept on auto-schedule for the legacy /falcon/* operator surface. Phase 2 cleanup will retire this.",
            "status":      "running" if lps.get("running") else "idle",
            "last_run":    lps.get("last_run"),
            "last_result": lps.get("last_result"),
            "implemented": True,
        })
    except Exception:
        pass

    # Zerodha auto-auth (Layer 1 — Playwright bot, every 30 min 06:30–16:30 IST)
    try:
        from power_user.services.auth_status import get_status
        from power_user.config import POWER_DB_PATH
        st = get_status(POWER_DB_PATH)
        jobs.append({
            "name":        "Zerodha auto-auth",
            "cadence":     "every 30 min · 06:30–16:30 IST",
            "description": ("Playwright bot refreshes the Kite access token. "
                            "Each cycle skips cheaply if today's token is logged AND "
                            "passes a live Kite profile() check — so mid-day "
                            "invalidations recover within 30 min. Push fallback if "
                            "a scheduled cycle at/after 09:00 IST fails."),
            "status":      "token_valid" if st.get("token_valid") else "token_invalid",
            "next_run":    st.get("next_scheduled_at"),
            "last_run":    (st.get("last_auto_attempt") or {}).get("attempt_at"),
            "implemented": True,
        })
    except Exception:
        pass

    # Featured-replay pre-warmer (6h interval)
    try:
        from power_user.services.replay_warmer import status as _warm_status
        ws = _warm_status()
        jobs.append({
            "name":        "Featured replay pre-warmer",
            "cadence":     "Every 6 hours",
            "description": "Pre-computes the 3 featured-replay payloads so first-load latency stays sub-100ms.",
            "status":      "running",
            "last_run":    (ws or {}).get("last_pass_at"),
            "implemented": True,
        })
    except Exception:
        pass

    # Weekly re-mining — code exists at falcon/jobs/weekly_remine.py.
    # Not on the live schedule yet; operator triggers manually. This is the
    # 4-yr full pattern mine that produces falcon_pattern_taxonomy (865 rows).
    # Daily pipeline ABOVE just APPLIES these patterns — does not re-mine.
    jobs.append({
        "name":        "Weekly pattern re-mining",
        "cadence":     "Manual (Phase 2: Sunday 02:00 IST cron)",
        "description": "FULL re-mine of falcon_pattern_taxonomy across the rolling 4-yr window. Heavy compute (~30-60 min, 8 worker procs). Run weekly OR when methodology changes. Daily pipeline only APPLIES the result of this job — never re-mines.",
        "status":      "manual",
        "implemented": True,
    })

    # 1-minute data fetcher + intraday miner — DEFERRED (Phase 2c)
    jobs.append({
        "name":        "1-min data fetcher",
        "cadence":     "09:15–15:30 IST · weekdays (planned)",
        "description": "Continuous Kite WebSocket ingest of 1-min bars into RND ohlc_1min (already 87.8M historical rows). Not yet live.",
        "status":      "not_implemented",
        "implemented": False,
    })
    jobs.append({
        "name":        "Intraday pattern miner",
        "cadence":     "After 10:45 IST · weekdays (planned)",
        "description": "Mines smart-entry rules on the 1-min stream; emits /power/live decisions and the 09:30 IST validation gate.",
        "status":      "not_implemented",
        "implemented": False,
    })

    return {"n": len(jobs), "jobs": jobs}


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
