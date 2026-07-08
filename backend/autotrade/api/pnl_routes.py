"""Performance-dashboard P&L API — /api/power/autotrade/pnl/*.

Portal-facing, READ-ONLY, additive. Power_jwt-gated (Bearer header OR the
power_jwt cookie) + scoped to the caller's user_id: a non-admin sees ONLY their
own sessions; an admin sees all and may pass ?user_id= to filter to one user.

These routes are MOUNTED UNDER /api/power (alongside the other power_user
routers, no operator-token gate) — they are strictly authenticated by the portal
JWT here, and read from the isolated autotrade_* tables via falcon_conn (never
falcon_position_state).

Endpoints:
  GET /api/power/autotrade/pnl/summary     — strategy/segment/product rollup
  GET /api/power/autotrade/pnl/export.csv  — one row per closed trade (streamed)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from falcon.db import falcon_conn

from .autotrade_routes import Caller, _extract_jwt
from . import pnl_summary as svc

log = logging.getLogger("kanida.autotrade.pnl.api")
IST = timezone(timedelta(hours=5, minutes=30))

router = APIRouter(prefix="/api/power/autotrade/pnl", tags=["Power-User"])


# ── Auth: strict power_jwt (Bearer OR power_jwt cookie) ───────────────────────

def require_power_caller(request: Request) -> Caller:
    """Resolve the portal user from the power_jwt (Authorization: Bearer <jwt> OR
    the power_jwt cookie). 401 if absent/invalid. Admin = role=='admin'.

    Reuses autotrade_routes._extract_jwt (the exact same JWT transport the rest of
    the AutoTrade portal surface uses) + power_user.services.auth.verify_jwt.
    Unlike autotrade_routes.resolve_caller, this NEVER degrades to an
    unauthenticated operator-admin — the Performance dashboard is portal-only, so
    a missing/invalid token is a hard 401.
    """
    token = _extract_jwt(request)
    if not token:
        raise HTTPException(401, {"code": "AUTH_REQUIRED",
                                   "message": "Power user session required"})
    try:
        from power_user.services.auth import verify_jwt  # lazy, defensive
        payload = verify_jwt(token)
    except Exception:
        raise HTTPException(401, {"code": "AUTH_INVALID",
                                   "message": "Invalid or expired session"})
    return Caller(user_id=str(payload.user_id),
                  is_admin=(payload.role == "admin"),
                  authenticated=True)


def _admin_filter_user_id(caller: Caller,
                          user_id_param: Optional[str]) -> Optional[str]:
    """?user_id= is honoured ONLY for admins; a non-admin is always locked to
    their own id (the param is ignored — no cross-user probing)."""
    if caller.is_admin:
        return user_id_param
    return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/summary")
def pnl_summary(
    request: Request,
    period:  str = Query("1w",
                         description="yesterday|1w|2w|mtd|ytd|custom"),
    from_:   Optional[str] = Query(None, alias="from",
                                   description="YYYY-MM-DD (custom)"),
    to:      Optional[str] = Query(None, description="YYYY-MM-DD (custom)"),
    mode:    str = Query("live", description="live|paper|all (default live)"),
    user_id: Optional[str] = Query(None,
                                   description="admin-only: filter to one user"),
    caller:  Caller = Depends(require_power_caller),
):
    """Realised-P&L rollup by strategy (+ segment/product tags + drill-down).

    Empty period → strategies:[] + zeroed totals (200, never 500)."""
    filter_uid = _admin_filter_user_id(caller, user_id)
    try:
        with falcon_conn() as con:
            return svc.build_pnl_summary(
                con,
                viewer_user_id=caller.user_id,
                is_admin=caller.is_admin,
                period=period,
                from_date=from_,
                to_date=to,
                mode=mode,
                filter_user_id=filter_uid,
                now_ist=datetime.now(IST),
            )
    except HTTPException:
        raise
    except Exception as e:  # never 500 the dashboard — degrade to empty
        log.error("pnl_summary failed (%s): %s", type(e).__name__, e)
        now = datetime.now(IST)
        from_iso, to_iso = svc.resolve_window(period, from_, to, now)
        return {
            "period": period, "from": from_iso, "to": to_iso,
            "as_of": now.isoformat(), "capital_deployed": 0.0,
            "totals": {"net": 0.0, "gross": 0.0, "charges": 0.0, "trades": 0,
                       "wins": 0, "losses": 0, "win_rate": 0.0},
            "strategies": [],
        }


@router.get("/export.csv")
def pnl_export_csv(
    request: Request,
    period:  str = Query("1w",
                         description="yesterday|1w|2w|mtd|ytd|custom"),
    from_:   Optional[str] = Query(None, alias="from",
                                   description="YYYY-MM-DD (custom)"),
    to:      Optional[str] = Query(None, description="YYYY-MM-DD (custom)"),
    mode:    str = Query("live", description="live|paper|all (default live)"),
    user_id: Optional[str] = Query(None,
                                   description="admin-only: filter to one user"),
    caller:  Caller = Depends(require_power_caller),
):
    """One CSV row per CLOSED trade in the window (streamed). Same filters as
    /summary. Never raises on a bad row (skips + logs)."""
    filter_uid = _admin_filter_user_id(caller, user_id)

    def _gen():
        # Own the connection for the life of the stream (StreamingResponse pulls
        # lazily; the `with` must stay open until the generator is exhausted).
        with falcon_conn() as con:
            yield from svc.iter_csv_rows(
                con,
                viewer_user_id=caller.user_id,
                is_admin=caller.is_admin,
                period=period,
                from_date=from_,
                to_date=to,
                mode=mode,
                filter_user_id=filter_uid,
                now_ist=datetime.now(IST),
            )

    safe_period = "".join(c for c in period if c.isalnum() or c in ("-", "_")) or "period"
    filename = f"autotrade_trades_{safe_period}.csv"
    return StreamingResponse(
        _gen(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
