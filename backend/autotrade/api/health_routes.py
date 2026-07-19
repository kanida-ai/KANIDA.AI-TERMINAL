"""GET /api/health/system — the read-only correlated platform-health view.

AUTH: operator-token gated with the SAME dependency + semantics as the AutoTrade
routes (X-Operator-Token vs server-side FALCON_OPERATOR_TOKEN, fail-closed if
unset, constant-time compare). The health view can name subsystem status, so it
is operator/admin-only — never public.

READ-ONLY: this endpoint never triggers an action. When the layer is disabled
(default) it returns {"enabled": false, "status": "disabled"}. When enabled it
returns the latest persisted correlated view; `?refresh=1` forces a fresh collect
(still NO page — a GET must never page). Every path is additive + safe.
"""
from __future__ import annotations

import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query

log = logging.getLogger("kanida.sysagents.api")


def require_operator_token(
        x_operator_token: Optional[str] = Header(default=None)) -> None:
    """Operator-token gate — identical semantics to autotrade_routes /
    trade_router (X-Operator-Token vs FALCON_OPERATOR_TOKEN, fail-closed if the
    server secret is unset, constant-time compare)."""
    expected = os.environ.get("FALCON_OPERATOR_TOKEN", "").strip()
    if not expected:
        raise HTTPException(503, "operator token not configured on server")
    if not x_operator_token or not secrets.compare_digest(
            x_operator_token, expected):
        raise HTTPException(403, "operator token required")


router = APIRouter(dependencies=[Depends(require_operator_token)])


@router.get("/health/system")
def system_health(refresh: int = Query(default=0, ge=0, le=1)):
    """The current correlated platform-health view (per-subsystem status + the
    orchestrator's incident summary). Read-only; never pages."""
    from ..sysagents import orchestrator
    return orchestrator.current_view(refresh=bool(refresh))
