"""Strategy-intent intake routes — /api/autotrade/intents/* (operator-token gated, like the rest of autotrade).

The calling app (kanida-app Strategy Builder) holds only the operator token. ARMING additionally needs
X-Operator-Arm-Token == FALCON_OPERATOR_ARM_TOKEN, a secret the calling app never holds, so an app can never arm
itself. Disarm needs only the operator token (it can only make things safer).
"""
from __future__ import annotations

import asyncio
import os
import secrets
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from ..intents import dispatcher as D
from ..intents import gates as G
from ..intents import policy
from ..intents import store
from .autotrade_routes import require_operator_token, resolve_caller

router = APIRouter(prefix="/autotrade/intents", tags=["AutoTrade-Intents"],
                   dependencies=[Depends(require_operator_token)])
_tasks: set = set()


def _err(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": message, "code": code})


def _scope_user(request: Request, claimed: Optional[str]) -> Optional[str]:
    """An authenticated end-user can act only as themselves; the operator/service path may name the user."""
    caller = resolve_caller(request)
    if caller.authenticated and not caller.is_admin:
        if claimed not in (None, "", caller.user_id):
            raise HTTPException(403, "cannot act for another user")
        return caller.user_id
    return claimed


def _visible(request: Request, rec: Dict[str, Any]) -> bool:
    caller = resolve_caller(request)
    return not caller.authenticated or caller.is_admin or rec.get("user_id") == caller.user_id


def _spawn(iid: str) -> None:
    task = asyncio.get_running_loop().create_task(D.run(iid))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@router.get("/capability")
def capability(request: Request, broker: str = "zerodha", user_id: Optional[str] = None,
               broker_account_id: Optional[str] = None):
    uid = _scope_user(request, user_id)
    ev = G.evaluate(broker=broker.lower(), user_id=uid, broker_account_id=broker_account_id)
    arm = ev["arm"]
    return {"live_allowed": ev["live_allowed"], "gates": ev["gates"], "default_mode": "dry_run",
            "accepts": {"underlyings": sorted(policy.UNDERLYINGS),
                        "risk": "defined only", "order_type": "LIMIT", "sequencing": "BUY groups before SELL groups"},
            "arm": None if not arm else {k: arm[k] for k in ("expires_at", "armed_by", "max_baskets", "baskets_used",
                                                              "max_loss_per_basket")}}


@router.post("")
async def submit(request: Request, body: Dict[str, Any] = Body(...)):
    body = dict(body)
    body["user_id"] = _scope_user(request, body.get("user_id"))
    try:
        rec, replayed = await asyncio.to_thread(D.submit, body)     # sync DB work off the event loop
    except D.IntakeError as e:
        return _err(e.status, e.code, e.message)
    if not replayed and rec["state"] == "accepted":
        _spawn(rec["id"])
    return JSONResponse(status_code=200 if replayed else 201, content={"intent": rec, "replayed": replayed})


@router.get("")
def list_intents(request: Request, source: Optional[str] = None, limit: int = 50):
    caller = resolve_caller(request)
    uid = caller.user_id if caller.authenticated and not caller.is_admin else None
    return {"intents": store.list_for(source, uid, max(1, min(limit, 200)))}


@router.get("/{iid}")
def get_intent(request: Request, iid: str):
    rec = store.get(iid)
    if not rec or not _visible(request, rec):
        raise HTTPException(404, "intent not found")
    return {"intent": rec}


@router.post("/{iid}/cancel")
def cancel_intent(request: Request, iid: str):
    rec = store.get(iid)
    if not rec or not _visible(request, rec):
        raise HTTPException(404, "intent not found")
    return {"intent": D.cancel(iid)}


def _require_arm_token(x_operator_arm_token: Optional[str] = Header(default=None)) -> None:
    expected = os.environ.get("FALCON_OPERATOR_ARM_TOKEN", "").strip()
    if not expected:
        raise HTTPException(503, "arming is not configured on this server")
    if secrets.compare_digest(expected, os.environ.get("FALCON_OPERATOR_TOKEN", "").strip()):
        raise HTTPException(503, "the arm token must differ from the operator token")
    if not x_operator_arm_token or not secrets.compare_digest(x_operator_arm_token, expected):
        raise HTTPException(403, "operator arm token required")


@router.post("/arm", dependencies=[Depends(_require_arm_token)])
def arm(body: Dict[str, Any] = Body(...)):
    try:
        ttl = int(body.get("ttl_minutes", 60))
        n = int(body.get("max_baskets", 1))
        cap = float(body.get("max_loss_per_basket"))
    except (TypeError, ValueError):
        return _err(400, "BAD_ARM", "ttl_minutes, max_baskets and max_loss_per_basket must be numbers")
    who = str(body.get("armed_by") or "").strip()
    if not who:
        return _err(400, "BAD_ARM", "armed_by (the human operator's name) is required")
    if not (1 <= ttl <= 480) or not (1 <= n <= 20) or not (0 < cap <= 1_000_000):
        return _err(400, "BAD_ARM", "ttl_minutes 1-480, max_baskets 1-20, max_loss_per_basket above 0")
    uid, acct = str(body.get("user_id") or "").strip(), str(body.get("broker_account_id") or "").strip()
    if not uid or not acct:
        return _err(400, "BAD_ARM", "user_id and broker_account_id are required")
    from .. import vault
    if vault.vault_enabled() and vault.get_decrypted_creds(acct, user_id=uid) is None:
        return _err(400, "BAD_ARM", "that broker account does not belong to that user")
    rec = store.arm(user_id=body.get("user_id"), broker_account_id=body.get("broker_account_id"), armed_by=who[:80],
                    ttl_minutes=ttl, max_baskets=n, max_loss_per_basket=cap, note=str(body.get("note") or ""))
    return {"arm": rec}


@router.post("/disarm")
def disarm(request: Request, body: Dict[str, Any] = Body(default={})):
    uid = _scope_user(request, (body or {}).get("user_id"))
    return {"disarmed": store.disarm(uid, (body or {}).get("broker_account_id"))}
