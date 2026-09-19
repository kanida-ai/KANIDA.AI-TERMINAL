"""
FastAPI router for the Agent Builder. Drops into the existing backend with ONE line (see INTEGRATION.md):
    from agent_builder.router import router as agent_builder_router
    app.include_router(agent_builder_router, prefix="/api", tags=["Builder"])
Endpoints:
    GET  /api/builder/indicators              -> catalog for the builder form
    POST /api/builder/quote                   -> token cost for a strategy (price before you run)
    POST /api/builder/backtest                -> charge wallet, run backtest, return evidence + worlds
    GET  /api/builder/wallet                  -> balance
    POST /api/builder/wallet/topup            -> add tokens (wire to Razorpay in prod)
Auth: `user_id` comes from get_user_id() — swap it for the existing power-auth JWT dependency in prod.
"""
from __future__ import annotations
from typing import List, Optional, Literal
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from . import engine, wallet, data

router = APIRouter()


class Condition(BaseModel):
    indicator: str
    params: dict = {}
    op: Literal[">", "<", ">=", "<="]
    value: float


class Entry(BaseModel):
    logic: Literal["AND", "OR"] = "AND"
    conditions: List[Condition]


class Exit(BaseModel):
    type: Literal["horizon", "target_stop", "trail"] = "horizon"
    days: int = 5
    target: Optional[float] = None
    stop: Optional[float] = None
    pct: Optional[float] = None
    max_days: int = 20


class Strategy(BaseModel):
    name: str = "My Agent"
    direction: Literal["long", "short"] = "long"
    entry: Entry
    exit: Exit = Exit()
    cost_bps: int = 30
    granularity: Literal["daily", "1min"] = "daily"


def _dict(strat: Strategy) -> dict:
    d = strat.model_dump()
    d["exit"] = {k: v for k, v in d["exit"].items() if v is not None}
    return d


def get_user_id(x_user_id: Optional[str] = Header(default=None)) -> str:
    """Standalone: take X-User-Id header. PROD: replace with the power-auth JWT dependency."""
    if not x_user_id:
        raise HTTPException(401, "missing X-User-Id (wire power-auth JWT in prod)")
    return x_user_id


@router.get("/builder/indicators")
def indicators():
    return {"indicators": [{"name": k, "defaults": v[0], "label": v[1]} for k, v in engine.IND_META.items()],
            "ops": [">", "<", ">=", "<="], "exits": ["horizon", "target_stop", "trail"],
            "directions": ["long", "short"], "universe": {"stocks": data.n_symbols(), "bars": data.n_bars()}}


@router.post("/builder/quote")
def quote(strat: Strategy):
    return {"tokens": engine.token_cost(_dict(strat), strat.granularity)}


@router.get("/builder/wallet")
def get_wallet(user_id: str = None, x_user_id: Optional[str] = Header(default=None)):
    uid = user_id or x_user_id
    if not uid: raise HTTPException(401, "missing user")
    return {"user_id": uid, "balance": wallet.balance(uid)}


@router.post("/builder/wallet/topup")
def topup(tokens: int, x_user_id: Optional[str] = Header(default=None)):
    uid = get_user_id(x_user_id)
    return {"user_id": uid, "balance": wallet.topup(uid, tokens)}


@router.post("/builder/backtest")
def run_backtest(strat: Strategy, x_user_id: Optional[str] = Header(default=None)):
    uid = get_user_id(x_user_id)
    d = _dict(strat)
    cost = engine.token_cost(d, strat.granularity)["total"]
    ok, bal = wallet.charge(uid, cost, reason=f"backtest:{strat.name}")
    if not ok:
        raise HTTPException(402, f"insufficient tokens: need {cost}, have {bal}. Top up to run.")
    result = engine.run(d, strat.granularity)          # charged only after balance confirmed
    result["tokens_charged"] = cost
    result["wallet_balance"] = bal
    return result


@router.get("/builder/health")
def health():
    return {"ok": True, "stocks": data.n_symbols(), "bars": data.n_bars()}
