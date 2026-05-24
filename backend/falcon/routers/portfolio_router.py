"""/api/falcon/portfolio/* — backtest results + equity curve API.

Reads from falcon_portfolio_runs (latest stored backtest) so the frontend can
render the historical equity curve, monthly P&L, and per-year metrics that
were validated in offline R&D.
"""
import json
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..db import falcon_conn

router = APIRouter()


class PortfolioSummary(BaseModel):
    engine_version:    str
    starting_capital:  float
    ending_equity:     float
    total_pnl:         float
    return_pct:        float
    trades_taken:      int
    trades_skipped:    int
    win_rate:          float
    win_loss_ratio:    Optional[float]
    avg_win:           float
    avg_loss:          float
    best_trade:        float
    worst_trade:       float
    max_drawdown_pct:  float
    max_concurrent:    int
    avg_utilization:   float
    yearly_pnl:        Dict[str, float]
    monthly_pnl:       Dict[str, float]


class TradeRow(BaseModel):
    symbol:           str
    signal_date:      str
    entry_date:       str
    exit_date:        str
    exit_reason:      str
    n_fires:          int
    score:            float
    net_pnl:          float
    ret_pct:          float


@router.get("/falcon/portfolio/summary", response_model=PortfolioSummary)
def portfolio_summary():
    """Most recent backtest summary stored from offline R&D."""
    with falcon_conn() as con:
        # Try the JSON blob first (if stored from R&D run); fallback to None
        try:
            row = con.execute(
                """SELECT data_json FROM falcon_kv_store
                   WHERE key = 'portfolio_latest_summary'"""
            ).fetchone()
        except Exception:
            row = None
    if not row:
        # Empty placeholder — frontend will show "no backtest yet"
        raise HTTPException(404, "No portfolio summary stored. Run an offline backtest first.")
    data = json.loads(row["data_json"])
    return PortfolioSummary(**data)


@router.get("/falcon/portfolio/trades", response_model=List[TradeRow])
def portfolio_trades(limit: int = 200):
    """Latest stored trade ledger — for the trades tab."""
    with falcon_conn() as con:
        try:
            rows = con.execute(
                """SELECT * FROM falcon_kv_trades ORDER BY entry_date DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        except Exception:
            rows = []
    return [TradeRow(
        symbol=r["symbol"], signal_date=r["signal_date"],
        entry_date=r["entry_date"], exit_date=r["exit_date"],
        exit_reason=r["exit_reason"], n_fires=r["n_fires"], score=r["score"],
        net_pnl=r["net_pnl"], ret_pct=r["ret_pct"],
    ) for r in rows]
