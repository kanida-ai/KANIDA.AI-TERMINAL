"""
Jobs & system status router.
GET /api/jobs/status  — pipeline log status + data freshness
GET /api/jobs/context/{ticker}  — enriched stock context
GET /api/jobs/explain/{ticker}  — opportunity explainer
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.get("/jobs/status")
def jobs_status():
    from services.data_freshness import get_freshness
    return get_freshness()


@router.get("/jobs/context/{ticker}")
def stock_context(ticker: str, market: str = "NSE"):
    from services.stock_context import get_stock_context
    ctx = get_stock_context(ticker.upper(), market.upper())
    if not ctx:
        raise HTTPException(status_code=404, detail=f"No OHLCV data for {ticker}")
    return ctx


@router.get("/jobs/explain/{ticker}")
def explain_ticker(ticker: str, market: str = "NSE"):
    from services.opportunity_explainer import explain_ticker
    result = explain_ticker(ticker.upper(), market.upper())
    if not result:
        raise HTTPException(status_code=404, detail=f"No live opportunity for {ticker}")
    return result
