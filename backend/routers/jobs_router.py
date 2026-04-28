"""
Jobs & system status router.
GET  /api/jobs/status           — data freshness + pipeline log status
GET  /api/jobs/context/{ticker} — enriched stock context
GET  /api/jobs/explain/{ticker} — opportunity explainer
POST /api/jobs/run              — manually trigger the pipeline (admin secret required)
GET  /api/jobs/pipeline         — current pipeline run state
"""
from __future__ import annotations

import os
import threading

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

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


@router.get("/jobs/pipeline")
def pipeline_state():
    """Return current pipeline run state without triggering a run."""
    try:
        import main as m
        return m._pipeline_status
    except Exception:
        return {"running": False, "last_run": None, "last_result": None}


class RunRequest(BaseModel):
    secret: str


@router.post("/jobs/run")
def trigger_pipeline(body: RunRequest):
    """Manually trigger the pipeline. Returns immediately; pipeline runs in background."""
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or body.secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        import main as m
        if m._pipeline_status.get("running"):
            return {"status": "already_running", "message": "Pipeline is already in progress."}

        t = threading.Thread(target=m._run_pipeline_sync, daemon=True, name="pipeline-manual")
        t.start()
        return {
            "status":  "started",
            "message": "Pipeline started in background. Check GET /api/jobs/pipeline for progress.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
