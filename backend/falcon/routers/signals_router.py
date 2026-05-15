"""/api/falcon/signals/* — live picks API.

Read-mostly. Reads from falcon_signals_live (populated by daily_signals cron).
"""
import json
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..db import falcon_conn

router = APIRouter()


# ── Response models ──────────────────────────────────────────────────────────

class LiveSignal(BaseModel):
    signal_date:       str
    entry_date:        Optional[str]
    rank:              int
    symbol:            str
    sector:            Optional[str]
    n_fires:           int
    score:             float
    close_at_signal:   Optional[float]
    avg_value_60d:     Optional[float]
    fired_pattern_ids: List[int]
    sample_rules:      List[dict]
    engine_version:    str
    emitted_at:        str


class SignalsTodayResponse(BaseModel):
    signal_date:    str
    entry_date:     Optional[str]
    n_picks:        int
    engine_version: str
    picks:          List[LiveSignal]


class SignalDay(BaseModel):
    signal_date: str
    n_picks:     int
    emitted_at:  str


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/falcon/signals/today", response_model=SignalsTodayResponse)
def signals_today(top_n: int = Query(100, ge=1, le=500)):
    """Latest signal_date's emitted picks. Top-N by rank."""
    with falcon_conn() as con:
        latest = con.execute(
            "SELECT MAX(signal_date) FROM falcon_signals_live"
        ).fetchone()[0]
        if latest is None:
            raise HTTPException(404, "No Falcon signals have been emitted yet")
        rows = con.execute(
            """SELECT * FROM falcon_signals_live
               WHERE signal_date = ?
               ORDER BY rank ASC LIMIT ?""", (latest, top_n)
        ).fetchall()

    picks = [_row_to_signal(r) for r in rows]
    entry_date = rows[0]["entry_date"] if rows else None
    engine = rows[0]["engine_version"] if rows else "7.1.0"
    return SignalsTodayResponse(
        signal_date=latest, entry_date=entry_date,
        n_picks=len(picks), engine_version=engine, picks=picks,
    )


@router.get("/falcon/signals/by-date", response_model=SignalsTodayResponse)
def signals_by_date(
    date: str = Query(..., description="YYYY-MM-DD"),
    top_n: int = Query(100, ge=1, le=500),
):
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT * FROM falcon_signals_live
               WHERE signal_date = ?
               ORDER BY rank ASC LIMIT ?""", (date, top_n)
        ).fetchall()
    if not rows:
        raise HTTPException(404, f"No signals emitted on {date}")
    picks = [_row_to_signal(r) for r in rows]
    return SignalsTodayResponse(
        signal_date=date, entry_date=rows[0]["entry_date"],
        n_picks=len(picks), engine_version=rows[0]["engine_version"], picks=picks,
    )


@router.get("/falcon/signals/dates", response_model=List[SignalDay])
def signal_dates(limit: int = Query(60, ge=1, le=365)):
    """Recent signal_dates with emit counts — for the date-picker UI."""
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT signal_date, COUNT(*) AS n_picks, MAX(emitted_at) AS emitted_at
               FROM falcon_signals_live
               GROUP BY signal_date
               ORDER BY signal_date DESC LIMIT ?""", (limit,)
        ).fetchall()
    return [SignalDay(signal_date=r["signal_date"], n_picks=r["n_picks"],
                       emitted_at=r["emitted_at"]) for r in rows]


@router.get("/falcon/signals/stock/{symbol}", response_model=List[LiveSignal])
def signals_for_stock(symbol: str, limit: int = Query(60, ge=1, le=365)):
    """All historical signal events for a single stock."""
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT * FROM falcon_signals_live
               WHERE symbol = ? ORDER BY signal_date DESC LIMIT ?""",
            (symbol.upper(), limit)
        ).fetchall()
    return [_row_to_signal(r) for r in rows]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _row_to_signal(r) -> LiveSignal:
    try:
        fired_ids = json.loads(r["fired_pattern_ids"]) if r["fired_pattern_ids"] else []
    except Exception:
        fired_ids = []
    try:
        samples = json.loads(r["sample_rules"]) if r["sample_rules"] else []
    except Exception:
        samples = []
    return LiveSignal(
        signal_date=r["signal_date"], entry_date=r["entry_date"],
        rank=r["rank"], symbol=r["symbol"], sector=r["sector"],
        n_fires=r["n_fires"], score=r["score"],
        close_at_signal=r["close_at_signal"], avg_value_60d=r["avg_value_60d"],
        fired_pattern_ids=fired_ids, sample_rules=samples,
        engine_version=r["engine_version"], emitted_at=r["emitted_at"],
    )
