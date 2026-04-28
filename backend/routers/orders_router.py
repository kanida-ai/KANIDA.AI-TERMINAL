"""
Orders router — autotrade order management.
Requires AUTOTRADE_ENABLED=true env var before any order is actually placed.

GET  /api/orders          — order history (today + recent)
POST /api/orders/place    — place a single order
GET  /api/orders/status   — autotrade system status (enabled/disabled, market open, daily count)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

_HERE = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

IST = timezone(timedelta(hours=5, minutes=30))


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


class PlaceOrderRequest(BaseModel):
    ticker:         str
    direction:      str          # 'BUY' | 'SELL'
    quantity:       int
    order_type:     str = "MARKET"
    price:          Optional[float] = None
    trigger_price:  Optional[float] = None
    product:        str = "CNC"
    opportunity_id: Optional[int] = None
    signal_date:    Optional[str] = None


@router.get("/orders/status")
def orders_status():
    from services.order_service import _is_enabled, _market_is_open, _daily_order_count, MAX_DAILY_ORDERS
    return {
        "autotrade_enabled": _is_enabled(),
        "market_open":       _market_is_open(),
        "daily_orders_placed": _daily_order_count(),
        "daily_order_limit": MAX_DAILY_ORDERS,
        "timestamp_ist":     datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    }


@router.post("/orders/place")
def place_order(body: PlaceOrderRequest):
    from services.order_service import place_order as _place
    try:
        result = _place(
            ticker=body.ticker.upper(),
            direction=body.direction.upper(),
            quantity=body.quantity,
            order_type=body.order_type.upper(),
            price=body.price,
            trigger_price=body.trigger_price,
            product=body.product.upper(),
            opportunity_id=body.opportunity_id,
            signal_date=body.signal_date,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result


@router.get("/orders")
def list_orders(days: int = 7):
    try:
        with _conn() as conn:
            conn.execute("SELECT 1 FROM orders LIMIT 1")
            rows = conn.execute(
                """
                SELECT * FROM orders
                WHERE DATE(created_at) >= DATE('now', ?)
                ORDER BY created_at DESC
                LIMIT 200
                """,
                (f"-{days} days",),
            ).fetchall()
        return {"orders": [dict(r) for r in rows]}
    except Exception:
        return {"orders": [], "note": "Orders table not yet created — place an order first"}
