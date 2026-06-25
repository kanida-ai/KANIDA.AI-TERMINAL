"""Slippage recording + report (spec 5.3).

record_slippage writes a row to autotrade_slippage and fires an alert when
|slippage| > 0.5%. Pure DB + alert; no order side effects.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.execution.slippage")
IST = timezone(timedelta(hours=5, minutes=30))

HIGH_SLIPPAGE_PCT = 0.5


def record_slippage(symbol: str, expected_price: float, actual_price: float,
                    qty: int, session_id: Optional[str] = None,
                    broker_profile: Optional[str] = None) -> float:
    if expected_price <= 0:
        slippage_pct = 0.0
    else:
        slippage_pct = (actual_price - expected_price) / expected_price * 100
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_slippage
               (session_id, broker_profile, symbol, expected_price, actual_price,
                slippage_pct, qty, recorded_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (session_id, broker_profile, symbol, expected_price, actual_price,
             slippage_pct, qty, datetime.now(IST).isoformat()),
        )
        con.commit()
    if abs(slippage_pct) > HIGH_SLIPPAGE_PCT:
        from ..alerts import send
        send(f"High slippage on {symbol}: {slippage_pct:.2f}%", severity="warn")
    return slippage_pct


def slippage_report(session_id: str) -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT * FROM autotrade_slippage WHERE session_id=? "
            "ORDER BY recorded_at DESC", (session_id,)
        ).fetchall()
    return [dict(r) for r in rows]
