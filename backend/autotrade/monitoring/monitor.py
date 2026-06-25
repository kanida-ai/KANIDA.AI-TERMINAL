"""PortfolioMonitor — tick-level aggregate gross return.

compute_gross_return(session_id) = sum(unrealised_pnl over OPEN positions)
                                   / total_allocated_capital

CRITICAL (spec Section 10 + addendum): the denominator is ALWAYS the original
total_allocated_capital. It does NOT shrink as positions are closed by trailing
stops / time-bound exits. A per-position exit reduces the numerator only.

DATA-ISOLATION: reads/writes ONLY autotrade_positions WHERE session_id=?.
Never touches falcon_position_state.

Mark source (pricing.resolve_*): KiteTicker / broker LTP if available, else the
latest close from ohlc_daily, else the position's entry price — never a stale
unrelated value.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from falcon.db import falcon_conn
from .pricing import resolve_brokers_ltp

log = logging.getLogger("kanida.autotrade.monitor")
IST = timezone(timedelta(hours=5, minutes=30))


class PortfolioMonitor:
    def __init__(self, session_id: str, total_allocated_capital: float):
        if total_allocated_capital <= 0:
            raise ValueError("total_allocated_capital must be > 0")
        self.session_id = session_id
        # Frozen at construction — the single source of truth denominator.
        self._total_allocated_capital = float(total_allocated_capital)

    @property
    def total_allocated_capital(self) -> float:
        return self._total_allocated_capital

    def _open_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT symbol, qty, avg_price, ltp, unrealised_pnl
                   FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def total_unrealised(self) -> float:
        total = 0.0
        for p in self._open_positions():
            ltp = p.get("ltp")
            if ltp is None:
                continue
            total += (ltp - (p.get("avg_price") or 0)) * (p.get("qty") or 0)
        return total

    def compute_gross_return(self) -> float:
        """sum(uPnL) / total_allocated_capital. Denominator never shrinks."""
        return self.total_unrealised() / self._total_allocated_capital

    def refresh_ltps(self, brokers: Dict[str, Any]) -> int:
        """Mark every open position to market and persist ltp + uPnL.

        Price per position: broker LTP → ohlc_daily latest close → entry price
        (pricing.resolve_brokers_ltp). Returns count updated.
        """
        updated = 0
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT id, symbol, broker_profile, avg_price
                   FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        for r in rows:
            ltp = resolve_brokers_ltp(
                r["symbol"], brokers or {},
                broker_profile=r["broker_profile"],
                fallback_entry=r["avg_price"])
            if ltp is None:
                continue
            with falcon_conn() as con:
                con.execute(
                    """UPDATE autotrade_positions
                       SET ltp=?, unrealised_pnl=(? - avg_price)*qty
                       WHERE id=?""",
                    (ltp, ltp, r["id"]),
                )
                con.commit()
            updated += 1
        return updated

    def snapshot(self) -> Dict[str, Any]:
        """Persist a portfolio snapshot row + return the computed numbers."""
        positions = self._open_positions()
        total_u = self.total_unrealised()
        gr = total_u / self._total_allocated_capital
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_portfolio_snapshots
                   (session_id, snapped_at, gross_return, total_unrealised,
                    total_allocated_capital, n_open_positions)
                   VALUES (?,?,?,?,?,?)""",
                (self.session_id, datetime.now(IST).isoformat(), gr, total_u,
                 self._total_allocated_capital, len(positions)),
            )
            con.execute(
                "UPDATE autotrade_sessions SET last_gross_return=? WHERE session_id=?",
                (gr, self.session_id),
            )
            con.commit()
        return {"gross_return": gr, "total_unrealised": total_u,
                "total_allocated_capital": self._total_allocated_capital,
                "n_open_positions": len(positions)}
