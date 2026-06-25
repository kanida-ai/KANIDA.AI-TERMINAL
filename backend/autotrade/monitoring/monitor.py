"""PortfolioMonitor — tick-level aggregate gross return.

compute_gross_return(session_id) = sum(unrealised_pnl over OPEN positions)
                                   / total_allocated_capital

CRITICAL (spec Section 10 + addendum): the denominator is ALWAYS the original
total_allocated_capital. It does NOT shrink as positions are closed by trailing
stops / time-bound exits. A per-position exit reduces the numerator only.

Tick source: Zerodha WS via kite_ticker.get_ltp, REST fallback to broker.get_ltp.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

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
                """SELECT symbol, qty, avg_entry, last_seen_price, unrealised_pnl
                   FROM falcon_position_state
                   WHERE session_id=? AND qty > 0
                     AND COALESCE(managed_by,'') <> 'falcon_exited'""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def total_unrealised(self) -> float:
        total = 0.0
        for p in self._open_positions():
            ltp = p.get("last_seen_price")
            if ltp is None:
                continue
            total += (ltp - (p.get("avg_entry") or 0)) * (p.get("qty") or 0)
        return total

    def compute_gross_return(self) -> float:
        """sum(uPnL) / total_allocated_capital. Denominator never shrinks."""
        return self.total_unrealised() / self._total_allocated_capital

    def refresh_ltps(self, brokers: Dict[str, Any]) -> int:
        """Pull fresh LTP per open position from its owning broker, persist it.
        `brokers` is {broker_profile: BrokerClient}. Returns count updated."""
        updated = 0
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT symbol, broker_profile FROM falcon_position_state
                   WHERE session_id=? AND qty > 0
                     AND COALESCE(managed_by,'') <> 'falcon_exited'""",
                (self.session_id,),
            ).fetchall()
        for r in rows:
            broker = brokers.get(r["broker_profile"]) or next(iter(brokers.values()), None)
            if broker is None:
                continue
            ltp = broker.get_ltp(r["symbol"])
            if ltp is None:
                continue
            with falcon_conn() as con:
                con.execute(
                    """UPDATE falcon_position_state
                       SET last_seen_price=?, unrealised_pnl=(? - avg_entry)*qty,
                           last_polled_at=?
                       WHERE symbol=?""",
                    (ltp, ltp, datetime.now(IST).isoformat(), r["symbol"]),
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
