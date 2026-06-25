"""PositionRegistry — live position CRUD.

REUSES the existing falcon_position_state table (one row per symbol) plus the
new columns added by the migration (session_id, broker_profile,
total_allocated_capital, unrealised_pnl, exit_lock, exit_initiated_by).

We do NOT create a parallel live_positions table — the spec's Postgres
live_positions maps onto falcon_position_state here (SQLite, AUDIT FINDINGS L5).
This keeps a single source of truth and lets the existing monitor / trail
manager keep working untouched.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.registry")
IST = timezone(timedelta(hours=5, minutes=30))


class PositionRegistry:
    def __init__(self, session_id: str, total_allocated_capital: float):
        self.session_id = session_id
        self.total_allocated_capital = total_allocated_capital

    # ── Register / partial-fill ───────────────────────────────────────────────
    def register(self, *, symbol: str, broker_profile: str, qty: int,
                 avg_price: float, product: str = "CNC",
                 entry_date: Optional[str] = None) -> None:
        """Insert or update a live position row for this session.

        Reuses falcon_position_state. Required NOT-NULL columns from the existing
        schema (initial_sl_price/current_sl_price/target_price/high_water_price)
        are seeded conservatively so the existing monitor can still operate;
        actual SL/target are managed by the existing trail layer at adoption.
        """
        now = datetime.now(IST).isoformat()
        entry_date = entry_date or datetime.now(IST).date().isoformat()
        with falcon_conn() as con:
            existing = con.execute(
                "SELECT symbol FROM falcon_position_state WHERE symbol=?", (symbol,)
            ).fetchone()
            if existing:
                con.execute(
                    """UPDATE falcon_position_state
                       SET qty=?, avg_entry=?, product=?, session_id=?,
                           broker_profile=?, total_allocated_capital=?,
                           last_polled_at=?
                       WHERE symbol=?""",
                    (qty, avg_price, product, self.session_id, broker_profile,
                     self.total_allocated_capital, now, symbol),
                )
            else:
                con.execute(
                    """INSERT INTO falcon_position_state
                       (symbol, managed_by, product, qty, avg_entry,
                        initial_sl_price, current_sl_price, target_price,
                        high_water_price, hw_reached, trail_active,
                        entry_date, hold_days_max, last_seen_price, last_polled_at,
                        session_id, broker_profile, total_allocated_capital,
                        exit_lock)
                       VALUES (?,?,?,?,?,?,?,?,?,0,0,?,7,?,?,?,?,?,0)""",
                    (symbol, "falcon", product, qty, avg_price,
                     avg_price, avg_price, avg_price, avg_price,
                     entry_date, avg_price, now,
                     self.session_id, broker_profile, self.total_allocated_capital),
                )
            con.commit()

    def register_partial(self, symbol: str, broker_profile: str,
                         filled_qty: int, avg_price: float,
                         product: str = "CNC") -> None:
        """Partial fill: register the FILLED qty (spec parity check #3 — 80 not
        0/100)."""
        log.warning("Partial fill registered: %s filled=%d", symbol, filled_qty)
        self.register(symbol=symbol, broker_profile=broker_profile,
                      qty=filled_qty, avg_price=avg_price, product=product)

    # ── Reads ─────────────────────────────────────────────────────────────────
    def get_open_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM falcon_position_state
                   WHERE session_id=? AND qty > 0
                     AND COALESCE(managed_by,'') NOT IN ('falcon_exited')""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT * FROM falcon_position_state WHERE session_id=?",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_ltp(self, symbol: str, ltp: float) -> None:
        with falcon_conn() as con:
            con.execute(
                """UPDATE falcon_position_state
                   SET last_seen_price=?,
                       unrealised_pnl=(? - avg_entry) * qty,
                       last_polled_at=?
                   WHERE symbol=?""",
                (ltp, ltp, datetime.now(IST).isoformat(), symbol),
            )
            con.commit()

    def mark_closed(self, symbol: str, reason: str) -> None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE falcon_position_state SET managed_by='falcon_exited', "
                "last_event_kind=?, last_event_at=? WHERE symbol=?",
                (reason, datetime.now(IST).isoformat(), symbol),
            )
            con.commit()
