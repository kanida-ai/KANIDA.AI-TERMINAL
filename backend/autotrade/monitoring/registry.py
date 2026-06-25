"""PositionRegistry — ISOLATED autotrade session position CRUD.

DATA-ISOLATION (CRITICAL FIX): all session positions live in the dedicated
`autotrade_positions` table, keyed by (session_id, symbol[, broker_profile]).
This registry NEVER reads, writes, upserts, or locks `falcon_position_state` —
that table is owned by the existing Falcon swing system and keyed by `symbol`,
so a paper session writing into it would overwrite a real held position and
plant a phantom row the live monitor would put a real SL on.

Every method here is scoped to self.session_id; a session can only ever see /
mutate its own rows.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn
from .pricing import resolve_ltp

log = logging.getLogger("kanida.autotrade.registry")
IST = timezone(timedelta(hours=5, minutes=30))


class PositionRegistry:
    def __init__(self, session_id: str, total_allocated_capital: float):
        self.session_id = session_id
        self.total_allocated_capital = total_allocated_capital

    # ── Register / partial-fill ───────────────────────────────────────────────
    def register(self, *, symbol: str, broker_profile: str, qty: int,
                 avg_price: float, product: str = "CNC",
                 instrument_type: str = "EQ", exchange: Optional[str] = None,
                 sl_level: Optional[float] = None,
                 target_price: Optional[float] = None,
                 entry_date: Optional[str] = None) -> None:
        """Insert or update a session position in autotrade_positions.

        Keyed by (session_id, symbol, broker_profile). Seeds the mark (ltp) to
        avg_price so a pre-open gross_return ~= 0 rather than a bogus value.
        """
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            existing = con.execute(
                """SELECT id FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (self.session_id, symbol, broker_profile),
            ).fetchone()
            if existing:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?, avg_price=?, instrument_type=?, exchange=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price),
                           ltp=COALESCE(ltp, ?),
                           status='OPEN'
                       WHERE id=?""",
                    (qty, avg_price, instrument_type, exchange,
                     sl_level, target_price, avg_price, existing[0]),
                )
            else:
                con.execute(
                    """INSERT INTO autotrade_positions
                       (session_id, broker_profile, symbol, instrument_type,
                        exchange, qty, avg_price, sl_level, target_price, ltp,
                        unrealised_pnl, status, exit_lock, opened_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?, 'OPEN', 0, ?)""",
                    (self.session_id, broker_profile, symbol, instrument_type,
                     exchange, qty, avg_price, sl_level, target_price, avg_price,
                     0.0, now),
                )
            con.commit()

    def register_partial(self, symbol: str, broker_profile: str,
                         filled_qty: int, avg_price: float,
                         product: str = "CNC",
                         instrument_type: str = "EQ") -> None:
        """Partial fill: register the FILLED qty (spec parity check #3 — 80 not
        0/100)."""
        log.warning("Partial fill registered: %s filled=%d", symbol, filled_qty)
        self.register(symbol=symbol, broker_profile=broker_profile,
                      qty=filled_qty, avg_price=avg_price, product=product,
                      instrument_type=instrument_type)

    # ── Reads ─────────────────────────────────────────────────────────────────
    def get_open_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_positions(self) -> List[Dict[str, Any]]:
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT * FROM autotrade_positions WHERE session_id=?",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_ltp(self, symbol: str, ltp: Optional[float] = None,
                   broker: Any = None,
                   broker_profile: Optional[str] = None) -> None:
        """Mark a position to market and recompute uPnL.

        If `ltp` is None, resolve a SANE price (broker LTP → ohlc_daily latest
        close → existing avg_price) so we never persist a stale unrelated value.
        Scoped to (session_id, symbol[, broker_profile]).
        """
        with falcon_conn() as con:
            row = con.execute(
                """SELECT id, avg_price FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (self.session_id, symbol, broker_profile),
            ).fetchone()
            if row is None:
                # If no broker_profile given, fall back to any row for the symbol.
                row = con.execute(
                    """SELECT id, avg_price FROM autotrade_positions
                       WHERE session_id=? AND symbol=?""",
                    (self.session_id, symbol),
                ).fetchone()
            if row is None:
                return
            if ltp is None:
                ltp = resolve_ltp(symbol, broker=broker,
                                  fallback_entry=row["avg_price"])
            if ltp is None:
                return
            con.execute(
                """UPDATE autotrade_positions
                   SET ltp=?, unrealised_pnl=(? - avg_price) * qty
                   WHERE id=?""",
                (ltp, ltp, row["id"]),
            )
            con.commit()

    def mark_closed(self, symbol: str, reason: str,
                    exit_price: Optional[float] = None,
                    broker_profile: Optional[str] = None) -> None:
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='CLOSED', close_reason=?, closed_at=?,
                           exit_price=COALESCE(?, ltp),
                           realised_pnl=(COALESCE(?, ltp, avg_price) - avg_price)*qty
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (reason, now, exit_price, exit_price,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='CLOSED', close_reason=?, closed_at=?,
                           exit_price=COALESCE(?, ltp),
                           realised_pnl=(COALESCE(?, ltp, avg_price) - avg_price)*qty
                       WHERE session_id=? AND symbol=?""",
                    (reason, now, exit_price, exit_price,
                     self.session_id, symbol),
                )
            con.commit()

    def mark_exit_failed(self, symbol: str, error: str,
                         broker_profile: Optional[str] = None) -> None:
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            con.execute(
                """UPDATE autotrade_positions
                   SET status='EXIT_FAILED', close_reason=?, closed_at=?
                   WHERE session_id=? AND symbol=?""",
                (f"EXIT_FAILED: {error}", now, self.session_id, symbol),
            )
            con.commit()
