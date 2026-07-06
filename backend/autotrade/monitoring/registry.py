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
                 entry_date: Optional[str] = None,
                 broker_account_id: Optional[str] = None,
                 direction: str = "long") -> None:
        """Insert or update a session position in autotrade_positions.

        Keyed by (session_id, symbol, broker_profile). Seeds the mark (ltp) to
        avg_price so a pre-open gross_return ~= 0 rather than a bogus value.

        PHASE-2 MULTI-TENANT: broker_account_id (NULLABLE) records WHICH vaulted
        account this position was opened through, for per-account audit. NULL =
        the operator/global account (today's behaviour).

        FUTURES long/short: `direction` ('long' default | 'short') is persisted
        so the P&L sign + exit side invert ONLY for shorts. 'long' is byte-for-
        byte unchanged.
        """
        now = datetime.now(IST).isoformat()
        direction = "short" if str(direction).lower() == "short" else "long"
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
                           direction=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price),
                           ltp=COALESCE(ltp, ?),
                           status='OPEN'
                       WHERE id=?""",
                    (qty, avg_price, instrument_type, exchange, direction,
                     sl_level, target_price, avg_price, existing[0]),
                )
            else:
                con.execute(
                    """INSERT INTO autotrade_positions
                       (session_id, broker_profile, broker_account_id, symbol,
                        instrument_type, exchange, qty, avg_price, sl_level,
                        target_price, ltp, unrealised_pnl, status, exit_lock,
                        opened_at, direction)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'OPEN', 0, ?, ?)""",
                    (self.session_id, broker_profile, broker_account_id, symbol,
                     instrument_type, exchange, qty, avg_price, sl_level,
                     target_price, avg_price, 0.0, now, direction),
                )
            con.commit()

    def register_partial(self, symbol: str, broker_profile: str,
                         filled_qty: int, avg_price: float,
                         product: str = "CNC",
                         instrument_type: str = "EQ",
                         exchange: Optional[str] = None,
                         broker_account_id: Optional[str] = None,
                         direction: str = "long") -> None:
        """Partial fill: register the FILLED qty (spec parity check #3 — 80 not
        0/100)."""
        log.warning("Partial fill registered: %s filled=%d", symbol, filled_qty)
        self.register(symbol=symbol, broker_profile=broker_profile,
                      qty=filled_qty, avg_price=avg_price, product=product,
                      instrument_type=instrument_type, exchange=exchange,
                      broker_account_id=broker_account_id,
                      direction=direction)

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
            # FUTURES long/short: sign-aware uPnL. For 'long' the CASE is +1 so
            # this is byte-identical to (ltp-avg)*qty; for 'short' it is
            # (avg-ltp)*qty (profit when price falls).
            con.execute(
                """UPDATE autotrade_positions
                   SET ltp=?,
                       unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                            ELSE 1 END) * (? - avg_price) * qty
                   WHERE id=?""",
                (ltp, ltp, row["id"]),
            )
            con.commit()

    def mark_closed(self, symbol: str, reason: str,
                    exit_price: Optional[float] = None,
                    broker_profile: Optional[str] = None) -> None:
        now = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            # FUTURES long/short: sign-aware realised P&L. 'long' CASE = +1 →
            # byte-identical to (exit-avg)*qty; 'short' = (avg-exit)*qty.
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET status='CLOSED', close_reason=?, closed_at=?,
                           exit_price=COALESCE(?, ltp),
                           realised_pnl=(CASE WHEN direction='short' THEN -1
                                              ELSE 1 END)
                                        * (COALESCE(?, ltp, avg_price) - avg_price)*qty
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
                           realised_pnl=(CASE WHEN direction='short' THEN -1
                                              ELSE 1 END)
                                        * (COALESCE(?, ltp, avg_price) - avg_price)*qty
                       WHERE session_id=? AND symbol=?""",
                    (reason, now, exit_price, exit_price,
                     self.session_id, symbol),
                )
            con.commit()

    def set_qty(self, symbol: str, qty: int,
                avg_price: Optional[float] = None,
                broker_profile: Optional[str] = None) -> None:
        """Correct a position's qty (and optionally avg_price) to the BROKER's
        truth — used by the authoritative position reconciler when the broker's
        net qty diverges from our DB (partial fill, missed reconcile). Recomputes
        unrealised_pnl from the persisted ltp so the panel stays consistent.

        Sign-aware uPnL (FUTURES long/short): long CASE = +1 (byte-identical to
        (ltp-avg)*qty), short = (avg-ltp)*qty. avg_price is updated only when a
        non-None value is passed (COALESCE keeps the existing avg otherwise).
        Scoped to (session_id, symbol[, broker_profile])."""
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           avg_price=COALESCE(?, avg_price),
                           unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                                ELSE 1 END)
                               * (COALESCE(ltp, avg_price)
                                  - COALESCE(?, avg_price)) * ?
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (qty, avg_price, avg_price, qty,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET qty=?,
                           avg_price=COALESCE(?, avg_price),
                           unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                                ELSE 1 END)
                               * (COALESCE(ltp, avg_price)
                                  - COALESCE(?, avg_price)) * ?
                       WHERE session_id=? AND symbol=?""",
                    (qty, avg_price, avg_price, qty, self.session_id, symbol),
                )
            con.commit()

    # ── GTT-OCO backup (FEATURE 1) ────────────────────────────────────────────
    def set_gtt(self, symbol: str, gtt_id: Optional[str],
                gtt_stop: Optional[float] = None,
                gtt_target: Optional[float] = None,
                broker_profile: Optional[str] = None) -> None:
        """Store the per-position GTT-OCO id + levels on the position row.

        In paper mode gtt_id is None (no real GTT) but gtt_stop/gtt_target are
        still recorded so the UI shows the intended floor/ceiling. Also mirrors
        the levels into sl_level / target_price for the existing UI fields.
        Scoped to (session_id, symbol[, broker_profile])."""
        with falcon_conn() as con:
            if broker_profile is not None:
                con.execute(
                    """UPDATE autotrade_positions
                       SET gtt_id=?, gtt_stop=?, gtt_target=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price)
                       WHERE session_id=? AND symbol=?
                         AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                    (gtt_id, gtt_stop, gtt_target, gtt_stop, gtt_target,
                     self.session_id, symbol, broker_profile),
                )
            else:
                con.execute(
                    """UPDATE autotrade_positions
                       SET gtt_id=?, gtt_stop=?, gtt_target=?,
                           sl_level=COALESCE(?, sl_level),
                           target_price=COALESCE(?, target_price)
                       WHERE session_id=? AND symbol=?""",
                    (gtt_id, gtt_stop, gtt_target, gtt_stop, gtt_target,
                     self.session_id, symbol),
                )
            con.commit()

    def get_open_positions_missing_gtt(self) -> List[Dict[str, Any]]:
        """OPEN positions that have NO gtt_id yet — used to backfill the broker
        backup on session start and boot-resume (e.g. positions opened before
        this feature deployed)."""
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT * FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0
                     AND (gtt_id IS NULL OR gtt_id='')""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

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
        # Release the exit gate so a future retry can reclaim it.
        # Import here to avoid circular import at module level.
        from autotrade.exit_gate import release_exit_session
        release_exit_session(self.session_id, symbol)

    def update_partial_exit(self, symbol: str, filled_qty: int,
                            exit_price: Optional[float],
                            broker_profile: Optional[str] = None) -> None:
        """Record a partial fill on an exit — reduce qty and record partial P&L.

        The position remains OPEN (status unchanged) so the remaining qty is still
        tracked. The realised_pnl is updated to reflect the partial exit at
        exit_price (or current ltp when exit_price is None/0).
        Called by exit_poller.confirm_exit when Kite reports COMPLETE but filled_qty
        < expected qty.
        """
        now = datetime.now(IST).isoformat()
        log.warning(
            "partial exit for %s/%s: filled=%d exit_price=%s",
            self.session_id, symbol, filled_qty, exit_price)
        with falcon_conn() as con:
            row = con.execute(
                """SELECT avg_price, qty, direction FROM autotrade_positions
                   WHERE session_id=? AND symbol=?""",
                (self.session_id, symbol),
            ).fetchone()
            if row is None:
                log.warning("update_partial_exit: no row for %s/%s",
                            self.session_id, symbol)
                return
            avg_price = float(row["avg_price"] or 0)
            orig_qty = int(row["qty"] or 0)
            remaining_qty = max(0, orig_qty - filled_qty)
            # Compute realised P&L for the filled portion only. FUTURES
            # long/short: sign +1 for long (byte-identical), -1 for short.
            fill_price = float(exit_price) if exit_price else avg_price
            _sign = -1.0 if str(row["direction"]).lower() == "short" else 1.0
            partial_realised = _sign * (fill_price - avg_price) * filled_qty
            con.execute(
                """UPDATE autotrade_positions
                   SET qty=?,
                       realised_pnl=COALESCE(realised_pnl, 0.0) + ?,
                       exit_price=COALESCE(?, exit_price)
                   WHERE session_id=? AND symbol=?""",
                (remaining_qty, partial_realised, exit_price,
                 self.session_id, symbol),
            )
            con.commit()
