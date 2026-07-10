"""PortfolioMonitor — tick-level aggregate gross return.

TWO capital bases (both reported; the kill switch measures against the INVESTED
one):

  1. INVESTED (notional) basis — THE KILL BASIS.
     compute_gross_return_invested() = sum(unrealised_pnl over OPEN positions)
                                       / invested_basis
     invested_basis = Σ(qty * avg_price) across the session's positions AT ENTRY,
     FROZEN once when the orders are placed (stored on autotrade_sessions). This
     is the capital actually put to work IN THE PRODUCT and is product-aware
     automatically: under MTF it is the LEVERAGED invested value, under CNC/NRML
     it is the deployed cash. It does NOT shrink as positions close (frozen at
     entry), so returns are measured against what was committed.

  2. ON-FUND basis — the secondary "on your fund/margin" view.
     compute_gross_return() = sum(unrealised_pnl) / total_allocated_capital.

CRITICAL (spec Section 10 + addendum): NEITHER denominator shrinks as positions
are closed by trailing stops / time-bound exits. A per-position exit reduces the
numerator only.

DATA-ISOLATION: reads/writes ONLY autotrade_positions / autotrade_sessions WHERE
session_id=?. Never touches falcon_position_state.

Mark source (pricing.resolve_*): KiteTicker / broker LTP if available, else the
latest close from ohlc_daily, else the position's entry price — never a stale
unrelated value.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn
from .pricing import resolve_brokers_ltp

log = logging.getLogger("kanida.autotrade.monitor")
IST = timezone(timedelta(hours=5, minutes=30))


def compute_kill_preview(*, kill_switch_enabled: bool, kill_switch_pct: float,
                         kill_switch_direction: str, invested_basis: float,
                         total_allocated_capital: float,
                         kill_switch_target_pct: Optional[float] = None,
                         kill_switch_stop_pct: Optional[float] = None
                         ) -> Optional[Dict[str, Any]]:
    """Build the kill_preview object: the ₹ you'd make/lose at the kill
    thresholds, plus the equivalent % on your fund.

    The kill switch measures gross_return on the INVESTED basis, so:
      target ₹ = +target_pct * invested_basis   (target_pct or kill_switch_pct)
      stop   ₹ = -stop_pct   * invested_basis   (stop_pct   or kill_switch_pct)
      fund_pct = that ₹ / total_allocated_capital
    `target` is present when direction is profit/both; `stop` when loss/both.

    ASYMMETRIC (FEATURE B): kill_switch_target_pct / kill_switch_stop_pct, when
    set, OVERRIDE the symmetric kill_switch_pct on their respective side so the
    UI can render distinct +target / -stop distances. When both are None this is
    byte-for-byte the old symmetric preview.
    Returns None when the kill switch is disabled (nothing to preview).

    Shared by TradingSession.status() and the /preview endpoint so both surface
    identical math.
    """
    if not kill_switch_enabled:
        return None
    basis = invested_basis if invested_basis and invested_basis > 0 \
        else total_allocated_capital
    fund = total_allocated_capital if total_allocated_capital and \
        total_allocated_capital > 0 else basis
    pct = float(kill_switch_pct)
    target_pct = float(kill_switch_target_pct) \
        if kill_switch_target_pct is not None else pct
    stop_pct = float(kill_switch_stop_pct) \
        if kill_switch_stop_pct is not None else pct
    out: Dict[str, Any] = {}
    if kill_switch_direction in ("profit", "both"):
        rs = target_pct * basis
        out["target"] = {"pct": target_pct, "basis_value_rs": rs,
                         "fund_pct": rs / fund if fund else 0.0}
    if kill_switch_direction in ("loss", "both"):
        rs = -abs(stop_pct) * basis
        out["stop"] = {"pct": -abs(stop_pct), "basis_value_rs": rs,
                       "fund_pct": rs / fund if fund else 0.0}
    return out


class PortfolioMonitor:
    def __init__(self, session_id: str, total_allocated_capital: float):
        if total_allocated_capital <= 0:
            raise ValueError("total_allocated_capital must be > 0")
        self.session_id = session_id
        # Frozen at construction — the on-fund denominator.
        self._total_allocated_capital = float(total_allocated_capital)

    @property
    def total_allocated_capital(self) -> float:
        return self._total_allocated_capital

    # ── Invested (notional) capital basis — THE KILL BASIS ─────────────────────
    def invested_basis(self) -> float:
        """The FROZEN invested-capital basis for this session.

        Reads autotrade_sessions.invested_basis (captured once at entry =
        Σ qty*avg_price across the session's positions, product-aware: MTF
        leveraged value / CNC cash). Falls back to total_allocated_capital when
        it is missing or 0 (no positions placed yet) so callers never divide by
        zero. Never recomputed from live positions — it must NOT shrink as
        positions close.
        """
        with falcon_conn() as con:
            row = con.execute(
                "SELECT invested_basis FROM autotrade_sessions WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        ib = (row["invested_basis"] if row else None)
        if ib is None or ib <= 0:
            return self._total_allocated_capital
        return float(ib)

    @staticmethod
    def compute_invested_basis(positions: List[Dict[str, Any]]) -> float:
        """Σ(qty * avg_price) over the given positions. Product-aware purely by
        construction — avg_price is the entry fill, so under MTF this sums the
        full (leveraged) invested value and under CNC the deployed cash."""
        total = 0.0
        for p in positions:
            total += float(p.get("qty") or 0) * float(p.get("avg_price") or 0)
        return total

    @staticmethod
    def _is_fno(positions: List[Dict[str, Any]]) -> bool:
        """True if the basket is F&O (any FUT/OPT leg). For F&O, qty*avg_price is
        NOTIONAL exposure (~4-5x the margin), not capital deployed."""
        return any(
            (p.get("instrument_type") or "EQ").upper() in ("FUT", "OPT", "CE", "PE")
            for p in positions)

    def freeze_invested_basis(self) -> float:
        """Capture and FREEZE the trailing/kill capital basis on the session row.
        Idempotent — call once after entries are placed. Falls back to
        total_allocated_capital when there are no positions (avoids 0/div).

        Product-aware:
          * cash equity     -> Σ(qty*avg_price): the real position value (CNC cash
            / MTF leveraged value), as before.
          * FUTURES/OPTIONS -> total_allocated_capital (the allocated capital ≈ the
            margin at risk). qty*avg_price for F&O is NOTIONAL exposure (~4-5x the
            margin), so trailing % MUST key off the capital deployed, not the
            notional — else arm/floor/stop are measured against a ~5x-too-large
            number and the trail barely reacts to the money actually at risk.
        """
        positions = self._open_positions()
        if self._is_fno(positions):
            ib = self._total_allocated_capital
        else:
            ib = self.compute_invested_basis(positions)
        stored = ib if ib > 0 else self._total_allocated_capital
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET invested_basis=? WHERE session_id=?",
                (stored, self.session_id),
            )
            con.commit()
        return stored

    def refreeze_invested_basis(self) -> float:
        """FORCE-recompute the invested basis over the CURRENT OPEN positions and
        write it back onto the session row.

        Unlike freeze_invested_basis() (idempotent-once, called after entry),
        this is used by the AUTHORITATIVE position reconciler AFTER it corrects
        the book (a position closed externally / a qty corrected to the broker):
        the frozen basis must be re-derived from what is ACTUALLY still open so
        the kill / trail denominator matches reality.

        Product-aware, mirroring freeze_invested_basis:
          * F&O basket    → total_allocated_capital (qty*avg is NOTIONAL, ~4-5x
            the margin at risk).
          * cash equity   → Σ(qty*avg_price) over the remaining OPEN positions.
        Falls back to total_allocated_capital when nothing is open (avoids 0/div).
        Returns the value written."""
        positions = self._open_positions()
        if self._is_fno(positions):
            ib = self._total_allocated_capital
        else:
            ib = self.compute_invested_basis(positions)
        stored = ib if ib > 0 else self._total_allocated_capital
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET invested_basis=? WHERE session_id=?",
                (stored, self.session_id),
            )
            con.commit()
        return stored

    # ── PER-STOCK step-lock: frozen entry BASKET NOTIONAL (2026-07-10 Fix A) ────
    def freeze_entry_basket_notional(self) -> float:
        """Capture + FREEZE the ENTRY basket notional = Σ(qty*avg_price) over ALL
        originally-filled positions, onto autotrade_sessions.entry_basket_notional.

        This is the FIXED denominator for the per-stock step-lock capital slice
        (session._run_per_stock_step_lock). Unlike freeze_invested_basis() this is
        PRODUCT-AGNOSTIC — it is the raw Σ(qty*avg) (NOT the F&O→total_capital
        override), because the per-stock slice weight (leg_notional / basket_notional)
        must be on the SAME notional units for both numerator and leg so leverage
        cancels in the proportion and each name's slice is total_cap*(weight).

        Captured ONCE in session._fire_entries after entries are placed; it must
        NOT shrink as names close (the whole point of Fix A — a shrinking Σ-over-OPEN
        denominator inflated survivors' slices, so their -per_stock_stop_pct stop
        fired at a progressively larger rupee loss; BRIGADE/GRANULES 2026-07-10).
        Falls back to total_allocated_capital when nothing was placed (avoids 0/div).
        Single write site → plain overwrite is safe (never re-frozen)."""
        positions = self._open_positions()
        val = self.compute_invested_basis(positions)
        stored = val if val > 0 else self._total_allocated_capital
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET entry_basket_notional=? "
                "WHERE session_id=?",
                (stored, self.session_id),
            )
            con.commit()
        return stored

    def entry_basket_notional(self) -> Optional[float]:
        """The FROZEN entry basket notional (Σ qty*avg at entry), or None when it
        was never captured (a session created before this column, or before its
        entries fired). None → the caller falls back to the live Σ-over-OPEN
        (pre-Fix-A behaviour) so nothing regresses for legacy sessions."""
        with falcon_conn() as con:
            row = con.execute(
                "SELECT entry_basket_notional FROM autotrade_sessions "
                "WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        val = row["entry_basket_notional"] if row else None
        if val is None or val <= 0:
            return None
        return float(val)

    # ── Intraday-basket trail state (strategy=="intraday_basket") ──────────────
    def load_trail_state(self):
        """Read the persisted (armed, peak) trail state for this session.

        Returns a trail_engine.TrailState. Defaults to (armed=False, peak=0.0)
        when unset (a fresh / non-intraday session). Restored on boot-resume so
        a mid-day trail continues correctly after a restart.
        """
        from .trail_engine import TrailState
        with falcon_conn() as con:
            row = con.execute(
                "SELECT trail_armed, trail_peak FROM autotrade_sessions "
                "WHERE session_id=?",
                (self.session_id,),
            ).fetchone()
        if not row:
            return TrailState(armed=False, peak=0.0)
        armed = bool(row["trail_armed"]) if row["trail_armed"] is not None else False
        peak = float(row["trail_peak"]) if row["trail_peak"] is not None else 0.0
        return TrailState(armed=armed, peak=peak)

    def save_trail_state(self, state) -> None:
        """Persist the (armed, peak) trail state on the session row.

        R1 — MONOTONE RATCHET: trail_peak is written as MAX(existing, new) and
        trail_armed as MAX(existing, new). The 5s tick_driver and the sub-second
        ws_driver both load→decide→save this state; a STALE writer (whose loaded
        peak lagged a concurrent ratchet) must never REGRESS the peak — a lower
        peak would drop the exit trigger and over-give-back real profit. The trail
        engine only ever ratchets peak UP and arms 0→1 (never disarms), so MAX is
        behaviour-preserving for the single-writer path and race-safe for two."""
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions "
                "SET trail_armed = MAX(COALESCE(trail_armed,0), ?), "
                "    trail_peak  = MAX(COALESCE(trail_peak,0), ?) "
                "WHERE session_id=?",
                (1 if state.armed else 0, float(state.peak), self.session_id),
            )
            con.commit()

    def _open_positions(self) -> List[Dict[str, Any]]:
        """Return all OPEN positions for this session with the fields needed by
        the trail engine (symbol/qty/avg_price/ltp/unrealised_pnl), the per-stock
        stop (gtt_id/broker_profile/instrument_type) AND the PER-STOCK step-lock
        (pos_trail_armed/pos_trail_peak — each position's own ratchet state)."""
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT symbol, qty, avg_price, ltp, unrealised_pnl,
                          gtt_id, broker_profile, instrument_type, direction,
                          pos_trail_armed, pos_trail_peak
                   FROM autotrade_positions
                   WHERE session_id=? AND status='OPEN' AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def save_per_stock_trail_state(self, symbol: str, state) -> None:
        """Persist ONE position's per-stock step-lock trail state (pos_trail_armed,
        pos_trail_peak) so its ratchet is durable across restarts. Scoped to
        (session_id, symbol) among OPEN rows. Used only in step_lock_scope=="stock".
        NEVER touches the session-level trail_armed/trail_peak (basket state).

        R1 — MONOTONE RATCHET (per-stock): same as save_trail_state, pos_trail_peak
        is written as MAX(existing, new) so an out-of-order writer can't regress a
        position's ratchet high-water and over-give-back."""
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_positions "
                "SET pos_trail_armed = MAX(COALESCE(pos_trail_armed,0), ?), "
                "    pos_trail_peak  = MAX(COALESCE(pos_trail_peak,0), ?) "
                "WHERE session_id=? AND symbol=? AND status='OPEN'",
                (1 if state.armed else 0, float(state.peak),
                 self.session_id, symbol),
            )
            con.commit()

    def get_exit_failed_positions(self) -> List[Dict[str, Any]]:
        """Return EXIT_FAILED positions whose exit gate is FREE (exit_lock=0).

        Only positions with exit_lock=0 are returned.  Once a retry task claims
        the gate (exit_lock=1), this query won't surface the position again until
        mark_exit_failed() releases it — preventing a new task from being spawned
        on every tick while a retry is already in flight.
        """
        with falcon_conn() as con:
            rows = con.execute(
                """SELECT symbol, qty, avg_price, ltp,
                          gtt_id, broker_profile, instrument_type, direction
                   FROM autotrade_positions
                   WHERE session_id=? AND status='EXIT_FAILED'
                     AND qty > 0 AND exit_lock=0""",
                (self.session_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def _total_realised(self) -> float:
        """Sum of realised_pnl across all CLOSED positions for this session.

        Called alongside total_unrealised() so that positions closed by a GTT
        or per-stock stop are included in the gross-return numerator. Without
        this a GTT-closed loss disappears from the numerator and the trail
        engine sees a falsely healthy portfolio.
        Returns 0.0 when there are no closed positions or realised_pnl is NULL.
        """
        with falcon_conn() as con:
            row = con.execute(
                "SELECT COALESCE(SUM(realised_pnl), 0.0) AS total "
                "FROM autotrade_positions "
                "WHERE session_id=? AND status='CLOSED' AND realised_pnl IS NOT NULL",
                (self.session_id,),
            ).fetchone()
        return float(row["total"]) if row else 0.0

    def _total_exit_failed_unrealised(self) -> float:
        """Σ persisted uPnL over EXIT_FAILED positions (qty>0) — C3.

        An EXIT_FAILED leg is still HELD (its exit never confirmed), so it is REAL
        exposure and its live uPnL must stay in the gross-return numerator; without
        this the % understates exposure while invested_basis (frozen at entry)
        still counts these shares in the denominator. refresh_ltps() marks these
        rows to market alongside OPEN so the figure is live. Returns 0.0 when there
        are no EXIT_FAILED rows (byte-identical to before for a normal book)."""
        with falcon_conn() as con:
            row = con.execute(
                "SELECT COALESCE(SUM(unrealised_pnl),0.0) AS total "
                "FROM autotrade_positions "
                "WHERE session_id=? AND status='EXIT_FAILED' AND qty>0 "
                "AND unrealised_pnl IS NOT NULL",
                (self.session_id,),
            ).fetchone()
        return float(row["total"]) if row else 0.0

    def total_unrealised(self) -> float:
        total = 0.0
        for p in self._open_positions():
            ltp = p.get("ltp")
            if ltp is None:
                continue
            # FUTURES long/short: sign +1 for long (byte-identical to before),
            # -1 for short (profit when price falls). invested_basis stays
            # positive for both — only the numerator sign flips.
            sign = -1.0 if str(p.get("direction")).lower() == "short" else 1.0
            total += sign * (ltp - (p.get("avg_price") or 0)) * (p.get("qty") or 0)
        return total

    def compute_gross_return(self) -> float:
        """ON-FUND view: (sum(uPnL) + realised_pnl) / total_allocated_capital.

        Secondary basis — for display only, not the kill basis. Includes
        realised P&L from CLOSED positions so a GTT-closed loss is reflected, and
        the live uPnL of EXIT_FAILED (still-held) legs so % reflects real exposure.
        Denominator never shrinks (frozen total_allocated_capital).
        """
        total_pnl = (self.total_unrealised() + self._total_realised()
                     + self._total_exit_failed_unrealised())
        return total_pnl / self._total_allocated_capital

    def compute_gross_return_invested(self) -> float:
        """KILL BASIS: (sum(uPnL) + realised_pnl) / invested_basis (frozen at entry).

        This is the return on the capital actually put to work in the product
        (MTF leveraged value / CNC cash). The kill switch is checked against
        THIS value. Includes realised P&L from CLOSED positions so that a
        GTT-closed loss cannot inflate the remaining portfolio's apparent return.
        Also includes EXIT_FAILED (still-held) legs so a stranded loss stays in the
        kill/trail numerator (C3). invested_basis() falls back to
        total_allocated_capital when no positions exist, so this never divides by
        zero."""
        total_pnl = (self.total_unrealised() + self._total_realised()
                     + self._total_exit_failed_unrealised())
        return total_pnl / self.invested_basis()

    def refresh_ltps(self, brokers: Dict[str, Any]) -> int:
        """Mark every open position to market and persist ltp + uPnL.

        Price per position: broker LTP → ohlc_daily latest close → entry price
        (pricing.resolve_brokers_ltp). Returns count updated.
        """
        updated = 0
        with falcon_conn() as con:
            # C3: mark EXIT_FAILED (still-held) legs to market too, so their live
            # uPnL feeding the gross-return numerator stays fresh (not the stale
            # mark from the tick their exit failed).
            rows = con.execute(
                """SELECT id, symbol, broker_profile, avg_price
                   FROM autotrade_positions
                   WHERE session_id=? AND status IN ('OPEN','EXIT_FAILED')
                     AND qty > 0""",
                (self.session_id,),
            ).fetchall()
        rows = [dict(r) for r in rows]
        brokers = brokers or {}

        # P1(a): ONE batched LTP call per broker profile (get_ltps_batch = one WS
        # pass + a single kite.ltp() REST fallback for the whole list) instead of
        # a per-symbol broker round-trip. Only symbols the batch MISSED fall back
        # to the per-symbol resolver (ohlc close → entry). O(profiles) broker
        # calls per tick, not O(positions). Byte-identical marks (the default
        # get_ltps_batch loops get_ltp, so a mock/stub is unchanged).
        from collections import defaultdict as _dd
        syms_by_prof: Dict[Optional[str], set] = _dd(set)
        for r in rows:
            syms_by_prof[r["broker_profile"]].add(r["symbol"])
        batched: Dict[tuple, float] = {}
        for prof, syms in syms_by_prof.items():
            broker = brokers.get(prof) or (
                next(iter(brokers.values()), None) if brokers else None)
            if broker is None:
                continue
            try:
                b = broker.get_ltps_batch(list(syms)) or {}
            except Exception as e:  # pragma: no cover - defensive
                log.debug("refresh_ltps: get_ltps_batch failed for %s: %s",
                          prof, e)
                b = {}
            for s, v in b.items():
                if v is not None and float(v) > 0:
                    batched[(prof, s)] = float(v)

        now_iso = datetime.now(IST).isoformat()
        with falcon_conn() as con:
            for r in rows:
                ltp = batched.get((r["broker_profile"], r["symbol"]))
                if ltp is None:
                    # Residual miss only → per-symbol resolve (broker single probe
                    # → ohlc close → entry). Unchanged fallback semantics.
                    ltp = resolve_brokers_ltp(
                        r["symbol"], brokers,
                        broker_profile=r["broker_profile"],
                        fallback_entry=r["avg_price"])
                if ltp is None:
                    continue
                # FUTURES long/short: sign-aware persisted uPnL so the trail /
                # kill see profit correctly for shorts. 'long' CASE = +1 →
                # byte-identical to (ltp-avg)*qty. Lifecycle#8: stamp the mark
                # time so a stalled tick (stale mark) is visible in status().
                con.execute(
                    """UPDATE autotrade_positions
                       SET ltp=?, ltp_as_of=?,
                           unrealised_pnl=(CASE WHEN direction='short' THEN -1
                                                ELSE 1 END) * (? - avg_price)*qty
                       WHERE id=?""",
                    (ltp, now_iso, ltp, r["id"]),
                )
                updated += 1
            con.commit()
        return updated

    def snapshot(self) -> Dict[str, Any]:
        """Persist a portfolio snapshot row + return the computed numbers."""
        positions = self._open_positions()
        total_u = self.total_unrealised()
        # Lifecycle#7 — the persisted snapshot / last_gross_return must MATCH the
        # live panel, which includes realised P&L (CLOSED legs) + EXIT_FAILED
        # (still-held) uPnL. Previously this stored uPnL-only, so history diverged
        # from the panel the moment any leg closed.
        gr = (total_u + self._total_realised()
              + self._total_exit_failed_unrealised()) / self._total_allocated_capital
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
