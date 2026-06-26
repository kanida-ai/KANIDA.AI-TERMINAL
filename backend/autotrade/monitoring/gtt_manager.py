"""GTTManager — per-position broker-held OCO backup (FEATURE 1 + coordination).

The portfolio kill switch is the PRIMARY exit (software, ours). This module adds
the BACKUP floor: a Kite two-leg OCO GTT per LIVE session position — a STOP leg
(SELL when price <= stop) plus a TARGET leg (SELL when price >= target). The
broker holds it, so a position is still protected if our process is down.

Widths are CONFIG-driven and default WIDER than the portfolio kill_switch_pct so
the portfolio target usually fires first:
    stop   = entry * (1 - per_position_stop_pct)     (default 0.03 → -3%)
    target = entry * (1 + per_position_target_pct)   (default 0.06 → +6%)

SAFETY:
  * LIVE-ONLY real GTTs. In paper mode (broker.place_gtt_oco returns None because
    dry_run/live-disabled) NO real GTT is placed — we still RECORD the intended
    stop/target levels so the UI shows them.
  * Best-effort: a GTT place/cancel failure NEVER blocks entry or the flatten.
  * DATA-ISOLATION: writes ONLY autotrade_positions (via PositionRegistry).

COORDINATION (FEATURE 3):
  * cancel_session_gtts() is called by the kill switch BEFORE/with the market
    exits so no orphan GTTs remain at the broker.
  * reconcile_gtt_fills() detects a position closed externally by a fired GTT
    (no longer in the broker's net positions / GTT gone) → marks the row CLOSED
    with close_reason='GTT'. gross_return then recomputes on the REMAINING
    positions (denominator stays total_allocated_capital — handled by the
    PortfolioMonitor, which reads only OPEN rows).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("kanida.autotrade.gtt_manager")


def compute_levels(entry_price: float, stop_pct: float,
                   target_pct: float) -> Tuple[float, float]:
    """Return (stop_price, target_price) rounded to tick-friendly 2dp.

    stop   = entry * (1 - stop_pct)
    target = entry * (1 + target_pct)
    """
    stop = round(float(entry_price) * (1.0 - float(stop_pct)), 2)
    target = round(float(entry_price) * (1.0 + float(target_pct)), 2)
    return stop, target


class GTTManager:
    def __init__(self, session_id: str, config, brokers: Dict[str, Any],
                 registry):
        self.session_id = session_id
        self.config = config
        self.brokers = brokers          # {broker_profile: BrokerClient}
        self.registry = registry        # PositionRegistry

    # ── Place one position's GTT-OCO ──────────────────────────────────────────
    def place_for_position(self, pos: Dict[str, Any]) -> Dict[str, Any]:
        """Compute levels + place a GTT-OCO for one open position, then persist
        gtt_id + levels. Paper / unsupported broker → records levels only
        (gtt_id stays None). Best-effort — never raises."""
        symbol = pos["symbol"]
        prof_id = pos.get("broker_profile")
        qty = int(pos.get("qty") or 0)
        entry = float(pos.get("avg_price") or 0.0)
        if qty <= 0 or entry <= 0:
            return {"symbol": symbol, "status": "SKIPPED_NO_QTY_OR_PRICE"}

        stop, target = compute_levels(
            entry, self.config.per_position_stop_pct,
            self.config.per_position_target_pct)

        broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
        gtt_id: Optional[str] = None
        if broker is not None:
            # last_price: use a fresh LTP if available, else the entry price.
            try:
                last = broker.get_ltp(symbol) or entry
            except Exception:
                last = entry
            product = (pos.get("instrument_type") and self.config.order_product) \
                or self.config.order_product
            try:
                gtt_id = broker.place_gtt_oco(
                    symbol=symbol, qty=qty, stop_price=stop, target_price=target,
                    last_price=last, product=product,
                    exchange=pos.get("exchange") or "NSE")
            except Exception as e:  # best-effort — never block on the backup
                log.error("place_gtt_oco raised for %s: %s", symbol, e)
                gtt_id = None

        self.registry.set_gtt(symbol, gtt_id, gtt_stop=stop, gtt_target=target,
                              broker_profile=prof_id)
        status = "PLACED" if gtt_id else "RECORDED_ONLY"
        log.info("GTT %s for %s/%s stop=%.2f target=%.2f gtt_id=%s",
                 status, self.session_id, symbol, stop, target, gtt_id)
        return {"symbol": symbol, "status": status, "gtt_id": gtt_id,
                "stop": stop, "target": target}

    # ── Backfill missing GTTs (session start + boot-resume) ───────────────────
    def backfill_missing(self) -> List[Dict[str, Any]]:
        """Place a GTT for every OPEN position that lacks a gtt_id. Idempotent —
        positions that already have a gtt_id are skipped. Paper records levels
        only. Returns per-position outcomes."""
        if not getattr(self.config, "per_position_gtt_enabled", True):
            return []
        out: List[Dict[str, Any]] = []
        for pos in self.registry.get_open_positions_missing_gtt():
            out.append(self.place_for_position(pos))
        return out

    # ── Cancel all session GTTs (called by kill switch before flatten) ────────
    def cancel_session_gtts(self) -> List[Dict[str, Any]]:
        """Best-effort cancel every open position's GTT so no orphan remains at
        the broker after a portfolio flatten. A cancel failure is logged and
        skipped — it must NEVER block the flatten."""
        out: List[Dict[str, Any]] = []
        for pos in self.registry.get_open_positions():
            gtt_id = pos.get("gtt_id")
            if not gtt_id:
                continue
            prof_id = pos.get("broker_profile")
            broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            try:
                broker.cancel_gtt(gtt_id)
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "CANCELLED"})
            except Exception as e:  # never block the flatten on a GTT cancel
                log.warning("cancel_gtt failed for %s (%s): %s",
                            pos["symbol"], gtt_id, e)
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "CANCEL_FAILED", "error": str(e)})
        return out

    async def cancel_session_gtts_async(self) -> List[Dict[str, Any]]:
        """SPEED PASS: parallel GTT-cancel sweep. Same semantics as
        cancel_session_gtts (best-effort, a cancel failure NEVER blocks the
        flatten) but all broker.cancel_gtt calls run CONCURRENTLY via a thread
        pool (cancel_gtt is a sync Kite call) so N GTTs are cancelled in ~one
        round-trip's time, not N serial ones.

        ORDERING (critical, unchanged): the kill switch AWAITS this BEFORE placing
        any market exit, so no orphan GTT can re-fire on a symbol we then flatten.
        """
        targets: List[Dict[str, Any]] = []
        for pos in self.registry.get_open_positions():
            gtt_id = pos.get("gtt_id")
            if not gtt_id:
                continue
            prof_id = pos.get("broker_profile")
            broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            targets.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "broker": broker})
        if not targets:
            return []

        async def _one(t):
            try:
                await asyncio.to_thread(t["broker"].cancel_gtt, t["gtt_id"])
                return {"symbol": t["symbol"], "gtt_id": t["gtt_id"],
                        "status": "CANCELLED"}
            except Exception as e:  # never block the flatten on a GTT cancel
                log.warning("cancel_gtt failed for %s (%s): %s",
                            t["symbol"], t["gtt_id"], e)
                return {"symbol": t["symbol"], "gtt_id": t["gtt_id"],
                        "status": "CANCEL_FAILED", "error": str(e)}

        return list(await asyncio.gather(*[_one(t) for t in targets]))

    # ── Reconcile GTT fills (a position closed externally by a fired GTT) ──────
    def reconcile_gtt_fills(self) -> List[Dict[str, Any]]:
        """Detect positions whose broker-held GTT FIRED (position closed at the
        broker outside our software) and mark the row CLOSED (close_reason='GTT').

        Detection (live only): for each OPEN position with a gtt_id, ask the
        broker for the GTT's state via get_gtt — a 'triggered' status (or a GTT
        that no longer exists) means it fired and the underlying SELL executed.
        Paper / brokers returning None for get_gtt → no-op (nothing to reconcile).

        After marking rows CLOSED here, the caller (session.tick) recomputes
        gross_return on the REMAINING positions; the denominator stays
        total_allocated_capital (PortfolioMonitor reads only OPEN rows)."""
        out: List[Dict[str, Any]] = []
        for pos in self.registry.get_open_positions():
            gtt_id = pos.get("gtt_id")
            if not gtt_id:
                continue
            prof_id = pos.get("broker_profile")
            broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            try:
                state = broker.get_gtt(gtt_id)
            except Exception as e:  # pragma: no cover - defensive
                log.debug("get_gtt failed for %s (%s): %s",
                          pos["symbol"], gtt_id, e)
                continue
            if not self._gtt_fired(state):
                continue
            # GTT fired → the broker already sold this position. Mark CLOSED.
            self.registry.mark_closed(pos["symbol"], "GTT",
                                      broker_profile=prof_id)
            out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                        "status": "CLOSED_GTT"})
            log.warning("GTT FIRED externally for %s/%s — marked CLOSED",
                        self.session_id, pos["symbol"])
        return out

    @staticmethod
    def _gtt_fired(state: Optional[Any]) -> bool:
        """True if the GTT no longer protects an open position — it triggered.

        Kite get_gtt returns a dict with a 'status' field: 'active' (still
        armed), 'triggered'/'cancelled'/'deleted' (no longer guarding). We treat
        anything that is not clearly still 'active' (and is non-None) as fired.
        A None return (paper / unsupported / lookup miss) is NOT a fire — we
        never close a position on missing data."""
        if state is None:
            return False
        status = None
        if isinstance(state, dict):
            status = state.get("status")
        else:
            status = getattr(state, "status", None)
        if status is None:
            return False
        return str(status).lower() not in ("active",)
