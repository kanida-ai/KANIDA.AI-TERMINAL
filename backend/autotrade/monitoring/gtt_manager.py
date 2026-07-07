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
import os
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("kanida.autotrade.gtt_manager")

# ── GTT limit-price slippage buffer ──────────────────────────────────────────
# When price gaps below a GTT stop trigger the limit sell order must be set
# BELOW the trigger so it fills even in a fast gap.  Without the buffer the
# limit equals the trigger exactly and the order can be stuck pending.
#
# FALCON_GTT_STOP_BUFFER: fraction below the stop trigger used as limit price.
#   default 0.003 (0.3%)  →  limit = trigger * (1 - 0.003)
# Override at runtime: set env FALCON_GTT_STOP_BUFFER=0.005 for 0.5%.
_GTT_STOP_LIMIT_BUFFER_DEFAULT = 0.003


# ── GUARD G1 (mode F5): NO GTT on intraday / MIS legs ─────────────────────────
# HIT-LIVE 2026-07-06: MIS positions had GTT-OCO stops placed. MIS is auto-squared
# same-day (~15:12 by the square-off scheduler + the tick-driver stop). A GTT that
# survives past square-off is ORPHANED — if it later triggers it SELLS shares we no
# longer hold → a naked SHORT. Protection for an MIS leg is the tick-driver
# arm/stop + the intraday square-off, NOT a broker GTT. So GTTs are placed ONLY
# for CARRIED products (CNC / MTF / NRML); MIS is ALWAYS suppressed.
#
# _gtt_allowed_for_product is the SINGLE choke-point every GTT-place path funnels
# through (place_for_position) so the gate can't be bypassed. It mirrors the
# reconciler's _kite_product normalisation (EQ→CNC) so the product bucket the GTT
# gate keys on is the SAME one the reconciler buckets the invariant on.
_CARRIED_PRODUCTS = {"CNC", "MTF", "NRML"}


def _norm_product(product: Optional[str]) -> str:
    """Normalise to CNC | MIS | NRML | MTF (EQ→CNC), matching the reconciler's
    position_reconciler._kite_product so GTT gating and invariant bucketing agree."""
    p = str(product or "CNC").upper()
    if p == "EQ":
        return "CNC"
    if p in ("CNC", "MIS", "NRML", "MTF"):
        return p
    return "CNC"


def _gtt_allowed_for_product(product: Optional[str]) -> bool:
    """True iff a broker GTT-OCO may be placed for this product. CARRIED products
    (CNC / MTF / NRML) → True. MIS (intraday) → False (its protection is the
    tick-driver arm/stop + the intraday square-off; a surviving GTT would orphan).
    Any unknown value normalises to CNC → allowed (a carried default is the safe
    fallback; the reconciler's sweep is the belt-and-suspenders backstop)."""
    return _norm_product(product) in _CARRIED_PRODUCTS


def _gtt_stop_buffer() -> float:
    """Read FALCON_GTT_STOP_BUFFER from env at call time (so tests can patch
    os.environ without re-importing)."""
    raw = os.environ.get("FALCON_GTT_STOP_BUFFER", "")
    try:
        val = float(raw)
        if val <= 0 or val > 0.10:
            raise ValueError(f"out of range: {val}")
        return val
    except (ValueError, TypeError):
        return _GTT_STOP_LIMIT_BUFFER_DEFAULT


def compute_levels(entry_price: float, stop_pct: float,
                   target_pct: float,
                   direction: str = "long"
                   ) -> Tuple[float, float, float, float]:
    """Return (stop_trigger, stop_limit, target_trigger, target_limit) rounded
    to tick-friendly 2dp.

    LONG (default, UNCHANGED — byte-for-byte):
      stop_trigger   = entry * (1 - stop_pct)     (below entry; a SELL stop)
      stop_limit     = stop_trigger * (1 - buffer)  (below trigger, fills a gap-down)
      target_trigger = entry * (1 + target_pct)   (above entry; a SELL limit)
      target_limit   = target_trigger

    SHORT (FUTURES): everything INVERTS — the position profits when price falls,
    so the STOP is ABOVE entry (a BUY-to-cover stop) and the TARGET is BELOW:
      stop_trigger   = entry * (1 + stop_pct)     (above entry; a BUY stop)
      stop_limit     = stop_trigger * (1 + buffer)  (ABOVE trigger, fills a gap-up)
      target_trigger = entry * (1 - target_pct)   (below entry; a BUY limit)
      target_limit   = target_trigger

    Legacy callers that unpacked only two values still work because they only
    see (stop_trigger, target_trigger) in positions [0] and [2].
    """
    buf = _gtt_stop_buffer()
    e = float(entry_price)
    if str(direction).lower() == "short":
        # Stop ABOVE, target BELOW; both are BUY-to-cover legs.
        stop_trig = round(e * (1.0 + float(stop_pct)), 2)
        stop_lim  = round(stop_trig * (1.0 + buf), 2)   # above trigger, fills gap-up
        tgt_trig  = round(e * (1.0 - float(target_pct)), 2)
        tgt_lim   = tgt_trig
        return stop_trig, stop_lim, tgt_trig, tgt_lim
    # LONG (unchanged).
    stop_trig  = round(e * (1.0 - float(stop_pct)), 2)
    stop_lim   = round(stop_trig * (1.0 - buf), 2)
    tgt_trig   = round(e * (1.0 + float(target_pct)), 2)
    tgt_lim    = tgt_trig          # target limit equals trigger (no buffer needed)
    return stop_trig, stop_lim, tgt_trig, tgt_lim


class GTTManager:
    def __init__(self, session_id: str, config, brokers: Dict[str, Any],
                 registry):
        self.session_id = session_id
        self.config = config
        self.brokers = brokers          # {broker_profile: BrokerClient}
        self.registry = registry        # PositionRegistry

    def _effective_product(self, prof_id: Optional[str]) -> str:
        """The order_product for a position's broker_profile, or the session-level
        order_product when the profile can't be matched. Used by the GUARD G1 GTT
        gate. Never raises (defaults to the session product, then CNC)."""
        try:
            for bp in (getattr(self.config, "broker_profiles", None) or []):
                if str(getattr(bp, "profile_id", "")) == str(prof_id):
                    p = getattr(bp, "order_product", None)
                    if p:
                        return str(p)
        except Exception:  # pragma: no cover - defensive
            pass
        return str(getattr(self.config, "order_product", "CNC") or "CNC")

    # ── Place one position's GTT-OCO ──────────────────────────────────────────
    def place_for_position(self, pos: Dict[str, Any]) -> Dict[str, Any]:
        """Compute levels + place a GTT-OCO for one open position, then persist
        gtt_id + levels. Paper / unsupported broker → records levels only
        (gtt_id stays None). Best-effort — never raises."""
        symbol = pos["symbol"]
        prof_id = pos.get("broker_profile")
        qty = int(pos.get("qty") or 0)
        entry = float(pos.get("avg_price") or 0.0)
        direction = pos.get("direction") or "long"
        if qty <= 0 or entry <= 0:
            return {"symbol": symbol, "status": "SKIPPED_NO_QTY_OR_PRICE"}

        # GUARD G1 (mode F5): the effective product for THIS leg. Prefer the
        # position's broker_profile order_product (the product it was actually
        # opened with); fall back to the session order_product. This is resolved
        # with the SAME _norm_product the reconciler buckets on (EQ→CNC), so the
        # GTT gate and the invariant agree on what an "MIS leg" is.
        eff_product = self._effective_product(prof_id)
        if not _gtt_allowed_for_product(eff_product):
            # MIS (intraday) — NEVER place a broker GTT. Record levels only so the
            # UI still shows the intended stop/target; the tick-driver arm/stop +
            # the intraday square-off are the real protection.
            log.info("GTT SUPPRESSED (intraday leg) for %s/%s product=%s — "
                     "tick-stop + square-off cover it (no orphan GTT)",
                     self.session_id, symbol, _norm_product(eff_product))
            stop_trig, _sl, tgt_trig, _tl = compute_levels(
                entry, self.config.per_position_stop_pct,
                self.config.per_position_target_pct, direction=direction)
            self.registry.set_gtt(symbol, None, gtt_stop=stop_trig,
                                  gtt_target=tgt_trig, broker_profile=prof_id)
            return {"symbol": symbol, "status": "SUPPRESSED_INTRADAY",
                    "gtt_id": None, "stop": stop_trig, "target": tgt_trig}

        # FUTURES long/short: for a short the STOP is ABOVE entry (buy-stop) and
        # the TARGET is BELOW (buy-limit). compute_levels inverts on direction.
        stop_trig, stop_lim, tgt_trig, tgt_lim = compute_levels(
            entry, self.config.per_position_stop_pct,
            self.config.per_position_target_pct, direction=direction)

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
            # EXCHANGE-CONSISTENCY (F&O): the GTT must be placed on the SAME
            # segment the contract trades. Prefer the exchange persisted on the
            # position row; but never let a missing/legacy NULL default a FUT/OPT
            # contract to NSE (Kite would reject the OCO). Derive NFO from the
            # instrument_type as a hard fallback for F&O instruments.
            _itype = str(pos.get("instrument_type") or "EQ").upper()
            _exch = pos.get("exchange")
            if not _exch:
                _exch = "NFO" if _itype in ("FUT", "OPT", "CE", "PE") else "NSE"
            try:
                gtt_id = broker.place_gtt_oco(
                    symbol=symbol, qty=qty,
                    stop_price=stop_trig, stop_limit_price=stop_lim,
                    target_price=tgt_trig,
                    last_price=last, product=product,
                    exchange=_exch,
                    direction=direction)
            except Exception as e:  # best-effort — never block on the backup
                log.error("place_gtt_oco raised for %s: %s", symbol, e)
                gtt_id = None

        # gtt_stop / gtt_target stored as the TRIGGER prices (the levels the UI
        # and the reconciler care about — the limit offset is a broker mechanic).
        self.registry.set_gtt(symbol, gtt_id, gtt_stop=stop_trig, gtt_target=tgt_trig,
                              broker_profile=prof_id)
        status = "PLACED" if gtt_id else "RECORDED_ONLY"
        log.info("GTT %s for %s/%s stop_trig=%.2f stop_lim=%.2f "
                 "target=%.2f gtt_id=%s",
                 status, self.session_id, symbol,
                 stop_trig, stop_lim, tgt_trig, gtt_id)
        return {"symbol": symbol, "status": status, "gtt_id": gtt_id,
                "stop": stop_trig, "stop_limit": stop_lim, "target": tgt_trig}

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

    # ── GUARD G1 sweep: cancel any stray GTT on an INTRADAY (MIS) leg ─────────
    def sweep_intraday_gtts(self) -> List[Dict[str, Any]]:
        """Belt-and-suspenders (mode F5): cancel any GTT that somehow exists on an
        OPEN intraday/MIS position. G1's place-gate should prevent MIS GTTs from
        ever being created, but a legacy row (opened before this guard) or a
        cross-session backfill could still carry one — a GTT that outlives the MIS
        square-off is an orphan-short risk. Called on the MIS square-off / exit
        path (defensive; the full flatten's cancel_session_gtts_async is the other
        layer). Best-effort — a cancel failure is logged, never raised."""
        out: List[Dict[str, Any]] = []
        for pos in self.registry.get_open_positions():
            gtt_id = pos.get("gtt_id")
            if not gtt_id:
                continue
            if _gtt_allowed_for_product(self._effective_product(
                    pos.get("broker_profile"))):
                continue  # carried product — its GTT is legitimate, leave it
            prof_id = pos.get("broker_profile")
            broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            try:
                broker.cancel_gtt(gtt_id)
                # Clear the id so the reconciler doesn't chase a cancelled GTT.
                try:
                    self.registry.set_gtt(pos["symbol"], None, broker_profile=prof_id)
                except Exception as _clr:  # pragma: no cover - defensive
                    log.debug("sweep_intraday_gtts: gtt_id clear failed %s: %s",
                              pos["symbol"], _clr)
                log.warning("GUARD G1 SWEEP: cancelled STRAY intraday GTT %s for "
                            "%s/%s (MIS leg must not carry a GTT)",
                            gtt_id, self.session_id, pos["symbol"])
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "SWEPT_INTRADAY_GTT"})
            except Exception as e:  # never block the exit on a cancel
                log.warning("sweep_intraday_gtts: cancel_gtt failed for %s (%s): %s",
                            pos["symbol"], gtt_id, e)
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "SWEEP_CANCEL_FAILED", "error": str(e)})
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
                await asyncio.wait_for(
                    asyncio.to_thread(t["broker"].cancel_gtt, t["gtt_id"]),
                    timeout=8.0,
                )
                return {"symbol": t["symbol"], "gtt_id": t["gtt_id"],
                        "status": "CANCELLED"}
            except asyncio.TimeoutError:
                log.warning("cancel_gtt timed out for %s (%s) — skipping",
                            t["symbol"], t["gtt_id"])
                return {"symbol": t["symbol"], "gtt_id": t["gtt_id"],
                        "status": "CANCEL_TIMEOUT"}
            except Exception as e:  # never block the flatten on a GTT cancel
                log.warning("cancel_gtt failed for %s (%s): %s",
                            t["symbol"], t["gtt_id"], e)
                return {"symbol": t["symbol"], "gtt_id": t["gtt_id"],
                        "status": "CANCEL_FAILED", "error": str(e)}

        return list(await asyncio.gather(*[_one(t) for t in targets]))

    # ── Reconcile GTT fills (a position closed externally by a fired GTT) ──────
    async def reconcile_gtt_fills(self) -> List[Dict[str, Any]]:
        """Detect positions whose broker-held GTT FIRED (position closed at the
        broker outside our software) and mark the row CLOSED (close_reason='GTT')
        ONLY when the underlying SELL order is confirmed COMPLETE.

        Detection (live only): for each OPEN position with a gtt_id, ask the
        broker for the GTT's state via get_gtt, then call _gtt_execution_result
        to determine the confirmed fill status.

        Key safety rule: 'triggered' means Kite placed a sell ORDER, NOT that
        the fill happened. If the limit sell is still PENDING (e.g. price gapped
        past the limit), we skip the tick (pending) rather than marking CLOSED
        prematurely. We only close on a confirmed COMPLETE fill.

        Paper / brokers returning None for get_gtt → no-op (nothing to reconcile).
        After marking rows CLOSED here, the caller (session.tick) recomputes
        gross_return on the REMAINING positions."""
        out: List[Dict[str, Any]] = []
        closed_any = False
        for pos in self.registry.get_open_positions():
            gtt_id = pos.get("gtt_id")
            if not gtt_id:
                continue
            prof_id = pos.get("broker_profile")
            broker = self.brokers.get(prof_id) or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            try:
                state = await asyncio.to_thread(broker.get_gtt, gtt_id)
            except Exception as e:  # pragma: no cover - defensive
                log.debug("get_gtt failed for %s (%s): %s",
                          pos["symbol"], gtt_id, e)
                continue
            result = await self._resolve_gtt_result(broker, gtt_id, state)
            if result is None:
                # GTT still active — nothing to do this tick.
                continue
            if result["status"] == "pending":
                # GTT triggered (Kite placed the order) but fill not confirmed yet.
                # Skip this tick — never close prematurely.
                log.debug("GTT triggered but fill PENDING for %s/%s — skipping",
                          self.session_id, pos["symbol"])
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "GTT_PENDING"})
                continue
            if result["status"] == "cancelled":
                # GTT was cancelled/deleted/expired WITHOUT firing. Position is
                # still open and now UNPROTECTED — log a warning but do not close.
                # Clear gtt_id so this message doesn't repeat every tick.
                log.warning(
                    "GTT CANCELLED without fill for %s/%s (gtt_id=%s) — "
                    "position unprotected, manual review required",
                    self.session_id, pos["symbol"], gtt_id)
                try:
                    self.registry.set_gtt(pos["symbol"], None,
                                          broker_profile=prof_id)
                except Exception as _gtt_clr_e:
                    log.debug("gtt_id clear failed for %s: %s",
                              pos["symbol"], _gtt_clr_e)
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "GTT_CANCELLED_UNPROTECTED"})
                continue
            if result["status"] == "complete":
                # Fill confirmed — use the ACTUAL fill price, not the trigger.
                # RECONCILIATION Phase 1: record the fired GTT order-id as the
                # exit_order_id so the close is attributable by order-id.
                exit_price = result.get("exit_price")
                self.registry.mark_closed(pos["symbol"], "GTT",
                                          exit_price=exit_price,
                                          broker_profile=prof_id,
                                          exit_order_id=result.get("order_id"))
                closed_any = True
                out.append({"symbol": pos["symbol"], "gtt_id": gtt_id,
                            "status": "CLOSED_GTT",
                            "exit_price": exit_price,
                            "filled_qty": result.get("filled_qty"),
                            "exit_order_id": result.get("order_id")})
                log.warning("GTT FIRED + FILLED for %s/%s @ %.2f "
                            "(order_id=%s) — marked CLOSED",
                            self.session_id, pos["symbol"],
                            exit_price if exit_price else 0.0,
                            result.get("order_id"))
        # RECONCILIATION Phase 4: after a GTT close, RE-FREEZE this session's
        # invested_basis over the REMAINING open rows so the kill/trail
        # denominator reflects reality (a closed position must leave the basis).
        if closed_any:
            try:
                self._refreeze_invested_basis()
            except Exception as e:  # pragma: no cover - never block the tick
                log.warning("refreeze_invested_basis after GTT close failed "
                            "for %s: %s", self.session_id, e)
        return out

    def _refreeze_invested_basis(self) -> None:
        """Re-freeze the session's invested_basis over the CURRENT open rows via
        PortfolioMonitor.refreeze_invested_basis (product-aware). Best-effort —
        imported locally to avoid a module-level cycle. total_allocated_capital
        is read from the registry (the monitor needs it for the fund basis)."""
        from .monitor import PortfolioMonitor
        cap = float(getattr(self.registry, "total_allocated_capital", 0.0) or 0.0)
        if cap <= 0:
            # PortfolioMonitor requires a positive fund; nothing sane to refreeze.
            return
        PortfolioMonitor(self.session_id, cap).refreeze_invested_basis()

    async def _resolve_gtt_result(self, broker: Any, gtt_id: Any,
                                  state: Optional[Any]
                                  ) -> Optional[Dict[str, Any]]:
        """RECONCILIATION Phase 4: resolve a GTT's confirmed execution status,
        using the FIRED ORDER as positive evidence when the GTT is 'triggered'.

        Kite marks a fired GTT status='triggered' and PLACES an order, but the
        GTT object alone often can't confirm the fill (this left ACUTAAS OPEN on
        2026-07-07). So for a triggered GTT we ask the broker for the actual fill
        via broker.get_gtt_fill(gtt_id) — which resolves the fired order-id and
        calls get_order_status on THAT order:

          COMPLETE + filled_qty>0 → {"status":"complete","exit_price":avg,
                                     "filled_qty":q,"order_id":oid} → the caller
                                     marks CLOSED at the REAL fill with the id.
          any other order status / unresolved → {"status":"pending"} → NEVER
                                     close early; retry next tick.

        NON-triggered states (active / cancelled / deleted / expired / ambiguous)
        are resolved by the pure _gtt_execution_result(state) parse — unchanged
        (a cancelled-without-fill GTT stays "cancelled", an active GTT stays
        None). get_gtt_fill returns None for those, so we fall through to the
        object parse.

        get_gtt_fill defaults to None on the base broker (paper / stub / mocks
        without the override) → this method is byte-for-byte the old
        _gtt_execution_result(state) for them."""
        # Positive fill evidence for a triggered GTT (live only; None otherwise).
        try:
            fill = await asyncio.to_thread(broker.get_gtt_fill, gtt_id)
        except Exception as e:  # pragma: no cover - defensive; never raise
            log.debug("get_gtt_fill failed for %s: %s", gtt_id, e)
            fill = None
        if fill is not None:
            status = str(fill.get("status") or "").upper()
            filled = int(fill.get("filled_quantity") or 0)
            if status == "COMPLETE" and filled > 0:
                avg = fill.get("average_price")
                return {
                    "status": "complete",
                    "exit_price": float(avg) if avg else None,
                    "filled_qty": filled,
                    "order_id": fill.get("order_id"),
                    "close_reason": "GTT",
                }
            # Triggered but the order is not a confirmed COMPLETE fill yet
            # (OPEN / PENDING / partial / rejected-in-flight) — conservative:
            # pending. NEVER close on anything but a positive complete fill.
            log.debug("GTT %s triggered, fired order status=%s filled=%d — "
                      "pending (not closing)", gtt_id, status, filled)
            return {"status": "pending"}
        # No positive fill evidence (not triggered, or paper/stub broker) — fall
        # back to the pure GTT-object parse (active/cancelled/ambiguous).
        return self._gtt_execution_result(state)

    @staticmethod
    def _gtt_execution_result(state: Optional[Any]) -> Optional[Dict[str, Any]]:
        """Inspect a Kite GTT state dict and return the confirmed execution status.

        Returns:
          None                              — GTT still active, no action needed.
          {"status": "pending"}             — GTT triggered, sell order placed but
                                             fill not yet confirmed (OPEN/PENDING).
                                             Caller MUST NOT close the position.
          {"status": "complete",
           "exit_price": float,
           "filled_qty": int}              — Confirmed COMPLETE fill. Caller may
                                             mark position CLOSED with exit_price.
          {"status": "cancelled"}           — GTT was cancelled/deleted/expired
                                             without firing. Position stays OPEN.

        Conservative rule: if the response is ambiguous (missing orders field,
        malformed orders, unknown status), treat as "pending" (never close on
        missing data). A None state (paper/unsupported) returns None (no action).
        """
        if state is None:
            return None

        # Normalise: accept both dict and object-with-.status attribute.
        if isinstance(state, dict):
            status_raw = state.get("status")
        else:
            status_raw = getattr(state, "status", None)

        if status_raw is None:
            # No status at all — can't determine state.
            return None

        status = str(status_raw).lower()

        if status == "active":
            return None  # still armed, nothing to do

        if status in ("cancelled", "deleted", "expired"):
            # GTT removed without triggering. Position is still open + unprotected.
            return {"status": "cancelled"}

        if status == "triggered":
            # Kite placed the sell ORDER — now check if the underlying fill
            # is actually COMPLETE (the order may still be PENDING on limit).
            orders: Optional[List] = None
            if isinstance(state, dict):
                orders = state.get("orders")
            else:
                orders = getattr(state, "orders", None)

            if not orders:
                # Triggered but no orders field — conservative: treat as pending.
                log.debug("GTT triggered but no orders field in response — "
                          "treating as pending (conservative)")
                return {"status": "pending"}

            # Find the CLOSING leg that is COMPLETE. In an OCO there are two legs;
            # only one fires (stop or target). Both legs share the SAME closing
            # side: SELL for a LONG position (default), BUY-to-cover for a SHORT
            # FUTURE. We accept EITHER a COMPLETE SELL or a COMPLETE BUY so a
            # fired short-future GTT is recognised — otherwise the position would
            # stay OPEN in our DB while closed at the broker, and the monitor
            # would keep trying to exit an already-flat position (the naked-order
            # / EXIT_FAILED loop). A GTT only ever holds closing legs, so matching
            # both sides cannot mistake an entry for an exit here.
            for leg in orders:
                if isinstance(leg, dict):
                    tx = str(leg.get("transaction_type") or "").upper()
                    leg_status = str(leg.get("status") or "").upper()
                    avg_price = leg.get("average_price") or leg.get("avg_price")
                    qty = leg.get("quantity") or leg.get("qty") or leg.get("filled_qty")
                else:
                    tx = str(getattr(leg, "transaction_type", "") or "").upper()
                    leg_status = str(getattr(leg, "status", "") or "").upper()
                    avg_price = (getattr(leg, "average_price", None)
                                 or getattr(leg, "avg_price", None))
                    qty = (getattr(leg, "quantity", None)
                           or getattr(leg, "qty", None)
                           or getattr(leg, "filled_qty", None))

                if tx in ("SELL", "BUY") and leg_status == "COMPLETE":
                    return {
                        "status": "complete",
                        "exit_price": float(avg_price) if avg_price is not None else None,
                        "filled_qty": int(qty) if qty is not None else None,
                        "close_reason": "GTT",
                    }

            # Triggered + orders present but no COMPLETE closing leg yet → pending.
            return {"status": "pending"}

        # Unknown status string — conservative: treat as pending (don't close).
        log.debug("GTT unknown status %r — treating as pending (conservative)",
                  status_raw)
        return {"status": "pending"}

    @staticmethod
    def _gtt_fired(state: Optional[Any]) -> bool:
        """LEGACY helper — retained for any external callers that reference it.

        Prefer _gtt_execution_result() for new code. This implementation now
        delegates to _gtt_execution_result and returns True only on a confirmed
        complete fill or a cancelled state (i.e., not still active and not
        pending). Callers that used _gtt_fired to mark CLOSED should switch to
        _gtt_execution_result to get the confirmed exit_price.
        """
        result = GTTManager._gtt_execution_result(state)
        if result is None:
            return False  # still active
        return result["status"] in ("complete", "cancelled")
