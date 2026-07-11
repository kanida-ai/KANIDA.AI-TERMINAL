"""KillSwitchExecutor — instant multi-broker flatten on threshold breach.

Highest priority in the system. fire() sequence is CRITICAL (spec 7.3):
  1. Cancel ALL pending orders across ALL brokers — in parallel.
  2. Flatten ALL open positions across ALL brokers — in parallel
     (asyncio.gather, return_exceptions=True so one broker failure can't block
     the others). Every exit is claimed through the exit_gate first, so the
     kill switch and the existing day-bound / per-position exits never
     double-exit the same position.
  3. Failed exits → urgent alert + mark EXIT_FAILED for a human.
  4. Mark session CLOSED + log the fire.

DISABLED BY DEFAULT: check_threshold returns None unless config.kill_switch_
enabled is True. Real exits additionally require the broker to be in live mode
(dry_run off + FALCON_AUTOTRADE_ENABLED) — in paper mode fire() still runs the
full parallel sequence but every broker returns DRY_RUN (no real orders).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn
from .. import alerts, exit_gate

log = logging.getLogger("kanida.autotrade.kill_switch")
IST = timezone(timedelta(hours=5, minutes=30))


class KillSwitchExecutor:
    def __init__(self, session_id: str, config, brokers: Dict[str, Any],
                 registry, gtt_manager=None):
        self.session_id = session_id
        self.config = config
        self.brokers = brokers          # {broker_profile: BrokerClient}
        self.registry = registry        # PositionRegistry
        self.gtt_manager = gtt_manager  # GTTManager | None (FEATURE 3)

    # ── Threshold check (called every tick) ───────────────────────────────────
    def check_threshold(self, gross_return: float) -> Optional[str]:
        # `gross_return` is the INVESTED-basis gross (÷ frozen invested_basis) —
        # the callers (session.tick / ws_driver / manual kill) pass that value.
        # kill_switch_pct is a validated FRACTION (0.012 = 1.2%).
        #
        # ASYMMETRIC (FEATURE B): the PROFIT side fires at target_pct and the LOSS
        # side at stop_pct when those overrides are set; each falls back to the
        # symmetric kill_switch_pct when None (so None/None is byte-for-byte the
        # old behaviour). kill_switch_direction still gates which side(s) are live.
        if not self.config.kill_switch_enabled:
            return None
        d = self.config.kill_switch_direction
        pct = self.config.kill_switch_pct
        target = getattr(self.config, "kill_switch_target_pct", None) or pct
        stop = getattr(self.config, "kill_switch_stop_pct", None) or pct
        if d in ("profit", "both") and gross_return >= target:
            return f"PROFIT_TARGET gross_return={gross_return:.4f}"
        if d in ("loss", "both") and gross_return <= -abs(stop):
            return f"LOSS_LIMIT gross_return={gross_return:.4f}"
        return None

    # ── Fire (the critical sequence) ──────────────────────────────────────────
    async def fire(self, trigger_reason: str,
                   gross_return: Optional[float] = None,
                   close_reason: str = "KILL_SWITCH") -> Dict[str, Any]:
        """Flatten the basket. `close_reason` is the tag written to each closed
        position's close_reason column (and used to claim the exit gate) — it
        defaults to "KILL_SWITCH" (the portfolio-kill-switch path, UNCHANGED).
        The intraday-basket trail engine passes its trail reason
        (TRAIL_EXIT / FLOOR_EXIT / STOP / SQUARE_OFF) through here so the per-day
        report shows WHY the basket exited. The flatten sequence is identical."""
        log.critical("KILL SWITCH FIRED [%s]: %s", self.session_id, trigger_reason)
        # SPEED PASS: time trigger → all-flat for exit_latency_ms observability.
        _exit_t0 = time.monotonic()
        self._set_session_status("KILLING", kill_reason=trigger_reason)

        # STEP 1 — cancel THIS SESSION's own pending orders across all brokers.
        # OWNERSHIP FILTER (Fix 5, 2026-07-11 audit): only cancel order-ids Falcon
        # RECORDED for this session (entry_order_id / exit_order_id / gtt_id on
        # autotrade_positions). A kill previously cancelled EVERY pending order on
        # the account — which would wipe the operator's OWN manual resting limit
        # orders. We never cancel an order-id we don't own; a foreign/unrecognised
        # pending order is logged and SKIPPED. (Broker-tag precision is a later
        # cluster; the recorded-id filter is the precise interim.)
        owned_ids = self._falcon_owned_order_ids()
        cancel_tasks = []
        for prof_id, broker in self.brokers.items():
            try:
                pending = await broker.get_pending_orders()
            except Exception as e:
                log.error("get_pending_orders failed on %s: %s", prof_id, e)
                pending = []
            for o in pending:
                oid = o.get("order_id") if isinstance(o, dict) else getattr(o, "id", None)
                if not oid:
                    continue
                if str(oid) not in owned_ids:
                    log.warning("kill %s: SKIP cancel of NON-Falcon pending order "
                                "%s on %s (not owned by this session — leaving the "
                                "operator's order intact)", self.session_id, oid,
                                prof_id)
                    continue
                cancel_tasks.append(broker.cancel_order(oid))
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

        # STEP 1b — COORDINATION (FEATURE 3): cancel each position's broker-held
        # GTT-OCO BEFORE the market exits so no orphan GTT remains to re-fire on
        # a symbol we just flattened. Best-effort — a GTT-cancel failure must
        # NOT block the flatten (logged + continue inside cancel_session_gtts).
        gtt_cancels: List[Dict[str, Any]] = []
        if self.gtt_manager is not None:
            try:
                # SPEED PASS: parallel cancel sweep (still BEFORE the market exits
                # so no orphan GTT can re-fire). Falls back to the sequential sweep
                # if the async variant isn't available.
                cancel_async = getattr(
                    self.gtt_manager, "cancel_session_gtts_async", None)
                if cancel_async is not None:
                    gtt_cancels = await cancel_async()
                else:
                    gtt_cancels = self.gtt_manager.cancel_session_gtts()
            except Exception as e:  # never block the flatten on GTT cancels
                log.warning("kill switch: GTT cancel sweep failed for %s: %s",
                            self.session_id, e)

        # STEP 2 — flatten all open positions across all brokers (parallel).
        positions = self.registry.get_open_positions()
        exit_coros = []
        exit_meta: List[Dict[str, Any]] = []
        for pos in positions:
            symbol = pos["symbol"]
            broker = self.brokers.get(pos.get("broker_profile")) \
                or next(iter(self.brokers.values()), None)
            if broker is None:
                continue
            # Single exit gate (session-scoped on autotrade_positions): claim
            # first. If another mechanism already owns this exit (e.g. day-bound
            # fired the same second), skip — no duplicate order.
            if not exit_gate.claim_exit_session(self.session_id, symbol, close_reason):
                exit_meta.append({"symbol": symbol, "claimed": False})
                continue
            qty = int(pos.get("qty") or 0)   # recompute open qty (spec rule)
            itype = pos.get("instrument_type") or "EQ"
            prof_id = pos.get("broker_profile")
            # FUTURES long/short: exit the CLOSING side. long→SELL (unchanged),
            # short→BUY-to-cover. Threaded from the position's stored direction.
            direction = pos.get("direction") or "long"
            kite_product = getattr(self.config, "order_product", None)
            # PRE-EXIT RECONCILIATION GUARD (real-money safety): if the broker
            # already shows this leg flat (operator closed it, or a broker SL/GTT
            # fired), skip the market exit — a blind order would go NAKED. Returns
            # None in paper / when unknown → we proceed with the normal exit
            # (paper unchanged). Best-effort: a probe error never blocks the kill.
            probe_raised = False
            try:
                net_qty = broker.get_net_position_qty(symbol, itype)
            except Exception as _net_e:
                # FAIL-SAFE (Fix B1, 2026-07-10 BRIGADE double-cover). The pre-exit
                # position read RAISED (broker connection/timeout — e.g.
                # ConnectionResetError 10054). We CANNOT confirm the live position,
                # so a market exit here would be BLIND: a short's buy-to-cover would
                # DOUBLE into a naked long. NEVER place an exit without a successful
                # position read — skip this leg (do NOT place), release the gate, and
                # leave it OPEN so a later kill/tick retries once the broker is
                # reachable. Paper / not-live returns None WITHOUT raising →
                # probe_raised stays False → unchanged.
                log.error("kill: net-position probe RAISED %s/%s: %s — NOT placing "
                          "exit (no blind order); leg left OPEN for retry",
                          self.session_id, symbol, _net_e)
                net_qty = None
                probe_raised = True
            if probe_raised:
                exit_gate.release_exit_session(self.session_id, symbol)
                exit_meta.append({"symbol": symbol, "claimed": False,
                                  "probe_failed": True})
                continue
            # SESSION-SCOPED flat decision (C1): subtract OTHER sessions' still-held
            # qty so session A never treats session B's shares as flat / sells into
            # B's lot. our_held==0 ⟺ THIS session's shares are gone (not the account).
            from .registry import our_held_at_broker as _our_held
            our_held = _our_held(self.session_id, symbol, itype, qty, net_qty)
            if net_qty is not None and our_held == 0:
                log.warning("kill: %s/%s already flat at broker — reconciling, "
                            "no exit order", self.session_id, symbol)
                # POSITIVE-EVIDENCE RECONCILE (2026-07-07 CEMPRO circuit incident).
                # The leg was closed by ANOTHER order (a fired GTT / manual / RMS).
                # Resolve its close at the REAL fill from the broker orderbook via
                # the framework's order-id-driven primitive — NEVER fabricate a
                # 0/mark price (a 0 exit books a phantom ~-100% realised loss).
                #   * confirmed real fill (gtt/exit order COMPLETE) → CLOSE at it.
                #   * else a POSITIVE mark exists → RECONCILED_FLAT at the mark
                #     (pre-existing 2026-07-02 behaviour; never a 0 price).
                #   * else (no evidence AND no positive mark) → EXIT_FAILED so the
                #     reconciler/human resolves it; do NOT book CLOSED@0.
                from .position_reconciler import _confirmed_close
                try:
                    ev = await asyncio.to_thread(_confirmed_close, pos, broker)
                except Exception as _ce:  # pragma: no cover - defensive
                    log.debug("kill: _confirmed_close raised %s: %s", symbol, _ce)
                    ev = None
                ltp_val = pos.get("ltp")
                if ev is not None:
                    self.registry.mark_closed(
                        symbol, f"{close_reason}_RECONCILED_FLAT",
                        exit_price=ev.get("exit_price"),
                        broker_profile=prof_id,
                        exit_order_id=ev.get("exit_order_id"))
                    exit_gate.release_exit_session(self.session_id, symbol)
                    exit_meta.append({"symbol": symbol, "claimed": False,
                                      "reconciled_flat": True,
                                      "exit_price": ev.get("exit_price")})
                elif ltp_val and float(ltp_val) > 0:
                    self.registry.mark_closed(
                        symbol, f"{close_reason}_RECONCILED_FLAT",
                        broker_profile=prof_id)
                    exit_gate.release_exit_session(self.session_id, symbol)
                    exit_meta.append({"symbol": symbol, "claimed": False,
                                      "reconciled_flat": True})
                else:
                    # mark_exit_failed releases the exit gate itself.
                    self.registry.mark_exit_failed(
                        symbol,
                        f"{close_reason}: broker flat, no attributable exit fill",
                        broker_profile=prof_id)
                    exit_meta.append({"symbol": symbol, "claimed": False,
                                      "reconciled_flat_unattributed": True})
                continue
            # STANDING INVARIANT (Fix 3, 2026-07-11): NEVER place an exit larger than
            # the qty THIS session still holds at the broker — regardless of WHY it
            # shrank (a partial RMS/GTT fill, an external partial close). The clamp
            # is UNCONDITIONAL: a partial external fill can shrink our_held without
            # any GTT-cancel timeout, so gating the clamp on a timeout would let the
            # full raw qty oversell (qty-our_held) into a reverse/naked position.
            # our_held is None in paper (get_net_position_qty None) → no clamp,
            # byte-for-byte unchanged. our_held >= qty → no-op.
            if our_held is not None and 0 < our_held < qty:
                log.warning("kill: %s/%s CLAMP exit qty %d→%d (session-scoped "
                            "live-held; broker shrank under us)",
                            self.session_id, symbol, qty, int(our_held))
                qty = int(our_held)
            exit_coros.append(broker.place_market_exit(
                symbol, qty, itype, kite_product=kite_product,
                direction=direction, exec_cfg=self.config))
            exit_meta.append({"symbol": symbol, "claimed": True, "qty": qty,
                              "broker_profile": prof_id, "direction": direction,
                              "instrument_type": itype, "kite_product": kite_product})

        results = await asyncio.gather(*exit_coros, return_exceptions=True)

        # STEP 3 — confirm fills (not mark_closed on PLACED).
        # For each placed order we poll until COMPLETE / REJECTED / TIMEOUT
        # rather than optimistically marking CLOSED on PLACED. DRY_RUN orders
        # are confirmed immediately (no polling) so paper mode is unchanged.
        # R2 — the fill CONFIRMATIONS run CONCURRENTLY (asyncio.gather), not
        # serially: each confirm_exit can block up to 60s, so N legs used to take
        # up to N×60s. One bad/slow leg can no longer stall the rest; per-leg
        # EXIT_FAILED handling + fire_guard/exit_gate scoping are preserved.
        from .exit_poller import confirm_exit, cancel_and_retry_exit

        async def _confirm_live_leg(meta, order_id):
            """Confirm ONE live placed leg (+ partial/timeout retry). Returns
            (ok: bool, detail: dict). Isolated so legs confirm in parallel."""
            prof_id = meta.get("broker_profile")
            broker_for_pos = (self.brokers.get(prof_id)
                              or next(iter(self.brokers.values()), None))
            confirm_result = await confirm_exit(
                session_id=self.session_id, symbol=meta["symbol"],
                order_id=order_id, qty=meta["qty"], broker=broker_for_pos,
                registry=self.registry, close_reason=close_reason,
                max_wait_sec=60, poll_interval_sec=5.0,
                broker_profile=prof_id)
            confirm_status = confirm_result.get("status", "UNKNOWN")

            if confirm_status == "COMPLETE":
                return True, {**meta, "status": "COMPLETE",
                              "broker_order_id": order_id,
                              "exit_price": confirm_result.get("exit_price")}

            if confirm_status == "PARTIAL":
                remaining = confirm_result.get("remaining_qty", 0)
                log.warning("kill switch: PARTIAL fill for %s/%s "
                            "(filled=%d remaining=%d) — retrying remainder",
                            self.session_id, meta["symbol"],
                            confirm_result.get("filled_qty", 0), remaining)
                if remaining > 0:
                    retry_result = await cancel_and_retry_exit(
                        session_id=self.session_id, symbol=meta["symbol"],
                        order_id=order_id, qty=remaining, broker=broker_for_pos,
                        registry=self.registry, close_reason=close_reason,
                        max_retries=3, direction=meta.get("direction", "long"),
                        instrument_type=meta.get("instrument_type", "EQ"),
                        kite_product=meta.get("kite_product"),
                        broker_profile=prof_id)
                    if retry_result.get("status") == "COMPLETE":
                        return True, {**meta, "status": "COMPLETE_AFTER_PARTIAL",
                                      "broker_order_id": order_id}
                    alerts.send_urgent(
                        f"MANUAL EXIT REQUIRED (partial): "
                        f"{meta['symbol']} ({self.session_id})")
                    return False, {**meta, "status": "PARTIAL_FAILED",
                                   "remaining_qty": remaining}
                return True, {**meta, "status": "COMPLETE_AFTER_PARTIAL"}

            if confirm_status == "TIMEOUT":
                log.error("kill switch: TIMEOUT confirming exit for %s/%s "
                          "— cancelling + retrying", self.session_id,
                          meta["symbol"])
                retry_result = await cancel_and_retry_exit(
                    session_id=self.session_id, symbol=meta["symbol"],
                    order_id=order_id, qty=meta["qty"], broker=broker_for_pos,
                    registry=self.registry, close_reason=close_reason,
                    max_retries=3, direction=meta.get("direction", "long"),
                    instrument_type=meta.get("instrument_type", "EQ"),
                    kite_product=meta.get("kite_product"),
                    broker_profile=prof_id)
                if retry_result.get("status") == "COMPLETE":
                    return True, {**meta, "status": "COMPLETE_AFTER_TIMEOUT",
                                  "broker_order_id": order_id}
                alerts.send_urgent(
                    f"MANUAL EXIT REQUIRED (timeout): "
                    f"{meta['symbol']} ({self.session_id})")
                self.registry.mark_exit_failed(
                    meta["symbol"], "timeout + retry exhausted",
                    broker_profile=meta.get("broker_profile"))
                return False, {**meta, "status": "EXIT_FAILED_TIMEOUT"}

            # REJECTED / CANCELLED — exit_gate already released by confirm_exit.
            alerts.send_urgent(
                f"MANUAL EXIT REQUIRED ({confirm_status}): "
                f"{meta['symbol']} ({self.session_id})")
            return False, {**meta, "status": f"EXIT_FAILED_{confirm_status}"}

        n_ok, n_failed = 0, 0
        result_iter = iter(results)
        details: List[Dict[str, Any]] = []
        confirm_jobs: List[Any] = []       # (meta, coro) for the LIVE placed legs
        for meta in exit_meta:
            if not meta["claimed"]:
                details.append({**meta, "status": "BLOCKED"})
                continue
            res = next(result_iter)

            # --- placement-level failure (exception or FAILED status) ----------
            if isinstance(res, Exception):
                n_failed += 1
                alerts.send_urgent(
                    f"MANUAL EXIT REQUIRED: {meta['symbol']} ({self.session_id})")
                self.registry.mark_exit_failed(
                    meta["symbol"], str(res),
                    broker_profile=meta.get("broker_profile"))
                details.append({**meta, "status": "EXIT_FAILED", "error": str(res)})
                continue

            if getattr(res, "status", None) == "FAILED":
                n_failed += 1
                alerts.send_urgent(
                    f"MANUAL EXIT REQUIRED: {meta['symbol']} ({self.session_id})")
                self.registry.mark_exit_failed(
                    meta["symbol"], res.error or "exit failed",
                    broker_profile=meta.get("broker_profile"))
                details.append({**meta, "status": "EXIT_FAILED",
                                "error": getattr(res, "error", None)})
                continue

            # --- order placed (PLACED or DRY_RUN) — now confirm the fill -------
            order_id = getattr(res, "broker_order_id", None)
            is_dry = (order_id is None or
                      str(order_id).upper() in ("DRY_RUN", "NONE", ""))

            if is_dry:
                # Paper mode: mark_closed immediately (no polling needed).
                self.registry.mark_closed(
                    meta["symbol"], close_reason,
                    broker_profile=meta.get("broker_profile"))
                n_ok += 1
                details.append({**meta, "status": "DRY_RUN",
                                "broker_order_id": order_id})
                continue

            # Live placed leg → defer to the concurrent confirm phase.
            confirm_jobs.append((meta, _confirm_live_leg(meta, order_id)))

        # R2: confirm all live legs CONCURRENTLY. return_exceptions=True so one
        # bad leg can't block (or crash) the others.
        if confirm_jobs:
            outcomes = await asyncio.gather(
                *[c for _m, c in confirm_jobs], return_exceptions=True)
            for (meta, _coro), outcome in zip(confirm_jobs, outcomes):
                if isinstance(outcome, Exception):
                    n_failed += 1
                    alerts.send_urgent(
                        f"MANUAL EXIT REQUIRED: {meta['symbol']} "
                        f"({self.session_id})")
                    self.registry.mark_exit_failed(
                        meta["symbol"], str(outcome),
                        broker_profile=meta.get("broker_profile"))
                    details.append({**meta, "status": "EXIT_FAILED",
                                    "error": str(outcome)})
                    continue
                ok, detail = outcome
                n_ok += 1 if ok else 0
                n_failed += 0 if ok else 1
                details.append(detail)

        # STEP 4 — close session + log. Record exit_latency_ms (trigger → flat).
        # C2: only mark CLOSED (terminal) when EVERY leg confirmed flat. If any leg
        # is EXIT_FAILED / unconfirmed, set the NON-terminal KILLING_INCOMPLETE so
        # the tick driver keeps servicing the EXIT_FAILED retry sweep instead of
        # self-stopping and stranding the leg. tick() promotes it to CLOSED once
        # the last leg is flat. The all-success path stays byte-for-byte CLOSED.
        self._set_session_status("CLOSED" if n_failed == 0 else "KILLING_INCOMPLETE")
        exit_latency_ms = int((time.monotonic() - _exit_t0) * 1000)
        try:
            with falcon_conn() as con:
                con.execute(
                    "UPDATE autotrade_sessions SET exit_latency_ms=? "
                    "WHERE session_id=?", (exit_latency_ms, self.session_id))
                con.commit()
        except Exception as e:  # pragma: no cover - never block on observability
            log.debug("exit_latency record failed for %s: %s", self.session_id, e)
        log.critical("KILL SWITCH flatten complete in %dms [%s]",
                     exit_latency_ms, self.session_id)
        self._log_fire(trigger_reason, gross_return, len(positions),
                       n_ok, n_failed, details)
        summary = {"session_id": self.session_id, "trigger_reason": trigger_reason,
                   "n_positions": len(positions), "n_exited_ok": n_ok,
                   "n_exit_failed": n_failed, "details": details,
                   "gtt_cancelled": gtt_cancels}
        log.critical("KILL SWITCH COMPLETE: %s", summary)
        return summary

    # ── DB helpers ────────────────────────────────────────────────────────────
    def _falcon_owned_order_ids(self) -> set:
        """The set of broker order-ids Falcon RECORDED for THIS session — the entry
        fill, the exit fill, and the GTT id on every autotrade_positions row (any
        status). Used by the kill's STEP 1 so it cancels ONLY our own pending
        orders and never the operator's manual resting orders. Best-effort: on any
        DB error return the empty set (→ cancel NOTHING, the fail-safe side: a
        stranded Falcon order is a manual cleanup, cancelling a foreign order is
        real-money harm)."""
        ids: set = set()
        try:
            with falcon_conn() as con:
                rows = con.execute(
                    "SELECT entry_order_id, exit_order_id, gtt_id "
                    "FROM autotrade_positions WHERE session_id=?",
                    (self.session_id,),
                ).fetchall()
            for r in rows:
                for v in (r["entry_order_id"], r["exit_order_id"], r["gtt_id"]):
                    if v not in (None, ""):
                        ids.add(str(v))
        except Exception as e:  # pragma: no cover - defensive
            log.warning("kill %s: could not load owned order-ids (%s) — cancelling "
                        "NO pending orders (fail-safe)", self.session_id, e)
        return ids

    def _set_session_status(self, status: str, kill_reason: Optional[str] = None) -> None:
        with falcon_conn() as con:
            if kill_reason is not None:
                con.execute(
                    "UPDATE autotrade_sessions SET status=?, kill_reason=? WHERE session_id=?",
                    (status, kill_reason, self.session_id))
            else:
                closed_at = (datetime.now(IST).isoformat()
                             if status == "CLOSED" else None)
                con.execute(
                    "UPDATE autotrade_sessions SET status=?, "
                    "closed_at=COALESCE(?, closed_at) WHERE session_id=?",
                    (status, closed_at, self.session_id))
            con.commit()

    def _log_fire(self, trigger_reason, gross_return, n_positions,
                  n_ok, n_failed, details) -> None:
        mode = "live" if any(not b.dry_run for b in self.brokers.values()) else "paper"
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO autotrade_kill_switch_log
                   (session_id, fired_at, trigger_reason, gross_return,
                    n_positions, n_exited_ok, n_exit_failed, mode, detail_json)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (self.session_id, datetime.now(IST).isoformat(), trigger_reason,
                 gross_return, n_positions, n_ok, n_failed, mode,
                 json.dumps(details)),
            )
            con.commit()
