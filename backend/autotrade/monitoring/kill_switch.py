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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from falcon.db import falcon_conn
from .. import alerts, exit_gate

log = logging.getLogger("kanida.autotrade.kill_switch")
IST = timezone(timedelta(hours=5, minutes=30))


class KillSwitchExecutor:
    def __init__(self, session_id: str, config, brokers: Dict[str, Any],
                 registry):
        self.session_id = session_id
        self.config = config
        self.brokers = brokers          # {broker_profile: BrokerClient}
        self.registry = registry        # PositionRegistry

    # ── Threshold check (called every tick) ───────────────────────────────────
    def check_threshold(self, gross_return: float) -> Optional[str]:
        if not self.config.kill_switch_enabled:
            return None
        d = self.config.kill_switch_direction
        pct = self.config.kill_switch_pct
        if d in ("profit", "both") and gross_return >= pct:
            return f"PROFIT_TARGET gross_return={gross_return:.4f}"
        if d in ("loss", "both") and gross_return <= -abs(pct):
            return f"LOSS_LIMIT gross_return={gross_return:.4f}"
        return None

    # ── Fire (the critical sequence) ──────────────────────────────────────────
    async def fire(self, trigger_reason: str,
                   gross_return: Optional[float] = None) -> Dict[str, Any]:
        log.critical("KILL SWITCH FIRED [%s]: %s", self.session_id, trigger_reason)
        self._set_session_status("KILLING", kill_reason=trigger_reason)

        # STEP 1 — cancel all pending orders across all brokers (parallel).
        cancel_tasks = []
        for prof_id, broker in self.brokers.items():
            try:
                pending = await broker.get_pending_orders()
            except Exception as e:
                log.error("get_pending_orders failed on %s: %s", prof_id, e)
                pending = []
            for o in pending:
                oid = o.get("order_id") if isinstance(o, dict) else getattr(o, "id", None)
                if oid:
                    cancel_tasks.append(broker.cancel_order(oid))
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

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
            # Single exit gate: claim first. If another mechanism already owns
            # this exit (e.g. day-bound fired the same second), skip — no
            # duplicate order.
            if not exit_gate.claim_exit(symbol, "KILL_SWITCH"):
                exit_meta.append({"symbol": symbol, "claimed": False})
                continue
            qty = int(pos.get("qty") or 0)   # recompute open qty (spec rule)
            itype = pos.get("instrument_type") or "EQ"
            exit_coros.append(broker.place_market_exit(symbol, qty, itype))
            exit_meta.append({"symbol": symbol, "claimed": True, "qty": qty})

        results = await asyncio.gather(*exit_coros, return_exceptions=True)

        # STEP 3 — handle failures.
        n_ok, n_failed = 0, 0
        result_iter = iter(results)
        details: List[Dict[str, Any]] = []
        for meta in exit_meta:
            if not meta["claimed"]:
                details.append({**meta, "status": "BLOCKED"})
                continue
            res = next(result_iter)
            if isinstance(res, Exception):
                n_failed += 1
                alerts.send_urgent(
                    f"MANUAL EXIT REQUIRED: {meta['symbol']} ({self.session_id})")
                self._mark_exit_failed(meta["symbol"], str(res))
                details.append({**meta, "status": "EXIT_FAILED", "error": str(res)})
            elif getattr(res, "status", None) == "FAILED":
                n_failed += 1
                alerts.send_urgent(
                    f"MANUAL EXIT REQUIRED: {meta['symbol']} ({self.session_id})")
                self._mark_exit_failed(meta["symbol"], res.error or "exit failed")
                details.append({**meta, "status": "EXIT_FAILED", "error": res.error})
            else:
                n_ok += 1
                self.registry.mark_closed(meta["symbol"], "KILL_SWITCH")
                details.append({**meta, "status": getattr(res, "status", "PLACED"),
                                "broker_order_id": getattr(res, "broker_order_id", None)})

        # STEP 4 — close session + log.
        self._set_session_status("CLOSED")
        self._log_fire(trigger_reason, gross_return, len(positions),
                       n_ok, n_failed, details)
        summary = {"session_id": self.session_id, "trigger_reason": trigger_reason,
                   "n_positions": len(positions), "n_exited_ok": n_ok,
                   "n_exit_failed": n_failed, "details": details}
        log.critical("KILL SWITCH COMPLETE: %s", summary)
        return summary

    # ── DB helpers ────────────────────────────────────────────────────────────
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

    def _mark_exit_failed(self, symbol: str, error: str) -> None:
        with falcon_conn() as con:
            con.execute(
                "UPDATE falcon_position_state SET last_event_kind='EXIT_FAILED', "
                "last_event_at=?, last_event_kind=? WHERE symbol=?",
                (datetime.now(IST).isoformat(), "EXIT_FAILED", symbol))
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
