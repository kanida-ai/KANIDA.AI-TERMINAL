"""Per-session WebSocket-driven portfolio-exit driver (FEATURE 2).

Drives the kill-switch threshold check off KiteTicker LIVE ticks (sub-second), in
ADDITION to the existing 5s tick_driver heartbeat. On each relevant tick it:
  1. updates that symbol's ltp on the session's OPEN positions,
  2. recomputes the INVESTED-basis gross_return (÷ frozen invested_basis),
  3. if it crosses ±kill_switch_pct → fires the flatten — coordinated with the
     5s poll via the per-session fire guard so the two NEVER double-fire.

The 5s tick_driver remains as the fallback/heartbeat (it also snapshots + does
GTT reconcile). This driver is the LOW-LATENCY path.

DESIGN: rather than hook the global KiteTicker's _on_ticks callback (which is
owned by the Falcon swing monitor), this driver SUBSCRIBES the session's symbols
to the shared ticker (refresh_subscriptions is symbol-driven off
falcon_position_state, so we subscribe explicitly) and POLLS the ticker's LTP
cache on a sub-second interval. Reading a ~0–2s-old cached tick gives sub-second
reaction without contending for the callback. The poll interval defaults to
0.25s (4× per second).

SAFETY:
  * Paper sessions place NO real orders on fire (brokers built dry_run=True).
  * Idempotent: start_for_session is a no-op if already running for that session.
  * Self-stopping: exits as soon as session status != RUNNING.
  * Best-effort subscription: if the ticker isn't connected (e.g. tests, no
    Kite), the loop still runs and reads whatever the injected ltp_source gives.
  * Never raises out of the loop (defence in depth around live money).
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Any, Callable, Dict, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.ws_driver")

DEFAULT_POLL_SEC = 0.25

# Production leaves this on; tests flip it off so a background WS thread can't
# race assertions on the shared temp DB. The dedicated WS test re-enables it.
_AUTOSTART = True


def set_autostart(enabled: bool) -> None:
    global _AUTOSTART
    _AUTOSTART = bool(enabled)


def autostart_enabled() -> bool:
    if os.environ.get("FALCON_AUTOTRADE_WS_DISABLE") == "1":
        return False
    return _AUTOSTART


# session_id -> _WSDriver
_DRIVERS: Dict[str, "_WSDriver"] = {}
_LOCK = threading.Lock()


def _session_status(session_id: str) -> Optional[str]:
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    return row["status"] if row else None


def _default_ltp_source(symbol: str) -> Optional[float]:
    """Latest LTP from the shared KiteTicker WS cache (sub-second fresh)."""
    try:
        from falcon.trade.services.kite_ticker import get_ltp as ws_ltp
        return ws_ltp(symbol)
    except Exception:  # pragma: no cover - defensive
        return None


def _subscribe_session_symbols(symbols) -> None:
    """Best-effort subscribe the session's symbols on the shared KiteTicker so
    the LTP cache fills for them. No-op if the ticker isn't connected."""
    try:
        from falcon.trade.services import kite_ticker
        if not kite_ticker.is_connected():
            return
        from services.kite_auth import get_kite_client
        from falcon.trade.services import mtf_eligibility
        kite = get_kite_client(check=False)
        tokens = []
        for sym in symbols:
            tok = mtf_eligibility.get_instrument_token(kite, sym)
            if tok and tok > 0:
                tokens.append(int(tok))
                with kite_ticker._state.lock:
                    kite_ticker._state.sym_to_token[sym] = int(tok)
                    kite_ticker._state.token_to_sym[int(tok)] = sym
        if not tokens:
            return
        with kite_ticker._state.lock:
            kt = kite_ticker._state.kt
        if kt is not None:
            kt.subscribe(tokens)
            kt.set_mode(kt.MODE_LTP, tokens)
            with kite_ticker._state.lock:
                kite_ticker._state.subscribed_tokens |= set(tokens)
            log.info("ws_driver subscribed %d session tokens", len(tokens))
    except Exception as e:  # pragma: no cover - never block on subscribe
        log.debug("ws_driver subscribe failed: %s", e)


class _WSDriver:
    def __init__(self, session_id: str, poll_sec: float,
                 ltp_source: Optional[Callable[[str], Optional[float]]] = None):
        self.session_id = session_id
        self.poll_sec = poll_sec
        self.ltp_source = ltp_source or _default_ltp_source
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"autotrade-ws-{session_id[:8]}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        from ..session import TradingSession
        from . import fire_guard
        try:
            sess = TradingSession.load(self.session_id)
        except Exception as e:  # pragma: no cover - defensive
            log.error("ws_driver: failed to load session %s: %s",
                      self.session_id, e)
            self._deregister()
            return
        if sess is None:
            log.warning("ws_driver: session %s not found, exiting", self.session_id)
            self._deregister()
            return
        sess._build_brokers()

        # Best-effort subscribe the session's symbols to the shared ticker.
        try:
            syms = [p["symbol"] for p in sess.registry.get_open_positions()]
            _subscribe_session_symbols(syms)
        except Exception:  # pragma: no cover
            pass

        log.info("ws_driver started for %s (poll=%ss)",
                 self.session_id, self.poll_sec)
        try:
            while not self._stop.is_set():
                if _session_status(self.session_id) != "RUNNING":
                    break
                try:
                    self._tick_once(sess, fire_guard)
                except Exception as e:  # pragma: no cover - never kill the thread
                    log.exception("ws_driver tick error for %s: %s",
                                  self.session_id, e)
                self._stop.wait(self.poll_sec)
        finally:
            self._deregister()
            log.info("ws_driver stopped for %s", self.session_id)

    def _tick_once(self, sess, fire_guard) -> None:
        """One sub-second pass: pull fresh WS LTPs → recompute → maybe fire."""
        positions = sess.registry.get_open_positions()
        if not positions:
            return
        for pos in positions:
            ltp = self.ltp_source(pos["symbol"])
            if ltp is not None and ltp > 0:
                sess.registry.update_ltp(
                    pos["symbol"], float(ltp),
                    broker_profile=pos.get("broker_profile"))
        # KILL BASIS: check against the INVESTED-basis gross return (÷ frozen
        # invested_basis), matching the 5s poll path.
        gr = sess.monitor.compute_gross_return_invested()

        # INTRADAY BASKET: run the sub-second trail engine (arm/peak ratchet,
        # giveback/floor trigger, hard stop, square-off) — mirrors session.tick's
        # intraday branch. State changes are persisted so a restart resumes the
        # trail; on EXIT we reuse kill_switch.fire with the trail close_reason.
        if getattr(sess.config, "strategy", "portfolio_kill_switch") \
                == "intraday_basket":
            from . import trail_engine
            state = sess.monitor.load_trail_state()
            params = trail_engine.params_from_config(sess.config)
            decision = trail_engine.decide(gr, state, params)
            if decision.state_changed:
                sess.monitor.save_trail_state(decision.state)
            if decision.action != "EXIT":
                return
            with fire_guard.claim_fire(self.session_id) as won:
                if not won:
                    return  # the 5s poll (or a manual kill) already fired
                log.critical("ws_driver: intraday trail AUTO-fired (sub-second) "
                             "for %s (%s)", self.session_id, decision.reason)
                asyncio.run(sess.kill_switch.fire(
                    f"INTRADAY_BASKET {decision.reason} gross_return={gr:.4f}",
                    gross_return=gr, close_reason=decision.reason))
            return

        reason = (sess.kill_switch.check_threshold(gr)
                  if sess.kill_switch else None)
        if not reason:
            return
        with fire_guard.claim_fire(self.session_id) as won:
            if not won:
                return  # the 5s poll (or a manual kill) already fired
            log.critical("ws_driver: kill switch AUTO-fired (sub-second) for %s "
                         "(%s)", self.session_id, reason)
            asyncio.run(sess.kill_switch.fire(reason, gross_return=gr))

    def _deregister(self) -> None:
        with _LOCK:
            if _DRIVERS.get(self.session_id) is self:
                _DRIVERS.pop(self.session_id, None)


def start_for_session(
        session_id: str, poll_sec: float = DEFAULT_POLL_SEC,
        ltp_source: Optional[Callable[[str], Optional[float]]] = None) -> bool:
    """Start a WS driver for session_id. Idempotent — returns False if one is
    already running or autostart is disabled. Returns True if a new driver was
    started."""
    if not autostart_enabled():
        return False
    env_poll = os.environ.get("FALCON_AUTOTRADE_WS_POLL")
    if env_poll:
        try:
            poll_sec = float(env_poll)
        except ValueError:
            pass
    with _LOCK:
        existing = _DRIVERS.get(session_id)
        if existing is not None and existing.is_alive():
            return False
        drv = _WSDriver(session_id, poll_sec, ltp_source=ltp_source)
        _DRIVERS[session_id] = drv
    drv.start()
    return True


def stop_for_session(session_id: str) -> None:
    with _LOCK:
        drv = _DRIVERS.get(session_id)
    if drv is not None:
        drv.stop()


def is_running(session_id: str) -> bool:
    with _LOCK:
        drv = _DRIVERS.get(session_id)
    return bool(drv and drv.is_alive())
