"""Per-session WebSocket-driven portfolio-exit driver (FEATURE 2).

Drives the kill-switch threshold check off KiteTicker LIVE ticks (sub-second), in
ADDITION to the existing 5s tick_driver heartbeat. On each relevant tick it:
  1. updates that symbol's ltp on the session's OPEN positions,
  2. recomputes the INVESTED-basis gross_return (÷ frozen invested_basis),
  3. if it crosses ±kill_switch_pct → fires the flatten — coordinated with the
     5s poll via the per-session fire guard so the two NEVER double-fire.

The 5s tick_driver remains as the fallback/heartbeat (it also snapshots + does
GTT reconcile). This driver is the LOW-LATENCY path.

DESIGN (SPEED PASS — event-driven): this driver SUBSCRIBES the session's symbols
to the shared ticker and registers an ADDITIVE tick listener
(kite_ticker.add_tick_listener) so an incoming tick for a held symbol WAKES the
eval loop IMMEDIATELY — true sub-second reaction without waiting for a poll. The
listener only sets an Event (coalescing tick bursts into one wake); the actual
recompute + trail/kill decision still runs on the driver thread (so the
KiteTicker thread is never blocked and the Falcon monitor is never disturbed).
The poll remains as a BACKSTOP at a lowered default of 0.1s (override via
FALCON_AUTOTRADE_WS_POLL) in case the WS is briefly down. The legacy _on_ticks
default behaviour is UNCHANGED when no session has a listener registered.

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

DEFAULT_POLL_SEC = 0.1

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


# ── F&O instrument-token cache (NFO) ─────────────────────────────────────────
# The legacy mtf_eligibility cache pulls ONLY kite.instruments("NSE") (cash
# equity), so its get_instrument_token returns None for any FUT/OPT contract
# symbol (e.g. "INFY26JULFUT"). Without a token the WS never subscribes the
# contract → F&O positions get NO live ticks and the sub-second kill/trail path
# is blind to them (it silently falls back to the 5s REST poll). We resolve the
# NFO token ourselves here, NEVER modifying the legacy service.
_NFO_TOKENS: Dict[str, int] = {}
_NFO_FETCHED_AT: Optional[float] = None
_NFO_TTL_SEC = 6 * 3600  # F&O contracts are added/expire; refresh a few times/day.


def _nfo_token(kite, symbol: str) -> Optional[int]:
    """instrument_token for an NFO contract tradingsymbol, or None. Cached with a
    TTL; a refresh failure returns None (caller skips → poll-only for that
    symbol, never crashes)."""
    global _NFO_TOKENS, _NFO_FETCHED_AT
    import time as _time
    now = _time.monotonic()
    if _NFO_FETCHED_AT is None or (now - _NFO_FETCHED_AT) > _NFO_TTL_SEC:
        try:
            fresh: Dict[str, int] = {}
            for ins in kite.instruments("NFO"):
                ts = ins.get("tradingsymbol")
                tok = int(ins.get("instrument_token") or 0)
                if ts and tok > 0:
                    fresh[ts] = tok
            _NFO_TOKENS = fresh
            _NFO_FETCHED_AT = now
        except Exception as e:  # pragma: no cover - defensive
            log.debug("ws_driver: NFO instrument dump failed: %s", e)
            return _NFO_TOKENS.get(symbol)
    return _NFO_TOKENS.get(symbol)


def _resolve_token(kite, sym: str):
    """Resolve a WS instrument token for `sym`: try the legacy NSE-cash cache
    first (equity), then the NFO master (F&O contracts)."""
    from falcon.trade.services import mtf_eligibility
    tok = mtf_eligibility.get_instrument_token(kite, sym)
    if tok and tok > 0:
        return int(tok)
    # F&O contract symbols (…FUT / …<strike>CE|PE) live on NFO, not in the cash
    # cache. Only probe NFO when the symbol shape looks like a contract so we
    # don't pull the (large) NFO dump for a plain equity typo.
    su = sym.upper()
    looks_fno = su.endswith("FUT") or (
        (su.endswith("CE") or su.endswith("PE"))
        and len(su) >= 3 and su[-3].isdigit())
    if looks_fno:
        ntok = _nfo_token(kite, sym)
        if ntok and ntok > 0:
            return int(ntok)
    return None


def _subscribe_session_symbols(symbols) -> None:
    """Best-effort subscribe the session's symbols on the shared KiteTicker so
    the LTP cache fills for them. No-op if the ticker isn't connected. Resolves
    both cash-equity (NSE) and F&O contract (NFO) tokens."""
    try:
        from falcon.trade.services import kite_ticker
        if not kite_ticker.is_connected():
            return
        from services.kite_auth import get_kite_client
        kite = get_kite_client(check=False)
        tokens = []
        for sym in symbols:
            tok = _resolve_token(kite, sym)
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
        # EVENT-DRIVEN: set by the kite_ticker tick listener when a symbol this
        # session cares about ticks → the run loop wakes IMMEDIATELY (no waiting
        # for the next poll). Coalesces bursts (many ticks → one wake).
        self._wake = threading.Event()
        self._symbols: set = set()   # cached subscribed symbols (this session)
        self._listener = None        # registered tick listener (for cleanup)
        self._thread = threading.Thread(
            target=self._run, name=f"autotrade-ws-{session_id[:8]}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()  # unblock the loop so it exits promptly
        self._unregister_listener()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    # ── tick listener (event-driven trigger) ──────────────────────────────────
    def _on_tick(self, ticked_syms: set) -> None:
        """Called from the kite_ticker tick thread (off its lock). If any symbol
        this session holds just ticked, wake the eval loop immediately."""
        if self._stop.is_set():
            return
        if not self._symbols or (ticked_syms & self._symbols):
            self._wake.set()

    def _register_listener(self) -> None:
        try:
            from falcon.trade.services import kite_ticker
            self._listener = self._on_tick
            kite_ticker.add_tick_listener(self._listener)
        except Exception as e:  # pragma: no cover - degrade to poll-only
            log.debug("ws_driver: tick listener register failed for %s: %s",
                      self.session_id, e)
            self._listener = None

    def _unregister_listener(self) -> None:
        if self._listener is None:
            return
        try:
            from falcon.trade.services import kite_ticker
            kite_ticker.remove_tick_listener(self._listener)
        except Exception:  # pragma: no cover
            pass
        self._listener = None

    def _refresh_symbols(self, positions) -> None:
        """Cache the open-position symbol set + (re)subscribe them on the ticker.
        Called on entry/exit changes only (not every wake) — keeps the hot path
        free of redundant token lookups. AUTO-RECONNECT: re-subscribing each
        change also re-arms the subscription after a ticker reconnect."""
        syms = {p["symbol"] for p in positions}
        if syms != self._symbols:
            self._symbols = syms
            try:
                _subscribe_session_symbols(list(syms))
            except Exception:  # pragma: no cover
                pass

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

        # Cache + subscribe the session's symbols, and arm the event-driven
        # listener so a tick wakes us immediately (poll is the backstop).
        try:
            self._refresh_symbols(sess.registry.get_open_positions())
        except Exception:  # pragma: no cover
            pass
        self._register_listener()

        log.info("ws_driver started for %s (poll=%ss, event-driven)",
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
                # EVENT-DRIVEN wait: wake on the next tick OR after poll_sec
                # (backstop). Coalesce: clear the flag, then wait for the next.
                self._wake.clear()
                self._wake.wait(self.poll_sec)
        finally:
            self._unregister_listener()
            self._deregister()
            log.info("ws_driver stopped for %s", self.session_id)

    def _tick_once(self, sess, fire_guard) -> None:
        """One sub-second pass: pull fresh WS LTPs → recompute → maybe fire."""
        # LIVE CONFIG EDIT: this driver holds a long-lived `sess` (loaded once in
        # _run), so it must hot-reload any operator risk/exit edit BEFORE the
        # trail/kill decision below — mirrors session.tick(). Safe no-op when the
        # config_version is unchanged; never touches invested_basis / trail state.
        try:
            sess.maybe_reload_config()
        except Exception:  # pragma: no cover - never block the sub-second path
            pass
        positions = sess.registry.get_open_positions()
        if not positions:
            return
        # Keep the cached symbol set + subscription current (only acts on change).
        try:
            self._refresh_symbols(positions)
        except Exception:  # pragma: no cover
            pass
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
            # CAPITAL-BASIS TRAIL (2026-07-07): the intraday trail decides on the
            # ALLOCATED-CAPITAL gross return (÷ total_allocated_capital), NOT the
            # notional/invested basis `gr` used by the kill switch below. This
            # makes arm/floor/giveback/basket-stop "% of deployed capital",
            # leverage-correct (a 1x CNC basket is a no-op). Mirrors
            # session.py:_tick_intraday.
            gr_capital = sess.monitor.compute_gross_return()
            state = sess.monitor.load_trail_state()
            params = trail_engine.params_from_config(sess.config)
            decision = trail_engine.decide(gr_capital, state, params)
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
                    f"INTRADAY_BASKET {decision.reason} "
                    f"gross_return={gr_capital:.4f}",
                    gross_return=gr_capital, close_reason=decision.reason))
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
