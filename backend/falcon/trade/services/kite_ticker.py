"""Phase 3 — Kite WebSocket Ticker.

Maintains a real-time LTP cache for every Falcon-tracked symbol via the
KiteConnect KiteTicker WebSocket SDK. The position monitor reads from this
cache (via `get_ltp()`) instead of waiting for the next 60s holdings refresh,
which was the trail decision's bottleneck — moves now ratchet on the next
monitor poll using a price ≤2 seconds old instead of up to 60s old.

Architecture:
  - Singleton `_TickerState` holds the live KiteTicker connection + LTP cache.
  - `start()` is called once from main.py lifespan after the access token loads.
  - `refresh_subscriptions(kite)` is called by the monitor each poll — diffs the
    held symbols against the current subscription set and adds/removes tokens.
  - `get_ltp(symbol)` returns the latest tick's LTP (or None if not subscribed
    or never received a tick yet).

Failure mode: if the WebSocket can't connect or drops mid-session, callers
(`get_ltp`) return None and the monitor transparently falls back to the
existing kite.holdings() / kite.quote() path. No change in correctness — just
a latency increase.

The ticker writes nothing to the DB directly. The monitor's `upsert_state` is
the single writer of `falcon_position_state.last_seen_price`; we just feed it
fresher numbers via `get_ltp()`.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

log = logging.getLogger("kanida.falcon.trade.ticker")
IST = timezone(timedelta(hours=5, minutes=30))


class _TickerState:
    """Singleton holder. Don't instantiate elsewhere — use module-level helpers."""
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.kt = None                      # KiteTicker instance
        self.connected = False
        self.subscribed_tokens: Set[int] = set()
        # token -> {ltp, ts (datetime IST), symbol}
        self.tick_cache: Dict[int, Dict[str, Any]] = {}
        # symbol -> token (for fast lookup from get_ltp)
        self.sym_to_token: Dict[str, int] = {}
        self.token_to_sym: Dict[int, str] = {}
        self.last_error: Optional[str] = None
        self.last_tick_at: Optional[datetime] = None
        self.started_at: Optional[datetime] = None
        self.tick_count: int = 0


_state = _TickerState()


# ─── Tick callbacks (run in KiteTicker background thread) ────────────────────

def _on_ticks(ws, ticks: List[Dict[str, Any]]) -> None:
    now = datetime.now(IST)
    with _state.lock:
        for t in ticks:
            tok = int(t.get("instrument_token") or 0)
            if not tok:
                continue
            ltp = float(t.get("last_price") or 0)
            if ltp <= 0:
                continue
            sym = _state.token_to_sym.get(tok)
            _state.tick_cache[tok] = {"ltp": ltp, "ts": now, "symbol": sym}
            _state.tick_count += 1
        _state.last_tick_at = now


def _on_connect(ws, response) -> None:
    log.info("KiteTicker connected: %s", response)
    with _state.lock:
        _state.connected = True
        _state.last_error = None
        # Re-subscribe everything we know about (handles reconnect)
        if _state.subscribed_tokens:
            tokens = list(_state.subscribed_tokens)
            try:
                ws.subscribe(tokens)
                ws.set_mode(ws.MODE_LTP, tokens)
                log.info("KiteTicker resubscribed %d tokens", len(tokens))
            except Exception as e:
                log.warning("KiteTicker resubscribe failed: %s", e)


def _on_close(ws, code, reason) -> None:
    log.warning("KiteTicker closed: code=%s reason=%s", code, reason)
    with _state.lock:
        _state.connected = False
        _state.last_error = f"closed: {code} {reason}"


def _on_error(ws, code, reason) -> None:
    log.warning("KiteTicker error: code=%s reason=%s", code, reason)
    with _state.lock:
        _state.last_error = f"error: {code} {reason}"


def _on_reconnect(ws, attempts_count) -> None:
    log.info("KiteTicker reconnecting (attempt %d)", attempts_count)


def _on_noreconnect(ws) -> None:
    log.error("KiteTicker gave up reconnecting")
    with _state.lock:
        _state.connected = False
        _state.last_error = "noreconnect"


# ─── Public API ──────────────────────────────────────────────────────────────

def start(force: bool = False) -> bool:
    """Connect KiteTicker. If `force=True`, tears down any existing connection
    and re-creates with fresh credentials (use this after a token refresh,
    since KiteTicker doesn't expose a way to swap access_token mid-session).
    Idempotent without force when already connected.

    Returns True on connect-attempt-issued, False on fatal config error.
    The ticker reconnects automatically on transient disconnects.
    """
    # Already alive and not forcing? No-op.
    with _state.lock:
        already = _state.kt is not None and _state.connected
        had_error = bool(_state.last_error)
    if already and not force:
        return True

    # Tear down the stale instance before re-creating. Either we're forcing,
    # OR the existing one died (last_error set). Either way, drop it cleanly.
    if force or had_error:
        stop()

    try:
        from kiteconnect import KiteTicker
        from services.kite_auth import _get_credentials, get_access_token
        api_key, _ = _get_credentials()
        access_token = get_access_token()
    except Exception as e:
        log.warning("KiteTicker not started — auth not ready: %s", e)
        return False

    kt = KiteTicker(api_key, access_token)
    kt.on_ticks       = _on_ticks
    kt.on_connect     = _on_connect
    kt.on_close       = _on_close
    kt.on_error       = _on_error
    kt.on_reconnect   = _on_reconnect
    kt.on_noreconnect = _on_noreconnect

    with _state.lock:
        _state.kt = kt
        _state.started_at = datetime.now(IST)
        _state.last_error = None
        # Subscriptions get re-issued by on_connect via subscribed_tokens; the
        # next monitor poll will repopulate it from current Falcon-tracked syms.
        _state.subscribed_tokens = set()
        _state.tick_cache = {}

    # KiteTicker.connect(threaded=True) spawns its own thread; non-blocking.
    try:
        kt.connect(threaded=True)
        log.info("KiteTicker.connect(threaded=True) issued (force=%s)", force)
        return True
    except Exception as e:
        log.exception("KiteTicker connect failed: %s", e)
        with _state.lock:
            _state.last_error = str(e)
        return False


def stop() -> None:
    with _state.lock:
        kt = _state.kt
        _state.kt = None
        _state.connected = False
    if kt is not None:
        try:
            kt.close()
        except Exception:
            pass


def is_connected() -> bool:
    with _state.lock:
        return bool(_state.connected)


def refresh_subscriptions(kite) -> Dict[str, Any]:
    """Compare currently-tracked Falcon symbols vs subscribed; subscribe new,
    unsubscribe removed. Called from position_monitor each poll. Soft-fail."""
    if not is_connected():
        return {"status": "not_connected"}

    # Resolve token for every Falcon-tracked symbol via the MTF instrument cache.
    from . import mtf_eligibility  # noqa: WPS433
    from ...db import falcon_conn

    with falcon_conn() as con:
        rows = con.execute(
            "SELECT symbol FROM falcon_position_state WHERE qty > 0"
        ).fetchall()
    target_symbols = [r["symbol"] for r in rows]

    target_tokens: Set[int] = set()
    for sym in target_symbols:
        tok = mtf_eligibility.get_instrument_token(kite, sym)
        if tok and tok > 0:
            target_tokens.add(int(tok))
            with _state.lock:
                _state.sym_to_token[sym] = int(tok)
                _state.token_to_sym[int(tok)] = sym

    with _state.lock:
        already = set(_state.subscribed_tokens)
        kt = _state.kt

    to_add = list(target_tokens - already)
    to_remove = list(already - target_tokens)

    if kt is None:
        return {"status": "kt_none"}

    if to_add:
        try:
            kt.subscribe(to_add)
            kt.set_mode(kt.MODE_LTP, to_add)
            log.info("KiteTicker subscribed %d tokens", len(to_add))
        except Exception as e:
            log.warning("KiteTicker.subscribe failed: %s", e)
    if to_remove:
        try:
            kt.unsubscribe(to_remove)
            log.info("KiteTicker unsubscribed %d tokens", len(to_remove))
            with _state.lock:
                for tok in to_remove:
                    _state.tick_cache.pop(tok, None)
        except Exception as e:
            log.warning("KiteTicker.unsubscribe failed: %s", e)

    with _state.lock:
        _state.subscribed_tokens = target_tokens

    return {
        "status":     "ok",
        "subscribed": len(target_tokens),
        "added":      len(to_add),
        "removed":    len(to_remove),
    }


def get_ltp(symbol: str, max_age_sec: int = 30) -> Optional[float]:
    """Return latest LTP for symbol if cached and fresh.
    `max_age_sec` guards against stale ticks during low-activity periods. The
    monitor falls back to kite.holdings()'s last_price when this returns None.
    """
    with _state.lock:
        tok = _state.sym_to_token.get(symbol)
        if not tok:
            return None
        entry = _state.tick_cache.get(tok)
        if not entry:
            return None
        age = (datetime.now(IST) - entry["ts"]).total_seconds()
        if age > max_age_sec:
            return None
        return float(entry["ltp"])


def status() -> Dict[str, Any]:
    """Diagnostic snapshot for /api/falcon/trade/ticker endpoint."""
    with _state.lock:
        return {
            "connected":           _state.connected,
            "started_at":          _state.started_at.isoformat() if _state.started_at else None,
            "last_tick_at":        _state.last_tick_at.isoformat() if _state.last_tick_at else None,
            "tick_count":          _state.tick_count,
            "subscribed_count":    len(_state.subscribed_tokens),
            "cached_ltp_count":    len(_state.tick_cache),
            "last_error":          _state.last_error,
        }
