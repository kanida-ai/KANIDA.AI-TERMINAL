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
        # token -> {ltp, ts (datetime IST), symbol}  (+ bid/ask when a FULL tick)
        self.tick_cache: Dict[int, Dict[str, Any]] = {}
        # symbol -> token (for fast lookup from get_ltp)
        self.sym_to_token: Dict[str, int] = {}
        self.token_to_sym: Dict[int, str] = {}
        self.last_error: Optional[str] = None
        self.last_tick_at: Optional[datetime] = None
        self.started_at: Optional[datetime] = None
        self.tick_count: int = 0
        # ── ADDITIVE (AutoTrade Phase-2, Falcon-neutral) ──────────────────────
        # Tokens explicitly upgraded to MODE_FULL (depth) by an AutoTrade
        # subscribe_full() caller. A token here is a SUPERSET of MODE_LTP — a FULL
        # tick still carries last_price, so Falcon's get_ltp is unaffected. Falcon
        # never populates this set; it starts empty and stays empty in the
        # Falcon-only process. Used to (a) re-apply FULL on reconnect and (b) gate
        # get_quote_ws (only trust bid/ask for a token we deliberately made FULL).
        self.full_tokens: Set[int] = set()
        # order_id -> {status, filled_quantity, average_price, tradingsymbol, ts}
        # Populated by the KiteTicker on_order_update postback (event-driven fill
        # confirmation for AutoTrade). Empty + untouched in the Falcon-only path.
        self.order_updates: Dict[str, Dict[str, Any]] = {}


_state = _TickerState()


# ─── Optional tick listeners (ADDITIVE, default-empty) ───────────────────────
# AutoTrade Sessions register a lightweight callback here to get EVENT-DRIVEN
# (sub-second) notification of incoming ticks, instead of polling the cache.
# DEFAULT BEHAVIOUR IS UNCHANGED: with no listeners registered (the production
# default until an AutoTrade session arms one), _on_ticks does exactly what it
# did before. Listeners are invoked OUTSIDE the state lock, each wrapped so a
# listener exception can never disturb the cache update or the Falcon monitor.
_listeners_lock = threading.Lock()
_tick_listeners: List[Any] = []  # list of callables: fn(symbols: set[str]) -> None


def add_tick_listener(fn) -> None:
    """Register a callback invoked (best-effort, off-lock) after each tick batch
    with the SET of symbols that just ticked. Idempotent per callable."""
    with _listeners_lock:
        if fn not in _tick_listeners:
            _tick_listeners.append(fn)


def remove_tick_listener(fn) -> None:
    with _listeners_lock:
        try:
            _tick_listeners.remove(fn)
        except ValueError:
            pass


# ─── Optional ORDER-UPDATE listeners (ADDITIVE, default-empty) ────────────────
# KiteConnect pushes an on_order_update POSTBACK over the SAME WebSocket whenever
# an order changes state (COMPLETE / REJECTED / CANCELLED / …). AutoTrade uses
# this for SUB-SECOND fill confirmation instead of polling get_order_status.
# DEFAULT BEHAVIOUR IS UNCHANGED: with no order listeners registered (the Falcon-
# only default), the postback just stores the update in order_updates and returns.
# Listeners are invoked OUTSIDE the state lock, each wrapped so a listener
# exception can never disturb the ticker or the Falcon monitor. Falcon never
# registers an order listener → this whole path is inert for it.
_order_listeners: List[Any] = []       # callables: fn(order_id, data) -> None
_ORDER_UPDATES_MAX = 2000              # bound the dict so a long session can't grow it


def add_order_listener(fn) -> None:
    """Register a callback invoked (best-effort, off-lock) with (order_id, data)
    after each order postback. Idempotent per callable."""
    with _listeners_lock:
        if fn not in _order_listeners:
            _order_listeners.append(fn)


def remove_order_listener(fn) -> None:
    with _listeners_lock:
        try:
            _order_listeners.remove(fn)
        except ValueError:
            pass


def get_order_update(order_id: str) -> Optional[Dict[str, Any]]:
    """Latest postback state for an order_id, or None if none received yet.
    Returns {status, filled_quantity, average_price, tradingsymbol, ts}."""
    if not order_id:
        return None
    with _state.lock:
        u = _state.order_updates.get(str(order_id))
        return dict(u) if u else None


# order_id -> threading.Event set when a TERMINAL update arrives (for await).
_order_events_lock = threading.Lock()
_order_events: Dict[str, threading.Event] = {}
_TERMINAL_ORDER_STATUSES = {"COMPLETE", "REJECTED", "CANCELLED"}


def _order_event(order_id: str) -> threading.Event:
    with _order_events_lock:
        ev = _order_events.get(order_id)
        if ev is None:
            ev = threading.Event()
            _order_events[order_id] = ev
        return ev


def wait_order_terminal(order_id: str, timeout: float) -> Optional[Dict[str, Any]]:
    """Block up to `timeout` seconds for a TERMINAL postback (COMPLETE/REJECTED/
    CANCELLED) for `order_id`, returning its update dict, else None.

    Thread-safe + race-free: an update that ALREADY arrived before this call is
    honoured (we check the cache first), and the Event is armed BEFORE the wait so
    a postback landing mid-wait wakes us. Runs on the caller's thread; the
    reconciler calls it via asyncio.to_thread so the event loop is never blocked.
    """
    if not order_id:
        return None
    oid = str(order_id)
    # Fast path: a terminal update already cached?
    existing = get_order_update(oid)
    if existing and str(existing.get("status", "")).upper() in _TERMINAL_ORDER_STATUSES:
        return existing
    ev = _order_event(oid)
    if ev.wait(timeout):
        return get_order_update(oid)
    return None


# ─── Tick callbacks (run in KiteTicker background thread) ────────────────────

def _on_ticks(ws, ticks: List[Dict[str, Any]]) -> None:
    now = datetime.now(IST)
    ticked_syms = set()
    with _state.lock:
        for t in ticks:
            tok = int(t.get("instrument_token") or 0)
            if not tok:
                continue
            ltp = float(t.get("last_price") or 0)
            if ltp <= 0:
                continue
            sym = _state.token_to_sym.get(tok)
            entry = {"ltp": ltp, "ts": now, "symbol": sym}
            # ── ADDITIVE: FULL-mode depth → also cache best bid/ask. An LTP-only
            # tick has NO "depth" key → this block is skipped and `entry` is
            # EXACTLY the old {ltp,ts,symbol} dict (Falcon path byte-for-byte
            # unchanged). A 0/absent level = "no order there" → left out, so the
            # AutoTrade pricer falls back to LTP for that side. ltp is NEVER
            # dropped or altered here.
            depth = t.get("depth")
            if depth:
                try:
                    buy = depth.get("buy") or []
                    sell = depth.get("sell") or []
                    bid = float(buy[0].get("price") or 0) if buy else 0.0
                    ask = float(sell[0].get("price") or 0) if sell else 0.0
                    if bid > 0:
                        entry["bid"] = bid
                    if ask > 0:
                        entry["ask"] = ask
                except Exception:  # pragma: no cover - never disturb ltp caching
                    pass
            _state.tick_cache[tok] = entry
            _state.tick_count += 1
            if sym:
                ticked_syms.add(sym)
        _state.last_tick_at = now
    # Notify optional listeners OUTSIDE the lock. No listeners → no-op (unchanged
    # default behaviour). Each is wrapped so it can never affect the Falcon path.
    if ticked_syms:
        with _listeners_lock:
            listeners = list(_tick_listeners)
        for fn in listeners:
            try:
                fn(ticked_syms)
            except Exception as e:  # pragma: no cover - never disturb ticking
                log.debug("tick listener error: %s", e)


def last_tick_at():
    """The IST datetime of the newest tick received (None if none yet). Used by
    AutoTrade status() to report last_tick_age_ms."""
    with _state.lock:
        return _state.last_tick_at


def _on_order_update(ws, data: Dict[str, Any]) -> None:
    """KiteTicker on_order_update postback (runs on the KiteTicker background
    thread). Store the update by order_id (bounded), signal any waiter on a
    TERMINAL state, then notify order listeners OFF-lock. NEVER raises — a bad
    postback can't disturb ticking or the Falcon monitor. Falcon registers no
    order listeners, so for it this only writes to order_updates (inert)."""
    try:
        oid = str(data.get("order_id") or "")
        if not oid:
            return
        status = str(data.get("status") or "").upper()
        rec = {
            "status": status,
            "filled_quantity": int(data.get("filled_quantity") or 0),
            "average_price": float(data.get("average_price") or 0.0),
            "tradingsymbol": data.get("tradingsymbol"),
            "ts": datetime.now(IST),
        }
    except Exception as e:  # pragma: no cover - never disturb the ticker
        log.debug("order update parse error: %s", e)
        return
    with _state.lock:
        _state.order_updates[oid] = rec
        # Trim oldest if we somehow exceed the cap (bounded memory).
        if len(_state.order_updates) > _ORDER_UPDATES_MAX:
            for k in list(_state.order_updates.keys())[
                    :len(_state.order_updates) - _ORDER_UPDATES_MAX]:
                _state.order_updates.pop(k, None)
    # Wake any await_order_terminal waiter on a terminal state.
    if status in _TERMINAL_ORDER_STATUSES:
        with _order_events_lock:
            ev = _order_events.get(oid)
        if ev is not None:
            ev.set()
    # Notify listeners OUTSIDE the lock. No listeners → no-op (unchanged default).
    with _listeners_lock:
        listeners = list(_order_listeners)
    for fn in listeners:
        try:
            fn(oid, rec)
        except Exception as e:  # pragma: no cover - never disturb the ticker
            log.debug("order listener error: %s", e)


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
        # ADDITIVE: re-apply MODE_FULL to any tokens AutoTrade upgraded so depth
        # (bid/ask) survives a reconnect. These are already re-subscribed as LTP
        # above (full_tokens ⊆ subscribed_tokens); this only upgrades their mode.
        # FULL is a SUPERSET of LTP → a Falcon-shared token upgraded here still
        # feeds last_price to get_ltp. Empty in the Falcon-only path → no-op.
        if _state.full_tokens:
            ftoks = list(_state.full_tokens)
            try:
                ws.set_mode(ws.MODE_FULL, ftoks)
                log.info("KiteTicker re-applied MODE_FULL to %d tokens", len(ftoks))
            except Exception as e:
                log.warning("KiteTicker re-apply FULL failed: %s", e)


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
    kt.on_ticks        = _on_ticks
    kt.on_connect      = _on_connect
    kt.on_close        = _on_close
    kt.on_error        = _on_error
    kt.on_reconnect    = _on_reconnect
    kt.on_noreconnect  = _on_noreconnect
    # ADDITIVE: order postbacks over the SAME socket → event-driven fill
    # confirmation for AutoTrade. Inert for Falcon (no order listeners).
    kt.on_order_update = _on_order_update

    with _state.lock:
        _state.kt = kt
        _state.started_at = datetime.now(IST)
        _state.last_error = None
        # Subscriptions get re-issued by on_connect via subscribed_tokens; the
        # next monitor poll will repopulate it from current Falcon-tracked syms.
        _state.subscribed_tokens = set()
        _state.tick_cache = {}
        # ADDITIVE: on a fresh (re)connect the AutoTrade FULL set + order postbacks
        # are re-primed by the caller (subscribe_full / prewarm) — clear stale
        # state so a token isn't wrongly treated as FULL after a force-restart.
        _state.full_tokens = set()
        _state.order_updates = {}

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


def subscribe_full(symbols: List[str]) -> int:
    """ADDITIVE (AutoTrade Phase-2): subscribe `symbols` and upgrade them to
    MODE_FULL so the tick_cache also carries bid/ask (depth). Best-effort,
    soft-fail, idempotent. Returns the count of tokens successfully upgraded.

    FALCON-SAFE by construction:
      * MODE_FULL is a SUPERSET of MODE_LTP — a full tick still carries
        last_price, so a token SHARED with Falcon (already MODE_LTP) is only
        UPGRADED; get_ltp keeps working identically.
      * We only ADD to subscribed_tokens / full_tokens — never unsubscribe,
        never downgrade a token to LTP, never touch Falcon's tokens beyond an
        upgrade. Falcon never calls this, so its tokens stay MODE_LTP unless it
        happens to hold the same symbol AutoTrade is trading (an upgrade, safe).
      * Reuses the SAME instrument-token resolution refresh_subscriptions uses
        (mtf_eligibility.get_instrument_token) — no new resolution logic.
    """
    if not symbols:
        return 0
    try:
        from . import mtf_eligibility  # noqa: WPS433
        from services.kite_auth import get_kite_client
    except Exception as e:  # pragma: no cover - defensive
        log.debug("subscribe_full import failed: %s", e)
        return 0
    if not is_connected():
        return 0
    try:
        kite = get_kite_client(check=False)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("subscribe_full: no kite client: %s", e)
        return 0

    tokens: List[int] = []
    for sym in symbols:
        try:
            tok = mtf_eligibility.get_instrument_token(kite, sym)
        except Exception:  # pragma: no cover - per-symbol
            tok = None
        if tok and tok > 0:
            tok = int(tok)
            tokens.append(tok)
            with _state.lock:
                _state.sym_to_token[sym] = tok
                _state.token_to_sym[tok] = sym
    if not tokens:
        return 0

    with _state.lock:
        kt = _state.kt
    if kt is None:
        return 0
    try:
        kt.subscribe(tokens)
        kt.set_mode(kt.MODE_FULL, tokens)
    except Exception as e:  # pragma: no cover - best-effort
        log.warning("subscribe_full failed: %s", e)
        return 0
    with _state.lock:
        _state.subscribed_tokens |= set(tokens)
        _state.full_tokens |= set(tokens)
    log.info("KiteTicker subscribe_full upgraded %d tokens to FULL", len(tokens))
    return len(tokens)


def get_quote_ws(symbol: str, max_age_sec: int = 10) -> Optional[dict]:
    """ADDITIVE: return {ltp, bid, ask} for `symbol` from the WS cache — but ONLY
    when the symbol's token is in full_tokens (deliberately upgraded to FULL) AND
    the cached tick is FRESH AND actually carries a bid AND ask. Otherwise None,
    so the caller falls back to a REST quote. Network-free; a superset of get_ltp.
    """
    with _state.lock:
        tok = _state.sym_to_token.get(symbol)
        if not tok or tok not in _state.full_tokens:
            return None
        entry = _state.tick_cache.get(tok)
        if not entry:
            return None
        bid = entry.get("bid")
        ask = entry.get("ask")
        ltp = entry.get("ltp")
        if not (bid and bid > 0 and ask and ask > 0):
            return None
        age = (datetime.now(IST) - entry["ts"]).total_seconds()
        if age > max_age_sec:
            return None
        return {"ltp": float(ltp) if ltp else None,
                "bid": float(bid), "ask": float(ask)}


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
