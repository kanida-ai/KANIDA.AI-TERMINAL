"""PHASE-2 kite_ticker additive changes — WS FULL depth + order postbacks.

*** FALCON-SAFETY IS THE #1 CONCERN ***: kite_ticker.py is SHARED with the live
Falcon swing monitor. Every change must be PURELY ADDITIVE and must NOT change
the existing LTP behaviour or get_ltp for an existing token. These tests assert:

  * get_ltp + the _on_ticks cache entry for an LTP-ONLY tick are byte-for-byte
    unchanged ({ltp, ts, symbol} only — no bid/ask keys).
  * a FULL tick (with depth) populates bid/ask AND still updates ltp.
  * subscribe_full adds full_tokens WITHOUT dropping existing subscriptions;
    reconnect (_on_connect) re-applies MODE_FULL.
  * get_quote_ws freshness / absence semantics.
  * order postback: _on_order_update stores + notifies; get_order_update;
    wait_order_terminal resolves COMPLETE / REJECTED; listeners fire.

All in-process, no real Kite, no network. The Falcon-only default (no listeners,
no full_tokens) is exercised implicitly by the LTP-only assertions.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest

from falcon.trade.services import kite_ticker as kt


@pytest.fixture(autouse=True)
def _reset_ticker_state():
    """Isolate each test: reset the singleton's cache/maps/listeners so tests
    don't leak state into each other or into any real ticker."""
    st = kt._state
    with st.lock:
        st.tick_cache = {}
        st.sym_to_token = {}
        st.token_to_sym = {}
        st.subscribed_tokens = set()
        st.full_tokens = set()
        st.order_updates = {}
        st.last_tick_at = None
        st.tick_count = 0
    with kt._listeners_lock:
        kt._tick_listeners.clear()
        kt._order_listeners.clear()
    with kt._order_events_lock:
        kt._order_events.clear()
    yield


# ── FALCON-SAFETY: LTP-only tick is byte-for-byte unchanged ──────────────────

def test_ltp_only_tick_cache_entry_is_unchanged_shape():
    """An LTP-only tick (no depth) must produce EXACTLY {ltp, ts, symbol} — the
    old shape. No bid/ask keys leak in. This is the Falcon get_ltp contract."""
    st = kt._state
    with st.lock:
        st.token_to_sym[111] = "INFY"
        st.sym_to_token["INFY"] = 111
    kt._on_ticks(None, [{"instrument_token": 111, "last_price": 100.5}])
    with st.lock:
        entry = st.tick_cache[111]
    assert set(entry.keys()) == {"ltp", "ts", "symbol"}
    assert entry["ltp"] == 100.5
    assert entry["symbol"] == "INFY"
    assert isinstance(entry["ts"], datetime)


def test_get_ltp_unchanged_for_ltp_only_tick():
    """get_ltp returns the LTP for a plain LTP tick exactly as before."""
    st = kt._state
    with st.lock:
        st.token_to_sym[222] = "TCS"
        st.sym_to_token["TCS"] = 222
    kt._on_ticks(None, [{"instrument_token": 222, "last_price": 3800.0}])
    assert kt.get_ltp("TCS") == 3800.0
    # Absent symbol → None (unchanged).
    assert kt.get_ltp("NOPE") is None


def test_ltp_zero_tick_skipped_unchanged():
    """A tick with ltp<=0 is skipped (unchanged guard)."""
    st = kt._state
    with st.lock:
        st.token_to_sym[333] = "X"
        st.sym_to_token["X"] = 333
    kt._on_ticks(None, [{"instrument_token": 333, "last_price": 0}])
    with st.lock:
        assert 333 not in st.tick_cache


# ── FULL tick populates bid/ask AND still updates ltp ────────────────────────

def test_full_tick_populates_bid_ask_and_ltp():
    st = kt._state
    with st.lock:
        st.token_to_sym[444] = "RELIANCE"
        st.sym_to_token["RELIANCE"] = 444
        st.full_tokens.add(444)
    tick = {
        "instrument_token": 444,
        "last_price": 2500.0,
        "depth": {
            "buy": [{"price": 2499.5, "quantity": 100}],
            "sell": [{"price": 2500.5, "quantity": 120}],
        },
    }
    kt._on_ticks(None, [tick])
    with st.lock:
        entry = st.tick_cache[444]
    # ltp still present + correct.
    assert entry["ltp"] == 2500.0
    # bid/ask populated from depth top.
    assert entry["bid"] == 2499.5
    assert entry["ask"] == 2500.5
    # get_ltp STILL works off a full tick (MODE_FULL ⊇ MODE_LTP).
    assert kt.get_ltp("RELIANCE") == 2500.0


def test_full_tick_zero_level_is_absent():
    """A 0 price at a depth level means 'no order there' → not cached as bid/ask,
    so the pricer uses the LTP fallback for that side."""
    st = kt._state
    with st.lock:
        st.token_to_sym[555] = "Y"
        st.sym_to_token["Y"] = 555
        st.full_tokens.add(555)
    tick = {"instrument_token": 555, "last_price": 50.0,
            "depth": {"buy": [{"price": 0}], "sell": [{"price": 50.1}]}}
    kt._on_ticks(None, [tick])
    with st.lock:
        entry = st.tick_cache[555]
    assert "bid" not in entry
    assert entry["ask"] == 50.1
    assert entry["ltp"] == 50.0


# ── get_quote_ws freshness / absence ─────────────────────────────────────────

def test_get_quote_ws_returns_book_when_full_fresh():
    st = kt._state
    with st.lock:
        st.token_to_sym[666] = "Z"
        st.sym_to_token["Z"] = 666
        st.full_tokens.add(666)
    kt._on_ticks(None, [{"instrument_token": 666, "last_price": 10.0,
                         "depth": {"buy": [{"price": 9.95}],
                                   "sell": [{"price": 10.05}]}}])
    q = kt.get_quote_ws("Z")
    assert q == {"ltp": 10.0, "bid": 9.95, "ask": 10.05}


def test_get_quote_ws_none_when_not_full_token():
    """A token that was only MODE_LTP (Falcon's default) → get_quote_ws None even
    if a tick exists (we never trust bid/ask for a non-FULL token)."""
    st = kt._state
    with st.lock:
        st.token_to_sym[777] = "L"
        st.sym_to_token["L"] = 777
        # NOT added to full_tokens.
    kt._on_ticks(None, [{"instrument_token": 777, "last_price": 5.0}])
    assert kt.get_quote_ws("L") is None


def test_get_quote_ws_none_when_missing_bid_or_ask():
    st = kt._state
    with st.lock:
        st.token_to_sym[888] = "M"
        st.sym_to_token["M"] = 888
        st.full_tokens.add(888)
    # Only an ask, no bid → None (both required).
    kt._on_ticks(None, [{"instrument_token": 888, "last_price": 5.0,
                         "depth": {"buy": [{"price": 0}],
                                   "sell": [{"price": 5.1}]}}])
    assert kt.get_quote_ws("M") is None


def test_get_quote_ws_none_when_stale():
    st = kt._state
    old = datetime.now(kt.IST) - timedelta(seconds=30)
    with st.lock:
        st.token_to_sym[999] = "S"
        st.sym_to_token["S"] = 999
        st.full_tokens.add(999)
        st.tick_cache[999] = {"ltp": 1.0, "bid": 0.9, "ask": 1.1,
                              "ts": old, "symbol": "S"}
    assert kt.get_quote_ws("S", max_age_sec=10) is None


# ── subscribe_full: adds full_tokens without dropping existing subs; reconnect
#    re-applies FULL ─────────────────────────────────────────────────────────

class _FakeWS:
    def __init__(self):
        self.MODE_LTP = "ltp"
        self.MODE_FULL = "full"
        self.subscribed = []
        self.modes = []       # (mode, tokens)

    def subscribe(self, tokens):
        self.subscribed.append(list(tokens))

    def set_mode(self, mode, tokens):
        self.modes.append((mode, list(tokens)))

    def unsubscribe(self, tokens):
        raise AssertionError("subscribe_full must NEVER unsubscribe")


def test_subscribe_full_adds_without_dropping_existing(monkeypatch):
    st = kt._state
    fake = _FakeWS()
    with st.lock:
        st.kt = fake
        st.connected = True
        # Pre-existing Falcon token (MODE_LTP) — must survive untouched.
        st.subscribed_tokens = {100}
        st.token_to_sym[100] = "FALCONSYM"
        st.sym_to_token["FALCONSYM"] = 100

    # Fake the token resolution used by subscribe_full.
    import falcon.trade.services.mtf_eligibility as mtf
    monkeypatch.setattr(mtf, "get_instrument_token",
                        lambda kite, sym: {"A": 200, "B": 300}.get(sym))
    monkeypatch.setattr("services.kite_auth.get_kite_client",
                        lambda check=False: object())

    n = kt.subscribe_full(["A", "B"])
    assert n == 2
    with st.lock:
        # Existing Falcon token still subscribed (never dropped).
        assert 100 in st.subscribed_tokens
        # New tokens added + marked FULL.
        assert {200, 300} <= st.subscribed_tokens
        assert st.full_tokens == {200, 300}
    # A FULL set_mode was issued for exactly the new tokens.
    full_modes = [toks for (m, toks) in fake.modes if m == "full"]
    assert full_modes and set(full_modes[-1]) == {200, 300}


def test_reconnect_reapplies_full_mode():
    """_on_connect re-subscribes LTP for all tokens AND re-applies MODE_FULL to
    full_tokens so depth survives a reconnect."""
    st = kt._state
    fake = _FakeWS()
    with st.lock:
        st.subscribed_tokens = {100, 200, 300}
        st.full_tokens = {200, 300}
    kt._on_connect(fake, {"ok": True})
    # LTP re-subscribe applied to all.
    ltp_modes = [toks for (m, toks) in fake.modes if m == "ltp"]
    assert ltp_modes and set(ltp_modes[0]) == {100, 200, 300}
    # FULL re-applied to the full set.
    full_modes = [toks for (m, toks) in fake.modes if m == "full"]
    assert full_modes and set(full_modes[-1]) == {200, 300}


def test_reconnect_no_full_when_full_set_empty():
    """Falcon-only case: no full_tokens → no FULL set_mode issued at all (the LTP
    re-subscribe is byte-for-byte the old behaviour)."""
    st = kt._state
    fake = _FakeWS()
    with st.lock:
        st.subscribed_tokens = {100}
        st.full_tokens = set()
    kt._on_connect(fake, {"ok": True})
    assert not [toks for (m, toks) in fake.modes if m == "full"]


# ── ORDER POSTBACK: store + notify + wait_order_terminal ─────────────────────

def test_on_order_update_stores_and_notifies():
    seen = []
    kt.add_order_listener(lambda oid, data: seen.append((oid, data)))
    kt._on_order_update(None, {
        "order_id": "ORD1", "status": "COMPLETE",
        "filled_quantity": 10, "average_price": 101.25,
        "tradingsymbol": "INFY"})
    got = kt.get_order_update("ORD1")
    assert got["status"] == "COMPLETE"
    assert got["filled_quantity"] == 10
    assert got["average_price"] == 101.25
    assert got["tradingsymbol"] == "INFY"
    assert seen and seen[0][0] == "ORD1"


def test_on_order_update_never_raises_on_bad_payload():
    # Missing order_id → no-op, no crash.
    kt._on_order_update(None, {"status": "COMPLETE"})
    assert kt.get_order_update("") is None
    # Junk values → parse error swallowed, no crash.
    kt._on_order_update(None, {"order_id": "ORDX", "filled_quantity": "junk"})
    # ORDX may or may not be stored depending on where parse failed; the contract
    # is only that it does not raise.


def test_wait_order_terminal_resolves_complete():
    kt._on_order_update(None, {"order_id": "ORD2", "status": "COMPLETE",
                               "filled_quantity": 5, "average_price": 50.0})
    upd = kt.wait_order_terminal("ORD2", timeout=0.1)
    assert upd and upd["status"] == "COMPLETE" and upd["filled_quantity"] == 5


def test_wait_order_terminal_resolves_rejected():
    kt._on_order_update(None, {"order_id": "ORD3", "status": "REJECTED",
                               "filled_quantity": 0, "average_price": 0.0})
    upd = kt.wait_order_terminal("ORD3", timeout=0.1)
    assert upd and upd["status"] == "REJECTED"


def test_wait_order_terminal_times_out_when_no_postback():
    t0 = time.monotonic()
    upd = kt.wait_order_terminal("NEVER", timeout=0.15)
    assert upd is None
    assert time.monotonic() - t0 >= 0.1  # actually waited


def test_wait_order_terminal_wakes_on_late_postback():
    """A postback landing AFTER the wait starts must wake the waiter (Event armed
    before the wait). Simulate via a background thread firing the postback."""
    import threading
    def _late():
        time.sleep(0.05)
        kt._on_order_update(None, {"order_id": "ORD4", "status": "COMPLETE",
                                   "filled_quantity": 3, "average_price": 9.0})
    threading.Thread(target=_late, daemon=True).start()
    upd = kt.wait_order_terminal("ORD4", timeout=2.0)
    assert upd and upd["status"] == "COMPLETE" and upd["filled_quantity"] == 3
