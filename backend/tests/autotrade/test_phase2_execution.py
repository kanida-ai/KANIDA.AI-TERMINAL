"""PHASE-2 execution: WS-overlay get_quotes (warm = ZERO REST) + postback-driven
sub-second entry-fill reconcile.

Proves:
  * zerodha.get_quotes overlays the WS FULL cache first and uses the per-day
    circuit cache → a WARM fire calls kite.quote() ZERO times.
  * a cold symbol triggers exactly ONE batched REST kite.quote() and PRIMES the
    circuit day-cache so the next fire is warm.
  * _reconcile_entry_fill resolves COMPLETE from an order POSTBACK WITHOUT ever
    calling get_order_status (the poll); a REJECTED postback returns rejected.
  * paper / default execution_mode paths never touch any of this.
"""
from __future__ import annotations

import asyncio

import pytest

from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.broker.zerodha import ZerodhaBroker
import autotrade.broker.zerodha as zmod
from falcon.trade.services import kite_ticker as kt


# ── Fakes ────────────────────────────────────────────────────────────────────

class _CountingKite:
    """Records how many times .quote() is called + what it returns."""
    def __init__(self, quote_rows=None):
        self.quote_calls = 0
        self._rows = quote_rows or {}

    def quote(self, keys):
        self.quote_calls += 1
        # keys are 'NSE:SYM'; return only the ones we know.
        return {k: self._rows[k] for k in keys if k in self._rows}


def _mk_broker(kite, dry_run=False):
    prof = BrokerProfile(profile_id="p1", broker_name="zerodha",
                         allocated_capital=100_000.0)
    b = ZerodhaBroker(prof, dry_run=dry_run)
    b._kite = kite
    # Force _live_allowed True regardless of env (unit isolation).
    b._live_allowed = lambda: True  # type: ignore
    return b


@pytest.fixture(autouse=True)
def _clean_ticker_and_circuit():
    st = kt._state
    with st.lock:
        st.tick_cache = {}
        st.sym_to_token = {}
        st.token_to_sym = {}
        st.full_tokens = set()
    zmod._CIRCUIT_DAY.clear()
    yield
    zmod._CIRCUIT_DAY.clear()


def _seed_ws_full(sym, token, ltp, bid, ask):
    st = kt._state
    with st.lock:
        st.sym_to_token[sym] = token
        st.token_to_sym[token] = sym
        st.full_tokens.add(token)
    kt._on_ticks(None, [{"instrument_token": token, "last_price": ltp,
                         "depth": {"buy": [{"price": bid}],
                                   "sell": [{"price": ask}]}}])


def _rest_row(ltp, bid, ask, upper, lower):
    return {"last_price": ltp,
            "depth": {"buy": [{"price": bid}], "sell": [{"price": ask}]},
            "upper_circuit_limit": upper, "lower_circuit_limit": lower}


# ── WARM path: ZERO REST when WS + circuit cache are primed ───────────────────

def test_get_quotes_warm_is_zero_rest():
    """WS FULL cache has bid/ask/ltp AND the circuit day-cache is primed → a fire
    calls kite.quote() ZERO times (the benchmark)."""
    kite = _CountingKite()
    b = _mk_broker(kite)
    _seed_ws_full("INFY", 111, ltp=100.0, bid=99.9, ask=100.1)
    # Prime the circuit band day-cache (as prewarm would).
    zmod._circuit_put("INFY", 110.0, 90.0)

    out = b.get_quotes(["INFY"])
    assert kite.quote_calls == 0            # ← the warm-path proof
    assert out["INFY"]["bid"] == 99.9
    assert out["INFY"]["ask"] == 100.1
    assert out["INFY"]["ltp"] == 100.0
    assert out["INFY"]["upper_circuit"] == 110.0
    assert out["INFY"]["lower_circuit"] == 90.0


def test_get_quotes_cold_does_one_rest_and_primes_circuit():
    """A cold symbol (no WS, no cached circuit) → exactly ONE batched REST call,
    and it PRIMES the circuit day-cache so the NEXT call is warm (zero REST)."""
    kite = _CountingKite({"NSE:TCS": _rest_row(3800.0, 3799.5, 3800.5,
                                               4000.0, 3600.0)})
    b = _mk_broker(kite)

    out1 = b.get_quotes(["TCS"])
    assert kite.quote_calls == 1
    assert out1["TCS"]["upper_circuit"] == 4000.0
    # Circuit primed for the day.
    assert zmod._circuit_get("TCS") == (4000.0, 3600.0)

    # Now seed the WS cache (as if ticks arrived) — the next fire is warm.
    _seed_ws_full("TCS", 222, ltp=3801.0, bid=3800.9, ask=3801.1)
    out2 = b.get_quotes(["TCS"])
    assert kite.quote_calls == 1            # NO new REST call
    assert out2["TCS"]["bid"] == 3800.9
    assert out2["TCS"]["upper_circuit"] == 4000.0   # from the day-cache


def test_get_quotes_mixed_only_rests_cold_symbols():
    """One warm + one cold → ONE REST for just the cold symbol; the warm one
    contributes zero network."""
    kite = _CountingKite({"NSE:COLD": _rest_row(50.0, 49.9, 50.1, 55.0, 45.0)})
    b = _mk_broker(kite)
    _seed_ws_full("WARM", 1, ltp=10.0, bid=9.9, ask=10.1)
    zmod._circuit_put("WARM", 11.0, 9.0)

    out = b.get_quotes(["WARM", "COLD"])
    assert kite.quote_calls == 1
    assert out["WARM"]["bid"] == 9.9 and out["WARM"]["upper_circuit"] == 11.0
    assert out["COLD"]["ask"] == 50.1 and out["COLD"]["upper_circuit"] == 55.0


def test_get_quotes_none_when_paper():
    """Paper / not live → None sentinel, no kite access at all."""
    b = ZerodhaBroker(BrokerProfile("p1", "zerodha"), dry_run=True)
    assert b.get_quotes(["INFY"]) is None


def test_prime_circuit_limits_one_rest_populates_daycache():
    kite = _CountingKite({"NSE:AAA": _rest_row(1.0, 0.9, 1.1, 2.0, 0.5),
                          "NSE:BBB": _rest_row(2.0, 1.9, 2.1, 3.0, 1.0)})
    b = _mk_broker(kite)
    n = b.prime_circuit_limits(["AAA", "BBB"])
    assert n == 2
    assert kite.quote_calls == 1
    assert zmod._circuit_get("AAA") == (2.0, 0.5)
    assert zmod._circuit_get("BBB") == (3.0, 1.0)


# ── POSTBACK-DRIVEN reconcile: sub-second, no get_order_status poll ───────────

class _NoPollBroker:
    """A broker whose get_order_status EXPLODES if called — proves the postback
    path resolved the fill WITHOUT polling."""
    def __init__(self):
        self.status_calls = 0

    def get_order_status(self, order_id):
        self.status_calls += 1
        raise AssertionError("get_order_status must NOT be called when a postback "
                             "resolves the fill")


def _make_live_session():
    """A minimal TradingSession in LIVE (dry_run off) mode for the reconcile path.
    No orders are placed — we call _reconcile_entry_fill directly with a fake
    broker, so nothing real happens."""
    from autotrade.session import TradingSession
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0)
    sess = TradingSession.create(cfg, mode="live")   # dry_run=False
    return sess


@pytest.fixture(autouse=True)
def _reset_order_state():
    with kt._listeners_lock:
        kt._order_listeners.clear()
    with kt._order_events_lock:
        kt._order_events.clear()
    with kt._state.lock:
        kt._state.order_updates = {}
    yield


def test_reconcile_resolves_complete_from_postback_without_polling(clean_positions):
    sess = _make_live_session()
    assert sess.dry_run is False
    broker = _NoPollBroker()
    # A COMPLETE postback lands for the order.
    kt._on_order_update(None, {"order_id": "ORDP1", "status": "COMPLETE",
                               "filled_quantity": 7, "average_price": 250.5})
    rec = asyncio.run(sess._reconcile_entry_fill(broker, "ORDP1", 7))
    assert rec == {"avg_price": 250.5, "filled_qty": 7}
    assert broker.status_calls == 0        # ← poll never hit


def test_reconcile_returns_rejected_from_postback(clean_positions):
    sess = _make_live_session()
    broker = _NoPollBroker()
    kt._on_order_update(None, {"order_id": "ORDP2", "status": "REJECTED",
                               "filled_quantity": 0, "average_price": 0.0})
    rec = asyncio.run(sess._reconcile_entry_fill(broker, "ORDP2", 5))
    assert rec == {"rejected": True, "status": "REJECTED"}
    assert broker.status_calls == 0


def test_reconcile_falls_back_to_poll_when_no_postback(clean_positions):
    """No postback within the short window → fall through to the get_order_status
    poll (the backstop), which resolves COMPLETE."""
    sess = _make_live_session()

    class _PollBroker:
        def __init__(self):
            self.calls = 0
        def get_order_status(self, order_id):
            self.calls += 1
            return {"status": "COMPLETE", "filled_quantity": 4,
                    "average_price": 12.5}

    broker = _PollBroker()
    rec = asyncio.run(sess._reconcile_entry_fill(broker, "ORDNOPB", 4,
                                                 max_wait_sec=2.0,
                                                 poll_interval=0.05))
    assert rec == {"avg_price": 12.5, "filled_qty": 4}
    assert broker.calls >= 1               # the poll backstop DID run


def test_reconcile_skips_postback_in_dry_run(clean_positions):
    """Paper (dry_run) NEVER consults the postback — it uses the poll path
    directly (byte-for-byte the pre-Phase-2 behaviour)."""
    from autotrade.session import TradingSession
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0)
    sess = TradingSession.create(cfg, mode="paper")   # dry_run=True
    assert sess.dry_run is True

    class _PollBroker:
        def __init__(self):
            self.calls = 0
        def get_order_status(self, order_id):
            self.calls += 1
            return {"status": "COMPLETE", "filled_quantity": 2,
                    "average_price": 5.0}

    # Even though a COMPLETE postback exists, dry_run must ignore it and poll.
    kt._on_order_update(None, {"order_id": "ORDDRY", "status": "COMPLETE",
                               "filled_quantity": 99, "average_price": 999.0})
    broker = _PollBroker()
    rec = asyncio.run(sess._reconcile_entry_fill(broker, "ORDDRY", 2,
                                                 max_wait_sec=1.0,
                                                 poll_interval=0.05))
    assert rec == {"avg_price": 5.0, "filled_qty": 2}   # from the POLL, not postback
    assert broker.calls >= 1


# ── PRE-OPEN WARM: prewarm_execution wiring ──────────────────────────────────

def test_prewarm_is_noop_for_market_mode(clean_positions):
    """Default execution_mode='market' → prewarm does NOTHING (no subscribe, no
    prime). Byte-for-byte the pre-Phase-2 path."""
    from autotrade.session import TradingSession, prewarm_execution
    from tests.autotrade.conftest import seed_signals
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100_000.0,
                               execution_mode="market")  # explicit (env-independent)
    sess = TradingSession.create(cfg, mode="paper")
    out = prewarm_execution(sess)
    assert out["subscribed_full"] == 0
    assert out["circuits_primed"] == 0
    assert out["symbols"] == []


def test_prewarm_resolves_basket_and_calls_full_and_prime(monkeypatch,
                                                          clean_positions):
    """marketable_limit → prewarm resolves the Falcon basket, calls subscribe_full
    for the symbols, and primes circuits via the broker. Uses a quote-aware
    MockBroker + patched subscribe_full so no real Kite is touched."""
    from autotrade.session import TradingSession, prewarm_execution
    import autotrade.session as sess_mod
    import autotrade.broker.router as router_mod
    from tests.autotrade.conftest import seed_signals
    from tests.autotrade.mock_broker import MockBroker
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])

    primed_syms = {}

    class _PrimeMock(MockBroker):
        def prime_circuit_limits(self, symbols):
            primed_syms["symbols"] = list(symbols)
            return len(symbols)

    def fake_build_client(profile, dry_run=True):
        return _PrimeMock(profile=profile, dry_run=dry_run,
                          ltps={"A": 100.0, "B": 200.0})
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    captured = {}
    def fake_subscribe_full(symbols):
        captured["symbols"] = list(symbols)
        return len(symbols)
    monkeypatch.setattr("falcon.trade.services.kite_ticker.subscribe_full",
                        fake_subscribe_full)

    cfg = TradingSessionConfig(total_allocated_capital=100_000.0,
                               execution_mode="marketable_limit",
                               top_n_stocks=2, sizing_mode="equal")
    sess = TradingSession.create(cfg, mode="paper")
    out = prewarm_execution(sess)

    assert set(out["symbols"]) == {"A", "B"}
    assert set(captured["symbols"]) == {"A", "B"}      # subscribe_full called
    assert set(primed_syms["symbols"]) == {"A", "B"}   # prime called
    assert out["subscribed_full"] == 2
    assert out["circuits_primed"] == 2
