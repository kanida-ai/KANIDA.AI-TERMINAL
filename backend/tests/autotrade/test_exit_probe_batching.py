"""EXIT/KILL LATENCY — batched pre-exit net-position probe.

The pre-exit reconciliation guard (`broker.get_net_position_qty`) costs ONE broker
round trip PER POSITION and ran SERIALLY in kill_switch.fire's `for pos in
positions:` loop, BEFORE any exit coroutine reached the gather. Measured: ~14.4s
of a 15.9s 3-leg Vortex kill sat before the first EXIT_PLACED (the exits
themselves are concurrent — 8 legs placed in 0.24s). Per probe: Zerodha ~1.1s,
Vortex ~4.8-5.1s, paper ~0.

The fix hoists the probe: fetch each broker's FULL net book ONCE
(fetch_net_position_book) and answer every leg from it in memory
(net_qty_from_book) → O(N) round trips become O(1).

THE SAFETY CONSTRAINT THESE TESTS EXIST TO PIN:
get_positions_net() SWALLOWS broker errors to None, and None is ALSO the PAPER
sentinel that the guard reads as "no book to reconcile, proceed with the exit".
Reusing it would place BLIND exits on a broker error → the 2026-07-10 BRIGADE
double-cover class. So the batch primitive RAISES on a live error instead, and
ANY batch failure/None falls back to TODAY'S EXACT per-leg probe loop — which
fails loud per leg (probe_raised → skip the leg, no order). A batch failure must
NEVER abort all legs either (a fail-OPEN kill switch is worse than a slow one).

Covers: batch-hit clamps identically to the per-leg probe; batch error in LIVE →
per-leg fallback → leg ABORTED with NO order (anti-BRIGADE); batch error → other
legs still exit (no fail-open); paper → zero round trips; O(1) scaling; adapter
parity (batch matcher == per-leg matcher) on both Zerodha and Rupeezy.
"""
import asyncio
import uuid

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from tests.autotrade.mock_broker import MockBroker
# Reuse the adapter suite's `requests`-module double (it records every call and
# now models Session(), since _request() pools connections).
from tests.autotrade.test_rupeezy_adapter import _RequestsRecorder
from autotrade.broker import rupeezy as rupeezy_mod


@pytest.fixture
def fake_requests(monkeypatch):
    rec = _RequestsRecorder()
    import sys
    monkeypatch.setitem(sys.modules, "requests", rec)
    rupeezy_mod._SESSIONS.clear()   # module-level pool — never leak across tests
    yield rec
    rupeezy_mod._SESSIONS.clear()


def _sid():
    return uuid.uuid4().hex


def _make_session(sid, cap=500000.0):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json) VALUES (?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", "RUNNING", "paper", cap, "{}"))
        con.commit()


def _seed(sid, symbols_qty, cap=500000.0):
    reg = PositionRegistry(sid, cap)
    for sym, qty in symbols_qty.items():
        reg.register(symbol=sym, broker_profile="zer", qty=qty,
                     avg_price=100.0, product="CNC")
        reg.update_ltp(sym, 100.0, broker_profile="zer")
    return reg


def _cfg(cap=500000.0):
    return TradingSessionConfig(total_allocated_capital=cap,
                                kill_switch_enabled=True)


def _exits(broker):
    """{symbol: qty} of the exits the mock actually PLACED. MockBroker records
    each place_market_exit as a (symbol, qty) tuple on .exits."""
    return {sym: qty for sym, qty in broker.exits}


# ── 1. BATCH HIT: identical clamp decision, O(1) round trips ─────────────────

def test_batch_hit_clamps_identically_to_per_leg_probe(clean_positions):
    """The broker shrank under us (10 held, only 4 left). BOTH paths must clamp
    the exit to 4 — the batch is a pure fast path, never a different decision."""
    results = {}
    for label, batch in (("per_leg", False), ("batch", True)):
        sid = _sid()
        _make_session(sid)
        reg = _seed(sid, {"A": 10})
        broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                            ltps={"A": 100.0}, net_positions={"A": 4},
                            batch_probe=batch)
        ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
        asyncio.run(ks.fire("TEST"))
        ex = _exits(broker)
        assert len(ex) == 1, f"{label}: expected exactly one exit, got {ex}"
        results[label] = ex["A"]
        if batch:
            # O(1): ONE book fetch for the whole basket, ZERO per-leg probes.
            assert broker.batch_book_fetches == 1
            assert broker.per_leg_probes == 0
        else:
            assert broker.per_leg_probes == 1

    assert results["batch"] == results["per_leg"] == 4, (
        f"batch/per-leg diverged on the CLAMP: {results}")


def test_batch_probe_is_O1_in_number_of_legs(clean_positions):
    """8 legs → ONE book fetch (was 8 serial round trips ≈ 8×1.1s on Kite)."""
    sid = _sid()
    _make_session(sid)
    syms = {f"S{i}": 10 for i in range(8)}
    reg = _seed(sid, syms)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={s: 100.0 for s in syms},
                        net_positions={s: 10 for s in syms}, batch_probe=True)
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    assert broker.batch_book_fetches == 1      # O(1), NOT O(N)
    assert broker.per_leg_probes == 0
    assert len(_exits(broker)) == 8            # all 8 legs still exited


# ── 2. ANTI-BRIGADE: a batch error can NEVER produce a blind exit ────────────

def test_batch_error_in_live_falls_back_to_per_leg_and_aborts_leg_no_order(
        clean_positions):
    """THE anti-BRIGADE test.

    The batch fetch RAISES (live broker error) AND the per-leg fallback probe also
    RAISES. We CANNOT confirm the live position → the leg must be ABORTED with NO
    order placed (a blind buy-to-cover is how the 2026-07-10 BRIGADE double-cover
    went naked). The batch failure must degrade to the per-leg fail-loud path —
    never to "proceed unguarded".
    """
    sid = _sid()
    _make_session(sid)
    reg = _seed(sid, {"A": 10})
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 10},
                        batch_probe=True, batch_probe_raises=True,
                        net_probe_raise_symbols={"A"})
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    res = asyncio.run(ks.fire("TEST"))

    # The batch was ATTEMPTED and FAILED...
    assert broker.batch_book_fetches == 1
    # ...and we fell back to the EXACT per-leg probe (which also raised)...
    assert broker.per_leg_probes == 1, (
        "a failed batch MUST fall back to the per-leg probe, not skip the guard")
    # ...so NO exit order was placed. This is the whole point.
    assert _exits(broker) == {}, (
        "BLIND EXIT PLACED after a broker probe error — BRIGADE class regression")
    # The leg is reported as probe-failed and left OPEN for a later retry.
    details = res.get("details") or []
    assert any(d.get("probe_failed") for d in details), (
        f"expected a probe_failed leg in {details}")


def test_batch_error_does_not_abort_legs_whose_per_leg_probe_succeeds(
        clean_positions):
    """A batch failure must NOT fail-OPEN the kill switch. When the batch raises
    but the per-leg probe works, every leg still exits, correctly clamped."""
    sid = _sid()
    _make_session(sid)
    reg = _seed(sid, {"A": 10, "B": 10})
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 100.0},
                        net_positions={"A": 10, "B": 6},
                        batch_probe=True, batch_probe_raises=True)
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    assert broker.batch_book_fetches == 1
    assert broker.per_leg_probes == 2            # fell back for BOTH legs
    ex = _exits(broker)
    assert ex == {"A": 10, "B": 6}, (
        f"a batch failure must still flatten (clamped) — got {ex}")


def test_batch_hit_still_aborts_the_leg_when_the_book_cannot_answer(
        clean_positions):
    """A symbol the book cannot answer (absent → None from net_qty_from_book)
    falls back to the per-leg probe; if THAT raises, the leg is aborted with no
    order — the fast path never bypasses the guard for an unanswered leg."""
    sid = _sid()
    _make_session(sid)
    reg = _seed(sid, {"A": 10, "B": 10})
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 100.0},
                        # B is ABSENT from the book → the batch can't answer it.
                        net_positions={"A": 10},
                        batch_probe=True, net_probe_raise_symbols={"B"})
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    ex = _exits(broker)
    assert "A" in ex and ex["A"] == 10       # answered from the book, exited
    assert "B" not in ex, "B's probe raised — a blind exit must NOT be placed"


# ── 3. KILL still fires + clamps to our_held (no-fire / fire parity) ─────────

def test_kill_fires_and_reconciles_flat_leg_via_batch(clean_positions):
    """our_held == 0 through the BATCH path → reconcile, place NOTHING (same as
    the per-leg path). A broker-flat leg must never get an order."""
    sid = _sid()
    _make_session(sid)
    reg = _seed(sid, {"A": 10, "B": 10})
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 100.0},
                        net_positions={"A": 0, "B": 10},  # A already flat
                        batch_probe=True)
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    ex = _exits(broker)
    assert "A" not in ex, "A is flat at the broker — no exit order may be placed"
    assert ex.get("B") == 10
    assert broker.batch_book_fetches == 1


# ── 4. PAPER: zero broker round trips, unchanged ────────────────────────────

def test_paper_makes_zero_probe_round_trips(clean_positions):
    """Paper has no broker book. The real adapters return None from BOTH probes
    WITHOUT a round trip; the kill proceeds with its normal exit, unchanged."""
    sid = _sid()
    _make_session(sid)
    reg = _seed(sid, {"A": 10, "B": 10})
    # net_positions=None + batch_probe default OFF == the paper/stub shape.
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                        ltps={"A": 100.0, "B": 100.0})
    ks = KillSwitchExecutor(sid, _cfg(), {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    assert broker.batch_book_fetches == 0
    assert len(_exits(broker)) == 2           # both legs exit, unclamped


def test_paper_adapters_return_none_without_touching_the_broker():
    """The REAL adapters: paper/not-live → fetch_net_position_book() is None and
    NO broker call is made (zero round trips), so paper is byte-for-byte
    unchanged. This is the 'paper → None (fine, no book)' half of the explicit
    paper-vs-error distinction."""
    from autotrade.broker.zerodha import ZerodhaBroker
    from autotrade.broker.rupeezy import RupeezyBroker

    class _BoomKite:
        def positions(self):
            raise AssertionError("paper must make ZERO broker round trips")

    z = ZerodhaBroker(profile=BrokerProfile("p", "zerodha"), dry_run=True)
    z._kite = _BoomKite()
    assert z.fetch_net_position_book() is None
    assert z.get_net_position_qty("INFY") is None

    r = RupeezyBroker(profile=BrokerProfile("p", "rupeezy"), dry_run=True)
    assert r.fetch_net_position_book() is None
    assert r.get_net_position_qty("INFY") is None


# ── 5. ADAPTER PARITY — the batch matcher IS the per-leg matcher ─────────────

@pytest.mark.parametrize("symbol,itype,expected", [
    ("INFY", "EQ", 50),        # plain cash match
    ("TCS", "EQ", -25),        # signed short
    ("ABSENT", "EQ", 0),       # book retrieved, symbol absent → flat
    ("NIFTYFUT", "FUT", 75),   # NSE+FUT → NFO segment match
])
def test_zerodha_batch_matches_per_leg_exactly(monkeypatch, symbol, itype,
                                               expected):
    """net_qty_from_book(book) must return EXACTLY what get_net_position_qty
    would for that same book — they share _net_qty_match by construction."""
    from autotrade.broker.zerodha import ZerodhaBroker

    rows = [
        {"tradingsymbol": "INFY", "exchange": "NSE", "quantity": 50},
        {"tradingsymbol": "TCS", "exchange": "NSE", "quantity": -25},
        # A cash row must NOT shadow the FUT contract of the same name.
        {"tradingsymbol": "NIFTYFUT", "exchange": "NSE", "quantity": 11},
        {"tradingsymbol": "NIFTYFUT", "exchange": "NFO", "quantity": 75},
    ]

    class _Kite:
        def __init__(self):
            self.calls = 0

        def positions(self):
            self.calls += 1
            return {"net": rows}

    b = ZerodhaBroker(profile=BrokerProfile("p", "zerodha"), dry_run=False)
    fk = _Kite()
    b._kite = fk
    b._live_allowed = lambda: True   # force the live gate WITHOUT touching env

    per_leg = b.get_net_position_qty(symbol, itype)
    book = b.fetch_net_position_book()
    batched = b.net_qty_from_book(book, symbol, itype)

    assert per_leg == expected
    assert batched == per_leg          # identical decision
    assert fk.calls == 2               # 1 per-leg + 1 batch; the batch is ONE


def test_zerodha_batch_fetch_RAISES_on_error_never_none(monkeypatch):
    """CRITICAL CONTRACT: fetch_net_position_book RAISES on a live broker error.
    It must NOT behave like get_positions_net, which swallows to None — None is
    the PAPER sentinel and would be read as 'no book, proceed' → a BLIND exit."""
    from autotrade.broker.zerodha import ZerodhaBroker

    class _BoomKite:
        def positions(self):
            raise ConnectionResetError(10054, "forcibly closed")

    b = ZerodhaBroker(profile=BrokerProfile("p", "zerodha"), dry_run=False)
    b._kite = _BoomKite()
    b._live_allowed = lambda: True

    with pytest.raises(Exception):
        b.fetch_net_position_book()
    with pytest.raises(Exception):
        b.get_net_position_qty("INFY")
    # The error-swallowing sibling still returns None — proving the two have
    # DIFFERENT contracts on purpose, and that we did not reuse the unsafe one.
    assert b.get_positions_net() is None


def test_rupeezy_batch_matches_per_leg_and_raises_on_error(fake_requests):
    """Rupeezy parity: same matcher for batch + per-leg (bare-symbol match,
    product-aware _held_from_row), and the batch RAISES on a transport error."""
    from autotrade.broker.rupeezy import RupeezyBroker
    from types import SimpleNamespace

    prof = SimpleNamespace(api_key="app-123", api_secret="xkey",
                           access_token="tok", broker_account_id="acc-1",
                           broker_name="rupeezy")
    b = RupeezyBroker(profile=prof, dry_run=False)
    b._live_allowed = lambda: True

    # A DELIVERY row carries the position in buy/sell with quantity=0.
    fake_requests.on("/trading/portfolio/positions", _RupeezyResp())
    per_leg = b.get_net_position_qty("INFY")
    book = b.fetch_net_position_book()
    batched = b.net_qty_from_book(book, "INFY")
    assert per_leg == 7
    assert batched == per_leg
    assert b.net_qty_from_book(book, "NOTHERE") == 0   # absent → flat

    # Transport error → the batch RAISES (never a silent None → blind exit).
    def _boom(*a, **k):
        raise ConnectionResetError(10054, "forcibly closed")
    fake_requests.request = _boom
    with pytest.raises(Exception):
        b.fetch_net_position_book()


class _RupeezyResp:
    status_code = 200

    def json(self):
        return {"data": [{"symbol": "INFY", "quantity": 0,
                          "buy_quantity": 7, "sell_quantity": 0,
                          "exchange": "NSE_EQ", "product": "DELIVERY"}]}

    def raise_for_status(self):
        return None
