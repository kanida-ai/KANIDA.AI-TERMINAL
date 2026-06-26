"""SPEED PASS tests (mocks only — NO real Kite / orders).

Covers the additive latency-minimisation work:

  A. ENTRY
     * batched sizing == old per-stock sizing (same qty),
     * parallel entry places ALL legs + ISOLATES a single failing leg.
  B. DETECTION
     * the event-driven tick listener wakes the WS driver and fires the
       trail/kill on a simulated tick WITHOUT waiting for the poll.
  C. EXIT
     * the parallel GTT-cancel sweep cancels every GTT before the flatten,
     * a fully-parallel exit places NO double-sell (exit-gate stress).
  D. OBSERVABILITY
     * entry_latency_ms / exit_latency_ms / last_tick_age_ms surfaced.
"""
import asyncio
import time
import uuid

import pytest

import autotrade.broker.router as router_mod
from autotrade.capital import CapitalAllocator
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.monitoring import fire_guard, ws_driver
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


def _sid():
    return uuid.uuid4().hex


def _make_session_row(session_id, cap, mode="paper", status="RUNNING",
                      invested_basis=None):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                invested_basis, config_json)
               VALUES (?,?,?,?,?,?,?)""",
            (session_id, "2026-06-24T09:00:00", status, mode, cap,
             invested_basis, "{}"))
        con.commit()


@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 150.0, "E": 300.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=dry_run, ltps=shared)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


# ── A. batched sizing == per-stock sizing (same qty) ─────────────────────────

def test_batched_sizing_equals_per_stock_cash(clean_positions):
    """calculate_quantity_cached (batched) yields the SAME qty as the legacy
    per-symbol calculate_quantity for cash equity."""
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal")
    alloc = CapitalAllocator(cfg)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                        ltps={"A": 100.0, "B": 200.0, "C": 50.0})
    syms = ["A", "B", "C"]
    amounts = alloc.allocate(syms)
    cache = alloc.prefetch(syms, broker)
    for s in syms:
        legacy = alloc.calculate_quantity(s, amounts[s], broker)
        batched = alloc.calculate_quantity_cached(s, amounts[s], broker,
                                                  cache=cache)
        assert batched == legacy, s


def test_batched_sizing_equals_per_stock_mtf_with_margin(clean_positions):
    """MTF: batched margin sizing == per-symbol margin sizing; and a MISSING
    margin entry cash-falls-back (never over-deploys)."""
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", order_product="MTF")

    class MTFMock(MockBroker):
        def __init__(self, *a, margins=None, **k):
            super().__init__(*a, **k)
            self._margins = margins or {}

        def get_margin_per_share(self, symbol, product="MTF"):
            return self._margins.get(symbol)

        def get_margins_batch(self, symbols, product="MTF"):
            return {s: self._margins[s] for s in symbols if s in self._margins}

    # A,B have margins; C has NONE → cash fallback.
    margins = {"A": 40.0, "B": 80.0}
    alloc = CapitalAllocator(cfg)
    broker = MTFMock(profile=BrokerProfile("zer", "mock"), dry_run=True,
                     ltps={"A": 100.0, "B": 200.0, "C": 50.0}, margins=margins)
    syms = ["A", "B", "C"]
    amounts = alloc.allocate(syms)
    cache = alloc.prefetch(syms, broker)
    for s in syms:
        legacy = alloc.calculate_quantity(s, amounts[s], broker)
        batched = alloc.calculate_quantity_cached(s, amounts[s], broker,
                                                  cache=cache)
        assert batched == legacy, s
    # A leveraged off margin (100000/40 = 2500), C cash off ltp (100000/50=2000).
    assert alloc.calculate_quantity_cached("A", amounts["A"], broker, cache) == 2500
    assert alloc.calculate_quantity_cached("C", amounts["C"], broker, cache) == 2000


# ── A. parallel entry places all legs ────────────────────────────────────────

def test_parallel_entry_places_all_legs(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3
    assert sess.status()["n_open_positions"] == 3


# ── A. one failing leg never aborts the others (isolation) ───────────────────

def test_parallel_entry_isolates_one_failed_leg(clean_positions, monkeypatch):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])

    def fake_build_client(profile, dry_run=True):
        # B's place_order raises → that leg FAILS, others must still register.
        mb = MockBroker(profile=profile, dry_run=False,
                        ltps={"A": 100.0, "B": 200.0, "C": 50.0})
        orig = mb.place_order

        async def maybe_fail(order):
            if order.symbol == "B":
                raise RuntimeError("broker rejected B")
            return await orig(order)
        mb.place_order = maybe_fail
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    statuses = {r["symbol"]: r["status"] for r in res["orders"]}
    assert statuses["B"] == "FAILED"
    assert statuses["A"] in ("PLACED", "DRY_RUN")
    assert statuses["C"] in ("PLACED", "DRY_RUN")
    # A and C registered; B did not.
    opens = {p["symbol"] for p in sess.registry.get_open_positions()}
    assert opens == {"A", "C"}


# ── B. event-driven trigger fires WITHOUT waiting for the poll ────────────────

def test_event_driven_listener_wakes_and_fires(clean_positions, patched_brokers):
    """The kite_ticker tick listener wakes the WS driver immediately; the eval
    fires the kill switch on a crossing tick well before a poll would. Uses a
    LONG poll backstop (5s) so any fire within ~1s proves it was event-driven."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 2

    # Crossing LTPs fed via an injected sub-second source.
    live = {"A": 130.0, "B": 260.0}
    # Long backstop poll so a fast fire can only come from the event wake.
    drv = ws_driver._WSDriver(sess.session_id, poll_sec=5.0,
                              ltp_source=lambda s: live.get(s))
    ws_driver._DRIVERS[sess.session_id] = drv
    drv.start()
    try:
        # Give the thread a moment to arm + register the listener.
        time.sleep(0.3)
        t0 = time.time()
        # Simulate a tick arriving on the shared ticker for a held symbol.
        from falcon.trade.services import kite_ticker
        # Fire the listener directly (what _on_ticks does after a tick batch).
        with kite_ticker._listeners_lock:
            listeners = list(kite_ticker._tick_listeners)
        assert listeners, "WS driver did not register a tick listener"
        for fn in listeners:
            fn({"A"})

        # Poll for the session to close — must happen well under the 5s backstop.
        deadline = time.time() + 3.0
        from falcon.db import falcon_conn
        status = None
        while time.time() < deadline:
            with falcon_conn() as con:
                status = con.execute(
                    "SELECT status FROM autotrade_sessions WHERE session_id=?",
                    (sess.session_id,)).fetchone()["status"]
            if status == "CLOSED":
                break
            time.sleep(0.05)
        elapsed = time.time() - t0
        assert status == "CLOSED", "event-driven fire did not close the session"
        assert elapsed < 4.5, f"fire took {elapsed:.2f}s — looks poll-driven"
    finally:
        drv.stop()
        drv._thread.join(timeout=3.0)


# ── C. parallel GTT-cancel sweep cancels all before flatten ──────────────────

def test_parallel_gtt_cancel_sweep(clean_positions):
    sid = _sid()
    cap = 500000.0
    _make_session_row(sid, cap, mode="live")
    reg = PositionRegistry(sid, cap)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 200.0, "C": 50.0})
    for s, a in (("A", 100.0), ("B", 200.0), ("C", 50.0)):
        reg.register(symbol=s, broker_profile="zer", qty=10, avg_price=a)
        reg.update_ltp(s, a)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()
    placed_ids = {g["gtt_id"] for g in broker.gtts}
    assert len(placed_ids) == 3

    out = asyncio.run(mgr.cancel_session_gtts_async())
    assert {o["status"] for o in out} == {"CANCELLED"}
    assert set(broker.cancelled_gtts) == placed_ids


# ── C. fully-parallel exit, NO double-sell (exit-gate stress) ────────────────

def test_parallel_exit_no_double_sell_stress(clean_positions):
    """Hammer the kill switch + a concurrent manual fire on the SAME session at
    once. The fire guard + exit gate must yield EXACTLY one sell per symbol."""
    sid = _sid()
    cap = 1000000.0
    _make_session_row(sid, cap, mode="live", invested_basis=cap)
    reg = PositionRegistry(sid, cap)
    n = 8
    syms = [f"S{i}" for i in range(n)]
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={s: 100.0 for s in syms},
                        exit_delay_sec=0.02)  # widen the race window
    for s in syms:
        reg.register(symbol=s, broker_profile="zer", qty=10, avg_price=100.0)
        reg.update_ltp(s, 100.0)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg, gtt_manager=mgr)

    async def two_fires_racing():
        async def guarded():
            with fire_guard.claim_fire(sid) as won:
                if won:
                    return await ks.fire("STRESS")
                return {"already_fired": True}
        return await asyncio.gather(guarded(), guarded())

    results = asyncio.run(two_fires_racing())
    fired = [r for r in results if not r.get("already_fired")]
    assert len(fired) == 1, "fire guard let TWO fires through"
    assert fired[0]["n_exited_ok"] == n
    # CRITICAL: exactly one sell per symbol — no double-exit.
    exit_syms = [s for (s, q) in broker.exits]
    assert sorted(exit_syms) == sorted(syms)
    assert len(exit_syms) == n


# ── D. latency observability fields populated ────────────────────────────────

def test_entry_latency_recorded(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    st = sess.status()
    assert "entry_latency_ms" in st
    assert isinstance(st["entry_latency_ms"], int)
    assert st["entry_latency_ms"] >= 0
    assert "exit_latency_ms" in st          # None until a flatten
    assert "last_tick_age_ms" in st         # None in tests (no real ticker)


def test_exit_latency_recorded(clean_positions):
    sid = _sid()
    cap = 100000.0
    _make_session_row(sid, cap, mode="paper", invested_basis=cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 110.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                        ltps={"A": 110.0})
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST", gross_return=0.1))
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT exit_latency_ms FROM autotrade_sessions WHERE session_id=?",
            (sid,)).fetchone()
    assert row["exit_latency_ms"] is not None
    assert int(row["exit_latency_ms"]) >= 0
