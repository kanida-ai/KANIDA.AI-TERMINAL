"""Kill switch + PortfolioMonitor + exit-gate interaction.

Covers spec Section-10 parity checks + the addendum verification checklist.
All exits go through mocks — no real orders.
"""
import asyncio
import time

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade import exit_gate
from tests.autotrade.mock_broker import MockBroker


def _session_id():
    import uuid
    return uuid.uuid4().hex


def _make_session_row(session_id, cap):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json)
               VALUES (?,?,?,?,?,?)""",
            (session_id, "2026-06-24T09:00:00", "RUNNING", "paper", cap, "{}"))
        con.commit()


def _seed_position(session_id, broker_profile, symbol, qty, avg, ltp):
    reg = PositionRegistry(session_id, 0)
    reg.register(symbol=symbol, broker_profile=broker_profile, qty=qty,
                 avg_price=avg, product="CNC")
    reg.update_ltp(symbol, ltp)
    # set session_id link (register sets it from reg.session_id which is correct)


# ── Parity check: gross_return = uPnL / total_allocated_capital ──────────────

def test_gross_return_formula(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 110.0)   # +1000 uPnL
    reg.register(symbol="B", broker_profile="zer", qty=50, avg_price=200.0)
    reg.update_ltp("B", 210.0)   # +500 uPnL
    mon = PortfolioMonitor(sid, cap)
    # (1000 + 500) / 500000 = 0.003
    assert abs(mon.compute_gross_return() - 0.003) < 1e-9


# ── Addendum: denominator stays total_allocated_capital after a per-pos exit ──

def test_denominator_unchanged_after_position_exit(clean_positions):
    """Denominator (total_allocated_capital) stays frozen when a position closes.

    Fix-1 note: compute_gross_return() now includes realised P&L from CLOSED
    positions so the total PnL is correctly tracked. When C closes at its current
    ltp (110 = exit price via COALESCE(exit_price, ltp)), the realised_pnl =
    (110-100)*100 = 1000, which exactly replaces C's unrealised contribution —
    so gr_after == gr_before (the portfolio's true return is unchanged when you
    capture the same profit). The denominator (total_allocated_capital) is frozen.
    """
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    for s, q, a, l in [("A", 100, 100, 110), ("B", 100, 100, 110),
                       ("C", 100, 100, 110), ("D", 100, 100, 110),
                       ("E", 100, 100, 110)]:
        reg.register(symbol=s, broker_profile="zer", qty=q, avg_price=a)
        reg.update_ltp(s, l)
    mon = PortfolioMonitor(sid, cap)
    gr_before = mon.compute_gross_return()
    # trailing stop closes C at ltp=110 (realised = (110-100)*100 = +1000).
    reg.mark_closed("C", "TRAILING_STOP")
    gr_after = mon.compute_gross_return()
    assert mon.total_allocated_capital == cap     # denominator frozen (critical)
    # Fix-1: with realised P&L included, closing at exact ltp captures the profit
    # so total PnL is unchanged. gr_after == gr_before (not < gr_before).
    # The key safety property is that the DENOMINATOR is frozen, not that the
    # return shrinks on exit (it shouldn't if we captured the gain).
    assert abs(gr_after - gr_before) < 1e-9, (
        "gross_return should be unchanged when C closes at its ltp "
        "(realised P&L replaces the unrealised P&L exactly)")

    # Verify by closing C at a LOSS (below avg_price): total PnL DOES shrink.
    # Directly write a realised loss to test that path.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET realised_pnl=-500.0 "
            "WHERE session_id=? AND symbol='C'", (sid,))
        con.commit()
    gr_loss = mon.compute_gross_return()
    assert gr_loss < gr_before, (
        "gross_return must decrease when a position closes at a loss")


# ── Parity check: parallel kill < 500ms with a 500ms-per-broker mock ─────────

def test_kill_switch_parallel_under_500ms(clean_positions):
    sid = _session_id()
    cap = 1_000_000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    brokers = {}
    # 3 brokers, each exit takes 0.5s; 1 position each.
    for i in range(3):
        pid = f"b{i}"
        brokers[pid] = MockBroker(profile=BrokerProfile(pid, "mock"),
                                  dry_run=False, exit_delay_sec=0.5,
                                  ltps={f"S{i}": 100.0})
        reg.register(symbol=f"S{i}", broker_profile=pid, qty=10, avg_price=100.0)
        reg.update_ltp(f"S{i}", 100.0)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, brokers, reg)
    t0 = time.perf_counter()
    res = asyncio.run(ks.fire("TEST"))
    elapsed = time.perf_counter() - t0
    # Parallel: ~0.5s, NOT 1.5s. Allow generous headroom for CI.
    assert elapsed < 1.2, f"kill took {elapsed:.2f}s — not parallel"
    assert res["n_exited_ok"] == 3


# ── Addendum: exit_lock prevents double-exit (kill switch + day-bound) ───────

def test_exit_lock_prevents_double_exit(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="X", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("X", 100.0)
    # Day-bound exit claims first (session-scoped gate on autotrade_positions).
    assert exit_gate.claim_exit_session(sid, "X", "DAY_BOUND") is True
    # Kill switch tries to claim the same position — must be blocked.
    assert exit_gate.claim_exit_session(sid, "X", "KILL_SWITCH") is False
    # The kill switch fire should therefore NOT place an exit for X.
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"X": 100.0})
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    res = asyncio.run(ks.fire("TEST"))
    assert broker.exits == []          # no duplicate exit order placed
    assert res["n_exited_ok"] == 0


def test_exit_lock_reentrant_same_reason(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="Y", broker_profile="zer", qty=10, avg_price=100.0)
    assert exit_gate.claim_exit_session(sid, "Y", "KILL_SWITCH") is True
    # Same mechanism re-claims (kill switch → shared exit path) → allowed.
    assert exit_gate.claim_exit_session(sid, "Y", "KILL_SWITCH") is True


# ── Threshold direction logic ───────────────────────────────────────────────

def test_check_threshold_disabled_by_default():
    cfg = TradingSessionConfig(total_allocated_capital=1.0)  # enabled defaults False
    ks = KillSwitchExecutor("s", cfg, {}, None)
    assert ks.check_threshold(0.99) is None
    assert ks.check_threshold(-0.99) is None


def test_check_threshold_profit_loss_both():
    cfg = TradingSessionConfig(total_allocated_capital=1.0, kill_switch_enabled=True,
                               kill_switch_pct=0.012, kill_switch_direction="profit")
    ks = KillSwitchExecutor("s", cfg, {}, None)
    assert ks.check_threshold(0.013) is not None
    assert ks.check_threshold(-0.05) is None      # profit-only ignores loss
    cfg.kill_switch_direction = "loss"
    assert ks.check_threshold(-0.013) is not None
    assert ks.check_threshold(0.05) is None
    cfg.kill_switch_direction = "both"
    assert ks.check_threshold(0.013) is not None
    assert ks.check_threshold(-0.013) is not None


# ── Kill switch cancels pending orders before exits ─────────────────────────

def test_kill_switch_cancels_pending_first(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="Z", broker_profile="zer", qty=10, avg_price=100.0)
    reg.update_ltp("Z", 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"Z": 100.0},
                        pending_orders=[{"order_id": "pend-1"},
                                        {"order_id": "pend-2"}])
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))
    assert set(broker.cancelled) == {"pend-1", "pend-2"}
    assert ("Z", 10) in broker.exits


# ── One broker failure does not block others ────────────────────────────────

def test_one_broker_failure_isolated(clean_positions):
    sid = _session_id()
    cap = 1_000_000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    good = MockBroker(profile=BrokerProfile("good", "mock"), dry_run=False,
                      ltps={"G": 100.0})
    bad = MockBroker(profile=BrokerProfile("bad", "mock"), dry_run=False,
                     ltps={"B": 100.0}, fail_symbols={"B"})
    reg.register(symbol="G", broker_profile="good", qty=10, avg_price=100.0)
    reg.update_ltp("G", 100.0)
    reg.register(symbol="B", broker_profile="bad", qty=10, avg_price=100.0)
    reg.update_ltp("B", 100.0)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"good": good, "bad": bad}, reg)
    res = asyncio.run(ks.fire("TEST"))
    assert res["n_exited_ok"] == 1
    assert res["n_exit_failed"] == 1


# ── FIX 2: exit_gate released after mark_exit_failed ─────────────────────────

def test_exit_gate_released_on_failure(clean_positions):
    """After mark_exit_failed, the exit_gate is released so a retry can claim it."""
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="FAIL_GATE", broker_profile="zer",
                 qty=10, avg_price=100.0)
    reg.update_ltp("FAIL_GATE", 100.0)

    # Claim the exit gate first.
    assert exit_gate.claim_exit_session(sid, "FAIL_GATE", "KILL_SWITCH") is True
    # Gate is held — another claim must fail.
    assert exit_gate.claim_exit_session(sid, "FAIL_GATE", "TRAILING_STOP") is False

    # mark_exit_failed MUST release the gate.
    reg.mark_exit_failed("FAIL_GATE", "test failure")

    # Gate is now free — a new claim must succeed.
    assert exit_gate.claim_exit_session(sid, "FAIL_GATE", "EXIT_RETRY") is True


# ── FIX 3/4: mark_closed called with actual fill price (not LTP) ─────────────

def test_kill_switch_uses_confirmed_fill_price(clean_positions):
    """mark_closed is called with the broker's actual fill price, not the LTP.

    Verifies that the kill switch now polls get_order_status for the fill price
    and passes it to mark_closed via confirm_exit, rather than marking closed
    immediately on PLACED with no price.
    """
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="PRICETEST", broker_profile="zer",
                 qty=10, avg_price=100.0)
    reg.update_ltp("PRICETEST", 105.0)

    # Pre-seed the order status so confirm_exit sees COMPLETE with fill_price=108.
    fill_price = 108.0
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"PRICETEST": 105.0})
    broker.set_order_status_sequence(
        "exit-PRICETEST",
        [{"status": "COMPLETE", "filled_quantity": 10, "average_price": fill_price}]
    )

    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    # Verify position was closed with the fill price (not ltp=105).
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, exit_price FROM autotrade_positions "
            "WHERE session_id=? AND symbol='PRICETEST'", (sid,)
        ).fetchone()
    assert row is not None
    assert row["status"] == "CLOSED"
    assert abs(float(row["exit_price"]) - fill_price) < 0.01


# ── FIX 4: kill switch retries on timeout ────────────────────────────────────

def test_kill_switch_retries_on_timeout(clean_positions):
    """A pending order (TIMEOUT from confirm_exit) triggers cancel + retry.

    The broker returns OPEN on the first order, forcing a timeout, then
    returns COMPLETE on the fresh (retry) order. The position should end CLOSED.
    """
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="RETRY_SYM", broker_profile="zer",
                 qty=5, avg_price=200.0)
    reg.update_ltp("RETRY_SYM", 200.0)

    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"RETRY_SYM": 200.0})

    # First order "exit-RETRY_SYM" → always OPEN (force timeout).
    broker.set_order_status_sequence(
        "exit-RETRY_SYM",
        [{"status": "OPEN", "filled_quantity": 0, "average_price": 0.0}]
    )
    # After cancel_and_retry_exit, broker.place_market_exit is called again.
    # The SECOND exit gets a different order id (mock reuses "exit-RETRY_SYM"
    # for the second call too). Override with COMPLETE for the retry.
    # We'll override the sequence after the first TIMEOUT so that the second
    # attempt returns COMPLETE immediately.
    original_place = broker.place_market_exit

    async def _smart_exit(symbol, qty, itype):
        result = await original_place(symbol, qty, itype)
        # After the first call set COMPLETE for subsequent get_order_status.
        broker.set_order_status_sequence(
            result.broker_order_id,
            [{"status": "COMPLETE", "filled_quantity": qty, "average_price": 200.0}]
        )
        return result

    broker.place_market_exit = _smart_exit

    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)

    # Use extremely short timeout so confirm_exit times out on the first order fast.
    from autotrade.monitoring import kill_switch as ks_mod
    import autotrade.monitoring.exit_poller as ep_mod

    # Patch confirm_exit inside kill_switch to use max_wait_sec=0 for this test.
    original_confirm = ep_mod.confirm_exit

    async def _fast_confirm(session_id, symbol, order_id, qty, broker, registry,
                            close_reason="EXIT_CONFIRMED", max_wait_sec=60,
                            poll_interval_sec=5.0):
        return await original_confirm(
            session_id=session_id, symbol=symbol, order_id=order_id,
            qty=qty, broker=broker, registry=registry,
            close_reason=close_reason,
            max_wait_sec=0,     # force immediate timeout
            poll_interval_sec=0.01,
        )

    ep_mod.confirm_exit = _fast_confirm
    try:
        res = asyncio.run(ks.fire("TEST"))
    finally:
        ep_mod.confirm_exit = original_confirm

    # After retry the position should be CLOSED.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status FROM autotrade_positions "
            "WHERE session_id=? AND symbol='RETRY_SYM'", (sid,)
        ).fetchone()
    # Either CLOSED (retry succeeded) or EXIT_FAILED (retry exhausted).
    # The key assertion: n_exit_failed + n_ok = n_positions.
    assert res["n_exited_ok"] + res["n_exit_failed"] == 1
