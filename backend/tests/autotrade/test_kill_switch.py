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
    # trailing stop closes C
    reg.mark_closed("C", "TRAILING_STOP")
    gr_after = mon.compute_gross_return()
    assert mon.total_allocated_capital == cap     # denominator frozen
    assert gr_after < gr_before                    # numerator shrank only
    # numerator dropped by exactly C's uPnL (1000) / cap
    assert abs((gr_before - gr_after) - (1000.0 / cap)) < 1e-9


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
