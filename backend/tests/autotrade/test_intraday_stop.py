"""Tests for the per-stock software stop in _tick_intraday (Fix 3).

The per-stock stop fires BEFORE the portfolio trail engine when a single stock
falls more than config.stop_pct from its entry price. It places a market sell
directly (not via GTT), cancels the GTT for that stock first, and marks the
position CLOSED. Other positions are unaffected.

Covers:
  test_per_stock_stop_fires_before_gtt
      — stock at -1.6% from entry (> default stop_pct 1.5%) →
        backend places exit, does not wait for GTT at -3%

  test_per_stock_stop_does_not_double_fire
      — per-stock stop fires → next tick shows position CLOSED,
        no second attempt (exit_gate prevents double-fire)

  test_per_stock_stop_only_exits_the_one_stock
      — only the declining stock exits; other positions stay OPEN

  test_per_stock_stop_cancels_gtt
      — GTT cancel is attempted for the exited position

  test_per_stock_stop_uses_market_order
      — exit goes via broker.place_market_exit (not a GTT)

  test_portfolio_trail_still_runs_after_per_stock_exits
      — after per-stock exits the trail engine still runs on remaining positions
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.gtt_manager import GTTManager
from autotrade.session import _exit_single_position
from autotrade import exit_gate
from tests.autotrade.mock_broker import MockBroker


# ── helpers ────────────────────────────────────────────────────────────────────

def _sid():
    return uuid.uuid4().hex


def _make_session(sid, cap=500_000.0, strategy="intraday_basket",
                  stop_pct=0.015, invested_basis=None):
    from falcon.db import falcon_conn
    import json
    cfg_dict = {
        "total_allocated_capital": cap,
        "strategy": strategy,
        "stop_pct": stop_pct,
        "arm_pct": 0.01,
        "floor_pct": 0.01,
        "trail_giveback_pct": 0.0075,
        "square_off_time": "15:29:00",
    }
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                invested_basis, config_json, trail_armed, trail_peak)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (sid, "2026-06-28T09:00:00", "RUNNING", "paper", cap,
             invested_basis or cap, json.dumps(cfg_dict), 0, 0.0))
        con.commit()


def _make_broker(ltps=None, gtt_states=None):
    broker = MockBroker(
        profile=BrokerProfile("zer", "mock"),
        dry_run=False,
        ltps=ltps or {},
    )
    if gtt_states:
        broker.gtt_states.update(gtt_states)
    return broker


def _run(coro):
    """Run a coroutine synchronously — works on Python 3.10+ where
    asyncio.get_event_loop() in a non-async context is deprecated."""
    return asyncio.run(coro)


def _setup_session(sid, cap, symbols_ltps, avg_prices, stop_pct=0.015):
    """Register positions and arm the GTT manager."""
    reg = PositionRegistry(sid, cap)
    for sym, (avg, ltp) in zip(
            list(symbols_ltps.keys()) if isinstance(symbols_ltps, dict) else [],
            avg_prices.items() if isinstance(avg_prices, dict) else []):
        pass  # placeholder — see actual call below

    # Properly set up: symbols_ltps={sym: ltp}, avg_prices={sym: avg}
    reg = PositionRegistry(sid, cap)
    for sym in symbols_ltps:
        avg = avg_prices.get(sym, 100.0)
        reg.register(symbol=sym, broker_profile="zer", qty=100, avg_price=avg)
        reg.update_ltp(sym, symbols_ltps[sym])
    return reg


# ── _exit_single_position unit tests (module-level fn) ──────────────────────

class TestExitSinglePosition:

    def test_exit_single_marks_closed(self, clean_positions):
        """_exit_single_position places a market exit and marks position CLOSED."""
        sid = _sid()
        _make_session(sid, cap=200_000.0)
        reg = PositionRegistry(sid, 200_000.0)
        reg.register(symbol="DROPSTOCK", broker_profile="zer", qty=50,
                     avg_price=100.0)
        reg.update_ltp("DROPSTOCK", 83.0)  # -17%

        broker = _make_broker(ltps={"DROPSTOCK": 83.0})
        pos = reg.get_open_positions()[0]

        result = _run(_exit_single_position(
            session_id=sid,
            position=pos,
            reason="STOP_STOCK",
            brokers={"zer": broker},
            registry=reg,
            gtt_manager=None,
        ))

        assert result["status"] == "EXITED"
        all_pos = reg.get_all_positions()
        row = next(p for p in all_pos if p["symbol"] == "DROPSTOCK")
        assert row["status"] == "CLOSED"
        assert row["close_reason"] == "STOP_STOCK"

    def test_exit_single_uses_market_order(self, clean_positions):
        """Exit goes via broker.place_market_exit, not a GTT."""
        sid = _sid()
        _make_session(sid)
        reg = PositionRegistry(sid, 200_000.0)
        reg.register(symbol="MKTSTOCK", broker_profile="zer", qty=30,
                     avg_price=100.0)
        reg.update_ltp("MKTSTOCK", 80.0)

        broker = _make_broker(ltps={"MKTSTOCK": 80.0})
        pos = reg.get_open_positions()[0]

        _run(_exit_single_position(
            session_id=sid,
            position=pos,
            reason="STOP_STOCK",
            brokers={"zer": broker},
            registry=reg,
            gtt_manager=None,
        ))

        # Must have called place_market_exit (tracked in broker.exits).
        assert len(broker.exits) == 1
        assert broker.exits[0][0] == "MKTSTOCK"

    def test_exit_single_cancels_gtt(self, clean_positions):
        """_exit_single_position cancels the position's GTT before placing sell."""
        sid = _sid()
        _make_session(sid)
        reg = PositionRegistry(sid, 200_000.0)
        reg.register(symbol="GTTSTOCK", broker_profile="zer", qty=20,
                     avg_price=100.0)
        reg.update_ltp("GTTSTOCK", 85.0)
        reg.set_gtt("GTTSTOCK", "gtt-99", gtt_stop=97.0, gtt_target=106.0)

        broker = _make_broker(ltps={"GTTSTOCK": 85.0},
                              gtt_states={"gtt-99": {"status": "active"}})
        # GTTManager needed for cancel; pass as None → cancel skipped gracefully.
        # Test with a real GTTManager.
        cfg = TradingSessionConfig(total_allocated_capital=200_000.0,
                                   per_position_stop_pct=0.03,
                                   per_position_target_pct=0.06)
        gtt_mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
        pos = reg.get_open_positions()[0]

        _run(_exit_single_position(
            session_id=sid,
            position=pos,
            reason="STOP_STOCK",
            brokers={"zer": broker},
            registry=reg,
            gtt_manager=gtt_mgr,
        ))

        # cancel_gtt must have been called for the position's gtt_id.
        assert "gtt-99" in broker.cancelled_gtts, (
            "GTT must be cancelled before the market exit is placed")

    def test_exit_single_does_not_double_fire(self, clean_positions):
        """Exit gate blocks a concurrent DIFFERENT-reason claim on the same position.

        The per-stock stop claims exit with reason='STOP_STOCK'. A concurrent path
        (e.g. kill switch with 'KILL_SWITCH') finds the lock held and is blocked.
        This is the double-fire protection model: first reason wins.
        """
        sid = _sid()
        _make_session(sid)
        reg = PositionRegistry(sid, 200_000.0)
        reg.register(symbol="DOUBLESTOP", broker_profile="zer", qty=10,
                     avg_price=100.0)
        reg.update_ltp("DOUBLESTOP", 85.0)

        broker = _make_broker(ltps={"DOUBLESTOP": 85.0})
        pos = reg.get_open_positions()[0]

        # First exit (per-stock stop) — claims the lock.
        r1 = _run(_exit_single_position(
            session_id=sid, position=pos, reason="STOP_STOCK",
            brokers={"zer": broker}, registry=reg, gtt_manager=None))
        assert r1["status"] == "EXITED"
        assert len(broker.exits) == 1

        # Refresh position (now CLOSED, exit_lock=1).
        all_pos = reg.get_all_positions()
        pos_closed = next(p for p in all_pos if p["symbol"] == "DOUBLESTOP")

        # Concurrent DIFFERENT-reason attempt (e.g. kill switch) must be BLOCKED.
        # Using reason='KILL_SWITCH' — different from 'STOP_STOCK' → blocked.
        from autotrade import exit_gate as eg
        blocked = eg.claim_exit_session(sid, "DOUBLESTOP", "KILL_SWITCH")
        assert not blocked, (
            "A concurrent kill-switch claim must be BLOCKED when STOP_STOCK "
            "already owns the exit lock")

        # Only one market sell placed.
        assert len(broker.exits) == 1, (
            "broker.place_market_exit must only be called once")


# ── Integration test: _tick_intraday per-stock stop loop ─────────────────────

class TestTickIntradayPerStockStop:
    """Drive session._tick_intraday directly to verify the per-stock stop loop."""

    def _make_intraday_session(self, clean_positions_fixture, cap=300_000.0,
                               stop_pct=0.015):
        """Create a TradingSession with mock broker for intraday tests."""
        from autotrade.session import TradingSession
        from autotrade.config import TradingSessionConfig
        sid = _sid()
        invested = cap
        _make_session(sid, cap=cap, stop_pct=stop_pct, invested_basis=invested)
        cfg = TradingSessionConfig.from_json(
            f'{{"total_allocated_capital": {cap}, "strategy": "intraday_basket", '
            f'"stop_pct": {stop_pct}, "arm_pct": 0.01, "floor_pct": 0.01, '
            f'"trail_giveback_pct": 0.0075, "square_off_time": "15:29:00"}}')
        sess = TradingSession(sid, cfg, mode="paper")
        return sess, sid, cap

    def test_per_stock_stop_fires_before_gtt(self, clean_positions):
        """Stock at -1.6% exits via our software (not GTT at -3%)."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions)

        # Register position: avg=100, ltp=98.4 → return = -1.6% (> -1.5% stop).
        reg = PositionRegistry(sid, cap)
        reg.register(symbol="LOSER", broker_profile="zer", qty=100, avg_price=100.0)
        reg.update_ltp("LOSER", 98.4)
        # Register a healthy position to verify it stays open.
        reg.register(symbol="WINNER", broker_profile="zer", qty=100, avg_price=100.0)
        reg.update_ltp("WINNER", 103.0)

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"LOSER": 98.4, "WINNER": 103.0},
        )
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        # Compute gr_invested (LOSER -160, WINNER +300; basis = cap = 300k).
        gr_invested = sess.monitor.compute_gross_return_invested()
        snap = {"gross_return": 0.0}

        result = _run(sess._tick_intraday(gr_invested, snap, []))

        # The per-stock exits should include LOSER.
        per_exits = result.get("per_stock_exits", [])
        loser_exit = next((e for e in per_exits if e["symbol"] == "LOSER"), None)
        assert loser_exit is not None, (
            "LOSER should be in per_stock_exits (return -1.6% ≤ -1.5% stop)")
        assert loser_exit["status"] == "EXITED"
        # WINNER must stay OPEN.
        winner_exit = next((e for e in per_exits if e["symbol"] == "WINNER"), None)
        assert winner_exit is None, "WINNER should not be in per_stock_exits"

    def test_per_stock_stop_only_exits_the_one_stock(self, clean_positions):
        """Only the declining stock exits; other positions remain OPEN."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions)

        reg = PositionRegistry(sid, cap)
        reg.register(symbol="DROP", broker_profile="zer", qty=100, avg_price=100.0)
        reg.update_ltp("DROP", 98.0)  # -2% → exceeds -1.5% stop
        reg.register(symbol="HOLD_A", broker_profile="zer", qty=100, avg_price=100.0)
        reg.update_ltp("HOLD_A", 101.0)  # +1%: hold
        reg.register(symbol="HOLD_B", broker_profile="zer", qty=100, avg_price=100.0)
        reg.update_ltp("HOLD_B", 100.5)  # +0.5%: hold

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"DROP": 98.0, "HOLD_A": 101.0, "HOLD_B": 100.5},
        )
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        gr_invested = sess.monitor.compute_gross_return_invested()
        _run(sess._tick_intraday(gr_invested, {"gross_return": 0.0}, []))

        open_pos = reg.get_open_positions()
        open_syms = {p["symbol"] for p in open_pos}
        assert "DROP" not in open_syms, "DROP must have been closed by the stop"
        assert "HOLD_A" in open_syms, "HOLD_A must remain OPEN"
        assert "HOLD_B" in open_syms, "HOLD_B must remain OPEN"

    def test_per_stock_stop_does_not_double_fire(self, clean_positions):
        """Second tick on a CLOSED position is not re-exited (no double-fire)."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions)

        reg = PositionRegistry(sid, cap)
        reg.register(symbol="ONCESTOP", broker_profile="zer", qty=100,
                     avg_price=100.0)
        reg.update_ltp("ONCESTOP", 97.0)  # -3%

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"ONCESTOP": 97.0},
        )
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        # Tick 1: position is OPEN and below stop → exits.
        gr = sess.monitor.compute_gross_return_invested()
        _run(sess._tick_intraday(gr, {"gross_return": 0.0}, []))
        assert len(broker.exits) == 1

        # Tick 2: position is already CLOSED, exit_lock=1 → must not re-fire.
        gr2 = sess.monitor.compute_gross_return_invested()
        _run(sess._tick_intraday(gr2, {"gross_return": 0.0}, []))
        assert len(broker.exits) == 1, (
            "place_market_exit must only be called ONCE across two ticks")

    def test_per_stock_stop_cancels_gtt(self, clean_positions):
        """GTT is cancelled for the stopped position before the market sell."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions)

        reg = PositionRegistry(sid, cap)
        reg.register(symbol="GTTDROP", broker_profile="zer", qty=100,
                     avg_price=100.0)
        reg.update_ltp("GTTDROP", 98.0)  # -2%
        reg.set_gtt("GTTDROP", "gtt-stop-test", gtt_stop=97.0, gtt_target=106.0)

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"GTTDROP": 98.0},
        )
        broker.gtt_states["gtt-stop-test"] = {"status": "active"}
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        gr = sess.monitor.compute_gross_return_invested()
        _run(sess._tick_intraday(gr, {"gross_return": 0.0}, []))

        assert "gtt-stop-test" in broker.cancelled_gtts, (
            "GTT must be cancelled before the market exit for the stopped stock")
        assert len(broker.exits) == 1  # market sell also placed

    def test_portfolio_trail_still_runs_after_per_stock_exits(self, clean_positions):
        """After per-stock exits, the portfolio trail engine still evaluates
        the remaining positions (trail_action key is present in the result)."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions,
                                                       stop_pct=0.015)

        reg = PositionRegistry(sid, cap)
        reg.register(symbol="EXITED_STOCK", broker_profile="zer", qty=100,
                     avg_price=100.0)
        reg.update_ltp("EXITED_STOCK", 98.0)  # -2% → per-stock stop fires
        reg.register(symbol="REMAINING", broker_profile="zer", qty=100,
                     avg_price=100.0)
        reg.update_ltp("REMAINING", 100.5)

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"EXITED_STOCK": 98.0, "REMAINING": 100.5},
        )
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        gr = sess.monitor.compute_gross_return_invested()
        result = _run(sess._tick_intraday(gr, {"gross_return": 0.0}, []))

        # The trail engine must still have run (trail_action is always in the result).
        assert "trail_action" in result, (
            "trail_action must be present even after per-stock exits")
        # The per-stock exit must have fired.
        per_exits = result.get("per_stock_exits", [])
        assert any(e["symbol"] == "EXITED_STOCK" for e in per_exits)

    def test_per_stock_stop_does_not_fire_for_stock_within_stop_pct(
            self, clean_positions):
        """Stock at -1.4% (below -1.5% stop) must NOT be exited."""
        from autotrade.session import TradingSession
        sess, sid, cap = self._make_intraday_session(clean_positions, stop_pct=0.015)

        reg = PositionRegistry(sid, cap)
        reg.register(symbol="BARELY_OK", broker_profile="zer", qty=100,
                     avg_price=100.0)
        reg.update_ltp("BARELY_OK", 98.6)  # -1.4% → below stop (-1.5%)

        broker = MockBroker(
            profile=BrokerProfile("zer", "mock"),
            dry_run=False,
            ltps={"BARELY_OK": 98.6},
        )
        sess.brokers = {"zer": broker}
        sess.registry = reg
        sess.monitor = PortfolioMonitor(sid, cap)
        sess.gtt_manager = GTTManager(sid, sess.config, {"zer": broker}, reg)
        sess.kill_switch = KillSwitchExecutor(sid, sess.config, {"zer": broker}, reg,
                                              gtt_manager=sess.gtt_manager)

        gr = sess.monitor.compute_gross_return_invested()
        result = _run(sess._tick_intraday(gr, {"gross_return": 0.0}, []))

        # No per-stock stop should have fired.
        per_exits = result.get("per_stock_exits", [])
        assert len(per_exits) == 0, (
            f"No stop should fire for -1.4% when stop_pct=1.5%. "
            f"Got per_exits={per_exits}")
        # Position should still be OPEN.
        assert len(reg.get_open_positions()) == 1
