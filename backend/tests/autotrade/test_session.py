"""End-to-end DRY-RUN session smoke + addendum full-day style sequence.

Patches broker.router.build_client to return MockBrokers so no real Kite is
touched, then drives create → start → tick → kill entirely in paper mode.
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade import exit_gate
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False,
                        ltps={"A": 100.0, "B": 200.0, "C": 50.0,
                              "D": 150.0, "E": 300.0})
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    # session.py imported build_client by name → patch there too.
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_full_paper_session(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0), ("D", 4, 6.0, 150.0),
                  ("E", 5, 5.0, 300.0)])
    cfg = TradingSessionConfig(total_allocated_capital=500000.0, top_n_stocks=5,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 5
    st = sess.status()
    assert st["n_open_positions"] == 5
    assert st["total_allocated_capital"] == 500000.0


def test_paper_kill_switch_fires_on_profit(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Move LTPs up so gross return > 0.5%.
    for mb in patched_brokers.values():
        mb.set_ltp("A", 120.0)
        mb.set_ltp("B", 240.0)
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    # After kill, positions marked exited.
    assert sess.status()["n_open_positions"] == 0


def test_manual_kill(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    res = asyncio.run(sess.kill(reason="OPERATOR"))
    assert res["n_positions"] >= 1
    assert sess.status()["n_open_positions"] == 0


def test_trailing_stop_then_kill_denominator(clean_positions, patched_brokers):
    """Addendum: trailing stop on one stock, kill switch keeps monitoring the
    reduced portfolio; denominator stays total_allocated_capital."""
    seed_signals([("A", 1, 9, 100.0), ("B", 2, 8, 200.0), ("C", 3, 7, 50.0),
                  ("D", 4, 6, 150.0), ("E", 5, 5, 300.0)])
    cap = 500000.0
    cfg = TradingSessionConfig(total_allocated_capital=cap, top_n_stocks=5,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.5)  # high → won't auto-fire
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Simulate a trailing-stop exit on C via the shared exit gate path.
    assert exit_gate.claim_exit("C", "TRAILING_STOP") is True
    sess.registry.mark_closed("C", "TRAILING_STOP")
    # Denominator unchanged.
    assert sess.monitor.total_allocated_capital == cap
    assert sess.status()["n_open_positions"] == 4
