"""RMS CAP 2 — portfolio daily-loss circuit breaker + global kill-all.

Proves:
  * aggregate_daily_pnl / check_portfolio_breaker sum realised+unrealised across
    ALL of a user's live sessions and fire when the aggregate loss breaches the
    tightest configured limit.
  * global_kill flattens every live session for a user in one sweep.
  * The breaker fires AUTOMATICALLY from session.tick() when two sessions'
    combined loss crosses the limit → both flatten in one sweep.

MUTATION-VERIFIED (test_tick_breaker_flattens_all_sessions):
  Revert = in risk_manager.check_portfolio_breaker change
      breached = pnl <= -abs(limit)
  to
      breached = False
  (disable the breach check). Then tick() never fires the breaker → both sessions
  stay RUNNING with open positions → this test's assert (both flattened) fails.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import risk_manager
from autotrade.config import TradingSessionConfig
from autotrade.monitoring.registry import PositionRegistry
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _row(sid, cap, cfg: TradingSessionConfig, user_id="u1",
         status="RUNNING", mode="live"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json, user_id, invested_basis)
               VALUES (?,?,?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", status, mode, cap, cfg.to_json(),
             user_id, cap))
        con.commit()


def _pos(sid, symbol, qty, avg, ltp):
    reg = PositionRegistry(sid, 0)
    reg.register(symbol=symbol, broker_profile="zer", qty=qty, avg_price=avg,
                 product="CNC")
    reg.update_ltp(symbol, ltp)


# ── aggregate P&L + breaker decision ─────────────────────────────────────────

def test_aggregate_and_breaker_decision(clean_positions):
    a = TradingSessionConfig(total_allocated_capital=500000.0,
                             max_daily_loss_amount=10000.0)
    b = TradingSessionConfig(total_allocated_capital=500000.0)  # no limit
    _row("A", 500000.0, a)
    _row("B", 500000.0, b)
    _pos("A", "X", 100, 100.0, 92.0)   # -800
    _pos("B", "Y", 100, 100.0, 88.0)   # -1200
    assert risk_manager.aggregate_daily_pnl(["A", "B"]) == pytest.approx(-2000.0)
    # Below the ₹10k limit → not breached.
    dec = risk_manager.check_portfolio_breaker("u1", mode="live")
    assert dec.breached is False
    assert dec.limit_rs == pytest.approx(10000.0)
    # Deepen the loss past the limit.
    _pos("A", "X", 100, 100.0, 20.0)   # -8000
    _pos("B", "Y", 100, 100.0, 70.0)   # -3000  → aggregate -11000
    dec2 = risk_manager.check_portfolio_breaker("u1", mode="live")
    assert dec2.aggregate_pnl == pytest.approx(-11000.0)
    assert dec2.breached is True


def test_breaker_disabled_without_limit(clean_positions):
    b = TradingSessionConfig(total_allocated_capital=500000.0)   # no limit
    _row("B", 500000.0, b)
    _pos("B", "Y", 100, 100.0, 1.0)   # catastrophic -9900
    dec = risk_manager.check_portfolio_breaker("u1", mode="live")
    assert dec.breached is False
    assert dec.limit_rs is None


# ── global_kill flattens every live session ──────────────────────────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    ltps = {"A": 100.0, "B": 200.0}

    def fake_build_client(profile, dry_run=True):
        mb = created.get(profile.profile_id)
        if mb is None:
            mb = MockBroker(profile=profile, dry_run=False, ltps=ltps)
            created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _status(sid):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_sessions WHERE session_id=?",
                        (sid,)).fetchone()
    return r["status"] if r else None


def test_global_kill_flattens_all(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    c1 = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                              sizing_mode="equal", kill_switch_enabled=False)
    c2 = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                              sizing_mode="equal", kill_switch_enabled=False)
    s1 = TradingSession.create(c1, mode="paper", user_id="u1")
    s2 = TradingSession.create(c2, mode="paper", user_id="u1")
    asyncio.run(s1.start(when="now"))
    asyncio.run(s2.start(when="now"))
    assert _status(s1.session_id) == "RUNNING"
    assert _status(s2.session_id) == "RUNNING"
    out = asyncio.run(risk_manager.global_kill("u1"))
    assert out["n_killed"] == 2
    assert _status(s1.session_id) == "CLOSED"
    assert _status(s2.session_id) == "CLOSED"


# ── AUTOMATIC breaker from tick() flattens both sessions (MUTATION proof) ─────

def test_tick_breaker_flattens_all_sessions(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0)])
    # A opts in with a tight ₹5k daily-loss limit; B has no limit but is in the
    # same user's live cohort → the breaker flattens BOTH.
    ca = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                              sizing_mode="equal", kill_switch_enabled=False,
                              max_daily_loss_amount=5000.0)
    cb = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                              sizing_mode="equal", kill_switch_enabled=False)
    sa = TradingSession.create(ca, mode="paper", user_id="u1")
    sb = TradingSession.create(cb, mode="paper", user_id="u1")
    asyncio.run(sa.start(when="now"))
    asyncio.run(sb.start(when="now"))
    # Both sessions share the cached zerodha_default MockBroker; drop A's price to
    # 90 so a's tick refresh marks its 1000-share @100 position to -10,000 (well
    # past the ₹5k limit). A's tick evaluates the breaker → flattens the cohort.
    patched_brokers["zerodha_default"].set_ltp("A", 90.0)
    out = asyncio.run(sa.tick())
    assert out.get("kill_reason") == "PORTFOLIO_DAILY_LOSS_BREAKER"
    assert _status(sa.session_id) == "CLOSED"
    assert _status(sb.session_id) == "CLOSED"
