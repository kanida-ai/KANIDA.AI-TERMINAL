"""Invested (notional) capital basis — the product-aware kill basis.

Covers:
  * invested_basis = Σ(qty*avg_price), FROZEN at entry (does NOT change when a
    position later closes),
  * compute_gross_return_invested() = uPnL / invested_basis (vs the on-fund
    compute_gross_return()),
  * the kill switch fires on the INVESTED-basis return crossing kill_switch_pct,
  * kill_preview math (₹ on the invested basis + equivalent % on the fund),
  * /autotrade/preview returns sizing estimates WITHOUT creating a session or
    placing any order,
  * MTF leverage makes invested_basis > total_allocated_capital.

All paper / dry-run — MockBroker only, no real Kite, no real orders.
"""
import asyncio

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, preview_session_sizing
from autotrade.monitoring.monitor import PortfolioMonitor, compute_kill_preview
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.api import autotrade_routes as routes
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


# ── helpers ───────────────────────────────────────────────────────────────────

def _session_id():
    import uuid
    return uuid.uuid4().hex


def _make_session_row(session_id, cap, invested_basis=None):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                invested_basis, config_json)
               VALUES (?,?,?,?,?,?,?)""",
            (session_id, "2026-06-24T09:00:00", "RUNNING", "paper", cap,
             invested_basis, "{}"))
        con.commit()


# ── 1. invested_basis = Σ(qty*avg_price), frozen, fund fallback ───────────────

def test_invested_basis_sum_and_freeze(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)  # 10000
    reg.register(symbol="B", broker_profile="zer", qty=50, avg_price=200.0)   # 10000
    mon = PortfolioMonitor(sid, cap)
    frozen = mon.freeze_invested_basis()
    assert abs(frozen - 20000.0) < 1e-6
    assert abs(mon.invested_basis() - 20000.0) < 1e-6


def test_invested_basis_falls_back_to_fund_when_no_positions(clean_positions):
    sid = _session_id()
    cap = 300000.0
    _make_session_row(sid, cap)   # invested_basis NULL, no positions
    mon = PortfolioMonitor(sid, cap)
    # No positions → freeze stores fund capital so we never div-by-zero.
    assert abs(mon.freeze_invested_basis() - cap) < 1e-6
    assert abs(mon.invested_basis() - cap) < 1e-6


def test_invested_basis_frozen_does_not_shrink_on_close(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    for s, q, a in [("A", 100, 100), ("B", 100, 100), ("C", 100, 100)]:
        reg.register(symbol=s, broker_profile="zer", qty=q, avg_price=a)
    mon = PortfolioMonitor(sid, cap)
    frozen = mon.freeze_invested_basis()
    assert abs(frozen - 30000.0) < 1e-6
    # A position closes — invested_basis must NOT change.
    reg.mark_closed("C", "TRAILING_STOP")
    assert abs(mon.invested_basis() - 30000.0) < 1e-6


# ── 2. gross_return_invested vs on-fund gross_return ──────────────────────────

def test_gross_return_invested_formula(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)  # ib 10000
    reg.update_ltp("A", 110.0)   # +1000 uPnL
    mon = PortfolioMonitor(sid, cap)
    mon.freeze_invested_basis()
    # invested basis: 1000 / 10000 = 0.10
    assert abs(mon.compute_gross_return_invested() - 0.10) < 1e-9
    # on-fund: 1000 / 500000 = 0.002
    assert abs(mon.compute_gross_return() - 0.002) < 1e-9


# ── 3. kill switch fires on the INVESTED-basis crossing ───────────────────────

def test_kill_fires_on_invested_basis_via_tick(clean_positions, monkeypatch):
    """uPnL is tiny vs the fund but large vs invested_basis → kill must fire on
    the invested basis. Proves the kill check uses gross_return_invested."""
    # Patch broker construction so session.tick uses MockBrokers (no real Kite).
    shared_ltps = {"A": 110.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    sid = _session_id()
    cap = 1_000_000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zerodha_default", qty=100,
                 avg_price=100.0)             # invested_basis = 10000
    reg.update_ltp("A", 110.0)                # +1000 uPnL
    mon = PortfolioMonitor(sid, cap)
    mon.freeze_invested_basis()

    # On the fund: 1000/1e6 = 0.001 (below 1.2% → would NOT fire).
    assert mon.compute_gross_return() < 0.012
    # On invested basis: 1000/10000 = 0.10 (above 1.2% → MUST fire).
    assert mon.compute_gross_return_invested() >= 0.012

    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               kill_switch_enabled=True, kill_switch_pct=0.012,
                               kill_switch_direction="both")
    sess = TradingSession(sid, cfg, mode="paper")
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "PROFIT_TARGET" in (out["kill_reason"] or "")
    # tick reports the invested-basis gross as the headline gross_return.
    assert out["gross_return"] >= 0.012
    assert out["gross_return_fund"] < 0.012


def test_kill_does_not_fire_when_only_fund_basis_would(clean_positions):
    """Sanity: with kill_switch_pct set so the FUND return is below it but the
    invested return is also below it, no fire — and vice versa covered above."""
    sid = _session_id()
    cap = 1_000_000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 100.5)   # +50 uPnL → 50/10000 = 0.005 invested
    mon = PortfolioMonitor(sid, cap)
    mon.freeze_invested_basis()
    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               kill_switch_enabled=True, kill_switch_pct=0.012,
                               kill_switch_direction="both")
    ks = KillSwitchExecutor(sid, cfg, {}, reg)
    assert ks.check_threshold(mon.compute_gross_return_invested()) is None


# ── 4. kill_preview math (₹ on invested basis + fund_pct) ─────────────────────

def test_kill_preview_both_directions():
    pv = compute_kill_preview(
        kill_switch_enabled=True, kill_switch_pct=0.012,
        kill_switch_direction="both", invested_basis=20000.0,
        total_allocated_capital=500000.0)
    # target ₹ = 0.012 * 20000 = 240; fund_pct = 240/500000 = 0.00048
    assert abs(pv["target"]["basis_value_rs"] - 240.0) < 1e-9
    assert abs(pv["target"]["fund_pct"] - (240.0 / 500000.0)) < 1e-12
    assert pv["target"]["pct"] == 0.012
    # stop ₹ = -240; fund_pct negative
    assert abs(pv["stop"]["basis_value_rs"] + 240.0) < 1e-9
    assert pv["stop"]["pct"] == -0.012
    assert pv["stop"]["fund_pct"] < 0


def test_kill_preview_profit_only_has_no_stop():
    pv = compute_kill_preview(
        kill_switch_enabled=True, kill_switch_pct=0.02,
        kill_switch_direction="profit", invested_basis=10000.0,
        total_allocated_capital=100000.0)
    assert "target" in pv and "stop" not in pv


def test_kill_preview_loss_only_has_no_target():
    pv = compute_kill_preview(
        kill_switch_enabled=True, kill_switch_pct=0.02,
        kill_switch_direction="loss", invested_basis=10000.0,
        total_allocated_capital=100000.0)
    assert "stop" in pv and "target" not in pv


def test_kill_preview_none_when_disabled():
    assert compute_kill_preview(
        kill_switch_enabled=False, kill_switch_pct=0.02,
        kill_switch_direction="both", invested_basis=10000.0,
        total_allocated_capital=100000.0) is None


def test_kill_preview_falls_back_to_fund_when_basis_zero():
    # invested_basis 0 → use fund as the basis so the ₹ figure is non-zero.
    pv = compute_kill_preview(
        kill_switch_enabled=True, kill_switch_pct=0.01,
        kill_switch_direction="both", invested_basis=0.0,
        total_allocated_capital=100000.0)
    assert abs(pv["target"]["basis_value_rs"] - 1000.0) < 1e-9


# ── 5. status() returns both bases + invested_basis + kill_preview ────────────

def test_status_exposes_both_bases_and_preview(clean_positions):
    sid = _session_id()
    cap = 500000.0
    _make_session_row(sid, cap)
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 110.0)   # +1000 uPnL
    mon = PortfolioMonitor(sid, cap)
    mon.freeze_invested_basis()
    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               kill_switch_enabled=True, kill_switch_pct=0.012,
                               kill_switch_direction="both")
    sess = TradingSession(sid, cfg, mode="paper")
    st = sess.status()
    assert abs(st["invested_basis"] - 10000.0) < 1e-6
    assert abs(st["gross_return"] - 0.10) < 1e-9          # kill basis
    assert abs(st["gross_return_fund"] - 0.002) < 1e-9    # on-fund
    assert st["total_allocated_capital"] == cap
    assert st["kill_preview"] is not None
    assert "target" in st["kill_preview"] and "stop" in st["kill_preview"]


# ── 6. /preview sizes WITHOUT creating a session or placing orders ────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)


def _count_sessions():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return con.execute("SELECT COUNT(*) FROM autotrade_sessions").fetchone()[0]


def _count_positions():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        return con.execute("SELECT COUNT(*) FROM autotrade_positions").fetchone()[0]


def test_preview_returns_estimates_without_session(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    before_sessions = _count_sessions()
    cfg_dict = {"total_allocated_capital": 300000.0, "top_n_stocks": 3,
                "sizing_mode": "equal", "kill_switch_enabled": True,
                "kill_switch_pct": 0.012, "kill_switch_direction": "both"}
    res = routes.autotrade_preview(routes.PreviewRequest(config=cfg_dict))

    # No session row, no positions created.
    assert _count_sessions() == before_sessions
    assert _count_positions() == 0

    # Each leg sized at 100000 (equal): A=1000sh, B=500sh, C=2000sh.
    # invested = 1000*100 + 500*200 + 2000*50 = 100000+100000+100000 = 300000.
    assert abs(res["invested_basis"] - 300000.0) < 1e-6
    assert res["total_allocated_capital"] == 300000.0
    assert res["n_positions"] == 3
    assert res["kill_preview"] is not None
    # target ₹ = 0.012 * 300000 = 3600.
    assert abs(res["kill_preview"]["target"]["basis_value_rs"] - 3600.0) < 1e-6


def test_preview_mtf_leverage_above_one(clean_positions, monkeypatch):
    """Under MTF the qty is sized off per-share MARGIN, so invested_basis (qty *
    LTP) exceeds the fund → leverage > 1."""
    seed_signals([("A", 1, 9.0, 100.0)])

    class MTFBroker(MockBroker):
        def get_margin_per_share(self, symbol, product="MTF"):
            return 25.0   # 25% margin → ~4x leverage

    def fake_build_client(profile, dry_run=True):
        return MTFBroker(profile=profile, dry_run=False, ltps={"A": 100.0})

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", order_product="MTF",
                               instrument_type="MTF", kill_switch_enabled=False)
    res = preview_session_sizing(cfg, mode="paper")
    # amount 100000 / margin 25 = 4000 shares; invested = 4000*100 = 400000.
    assert abs(res["invested_basis"] - 400000.0) < 1e-6
    assert abs(res["leverage"] - 4.0) < 1e-6
    assert _count_positions() == 0
