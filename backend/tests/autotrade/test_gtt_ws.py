"""Tests for the three real-money-execution AutoTrade features (mocks only):

FEATURE 1 — per-position GTT-OCO broker backup:
  * computed stop/target levels from per_position_*_pct,
  * GTT placed in LIVE, NOT in PAPER (levels still recorded in paper).
FEATURE 2 — WebSocket-driven portfolio exit:
  * a WS tick crossing the threshold fires the flatten ONCE; the per-session
    fire guard prevents the poll from double-firing.
FEATURE 3 — coordination:
  * the kill-switch flatten cancels each position's GTT,
  * a fired GTT (mock position gone) → row CLOSED (close_reason='GTT') +
    gross_return recomputed on the remainder (frozen denominator),
  * boot-resume backfills missing GTTs for LIVE positions (paper skipped).

NO real orders / GTTs are ever placed — every broker is a MockBroker.
"""
import asyncio
import uuid

import pytest

import autotrade.broker.router as router_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.monitor import PortfolioMonitor
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.gtt_manager import GTTManager, compute_levels
from autotrade.monitoring import fire_guard, ws_driver
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker


def _sid():
    return uuid.uuid4().hex


def _make_session_row(session_id, cap, mode="paper", status="RUNNING"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json)
               VALUES (?,?,?,?,?,?)""",
            (session_id, "2026-06-24T09:00:00", status, mode, cap, "{}"))
        con.commit()


@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 150.0, "E": 300.0}

    def fake_build_client(profile, dry_run=True):
        # Honour dry_run so paper vs live behaviour is exercised correctly.
        mb = MockBroker(profile=profile, dry_run=dry_run, ltps=shared)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


# ── FEATURE 1: level computation ─────────────────────────────────────────────

def test_compute_levels_from_pct():
    stop_trig, stop_lim, tgt_trig, tgt_lim = compute_levels(100.0, 0.03, 0.06)
    assert stop_trig == 97.0
    assert tgt_trig == 106.0
    # stop_limit must be below trigger (slippage buffer applied).
    assert stop_lim < stop_trig
    # target limit should equal trigger (no buffer needed for target leg).
    assert tgt_lim == tgt_trig
    # wider widths than a 1.2% portfolio kill switch → portfolio fires first.
    assert (1.0 - stop_trig / 100.0) > 0.012
    assert (tgt_trig / 100.0 - 1.0) > 0.012


# ── FEATURE 1: GTT placed in LIVE, levels recorded; NOT placed in PAPER ───────

def test_gtt_placed_in_live_recorded_in_paper(clean_positions):
    cap = 500000.0
    cfg = TradingSessionConfig(total_allocated_capital=cap,
                               per_position_stop_pct=0.03,
                               per_position_target_pct=0.06)

    # LIVE: broker (dry_run=False) → real GTT placed + id stored.
    sid_live = _sid()
    _make_session_row(sid_live, cap, mode="live")
    reg = PositionRegistry(sid_live, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 100.0)
    live_broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                             ltps={"A": 100.0})
    mgr = GTTManager(sid_live, cfg, {"zer": live_broker}, reg)
    res = mgr.backfill_missing()
    assert len(res) == 1 and res[0]["status"] == "PLACED"
    assert len(live_broker.gtts) == 1
    g = live_broker.gtts[0]
    assert g["stop"] == 97.0 and g["target"] == 106.0
    row = reg.get_open_positions()[0]
    assert row["gtt_id"] == g["gtt_id"]
    assert row["gtt_stop"] == 97.0 and row["gtt_target"] == 106.0

    # PAPER: broker (dry_run=True) → NO real GTT; levels still recorded.
    sid_paper = _sid()
    _make_session_row(sid_paper, cap, mode="paper")
    reg2 = PositionRegistry(sid_paper, cap)
    reg2.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg2.update_ltp("A", 100.0)
    paper_broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                              ltps={"A": 100.0})
    mgr2 = GTTManager(sid_paper, cfg, {"zer": paper_broker}, reg2)
    res2 = mgr2.backfill_missing()
    assert len(res2) == 1 and res2[0]["status"] == "RECORDED_ONLY"
    assert paper_broker.gtts == []                 # NO real GTT in paper
    row2 = reg2.get_open_positions()[0]
    assert row2["gtt_id"] is None                  # no broker id
    assert row2["gtt_stop"] == 97.0 and row2["gtt_target"] == 106.0  # levels shown


# ── FEATURE 1: session start places GTTs (live) ──────────────────────────────

def test_session_start_places_gtts_live(clean_positions, patched_brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    res = asyncio.run(sess.start())
    assert res["status"] == "RUNNING"
    # Every open position got a real GTT in live mode.
    rows = sess.registry.get_open_positions()
    assert len(rows) == 2
    for r in rows:
        assert r["gtt_id"] is not None
        assert r["gtt_stop"] is not None and r["gtt_target"] is not None


# ── FEATURE 2: WS tick crossing threshold fires once; poll can't double-fire ──

def test_ws_tick_fires_once_guarded(clean_positions):
    sid = _sid()
    cap = 100000.0
    _make_session_row(sid, cap, mode="paper")
    reg = PositionRegistry(sid, cap)
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.update_ltp("A", 100.0)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=True,
                        ltps={"A": 100.0})
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)

    # WS path observes a tick that crosses +0.5% and wins the fire guard.
    reg.update_ltp("A", 110.0)   # +1000 uPnL / 100000 = +1% > 0.5%
    mon = PortfolioMonitor(sid, cap)
    gr = mon.compute_gross_return()
    assert ks.check_threshold(gr) is not None

    fires = []
    with fire_guard.claim_fire(sid) as won:
        assert won is True
        fires.append(asyncio.run(ks.fire("WS_TEST", gross_return=gr)))
    # The 5s poll then sees the same crossing — but the guard blocks it.
    with fire_guard.claim_fire(sid) as won2:
        assert won2 is False         # already fired → poll backs off
    assert len(fires) == 1
    assert fires[0]["n_exited_ok"] == 1


def test_ws_driver_tick_once_fires(clean_positions, patched_brokers):
    """The _WSDriver's own _tick_once pulls an injected WS LTP, recomputes, and
    fires the flatten when the threshold is crossed (paper — no real orders)."""
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.005, kill_switch_direction="profit")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    assert sess.status()["n_open_positions"] == 2

    # Injected sub-second LTP source returns a price that crosses the threshold.
    live = {"A": 130.0, "B": 260.0}
    drv = ws_driver._WSDriver(sess.session_id, 0.25,
                              ltp_source=lambda s: live.get(s))
    inner = TradingSession.load(sess.session_id)
    inner._build_brokers()
    drv._tick_once(inner, fire_guard)

    from falcon.db import falcon_conn
    with falcon_conn() as con:
        status = con.execute(
            "SELECT status FROM autotrade_sessions WHERE session_id=?",
            (sess.session_id,)).fetchone()["status"]
        n_log = con.execute(
            "SELECT COUNT(*) FROM autotrade_kill_switch_log WHERE session_id=?",
            (sess.session_id,)).fetchone()[0]
    assert status == "CLOSED"
    assert n_log >= 1


# ── FEATURE 3: kill-switch flatten cancels GTTs ──────────────────────────────

def test_kill_switch_cancels_gtts(clean_positions):
    sid = _sid()
    cap = 500000.0
    _make_session_row(sid, cap, mode="live")
    reg = PositionRegistry(sid, cap)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 200.0})
    for s, a in (("A", 100.0), ("B", 200.0)):
        reg.register(symbol=s, broker_profile="zer", qty=10, avg_price=a)
        reg.update_ltp(s, a)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()
    placed_ids = {g["gtt_id"] for g in broker.gtts}
    assert len(placed_ids) == 2

    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg, gtt_manager=mgr)
    res = asyncio.run(ks.fire("TEST"))
    # Both positions flattened AND both GTTs cancelled (no orphans).
    assert res["n_exited_ok"] == 2
    assert set(broker.cancelled_gtts) == placed_ids
    assert len(res["gtt_cancelled"]) == 2


def test_kill_switch_flatten_survives_gtt_cancel_failure(clean_positions):
    """A GTT-cancel failure must NOT block the flatten."""
    sid = _sid()
    cap = 500000.0
    _make_session_row(sid, cap, mode="live")
    reg = PositionRegistry(sid, cap)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0})
    reg.register(symbol="A", broker_profile="zer", qty=10, avg_price=100.0)
    reg.update_ltp("A", 100.0)
    cfg = TradingSessionConfig(total_allocated_capital=cap, kill_switch_enabled=True)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()

    # Make cancel_gtt raise.
    def boom(_):
        raise RuntimeError("kite down")
    broker.cancel_gtt = boom

    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg, gtt_manager=mgr)
    res = asyncio.run(ks.fire("TEST"))
    assert res["n_exited_ok"] == 1            # flatten still succeeded
    assert ("A", 10) in broker.exits


# ── FEATURE 3: a fired GTT → row CLOSED + gross recomputed on remainder ───────

def test_gtt_fill_marks_closed_and_recomputes(clean_positions):
    sid = _sid()
    cap = 100000.0
    _make_session_row(sid, cap, mode="live")
    reg = PositionRegistry(sid, cap)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0, "B": 200.0})
    reg.register(symbol="A", broker_profile="zer", qty=100, avg_price=100.0)
    reg.register(symbol="B", broker_profile="zer", qty=50, avg_price=200.0)
    reg.update_ltp("A", 110.0)   # +1000
    reg.update_ltp("B", 210.0)   # +500
    cfg = TradingSessionConfig(total_allocated_capital=cap)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()
    mon = PortfolioMonitor(sid, cap)
    gr_before = mon.compute_gross_return()
    assert abs(gr_before - (1500.0 / cap)) < 1e-9

    # Simulate A's GTT firing at the broker (position sold externally).
    a_gtt = next(g["gtt_id"] for g in broker.gtts if g["symbol"] == "A")
    broker.fire_gtt(a_gtt)

    closed = mgr.reconcile_gtt_fills()
    assert len(closed) == 1 and closed[0]["symbol"] == "A"
    # A is CLOSED with reason GTT; B still OPEN.
    opens = {p["symbol"] for p in reg.get_open_positions()}
    assert opens == {"B"}
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status, close_reason FROM autotrade_positions "
            "WHERE session_id=? AND symbol='A'", (sid,)).fetchone()
    assert row["status"] == "CLOSED" and row["close_reason"] == "GTT"
    # gross recomputes on the REMAINDER (B open) + CLOSED A realised P&L.
    # Fix-1: compute_gross_return now includes realised_pnl from CLOSED rows.
    # A fired at broker.ltps["A"] = 100.0 (MockBroker's fire_gtt fills at ltps,
    # not at the updated reg ltp). So realised = (100-100)*100 = 0.
    # B still open: uPnL = (210-200)*50 = +500.
    # Total PnL = 0 + 500 = 500; denominator = cap.
    gr_after = mon.compute_gross_return()
    assert mon.total_allocated_capital == cap
    assert abs(gr_after - (500.0 / cap)) < 1e-9, (
        f"Expected 500/{cap:.0f}={500.0/cap:.6f}, got {gr_after:.6f}. "
        "A was filled at broker.ltps=100 (avg=100) → realised=0; "
        "only B open uPnL (+500) flows through.")
    assert gr_after < gr_before


def test_reconcile_noop_when_gtt_active_or_missing(clean_positions):
    sid = _sid()
    cap = 100000.0
    _make_session_row(sid, cap, mode="live")
    reg = PositionRegistry(sid, cap)
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0})
    reg.register(symbol="A", broker_profile="zer", qty=10, avg_price=100.0)
    reg.update_ltp("A", 100.0)
    cfg = TradingSessionConfig(total_allocated_capital=cap)
    mgr = GTTManager(sid, cfg, {"zer": broker}, reg)
    mgr.backfill_missing()
    # GTT still active → no close.
    assert mgr.reconcile_gtt_fills() == []
    assert {p["symbol"] for p in reg.get_open_positions()} == {"A"}


# ── FEATURE 3: boot-resume backfills missing GTTs for LIVE positions ──────────

def test_recovery_backfills_missing_gtts_live(clean_positions, patched_brokers):
    from autotrade import recovery
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    asyncio.run(sess.start())
    # Simulate positions that pre-date the feature: clear their gtt_id.
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET gtt_id=NULL, gtt_stop=NULL, "
            "gtt_target=NULL WHERE session_id=?", (sess.session_id,))
        con.commit()
    assert len(sess.registry.get_open_positions_missing_gtt()) == 2

    recovery.resume_active_sessions()

    # All live positions now have a GTT backfilled.
    assert sess.registry.get_open_positions_missing_gtt() == []
    for r in sess.registry.get_open_positions():
        assert r["gtt_id"] is not None


def test_recovery_skips_gtt_backfill_for_paper(clean_positions, patched_brokers):
    from autotrade import recovery
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start())
    # Paper: levels recorded but gtt_id is None. Clear everything to simulate
    # pre-feature, then resume — paper must NOT place a real GTT (id stays None).
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_positions SET gtt_id=NULL, gtt_stop=NULL, "
            "gtt_target=NULL WHERE session_id=?", (sess.session_id,))
        con.commit()

    recovery.resume_active_sessions()

    for r in sess.registry.get_open_positions():
        assert r["gtt_id"] is None    # paper: no real GTT placed on resume
