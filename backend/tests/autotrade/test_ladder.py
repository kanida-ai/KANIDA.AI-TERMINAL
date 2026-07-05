"""FALCON POSITIONAL AUTO-LADDER ORCHESTRATOR — campaign-layer tests.

Covers (per the operator-approved spec):
  * per-basket sizing = total / 3 (frozen at create); MIS rejected; only CNC|MTF.
  * daily tick OPENS a child when free, SKIPS when full (3 open); idempotent/day.
  * free-capital math on the MARGIN basis, incl. an MTF child whose leveraged
    notional (invested_basis) is > capital does NOT inflate the ceiling.
  * PAUSED / ENDED / COMPLETED open nothing.
  * KILL flatten_now flattens every open child; stop_new_let_finish leaves them.
  * month-end stops new + auto-completes when the last child closes.
  * 5-day-avg realized-return alert fires ONCE on the down-crossing.
  * restart-durability: state is re-derived from the persisted rows.

All paper-safe (MockBroker, no real orders); no falcon_position_state writes.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
import autotrade.ladder as ladder_mod
from autotrade.ladder import (
    LadderCampaign, STATUS_RUNNING, STATUS_PAUSED, STATUS_ENDED,
    STATUS_COMPLETED, KILL_FLATTEN_NOW, KILL_STOP_NEW_LET_FINISH,
)
from falcon.db import falcon_conn
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))


# ── shared broker patch (children fire through the session engine) ────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0, "D": 40.0, "E": 60.0}
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _basket_signals():
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0), ("D", 4, 6.0, 40.0),
                  ("E", 5, 5.0, 60.0)])


def _trading_day_now():
    # Thu 2026-06-25 10:00 IST — a real NSE trading day, market open. (This is
    # the conftest default frozen clock too.)
    return datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


# ── per-basket sizing + create validation ─────────────────────────────────────

def test_per_basket_is_total_over_three(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0, order_product="CNC")
    assert lad.per_basket_capital == pytest.approx(300000.0)
    assert lad.status == STATUS_RUNNING
    # persisted frozen value
    got = LadderCampaign.load(lad.ladder_id)
    assert got.per_basket_capital == pytest.approx(300000.0)


def test_create_rejects_mis(clean_positions):
    with pytest.raises(ValueError) as e:
        LadderCampaign.create(total_capital=300000.0, order_product="MIS")
    assert "MIS" in str(e.value) or "overnight" in str(e.value)


def test_create_rejects_unknown_product(clean_positions):
    with pytest.raises(ValueError):
        LadderCampaign.create(total_capital=300000.0, order_product="NRML")


def test_create_accepts_mtf(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0, order_product="MTF")
    assert lad.order_product == "MTF"


def test_create_manual_end_date_is_null(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0, end_date_mode="manual")
    assert lad.end_date is None


def test_create_month_end_lands_on_trading_day(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0)  # month_end default
    ed = datetime.strptime(lad.end_date, "%Y-%m-%d").date()
    from autotrade import trading_calendar as cal
    assert cal.is_trading_day(ed)


# ── free-capital math (MARGIN basis) ──────────────────────────────────────────

def _insert_child(ladder_id, *, status, capital, invested_basis=None,
                  started_at="2026-06-25T09:15:00+05:30", closed_at=None):
    """Insert a fake child session row tagged with the ladder (bypasses the fire
    path so free-capital math can be asserted deterministically)."""
    import uuid
    sid = uuid.uuid4().hex
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, started_at, closed_at, status, mode,
                total_allocated_capital, invested_basis, config_json, ladder_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (sid, "2026-06-25T09:00:00+05:30", started_at, closed_at, status,
             "paper", capital, invested_basis, "{}", ladder_id))
        con.commit()
    return sid


def test_free_capital_margin_basis(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)  # per_basket 300k
    assert lad.free_capital() == pytest.approx(900000.0)
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    assert lad.free_capital() == pytest.approx(600000.0)
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    assert lad.free_capital() == pytest.approx(300000.0)
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    assert lad.free_capital() == pytest.approx(0.0)  # fully deployed, 3 open
    assert lad.n_active_baskets() == 3


def test_mtf_child_notional_does_not_inflate_ceiling(clean_positions):
    """An MTF child holds a ~3x leveraged invested_basis; the ladder ceiling must
    count only its total_allocated_capital (the margin slice), so 3 open children
    of total/3 = fully deployed regardless of leverage."""
    lad = LadderCampaign.create(total_capital=900000.0, order_product="MTF")
    # invested_basis (leveraged notional) = 900k on a 300k slice (3x) — MUST be
    # ignored by free_capital.
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0,
                  invested_basis=900000.0)
    assert lad.free_capital() == pytest.approx(600000.0)  # NOT 0, not negative
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0,
                  invested_basis=900000.0)
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0,
                  invested_basis=900000.0)
    assert lad.free_capital() == pytest.approx(0.0)  # exactly full at 3 slices


def test_closed_child_frees_its_slice(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    cid = _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    assert lad.free_capital() == pytest.approx(300000.0)
    # Close the second child → its slice frees back to the ceiling.
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status='CLOSED', "
                    "closed_at=? WHERE session_id=?",
                    ("2026-06-25T15:29:00+05:30", cid))
        con.commit()
    assert lad.free_capital() == pytest.approx(600000.0)
    assert lad.n_active_baskets() == 1


# ── daily tick: opens when free, skips when full ──────────────────────────────

def _open_positions(ladder_id):
    with falcon_conn() as con:
        row = con.execute(
            """SELECT COUNT(*) c FROM autotrade_positions p
               JOIN autotrade_sessions s ON s.session_id=p.session_id
               WHERE s.ladder_id=? AND p.status='OPEN'""", (ladder_id,)
        ).fetchone()
    return row["c"]


def test_daily_tick_opens_one_basket(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)  # per_basket 300k
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is True
    assert lad.n_active_baskets() == 1
    assert _open_positions(lad.ladder_id) == 5           # Top-5 basket
    # slice consumed on the margin basis
    assert lad.free_capital() == pytest.approx(600000.0)


def test_daily_tick_idempotent_same_day(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.daily_tick(ref_now=_trading_day_now())
    n1 = lad.n_active_baskets()
    # second tick SAME day opens nothing (last_tick_date guard).
    res2 = lad.daily_tick(ref_now=_trading_day_now())
    assert res2["opened"] is False
    assert "already opened today" in (res2["reason"] or "")
    assert lad.n_active_baskets() == n1 == 1


def test_daily_tick_skips_when_full(clean_positions, patched_brokers):
    """3 open children (= total) → free < per_basket → open nothing."""
    lad = LadderCampaign.create(total_capital=900000.0)
    for _ in range(3):
        _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    assert lad.free_capital() == pytest.approx(0.0)
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is False
    assert "no free slice" in (res["reason"] or "")
    assert lad.n_active_baskets() == 3


def test_daily_tick_paused_opens_nothing(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.pause()
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is False
    assert lad.n_active_baskets() == 0


def test_daily_tick_non_trading_day(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    # Sun 2026-06-28.
    sunday = datetime(2026, 6, 28, 10, 0, 0, tzinfo=IST)
    res = lad.daily_tick(ref_now=sunday)
    assert res["opened"] is False
    assert "trading day" in (res["reason"] or "")


# ── lifecycle: pause / resume ─────────────────────────────────────────────────

def test_pause_resume(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0)
    lad.pause()
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_PAUSED
    lad.resume()
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_RUNNING


# ── KILL modes ────────────────────────────────────────────────────────────────

def test_kill_flatten_now(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.daily_tick(ref_now=_trading_day_now())
    assert _open_positions(lad.ladder_id) == 5
    res = asyncio.run(lad.kill(mode=KILL_FLATTEN_NOW))
    assert res["status"] == STATUS_COMPLETED
    assert res["children_flattened"] == 1
    assert _open_positions(lad.ladder_id) == 0           # flattened
    assert lad.n_active_baskets() == 0


def test_kill_stop_new_let_finish_leaves_open(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.daily_tick(ref_now=_trading_day_now())
    assert _open_positions(lad.ladder_id) == 5
    res = asyncio.run(lad.kill(mode=KILL_STOP_NEW_LET_FINISH))
    assert res["status"] == STATUS_ENDED
    # open children are LEFT to exit naturally.
    assert _open_positions(lad.ladder_id) == 5
    assert lad.n_active_baskets() == 1
    # ENDED opens nothing on a subsequent tick.
    lad2 = LadderCampaign.load(lad.ladder_id)
    r2 = lad2.daily_tick(ref_now=_trading_day_now())
    assert r2["opened"] is False


def test_kill_rejects_bad_mode(clean_positions):
    lad = LadderCampaign.create(total_capital=300000.0)
    with pytest.raises(ValueError):
        asyncio.run(lad.kill(mode="nuke"))


# ── month-end: stops new + auto-completes on last close ───────────────────────

def test_month_end_stops_new(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    # Force end_date into the past.
    lad._update(end_date="2026-06-24")
    lad = LadderCampaign.load(lad.ladder_id)
    res = lad.daily_tick(ref_now=_trading_day_now())  # 06-25 > 06-24
    assert res["opened"] is False
    assert "past end_date" in (res["reason"] or "") or lad.status == STATUS_COMPLETED


def test_auto_completes_when_last_child_closes(clean_positions, patched_brokers):
    lad = LadderCampaign.create(total_capital=900000.0)
    lad._update(end_date="2026-06-24")  # already past on the 06-25 tick
    cid = _insert_child(lad.ladder_id, status="RUNNING", capital=300000.0)
    # First tick past end with an OPEN child → stays not-completed.
    lad = LadderCampaign.load(lad.ladder_id)
    lad.daily_tick(ref_now=_trading_day_now())
    assert LadderCampaign.load(lad.ladder_id).status != STATUS_COMPLETED
    # Close the last child, tick again → COMPLETED.
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status='CLOSED', closed_at=? "
                    "WHERE session_id=?", ("2026-06-25T15:29:00+05:30", cid))
        con.commit()
    lad = LadderCampaign.load(lad.ladder_id)
    lad.daily_tick(ref_now=_trading_day_now())
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_COMPLETED


def test_stop_new_let_finish_completes_when_flat(clean_positions):
    """KILL stop_new_let_finish with NO open children → completes immediately."""
    lad = LadderCampaign.create(total_capital=300000.0)
    res = asyncio.run(lad.kill(mode=KILL_STOP_NEW_LET_FINISH))
    # no open children → _maybe_complete flips ENDED→COMPLETED right away.
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_COMPLETED


# ── 5-day-avg alert ───────────────────────────────────────────────────────────

def _insert_closed_position(ladder_id, session_id, *, realised_pnl, closed_at):
    import uuid
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_positions
               (session_id, symbol, qty, avg_price, status, closed_at,
                realised_pnl, close_reason, direction)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (session_id, "SYM" + uuid.uuid4().hex[:4], 10, 100.0, "CLOSED",
             closed_at, realised_pnl, "TRAIL_EXIT", "long"))
        con.commit()


def test_alert_fires_once_on_down_crossing(clean_positions):
    lad = LadderCampaign.create(total_capital=1000000.0)
    sid = _insert_child(lad.ladder_id, status="CLOSED", capital=300000.0)
    fired = []
    # Patch alerts.send to capture the fire count.
    orig = ladder_mod.alerts.send
    ladder_mod.alerts.send = lambda msg, severity="info": fired.append(msg)
    try:
        # 5 down days (avg negative). Each day realized = -1000 / 1,000,000.
        days = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18",
                "2026-06-19"]
        for i, d in enumerate(days):
            _insert_closed_position(lad.ladder_id, sid, realised_pnl=-1000.0,
                                    closed_at=f"{d}T15:29:00+05:30")
            lad._refresh_alert(datetime.strptime(d, "%Y-%m-%d").date())
        assert lad.alert_active == 1
        assert len(fired) == 1                     # fired exactly ONCE
        # A further down day does NOT re-fire (latched).
        _insert_closed_position(lad.ladder_id, sid, realised_pnl=-1000.0,
                                closed_at="2026-06-22T15:29:00+05:30")
        lad._refresh_alert(datetime(2026, 6, 22).date())
        assert len(fired) == 1
    finally:
        ladder_mod.alerts.send = orig


def test_alert_does_not_fire_when_positive(clean_positions):
    lad = LadderCampaign.create(total_capital=1000000.0)
    sid = _insert_child(lad.ladder_id, status="CLOSED", capital=300000.0)
    fired = []
    orig = ladder_mod.alerts.send
    ladder_mod.alerts.send = lambda msg, severity="info": fired.append(msg)
    try:
        days = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18",
                "2026-06-19"]
        for d in days:
            _insert_closed_position(lad.ladder_id, sid, realised_pnl=+2000.0,
                                    closed_at=f"{d}T15:29:00+05:30")
            lad._refresh_alert(datetime.strptime(d, "%Y-%m-%d").date())
        assert lad.alert_active == 0
        assert fired == []
    finally:
        ladder_mod.alerts.send = orig


def test_alert_needs_full_window(clean_positions):
    """Fewer than 5 recorded days → no alert even if negative."""
    lad = LadderCampaign.create(total_capital=1000000.0)
    sid = _insert_child(lad.ladder_id, status="CLOSED", capital=300000.0)
    for d in ["2026-06-15", "2026-06-16", "2026-06-17"]:
        _insert_closed_position(lad.ladder_id, sid, realised_pnl=-5000.0,
                                closed_at=f"{d}T15:29:00+05:30")
        lad._refresh_alert(datetime.strptime(d, "%Y-%m-%d").date())
    assert lad.alert_active == 0


# ── restart durability ────────────────────────────────────────────────────────

def test_restart_rederives_state(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.daily_tick(ref_now=_trading_day_now())
    assert lad.n_active_baskets() == 1
    # Simulate a restart: a fresh object loaded purely from the DB re-derives
    # everything (no in-memory state).
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_RUNNING
    assert reloaded.n_active_baskets() == 1
    assert reloaded.free_capital() == pytest.approx(600000.0)
    # Same-day resume tick opens NOTHING (idempotent via last_tick_date).
    r = reloaded.daily_tick(ref_now=_trading_day_now())
    assert r["opened"] is False
    assert reloaded.n_active_baskets() == 1


def test_resume_active_ladders_reopens_idempotent(clean_positions,
                                                  patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    # Freeze the module clock so resume's internal now() is a trading day.
    sess_mod.set_fake_now(_trading_day_now())
    try:
        s1 = ladder_mod.resume_active_ladders()
        assert s1["opened"] == 1
        assert LadderCampaign.load(lad.ladder_id).n_active_baskets() == 1
        # Running resume AGAIN the same day opens nothing.
        s2 = ladder_mod.resume_active_ladders()
        assert s2["opened"] == 0
    finally:
        sess_mod.set_fake_now(None)


# ── status shape (trader-facing) ──────────────────────────────────────────────

def test_status_trader_facing_fields(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.daily_tick(ref_now=_trading_day_now())
    st = lad.to_status()
    for key in ("total_capital", "capital_deployed", "capital_free",
                "n_active_baskets", "n_open_positions", "realized_pnl",
                "unrealized_pnl", "today_pnl", "status", "end_date", "alert",
                "sessions"):
        assert key in st, f"missing status field: {key}"
    assert st["n_active_baskets"] == 1
    assert st["n_open_positions"] == 5
    assert st["capital_deployed"] == pytest.approx(300000.0)
    assert st["capital_free"] == pytest.approx(600000.0)
    assert st["alert"]["active"] is False
    assert len(st["sessions"]) == 1
    # No leakage of internal jargon in the serialized status.
    assert "sleeve" not in json.dumps(st).lower()
