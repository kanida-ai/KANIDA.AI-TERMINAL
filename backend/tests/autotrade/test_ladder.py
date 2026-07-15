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
    LadderCampaign, STATUS_CREATED, STATUS_SCHEDULED, STATUS_RUNNING,
    STATUS_PAUSED, STATUS_ENDED, STATUS_COMPLETED, KILL_FLATTEN_NOW,
    KILL_STOP_NEW_LET_FINISH,
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
    # create() is a DRAFT — CREATED, not RUNNING (mirrors session create→start).
    assert lad.status == STATUS_CREATED
    # persisted frozen value + persisted status
    got = LadderCampaign.load(lad.ladder_id)
    assert got.per_basket_capital == pytest.approx(300000.0)
    assert got.status == STATUS_CREATED


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
    lad.start()  # now RUNNING (draft → started)
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is True
    assert lad.n_active_baskets() == 1
    assert _open_positions(lad.ladder_id) == 5           # Top-5 basket
    # slice consumed on the margin basis
    assert lad.free_capital() == pytest.approx(600000.0)


def test_daily_tick_idempotent_same_day(clean_positions, patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start()
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
    lad.start()
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
    lad.start()
    # Sun 2026-06-28.
    sunday = datetime(2026, 6, 28, 10, 0, 0, tzinfo=IST)
    res = lad.daily_tick(ref_now=sunday)
    assert res["opened"] is False
    assert "trading day" in (res["reason"] or "")


# ── START-NOW / SCHEDULE lifecycle (mirrors single-session create→start) ──────

def _future_trading_day():
    # Mon 2026-06-29 — the next NSE trading day after the frozen "today"
    # (Thu 2026-06-25); Fri 06-26 is a holiday, 27/28 weekend.
    return "2026-06-29"


def _future_trading_day_now():
    return datetime(2026, 6, 29, 10, 0, 0, tzinfo=IST)


def test_create_is_draft_spawns_nothing(clean_positions, patched_brokers):
    """create() → CREATED draft; a tick on the create-day opens nothing."""
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    assert lad.status == STATUS_CREATED
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is False
    assert "CREATED" in (res["reason"] or "")
    assert lad.n_active_baskets() == 0
    assert _open_positions(lad.ladder_id) == 0


def test_start_no_date_runs_immediately(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    out = lad.start()  # no date → RUNNING now
    assert out["status"] == STATUS_RUNNING
    assert out["when"] == "now"
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_RUNNING
    # start_date pinned to today (frozen 2026-06-25).
    assert reloaded.start_date == "2026-06-25"


def test_start_past_date_runs_immediately(clean_positions):
    """A start_date <= today behaves like start-now (RUNNING today)."""
    lad = LadderCampaign.create(total_capital=900000.0)
    out = lad.start(start_date="2026-06-01")  # in the past
    assert out["status"] == STATUS_RUNNING
    assert LadderCampaign.load(lad.ladder_id).start_date == "2026-06-25"


def test_start_future_date_schedules(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    out = lad.start(start_date=_future_trading_day())
    assert out["status"] == STATUS_SCHEDULED
    assert out["when"] == "scheduled"
    assert out["start_date"] == _future_trading_day()
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_SCHEDULED
    assert reloaded.start_date == _future_trading_day()


def test_scheduled_tick_before_start_date_spawns_nothing(clean_positions,
                                                         patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())
    # Tick on TODAY (06-25) — before the scheduled 06-29 start.
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is False
    assert "scheduled" in (res["reason"] or "").lower()
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_SCHEDULED
    assert lad.n_active_baskets() == 0
    assert _open_positions(lad.ladder_id) == 0


def test_scheduled_tick_on_start_date_activates_and_spawns(clean_positions,
                                                           patched_brokers):
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())
    # Tick ON the scheduled trading day → activates to RUNNING + opens a basket.
    res = lad.daily_tick(ref_now=_future_trading_day_now())
    assert res.get("activated") is True
    assert res["opened"] is True
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_RUNNING
    assert reloaded.n_active_baskets() == 1
    assert _open_positions(lad.ladder_id) == 5


def test_start_weekend_rejected_with_suggestion(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    # Sat 2026-06-27 → not a trading day; suggested next = Mon 2026-06-29.
    with pytest.raises(ValueError) as e:
        lad.start(start_date="2026-06-27")
    msg = str(e.value)
    assert "not an NSE trading day" in msg
    assert "2026-06-29" in msg
    assert "||suggested=2026-06-29" in msg  # route parses this sentinel
    # Unchanged — still a draft.
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_CREATED


def test_reschedule_a_scheduled_campaign(clean_positions):
    """A SCHEDULED campaign may be re-scheduled to a new future date."""
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date="2026-06-29")
    assert LadderCampaign.load(lad.ladder_id).start_date == "2026-06-29"
    lad = LadderCampaign.load(lad.ladder_id)
    lad.start(start_date="2026-06-30")  # re-schedule
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_SCHEDULED
    assert reloaded.start_date == "2026-06-30"


def test_start_rejected_from_running(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start()  # RUNNING
    with pytest.raises(ValueError) as e:
        lad.start()
    assert "RUNNING" in str(e.value)


def test_start_rejected_from_completed(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    asyncio.run(lad.kill(mode=KILL_STOP_NEW_LET_FINISH))  # → COMPLETED (flat)
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_COMPLETED
    lad = LadderCampaign.load(lad.ladder_id)
    with pytest.raises(ValueError):
        lad.start()


def test_start_future_after_end_date_rejected(clean_positions):
    """A start_date after end_date would never open a basket → rejected."""
    lad = LadderCampaign.create(total_capital=900000.0)
    lad._update(end_date="2026-06-29")
    lad = LadderCampaign.load(lad.ladder_id)
    with pytest.raises(ValueError) as e:
        lad.start(start_date="2026-06-30")  # after end
    assert "end_date" in str(e.value)


def test_scheduled_survives_restart_and_activates(clean_positions,
                                                  patched_brokers):
    """A SCHEDULED campaign is included in resume, stays SCHEDULED before its
    date, and auto-activates + spawns via resume ON its start_date."""
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())  # SCHEDULED for 06-29

    # Restart #1: it's still 06-25 (before start) → resume ticks it but it stays
    # SCHEDULED and opens nothing.
    sess_mod.set_fake_now(_trading_day_now())
    try:
        s1 = ladder_mod.resume_active_ladders()
        assert s1["resumed"] == 1          # SCHEDULED IS included in resume
        assert s1["opened"] == 0
    finally:
        sess_mod.set_fake_now(None)
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_SCHEDULED

    # Restart #2: now it's 06-29 (the start day) → resume activates + opens.
    sess_mod.set_fake_now(_future_trading_day_now())
    try:
        s2 = ladder_mod.resume_active_ladders()
        assert s2["opened"] == 1
    finally:
        sess_mod.set_fake_now(None)
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_RUNNING
    assert reloaded.n_active_baskets() == 1


def test_created_draft_not_resumed(clean_positions, patched_brokers):
    """A CREATED draft is NOT ticked/resumed (excluded from the scheduler set)."""
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)  # CREATED, never started
    sess_mod.set_fake_now(_trading_day_now())
    try:
        s = ladder_mod.resume_active_ladders()
        assert s["resumed"] == 0           # draft excluded
        assert s["opened"] == 0
    finally:
        sess_mod.set_fake_now(None)
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_CREATED
    assert lad.n_active_baskets() == 0


def test_status_surfaces_scheduled_state(clean_positions):
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())
    st = LadderCampaign.load(lad.ladder_id).to_status()
    assert st["status"] == STATUS_SCHEDULED
    assert st["start_date"] == _future_trading_day()


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
    lad.start()
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
    lad.start()
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
    lad.start()
    # Force end_date into the past.
    lad._update(end_date="2026-06-24")
    lad = LadderCampaign.load(lad.ladder_id)
    res = lad.daily_tick(ref_now=_trading_day_now())  # 06-25 > 06-24
    assert res["opened"] is False
    assert "past end_date" in (res["reason"] or "") or lad.status == STATUS_COMPLETED


def test_auto_completes_when_last_child_closes(clean_positions, patched_brokers):
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start()
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
    lad.start()
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
    lad.start()  # RUNNING → included in resume
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
    lad.start()
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


# ── Market-open TIME gate (a campaign must not activate/open before 09:15) ──

def _premarket_start_day_now():
    # 08:00 IST on the scheduled start day (Mon 06-29) — BEFORE the 09:15 open.
    return datetime(2026, 6, 29, 8, 0, 0, tzinfo=IST)


def test_scheduled_tick_premarket_stays_scheduled(clean_positions, patched_brokers):
    """On the start DAY but BEFORE the 09:15 open (e.g. a pre-market restart's
    resume tick), a scheduled campaign stays SCHEDULED and opens nothing — it must
    NOT flip to RUNNING. This is the reported bug."""
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())
    res = lad.daily_tick(ref_now=_premarket_start_day_now())
    assert res["opened"] is False
    assert res.get("activated") is not True
    reloaded = LadderCampaign.load(lad.ladder_id)
    assert reloaded.status == STATUS_SCHEDULED
    assert reloaded.n_active_baskets() == 0
    assert _open_positions(lad.ladder_id) == 0


def test_scheduled_tick_at_open_activates(clean_positions, patched_brokers):
    """At exactly 09:15 on the start day the campaign activates to RUNNING."""
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start(start_date=_future_trading_day())
    at_open = datetime(2026, 6, 29, 9, 15, 0, tzinfo=IST)
    res = lad.daily_tick(ref_now=at_open)
    assert res.get("activated") is True
    assert LadderCampaign.load(lad.ladder_id).status == STATUS_RUNNING


def test_schedule_today_before_open_is_scheduled(clean_positions):
    """Scheduling for TODAY before the 09:15 open lands SCHEDULED (with a
    countdown), NOT RUNNING — the reported 'can't schedule for today' fix."""
    sess_mod.set_fake_now(datetime(2026, 6, 25, 8, 0, 0, tzinfo=IST))  # pre-open, trading day
    try:
        lad = LadderCampaign.create(total_capital=900000.0)
        out = lad.start(start_date="2026-06-25")  # today
        assert out["status"] == STATUS_SCHEDULED
        assert out["when"] == "scheduled"
        assert out["start_date"] == "2026-06-25"
        assert LadderCampaign.load(lad.ladder_id).status == STATUS_SCHEDULED
    finally:
        sess_mod.set_fake_now(None)


def test_schedule_today_after_open_starts_now(clean_positions):
    """Scheduling for TODAY once the 09:15 open has passed → RUNNING (start-now)."""
    sess_mod.set_fake_now(datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST))  # after open
    try:
        lad = LadderCampaign.create(total_capital=900000.0)
        out = lad.start(start_date="2026-06-25")  # today, after open
        assert out["status"] == STATUS_RUNNING
        assert out["when"] == "now"
    finally:
        sess_mod.set_fake_now(None)


def test_delete_draft_removes_row(clean_positions):
    """A CREATED draft can be PERMANENTLY deleted (the Discard fix) — the row is
    gone, not just hidden."""
    lad = LadderCampaign.create(total_capital=900000.0)
    lid = lad.ladder_id
    assert lad.status == STATUS_CREATED
    assert lad.delete() is True
    assert LadderCampaign.load(lid) is None


def test_delete_refuses_when_basket_open(clean_positions, patched_brokers):
    """A campaign with an OPEN basket must be cancelled first — delete refuses so
    a live position is never orphaned."""
    import pytest
    _basket_signals()
    lad = LadderCampaign.create(total_capital=900000.0)
    lad.start()                                    # RUNNING
    lad.daily_tick(ref_now=_trading_day_now())     # opens a basket (10:00, market open)
    if lad.n_active_baskets() > 0:
        with pytest.raises(ValueError):
            lad.delete()
    else:
        assert lad.delete() is True   # nothing opened → safe to delete


# ── HOLD-LENGTH-AWARE SLEEVE SIZING (BTST) ────────────────────────────────────
# per_basket = total_capital / hold_length, hold_length = effective child
# max_hold_sessions (default POSITIONAL_MAX_HOLD_SESSIONS=3). Full-deploy at
# steady state = hold_length overlapping sleeves. See ladder.create().

def test_default_hold_length_unchanged_backward_compat(clean_positions):
    """No child override -> hold_length defaults to 3 -> per_basket == total/3,
    IDENTICAL to the legacy BASKET_DIVISOR path; child_config_json stays NULL."""
    lad = LadderCampaign.create(total_capital=900000.0, order_product="CNC")
    assert lad.per_basket_capital == pytest.approx(300000.0)   # total/3
    assert lad.child_config_json is None                       # byte-identical
    got = LadderCampaign.load(lad.ladder_id)
    assert got.per_basket_capital == pytest.approx(300000.0)
    assert got.child_config_json is None


def test_btst_per_basket_is_total_over_two(clean_positions):
    """BTST: total=X, child max_hold_sessions=2 -> per_basket == X/2 (NOT X/3).
    Mutation-verified: the divisor is the hold length, not the constant 3."""
    X = 1_000_000.0
    lad = LadderCampaign.create(
        total_capital=X, order_product="CNC",
        child_config={"max_hold_sessions": 2})
    assert lad.per_basket_capital == pytest.approx(X / 2)      # 500,000
    assert lad.per_basket_capital != pytest.approx(X / 3)      # mutation guard
    got = LadderCampaign.load(lad.ladder_id)
    assert got.per_basket_capital == pytest.approx(X / 2)
    assert got.child_config_overrides().get("max_hold_sessions") == 2


def test_btst_arbitrary_capital_over_two(clean_positions):
    """Capital is the trader's choice; per_basket derives from it + hold length.
    No hard-coded amount anywhere."""
    for X in (250000.0, 777777.0, 5_000_000.0):
        lad = LadderCampaign.create(
            total_capital=X, child_config={"max_hold_sessions": 2})
        assert lad.per_basket_capital == pytest.approx(round(X / 2, 2))


def test_btst_steady_state_two_concurrent_full_deploy(clean_positions):
    """2-session hold -> 2 overlapping sleeves of total/2 = the FULL pool at steady
    state; the free-slice gate never opens a 3rd."""
    X = 1_000_000.0
    lad = LadderCampaign.create(
        total_capital=X, child_config={"max_hold_sessions": 2})
    assert lad.free_capital() == pytest.approx(X)
    _insert_child(lad.ladder_id, status="RUNNING", capital=500000.0)
    assert lad.free_capital() == pytest.approx(500000.0)       # one slice free
    _insert_child(lad.ladder_id, status="RUNNING", capital=500000.0)
    assert lad.free_capital() == pytest.approx(0.0)            # full at 2 sleeves
    assert lad.n_active_baskets() == 2


def test_btst_daily_tick_never_opens_third(clean_positions, patched_brokers):
    """With 2 open sleeves (= total), the daily tick opens NOTHING (caps at
    hold_length concurrent - never over-deploys the ceiling)."""
    X = 1_000_000.0
    lad = LadderCampaign.create(
        total_capital=X, child_config={"max_hold_sessions": 2})
    lad.start()
    for _ in range(2):
        _insert_child(lad.ladder_id, status="RUNNING", capital=500000.0)
    assert lad.free_capital() == pytest.approx(0.0)
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is False
    assert "no free slice" in (res["reason"] or "")
    assert lad.n_active_baskets() == 2                          # still 2, no 3rd


def test_btst_daily_tick_opens_second_when_one_free(clean_positions,
                                                    patched_brokers):
    """With one sleeve free (total/2), the daily tick opens exactly ONE more real
    basket -> 2 concurrent, pool fully deployed."""
    _basket_signals()
    X = 1_000_000.0
    lad = LadderCampaign.create(
        total_capital=X, child_config={"max_hold_sessions": 2})
    lad.start()
    _insert_child(lad.ladder_id, status="RUNNING", capital=500000.0)  # 1 sleeve used
    res = lad.daily_tick(ref_now=_trading_day_now())
    assert res["opened"] is True
    assert lad.n_active_baskets() == 2                          # exactly 2
    assert _open_positions(lad.ladder_id) == 5                  # Top-5 basket
    assert lad.free_capital() == pytest.approx(0.0)             # full deploy


def test_btst_one_child_per_day(clean_positions, patched_brokers):
    """Idempotent: a second tick the SAME day opens nothing even with a free
    sleeve (one child/day)."""
    _basket_signals()
    X = 1_000_000.0
    lad = LadderCampaign.create(
        total_capital=X, child_config={"max_hold_sessions": 2})
    lad.start()
    r1 = lad.daily_tick(ref_now=_trading_day_now())
    assert r1["opened"] is True
    assert lad.n_active_baskets() == 1
    r2 = lad.daily_tick(ref_now=_trading_day_now())
    assert r2["opened"] is False
    assert "already opened today" in (r2["reason"] or "")
    assert lad.n_active_baskets() == 1                          # not 2 same day


def test_btst_child_config_applied_to_spawned_config(clean_positions):
    """The child config carries the overrides: max_hold=2, arm_pct=0.5 (unreachable
    in a 2-session hold -> trail never arms = 'trail off'), square_off stays False
    (positional carry), and it VALIDATES."""
    lad = LadderCampaign.create(
        total_capital=1_000_000.0, order_product="CNC",
        child_config={"max_hold_sessions": 2, "arm_pct": 0.5})
    cfg = lad._build_child_config()
    assert cfg.max_hold_sessions == 2
    assert cfg.arm_pct == pytest.approx(0.5)
    assert cfg.square_off_enabled is False                     # carry across days
    assert cfg.order_product == "CNC"
    assert cfg.top_n_stocks == 5                               # POSITIONAL_TOP_N
    assert cfg.total_allocated_capital == pytest.approx(500000.0)
    cfg.validate()                                             # passes the gate


def test_create_validates_child_override_rejects_out_of_range_arm(clean_positions):
    """FAIL-FAST: an out-of-range override (arm_pct=0.99 > the intraday 0.5 cap) is
    rejected AT CREATE - so a campaign that could never open a child is never
    created."""
    with pytest.raises(ValueError) as e:
        LadderCampaign.create(
            total_capital=1_000_000.0,
            child_config={"max_hold_sessions": 2, "arm_pct": 0.99})
    assert "child_config invalid" in str(e.value)


def test_create_rejects_bad_max_hold(clean_positions):
    """max_hold_sessions must be >= 1 (a 0/negative divisor is nonsensical)."""
    with pytest.raises(ValueError):
        LadderCampaign.create(
            total_capital=1_000_000.0, child_config={"max_hold_sessions": 0})


def test_btst_cnc_accepted_mis_rejected_with_override(clean_positions):
    """CNC BTST accepted; MIS still rejected at the door even with a child
    override (positional can't carry overnight)."""
    lad = LadderCampaign.create(
        total_capital=1_000_000.0, order_product="CNC",
        child_config={"max_hold_sessions": 2})
    assert lad.order_product == "CNC"
    with pytest.raises(ValueError) as e:
        LadderCampaign.create(
            total_capital=1_000_000.0, order_product="MIS",
            child_config={"max_hold_sessions": 2})
    assert "MIS" in str(e.value) or "overnight" in str(e.value)


def test_child_config_filtered_to_whitelist(clean_positions):
    """Non-whitelisted keys (e.g. total_allocated_capital) are dropped; only the
    whitelisted risk/exit knobs survive - capital can never be smuggled in via the
    child override."""
    lad = LadderCampaign.create(
        total_capital=1_000_000.0,
        child_config={"max_hold_sessions": 2,
                      "total_allocated_capital": 99.0,   # dropped
                      "order_product": "MIS",            # dropped
                      "arm_pct": 0.4})
    ov = lad.child_config_overrides()
    assert set(ov.keys()) == {"max_hold_sessions", "arm_pct"}
    assert "total_allocated_capital" not in ov
    assert lad.per_basket_capital == pytest.approx(500000.0)   # total/2, sane
    assert lad.order_product == "CNC"                          # default, not MIS
