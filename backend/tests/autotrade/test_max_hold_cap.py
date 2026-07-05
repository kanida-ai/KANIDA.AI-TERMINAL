"""MULTI-SESSION MAX-HOLD CAP for the POSITIONAL basket (the one missing
mechanism for Falcon Positional).

A positional basket (strategy=="intraday_basket", square_off_enabled=False,
max_hold_sessions>0) is squared off at square_off_time on the Nth NSE trading
session — counting the ENTRY day as session 1 — REGARDLESS of trail arm/peak
state. The cap datetime is computed from the PERSISTED entry timestamp
(started_at) so it is durable across a backend restart (no in-memory timer).

Covers:
  * cap-date math across a WEEKEND and across an NSE HOLIDAY (reuses
    trading_calendar; entry day = session 1).
  * max_hold_sessions=0 → no cap (backward-compatible: today's behaviour).
  * config validation + json round-trip (default 0; rejects negative).
  * enforcement: the cap FIRES the whole-basket flatten at square_off_time on
    day N even when the trail NEVER armed (close_reason=MAX_HOLD_EXIT).
  * enforcement does NOT fire before the cap moment.
  * durability: recompute after a simulated restart yields the SAME cap and the
    resumed tick fires it.

All paper-safe (MockBroker, no real orders).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, compute_max_hold_cap_datetime
from autotrade.monitoring import square_off_scheduler
from falcon.db import falcon_conn
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))


# ── cap-date math (pure) ──────────────────────────────────────────────────────

def _iso(y, m, d, hh=15, mm=29, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=IST)


def test_cap_date_across_weekend():
    # Entry Fri 2026-07-10 (session 1) → Mon 07-13 (2) → Tue 07-14 (3).
    # (All three verified trading days; the gap crosses a plain weekend.)
    started = "2026-07-10T09:15:00+05:30"
    cap = compute_max_hold_cap_datetime(started, 3, "15:29:00")
    assert cap == _iso(2026, 7, 14, 15, 29, 0)


def test_cap_date_across_holiday():
    # Entry Thu 2026-06-25 (session 1). Next trading days skip the NSE holiday
    # Fri 2026-06-26 (Muharram) + Sat/Sun 06-27/28 → Mon 06-29 (2) →
    # Tue 06-30 (3).
    started = "2026-06-25T09:15:00+05:30"
    cap = compute_max_hold_cap_datetime(started, 3, "15:29:00")
    assert cap == _iso(2026, 6, 30, 15, 29, 0)


def test_cap_session_1_is_entry_day():
    # N=1 → cap on the entry day itself at square_off_time.
    started = "2026-06-25T09:15:00+05:30"   # Thu, a trading day
    cap = compute_max_hold_cap_datetime(started, 1, "15:29:00")
    assert cap == _iso(2026, 6, 25, 15, 29, 0)


def test_cap_honours_square_off_clock():
    started = "2026-06-25T09:15:00+05:30"
    cap = compute_max_hold_cap_datetime(started, 1, "14:00:00")
    assert (cap.hour, cap.minute, cap.second) == (14, 0, 0)


def test_cap_zero_means_no_cap():
    assert compute_max_hold_cap_datetime("2026-06-25T09:15:00+05:30",
                                         0, "15:29:00") is None


def test_cap_missing_started_at_is_none():
    assert compute_max_hold_cap_datetime(None, 3, "15:29:00") is None
    assert compute_max_hold_cap_datetime("", 3, "15:29:00") is None
    assert compute_max_hold_cap_datetime("not-a-date", 3, "15:29:00") is None


def test_cap_entry_on_weekend_anchors_next_trading_day():
    # EDGE: started_at stamped on a Saturday → session 1 anchors on Mon 06-29;
    # N=2 → Tue 06-30. The cap never lands on a closed market.
    started = "2026-06-27T09:15:00+05:30"   # Sat
    cap = compute_max_hold_cap_datetime(started, 2, "15:29:00")
    assert cap == _iso(2026, 6, 30, 15, 29, 0)


# ── config ────────────────────────────────────────────────────────────────────

def test_config_default_max_hold_is_zero_and_roundtrips():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5)
    assert cfg.max_hold_sessions == 0                       # default = no cap
    cfg.validate()
    d = cfg.to_dict()
    assert d["max_hold_sessions"] == 0
    # A dict WITHOUT the key loads as 0 (every existing session unchanged).
    del d["max_hold_sessions"]
    assert TradingSessionConfig.from_dict(d).max_hold_sessions == 0
    # Explicit value round-trips.
    d2 = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False,
        max_hold_sessions=3).to_dict()
    assert TradingSessionConfig.from_dict(d2).max_hold_sessions == 3


def test_config_rejects_negative_max_hold():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False,
        max_hold_sessions=-1)
    with pytest.raises(ValueError, match="max_hold_sessions"):
        cfg.validate()


def test_config_allows_positional_cnc_with_cap():
    cfg = TradingSessionConfig(
        total_allocated_capital=100000.0, strategy="intraday_basket",
        top_n_stocks=5, order_product="CNC", square_off_enabled=False,
        max_hold_sessions=3)
    cfg.validate()   # must not raise


# ── enforcement (session-level) ───────────────────────────────────────────────

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    shared_ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _basket_signals():
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])


def _positional_cfg(max_hold_sessions):
    return TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, square_off_enabled=False,
        arm_pct=0.01, floor_pct=0.01, square_off_time="15:29:00",
        max_hold_sessions=max_hold_sessions)


def _set_started_at(session_id, iso):
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET started_at=? WHERE session_id=?",
                    (iso, session_id))
        con.commit()


def _open_count(session_id):
    with falcon_conn() as con:
        row = con.execute(
            "SELECT COUNT(*) c FROM autotrade_positions "
            "WHERE session_id=? AND status='OPEN'", (session_id,)).fetchone()
    return row["c"]


def test_max_hold_fires_on_day_n_even_unarmed(clean_positions, patched_brokers):
    """The cap flattens the whole basket at square_off_time on session N even
    though the trail NEVER armed (flat return the whole time)."""
    _basket_signals()
    sess = TradingSession.create(_positional_cfg(3), mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    # Anchor entry on Thu 07-02 (S1) → Fri 07-03 (2) → Mon 07-06 (3; 07-06 is a
    # normal trading day, weekend 07-04/05 skipped). Cap = Mon 07-06 @ 15:29.
    _set_started_at(sid, "2026-07-02T09:15:00+05:30")
    assert _open_count(sid) == 3

    # The trail is UNARMED (never save_trail_state). Freeze "now" to the cap
    # moment and tick.
    sess_mod.set_fake_now(_iso(2026, 7, 6, 15, 29, 0))
    try:
        # fresh object to prove nothing in-memory carries the cap.
        s2 = TradingSession.load(sid)
        st = s2.monitor.load_trail_state()
        assert st.armed is False                     # never armed
        res = asyncio.run(s2.tick())
    finally:
        sess_mod.set_fake_now(None)

    assert res["kill_reason"] == "MAX_HOLD_EXIT"
    assert res["kill_switch_fired"] is True
    assert _open_count(sid) == 0                      # basket flattened
    # close_reason tag written to the positions.
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT DISTINCT close_reason FROM autotrade_positions "
            "WHERE session_id=?", (sid,)).fetchall()
    assert [r["close_reason"] for r in rows] == ["MAX_HOLD_EXIT"]


def test_max_hold_does_not_fire_before_cap(clean_positions, patched_brokers):
    """One trading day before the cap (session 2 of 3) the basket is HELD."""
    _basket_signals()
    sess = TradingSession.create(_positional_cfg(3), mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    _set_started_at(sid, "2026-07-02T09:15:00+05:30")   # cap N=3 = Mon 07-06

    # Fri 07-03 @ 15:29 = session 2 — before the cap → no MAX_HOLD_EXIT.
    sess_mod.set_fake_now(_iso(2026, 7, 3, 15, 29, 0))
    try:
        res = asyncio.run(TradingSession.load(sid).tick())
    finally:
        sess_mod.set_fake_now(None)
    assert res.get("kill_reason") != "MAX_HOLD_EXIT"
    assert _open_count(sid) == 3                        # still holding


def test_no_cap_when_zero_carries_past_day_n(clean_positions, patched_brokers):
    """max_hold_sessions=0 → NO cap: even far past what would be day 3 the basket
    is NOT force-closed (backward-compatible, today's positional behaviour)."""
    _basket_signals()
    sess = TradingSession.create(_positional_cfg(0), mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    _set_started_at(sid, "2026-07-02T09:15:00+05:30")

    # Way past any Nth-session cap; flat return so no trail exit either.
    sess_mod.set_fake_now(_iso(2026, 7, 15, 15, 29, 0))
    try:
        res = asyncio.run(TradingSession.load(sid).tick())
    finally:
        sess_mod.set_fake_now(None)
    assert res.get("kill_reason") != "MAX_HOLD_EXIT"
    assert _open_count(sid) == 3                        # carries on


def test_max_hold_durable_across_restart(clean_positions, patched_brokers):
    """After a simulated restart (fresh session object) the cap datetime
    recomputes identically from the persisted started_at and the resumed tick
    fires MAX_HOLD_EXIT — proving durability with no in-memory timer."""
    _basket_signals()
    sess = TradingSession.create(_positional_cfg(2), mode="paper")
    asyncio.run(sess.start())
    sid = sess.session_id
    _set_started_at(sid, "2026-07-02T09:15:00+05:30")   # cap N=2 = Fri 07-03

    # Recompute purely from persisted state (what recovery/tick will do).
    cap = compute_max_hold_cap_datetime(
        "2026-07-02T09:15:00+05:30", 2, "15:29:00")
    assert cap == _iso(2026, 7, 3, 15, 29, 0)

    # "Restart": brand-new object + recovery re-arm, then tick at the cap.
    from autotrade import recovery
    recovery._resume_running(sid)
    sess_mod.set_fake_now(_iso(2026, 7, 3, 15, 29, 0))
    try:
        res = asyncio.run(TradingSession.load(sid).tick())
    finally:
        sess_mod.set_fake_now(None)
    assert res["kill_reason"] == "MAX_HOLD_EXIT"
    assert _open_count(sid) == 0


def test_status_surfaces_cap(clean_positions, patched_brokers):
    _basket_signals()
    sess = TradingSession.create(_positional_cfg(3), mode="paper")
    asyncio.run(sess.start())
    _set_started_at(sess.session_id, "2026-07-02T09:15:00+05:30")
    st = TradingSession.load(sess.session_id).status()
    assert st["trail"]["max_hold_sessions"] == 3
    assert st["trail"]["max_hold_cap_datetime"] == _iso(
        2026, 7, 6, 15, 29, 0).isoformat()
