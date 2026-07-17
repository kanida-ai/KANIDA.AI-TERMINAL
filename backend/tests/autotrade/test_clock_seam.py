"""CLOCK SEAM (DETERMINISM) — the money-path time gates honour now_ist().

session.py documents now_ist() (env FALCON_AUTOTRADE_FAKE_NOW / set_fake_now) as
"used by every fire-gate path". It previously WASN'T: the MIS defensive
square-off backstop, the 09:15-15:29 market-hours guards and trail_engine.decide()
read the RAW wall clock, so those real-money branches were untestable and the
suite's outcome depended on the time of day it ran.

These tests pin the boundary of each routed site through the SEAM (never by
freezing datetime from the outside) and assert BOTH sides — fire AND no-fire.

THE SAFETY PROPERTY under test: with the seam UNSET, now_ist() IS the real IST
clock, so production behaviour is unchanged. test_seam_unset_is_the_real_clock
guards that; a differential prod-identity proof over 640 decide() cases against
the pre-change module was run at build time.

All paper-safe (MockBroker, no real orders).
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, now_ist, set_fake_now
from autotrade.monitoring import trail_engine as te
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))

# A known NSE trading day (Thu), matching the conftest's advertised "now".
DAY = (2026, 6, 25)


def _at(h, m, s=0):
    return datetime(DAY[0], DAY[1], DAY[2], h, m, s, tzinfo=IST)


@pytest.fixture
def seam():
    """Drive now_ist() explicitly; always release it so no test leaks a clock."""
    def _set(dt):
        set_fake_now(dt)
        return dt
    yield _set
    set_fake_now(None)


@pytest.fixture
def brokers(monkeypatch):
    shared = {"A": 100.0, "B": 100.0, "C": 100.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return shared


# ── the seam itself ─────────────────────────────────────────────────────────

def test_seam_unset_is_the_real_clock(monkeypatch):
    """PROD-IDENTITY: with no override, now_ist() IS datetime.now(IST).

    This is the whole safety property of routing the money path through the
    seam — in production (env unset, set_fake_now never called) every routed
    site reads exactly what it read before.
    """
    set_fake_now(None)
    monkeypatch.delenv("FALCON_AUTOTRADE_FAKE_NOW", raising=False)
    before = datetime.now(IST)
    got = now_ist()
    after = datetime.now(IST)
    assert before <= got <= after          # it IS the real clock, not a fake
    assert got.tzinfo is not None          # tz-AWARE (a naive dt would break >=)
    assert got.utcoffset() == timedelta(hours=5, minutes=30)   # and it is IST


def test_seam_env_is_honoured_and_is_ist_aware(monkeypatch):
    monkeypatch.setenv("FALCON_AUTOTRADE_FAKE_NOW", "2026-06-25T15:12:00")
    set_fake_now(None)
    got = now_ist()
    assert got == _at(15, 12, 0)
    assert got.utcoffset() == timedelta(hours=5, minutes=30)


def test_seam_unparseable_env_falls_back_to_real_clock(monkeypatch):
    """A bad seam value must NEVER crash or freeze a fire path."""
    monkeypatch.setenv("FALCON_AUTOTRADE_FAKE_NOW", "not-a-timestamp")
    set_fake_now(None)
    before = datetime.now(IST)
    got = now_ist()
    assert before <= got <= datetime.now(IST)


def test_trail_engine_clock_routes_through_the_seam(seam):
    """trail_engine imports session lazily (session imports it at module load);
    prove the seam actually reaches it."""
    seam(_at(11, 0))
    assert te._clock_now() == _at(11, 0)


# ── trail_engine.decide() — square-off boundary via the seam ────────────────

def _params(**kw):
    base = dict(arm_pct=0.05, floor_pct=0.01, trail_giveback_pct=0.015,
                stop_pct=0.03, square_off_time="15:29", square_off_enabled=True)
    base.update(kw)
    return te.TrailParams(**base)


def test_decide_mid_session_is_not_square_off(seam):
    """At 11:00 IST decide() judges on the RETURN, not the clock."""
    seam(_at(11, 0))
    p = _params()
    # below arm -> HOLD
    assert te.decide(0.01, te.TrailState(), p).action == "HOLD"
    # at arm -> ARM
    d = te.decide(0.05, te.TrailState(), p)
    assert (d.action, d.state.armed, d.state.peak) == ("ARM", True, 0.05)
    # hard stop -> EXIT/STOP
    assert te.decide(-0.03, te.TrailState(), p).reason == "STOP"


def test_decide_after_close_is_square_off_regardless_of_return(seam):
    """Past square_off_time the time branch takes precedence over EVERY input —
    the behaviour that silently made post-close test runs 'fail'."""
    seam(_at(15, 30))
    p = _params()
    for g in (-0.10, 0.0, 0.01, 0.05, 0.50):
        d = te.decide(g, te.TrailState(), p)
        assert (d.action, d.reason) == ("EXIT", "SQUARE_OFF"), f"g={g}"


def test_decide_square_off_boundary_either_side(seam):
    """No-fire at 15:28:59, fire at 15:29:00 — the exact production boundary."""
    p = _params(square_off_time="15:29")
    seam(_at(15, 28, 59))
    assert te.decide(0.06, te.TrailState(armed=True, peak=0.06), p).action == "HOLD"
    seam(_at(15, 29, 0))
    assert te.decide(0.06, te.TrailState(armed=True, peak=0.06), p).reason == "SQUARE_OFF"


def test_decide_positional_ignores_square_off_at_any_clock(seam):
    """square_off_enabled=False (POSITIONAL) must never time-exit — no-fire case."""
    p = _params(square_off_enabled=False)
    seam(_at(23, 59, 59))
    assert te.decide(0.06, te.TrailState(armed=True, peak=0.06), p).action == "HOLD"


def test_decide_explicit_now_overrides_the_seam(seam):
    """An explicit now= still wins (callers may thread their own tick clock)."""
    seam(_at(11, 0))
    d = te.decide(0.06, te.TrailState(armed=True, peak=0.06), _params(),
                  now=_at(15, 30))
    assert d.reason == "SQUARE_OFF"


# ── MIS defensive square-off backstop — the REAL 15:12 boundary ─────────────
#
# The pre-existing backstop tests set mis_square_off_time="00:00:01" (a time
# always in the past) so they never exercise the real threshold. These pin the
# PRODUCTION default (15:12) and assert both sides through the seam.

def _mis_cfg(product="MIS"):
    return TradingSessionConfig(
        total_allocated_capital=100000.0, top_n_stocks=1, sizing_mode="equal",
        strategy="portfolio_kill_switch", order_product=product,
        kill_switch_enabled=False, per_position_gtt_enabled=False,
        mis_square_off_time="15:12:00")


def _started_session(product="MIS"):
    seed_signals([("A", 1, 9.0, 100.0)])
    sess = TradingSession.create(_mis_cfg(product), mode="paper")
    asyncio.run(sess.start())          # entry at the conftest's 10:00 seam
    assert sess.status()["n_open_positions"] == 1
    return sess


def test_mis_backstop_does_not_fire_before_1512(clean_positions, brokers, seam):
    """NO-FIRE: one second before the threshold the MIS book stays open."""
    sess = _started_session()
    seam(_at(15, 11, 59))
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is False
    assert sess.status()["n_open_positions"] == 1


def test_mis_backstop_fires_at_1512(clean_positions, brokers, seam):
    """FIRE: at the threshold the MIS book is flattened, tagged MIS_SQUARE_OFF."""
    sess = _started_session()
    seam(_at(15, 12, 0))
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is True
    assert "MIS_SQUARE_OFF" in (out["kill_reason"] or "")
    assert sess.status()["n_open_positions"] == 0


def test_mis_backstop_boundary_same_session_no_fire_then_fire(
        clean_positions, brokers, seam):
    """The seam alone moves the verdict: identical session + identical prices,
    only the clock advances 15:11:59 -> 15:12:00."""
    sess = _started_session()
    seam(_at(15, 11, 59))
    assert asyncio.run(sess.tick())["kill_switch_fired"] is False
    seam(_at(15, 12, 0))
    assert asyncio.run(sess.tick())["kill_switch_fired"] is True


def test_mis_backstop_never_fires_for_cnc_at_any_clock(
        clean_positions, brokers, seam):
    """NO-FIRE: a CNC book must never be squared off by the MIS backstop, even
    well past the threshold. Guards against the routing widening the gate."""
    sess = _started_session(product="CNC")
    seam(_at(15, 20, 0))
    out = asyncio.run(sess.tick())
    assert out["kill_switch_fired"] is False
    assert sess.status()["n_open_positions"] == 1


def test_parse_entry_time_today_stamps_the_passed_now_date(seam):
    """The parse and the comparison must share ONE clock, so the returned
    datetime takes its DATE from the `now` the caller passes (the MIS backstops
    pass the same now_ist() they compare against)."""
    seam(_at(15, 11, 59))
    got = sess_mod._parse_entry_time_today_ist("15:12:00", now_ist())
    assert got == _at(15, 12, 0)
    assert got.utcoffset() == timedelta(hours=5, minutes=30)
    other = sess_mod._parse_entry_time_today_ist(
        "15:12:00", datetime(2026, 6, 26, 9, 0, tzinfo=IST))
    assert other == datetime(2026, 6, 26, 15, 12, tzinfo=IST)


def test_parse_entry_time_default_is_the_raw_clock_by_design(seam):
    """BY DESIGN the DEFAULT is the RAW clock, not the seam.

    _arm_square_off() feeds its target to square_off_scheduler, whose daemon
    sleeps (target - real now) REAL seconds; a timer cannot sleep against a fake
    clock. If this default followed the seam, the target would carry the seam's
    DATE while the daemon measured it against the real clock — two clocks in one
    decision, and the scheduler would refuse to arm ("already passed"). Seam-
    driven callers pass `now` explicitly instead. Locking that contract here.
    """
    seam(_at(15, 11, 59))                      # seam says 2026-06-25
    got = sess_mod._parse_entry_time_today_ist("23:59:00")   # no now= passed
    assert got.date() == datetime.now(IST).date()            # REAL today, not the seam's
    assert (got.hour, got.minute) == (23, 59)


# ── 09:15-15:29 market-hours guard ──────────────────────────────────────────
#
# Observable: the intraday_basket per-stock software stop only fires INSIDE
# market hours (session.py: `if stock_return <= -stop_pct and _in_market_hours`).

def _basket_cfg():
    return TradingSessionConfig(
        total_allocated_capital=30000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False,
        per_stock_stop_enabled=True, stop_pct=0.03,
        square_off_enabled=False,      # isolate the market-hours guard from
                                       # the 15:29 square-off branch
        entry_time="09:15:00", square_off_time="15:29:00")


def _started_basket(brokers):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 100.0),
                  ("C", 3, 7.0, 100.0)])
    sess = TradingSession.create(_basket_cfg(), mode="paper")
    asyncio.run(sess.start())
    return sess


def test_market_hours_guard_admits_inside_hours(clean_positions, brokers, seam):
    """FIRE: at 11:00 a stock through its stop is cut."""
    sess = _started_basket(brokers)
    brokers["A"] = 90.0            # -10% -> through the 3% per-stock stop
    seam(_at(11, 0))
    out = asyncio.run(sess.tick())
    assert len(out.get("per_stock_exits") or []) == 1


def test_market_hours_guard_refuses_before_open(clean_positions, brokers, seam):
    """NO-FIRE: same -10% at 09:14:59 must NOT cut — pre-open marks are stale."""
    sess = _started_basket(brokers)
    brokers["A"] = 90.0
    seam(_at(9, 14, 59))
    out = asyncio.run(sess.tick())
    assert len(out.get("per_stock_exits") or []) == 0


def test_market_hours_guard_refuses_after_close(clean_positions, brokers, seam):
    """NO-FIRE: same -10% at 15:29:01 must NOT cut."""
    sess = _started_basket(brokers)
    brokers["A"] = 90.0
    seam(_at(15, 29, 1))
    out = asyncio.run(sess.tick())
    assert len(out.get("per_stock_exits") or []) == 0


def test_market_hours_guard_admits_exactly_at_open(clean_positions, brokers, seam):
    """Boundary is INCLUSIVE at 09:15:00 (>= open)."""
    sess = _started_basket(brokers)
    brokers["A"] = 90.0
    seam(_at(9, 15, 0))
    out = asyncio.run(sess.tick())
    assert len(out.get("per_stock_exits") or []) == 1


# ── exit_gate vocabulary ────────────────────────────────────────────────────

def test_mis_square_off_is_a_recognised_exit_reason():
    """MIS_SQUARE_OFF is the close_reason every square-off path fires with; it
    was logging 'unrecognised reason ... (allowing)' on every real square-off."""
    from autotrade import exit_gate
    assert "MIS_SQUARE_OFF" in exit_gate.VALID_REASONS


def test_unknown_exit_reason_still_fails_open(clean_positions, caplog):
    """SAFETY: the gate must NEVER block an exit on an unknown reason. Adding to
    the vocabulary must not have turned the warning into a rejection."""
    from autotrade import exit_gate
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_positions
               (session_id, symbol, qty, avg_price, status)
               VALUES ('seam-gate-test','ZZZ',1,100.0,'OPEN')""")
        con.commit()
    assert exit_gate.claim_exit_session(
        "seam-gate-test", "ZZZ", "SOME_BRAND_NEW_REASON") is True
