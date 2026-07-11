"""CLUSTER 3 ITEM 1 — resume a session stranded in KILLING / KILLING_INCOMPLETE.

A crash MID-FLATTEN leaves a session KILLING_INCOMPLETE with some legs OPEN and
some EXIT_FAILED and NO driver → real exposure, zero automation. recovery must
resume it: reconcile → re-flatten the OPEN leg → re-arm the tick driver so the
EXIT_FAILED sweep drives the rest to CLOSED.

MUTATION (verified): revert recovery._active_sessions to
`WHERE status IN ('RUNNING','SCHEDULED')` (exclude the kill states) → resume skips
the session → the OPEN leg is never re-flattened (stays OPEN) and the session
never reaches CLOSED → this test's asserts fail.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import recovery
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mk(monkeypatch, *, fail_symbols=None):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"A": 99.0, "B": 99.0},
                          fail_symbols=set(fail_symbols or set()))

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _set_status(session_id, status):
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status=? WHERE session_id=?",
                    (status, session_id))
        con.commit()


def _sess_status(session_id):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_sessions WHERE session_id=?",
                        (session_id,)).fetchone()
    return r["status"] if r else None


def _pos(session_id, sym):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (session_id, sym)).fetchone()
    return r["status"] if r else None


def _force_market_hours(monkeypatch):
    import autotrade.session as sess_mod
    _real = sess_mod.datetime

    class _DT(_real):
        @classmethod
        def now(cls, tz=None):
            base = _real(2026, 6, 25, 10, 0, 0)
            return base.replace(tzinfo=tz) if tz else base

    monkeypatch.setattr(sess_mod, "datetime", _DT)


def test_resume_killing_incomplete_drives_to_closed(clean_positions, monkeypatch):
    sess = _mk(monkeypatch)   # broker healthy — exits succeed
    prof = sess.config.broker_profiles[0].profile_id
    # A = OPEN (never got an exit attempt before the crash);
    # B = EXIT_FAILED (a prior exit failed).
    for sym in ("A", "B"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()
    sess.registry.mark_exit_failed("B", "prior failure", broker_profile=prof)
    _set_status(sess.session_id, "KILLING_INCOMPLETE")

    assert _pos(sess.session_id, "A") == "OPEN"
    assert _pos(sess.session_id, "B") == "EXIT_FAILED"

    # RESUME: reconciles + re-flattens the OPEN leg + re-arms the tick driver.
    summary = recovery.resume_active_sessions()
    assert summary.get("killing", 0) == 1

    # The OPEN leg A was re-flattened synchronously on resume.
    assert _pos(sess.session_id, "A") == "CLOSED"

    # The tick driver's EXIT_FAILED sweep (driven manually here — autostart off in
    # tests) clears B and promotes the session to CLOSED.
    _force_market_hours(monkeypatch)
    asyncio.run(sess.tick())
    assert _pos(sess.session_id, "B") == "CLOSED"
    assert _sess_status(sess.session_id) == "CLOSED"


def test_resume_bare_killing_normalises_and_reflattens(clean_positions, monkeypatch):
    """A bare KILLING (crash before the flatten finished) with only an OPEN leg is
    resumed: reconcile → re-flatten → since nothing is left unflat, promote to
    CLOSED on resume."""
    sess = _mk(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC",
                           instrument_type="EQ", exchange="NSE")
    sess.registry.update_ltp("A", 99.0, broker_profile=prof)
    sess.monitor.freeze_invested_basis()
    _set_status(sess.session_id, "KILLING")

    recovery.resume_active_sessions()
    assert _pos(sess.session_id, "A") == "CLOSED"
    assert _sess_status(sess.session_id) == "CLOSED"
