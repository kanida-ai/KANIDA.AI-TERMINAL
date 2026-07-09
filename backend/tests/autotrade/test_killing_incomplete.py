"""C2 — a kill that strands an EXIT_FAILED leg must NOT mark the session CLOSED.

If any leg fails/times out, kill_switch.fire sets the NON-terminal
KILLING_INCOMPLETE status so the tick driver keeps servicing the EXIT_FAILED
retry sweep (instead of self-stopping and stranding the leg). tick() promotes it
to CLOSED only once every leg is confirmed flat. The all-success path is
byte-for-byte CLOSED.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
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


def _mk(monkeypatch, *, fail_symbols=None, ltps=None):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps or {},
                        fail_symbols=set(fail_symbols or set()))
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=True,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    sess.monitor.freeze_invested_basis()
    return sess, created


def _reg3(sess, ltps):
    prof = sess.config.broker_profiles[0].profile_id
    for sym in ("A", "B", "C"):
        sess.registry.register(symbol=sym, broker_profile=prof, qty=100,
                               avg_price=100.0, product="CNC",
                               instrument_type="EQ", exchange="NSE")
        sess.registry.update_ltp(sym, ltps[sym], broker_profile=prof)
    sess.monitor.freeze_invested_basis()


def _status(sess):
    return sess._current_status()


def _pos(sess, sym):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, sym)).fetchone()
    return r["status"] if r else None


def test_all_success_marks_closed(clean_positions, monkeypatch):
    ltps = {"A": 99.0, "B": 99.0, "C": 99.0}
    sess, _ = _mk(monkeypatch, ltps=ltps)
    _reg3(sess, ltps)

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))
    assert _status(sess) == "CLOSED"


def test_one_failed_leg_keeps_session_non_terminal(clean_positions, monkeypatch):
    ltps = {"A": 99.0, "B": 99.0, "C": 99.0}
    sess, created = _mk(monkeypatch, fail_symbols={"B"}, ltps=ltps)
    _reg3(sess, ltps)

    summary = asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))
    assert summary["n_exit_failed"] == 1
    # NOT terminal — the retry path must keep running.
    assert _status(sess) == "KILLING_INCOMPLETE"
    assert _pos(sess, "B") == "EXIT_FAILED"
    assert _pos(sess, "A") == "CLOSED"
    assert _pos(sess, "C") == "CLOSED"


def _force_market_hours(monkeypatch):
    """tick()'s EXIT_FAILED-retry sweep is gated on the REAL wall clock
    (09:15–15:29 IST), not the fake-now seam, so pin datetime.now() into market
    hours for this test regardless of when the suite runs."""
    import autotrade.session as sess_mod
    _real = sess_mod.datetime

    class _DT(_real):
        @classmethod
        def now(cls, tz=None):
            base = _real(2026, 6, 25, 10, 0, 0)
            return base.replace(tzinfo=tz) if tz else base

    monkeypatch.setattr(sess_mod, "datetime", _DT)


def test_retry_sweep_still_active_then_closes(clean_positions, monkeypatch):
    """A KILLING_INCOMPLETE session keeps ticking; once the leg finally exits the
    next tick promotes it to CLOSED."""
    ltps = {"A": 99.0, "B": 99.0, "C": 99.0}
    sess, created = _mk(monkeypatch, fail_symbols={"B"}, ltps=ltps)
    _reg3(sess, ltps)
    _force_market_hours(monkeypatch)
    prof = sess.config.broker_profiles[0].profile_id

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))
    assert _status(sess) == "KILLING_INCOMPLETE"

    # Tick while B still fails → retry re-fails → stays KILLING_INCOMPLETE.
    out = asyncio.run(sess.tick())
    assert out.get("killing_incomplete") is True
    assert _status(sess) == "KILLING_INCOMPLETE"
    assert _pos(sess, "B") == "EXIT_FAILED"

    # Broker recovers → next tick's retry sweep exits B → session CLOSED.
    created[prof].fail_symbols = set()
    out = asyncio.run(sess.tick())
    assert _status(sess) == "CLOSED"
    assert _pos(sess, "B") == "CLOSED"
