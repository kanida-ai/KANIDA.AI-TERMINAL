"""C3 — EXIT_FAILED (still-held) legs must be VISIBLE + count in gross_return.

An EXIT_FAILED leg is dropped from open_positions (OPEN-only) and from the
realised total (CLOSED-only), while invested_basis (frozen at entry) still counts
it — so % understated real exposure and the panel was blind to the stranded leg.
This surfaces the row in status() with an explicit flag and folds its live uPnL
back into the gross-return numerator.
"""
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


def _mk(monkeypatch, ltps):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=ltps)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               kill_switch_pct=0.02, order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _reg(sess, sym, qty, avg, ltp, status="OPEN"):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=sym, broker_profile=prof, qty=qty,
                           avg_price=avg, product="CNC", instrument_type="EQ",
                           exchange="NSE")
    sess.registry.update_ltp(sym, ltp, broker_profile=prof)
    if status != "OPEN":
        with falcon_conn() as con:
            con.execute("UPDATE autotrade_positions SET status=? "
                        "WHERE session_id=? AND symbol=?",
                        (status, sess.session_id, sym))
            con.commit()


def _strand(sess, sym):
    """Mark a leg EXIT_FAILED AFTER the invested basis froze (as happens live —
    the basis is frozen at entry with the leg OPEN, then an exit fails)."""
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_positions SET status='EXIT_FAILED' "
                    "WHERE session_id=? AND symbol=?", (sess.session_id, sym))
        con.commit()


def test_exit_failed_row_surfaced_in_status(clean_positions, monkeypatch):
    sess = _mk(monkeypatch, {"A": 99.0, "B": 90.0})
    _reg(sess, "A", 100, 100.0, 99.0, status="OPEN")
    _reg(sess, "B", 100, 100.0, 90.0, status="OPEN")   # still held, -1000
    sess.monitor.freeze_invested_basis()
    _strand(sess, "B")

    st = sess.status()
    assert st["has_exit_failed"] is True
    assert st["n_exit_failed_positions"] == 1
    assert st["exit_failed_positions"][0]["symbol"] == "B"
    assert st["exit_failed_positions"][0]["exit_failed"] is True
    # B is NOT in the OPEN list (that's the invisibility being fixed).
    assert all(p["symbol"] != "B" for p in st["open_positions"])


def test_exit_failed_upnl_counts_in_gross_return(clean_positions, monkeypatch):
    sess = _mk(monkeypatch, {"A": 100.0, "B": 90.0})
    # A flat (uPnL 0), B EXIT_FAILED at -1000 (100 sh × -10). invested_basis =
    # 100*100 + 100*100 = 20000. gross_return_invested should be -1000/20000.
    _reg(sess, "A", 100, 100.0, 100.0, status="OPEN")
    _reg(sess, "B", 100, 100.0, 90.0, status="OPEN")
    sess.monitor.freeze_invested_basis()
    _strand(sess, "B")

    gr = sess.monitor.compute_gross_return_invested()
    assert gr == pytest.approx(-1000.0 / 20000.0)
    # The EXIT_FAILED loss is present in the on-fund view too.
    assert sess.monitor.compute_gross_return() == pytest.approx(-1000.0 / 300000.0)


def test_no_exit_failed_is_byte_identical(clean_positions, monkeypatch):
    sess = _mk(monkeypatch, {"A": 110.0})
    _reg(sess, "A", 100, 100.0, 110.0, status="OPEN")  # +1000
    sess.monitor.freeze_invested_basis()
    st = sess.status()
    assert st["has_exit_failed"] is False
    assert st["n_exit_failed_positions"] == 0
    # Numerator unchanged when there are no EXIT_FAILED rows.
    assert sess.monitor.compute_gross_return_invested() == pytest.approx(
        1000.0 / 10000.0)
