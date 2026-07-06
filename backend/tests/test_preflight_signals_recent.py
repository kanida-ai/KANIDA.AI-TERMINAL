"""check_signals_recent — trading-day-aware freshness gate.

Regression for 2026-07-06: the check measured a CALENDAR-day gap and blocked
anything >1 day old, so every Monday morning (Fri->Mon = 3 cal. days) and every
post-holiday morning went RED — stranding the 09:15 auto-trade entries even
though Friday's close IS the freshest possible data. The gate must measure
TRADING days (compare against the previous trading day).

DB + clock are stubbed; the REAL NSE trading calendar is used for the
previous-trading-day math (so weekend/holiday logic is exercised for real).
"""
from datetime import datetime

import pytest

from falcon import preflight as pf
from falcon.preflight import IST


class _Cur:
    def __init__(self, v):
        self.v = v

    def fetchone(self):
        return (self.v,)


class _FakeCon:
    """Answers the two MAX() queries check_signals_recent runs."""
    def __init__(self, sig, ohlc):
        self.sig, self.ohlc = sig, ohlc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql):
        return _Cur(self.sig if "falcon_signals_live" in sql else self.ohlc)


@pytest.fixture
def stub(monkeypatch):
    def _apply(now, sig, ohlc):
        monkeypatch.setattr(pf, "_now_ist", lambda: now)
        monkeypatch.setattr(pf, "_falcon_conn", lambda: _FakeCon(sig, ohlc))
    return _apply


def _at(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=IST)


def test_monday_with_friday_signals_is_green(stub):
    """THE BUG: Mon 09:15 with Fri signals must be GREEN (Fri = prev trading day)."""
    stub(_at(2026, 7, 6, 9, 15), "2026-07-03", "2026-07-03")   # Mon / Fri
    r = pf.check_signals_recent(None, {})
    assert r.status == pf.GREEN, r.detail


def test_weekday_with_yesterday_signals_is_green(stub):
    stub(_at(2026, 7, 7, 9, 15), "2026-07-06", "2026-07-06")   # Tue / Mon
    assert pf.check_signals_recent(None, {}).status == pf.GREEN


def test_weekday_past_open_genuinely_stale_is_red(stub):
    """Signals OLDER than the previous trading day, past 09:00 → RED (real miss)."""
    stub(_at(2026, 7, 6, 9, 15), "2026-07-02", "2026-07-02")   # Mon, but Thu signals
    r = pf.check_signals_recent(None, {})
    assert r.status == pf.RED and "older than the previous trading day" in r.detail


def test_preopen_stale_is_soft_not_red(stub):
    """Before 09:00 the EOD may still be pending → never a hard RED."""
    stub(_at(2026, 7, 6, 8, 30), "2026-07-02", "2026-07-02")
    assert pf.check_signals_recent(None, {}).status != pf.RED


def test_ohlc_lag_is_red(stub):
    """Fresh OHLC bar but stale signals → RED (pipeline ordering bug)."""
    stub(_at(2026, 7, 6, 9, 15), "2026-07-02", "2026-07-03")
    r = pf.check_signals_recent(None, {})
    assert r.status == pf.RED and "signals lag OHLC" in r.detail


def test_no_signals_is_red(stub):
    stub(_at(2026, 7, 6, 9, 15), None, None)
    assert pf.check_signals_recent(None, {}).status == pf.RED


def test_previous_trading_day_skips_weekend():
    from autotrade.trading_calendar import previous_trading_day
    # Monday 2026-07-06 → previous trading day is Friday 2026-07-03.
    assert previous_trading_day("2026-07-06").isoformat() == "2026-07-03"
    # inclusive: a trading day maps to itself.
    assert previous_trading_day("2026-07-06", inclusive=True).isoformat() == "2026-07-06"
