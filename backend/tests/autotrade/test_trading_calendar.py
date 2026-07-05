"""Tests for the EXECUTION-DATE / TRADING-DAY rule:
  * trading_calendar.is_trading_day / next_trading_day / is_market_open
  * config.validate() rejecting a non-trading-day entry_date
  * config.resolve_fire_datetime (today vs next trading session)
  * session.evaluate_fire_gate (the single fire decision) — all branches
  * a 'now' instant start on a non-trading day / closed market refused, no order

DETERMINISM: 'now' is always passed explicitly (frozen) — never the wall clock.

Calendar anchors used:
  2026-06-25 Thu  → trading day, mid-session at 10:00 IST
  2026-06-26 Fri  → NSE HOLIDAY (Muharram, via the override file)
  2026-06-27 Sat  → weekend
  2026-06-28 Sun  → weekend
  2026-06-29 Mon  → trading day
  2026-07-02 Thu / 2026-07-03 Fri → consecutive trading days
  2026-01-26 Mon  → Republic Day (in NSE_HOLIDAYS)
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade import trading_calendar as cal
from autotrade.config import TradingSessionConfig
from autotrade.session import evaluate_fire_gate

IST = timezone(timedelta(hours=5, minutes=30))


def _cfg(**kw) -> TradingSessionConfig:
    base = dict(total_allocated_capital=500000.0, kill_switch_enabled=False)
    base.update(kw)
    return TradingSessionConfig(**base)


# ── trading_calendar primitives ───────────────────────────────────────────────

def test_is_trading_day_weekday_vs_weekend():
    assert cal.is_trading_day("2026-06-24") is True   # Wed
    assert cal.is_trading_day("2026-06-25") is True   # Thu
    assert cal.is_trading_day("2026-06-27") is False  # Sat
    assert cal.is_trading_day("2026-06-28") is False  # Sun
    assert cal.is_trading_day("2026-06-29") is True   # Mon


def test_is_trading_day_holiday():
    assert cal.is_trading_day("2026-01-26") is False  # Republic Day
    assert cal.is_trading_day("2026-04-03") is False  # Good Friday
    assert cal.is_trading_day("2026-06-26") is False  # Muharram (override file)


# GUARD: pin the full 2026 NSE holiday calendar (reconciled 2026-07-05 vs the
# official NSE circular via 3 agreeing public sources). This test exists so the
# calendar can NEVER silently drift again — if someone edits a date, this fails.
# Weekend-only festivals are intentionally excluded (already non-trading).
NSE_2026_HOLIDAYS = [
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26",
    "2026-09-14", "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24",
    "2026-12-25",
]
# Dates that were WRONG in a prior list and must be TRADING days now.
NSE_2026_MUST_TRADE = [
    "2026-03-04", "2026-04-01", "2026-05-27", "2026-07-06",
    "2026-08-28", "2026-10-21", "2026-11-09",
]


def test_2026_holiday_calendar_matches_official():
    for d in NSE_2026_HOLIDAYS:
        assert cal.is_trading_day(d) is False, f"{d} must be an NSE holiday"


def test_2026_prior_wrong_dates_are_trading_days():
    for d in NSE_2026_MUST_TRADE:
        # weekend-safe: only assert for weekdays (Sat/Sun are non-trading anyway)
        from datetime import date as _date
        y, m, dd = map(int, d.split("-"))
        if _date(y, m, dd).weekday() < 5:
            assert cal.is_trading_day(d) is True, f"{d} must be a trading day"


def test_next_trading_day_skips_weekend_and_holiday():
    # Friday 06-26 → next is Monday 06-29 (skips Sat/Sun).
    assert cal.next_trading_day("2026-06-26").isoformat() == "2026-06-29"
    # Day before Republic Day (Sun 2026-01-25) → 01-27 (26th is a holiday).
    assert cal.next_trading_day("2026-01-25").isoformat() == "2026-01-27"


def test_next_trading_day_inclusive():
    assert cal.next_trading_day("2026-06-25", inclusive=True).isoformat() == "2026-06-25"
    assert cal.next_trading_day("2026-06-27", inclusive=True).isoformat() == "2026-06-29"


def test_is_market_open_window():
    d = "2026-06-25"  # trading day
    assert cal.is_market_open(datetime(2026, 6, 25, 9, 14, tzinfo=IST)) is False
    assert cal.is_market_open(datetime(2026, 6, 25, 9, 15, tzinfo=IST)) is True
    assert cal.is_market_open(datetime(2026, 6, 25, 12, 0, tzinfo=IST)) is True
    assert cal.is_market_open(datetime(2026, 6, 25, 15, 29, tzinfo=IST)) is True
    assert cal.is_market_open(datetime(2026, 6, 25, 15, 30, tzinfo=IST)) is False
    # Sunday during the clock window → closed.
    assert cal.is_market_open(datetime(2026, 6, 28, 11, 0, tzinfo=IST)) is False


# ── config.validate(): non-trading-day entry_date rejected ────────────────────

def test_validate_rejects_sunday_entry_date():
    cfg = _cfg(entry_date="2026-06-28")  # Sunday
    with pytest.raises(ValueError, match="NOT an NSE trading day"):
        cfg.validate()


def test_validate_rejects_holiday_entry_date_with_suggestion():
    cfg = _cfg(entry_date="2026-01-26")  # Republic Day
    with pytest.raises(ValueError, match="Next trading day: 2026-01-27"):
        cfg.validate()


def test_validate_accepts_trading_day_entry_date():
    _cfg(entry_date="2026-06-25").validate()  # no raise


def test_validate_bad_on_missed_window():
    with pytest.raises(ValueError, match="on_missed_window"):
        _cfg(on_missed_window="nope").validate()


# ── config.resolve_fire_datetime ──────────────────────────────────────────────

def test_resolve_explicit_entry_date():
    cfg = _cfg(entry_date="2026-07-03", entry_time="09:15:00")
    fdt = cfg.resolve_fire_datetime(datetime(2026, 7, 2, 10, 0, tzinfo=IST))
    assert fdt == datetime(2026, 7, 3, 9, 15, tzinfo=IST)


def test_resolve_unset_today_future_clock():
    # Today is a trading day and entry_time is still ahead → today.
    cfg = _cfg(entry_time="14:00:00")
    now = datetime(2026, 6, 25, 10, 0, tzinfo=IST)
    assert cfg.resolve_fire_datetime(now) == datetime(2026, 6, 25, 14, 0, tzinfo=IST)


def test_resolve_unset_today_past_clock_rolls_next_day():
    # Today is a trading day but entry_time already passed → next trading day.
    cfg = _cfg(entry_time="09:15:00")
    now = datetime(2026, 7, 2, 10, 0, tzinfo=IST)
    assert cfg.resolve_fire_datetime(now) == datetime(2026, 7, 3, 9, 15, tzinfo=IST)


def test_resolve_unset_on_weekend_rolls_to_monday():
    cfg = _cfg(entry_time="09:15:00")
    now = datetime(2026, 6, 28, 11, 0, tzinfo=IST)  # Sunday
    assert cfg.resolve_fire_datetime(now) == datetime(2026, 6, 29, 9, 15, tzinfo=IST)


# ── evaluate_fire_gate: every branch ──────────────────────────────────────────

def test_gate_fires_when_trading_day_and_open_within_grace():
    cfg = _cfg(entry_date="2026-06-25", entry_time="10:00:00", entry_grace_seconds=120)
    now = datetime(2026, 6, 25, 10, 0, 30, tzinfo=IST)  # 30s past, within grace
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is True
    assert g.status == "RUNNING"


def test_gate_future_target_waits():
    cfg = _cfg(entry_date="2026-06-25", entry_time="14:00:00")
    now = datetime(2026, 6, 25, 10, 0, tzinfo=IST)
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is False
    assert g.status == "SCHEDULED"
    assert g.carry_to is None


def test_gate_before_bell_defers():
    cfg = _cfg(entry_date="2026-06-25", entry_time="09:15:00")
    now = datetime(2026, 6, 25, 8, 0, tzinfo=IST)  # before open, target also 9:15
    # target 09:15 > now 08:00 → future → SCHEDULED (waits for the bell).
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is False
    assert g.status == "SCHEDULED"


def test_gate_past_beyond_grace_expires():
    cfg = _cfg(entry_date="2026-06-25", entry_time="10:00:00",
               entry_grace_seconds=60, on_missed_window="expire")
    now = datetime(2026, 6, 25, 10, 5, 0, tzinfo=IST)  # 5min past, > 60s grace
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is False
    assert g.status == "EXPIRED_MISSED_WINDOW"


def test_gate_after_close_expires():
    cfg = _cfg(entry_date="2026-06-25", entry_time="10:00:00")
    now = datetime(2026, 6, 25, 16, 0, 0, tzinfo=IST)  # after 15:30 close
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is False
    assert g.status == "EXPIRED_MISSED_WINDOW"


def test_gate_non_trading_day_rejected():
    # entry_date can't be a Sunday (validate rejects), but a resolved/unset
    # config landing on a non-trading day via fire_dt must reject.
    cfg = _cfg(entry_time="10:00:00", on_missed_window="expire")
    now = datetime(2026, 6, 28, 11, 0, tzinfo=IST)  # Sunday
    sunday_target = datetime(2026, 6, 28, 10, 0, tzinfo=IST)
    g = evaluate_fire_gate(cfg, now, fire_dt=sunday_target)
    assert g.allow is False
    assert g.status == "REJECTED_NON_TRADING_DAY"


def test_gate_non_trading_day_carry():
    cfg = _cfg(entry_time="10:00:00", on_missed_window="carry_next_trading_day")
    now = datetime(2026, 6, 28, 11, 0, tzinfo=IST)  # Sunday
    sunday_target = datetime(2026, 6, 28, 10, 0, tzinfo=IST)
    g = evaluate_fire_gate(cfg, now, fire_dt=sunday_target)
    assert g.allow is False
    assert g.status == "SCHEDULED"
    assert g.carry_to == "2026-06-29"  # Monday


def test_gate_missed_window_carry_rolls_forward():
    cfg = _cfg(entry_date="2026-07-02", entry_time="09:15:00",
               on_missed_window="carry_next_trading_day")
    now = datetime(2026, 7, 2, 10, 0, tzinfo=IST)  # 09:15 missed
    g = evaluate_fire_gate(cfg, now)
    assert g.allow is False
    assert g.carry_to == "2026-07-03"  # Fri (next trading day)
