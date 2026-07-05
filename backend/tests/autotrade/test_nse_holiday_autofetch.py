"""Tests for the SELF-UPDATING NSE holiday source + the coverage safety guard.

WHY (real-money safety): trading_calendar hardcoded 2025/2026 holidays. For any
future date OUT of ohlc_daily coverage, is_trading_day returned "iso not in
holidays" — so in an un-seeded year EVERY holiday would silently read as a
TRADING day and the scheduler could fire trades on a real NSE holiday.

The fix (tested here):
  * nse_holiday_source: fetch the CM (equity) holiday list from NSE, parse to
    ISO, persist to a JSON cache. Best-effort — network/parse errors NEVER raise;
    the last good cache is kept. refresh_if_stale(max_age_days) only re-fetches
    when stale/missing.
  * trading_calendar merges seed ∪ override-file ∪ fetched-cache (cache read
    per-call, like the override, so a refresh needs no restart).
  * A COVERAGE GUARD: covered_years() / is_calendar_authoritative(d) /
    assert_calendar_covers(d) raising CalendarCoverageError; enforced at the
    SCHEDULING/FIRE decision points (config.validate on entry_date, the fire
    gate, the max-hold cap) — NOT inside is_trading_day (kept non-throwing).

NO NETWORK: every test monkeypatches the raw fetch to a saved fixture.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from autotrade import nse_holiday_source as src
from autotrade import trading_calendar as cal
from autotrade.config import TradingSessionConfig

IST = timezone(timedelta(hours=5, minutes=30))


# ── A saved sample of the NSE /api/holiday-master?type=trading CM payload ───────
# Shape mirrors the real API: {"CM": [{"tradingDate":"15-Jan-2026", ...}, ...]}.
# We include a 2027 date to prove fetched years extend coverage past the seed.
SAMPLE_PAYLOAD = {
    "CM": [
        {"tradingDate": "26-Jan-2026", "weekDay": "Monday",
         "description": "Republic Day"},
        {"tradingDate": "03-Mar-2026", "weekDay": "Tuesday",
         "description": "Holi"},
        {"tradingDate": "25-Dec-2026", "weekDay": "Friday",
         "description": "Christmas"},
        # NEXT YEAR — the whole point: fetching extends coverage.
        {"tradingDate": "26-Jan-2027", "weekDay": "Tuesday",
         "description": "Republic Day"},
        {"tradingDate": "01-May-2027", "weekDay": "Saturday",
         "description": "Maharashtra Day"},
    ],
    "FO": [  # a segment we must IGNORE
        {"tradingDate": "01-Jan-1999", "weekDay": "Friday",
         "description": "should be ignored"},
    ],
}


@pytest.fixture
def tmp_cache(tmp_path, monkeypatch):
    """Point the source module's cache at a temp file + patch the raw fetch to
    return the saved sample (NO network). Yields the cache Path."""
    cache_file = tmp_path / "nse_holidays_cache.json"
    monkeypatch.setattr(src, "_cache_path", lambda: cache_file)
    monkeypatch.setattr(src, "_raw_fetch_cm", lambda: SAMPLE_PAYLOAD)
    # Ensure trading_calendar reads THIS cache too.
    monkeypatch.setattr(cal, "_fetched_cache_path", src._cache_path, raising=False)
    return cache_file


# ── fetch + parse ──────────────────────────────────────────────────────────────

def test_parse_cm_payload_to_iso():
    got = src._parse_cm_holidays(SAMPLE_PAYLOAD)
    assert "2026-01-26" in got
    assert "2026-03-03" in got
    assert "2026-12-25" in got
    assert "2027-01-26" in got
    assert "2027-05-01" in got
    # FO segment ignored.
    assert "1999-01-01" not in got


def test_fetch_and_cache_writes_file(tmp_cache):
    result = src.fetch_and_cache()
    assert result is not None
    assert tmp_cache.exists()
    saved = json.loads(tmp_cache.read_text(encoding="utf-8"))
    assert "fetched_at" in saved and "holidays" in saved and "years" in saved
    assert "2027-01-26" in saved["holidays"]
    assert 2027 in saved["years"] and 2026 in saved["years"]


def test_load_cache_roundtrip(tmp_cache):
    src.fetch_and_cache()
    hol = src.load_cached_holidays()
    yrs = src.load_cached_years()
    assert "2027-01-26" in hol
    assert 2026 in yrs and 2027 in yrs


# ── stale logic ────────────────────────────────────────────────────────────────

def test_refresh_if_stale_fetches_when_missing(tmp_cache):
    assert not tmp_cache.exists()
    did = src.refresh_if_stale(max_age_days=7)
    assert did is True
    assert tmp_cache.exists()


def test_refresh_if_stale_skips_when_fresh(tmp_cache, monkeypatch):
    src.fetch_and_cache()  # fresh cache now on disk
    calls = {"n": 0}

    def _counting_fetch():
        calls["n"] += 1
        return SAMPLE_PAYLOAD
    monkeypatch.setattr(src, "_raw_fetch_cm", _counting_fetch)
    did = src.refresh_if_stale(max_age_days=7)
    assert did is False
    assert calls["n"] == 0  # not re-fetched


def test_refresh_if_stale_refetches_when_old(tmp_cache):
    # Write a cache stamped 30 days ago → stale for max_age_days=7.
    old = (datetime.now(IST) - timedelta(days=30)).isoformat()
    tmp_cache.write_text(json.dumps(
        {"fetched_at": old, "holidays": ["2026-01-26"], "years": [2026]}),
        encoding="utf-8")
    did = src.refresh_if_stale(max_age_days=7)
    assert did is True
    saved = json.loads(tmp_cache.read_text(encoding="utf-8"))
    assert "2027-01-26" in saved["holidays"]  # re-fetched sample


# ── best-effort: fetch failure NEVER raises + keeps last good cache ─────────────

def test_fetch_failure_never_raises_and_keeps_cache(tmp_cache, monkeypatch):
    src.fetch_and_cache()  # good cache
    before = tmp_cache.read_text(encoding="utf-8")

    def _boom():
        raise RuntimeError("network down")
    monkeypatch.setattr(src, "_raw_fetch_cm", _boom)
    # Neither call raises.
    assert src.fetch_and_cache() is None
    assert src.refresh_if_stale(max_age_days=0) is False
    # Prior cache untouched.
    assert tmp_cache.read_text(encoding="utf-8") == before


def test_load_cache_missing_returns_empty(tmp_cache):
    assert not tmp_cache.exists()
    assert src.load_cached_holidays() == set()
    assert src.load_cached_years() == set()


# ── merge into trading_calendar ────────────────────────────────────────────────

def test_merged_holiday_set_includes_fetched_dates(tmp_cache):
    src.fetch_and_cache()
    # 2027-01-26 is NOT in the hardcoded seed nor the override file, only fetched.
    assert cal.is_holiday("2027-01-26") is True
    assert cal.is_trading_day("2027-01-26") is False  # Tue, but a fetched holiday
    # And a NON-holiday 2027 weekday stays a trading day.
    assert cal.is_trading_day("2027-01-27") is True   # Wed, not a holiday


def test_merge_does_not_lose_seed_or_override(tmp_cache):
    src.fetch_and_cache()
    assert cal.is_holiday("2026-01-26") is True        # seed
    assert cal.is_holiday("2026-06-26") is True        # override file


# ── coverage guard ─────────────────────────────────────────────────────────────

def test_covered_years_reflects_seed_override_cache(tmp_cache):
    src.fetch_and_cache()
    yrs = cal.covered_years()
    assert 2025 in yrs   # seed
    assert 2026 in yrs   # seed + override
    assert 2027 in yrs   # fetched cache


def test_is_calendar_authoritative(tmp_cache):
    src.fetch_and_cache()
    assert cal.is_calendar_authoritative("2026-06-29") is True
    assert cal.is_calendar_authoritative("2027-03-10") is True   # fetched year
    # A far future year with no seed/override/cache/ohlc coverage.
    assert cal.is_calendar_authoritative("2029-06-01") is False


def test_assert_calendar_covers_raises_for_uncovered_year(tmp_cache):
    src.fetch_and_cache()
    from datetime import date
    with pytest.raises(cal.CalendarCoverageError) as ei:
        cal.assert_calendar_covers(date(2029, 6, 1))
    msg = str(ei.value)
    assert "2029" in msg
    assert "nse_holidays.txt" in msg  # names the remedy


def test_assert_calendar_covers_passes_for_2026(tmp_cache):
    # No exception for a covered year.
    from datetime import date
    cal.assert_calendar_covers(date(2026, 6, 29))


# ── guard is a NO-OP for currently-covered years (nothing breaks today) ─────────

def test_guard_noop_for_2026_without_any_cache():
    # Even with NO fetched cache at all, 2025/2026 are covered by the seed.
    from datetime import date
    cal.assert_calendar_covers(date(2026, 6, 29))
    cal.assert_calendar_covers(date(2025, 3, 14))
    assert cal.is_calendar_authoritative("2026-01-05") is True


def test_config_validate_covered_year_still_works():
    # A covered-year schedule (2026 trading day) validates fine — NO regression.
    cfg = TradingSessionConfig(
        total_allocated_capital=500000.0, entry_date="2026-06-29")
    cfg.validate()  # must not raise


def test_config_validate_refuses_uncovered_year():
    # entry_date in an uncovered future year → CalendarCoverageError at validate.
    cfg = TradingSessionConfig(
        total_allocated_capital=500000.0, entry_date="2029-06-01")
    with pytest.raises(cal.CalendarCoverageError):
        cfg.validate()


def test_max_hold_cap_refuses_uncovered_year():
    from autotrade.session import compute_max_hold_cap_datetime
    # An entry stamped so that the Nth-session cap lands in 2029 (uncovered).
    with pytest.raises(cal.CalendarCoverageError):
        compute_max_hold_cap_datetime("2029-05-28T09:15:00+05:30", 3, "15:29:00")


def test_max_hold_cap_covered_year_ok():
    from autotrade.session import compute_max_hold_cap_datetime
    dt = compute_max_hold_cap_datetime("2026-06-25T09:15:00+05:30", 3, "15:29:00")
    assert dt is not None and dt.year == 2026
