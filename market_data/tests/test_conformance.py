"""The provider conformance suite (contract §1, last paragraph).

    "any provider must pass — bar alignment to the 15-minute grid, no
     overlapping/duplicate bars, session boundaries, `candle_complete`
     correctness, delay honoured, missing data reported rather than
     fabricated, idempotent re-fetch of the same window."

``assert_provider_conformance`` is the reusable entry point.  It is imported by
``test_kite_provider.py`` for the live smoke test and can be imported by any
future vendor's test module.  A vendor that passes this is a legal drop-in; a
vendor that does not is not swapped in, however good its sales deck is.

Run:  market_scanner/.venv/Scripts/python.exe -m pytest market_data/tests -q
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from market_data import get_provider, clear_provider_cache  # noqa: E402
from market_data.fake_provider import FakeProvider  # noqa: E402
from market_data.ratelimit import TokenBucket, get_limiter, reset_limiters  # noqa: E402
from market_data.types import (  # noqa: E402
    IST,
    BASE_GRID_SECONDS,
    ProviderError,
    QualityFlag,
    RawCandle,
    bar_end_for,
    bars_per_session,
    is_on_grid,
    session_close,
    session_grid,
    session_open,
    to_ist,
    trading_days,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"


# =============================================================================
#  The reusable suite
# =============================================================================

def session_bar_census(candles, timeframe: str) -> dict[date, int]:
    """Bars per session — the input to contract §2 check 4 (session length)."""
    return dict(Counter(c.session_date for c in candles))


def assert_bar_shape(candles, timeframe: str, *, provider_id: str = "") -> None:
    """Grid alignment, session containment, OHLC sanity, ordering, no overlap."""
    prev = None
    for c in candles:
        where = f"{provider_id} {c.symbol} {timeframe} {c.bar_start.isoformat()}"
        assert isinstance(c, RawCandle), f"{where}: not a RawCandle"
        assert c.bar_start.tzinfo is not None, f"{where}: naive bar_start"
        assert c.bar_end.tzinfo is not None, f"{where}: naive bar_end"
        assert c.bar_end > c.bar_start, f"{where}: bar_end <= bar_start"
        assert c.timeframe == timeframe, f"{where}: timeframe mismatch {c.timeframe}"

        # 15-minute grid alignment (owner decision 1)
        ist = to_ist(c.bar_start)
        offset = (ist - session_open(ist.date())).total_seconds()
        assert offset >= 0 and offset % BASE_GRID_SECONDS == 0, (
            f"{where}: bar_start is off the 15-minute session grid (offset {offset}s)"
        )
        assert is_on_grid(c.bar_start, timeframe), f"{where}: off grid for {timeframe}"
        assert c.bar_end == bar_end_for(c.bar_start, timeframe), f"{where}: wrong bar_end"

        # session boundaries
        assert ist.weekday() < 5, f"{where}: bar on a weekend"
        assert session_open(ist.date()) <= ist, f"{where}: bar starts before 09:15"
        assert to_ist(c.bar_end) <= session_close(ist.date()), f"{where}: bar ends after 15:30"

        # OHLC sanity — a provider may FLAG a bad row but must not silently ship
        # a broken one as clean.
        clean = not (c.quality_flags & (QualityFlag.OHLC_INCONSISTENT | QualityFlag.NON_POSITIVE_PRICE))
        if clean:
            assert min(c.open, c.high, c.low, c.close) > 0, f"{where}: non-positive price unflagged"
            assert c.low <= min(c.open, c.close), f"{where}: low > min(open, close) unflagged"
            assert c.high >= max(c.open, c.close), f"{where}: high < max(open, close) unflagged"
        assert c.volume >= 0, f"{where}: negative volume"

        # no duplicates, no overlaps, strictly increasing
        if prev is not None:
            assert c.bar_start > prev.bar_start, f"{where}: duplicate/unsorted bar_start"
            assert c.bar_start >= prev.bar_end, f"{where}: overlaps previous bar"
        prev = c


def assert_provider_conformance(
    provider,
    *,
    symbol: str,
    timeframes=("15minute", "day"),
    start: datetime,
    end: datetime,
    now: datetime | None = None,
    empty_window: tuple[datetime, datetime] | None = None,
    check_idempotent: bool = True,
    min_rows: int = 1,
) -> dict:
    """Assert contract §1 for ``provider``.  Returns a small report dict."""
    report: dict = {"provider_id": getattr(provider, "provider_id", "?")}

    # -- interface surface ----------------------------------------------
    for attr in ("provider_id", "delay_seconds", "max_days_per_request", "rate_limit_per_second"):
        assert hasattr(provider, attr), f"provider is missing required attribute {attr!r}"
    assert isinstance(provider.delay_seconds, int) and provider.delay_seconds >= 0
    assert provider.rate_limit_per_second > 0
    assert callable(provider.instruments) and callable(provider.candles)
    assert callable(provider.latest_completed_bar)

    now = now or datetime.now(IST)

    for tf in timeframes:
        assert tf in provider.max_days_per_request, f"no max_days_per_request for {tf!r}"
        assert provider.max_days_per_request[tf] >= 1

        bars = provider.candles(symbol, tf, start, end)
        report[f"{tf}_rows"] = len(bars)
        assert len(bars) >= min_rows, f"{tf}: expected at least {min_rows} rows, got {len(bars)}"

        assert_bar_shape(bars, tf, provider_id=provider.provider_id)

        # -- delay honoured ---------------------------------------------
        # Nothing may be returned whose bar_end is not yet published.
        cutoff = now.timestamp() - provider.delay_seconds
        for c in bars:
            if c.candle_complete:
                assert c.bar_end.timestamp() <= cutoff + 1, (
                    f"{provider.provider_id} {tf}: bar {c.bar_start.isoformat()} marked complete "
                    f"but bar_end is inside the {provider.delay_seconds}s publication delay"
                )

        # -- candle_complete correctness --------------------------------
        for c in bars:
            expected = c.bar_end.timestamp() <= cutoff + 1
            assert c.candle_complete == expected or (
                # tolerate one boundary bar straddling the cutoff
                abs(c.bar_end.timestamp() - cutoff) <= max(60, provider.delay_seconds)
            ), (
                f"{provider.provider_id} {tf}: candle_complete={c.candle_complete} is wrong for "
                f"{c.bar_start.isoformat()} (bar_end {c.bar_end.isoformat()}, "
                f"delay {provider.delay_seconds}s)"
            )
            if not c.candle_complete:
                assert c.quality_flags & QualityFlag.PARTIAL_BAR, (
                    f"{provider.provider_id}: incomplete bar not flagged PARTIAL_BAR"
                )

        # -- bars are inside the requested window ------------------------
        lo = session_open(to_ist(start).date())
        hi = session_close(to_ist(end).date())
        for c in bars:
            assert lo <= c.bar_start <= hi, (
                f"{provider.provider_id} {tf}: bar {c.bar_start.isoformat()} outside the "
                f"requested window"
            )

        # -- idempotent re-fetch -----------------------------------------
        if check_idempotent:
            again = provider.candles(symbol, tf, start, end)
            key = lambda c: (c.bar_start, c.open, c.high, c.low, c.close, c.volume)  # noqa: E731
            assert [key(c) for c in again] == [key(c) for c in bars], (
                f"{provider.provider_id} {tf}: re-fetching the same window changed the data"
            )

        # -- missing data is REPORTED, never fabricated -------------------
        if empty_window is not None:
            none_bars = provider.candles(symbol, tf, empty_window[0], empty_window[1])
            assert none_bars == [], (
                f"{provider.provider_id} {tf}: fabricated {len(none_bars)} bars for a window "
                f"with no trading"
            )

        # -- latest_completed_bar ----------------------------------------
        lcb = provider.latest_completed_bar(tf, now)
        assert lcb.tzinfo is not None, "latest_completed_bar returned a naive datetime"
        assert is_on_grid(lcb, tf), "latest_completed_bar is off the session grid"
        assert bar_end_for(lcb, tf).timestamp() <= now.timestamp() - provider.delay_seconds + 1, (
            f"{provider.provider_id} {tf}: latest_completed_bar ignores the "
            f"{provider.delay_seconds}s delay"
        )
        report[f"{tf}_latest_completed_bar"] = lcb.isoformat()
        report[f"{tf}_census"] = session_bar_census(bars, tf)

    return report


# =============================================================================
#  Fixture HTTP server for the vendor15 adapter
# =============================================================================

class _FixtureHandler(BaseHTTPRequestHandler):
    payload: dict = {}
    instruments_payload: dict = {}
    seen: list = []

    def log_message(self, *a):  # silence the test run
        return

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        type(self).seen.append((u.path, q))
        if u.path == "/instruments":
            return self._json(200, type(self).instruments_payload)
        if u.path != "/candles":
            return self._json(404, {"error": "not found"})
        if q.get("apikey") != "fixture-key" and self.headers.get("X-API-Key") != "fixture-key":
            return self._json(401, {"error": "bad key"})
        interval = {"15m": "15minute", "1d": "day"}.get(q.get("interval", ""))
        rows = type(self).payload["candles"].get(interval, [])
        frm = date.fromisoformat(q["from"])
        to = date.fromisoformat(q["to"])
        # The fixture vendor stamps bar END in UTC; select on the IST session date.
        def sess(r):
            dt = datetime.fromisoformat(r["ts"].replace("Z", "+00:00")).astimezone(IST)
            return (dt - timedelta(seconds=1)).date()
        sel = [r for r in rows if frm <= sess(r) <= to]
        return self._json(200, {"status": "ok", "candles": sel})

    def _json(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


@pytest.fixture(scope="module")
def vendor_server():
    _FixtureHandler.payload = json.loads(
        (FIXTURES / "vendor15_reliance.json").read_text(encoding="utf-8")
    )
    _FixtureHandler.instruments_payload = json.loads(
        (FIXTURES / "vendor15_instruments.json").read_text(encoding="utf-8")
    )
    srv = HTTPServer(("127.0.0.1", 0), _FixtureHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.fixture
def vendor15(vendor_server, monkeypatch):
    """A Vendor15Provider wired to the fixture server, configured ONLY by env.

    This is the swap rehearsal: the recorded payload uses the vendor's own
    field names, a nested ohlc object, UTC timestamps and a bar-END convention,
    and *nothing but configuration* adapts it.
    """
    from market_data import vendor15_provider as v15

    monkeypatch.setenv("VENDOR15_BASE_URL", vendor_server)
    monkeypatch.setenv("VENDOR15_API_KEY", "fixture-key")
    monkeypatch.setenv("VENDOR15_ROWS_KEY", "candles")
    monkeypatch.setenv("VENDOR15_INSTRUMENTS_ROWS_KEY", "data")
    monkeypatch.setenv("VENDOR15_TIMEZONE", "UTC")
    monkeypatch.setenv("VENDOR15_TIMESTAMP_CONVENTION", "end")
    monkeypatch.setenv("VENDOR15_ADJUSTMENT_BASIS_ID", "vendor15-fixture")
    monkeypatch.setenv("VENDOR15_RATE_PER_SECOND", "500")
    monkeypatch.setitem(v15.FIELD_MAP, "timestamp", "ts")
    monkeypatch.setitem(v15.FIELD_MAP, "open", "ohlc.o")
    monkeypatch.setitem(v15.FIELD_MAP, "high", "ohlc.h")
    monkeypatch.setitem(v15.FIELD_MAP, "low", "ohlc.l")
    monkeypatch.setitem(v15.FIELD_MAP, "close", "ohlc.c")
    monkeypatch.setitem(v15.FIELD_MAP, "volume", "vol")
    monkeypatch.setitem(v15.FIELD_MAP, "vendor_revision", "rev")
    monkeypatch.setitem(v15.FIELD_MAP, "symbol", "sym")
    reset_limiters()
    return v15.Vendor15Provider()


# =============================================================================
#  types / session maths
# =============================================================================

def test_full_nse_session_is_25_fifteen_minute_bars():
    grid = session_grid(date(2026, 6, 15), "15minute")
    assert len(grid) == 25 == bars_per_session("15minute")
    assert grid[0] == session_open(date(2026, 6, 15))
    assert bar_end_for(grid[-1], "15minute") == session_close(date(2026, 6, 15))


def test_grid_and_session_predicates():
    d = date(2026, 6, 15)
    assert is_on_grid(session_open(d), "15minute")
    assert not is_on_grid(session_open(d) + timedelta(minutes=7), "15minute")
    assert not is_on_grid(session_open(d) + timedelta(minutes=15), "60minute")
    # a bar that would run past 15:30 is not in-session
    from market_data.types import in_session

    assert in_session(session_open(d) + timedelta(minutes=15 * 24), "15minute")
    assert not in_session(session_close(d), "15minute")


def test_trading_days_skips_weekends_and_holidays():
    days = trading_days(date(2026, 6, 15), date(2026, 6, 21), holidays=[date(2026, 6, 17)])
    assert days == [date(2026, 6, 15), date(2026, 6, 16), date(2026, 6, 18), date(2026, 6, 19)]


def test_quality_flags_catch_broken_rows():
    from market_data.types import basic_quality_flags

    assert basic_quality_flags(10, 11, 9, 10, 100) == QualityFlag.NONE
    assert basic_quality_flags(10, 9, 9, 10, 100) & QualityFlag.OHLC_INCONSISTENT
    assert basic_quality_flags(0, 1, 0, 1, 100) & QualityFlag.NON_POSITIVE_PRICE
    assert basic_quality_flags(10, 11, 9, 10, 0) & QualityFlag.ZERO_VOLUME


# =============================================================================
#  rate limiter (contract §1: extra workers must not exceed the global rate)
# =============================================================================

def test_token_bucket_paces_a_single_thread():
    b = TokenBucket(rate=20.0)
    t0 = time.monotonic()
    for _ in range(10):
        b.acquire()
    elapsed = time.monotonic() - t0
    assert elapsed >= (10 - b.capacity) / 20.0 * 0.9, elapsed


def test_extra_threads_do_not_exceed_the_global_rate():
    """8 threads sharing one bucket must not go faster than the bucket."""
    b = TokenBucket(rate=25.0)
    n_threads, per_thread = 8, 6
    total = n_threads * per_thread

    def worker():
        for _ in range(per_thread):
            b.acquire()

    t0 = time.monotonic()
    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0
    assert b.granted == total
    floor = (total - b.capacity) / b.rate
    assert elapsed >= floor * 0.9, (
        f"{total} acquisitions across {n_threads} threads took {elapsed:.3f}s; "
        f"the shared bucket should have needed >= {floor:.3f}s"
    )
    observed_rate = total / elapsed
    assert observed_rate <= b.rate * 1.25, f"observed {observed_rate:.1f}/s > limit {b.rate}/s"


def test_limiter_registry_is_process_wide_and_only_slows_down():
    reset_limiters()
    a = get_limiter("unit-test", 10.0)
    b = get_limiter("unit-test", 3.0)
    assert a is b, "two callers got different limiters for the same key"
    assert a.rate == 3.0, "the shared limiter must clamp to the slowest requested rate"
    c = get_limiter("unit-test", 99.0)
    assert c.rate == 3.0, "a later caller must not be able to speed the bucket up"
    reset_limiters()


# =============================================================================
#  registry / the vendor swap is a config change
# =============================================================================

def test_get_provider_is_driven_by_MARKET_DATA_PROVIDER(monkeypatch):
    clear_provider_cache()
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "fake")
    assert get_provider().provider_id == "fake"
    clear_provider_cache()
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "vendor15")
    assert get_provider().provider_id == "vendor15"
    clear_provider_cache()
    monkeypatch.delenv("MARKET_DATA_PROVIDER", raising=False)


def test_unknown_provider_is_a_clear_error(monkeypatch):
    clear_provider_cache()
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "nope")
    with pytest.raises(ProviderError):
        get_provider()
    clear_provider_cache()


def test_every_shipped_provider_satisfies_the_protocol():
    from market_data.provider import MarketDataProvider
    from market_data.vendor15_provider import Vendor15Provider

    assert isinstance(FakeProvider(), MarketDataProvider)
    assert isinstance(Vendor15Provider(base_url="http://x"), MarketDataProvider)


# =============================================================================
#  conformance: FakeProvider (the reference implementation)
# =============================================================================

NOW = datetime(2026, 6, 30, 16, 0, tzinfo=IST)
START = datetime(2026, 6, 1, tzinfo=IST)
END = datetime(2026, 6, 30, tzinfo=IST)
HOLIDAY = (datetime(2026, 6, 17, tzinfo=IST), datetime(2026, 6, 17, tzinfo=IST))


def test_fake_provider_conformance():
    p = FakeProvider(now=NOW, holidays=[date(2026, 6, 17)], missing_days=[date(2026, 6, 18)])
    rep = assert_provider_conformance(
        p, symbol="RELIANCE", start=START, end=END, now=NOW, empty_window=HOLIDAY
    )
    assert rep["15minute_rows"] == 500, rep          # 20 sessions x 25 bars
    assert set(rep["15minute_census"].values()) == {25}
    assert rep["day_rows"] == 20
    assert date(2026, 6, 18) not in rep["15minute_census"], "missing day was fabricated"


def test_fake_provider_with_a_900s_delay_conformance():
    """Same suite, delayed vendor semantics — 'delayed by design', not stale."""
    p = FakeProvider(now=NOW, delay_seconds=900, holidays=[date(2026, 6, 17)])
    assert_provider_conformance(
        p, symbol="RELIANCE", start=START, end=END, now=NOW, empty_window=HOLIDAY
    )
    # at 16:00 with a 900s delay the last published 15m bar is still 15:15's
    assert p.latest_completed_bar("15minute", NOW) == datetime(2026, 6, 30, 15, 15, tzinfo=IST)
    mid = datetime(2026, 6, 30, 11, 7, tzinfo=IST)
    assert p.latest_completed_bar("15minute", mid) == datetime(2026, 6, 30, 10, 30, tzinfo=IST)


def test_delay_shifts_latest_completed_bar_by_exactly_one_bar():
    mid = datetime(2026, 6, 30, 11, 7, tzinfo=IST)
    live = FakeProvider(now=mid).latest_completed_bar("15minute", mid)
    delayed = FakeProvider(now=mid, delay_seconds=900).latest_completed_bar("15minute", mid)
    assert live == datetime(2026, 6, 30, 10, 45, tzinfo=IST)
    assert delayed == live - timedelta(minutes=15)


def test_provider_never_returns_future_bars():
    mid = datetime(2026, 6, 30, 11, 7, tzinfo=IST)
    p = FakeProvider(now=mid)
    bars = p.candles("RELIANCE", "15minute", datetime(2026, 6, 30, tzinfo=IST), END)
    assert bars, "expected the sessions up to 'now'"
    assert max(c.bar_end for c in bars) <= mid, "a bar from the future was returned"


def test_incomplete_trailing_bar_is_flagged_not_hidden():
    mid = datetime(2026, 6, 30, 11, 7, tzinfo=IST)
    p = FakeProvider(now=mid)
    bars = p.candles("RELIANCE", "15minute", datetime(2026, 6, 30, tzinfo=IST), END)
    complete = [c for c in bars if c.candle_complete]
    assert complete[-1].bar_start == datetime(2026, 6, 30, 10, 45, tzinfo=IST)


def test_chunking_does_not_duplicate_or_lose_bars_at_the_seam():
    """A window far wider than max_days_per_request must stitch cleanly."""
    p = FakeProvider(now=NOW)
    assert p.max_days_per_request["15minute"] == 30
    wide = p.candles("RELIANCE", "15minute", datetime(2026, 3, 2, tzinfo=IST), END)
    starts = [c.bar_start for c in wide]
    assert len(starts) == len(set(starts)), "chunk seam produced duplicate bars"
    assert starts == sorted(starts)
    expected_sessions = len(trading_days(date(2026, 3, 2), date(2026, 6, 30)))
    assert len(session_bar_census(wide, "15minute")) == expected_sessions


# =============================================================================
#  conformance: Vendor15Provider against the recorded fixture server
# =============================================================================

def test_vendor15_conformance_against_fixture_server(vendor15):
    rep = assert_provider_conformance(
        vendor15, symbol="RELIANCE", start=START, end=END, now=NOW, empty_window=HOLIDAY
    )
    assert rep["15minute_rows"] == 500, rep
    assert set(rep["15minute_census"].values()) == {25}
    assert rep["day_rows"] == 20
    assert date(2026, 6, 18) not in rep["15minute_census"], (
        "the day the vendor has no data for was fabricated"
    )


def test_vendor15_delay_is_900_seconds_by_design(vendor15):
    assert vendor15.delay_seconds == 900
    assert vendor15.latest_completed_bar("15minute", NOW) == datetime(
        2026, 6, 30, 15, 15, tzinfo=IST
    )


def test_vendor15_translates_bar_END_utc_timestamps_to_bar_START_ist(vendor15):
    """Checklist §3: the single most dangerous unconfirmed convention."""
    bars = vendor15.candles(
        "RELIANCE", "15minute", datetime(2026, 6, 15, tzinfo=IST), datetime(2026, 6, 15, tzinfo=IST)
    )
    assert len(bars) == 25
    assert bars[0].bar_start == datetime(2026, 6, 15, 9, 15, tzinfo=IST)
    assert bars[0].bar_end == datetime(2026, 6, 15, 9, 30, tzinfo=IST)
    assert bars[-1].bar_end == session_close(date(2026, 6, 15))
    assert all(b.adjustment_basis_id == "vendor15-fixture" for b in bars)
    assert all(b.vendor_id == "vendor15" and b.vendor_revision == "1" for b in bars)


def test_vendor15_field_mapping_is_the_only_thing_that_changes(vendor15, monkeypatch):
    """Point FIELD_MAP at the wrong key -> it breaks. That proves the mapping
    (and only the mapping) is what adapts a vendor's JSON."""
    from market_data import vendor15_provider as v15

    monkeypatch.setitem(v15.FIELD_MAP, "close", "ohlc.NOPE")
    with pytest.raises((TypeError, ValueError)):
        vendor15.candles(
            "RELIANCE", "15minute", datetime(2026, 6, 15, tzinfo=IST),
            datetime(2026, 6, 15, tzinfo=IST),
        )


def test_vendor15_instruments_mapping(vendor15):
    insts = vendor15.instruments()
    assert [i.symbol for i in insts] == ["RELIANCE", "TCS"]
    assert insts[0].instrument_id == "V-RELIANCE"
    assert insts[0].isin == "INE002A01018"


def test_vendor15_bad_credentials_raise_TokenError(vendor_server, monkeypatch):
    from market_data.types import TokenError
    from market_data.vendor15_provider import Vendor15Provider

    p = Vendor15Provider(base_url=vendor_server, api_key="wrong-key")
    with pytest.raises(TokenError):
        p.candles("RELIANCE", "15minute", START, END)


def test_vendor15_404_is_an_empty_window_not_a_fabricated_bar(vendor_server):
    from market_data.vendor15_provider import Vendor15Provider

    p = Vendor15Provider(base_url=vendor_server, api_key="fixture-key")
    p.candles_path = "/does-not-exist"
    assert p.candles("RELIANCE", "15minute", START, END) == []


def test_vendor15_chunks_by_its_configured_max_range(vendor_server, monkeypatch, vendor15):
    """Checklist §5: the per-request span is config, and chunking honours it."""
    _FixtureHandler.seen.clear()
    vendor15.max_days_per_request["15minute"] = 7
    bars = vendor15.candles("RELIANCE", "15minute", START, END)
    calls = [q for path, q in _FixtureHandler.seen if path == "/candles"]
    assert len(calls) == 5, calls          # 30 days / 7 = 5 requests
    for q in calls:
        span = (date.fromisoformat(q["to"]) - date.fromisoformat(q["from"])).days + 1
        assert span <= 7, q
    starts = [c.bar_start for c in bars]
    assert len(starts) == len(set(starts)) == 500


# =============================================================================
#  the Kite normalizer, offline, against a real recorded payload
# =============================================================================

def test_kite_normalizer_on_a_real_recorded_session():
    """Offline: the recorded 2025-06-30 RELIANCE session must normalize cleanly.

    This is a REAL Kite payload (25 bars, 09:15..15:15) recorded on
    2026-09-16, so the normalizer is tested against the vendor's true shape
    without needing the network.
    """
    from market_data.kite_provider import KiteProvider
    from market_data.types import Instrument

    blob = json.loads((FIXTURES / "kite_reliance_20250630.json").read_text(encoding="utf-8"))
    raw = [{**r, "date": datetime.fromisoformat(r["date"])} for r in blob["15minute"]]
    inst = Instrument(instrument_id="738561", symbol="RELIANCE", exchange="NSE")
    p = KiteProvider.__new__(KiteProvider)      # no client, no credentials
    p.delay_seconds = 0
    p.adjustment_basis_id = "kite-eod-adjusted"

    bars = KiteProvider._normalize(p, raw, inst, "15minute")
    assert len(bars) == 25
    assert_bar_shape(bars, "15minute", provider_id="kite")
    assert bars[0].bar_start == datetime(2025, 6, 30, 9, 15, tzinfo=IST)
    assert bars[-1].bar_end == session_close(date(2025, 6, 30))
    assert all(b.candle_complete for b in bars)
    assert all(b.quality_flags == QualityFlag.NONE for b in bars)
    assert all(b.vendor_id == "kite" for b in bars)

    rawd = [{**r, "date": datetime.fromisoformat(r["date"])} for r in blob["day"]]
    day = KiteProvider._normalize(p, rawd, inst, "day")
    assert len(day) == 1
    # Kite stamps daily bars at midnight IST; we store the SESSION window.
    assert day[0].bar_start == session_open(date(2025, 6, 30))
    assert day[0].bar_end == session_close(date(2025, 6, 30))
    assert day[0].session_date == date(2025, 6, 30)
    # Cross-timeframe agreement, contract §2 check 5.  NOTE the tolerance: NSE's
    # official daily close is the closing-session price, NOT the last traded
    # price of the 15:15 candle, so a few basis points of difference is normal
    # and must NOT be 'repaired'.  Here: daily 1500.60 vs intraday 1499.90
    # (0.05%).  What must hold is the contract's 0.8x / 1.2x envelope.
    i_high = max(b.high for b in bars)
    i_low = min(b.low for b in bars)
    assert abs(day[0].close - bars[-1].close) / day[0].close < 0.005
    assert i_low >= 0.8 * day[0].low
    assert i_high <= 1.2 * day[0].high
    assert day[0].open == bars[0].open


def test_kite_chunking_respects_the_measured_200_day_cap():
    from market_data.kite_provider import KITE_MAX_DAYS, KiteProvider

    assert KITE_MAX_DAYS["15minute"] == 200
    assert KITE_MAX_DAYS["day"] == 2000
    p = KiteProvider.__new__(KiteProvider)
    p.max_days_per_request = dict(KITE_MAX_DAYS)
    chunks = list(
        KiteProvider.chunk_days(p, "15minute", datetime(2024, 1, 1, tzinfo=IST),
                                datetime(2025, 12, 31, tzinfo=IST))
    )
    assert all((b - a).days + 1 <= 200 for a, b in chunks)
    assert chunks[0][0] == date(2024, 1, 1) and chunks[-1][1] == date(2025, 12, 31)
    for (a1, b1), (a2, _) in zip(chunks, chunks[1:]):
        assert a2 == b1 + timedelta(days=1), "chunk seam has a gap or an overlap"


def test_secrets_never_appear_in_provider_output():
    """Static check: no module in the package can stringify a credential.

    Parses the source rather than grepping it, so a *string literal* mentioning
    the word "token" is fine but passing the token VARIABLE to print/log — or
    interpolating it into an f-string — fails.
    """
    import ast

    from market_data import kite_provider as kp
    from market_data import vendor15_provider as v15

    SECRETS = {"token", "access_token", "api_key", "api_secret", "password",
               "_api_key", "tok", "secret"}

    def names_in(node):
        for n in ast.walk(node):
            if isinstance(n, ast.Name):
                yield n.id
            elif isinstance(n, ast.Attribute):
                yield n.attr

    for mod in (kp, v15):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            # 1. print(...) / log.<level>(...) must not receive a secret
            if isinstance(node, ast.Call):
                fn = node.func
                is_print = isinstance(fn, ast.Name) and fn.id == "print"
                is_log = (
                    isinstance(fn, ast.Attribute)
                    and fn.attr in {"debug", "info", "warning", "error", "exception", "critical"}
                    and isinstance(fn.value, ast.Name)
                    and fn.value.id in {"log", "logger", "logging"}
                )
                if is_print or is_log:
                    for arg in node.args + [k.value for k in node.keywords]:
                        bad = SECRETS & set(names_in(arg))
                        assert not bad, (
                            f"{Path(mod.__file__).name}:{node.lineno} logs a credential: {bad}"
                        )
            # 2. nothing raised may interpolate a secret into its message
            if isinstance(node, ast.Raise) and node.exc is not None:
                for sub in ast.walk(node.exc):
                    if isinstance(sub, ast.FormattedValue):
                        bad = SECRETS & set(names_in(sub.value))
                        assert not bad, (
                            f"{Path(mod.__file__).name}:{node.lineno} raises a message "
                            f"containing {bad}"
                        )

    # the auth worker's child output is captured, never echoed
    ksrc = Path(kp.__file__).read_text(encoding="utf-8")
    assert "capture_output=True" in ksrc
    assert "proc.stdout" not in ksrc and "proc.stderr" not in ksrc
    assert kp._token_fingerprint("abcdefghijklmnop") != "abcdefghijklmnop"
    assert len(kp._token_fingerprint("abcdefghijklmnop")) == 8
