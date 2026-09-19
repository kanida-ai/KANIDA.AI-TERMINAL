"""Kite provider tests.

Two kinds:

* **offline** — token resolution order, auto-auth wiring and error
  classification, all with fakes.  These always run.
* **live smoke** — a read-only historical fetch against the real Kite API.
  Skipped automatically when there is no usable token (offline, expired,
  machine without the engine project).  Force-skip with ``KANIDA_LIVE=0``;
  it never places an order and never writes to any database.

Run just the live part with::

    market_scanner/.venv/Scripts/python.exe -m pytest market_data/tests/test_kite_provider.py -q -s -k live
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from market_data import kite_provider as kp  # noqa: E402
from market_data.types import IST, QualityFlag, TokenError, session_close, session_open  # noqa: E402
from test_conformance import (  # noqa: E402
    assert_bar_shape,
    assert_provider_conformance,
    session_bar_census,
)

LIVE_ENABLED = os.environ.get("KANIDA_LIVE", "1") != "0"


# =============================================================================
#  offline
# =============================================================================

def test_token_resolution_prefers_the_db_then_the_env(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE kite_tokens (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "access_token TEXT NOT NULL, token_date TEXT NOT NULL)"
    )
    con.execute("INSERT INTO kite_tokens (access_token, token_date) VALUES ('older','2026-09-14')")
    con.execute("INSERT INTO kite_tokens (access_token, token_date) VALUES ('newest','2026-09-15')")
    con.commit()
    con.close()

    monkeypatch.setenv("KANIDA_DB_PATH", str(db))
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "from-env")
    assert kp.get_access_token() == "newest", "the NEWEST kite_tokens row must win"

    # DB unavailable -> .env fallback
    monkeypatch.setenv("KANIDA_DB_PATH", str(tmp_path / "missing.db"))
    assert kp.get_access_token() == "from-env"


def test_missing_token_everywhere_raises_TokenError(tmp_path, monkeypatch):
    monkeypatch.setenv("KANIDA_DB_PATH", str(tmp_path / "missing.db"))
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    monkeypatch.setattr(kp, "_ENV_CACHE", {}, raising=False)
    with pytest.raises(TokenError) as e:
        kp.get_access_token()
    assert "no Kite access token" in str(e.value)


def test_token_fingerprint_reveals_nothing():
    fp = kp._token_fingerprint("a-real-looking-access-token")
    assert len(fp) == 8 and fp not in "a-real-looking-access-token"
    assert kp._token_fingerprint(None) == "none"


def test_token_errors_are_classified():
    class TokenException(Exception):
        pass

    assert kp._is_token_error(TokenException("Incorrect `api_key` or `access_token`."))
    assert kp._is_token_error(Exception("TokenException: invalid session"))
    assert not kp._is_token_error(Exception("interval exceeds max limit: 200 days"))
    assert not kp._is_token_error(Exception("Connection reset"))


def test_auto_auth_runs_once_then_the_call_gives_up(monkeypatch):
    """A token failure triggers the engine's auth worker exactly once."""
    calls = {"auth": 0, "fetch": 0}

    class TokenException(Exception):
        pass

    def fake_auto_auth(timeout=300.0):
        calls["auth"] += 1
        return False, "rc=0 token_changed=no"

    monkeypatch.setattr(kp, "run_auto_auth", fake_auto_auth)
    p = kp.KiteProvider.__new__(kp.KiteProvider)
    p.max_retries = 3
    p.auto_auth = True
    p._auto_auth_done = False
    p._kite = object()
    p._kite_lock = __import__("threading").Lock()
    p.limiter = kp.get_limiter("kite-unit-test", 1000.0, capacity=1000.0)
    p.requests_made = 0

    def boom(_k):
        calls["fetch"] += 1
        raise TokenException("Incorrect `api_key` or `access_token`.")

    with pytest.raises(TokenError):
        kp.KiteProvider._call(p, boom)
    assert calls["auth"] == 1, "auto-auth must run exactly once, not in a loop"
    assert p._auto_auth_done is True


def test_auto_auth_retries_the_call_when_a_new_token_was_minted(monkeypatch):
    class TokenException(Exception):
        pass

    state = {"minted": False, "n": 0}
    monkeypatch.setattr(
        kp, "run_auto_auth", lambda timeout=300.0: (True, "rc=0 token_changed=yes")
    )
    p = kp.KiteProvider.__new__(kp.KiteProvider)
    p.max_retries = 3
    p.auto_auth = True
    p._auto_auth_done = False
    p._kite = object()
    p._kite_lock = __import__("threading").Lock()
    p.limiter = kp.get_limiter("kite-unit-test", 1000.0, capacity=1000.0)
    p.requests_made = 0
    monkeypatch.setattr(kp.KiteProvider, "_build_client", lambda self: object())

    def flaky(_k):
        state["n"] += 1
        if state["n"] == 1:
            raise TokenException("TokenException")
        return "ok"

    assert kp.KiteProvider._call(p, flaky) == "ok"
    assert state["n"] == 2


def test_auto_auth_calls_the_engine_worker_and_never_echoes_it(monkeypatch, tmp_path):
    """We must invoke the EXISTING worker as a subprocess, not re-login here."""
    seen = {}

    class Result:
        returncode = 0
        stdout = "token_preview=abcd1234"
        stderr = ""

    def fake_run(argv, **kw):
        seen["argv"] = argv
        seen["kw"] = kw
        return Result()

    monkeypatch.setattr(kp.subprocess, "run", fake_run)
    monkeypatch.setattr(kp, "AUTH_WORKER", tmp_path / "auth_worker.py")
    (tmp_path / "auth_worker.py").write_text("# stub", encoding="utf-8")
    changed, detail = kp.run_auto_auth()
    assert changed is False
    assert "rc=0" in detail and "abcd1234" not in detail, "child stdout must never be echoed"
    assert seen["argv"][1].endswith("auth_worker.py")
    assert seen["kw"]["capture_output"] is True
    assert seen["kw"]["env"]["PLAYWRIGHT_BROWSERS_PATH"] == kp.PLAYWRIGHT_BROWSERS_PATH


def test_auto_auth_is_a_no_op_when_the_worker_is_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(kp, "AUTH_WORKER", tmp_path / "nope.py")
    changed, detail = kp.run_auto_auth()
    assert changed is False and "not found" in detail


def test_latest_completed_bar_maths_without_a_client():
    p = kp.KiteProvider.__new__(kp.KiteProvider)
    p.delay_seconds = 0
    p.holidays = frozenset()
    p.supported_timeframes = ("15minute", "day")
    f = kp.KiteProvider.latest_completed_bar
    assert f(p, "15minute", datetime(2026, 9, 15, 11, 7, tzinfo=IST)) == datetime(
        2026, 9, 15, 10, 45, tzinfo=IST
    )
    # before the open, fall back to the previous session's last bar
    assert f(p, "15minute", datetime(2026, 9, 15, 8, 30, tzinfo=IST)) == datetime(
        2026, 9, 14, 15, 15, tzinfo=IST
    )
    # Sunday -> Friday's close
    assert f(p, "day", datetime(2026, 9, 13, 10, 0, tzinfo=IST)) == session_open(date(2026, 9, 11))


# =============================================================================
#  live smoke (read-only market data)
# =============================================================================

@pytest.fixture(scope="module")
def live_kite():
    if not LIVE_ENABLED:
        pytest.skip("KANIDA_LIVE=0")
    try:
        import kiteconnect  # noqa: F401
    except ImportError:
        pytest.skip("kiteconnect not installed in this interpreter")
    if not kp.ENV_FILE.exists():
        pytest.skip(f"engine .env not present at {kp.ENV_FILE}")
    p = kp.KiteProvider()
    ok, why = p.token_health()
    if not ok:
        pytest.skip(f"no usable Kite session ({why}) — offline or token expired")
    return p


def _recent_sessions(p, n=5):
    end = datetime.now(IST)
    start = end - timedelta(days=n * 3 + 7)
    bars = p.candles("RELIANCE", "15minute", start, end)
    days = sorted({c.session_date for c in bars})[-n:]
    return [c for c in bars if c.session_date in set(days)], days


def test_live_reliance_15minute_last_5_sessions(live_kite, capsys):
    bars, days = _recent_sessions(live_kite, 5)
    assert bars, "no 15minute bars returned"
    assert_bar_shape(bars, "15minute", provider_id="kite")
    census = session_bar_census(bars, "15minute")
    with capsys.disabled():
        print(f"\n[live] RELIANCE 15minute rows={len(bars)} "
              f"first={bars[0].bar_start.isoformat()} last={bars[-1].bar_start.isoformat()}")
        for d in days:
            day = [c for c in bars if c.session_date == d]
            print(f"       {d}  bars={census[d]:2d}  {day[0].bar_start.time()}"
                  f"..{day[-1].bar_end.time()}  complete={sum(c.candle_complete for c in day)}")
    # every session must be a SUBSET of the 25-bar grid, in order, no dupes
    for d, n in census.items():
        assert 1 <= n <= 25, f"{d}: {n} bars in one session (grid holds 25)"
    assert all(c.vendor_id == "kite" for c in bars)


def test_live_reliance_day_last_60_days(live_kite, capsys):
    end = datetime.now(IST)
    bars = live_kite.candles("RELIANCE", "day", end - timedelta(days=60), end)
    assert bars
    assert_bar_shape(bars, "day", provider_id="kite")
    with capsys.disabled():
        print(f"[live] RELIANCE day rows={len(bars)} "
              f"first={bars[0].bar_start.date()} last={bars[-1].bar_start.date()}")
    assert len(set(c.session_date for c in bars)) == len(bars), "duplicate daily bars"
    assert 35 <= len(bars) <= 45, f"{len(bars)} sessions in 60 calendar days looks wrong"


def test_live_conformance_suite(live_kite):
    end = datetime.now(IST)
    # a Sunday -> must come back empty, never fabricated
    sunday = end.date() - timedelta(days=end.weekday() + 1)
    assert sunday.weekday() == 6
    assert_provider_conformance(
        live_kite,
        symbol="RELIANCE",
        start=end - timedelta(days=20),
        end=end,
        now=end,
        empty_window=(
            datetime.combine(sunday, datetime.min.time()).replace(tzinfo=IST),
            datetime.combine(sunday, datetime.min.time()).replace(tzinfo=IST),
        ),
    )


def test_live_measured_per_request_day_caps(live_kite, capsys):
    """The caps in KITE_MAX_DAYS are MEASURED, not copied from documentation."""
    inst = live_kite.resolve("RELIANCE")
    k = live_kite.kite
    tok = int(inst.instrument_id)

    def span_ok(days, interval):
        e = date(2025, 6, 30)
        s = e - timedelta(days=days - 1)
        live_kite.limiter.acquire()
        try:
            k.historical_data(
                tok,
                datetime.combine(s, datetime.min.time()),
                datetime.combine(e, datetime.max.time().replace(microsecond=0)),
                interval,
            )
            return True, ""
        except Exception as ex:
            return False, f"{type(ex).__name__}: {ex}"

    ok200, _ = span_ok(200, "15minute")
    bad250, err250 = span_ok(250, "15minute")
    with capsys.disabled():
        print(f"[live] 15minute span 200d -> {'OK' if ok200 else 'REJECTED'}; "
              f"250d -> {'OK' if bad250 else 'REJECTED'} ({err250[:70]})")
    assert ok200, "the configured 200-day 15minute cap was rejected by Kite"
    assert not bad250 and "200 days" in err250, "Kite's 15minute cap is no longer 200 days"
    assert live_kite.max_days_per_request["15minute"] == 200


def test_live_sustained_request_rate_is_under_the_limit(live_kite, capsys):
    """The shared limiter must hold the real request rate at/below the key limit."""
    inst = live_kite.resolve("RELIANCE")
    k = live_kite.kite
    tok = int(inst.instrument_id)
    n = 12
    t0 = time.time()
    for i in range(n):
        d = date(2025, 6, 2) + timedelta(days=i)
        live_kite.limiter.acquire()
        k.historical_data(
            tok,
            datetime.combine(d, datetime.min.time()),
            datetime.combine(d, datetime.max.time().replace(microsecond=0)),
            "15minute",
        )
    elapsed = time.time() - t0
    rate = n / elapsed
    with capsys.disabled():
        print(f"[live] {n} requests in {elapsed:.2f}s -> {rate:.2f} req/s "
              f"(limiter {live_kite.limiter.rate}/s)")
    assert rate <= live_kite.rate_limit_per_second * 1.1, "exceeded the Kite rate limit"


def test_live_cross_timeframe_reconciliation_reports_truncated_sessions(live_kite, capsys):
    """Contract §2 check 5, run live.

    This is a REPORT, not a pass/fail on the vendor: it prints any recent
    session whose 15-minute bars do not reconcile with Kite's own daily bar.
    As of 2026-09-16 this fires for every session from 2026-08-03 onward,
    because Kite's intraday feed stops at 15:15 while the daily bar includes
    the 15:15-15:30 closing period.  Recorded here so the gap is visible and
    cannot be silently aggregated away.
    """
    end = datetime.now(IST)
    start = end - timedelta(days=30)
    intraday = live_kite.candles("RELIANCE", "15minute", start, end)
    daily = {c.session_date: c for c in live_kite.candles("RELIANCE", "day", start, end)}
    by_day: dict[date, list] = {}
    for c in intraday:
        by_day.setdefault(c.session_date, []).append(c)

    mismatches = []
    for d, bars in sorted(by_day.items()):
        dd = daily.get(d)
        if dd is None:
            continue
        if abs(bars[-1].close - dd.close) > 0.01 or len(bars) != 25:
            mismatches.append((d, len(bars), bars[-1].bar_end.time(), bars[-1].close, dd.close))
    with capsys.disabled():
        print(f"[live] cross-timeframe check over {len(by_day)} sessions: "
              f"{len(mismatches)} do not reconcile")
        for d, n, last_end, ic, dc in mismatches[:5]:
            print(f"       {d} bars={n} intraday_ends={last_end} "
                  f"intraday_close={ic} daily_close={dc}")
        if len(mismatches) > 5:
            print(f"       ... and {len(mismatches) - 5} more")
    # never an assertion failure: the provider's job is to report, W2/W3's
    # validation layer decides what to do about it.
    assert isinstance(mismatches, list)
