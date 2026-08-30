"""
Chart Agent · Kite daily-refresh (fetch_kite) tests — Kite client fully MOCKED (no live token).

Runnable two ways:
    pytest backend/agents/chart/tests/test_fetch_kite.py
    python  backend/agents/chart/tests/test_fetch_kite.py

Each test uses its OWN temp SQLite store (AGENT_CHART_DB), so it is fast and never touches the real
kanida.db or the prod Parquet store.
"""
from __future__ import annotations
import os
import sys
import sqlite3
import tempfile
from datetime import date, datetime, time, timedelta

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from agents.chart import data                    # noqa: E402
from agents.chart import fetch_kite              # noqa: E402

AS_OF = date(2026, 8, 28)


# --------------------------------------------------------------------------- fakes / fixtures
class FakeKite:
    """Mock KiteConnect: records every historical_data window so a test can assert the GAP range."""
    def __init__(self, tokens, extra_future=False, close_base=200.0):
        self._tokens = tokens
        self.calls = []            # [(token, from_date, to_date, interval)]
        self.extra_future = extra_future
        self.close_base = close_base

    def instruments(self, exchange):
        assert exchange == "NSE"
        return [{"tradingsymbol": s, "instrument_token": t, "segment": "NSE"}
                for s, t in self._tokens.items()]

    def historical_data(self, token, from_date, to_date, interval):
        self.calls.append((token, from_date, to_date, interval))
        fd = from_date.date() if isinstance(from_date, datetime) else from_date
        td = to_date.date() if isinstance(to_date, datetime) else to_date
        bars, price, d = [], self.close_base, fd
        while d <= td:
            if d.weekday() < 5:    # weekdays only, like a real exchange calendar
                bars.append({"date": datetime.combine(d, time(0, 0)), "open": price,
                             "high": price + 2, "low": price - 1, "close": price + 1,
                             "volume": 1000})
                price += 1
            d += timedelta(days=1)
        if self.extra_future:      # simulate a misbehaving feed returning a bar dated to+1
            nd = td + timedelta(days=1)
            bars.append({"date": datetime.combine(nd, time(0, 0)), "open": 999.0, "high": 999.0,
                         "low": 999.0, "close": 999.0, "volume": 1})
        return bars


def _new_store(seed):
    """Create a temp SQLite ohlc_daily store; seed = {symbol: last_stored_date}. Points AGENT_CHART_DB
    at it. Returns (path, saved_env)."""
    fd, path = tempfile.mkstemp(suffix=".db", prefix="fetch_kite_")
    os.close(fd)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE ohlc_daily (symbol TEXT NOT NULL, instrument_token INTEGER, "
                "bar_time TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL, "
                "volume INTEGER, PRIMARY KEY (symbol, bar_time))")
    for sym, last in seed.items():
        con.execute("INSERT INTO ohlc_daily VALUES (?,?,?,?,?,?,?,?)",
                    (sym, 111, f"{last.isoformat()} 00:00:00", 10, 11, 9, 10.5, 100))
    con.commit()
    con.close()
    saved = os.environ.get("AGENT_CHART_DB")
    saved_uri = os.environ.get("AGENT_DATA_URI")
    os.environ["AGENT_CHART_DB"] = path
    os.environ.pop("AGENT_DATA_URI", None)       # force the SQLite path
    return path, (saved, saved_uri)


def _new_store_bars(seed):
    """Like _new_store but seed = {symbol: (last_date, (open,high,low,close))} so a guard test can
    control the exact stored OHLC of the last bar. Returns (path, saved_env)."""
    fd, path = tempfile.mkstemp(suffix=".db", prefix="fetch_kite_")
    os.close(fd)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE ohlc_daily (symbol TEXT NOT NULL, instrument_token INTEGER, "
                "bar_time TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL, "
                "volume INTEGER, PRIMARY KEY (symbol, bar_time))")
    for sym, (last, ohlc) in seed.items():
        o, h, l, c = ohlc
        con.execute("INSERT INTO ohlc_daily VALUES (?,?,?,?,?,?,?,?)",
                    (sym, 111, f"{last.isoformat()} 00:00:00", o, h, l, c, 100))
    con.commit()
    con.close()
    saved = os.environ.get("AGENT_CHART_DB")
    saved_uri = os.environ.get("AGENT_DATA_URI")
    os.environ["AGENT_CHART_DB"] = path
    os.environ.pop("AGENT_DATA_URI", None)
    data.all_symbols.cache_clear()
    return path, (saved, saved_uri)


class GuardKite:
    """Mock KiteConnect for the adjustment-drift guard. historical_data returns weekday bars whose
    OHLC comes from an explicit {date_iso: (o,h,l,c)} table (else a default bar). If return_empty,
    historical_data always returns [] (simulates a probe that can't be fetched)."""
    def __init__(self, tokens, table=None, return_empty=False):
        self._tokens = tokens
        self.table = table or {}
        self.return_empty = return_empty

    def instruments(self, exchange):
        assert exchange == "NSE"
        return [{"tradingsymbol": s, "instrument_token": t, "segment": "NSE"}
                for s, t in self._tokens.items()]

    def historical_data(self, token, from_date, to_date, interval):
        if self.return_empty:
            return []
        fd = from_date.date() if isinstance(from_date, datetime) else from_date
        td = to_date.date() if isinstance(to_date, datetime) else to_date
        bars, d = [], fd
        while d <= td:
            if d.weekday() < 5:
                o, h, l, c = self.table.get(d.isoformat(), (500.0, 505.0, 495.0, 502.0))
                bars.append({"date": datetime.combine(d, time(0, 0)), "open": o, "high": h,
                             "low": l, "close": c, "volume": 1000})
            d += timedelta(days=1)
        return bars


def _restore(path, saved):
    s, s_uri = saved
    if s is None:
        os.environ.pop("AGENT_CHART_DB", None)
    else:
        os.environ["AGENT_CHART_DB"] = s
    if s_uri is not None:
        os.environ["AGENT_DATA_URI"] = s_uri
    try:
        os.remove(path)
    except OSError:
        pass


def _rows(path, symbol):
    con = sqlite3.connect(path)
    try:
        return con.execute("SELECT bar_time, open, high, low, close, volume FROM ohlc_daily "
                           "WHERE symbol=? ORDER BY bar_time", (symbol,)).fetchall()
    finally:
        con.close()


# --------------------------------------------------------------------------- tests
def test_gap_range_only_missing_dates_leq_asof():
    """A symbol current to as_of requests NOTHING; a symbol stale to D-4 requests D-3..as_of."""
    # AAA current to as_of; BBB last stored = Mon 2026-08-24 -> gap should be 08-25..08-28.
    path, saved = _new_store({"AAA": AS_OF, "BBB": date(2026, 8, 24)})
    try:
        kite = FakeKite({"AAA": 100, "BBB": 200})
        res = fetch_kite.refresh_daily(AS_OF, symbols=["AAA", "BBB"], _client=kite)

        called_tokens = {c[0] for c in kite.calls}
        assert 100 not in called_tokens, "AAA is current to as_of — must request nothing"
        assert 200 in called_tokens, "BBB is stale — must be fetched"
        bcall = next(c for c in kite.calls if c[0] == 200)
        assert bcall[1].date() == date(2026, 8, 25), ("gap must start last+1", bcall[1])
        assert bcall[2].date() == AS_OF, ("gap must end at as_of", bcall[2])
        # every requested/written bar is <= as_of
        for bt, *_ in _rows(path, "BBB"):
            assert bt[:10] <= AS_OF.isoformat()
        assert res["symbols_fetched"] == 1 and res["rows_added"] > 0
        return f"gap fetched {res['rows_added']} rows for BBB, AAA skipped"
    finally:
        _restore(path, saved)


def test_idempotent_append_no_duplicates():
    """Running the same date twice adds no duplicate (symbol, bar_time) rows."""
    path, saved = _new_store({"BBB": date(2026, 8, 24)})
    try:
        kite = FakeKite({"BBB": 200})
        r1 = fetch_kite.refresh_daily(AS_OF, symbols=["BBB"], _client=kite)
        rows1 = _rows(path, "BBB")
        r2 = fetch_kite.refresh_daily(AS_OF, symbols=["BBB"], _client=FakeKite({"BBB": 200}))
        rows2 = _rows(path, "BBB")

        assert r1["rows_added"] > 0
        assert r2["rows_added"] == 0, ("second run must add 0", r2["rows_added"])
        assert rows1 == rows2, "row set changed on idempotent re-run"
        bts = [r[0] for r in rows2]
        assert len(bts) == len(set(bts)), "duplicate bar_time detected"
        return f"idempotent: {len(rows2)} rows after two runs, 0 dupes"
    finally:
        _restore(path, saved)


def test_adjustment_reported_and_values_unmodified():
    """The adjustment string is reported verbatim, and Kite's native prices are written UNCHANGED
    (kite-native corp-action-adjusted basis — no re-adjustment)."""
    path, saved = _new_store({"BBB": date(2026, 8, 24)})
    try:
        kite = FakeKite({"BBB": 200}, close_base=200.0)
        res = fetch_kite.refresh_daily(AS_OF, symbols=["BBB"], _client=kite)
        assert res["adjustment"] == fetch_kite.ADJUSTMENT
        assert "corporate-action-adjusted, dividend-unadjusted" in res["adjustment"]
        # the first FETCHED bar (Tue 2026-08-25; 08-24 is the pre-seeded row) must carry the fake's
        # native OHLC unmodified
        fetched = [r for r in _rows(path, "BBB") if r[0] > "2026-08-24 99"]
        bt, o, h, l, c, v = fetched[0]
        assert bt == "2026-08-25 00:00:00", bt
        assert (o, h, l, c, v) == (200.0, 202.0, 199.0, 201.0, 1000), fetched[0]
        return "adjustment reported; native Kite prices written unmodified"
    finally:
        _restore(path, saved)


def test_point_in_time_never_writes_bar_after_asof():
    """A feed that returns a bar dated as_of+1 must never have it written."""
    path, saved = _new_store({"BBB": date(2026, 8, 24)})
    try:
        kite = FakeKite({"BBB": 200}, extra_future=True)
        res = fetch_kite.refresh_daily(AS_OF, symbols=["BBB"], _client=kite)
        for bt, *_ in _rows(path, "BBB"):
            assert bt[:10] <= AS_OF.isoformat(), f"PIT violation: wrote {bt} > {AS_OF}"
        assert res["date_range"][1] <= AS_OF.isoformat()
        return "PIT enforced: no bar dated after as_of written despite feed returning one"
    finally:
        _restore(path, saved)


def test_missing_token_recorded_not_fatal():
    """A symbol with no instrument token is recorded in errors and skipped; the batch survives."""
    path, saved = _new_store({"GOOD": date(2026, 8, 24), "GHOST": date(2026, 8, 24)})
    try:
        kite = FakeKite({"GOOD": 200})   # GHOST intentionally absent from instruments
        res = fetch_kite.refresh_daily(AS_OF, symbols=["GOOD", "GHOST"], _client=kite)
        assert res["symbols_fetched"] == 1
        assert any(e.get("symbol") == "GHOST" for e in res["errors"]), res["errors"]
        assert _rows(path, "GOOD"), "GOOD should still be written"
        return "missing token recorded (not fatal); other symbol still fetched"
    finally:
        _restore(path, saved)


def test_adjustment_guard_match_proceeds():
    """(a) A liquid anchor whose stored last bar matches Kite within tol -> guard passes, gap appends."""
    last = date(2026, 8, 24)
    path, saved = _new_store_bars({"RELIANCE": (last, (100.0, 102.0, 99.0, 101.0))})
    try:
        kite = GuardKite({"RELIANCE": 555}, table={"2026-08-24": (100.0, 102.0, 99.0, 101.0)})
        res = fetch_kite.refresh_daily(AS_OF, symbols=["RELIANCE"], _client=kite)
        assert not res.get("aborted"), res
        assert res["rows_added"] > 0, ("guard passed -> gap must append", res)
        return f"guard match -> proceeded, {res['rows_added']} rows appended"
    finally:
        _restore(path, saved)


def test_adjustment_guard_drift_aborts_no_rows():
    """(b) A liquid anchor whose Kite OHLC drifts beyond tol -> ABORT, and NO rows are appended."""
    last = date(2026, 8, 24)
    path, saved = _new_store_bars({"RELIANCE": (last, (100.0, 102.0, 99.0, 101.0))})
    try:
        # ~10% higher on every field — a genuine corp-action re-adjustment, far beyond DRIFT_TOL.
        kite = GuardKite({"RELIANCE": 555}, table={"2026-08-24": (110.0, 112.2, 108.9, 111.1)})
        rows_before = _rows(path, "RELIANCE")
        res = fetch_kite.refresh_daily(AS_OF, symbols=["RELIANCE"], _client=kite)
        assert res.get("aborted") is True, res
        assert res["reason"] == "adjustment drift vs store basis", res
        assert res["detail"] and res["detail"][0]["symbol"] == "RELIANCE", res
        assert _rows(path, "RELIANCE") == rows_before, "ABORT must append NOTHING"
        return f"guard drift -> aborted, {len(res['detail'])} field(s) flagged, 0 rows appended"
    finally:
        _restore(path, saved)


def test_adjustment_guard_no_probe_fetchable_aborts():
    """(c) No probe bar fetchable (Kite returns []) -> ABORT with the clear reason, nothing appended."""
    last = date(2026, 8, 24)
    path, saved = _new_store_bars({"RELIANCE": (last, (100.0, 102.0, 99.0, 101.0))})
    try:
        kite = GuardKite({"RELIANCE": 555}, return_empty=True)
        rows_before = _rows(path, "RELIANCE")
        res = fetch_kite.refresh_daily(AS_OF, symbols=["RELIANCE"], _client=kite)
        assert res.get("aborted") is True, res
        assert res["reason"] == "adjustment guard could not verify (no probe bar fetchable)", res
        assert _rows(path, "RELIANCE") == rows_before, "ABORT must append NOTHING"
        return "guard unverifiable (no probe bar) -> aborted, 0 rows appended"
    finally:
        _restore(path, saved)


if __name__ == "__main__":
    tests = [test_gap_range_only_missing_dates_leq_asof,
             test_idempotent_append_no_duplicates,
             test_adjustment_reported_and_values_unmodified,
             test_point_in_time_never_writes_bar_after_asof,
             test_missing_token_recorded_not_fatal,
             test_adjustment_guard_match_proceeds,
             test_adjustment_guard_drift_aborts_no_rows,
             test_adjustment_guard_no_probe_fetchable_aborts]
    results = []
    for fn in tests:
        try:
            results.append((fn.__name__, "PASS", fn()))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== fetch_kite tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
