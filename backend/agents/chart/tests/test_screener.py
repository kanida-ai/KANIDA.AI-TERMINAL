"""
Chart Agent · FULL-UNIVERSE SCREENER tests.

Runnable two ways:
    pytest backend/agents/chart/tests/test_screener.py
    python  backend/agents/chart/tests/test_screener.py    (prints a pass/fail + timing summary)

All DB-dependent tests skip gracefully when the R&D DB is absent. Point on point-in-time (v3 §0):
the screen for date D must only ever read bars dated <= D — asserted structurally below.

Runnable against the R&D DB via:
    AGENT_CHART_DB=C:/Users/SPS/Documents/Kanida_Falcon/db/kanida.db
"""
from __future__ import annotations
import os
import sys
import time
import tempfile
import shutil

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import pandas as pd                                            # noqa: E402
from agents import registry                                   # noqa: E402
from agents.chart import data                                 # noqa: E402
from agents.chart import screener as scr                      # noqa: E402
from agents.chart.patterns.horizontal_trendline import HorizontalTrendlineDetector  # noqa: E402

registry.load_builtin()

TITAN_DATE = "2022-08-30"
TITAN_LEVEL = 2565.0


def _skip(name: str) -> bool:
    if not data.db_available():
        print(f"SKIP {name} — DB absent: {data.db_path()}")
        return True
    return False


# (a) full-universe scan surfaces the known TITAN breakout ------------------------------------------
def test_scan_universe_includes_titan_breakout():
    if _skip("test_scan_universe_includes_titan_breakout"):
        return "SKIP"
    syms = data.all_symbols()
    assert len(syms) > 100, f"universe suspiciously small: {len(syms)}"          # full, not the 16
    assert data.NIFTY not in syms, "NIFTY must be excluded from the tradable universe"

    setups = scr.scan_universe(TITAN_DATE)
    assert setups, "empty screen"
    titan = [s for s in setups if s["symbol"] == "TITAN"]
    assert titan, "TITAN not in the full-universe screen for 2022-08-30"
    t = titan[0]
    assert t["stage"] == "BREAKOUT", f"expected BREAKOUT, got {t['stage']}"
    assert abs(t["level"] - TITAN_LEVEL) / TITAN_LEVEL < 0.01, f"level {t['level']} !~= {TITAN_LEVEL}"
    assert t["pattern"] == "horizontal_trendline"
    # triangle/channel are SPEC skeletons -> they contribute nothing (honest)
    assert {s["pattern"] for s in setups} == {"horizontal_trendline"}, \
        "only the BUILT pattern should produce setups today"
    return f"{len(setups)} setups over {len(syms)} symbols; TITAN BREAKOUT @ {t['level']}"


# (b) point-in-time: the D screen never references a bar dated > D ----------------------------------
def test_point_in_time_no_future_bars():
    if _skip("test_point_in_time_no_future_bars"):
        return "SKIP"
    D = pd.Timestamp(TITAN_DATE)

    # 1) STRUCTURAL: the windowed panel the classifier consumes holds ONLY bars <= D.
    panel = data.load_panel(TITAN_DATE, scr.LOOKBACK_DAYS)
    assert panel, "empty panel"
    worst = max(df.index.max() for df in panel.values() if len(df))
    assert worst <= D, f"panel leaked a future bar: {worst} > {D}"

    # 2) EQUIVALENCE: the windowed detect equals the FULL-HISTORY detect sliced at D. TITAN has many
    #    bars AFTER D; if any leaked through the windowing, the level/stage would differ. It doesn't.
    full = data.load_daily("TITAN")
    full.attrs["symbol"] = "TITAN"
    assert full.index.max() > D, "TITAN should have post-D history for this test to be meaningful"
    k = int(full.index.get_loc(D))
    occ_full = HorizontalTrendlineDetector().detect(full, as_of_idx=k)
    titan_scr = [s for s in scr.scan_universe(TITAN_DATE) if s["symbol"] == "TITAN"]
    assert occ_full and titan_scr, "TITAN missing from one path"
    assert occ_full[0].stage == titan_scr[0]["stage"]
    assert abs(occ_full[0].level - titan_scr[0]["level"]) < 1e-6, \
        (occ_full[0].level, titan_scr[0]["level"])          # post-D bars changed nothing
    return f"no future bars (max {worst.date()} <= {D.date()}); windowed==full-history"


# (c) build_screen -> load_screen round-trip is identical -------------------------------------------
def test_build_load_roundtrip():
    if _skip("test_build_load_roundtrip"):
        return "SKIP"
    tmp = tempfile.mkdtemp(prefix="chart_screen_")
    saved = os.environ.get("AGENT_CHART_SCREEN_DIR")
    try:
        os.environ["AGENT_CHART_SCREEN_DIR"] = tmp
        built = scr.build_screen(TITAN_DATE)
        loaded = scr.load_screen(TITAN_DATE)
        assert loaded is not None, "load_screen returned None right after build_screen"
        assert loaded["setups"] == built["setups"], "round-trip setups differ"
        assert loaded["count"] == built["count"] == len(built["setups"])
        assert loaded["as_of_date"] == TITAN_DATE
        # load_screen(None) and an unbuilt date honestly return None (no fabrication)
        assert scr.load_screen(None) is None
        assert scr.load_screen("1990-01-02") is None
    finally:
        if saved is None:
            os.environ.pop("AGENT_CHART_SCREEN_DIR", None)
        else:
            os.environ["AGENT_CHART_SCREEN_DIR"] = saved
        shutil.rmtree(tmp, ignore_errors=True)
    return f"round-trip identical over {loaded['count']} setups"


# (d) MEASURED timing — informational -------------------------------------------------------------
def test_timing_informational():
    if _skip("test_timing_informational"):
        return "SKIP"
    tmp = tempfile.mkdtemp(prefix="chart_screen_t_")
    saved = os.environ.get("AGENT_CHART_SCREEN_DIR")
    try:
        os.environ["AGENT_CHART_SCREEN_DIR"] = tmp
        t0 = time.perf_counter()
        payload = scr.build_screen(TITAN_DATE)          # POST-MARKET precompute (full universe)
        precompute_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        _ = scr.load_screen(TITAN_DATE)                 # SERVE (what the request path pays)
        serve_ms = (time.perf_counter() - t0) * 1000
        print(f"\n[timing] full-universe PRECOMPUTE: {payload['universe_size']} symbols, "
              f"{payload['count']} setups in {precompute_s:.2f}s  |  SERVE latency {serve_ms:.1f}ms")
        assert serve_ms < precompute_s * 1000, "serve must be far cheaper than precompute"
    finally:
        if saved is None:
            os.environ.pop("AGENT_CHART_SCREEN_DIR", None)
        else:
            os.environ["AGENT_CHART_SCREEN_DIR"] = saved
        shutil.rmtree(tmp, ignore_errors=True)
    return f"precompute {precompute_s:.2f}s / serve {serve_ms:.1f}ms"


# (e) FRESHNESS — a non-trading D returns an honest empty screen, not a relabeled prior session -----
HOLIDAY_DATE = "2022-10-26"   # Diwali Balipratipada — NSE closed; prior session traded 2022-10-25


def test_freshness_holiday_returns_empty():
    if _skip("test_freshness_holiday_returns_empty"):
        return "SKIP"
    res = scr.scan_universe_detailed(HOLIDAY_DATE)
    assert res["trading_day"] is False, "market did trade on this date — pick a real holiday"
    assert res["count"] == 0 and res["setups"] == [], \
        f"non-trading D leaked {res['count']} relabeled setups"
    assert res["scanned"] == 0, res["scanned"]
    assert res["note"] and "no trading" in res["note"], res["note"]
    # every in-window symbol is bucketed as stale (last bar < D), none classified
    assert res["skipped_stale"] > 0, res
    # and a real trading day still works
    td = scr.scan_universe_detailed(TITAN_DATE)
    assert td["trading_day"] is True
    titan = [s for s in td["setups"] if s["symbol"] == "TITAN"]
    assert titan and titan[0]["stage"] == "BREAKOUT", "trading-day screen lost TITAN"
    return f"holiday {HOLIDAY_DATE}: 0 setups (stale={res['skipped_stale']}); trading day still finds TITAN"


# (f) ACCOUNTING — coverage buckets are present, sum to the universe, and scanned is exact ----------
def test_coverage_accounting():
    if _skip("test_coverage_accounting"):
        return "SKIP"
    res = scr.scan_universe_detailed(TITAN_DATE)
    for k in ("scanned", "skipped_min_bars", "skipped_stale", "skipped_no_window", "universe_size"):
        assert isinstance(res[k], int), (k, res.get(k))
    total = (res["scanned"] + res["skipped_min_bars"] + res["skipped_stale"] + res["skipped_no_window"])
    assert total == res["universe_size"], (total, res["universe_size"])   # nothing double-counted/lost

    # independently reconstruct 'scanned' = in-window symbols with >= MIN_BARS bars that traded ON D
    as_of_ts = pd.Timestamp(TITAN_DATE).normalize()
    panel = data.load_panel(TITAN_DATE, scr.LOOKBACK_DAYS)
    expect_scanned = sum(1 for s in data.all_symbols()
                         if len(panel.get(s, [])) >= scr.MIN_BARS
                         and len(panel.get(s, [])) and panel[s].index[-1].normalize() == as_of_ts)
    assert res["scanned"] == expect_scanned, (res["scanned"], expect_scanned)
    assert res["scanned"] < res["universe_size"], "coverage gap should be visible, not hidden"
    return (f"universe={res['universe_size']} scanned={res['scanned']} "
            f"min_bars={res['skipped_min_bars']} stale={res['skipped_stale']} "
            f"no_window={res['skipped_no_window']} count={res['count']}")


# (g) S3 store — guarded read returns None on a miss, and a faked-client round-trips ----------------
def test_s3_store_guarded_and_roundtrip():
    st = scr.S3ScreenStore("s3://no-such-bucket-xyzzy/agents/chart")
    assert st.bucket == "no-such-bucket-xyzzy" and st.prefix == "agents/chart"
    assert st._key("2022-08-30") == "agents/chart/screen_2022-08-30.json"
    # guarded: a missing bucket/key (or absent creds/boto3) must return None, never raise
    assert st.read("2022-08-30") is None

    # faked boto3 client -> exercise write()/read() round-trip without real S3
    import types
    blob = {}

    class _FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            blob[(Bucket, Key)] = Body
        def get_object(self, Bucket, Key):  # noqa: N803
            if (Bucket, Key) not in blob:
                raise KeyError("NoSuchKey")
            return {"Body": types.SimpleNamespace(read=lambda: blob[(Bucket, Key)])}

    fake = types.ModuleType("boto3")
    fake.client = lambda svc: _FakeS3()
    saved = sys.modules.get("boto3")
    try:
        sys.modules["boto3"] = fake
        uri = st.write("2022-08-30", {"as_of_date": "2022-08-30", "setups": [], "count": 0})
        assert uri == "s3://no-such-bucket-xyzzy/agents/chart/screen_2022-08-30.json"
        got = st.read("2022-08-30")
        assert got and got["as_of_date"] == "2022-08-30"
        assert st.read("1990-01-02") is None   # still a clean miss
    finally:
        if saved is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = saved
    return "s3 guarded miss=None; faked put/get round-trip ok"


if __name__ == "__main__":
    results = []
    for fn in (test_scan_universe_includes_titan_breakout, test_point_in_time_no_future_bars,
               test_build_load_roundtrip, test_timing_informational,
               test_freshness_holiday_returns_empty, test_coverage_accounting,
               test_s3_store_guarded_and_roundtrip):
        try:
            r = fn()
            results.append((fn.__name__, "PASS", r))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== Chart Agent screener tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
