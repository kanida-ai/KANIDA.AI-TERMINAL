"""
Chart Agent · POST-MARKET EOD job tests.

Runnable two ways:
    pytest backend/agents/chart/tests/test_eod.py
    python  backend/agents/chart/tests/test_eod.py     (prints a pass/fail + timing summary)

DB-dependent tests skip gracefully when the R&D DB is absent. Run against the R&D DB via:
    AGENT_CHART_DB=C:/Users/SPS/Documents/Kanida_Falcon/db/kanida.db
"""
from __future__ import annotations
import os
import sys
import tempfile
import shutil

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from agents.chart import data                                  # noqa: E402
from agents.chart import screener as scr                       # noqa: E402
from agents.chart import eod                                   # noqa: E402

EOD_DATE = "2026-07-31"     # this date has data — expect ~69 setups across ~6 patterns


def _skip(name: str) -> bool:
    if not data.db_available():
        print(f"SKIP {name} — DB absent: {data.db_path()}")
        return True
    return False


def _tmp_screen_dir():
    """Isolate BOTH the screen store and the per-setup store so run_eod (which now precomputes
    per-setup bundles too) never touches the real artifacts / pollutes backend/var."""
    tmp = tempfile.mkdtemp(prefix="chart_eod_")
    saved = {"AGENT_CHART_SCREEN_DIR": os.environ.get("AGENT_CHART_SCREEN_DIR"),
             "AGENT_CHART_SETUP_DIR": os.environ.get("AGENT_CHART_SETUP_DIR")}
    os.environ["AGENT_CHART_SCREEN_DIR"] = tmp
    os.environ["AGENT_CHART_SETUP_DIR"] = os.path.join(tmp, "setups")
    return tmp, saved


def _restore_screen_dir(tmp, saved):
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    shutil.rmtree(tmp, ignore_errors=True)


# (a) run_eod(fetch=False) builds + stores + publishes; load_screen returns the same setups ---------
def test_run_eod_no_fetch_builds_and_publishes():
    if _skip("test_run_eod_no_fetch_builds_and_publishes"):
        return "SKIP"
    tmp, saved = _tmp_screen_dir()
    try:
        res = eod.run_eod(as_of_date=EOD_DATE, fetch=False)

        assert res["fetched"] is False, res["fetched"]              # honest: fetch was disabled
        assert res["as_of_date"] == EOD_DATE
        assert res["trading_day"] is True, "expected a trading day for 2026-07-31"
        assert res["count"] > 0, "empty screen on a known-good date"
        assert res["by_pattern"], "by_pattern not populated"
        assert sum(res["by_pattern"].values()) == res["count"], "by_pattern must sum to count"
        assert "horizontal_trendline" in res["by_pattern"], res["by_pattern"]
        assert res["scan_seconds"] is not None and res["scan_seconds"] > 0
        assert res["published"] is True, "artifact not confirmed on read-back"
        assert res["stored"], "no stored path"

        # the published artifact serves the SAME setups the run reported
        loaded = scr.load_screen(EOD_DATE)
        assert loaded is not None, "load_screen returned None after run_eod"
        assert loaded["count"] == res["count"]
        by_loaded: dict = {}
        for s in loaded["setups"]:
            by_loaded[s["pattern"]] = by_loaded.get(s["pattern"], 0) + 1
        assert by_loaded == res["by_pattern"], (by_loaded, res["by_pattern"])
        return (f"{res['count']} setups across {len(res['by_pattern'])} patterns in "
                f"{res['scan_seconds']}s; published + round-trips")
    finally:
        _restore_screen_dir(tmp, saved)


# (b) with no fetcher wired, the fetch step is SKIPPED with an honest note, scan still runs ----------
def test_run_eod_fetch_skips_honestly_without_hook():
    if _skip("test_run_eod_fetch_skips_honestly_without_hook"):
        return "SKIP"
    tmp, saved = _tmp_screen_dir()
    saved_hook = os.environ.pop("AGENT_CHART_FETCH_FN", None)     # ensure NO hook is wired
    try:
        res = eod.run_eod(as_of_date=EOD_DATE, fetch=True)        # fetch requested, none available
        assert isinstance(res["fetched"], str) and res["fetched"].startswith("skipped:"), res["fetched"]
        assert "prod daily pipeline must be current" in res["fetched"], res["fetched"]
        assert res["count"] > 0, "scan must still run when fetch is skipped"
        return f"fetch honestly skipped; scan still found {res['count']} setups"
    finally:
        if saved_hook is not None:
            os.environ["AGENT_CHART_FETCH_FN"] = saved_hook
        _restore_screen_dir(tmp, saved)


# (c) an injected fetch_fn is CALLED (guarded), and a failing one is recorded, never fatal -----------
def test_run_eod_fetch_fn_injection_and_guard():
    if _skip("test_run_eod_fetch_fn_injection_and_guard"):
        return "SKIP"
    tmp, saved = _tmp_screen_dir()
    calls = []
    try:
        res_ok = eod.run_eod(as_of_date=EOD_DATE, fetch=True,
                             fetch_fn=lambda d: calls.append(d))
        assert calls == [EOD_DATE], calls
        assert res_ok["fetched"] is True, res_ok["fetched"]
        assert res_ok["count"] > 0

        def _boom(d):
            raise RuntimeError("feed down")
        res_bad = eod.run_eod(as_of_date=EOD_DATE, fetch=True, fetch_fn=_boom)
        assert isinstance(res_bad["fetched"], str) and res_bad["fetched"].startswith("error:"), \
            res_bad["fetched"]
        assert res_bad["count"] > 0, "a fetch failure must not sink the scan"
        return "fetch_fn injected + called; failing fetch recorded, scan survived"
    finally:
        _restore_screen_dir(tmp, saved)


# (d) CLI smoke: --no-fetch exits 0 and prints the summary ------------------------------------------
def test_cli_smoke_no_fetch():
    if _skip("test_cli_smoke_no_fetch"):
        return "SKIP"
    tmp, saved = _tmp_screen_dir()
    try:
        rc = eod._main(["--date", EOD_DATE, "--no-fetch"])
        assert rc == 0, rc
        return "CLI --no-fetch exited 0"
    finally:
        _restore_screen_dir(tmp, saved)


if __name__ == "__main__":
    results = []
    for fn in (test_run_eod_no_fetch_builds_and_publishes,
               test_run_eod_fetch_skips_honestly_without_hook,
               test_run_eod_fetch_fn_injection_and_guard,
               test_cli_smoke_no_fetch):
        try:
            r = fn()
            results.append((fn.__name__, "PASS", r))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== Chart Agent EOD tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
