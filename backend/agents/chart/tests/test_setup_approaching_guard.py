"""
Chart Agent · APPROACHING-setup guard + endpoint no-500 + tier honesty + touch dedupe.

Regression tests for three defects found in live validation (2026-08-28) of the Chart Agent:

  FIX 1/3  build_setup must return a VALID bundle for an APPROACHING (or any) stage — never a crash —
           so the EOD precompute writes a bundle for EVERY setup (breakout AND approaching AND retest),
           not just the ones whose stats happened to serialise.
  FIX 2    chart_setup / chart_bars must NEVER 500. FastAPI renders the returned dict with
           allow_nan=False AFTER the handler returns (outside its try/except), so a NaN/Inf or a numpy
           scalar leaking into the payload escapes as a bare HTTP 500. The endpoints now sanitise +
           validate the payload inside the guard and degrade to ok:false, never a 500.
  FIX 4    tier honesty — an APPROACHING (or FAILED/forming) setup must NOT be tier="qualified".
           qualified requires a CONFIRMED, tradeable stage (BREAKOUT/RETEST) that also clears §9.
  FIX 5    geometry touches deduped by (date, price) so no stacked anchors are drawn.

Runnable two ways:
    pytest backend/agents/chart/tests/test_setup_approaching_guard.py
    python  backend/agents/chart/tests/test_setup_approaching_guard.py    (pass/fail summary)

DB-dependent tests skip gracefully when the R&D DB is absent. Run against the R&D DB via:
    AGENT_CHART_DB=C:/Users/SPS/Documents/Kanida_Falcon/db/kanida.db
"""
from __future__ import annotations
import os
import sys
import math
import time
import tempfile
import shutil

import numpy as np

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from agents import registry                                    # noqa: E402
from agents import router as R                                 # noqa: E402
from agents.chart import data                                  # noqa: E402
from agents.chart import setup as csetup                       # noqa: E402
from agents.chart import screener as scr                       # noqa: E402
from agents.chart.patterns import registry as patterns         # noqa: E402

registry.load_builtin()
patterns.load_builtin()

PATTERN = "horizontal_trendline"
SAMPLE_DATE = "2026-07-31"
# Symbols observed at each live stage on SAMPLE_DATE (horizontal). Used only to seed the DB tests
# cheaply (no full-universe scan); the tests assert on the REAL stage build_setup returns, not these.
STAGE_SEEDS = {
    "APPROACHING": ["MMP", "APOLLOPIPE", "SUDEEPPHRM", "AARTISURF", "WESTLIFE"],
    "BREAKOUT": ["BBTCL", "NAZARA"],
    "RETEST": ["ESAFSFB", "AARVI"],
    "FAILED": ["SAURASHCEM", "ACI"],
}


def _skip(name: str) -> bool:
    if not data.db_available():
        print(f"SKIP {name} — DB absent: {data.db_path()}")
        return True
    return False


def _first_approaching():
    """Return (symbol, build_setup-dict) for the first seed symbol that build_setup reports as
    APPROACHING on SAMPLE_DATE, else (None, None). Point-in-time; no full-universe scan."""
    for sym in STAGE_SEEDS["APPROACHING"]:
        res = csetup.build_setup(sym, PATTERN, SAMPLE_DATE)
        if res.get("ok") and res.get("stage") == "APPROACHING":
            return sym, res
    return None, None


# (1) json_safe unit — NaN/Inf/numpy get sanitised, never leak -------------------------------------
def test_json_safe_sanitises_nan_and_numpy():
    raw = {
        "f_nan": float("nan"), "f_inf": float("inf"), "f_ok": 1.25,
        "np_f_nan": np.float64("nan"), "np_i": np.int64(7), "np_b": np.bool_(True),
        "arr": np.array([1.0, np.nan, 3.0]),
        "nested": {"list": [np.int64(1), float("-inf"), "x", None]},
    }
    safe = csetup.json_safe(raw)
    assert safe["f_nan"] is None and safe["f_inf"] is None
    assert safe["f_ok"] == 1.25
    assert safe["np_f_nan"] is None
    assert safe["np_i"] == 7 and isinstance(safe["np_i"], int) and not isinstance(safe["np_i"], np.generic)
    assert safe["np_b"] is True and isinstance(safe["np_b"], bool)
    assert safe["arr"] == [1.0, None, 3.0]
    assert safe["nested"]["list"] == [1, None, "x", None]
    # proves it serialises under the SAME strictness FastAPI uses
    import json
    json.dumps(safe, allow_nan=False)
    return "json_safe: NaN/Inf->None, numpy->python, ndarray->list, nested; strict-serialisable"


# (2) FIX 1 — build_setup(APPROACHING) returns ok:true with geometry + watch_plan, no raise ---------
def test_build_setup_approaching_ok_no_crash():
    if _skip("test_build_setup_approaching_ok_no_crash"):
        return "SKIP"
    sym, res = _first_approaching()
    if sym is None:
        print("SKIP test_build_setup_approaching_ok_no_crash — no APPROACHING seed on", SAMPLE_DATE)
        return "SKIP"
    assert res["ok"] is True, res
    assert res["stage"] == "APPROACHING"
    geom = res.get("geometry")
    assert geom is not None, "approaching must still carry drawable geometry (level_line + touches)"
    assert geom.get("level_line") is not None
    assert isinstance(geom.get("touches"), list) and len(geom["touches"]) >= 1
    assert res.get("watch_plan") is not None and res["watch_plan"].get("confirmation") is not None
    # honest evidence: present-and-real OR the honest insufficient/None state — NEVER fabricated
    assert "evidence" in res and "paths" in res
    # whole bundle is strict-JSON-serialisable (no NaN/numpy leaked)
    import json
    json.dumps(res, allow_nan=False)
    return f"build_setup(APPROACHING {sym}) ok:true, geometry+watch_plan present, strict-serialisable"


# (3) FIX 2 — chart_setup / chart_bars never 500 (forced raise + forced NaN/numpy) ------------------
def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI()
    app.include_router(R.router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def test_chart_setup_never_500():
    c = _client()
    saved_db = data.db_available
    saved_build = csetup.build_setup
    saved_bundle = scr.load_setup_bundle
    try:
        data.db_available = lambda: True             # reach the build path even without a DB
        scr.load_setup_bundle = lambda *a, **k: None  # force the live path (no precomputed bundle)

        # (a) build_setup RAISES -> handler guard -> 200 ok:false, never 500
        def _boom(*a, **k):
            raise RuntimeError("forced build_setup crash")
        csetup.build_setup = _boom
        r = c.get("/api/agents/chart/setup",
                  params={"symbol": "MMP", "pattern": PATTERN, "date": SAMPLE_DATE})
        assert r.status_code == 200, r.status_code
        assert r.json().get("ok") is False and "forced build_setup crash" in (r.json().get("error") or "")

        # (b) build_setup returns NaN/Inf + numpy (the real live cause) -> 200, sanitised, never 500
        def _nan(*a, **k):
            return {"ok": True, "symbol": "MMP", "pattern": PATTERN, "date": SAMPLE_DATE,
                    "stage": "APPROACHING", "level": 100.0,
                    "bad_nan": float("nan"), "bad_inf": float("inf"),
                    "np_i": np.int64(3), "np_b": np.bool_(False),
                    "decision": {"ci_low": np.float64("nan")}}
        csetup.build_setup = _nan
        r = c.get("/api/agents/chart/setup",
                  params={"symbol": "MMP", "pattern": PATTERN, "date": SAMPLE_DATE})
        assert r.status_code == 200, r.status_code
        j = r.json()
        assert j.get("ok") is True
        assert j.get("bad_nan") is None and j.get("bad_inf") is None
        assert j.get("np_i") == 3 and j.get("np_b") is False
        assert j.get("decision", {}).get("ci_low") is None
    finally:
        data.db_available = saved_db
        csetup.build_setup = saved_build
        scr.load_setup_bundle = saved_bundle
    return "chart_setup never 500s: forced-raise -> ok:false; NaN/numpy -> 200 sanitised"


def test_chart_bars_never_500():
    c = _client()
    saved_db = data.db_available
    saved_load = None
    try:
        from agents.chart import data as cdata
        saved_db = cdata.db_available
        cdata.db_available = lambda: True
        # force the live read to blow up -> handler guard -> 200 ok:false
        from agents.chart import agent as cagent
        saved_load = cagent._as_of_idx
        cagent._as_of_idx = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("forced bars crash"))
        r = c.get("/api/agents/chart/bars",
                  params={"symbol": "MMP", "date": SAMPLE_DATE, "lookback": 50})
        assert r.status_code == 200, r.status_code
        assert r.json().get("ok") is False
    finally:
        from agents.chart import data as cdata
        cdata.db_available = saved_db
        if saved_load is not None:
            from agents.chart import agent as cagent
            cagent._as_of_idx = saved_load
    return "chart_bars never 500s: forced live-read crash -> 200 ok:false"


# (4) FIX 4 — tier honesty: APPROACHING/FAILED can never be qualified -------------------------------
def test_tier_approaching_not_qualified():
    t = csetup.tier_of
    # qualified requires a CONFIRMED, TRADE-verdict stage
    assert t("BREAKOUT", "TRADE", None, True, 50) == "qualified"
    assert t("RETEST", "TRADE", None, True, 50) == "qualified"
    # a TRADE-worthy edge that has NOT confirmed yet is NOT qualified — honest watch
    assert t("APPROACHING", "TRADE", None, True, 50) == "watch"
    assert t("FAILED", "TRADE", None, True, 50) == "watch"
    # NO_TRADE -> weak regardless of stage
    assert t("APPROACHING", "NO_TRADE", None, False, 50) == "weak"
    # strong is a confirmed stage with a positive edge but no TRADE verdict yet
    assert t("BREAKOUT", "WATCH", None, True, 15) == "strong"
    assert t("APPROACHING", "WATCH", None, True, 15) == "watch"
    return "tier: qualified only on confirmed BREAKOUT/RETEST+TRADE; approaching/failed never qualified"


# (5) FIX 5 — geometry touches deduped by (date, price) --------------------------------------------
def test_touch_dedupe_unit():
    dup = [{"date": "2026-06-15", "price": 141.4}, {"date": "2026-06-15", "price": 141.4},
           {"date": "2026-06-20", "price": 141.5}, None,
           {"date": "2026-06-15", "price": 141.4}]
    out = csetup._dedupe_touches(dup)
    assert out == [{"date": "2026-06-15", "price": 141.4}, {"date": "2026-06-20", "price": 141.5}]
    return "touch dedupe: identical (date,price) anchors collapsed, order preserved"


def test_build_setup_touches_have_no_dupes():
    if _skip("test_build_setup_touches_have_no_dupes"):
        return "SKIP"
    sym, res = _first_approaching()
    if sym is None:
        return "SKIP"
    touches = (res.get("geometry") or {}).get("touches") or []
    keys = [(t.get("date"), t.get("price")) for t in touches]
    assert len(keys) == len(set(keys)), f"duplicate touch anchors leaked: {keys}"
    return f"build_setup({sym}) geometry touches carry no (date,price) duplicates"


# (6) FIX 3 — precompute writes a bundle for EVERY setup (approaching included) ---------------------
def test_precompute_covers_every_stage():
    if _skip("test_precompute_covers_every_stage"):
        return "SKIP"
    tmp = tempfile.mkdtemp(prefix="chart_setups_cover_")
    saved = os.environ.get("AGENT_CHART_SETUP_DIR")
    os.environ["AGENT_CHART_SETUP_DIR"] = tmp
    try:
        seeds = [{"symbol": s, "pattern": PATTERN}
                 for lst in STAGE_SEEDS.values() for s in lst]
        summary = scr._precompute_setups(SAMPLE_DATE, seeds)
        assert summary["errors"] == 0, summary["error_sample"]
        assert summary["precomputed"] == len(seeds), summary
        stages = set()
        for seed in seeds:
            b = scr.load_setup_bundle(SAMPLE_DATE, seed["symbol"], PATTERN)
            assert isinstance(b, dict) and b.get("ok"), f"no bundle for {seed['symbol']}"
            assert isinstance(b.get("bars"), list) and b["bars"], "bundle missing embedded bars"
            if b.get("stage"):
                stages.add(b["stage"])
        # the whole point of the fix: APPROACHING now yields a bundle (it used to be skipped)
        assert "APPROACHING" in stages, f"approaching not covered; stages={stages}"
        assert stages & {"BREAKOUT", "RETEST"}, f"no confirmed stage covered; stages={stages}"
        return (f"precompute wrote a bundle for all {len(seeds)} seeds; "
                f"stages covered={sorted(stages)}")
    finally:
        if saved is None:
            os.environ.pop("AGENT_CHART_SETUP_DIR", None)
        else:
            os.environ["AGENT_CHART_SETUP_DIR"] = saved
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    tests = [
        test_json_safe_sanitises_nan_and_numpy,
        test_build_setup_approaching_ok_no_crash,
        test_chart_setup_never_500,
        test_chart_bars_never_500,
        test_tier_approaching_not_qualified,
        test_touch_dedupe_unit,
        test_build_setup_touches_have_no_dupes,
        test_precompute_covers_every_stage,
    ]
    results = []
    for fn in tests:
        t0 = time.perf_counter()
        try:
            r = fn()
            results.append((fn.__name__, "PASS", r, time.perf_counter() - t0))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e), time.perf_counter() - t0))
    print("\n=== Chart Agent approaching-guard / no-500 / tier / dedupe tests ===")
    for name, status, detail, secs in results:
        print(f"  [{status}] {name} ({secs:.2f}s)" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _, _ in results):
        sys.exit(1)
