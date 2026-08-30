"""
Chart Agent · tests for the 3-column setup backend: quality.py, setup.py, story.py, and the
/bars + /setup endpoints (router). DB-dependent tests skip gracefully when the R&D DB is absent.

Run:
    AGENT_CHART_DB=C:/Users/SPS/Documents/Kanida_Falcon/db/kanida.db \
        python backend/agents/chart/tests/test_setup_quality.py
    (or via pytest)

POINT-IN-TIME (v3 §0): every setup/paths/quality computation reads only bars <= the requested date —
asserted structurally (the geometry anchor dates and every bar date are <= the as-of date).
"""
from __future__ import annotations
import os
import sys

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import pandas as pd                                            # noqa: E402
from agents import registry                                   # noqa: E402
from agents.chart import data                                 # noqa: E402
from agents.chart import quality as q                         # noqa: E402
from agents.chart import setup as csetup                      # noqa: E402
from agents.chart import story as cstory                      # noqa: E402
from agents.chart.patterns import registry as patterns        # noqa: E402

registry.load_builtin()
patterns.load_builtin()

# Stable mid-history cases (well before the volatile end-of-data tail). Verified real live setups.
H_CASE = ("TITAN", "horizontal_trendline", "2026-04-08")      # BREAKOUT, has 8 precedents
FW_CASE = ("SBIN", "falling_wedge", "2025-04-17")             # BREAKOUT long, sloped geometry
AT_CASE = ("RELIANCE", "ascending_triangle", "2024-06-27")    # BREAKOUT long


def _skip(name: str) -> bool:
    if not data.db_available():
        print(f"SKIP {name} — DB absent: {data.db_path()}")
        return True
    return False


# ------------------------------------------------------------------ quality (pure, no DB)
def test_quality_bounds_and_weights():
    """Score is 0-100; weights of the AVAILABLE sub-scores renormalize to 1; horizontal drops
    contraction (renormalizes over 4); apex patterns keep all 5."""
    r = q.compute_quality("horizontal_trendline",
                          {"flatness": 0.9, "n_touches": 3, "distance_pct": 1.0,
                           "volume_x": 1.5, "contraction": None})
    assert 0 <= r["score"] <= 100, r
    assert r["subscores"]["contraction"] is None
    assert abs(sum(r["weights"].values()) - 1.0) < 1e-6, r["weights"]

    r2 = q.compute_quality("falling_wedge",
                           {"r2": 0.99, "n_touches": 3, "distance_pct": 3.0,
                            "volume_x": 2.0, "contraction": 0.4})
    assert 0 <= r2["score"] <= 100
    assert r2["subscores"]["contraction"] is not None
    assert abs(sum(r2["weights"].values()) - 1.0) < 1e-6
    print(f"PASS test_quality_bounds_and_weights (horiz={r['score']}, wedge={r2['score']})")


def test_quality_monotonic_in_volume():
    """More volume thrust never lowers the score (a real monotonicity of the formula)."""
    lo = q.compute_quality("rectangle", {"r2": 0.8, "n_touches": 3, "distance_pct": 1.0, "volume_x": 1.1})
    hi = q.compute_quality("rectangle", {"r2": 0.8, "n_touches": 3, "distance_pct": 1.0, "volume_x": 2.0})
    assert hi["score"] >= lo["score"]
    print(f"PASS test_quality_monotonic_in_volume ({lo['score']} <= {hi['score']})")


def test_quality_guarded():
    """Never raises on junk input."""
    r = q.compute_quality("channel", {"r2": None, "n_touches": None, "volume_x": None})
    assert "score" in r
    print("PASS test_quality_guarded")


# ------------------------------------------------------------------ watch plan (pure)
def test_watch_plan_directional():
    lp = csetup.watch_plan(100.0, "long")
    assert lp["confirmation"] == 100.0 and lp["invalidation"] < 100.0 < 1e9
    assert lp["warning"] < lp["confirmation"]
    sp = csetup.watch_plan(100.0, "short")
    assert sp["invalidation"] > 100.0 and sp["warning"] > sp["confirmation"]
    print(f"PASS test_watch_plan_directional (long inval={lp['invalidation']}, short inval={sp['invalidation']})")


# ------------------------------------------------------------------ setup builder (DB)
def _assert_pit(setup: dict, as_of: str):
    """Every date in the geometry must be <= the as-of date (point-in-time)."""
    g = setup.get("geometry") or {}
    dates = []
    for key in ("upper", "lower"):
        blk = g.get(key)
        if blk:
            for p in (blk.get("a"), blk.get("b"), blk.get("extend_to")):
                if p and p.get("date"):
                    dates.append(p["date"])
    for t in (g.get("touches") or []):
        if t.get("date"):
            dates.append(t["date"])
    ll = g.get("level_line")
    if ll:
        for p in (ll.get("from"), ll.get("to")):
            if p and p.get("date"):
                dates.append(p["date"])
    for d in dates:
        assert d <= as_of, f"geometry date {d} > as_of {as_of} — point-in-time violation"
    return len(dates)


def test_setup_horizontal_full():
    if _skip("test_setup_horizontal_full"):
        return
    sym, pat, dt = H_CASE
    s = csetup.build_setup(sym, pat, dt)
    assert s["ok"] and s["stage"] == "BREAKOUT", s
    assert s["level"] and s["quality"]["score"] is not None
    g = s["geometry"]
    assert g["level_line"] and g["touches"] and g["upper"] is None      # flat level, no sloped lines
    ndates = _assert_pit(s, s["as_of_date"])
    # evidence + paths + decision are REAL and resolved-only
    assert s["evidence"]["summary"]["n"] >= 1
    assert s["paths"]["n_total"] == s["evidence"]["summary"]["n"]
    assert s["decision"]["decision"] in ("TRADE", "WATCH", "NO_TRADE")
    print(f"PASS test_setup_horizontal_full (q={s['quality']['score']}, n={s['evidence']['summary']['n']}, "
          f"decision={s['decision']['decision']}, geom_dates={ndates})")


def test_setup_sloped_geometry():
    if _skip("test_setup_sloped_geometry"):
        return
    for sym, pat, dt in (FW_CASE, AT_CASE):
        s = csetup.build_setup(sym, pat, dt)
        assert s["ok"] and s["stage"] in ("BREAKOUT", "RETEST", "APPROACHING"), (pat, s)
        g = s["geometry"]
        assert g["upper"] and g["lower"], (pat, "sloped patterns must expose both lines")
        assert g["upper"]["a"]["price"] and g["lower"]["a"]["price"]
        _assert_pit(s, s["as_of_date"])
        assert s["quality"]["score"] is not None
        assert s["watch_plan"]["confirmation"] == s["level"]
        print(f"PASS test_setup_sloped_geometry[{pat}] (q={s['quality']['score']}, "
              f"upper_r2 via quality, stage={s['stage']})")


def test_setup_unknown_pattern_guarded():
    s = csetup.build_setup("TITAN", "does_not_exist", "2026-04-08")
    assert s["ok"] is False and "unknown pattern" in s["note"]
    print("PASS test_setup_unknown_pattern_guarded")


def test_setup_no_stage_honest():
    """A date with no live stage returns ok:true, stage:None (honest), not a crash."""
    if _skip("test_setup_no_stage_honest"):
        return
    s = csetup.build_setup("TITAN", "cup_and_handle", "2020-01-02")
    assert s["ok"] and s["stage"] is None
    print("PASS test_setup_no_stage_honest")


# ------------------------------------------------------------------ paths cohort split (DB)
def test_win_loss_paths_split():
    if _skip("test_win_loss_paths_split"):
        return
    sym, pat, dt = H_CASE
    s = csetup.build_setup(sym, pat, dt)
    p = s["paths"]
    assert p["n_win"] + p["n_loss"] == p["n_total"]
    if p["winners"]:
        assert len(p["winners"]) == 10 and p["winners"][-1] > 0    # winners end positive by definition
    if p["losers"]:
        assert len(p["losers"]) == 10 and p["losers"][-1] <= 0
    print(f"PASS test_win_loss_paths_split (n_win={p['n_win']}, n_loss={p['n_loss']}, small_n={p['small_n']})")


# ------------------------------------------------------------------ enrichment + story (DB)
def test_enrich_real():
    if _skip("test_enrich_real"):
        return
    e = csetup.enrich(*H_CASE)
    assert e["quality_score"] is not None
    assert e["evidence_summary"]["n"] >= 1
    assert e["tier"] in ("qualified", "strong", "watch", "weak")
    assert e["hook"] and "Horizontal Trendline" in e["hook"]
    print(f"PASS test_enrich_real (tier={e['tier']}, hook={e['hook']!r})")


def test_market_story_breadth_and_sector():
    setups = [
        {"symbol": "TITAN", "pattern": "horizontal_trendline", "stage": "BREAKOUT", "direction": "long"},
        {"symbol": "SBIN", "pattern": "falling_wedge", "stage": "BREAKOUT", "direction": "long"},
        {"symbol": "HDFCBANK", "pattern": "channel", "stage": "APPROACHING", "direction": "short"},
        {"symbol": "ZZUNMAPPED", "pattern": "rectangle", "stage": "FAILED", "direction": "long"},
    ]
    ms = cstory.market_story(setups, "2026-04-08")
    b = ms["breadth"]
    assert b["total"] == 4 and b["bullish"] == 3 and b["bearish"] == 1
    assert b["by_stage"]["BREAKOUT"] == 2
    sec = ms["sector"]
    if data.db_available() and data.sector_map():
        assert sec["available"] is True and sec["source"] == "instrument_labels.sector"
        assert sec["unmapped"] >= 1     # ZZUNMAPPED has no sector
        print(f"PASS test_market_story (sectors={list(sec['by_sector'])[:3]}, unmapped={sec['unmapped']})")
    else:
        assert sec["available"] is False
        print("PASS test_market_story (sector source unavailable — honest)")


# ------------------------------------------------------------------ endpoints (guarded)
def test_endpoints_guarded_no_500():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import agents.router as r
    app = FastAPI()
    app.include_router(r.router, prefix="/api")
    c = TestClient(app)
    # bad symbol must not 500
    assert c.get("/api/agents/chart/bars", params={"symbol": "___NOPE___"}).status_code == 200
    assert c.get("/api/agents/chart/setup",
                 params={"symbol": "___NOPE___", "pattern": "falling_wedge"}).status_code == 200
    if data.db_available():
        b = c.get("/api/agents/chart/bars", params={"symbol": "TITAN", "date": "2026-04-08", "lookback": 10}).json()
        assert b["ok"] and b["count"] == 10 and b["bars"][-1]["date"] <= "2026-04-08"
        s = c.get("/api/agents/chart/setup",
                  params={"symbol": FW_CASE[0], "pattern": FW_CASE[1], "date": FW_CASE[2]}).json()
        assert s["ok"] and s["geometry"]["upper"] and s["geometry"]["lower"]
    print("PASS test_endpoints_guarded_no_500")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for fn in fns:
        try:
            fn()
            passed += 1
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed + failed} passed")
    sys.exit(1 if failed else 0)
