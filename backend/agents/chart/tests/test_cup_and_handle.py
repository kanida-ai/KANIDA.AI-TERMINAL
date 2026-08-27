"""
Chart Agent · WAVE B — Cup & Handle tests.

Runnable:
    pytest backend/agents/chart/tests/test_cup_and_handle.py
    python  backend/agents/chart/tests/test_cup_and_handle.py

Synthetic frames build a deterministic rounded cup + handle inside the detector's governed windows.
DB-dependent PIT tests skip if the R&D DB is absent. Point-in-time: a future bar appended AFTER
as_of must never change the as_of screen.
"""
from __future__ import annotations
import os
import sys

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import numpy as np                                            # noqa: E402
import pandas as pd                                           # noqa: E402
from agents import registry                                  # noqa: E402
from agents.chart import data                                # noqa: E402
from agents.chart.patterns import registry as patterns       # noqa: E402
from agents.chart.patterns._geometry import pivot_highs       # noqa: E402, F401

registry.load_builtin()

N, LR, RR, RIM = 141, 25, 120, 100.0
DET = patterns.get("cup_and_handle")


def _build(depth=15.0, hbars=15, hdepth=4.0, shape="U", handle="dip", breakout=True):
    """Deterministic synthetic: lead-in -> rounded cup [LR,RR] -> handle -> (optional) rim breakout."""
    cl = np.zeros(N)
    for i in range(0, LR + 1):
        cl[i] = RIM - 10 + 10 * (i / LR)
    for i in range(LR, RR + 1):
        t = (i - LR) / (RR - LR)
        if shape == "U":
            cl[i] = RIM - 4 * depth * t * (1 - t)                  # parabola, min at middle
        elif shape == "flatV":
            cl[i] = RIM if abs(t - 0.5) > 0.06 else RIM - depth * (1 - abs(t - 0.5) / 0.06)
        elif shape == "V":
            cl[i] = RIM - depth * (1 - abs(2 * t - 1))             # symmetric linear V (tent)
        elif shape == "stair":
            frac = min(t, 1 - t) * 2                               # 0 at ends -> 1 at centre
            cl[i] = RIM - depth * (round(frac * 4) / 4)            # blocky symmetric staircase
    he = RR + hbars
    for j in range(1, hbars + 1):
        i = RR + j; s = j / hbars
        if handle == "dip":
            cl[i] = RIM - hdepth * (1 - abs(2 * s - 1))
        elif handle == "flat":
            cl[i] = RIM
        elif handle == "straight":
            cl[i] = RIM + 8 * s
    k = he + 1
    for i in range(k, N):
        cl[i] = cl[k - 1]
    hi = cl + 0.05; lo = cl - 0.05; op = cl.copy(); vol = np.ones(N)
    if breakout:
        cl[k] = RIM * 1.02; hi[k] = RIM * 1.03; lo[k] = RIM * 0.999; op[k] = RIM; vol[k] = 5.0
    idx = pd.date_range("2020-01-01", periods=N, freq="B")
    df = pd.DataFrame({"open": op, "high": hi, "low": lo, "close": cl, "volume": vol}, index=idx)
    df.attrs["symbol"] = "SYN"
    return df, k


def _skip(name):
    if not data.db_available():
        print(f"SKIP {name} — DB absent")
        return True
    return False


# ---- registration ---------------------------------------------------------------------------------
def test_registered_built():
    ids = {p.pattern_id for p in patterns.all_patterns()}
    assert "cup_and_handle" in ids
    agent = registry.get("chart-v1")
    pats = {p["pattern_id"]: p["status"] for p in agent.manifest.patterns}
    assert pats.get("cup_and_handle") == "built"
    return "cup_and_handle registered + advertised built"


# ---- positive synthetic ---------------------------------------------------------------------------
def test_cup_synthetic_breakout():
    df, k = _build()
    occ = DET.detect(df, as_of_idx=k)
    assert occ, "no cup occurrence"
    o = occ[0]
    assert o.pattern == "cup_and_handle" and o.stage == "BREAKOUT", (o.pattern, o.stage)
    assert o.direction == "long"
    assert o.entry_idx == o.signal_idx + 1 == k + 1
    assert abs(o.level - RIM * 1.0005) / RIM < 0.01, o.level               # level ~= rim (lower rim)
    g = o.geometry
    assert g["left_rim"] == LR and g["right_rim"] == RR
    assert g["curvature"] > 0 and g["r2"] >= 0.6                           # rounded base
    assert 8.0 <= g["cup_depth_pct"] <= 50.0
    assert g["handle_bars"] >= 1 and g["handle_depth_pct"] > 0             # handle recorded
    return f"BREAKOUT long @ {o.level}; depth {g['cup_depth_pct']}% handle {g['handle_depth_pct']}%/{g['handle_bars']}b r2={g['r2']}"


# ---- negatives (each must be rejected) ------------------------------------------------------------
def test_cup_negatives_rejected():
    # sharp V bottom -> low R^2 -> rejected
    df, k = _build(shape="flatV")
    assert DET.detect(df, as_of_idx=k) == [], "sharp V should be rejected"
    # no handle (flat drift, no pullback) -> rejected
    df, k = _build(handle="flat")
    assert DET.detect(df, as_of_idx=k) == [], "flat no-handle should be rejected"
    # no handle (price runs straight up through the rim) -> rejected
    df, k = _build(handle="straight")
    assert DET.detect(df, as_of_idx=k) == [], "straight-up no-handle should be rejected"
    # too deep (>50%) -> rejected
    df, k = _build(depth=60.0)
    assert DET.detect(df, as_of_idx=k) == [], "too-deep cup should be rejected"
    # too shallow (<8%) -> rejected
    df, k = _build(depth=4.0, hdepth=1.0)
    assert DET.detect(df, as_of_idx=k) == [], "too-shallow cup should be rejected"
    return "V / no-handle(x2) / too-deep / too-shallow all rejected"


def test_tent_discriminator_rejects_v_and_staircase():
    """Auditor Item 3: a symmetric linear-V (R^2~0.94) and a blocky staircase (R^2~0.89) pass the
    bare cup_r2_min floor but are NOT cups. The parabola-vs-tent ratio gate must reject them, while
    the true-U parabola control still fires a clean BREAKOUT."""
    # symmetric linear-V, ~15% deep -> REJECTED
    df, k = _build(shape="V", depth=15.0)
    assert DET.detect(df, as_of_idx=k) == [], "symmetric linear-V (15%) must be rejected by tent gate"
    # symmetric linear-V, ~30% deep -> REJECTED
    df, k = _build(shape="V", depth=30.0)
    assert DET.detect(df, as_of_idx=k) == [], "symmetric linear-V (30%) must be rejected by tent gate"
    # blocky staircase-tent, ~15% deep -> REJECTED
    df, k = _build(shape="stair", depth=15.0)
    assert DET.detect(df, as_of_idx=k) == [], "blocky staircase (15%) must be rejected by tent gate"
    # true-U parabola control -> still a clean BREAKOUT
    df, k = _build(shape="U", depth=15.0)
    occ = DET.detect(df, as_of_idx=k)
    assert occ and occ[0].stage == "BREAKOUT" and occ[0].direction == "long"
    assert occ[0].entry_idx == occ[0].signal_idx + 1
    assert abs(occ[0].level - RIM * 1.0005) / RIM < 0.01
    g = occ[0].geometry
    assert g["tent_ratio"] < 0.5, g["tent_ratio"]           # parabola beats the tent badly
    return f"V(15/30%) + staircase rejected; true-U BREAKOUT (tent_ratio={g['tent_ratio']})"


# ---- PIT: future bar after as_of never changes the as_of screen -----------------------------------
def _with_future(df, k, up=True):
    a = df.iloc[:k + 1]
    o, h, l, c, v = (a[x].to_numpy(float) for x in ["open", "high", "low", "close", "volume"])
    last = c[-1]
    ns = (1.5, 1.8, 1.4, 1.7) if up else (0.6, 0.65, 0.4, 0.55)
    o2, h2, l2, c2 = (np.append(arr, last * m) for arr, m in zip((o, h, l, c), ns))
    v2 = np.append(v, v[-1] * 5)
    d2 = pd.DataFrame({"open": o2, "high": h2, "low": l2, "close": c2, "volume": v2},
                      index=pd.date_range("2000-01-01", periods=len(o2), freq="B"))
    d2.attrs["symbol"] = "SYN"
    return d2


def test_cup_future_bar_ignored():
    df, k = _build()
    base = DET.detect(df, as_of_idx=k)[0]
    for up in (True, False):
        occ2 = DET.detect(_with_future(df, k, up=up), as_of_idx=k)
        assert occ2, "occurrence vanished with a future bar"
        assert occ2[0].stage == base.stage and abs(occ2[0].level - base.level) < 1e-9
    return "future bar above/below the rim never alters the as_of cup screen"


# ---- PIT on REAL data: windowed == full-history detect at k --------------------------------------
def test_cup_real_windowed_equals_full():
    if _skip("test_cup_real_windowed_equals_full"):
        return "SKIP"
    from agents.chart import screener as scr
    D = "2022-08-30"; as_of = pd.Timestamp(D)
    cups = [s for s in scr.scan_universe_detailed(D)["setups"] if s["pattern"] == "cup_and_handle"]
    if not cups:
        print("note: 0 cups on the sample date — nothing to sweep")
        return "0 cups on sample date (honest)"
    mism = leak = 0
    for s in cups:
        full = data.load_daily(s["symbol"]).copy(); full.attrs["symbol"] = s["symbol"]
        if as_of not in full.index:
            continue
        k = int(full.index.get_loc(as_of))
        occ = DET.detect(full, as_of_idx=k)
        if not occ or occ[0].stage != s["stage"] or abs(float(occ[0].level) - float(s["level"])) > 1e-6:
            mism += 1
            continue
        occ2 = DET.detect(_with_future(full, k, up=True), as_of_idx=k)   # future spike above rim
        if not occ2 or abs(float(occ2[0].level) - float(occ[0].level)) > 1e-6 or occ2[0].stage != occ[0].stage:
            leak += 1
    assert mism == 0 and leak == 0, (mism, leak)
    return f"{len(cups)} real cups: windowed==full, 0 future-leaks"


# ---- historical_events resolved-only (entry+9 <= as_of) -------------------------------------------
def test_cup_historical_events_resolved_only():
    if _skip("test_cup_historical_events_resolved_only"):
        return "SKIP"
    from agents.chart import screener as scr
    D = "2022-08-30"; as_of = pd.Timestamp(D)
    cups = [s for s in scr.scan_universe_detailed(D)["setups"] if s["pattern"] == "cup_and_handle"]
    checked = 0
    for s in cups[:8]:
        df = data.load_daily(s["symbol"])
        if as_of not in df.index:
            continue
        k = int(df.index.get_loc(as_of))
        for e in DET.historical_events(df, as_of_idx=k):
            assert e.entry_idx + 9 <= k, (s["symbol"], e.entry_idx, k)
            assert e.direction == "long"
        checked += 1
    return f"historical_events resolved-only across {checked} cup symbols"


if __name__ == "__main__":
    fns = [test_registered_built, test_cup_synthetic_breakout, test_cup_negatives_rejected,
           test_tent_discriminator_rejects_v_and_staircase, test_cup_future_bar_ignored,
           test_cup_real_windowed_equals_full, test_cup_historical_events_resolved_only]
    results = []
    for fn in fns:
        try:
            results.append((fn.__name__, "PASS", fn()))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== WAVE B · Cup & Handle tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
