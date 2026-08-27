"""
Chart Agent · WAVE A pattern tests — triangles / wedges / rectangle / channel + short mirror.

Runnable:
    pytest backend/agents/chart/tests/test_patterns_wave_a.py
    python  backend/agents/chart/tests/test_patterns_wave_a.py

Synthetic frames are built to sit inside the geometry engine's governed windows (level_window/L,
apex_max_frac, flat_eps) so each fires a clean BREAKOUT. DB-dependent PIT tests skip if the R&D DB
is absent. Point-in-time: a future bar appended AFTER as_of must never change the as_of screen.
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
from agents.chart import strategy as strat                   # noqa: E402
from agents.chart import evidence as ev                      # noqa: E402
from agents.chart.patterns import registry as patterns       # noqa: E402
from agents.chart.patterns import _geometry as G             # noqa: E402

registry.load_builtin()

N = 145
I0 = 24
FLAT = G.GEOM_PARAMS["flat_eps"]


def _seg(p0, slope):
    return lambda i: p0 + slope * (i - I0)


def _make(upper, lower, breakout, n=N, amp=0.2, dcap=3, step=10):
    """Synthetic OHLCV whose pivot highs ride ``upper`` and pivot lows ride ``lower``, ending in a
    clean volume BREAKOUT ('up' beyond the upper line, 'down' beyond the lower line)."""
    hp = list(range(24, n - 6, step))
    lp = list(range(26, n - 6, step))
    hi = np.zeros(n); lo = np.zeros(n); cl = np.zeros(n); op = np.zeros(n); vol = np.ones(n)
    for i in range(n):
        u, l = upper(i), lower(i)
        mid = (u + l) / 2
        dh = min(min(abs(i - p) for p in hp), dcap)
        dl = min(min(abs(i - p) for p in lp), dcap)
        hi[i] = u - amp * dh
        lo[i] = l + amp * dl
        cl[i] = mid; op[i] = mid
    k = n - 1
    uk, lk = upper(k), lower(k)
    if breakout == "up":
        cl[k - 1] = (upper(k - 1) + lower(k - 1)) / 2
        cl[k] = uk * 1.02; hi[k] = uk * 1.03; lo[k] = uk * 0.999; op[k] = uk
    else:
        cl[k - 1] = (upper(k - 1) + lower(k - 1)) / 2
        cl[k] = lk * 0.98; lo[k] = lk * 0.97; hi[k] = lk * 1.001; op[k] = lk
    vol[k] = 5.0
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    df = pd.DataFrame({"open": op, "high": hi, "low": lo, "close": cl, "volume": vol}, index=idx)
    df.attrs["symbol"] = "SYNTH"
    return df


# validated line geometry (apex far enough that progress<=apex_max_frac; slopes past flat_eps)
CFG = {
    "ascending_triangle":  (_seg(100, 0.0),    _seg(86.4, 0.08),  "up",   "long"),
    "descending_triangle": (_seg(113.6, -0.08), _seg(100, 0.0),   "down", "short"),
    "symmetrical_triangle": (_seg(113.6, -0.08), _seg(86.4, 0.08), "up",  "long"),
    "rising_wedge":        (_seg(90, 0.08),    _seg(86, 0.10),    "down", "short"),
    "falling_wedge":       (_seg(114, -0.10),  _seg(110, -0.08),  "up",   "long"),
    "rectangle":           (_seg(100, 0.0),    _seg(90, 0.0),     "up",   "long"),
    "channel":             (_seg(100, 0.08),   _seg(90, 0.08),    "up",   "long"),
}

# expected slope-sign of (upper, lower) relative to flat_eps
SIGNS = {
    "ascending_triangle":  ("flat", "up"),
    "descending_triangle": ("down", "flat"),
    "symmetrical_triangle": ("down", "up"),
    "rising_wedge":        ("up", "up"),
    "falling_wedge":       ("down", "down"),
    "rectangle":           ("flat", "flat"),
    "channel":             ("up", "up"),
}


def _sign(sp):
    return "up" if sp > FLAT else "down" if sp < -FLAT else "flat"


def _skip(name):
    if not data.db_available():
        print(f"SKIP {name} — DB absent")
        return True
    return False


# ---- per-pattern synthetic detect -----------------------------------------------------------------
def test_each_pattern_synthetic():
    for pid, (u, l, brk, exp_dir) in CFG.items():
        df = _make(u, l, brk)
        occ = patterns.get(pid).detect(df, as_of_idx=N - 1)
        assert occ, f"{pid}: no occurrence"
        o = occ[0]
        assert o.pattern == pid, (pid, o.pattern)
        assert o.stage == "BREAKOUT", (pid, o.stage)
        assert o.direction == exp_dir, (pid, o.direction)
        assert o.entry_idx == o.signal_idx + 1 == N, (pid, o.entry_idx, o.signal_idx)
        # geometry slope signs match the taxonomy
        us, ls = o.geometry["upper_slope"], o.geometry["lower_slope"]
        assert (_sign(us), _sign(ls)) == SIGNS[pid], (pid, _sign(us), _sign(ls))
        # level ~= the broken line's value at the signal bar
        line_val = (u(N - 1) if brk == "up" else l(N - 1))
        assert abs(o.level - line_val) / line_val < 0.005, (pid, o.level, line_val)
    return "7/7 patterns fire clean BREAKOUTs with correct direction/geometry"


# ---- taxonomy: each frame maps to exactly one label -----------------------------------------------
def test_taxonomy_exactly_one_label():
    labels = {}
    for pid, (u, l, brk, _d) in CFG.items():
        df = _make(u, l, brk)
        fit = G._live_context(df, N - 1, G.GEOM_PARAMS)["fit"]
        labels[pid] = fit.label
        assert fit.label == pid, (pid, fit.label)
    # the three ambiguity-resolving cases are genuinely distinct labels
    assert labels["rectangle"] == "rectangle"
    assert labels["channel"] == "channel"
    assert labels["symmetrical_triangle"] == "symmetrical_triangle"
    assert len(set(labels.values())) == 7
    return "each synthetic maps to exactly one taxonomy label"


# ---- flat_eps boundary flips ascending <-> symmetrical --------------------------------------------
def test_flat_eps_boundary_flip():
    lower = _seg(86, 0.055)                 # fixed rising bottom
    # upper slope just INSIDE the flat band -> flat top -> ascending_triangle
    df_a = _make(_seg(106, -0.045), lower, "up")
    lab_a = G._live_context(df_a, N - 1, G.GEOM_PARAMS)["fit"].label
    # upper slope just PAST -flat_eps (down) -> symmetrical_triangle
    df_s = _make(_seg(106, -0.060), lower, "up")
    lab_s = G._live_context(df_s, N - 1, G.GEOM_PARAMS)["fit"].label
    assert lab_a == "ascending_triangle", lab_a
    assert lab_s == "symmetrical_triangle", lab_s
    return f"upper slope crossing flat_eps flips {lab_a} -> {lab_s}"


# ---- apex/expiry: too far toward the apex -> no occurrence -----------------------------------------
def test_apex_expiry_blocks():
    # ascending whose lower meets the flat top at apex~170 -> progress (144-24)/(170-24)=0.82 > 0.75
    upper = _seg(100, 0.0)
    lower = _seg(88.32, 0.08)               # reaches 100 at i~170
    df = _make(upper, lower, "up")
    fit = G._live_context(df, N - 1, G.GEOM_PARAMS)["fit"]
    assert fit.label == "ascending_triangle", fit.label      # taxonomy still matches
    assert not G._apex_ok(fit, N - 1, G.GEOM_PARAMS), "should be past apex_max_frac"
    occ = patterns.get("ascending_triangle").detect(df, as_of_idx=N - 1)
    assert occ == [], "expired pattern must yield NO occurrence (honest)"
    return "past apex_max_frac -> no occurrence"


# ---- PIT: a future bar appended after as_of never changes the as_of screen -------------------------
def _with_future(df, k, up=True):
    a = df.iloc[:k + 1]
    o, h, l, c, v = (a[x].to_numpy(float) for x in ["open", "high", "low", "close", "volume"])
    last = c[-1]
    ns = (1.5, 1.8, 1.4, 1.7) if up else (0.6, 0.65, 0.4, 0.55)
    o2, h2, l2, c2 = (np.append(arr, last * m) for arr, m in zip((o, h, l, c), ns))
    v2 = np.append(v, v[-1] * 5)
    idx = pd.date_range("2020-01-01", periods=len(o2), freq="B")
    d2 = pd.DataFrame({"open": o2, "high": h2, "low": l2, "close": c2, "volume": v2}, index=idx)
    d2.attrs["symbol"] = "SYNTH"
    return d2


def test_future_bar_ignored_synthetic():
    for pid, (u, l, brk, _d) in CFG.items():
        df = _make(u, l, brk)
        base = patterns.get(pid).detect(df, as_of_idx=N - 1)[0]
        for up in (True, False):
            ext = _with_future(df, N - 1, up=up)
            occ2 = patterns.get(pid).detect(ext, as_of_idx=N - 1)   # same k, future bar present
            assert occ2, (pid, "occurrence vanished with a future bar")
            o2 = occ2[0]
            assert o2.stage == base.stage and o2.direction == base.direction
            assert abs(o2.level - base.level) < 1e-9, (pid, o2.level, base.level)
    return "future bars never alter the as_of screen (all 7)"


# ---- PIT on REAL data: windowed == full-history detect sliced at k (both directions) --------------
def test_real_windowed_equals_full():
    if _skip("test_real_windowed_equals_full"):
        return "SKIP"
    from agents.chart import screener as scr
    D = "2022-08-30"; as_of = pd.Timestamp(D)
    sloped = [s for s in scr.scan_universe_detailed(D)["setups"]
              if s["pattern"] != "horizontal_trendline"]
    assert sloped, "no sloped setups to check"
    checked = mism = leak = 0
    for s in sloped[:20]:
        det = patterns.get(s["pattern"])
        full = data.load_daily(s["symbol"]).copy()
        full.attrs["symbol"] = s["symbol"]
        if as_of not in full.index:
            continue
        k = int(full.index.get_loc(as_of))
        occ = det.detect(full, as_of_idx=k)
        checked += 1
        if not occ or occ[0].stage != s["stage"] or occ[0].direction != s["direction"] \
                or abs(float(occ[0].level) - float(s["level"])) > 1e-6:
            mism += 1
            continue
        ext = _with_future(full, k, up=(s["direction"] == "short"))   # adverse future spike
        occ2 = det.detect(ext, as_of_idx=k)
        if not occ2 or abs(float(occ2[0].level) - float(occ[0].level)) > 1e-6 \
                or occ2[0].stage != occ[0].stage:
            leak += 1
    assert mism == 0, f"{mism} windowed!=full mismatches"
    assert leak == 0, f"{leak} future-bar leaks"
    return f"{checked} sloped setups: windowed==full, 0 leaks (both directions)"


# ---- historical_events resolved-only (entry+9 <= as_of) -------------------------------------------
def test_historical_events_resolved_only():
    if _skip("test_historical_events_resolved_only"):
        return "SKIP"
    from agents.chart import screener as scr
    D = "2022-08-30"; as_of = pd.Timestamp(D)
    sloped = [s for s in scr.scan_universe_detailed(D)["setups"]
              if s["pattern"] != "horizontal_trendline"]
    checked = 0
    for s in sloped[:15]:
        det = patterns.get(s["pattern"])
        df = data.load_daily(s["symbol"])
        if as_of not in df.index:
            continue
        k = int(df.index.get_loc(as_of))
        evs = det.historical_events(df, as_of_idx=k, direction=s["direction"])
        for e in evs:
            assert e.entry_idx + 9 <= k, (s["symbol"], e.entry_idx, k)   # leak-proof as-of
            assert e.direction == s["direction"]
        checked += 1
    return f"historical_events resolved-only across {checked} sloped symbols"


# ---- SHORT replay: all five exit reasons fire -----------------------------------------------------
def _df(rows):
    idx = pd.date_range("2020-01-01", periods=len(rows), freq="B")
    a = np.array(rows, float)
    return pd.DataFrame({"open": a[:, 0], "high": a[:, 1], "low": a[:, 2], "close": a[:, 3],
                         "volume": 1.0}, index=idx)


def test_short_replay_exit_reasons():
    P = strat.StrategyPolicy
    S = {"entry_idx": 0, "direction": "short"}
    # STOP: 5% stop above -> 105 (level far above so no invalidation; trail off)
    df = _df([(100, 106, 99, 100), (100, 101, 99, 100), (100, 101, 99, 100)])
    r = strat.replay_one(df, {**S, "level": 200}, P(max_hold=3, trail_pct=0.99, stop_pct=0.05))
    assert r["exit_reason"] == "STOP" and abs(r["exit_px"] - 105) < 1e-9, r
    # TARGET: 5% target below -> 95
    df = _df([(100, 101, 94, 98), (98, 99, 97, 98), (98, 99, 97, 98)])
    r = strat.replay_one(df, {**S, "level": 200}, P(max_hold=3, trail_pct=0.99, target_pct=0.05))
    assert r["exit_reason"] == "TARGET" and abs(r["exit_px"] - 95) < 1e-9, r
    # TRAIL: 5% off the running trough of 92 -> 96.6
    df = _df([(100, 100, 98, 99), (99, 99, 92, 93), (93, 94, 92, 93)])
    r = strat.replay_one(df, {**S, "level": 200}, P(max_hold=3, trail_pct=0.05))
    assert r["exit_reason"] == "TRAIL" and abs(r["exit_px"] - 96.6) < 1e-9, r
    # INVALIDATION: close back above level*(1+buffer); trail disabled
    df = _df([(100, 101, 99, 100), (100, 102, 100, 101), (101, 102, 100, 101)])
    r = strat.replay_one(df, {**S, "level": 100}, P(max_hold=3, trail_pct=0.99))
    assert r["exit_reason"] == "INVALIDATION" and abs(r["exit_px"] - 101) < 1e-9, r
    # HORIZON: nothing triggers -> close[entry+H-1]
    df = _df([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100)])
    r = strat.replay_one(df, {**S, "level": 200}, P(max_hold=3, trail_pct=0.99))
    assert r["exit_reason"] == "HORIZON" and abs(r["exit_px"] - 100) < 1e-9, r
    return "short STOP/TARGET/TRAIL/INVALIDATION/HORIZON all fire"


# ---- SHORT pattern-vs-strategy separation ---------------------------------------------------------
def test_short_pattern_vs_strategy_separation():
    from types import SimpleNamespace
    rows = [(100, 101, 99, 100),          # 0 signal
            (100, 100, 100, 100),         # 1 entry short @100
            (100, 102, 100, 101)]         # 2 close 101 > 100.2 -> INVALIDATION (short exits at a loss)
    rows += [(100 - 4 * i, 101 - 4 * i, 99 - 4 * i, 100 - 4 * i) for i in range(1, 9)]  # 3..10 crash down
    rows += [(60, 61, 59, 60)] * 4
    df = _df(rows)
    occ = {"entry_idx": 1, "signal_idx": 0, "level": 100.0, "direction": "short"}
    r = strat.replay_one(df, occ)                       # default policy, short branch
    assert r["exit_reason"] == "INVALIDATION", r
    strat_ret = r["strategy_return"] * 100
    pf = ev.pattern_evidence(df, [SimpleNamespace(entry_idx=1)], max_h=10, direction="short")
    pat_t10 = pf["horizons"][10]["mean"]
    assert strat_ret < 0 < pat_t10, (strat_ret, pat_t10)     # opposite signs -> genuinely separate
    assert abs(pat_t10 - strat_ret) > 20, (pat_t10, strat_ret)
    # both families retained side by side
    se = strat.strategy_evidence(df, [occ], as_of_idx=len(df) - 1)
    assert se["n"] == 1 and se["exits"].get("INVALIDATION") == 1
    return round(strat_ret, 2), round(pat_t10, 2)


# ---- direction-aware baseline sign ---------------------------------------------------------------
def test_direction_aware_baseline_sign():
    # a steadily RISING series: long baseline ETV > 0, short baseline ETV < 0
    n = 60
    base = 100 * (1.01 ** np.arange(n))
    df = pd.DataFrame({"open": base, "high": base * 1.001, "low": base * 0.999,
                       "close": base * 1.001, "volume": 1.0},
                      index=pd.date_range("2020-01-01", periods=n, freq="B"))
    pol = strat.DEFAULT_POLICY
    long_etv = strat.strategy_baseline_etv(df, pol, direction="long")
    short_etv = strat.strategy_baseline_etv(df, pol, direction="short")
    assert long_etv is not None and short_etv is not None
    assert long_etv > 0 > short_etv, (long_etv, short_etv)
    # pattern-forward forward-return sign flips with direction too
    from types import SimpleNamespace
    ev_long = ev.pattern_evidence(df, [SimpleNamespace(entry_idx=1)], max_h=10, direction="long")
    ev_short = ev.pattern_evidence(df, [SimpleNamespace(entry_idx=1)], max_h=10, direction="short")
    assert ev_long["horizons"][10]["mean"] > 0 > ev_short["horizons"][10]["mean"]
    return f"rising series: long baseline {long_etv:.2f}% > 0 > short {short_etv:.2f}%"


if __name__ == "__main__":
    fns = [test_each_pattern_synthetic, test_taxonomy_exactly_one_label, test_flat_eps_boundary_flip,
           test_apex_expiry_blocks, test_future_bar_ignored_synthetic, test_real_windowed_equals_full,
           test_historical_events_resolved_only, test_short_replay_exit_reasons,
           test_short_pattern_vs_strategy_separation, test_direction_aware_baseline_sign]
    results = []
    for fn in fns:
        try:
            results.append((fn.__name__, "PASS", fn()))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== WAVE A pattern tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
