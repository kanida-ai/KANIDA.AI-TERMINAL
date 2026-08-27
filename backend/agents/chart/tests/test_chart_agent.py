"""
Chart Agent smoke tests.

Runnable two ways:
    pytest backend/agents/chart/tests/
    python  backend/agents/chart/tests/test_chart_agent.py   (prints a pass/fail summary)

Covers:
  (a) the package imports and chart-v1 registers with its pattern library;
  (b) the ported horizontal detector flags the known TITAN 2022-08-30 breakout at level ~= 2565
      (skips gracefully if the R&D DB is absent);
  (c) decide() returns a well-formed TRADE/WATCH/NO_TRADE dict.
"""
from __future__ import annotations
import os
import sys

# Put backend/ on the path so `agents.*` imports resolve (mirrors how main.py mounts the package).
_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from types import SimpleNamespace              # noqa: E402
import pandas as pd                            # noqa: E402
from agents import registry                    # noqa: E402
from agents.chart import data                  # noqa: E402
from agents.chart import strategy as strat     # noqa: E402
from agents.chart import evidence as ev        # noqa: E402
from agents.chart.patterns import registry as patterns  # noqa: E402
from agents.chart.patterns.horizontal_trendline import HorizontalTrendlineDetector  # noqa: E402

TITAN_DATE = "2022-08-30"
TITAN_LEVEL = 2565.0


def test_package_imports_and_registers():
    registry.load_builtin()
    agent = registry.get("chart-v1")
    assert agent is not None, "chart-v1 did not register"
    ids = {p.pattern_id for p in patterns.all_patterns()}
    assert "horizontal_trendline" in ids
    assert "triangle" in ids and "channel" in ids
    # manifest advertises the pattern library
    pats = {p["pattern_id"]: p["status"] for p in agent.manifest.patterns}
    assert pats.get("horizontal_trendline") == "built"
    assert pats.get("triangle") == "spec" and pats.get("channel") == "spec"


def test_titan_breakout_detected():
    if not data.db_available():
        print("SKIP test_titan_breakout_detected — DB absent:", data.db_path())
        return "SKIP"
    df = data.load_daily("TITAN")
    df.attrs["symbol"] = "TITAN"
    import pandas as pd
    ts = pd.Timestamp(TITAN_DATE)
    assert ts in df.index, f"{TITAN_DATE} not in TITAN history"
    k = int(df.index.get_loc(ts))
    occ = HorizontalTrendlineDetector().detect(df, as_of_idx=k)
    assert occ, "no occurrence at TITAN 2022-08-30"
    o = occ[0]
    assert o.stage == "BREAKOUT", f"expected BREAKOUT, got {o.stage}"
    assert abs(o.level - TITAN_LEVEL) / TITAN_LEVEL < 0.01, f"level {o.level} not ~= {TITAN_LEVEL}"
    assert o.entry_idx == o.signal_idx + 1, "entry must be next open (point-in-time)"
    return o.level


def test_decide_returns_valid_dict():
    if not data.db_available():
        print("SKIP test_decide_returns_valid_dict — DB absent")
        return "SKIP"
    registry.load_builtin()
    agent = registry.get("chart-v1")
    df = data.load_daily("TITAN")
    df.attrs["symbol"] = "TITAN"
    k = int(df.index.get_loc(pd.Timestamp(TITAN_DATE)))
    occ = HorizontalTrendlineDetector().detect(df, as_of_idx=k)
    assert occ
    res = agent.decide(occ[0].to_dict())
    assert res["decision"] in ("TRADE", "WATCH", "NO_TRADE"), res["decision"]
    assert isinstance(res["reason"], str) and res["reason"]
    # basis now the strategy-replay family (v3 §8.2)
    assert res.get("basis") == "strategy_replay", res.get("basis")
    # TITAN 2022-08-30 still WATCH at n=6 (G1 unchanged) — the honest §19 verdict
    assert res["decision"] == "WATCH" and "n=6" in res["reason"], (res["decision"], res["reason"])
    # both outcome families present, neither overwriting the other
    assert res["evidence"] is not None and res["evidence"]["summary"]["n"] == 6
    if res["decision"] == "TRADE":
        assert "intent" in res and res["intent"].mode == "paper"
    return res["decision"], res["reason"]


# ---------------------------------------------------------------- strategy-replay (§8.2) tests
def _df(rows):
    """Build a daily OHLC frame from (o,h,l,c) rows with a real DatetimeIndex."""
    import numpy as np
    idx = pd.date_range("2020-01-01", periods=len(rows), freq="B")
    a = np.array(rows, float)
    return pd.DataFrame({"open": a[:, 0], "high": a[:, 1], "low": a[:, 2],
                         "close": a[:, 3], "volume": 1.0}, index=idx)


def test_replay_exit_reasons():
    P = strat.StrategyPolicy
    # STOP: hard 5% stop, trail disabled (0.99), level far below so no invalidation
    df = _df([(100, 101, 96, 100), (100, 100, 94, 98), (98, 99, 97, 98)])
    r = strat.replay_one(df, {"entry_idx": 0, "level": 50}, P(max_hold=3, trail_pct=0.99, stop_pct=0.05))
    assert r["exit_reason"] == "STOP" and abs(r["exit_px"] - 95) < 1e-9, r

    # TARGET: 5% target
    df = _df([(100, 104, 99, 103), (103, 106, 102, 105), (105, 106, 104, 105)])
    r = strat.replay_one(df, {"entry_idx": 0, "level": 50}, P(max_hold=3, trail_pct=0.99, target_pct=0.05))
    assert r["exit_reason"] == "TARGET" and abs(r["exit_px"] - 105) < 1e-9, r

    # TRAIL: 5% trail off the peak of 110 -> 104.5
    df = _df([(100, 110, 108, 109), (109, 110, 104, 105), (105, 106, 103, 104)])
    r = strat.replay_one(df, {"entry_idx": 0, "level": 50}, P(max_hold=3, trail_pct=0.05))
    assert r["exit_reason"] == "TRAIL" and abs(r["exit_px"] - 104.5) < 1e-9, r

    # INVALIDATION: close below level*(1-buffer); trail disabled
    df = _df([(100, 101, 99, 100), (100, 100, 98, 99), (99, 100, 98, 99)])
    r = strat.replay_one(df, {"entry_idx": 0, "level": 100}, P(max_hold=3, trail_pct=0.99))
    assert r["exit_reason"] == "INVALIDATION" and abs(r["exit_px"] - 99) < 1e-9, r

    # HORIZON: nothing triggers -> exit at close[entry+H-1]
    df = _df([(100, 101, 100, 100), (100, 101, 100, 100), (100, 101, 100, 100)])
    r = strat.replay_one(df, {"entry_idx": 0, "level": 50}, P(max_hold=3, trail_pct=0.99))
    assert r["exit_reason"] == "HORIZON" and abs(r["exit_px"] - 100) < 1e-9, r
    return "STOP/TARGET/TRAIL/INVALIDATION/HORIZON all fire"


def test_pattern_vs_strategy_separation():
    """The keystone: a structural stop exits early so strategy_return != pattern T+10, and BOTH
    outcome families are retained (v3 §5/§8 — never conflate/overwrite)."""
    df = _df([
        (99, 100, 98, 99),      # 0 signal bar
        (100, 101, 99.5, 101),  # 1 entry: open=100, holds
        (100, 100, 98, 99),     # 2 close 99 < 99.8 -> INVALIDATION exits strategy early
        (100, 112, 100, 111),   # 3.. strong recovery the strategy no longer participates in
        (111, 116, 110, 115),
        (115, 120, 114, 119),
        (119, 124, 118, 123),
        (123, 127, 122, 126),
        (126, 130, 125, 129),
        (129, 132, 128, 131),
        (131, 133, 130, 130),   # 10 close=130 -> pattern T+10 anchor
        (130, 131, 129, 130), (130, 131, 129, 130), (130, 131, 129, 130), (130, 131, 129, 130),
    ])
    occ = {"entry_idx": 1, "signal_idx": 0, "level": 100.0}
    # strategy outcome
    r = strat.replay_one(df, occ)                       # default policy (H=10, trail 8%)
    assert r["exit_reason"] == "INVALIDATION", r
    strat_ret = r["strategy_return"] * 100
    # pattern-forward outcome (T+10, hold-to-close)
    pf = ev.pattern_evidence(df, [SimpleNamespace(entry_idx=1)], max_h=10)
    pat_t10 = pf["horizons"][10]["mean"]
    assert strat_ret < 0 < pat_t10, (strat_ret, pat_t10)     # opposite signs -> genuinely separate
    assert abs(pat_t10 - strat_ret) > 20, (pat_t10, strat_ret)
    # both retained through strategy_evidence + pattern_evidence side by side
    se = strat.strategy_evidence(df, [occ], as_of_idx=len(df) - 1)
    assert se["n"] == 1 and se["exits"].get("INVALIDATION") == 1
    return round(strat_ret, 2), round(pat_t10, 2)


def test_trade_emits_paper_intent():
    """On TRADE the agent must emit a paper Intent whose thesis cites the STRATEGY stats + policy.
    A natural TRADE needs the SPEC nested populations to lift N, so we inject a synthetic TRADE
    decision and assert the wrapping/citation (req 5) — the gate logic itself is covered elsewhere."""
    if not data.db_available():
        print("SKIP test_trade_emits_paper_intent — DB absent")
        return "SKIP"
    registry.load_builtin()
    agent = registry.get("chart-v1")
    orig = ev.decide
    fake = {"decision": "TRADE", "reason": "synthetic TRADE for intent-wiring test.",
            "gates": [], "basis": "strategy_replay", "evidence_ref_horizon": 3, "etv": 1.5,
            "edge": 0.9, "strategy": {"n": 25, "etv": 1.5, "win": 60.0, "payoff": 1.8},
            "policy": {"version": "S-horiz-v1", "trail_pct": 0.08, "max_hold": 10}}
    try:
        ev.decide = lambda *a, **k: fake
        df = data.load_daily("TITAN")
        df.attrs["symbol"] = "TITAN"
        k = int(df.index.get_loc(pd.Timestamp(TITAN_DATE)))
        occ = HorizontalTrendlineDetector().detect(df, as_of_idx=k)[0].to_dict()
        res = agent.decide(occ)
    finally:
        ev.decide = orig
    assert res["decision"] == "TRADE"
    it = res.get("intent")
    assert it is not None and it.mode == "paper", "TRADE must emit a paper Intent"
    assert "S-horiz-v1" in it.thesis and "Strategy-ETV" in it.thesis, it.thesis
    assert res["strategy"]["n"] == 25 and res["policy"]["version"] == "S-horiz-v1"
    return "paper intent cites strategy + policy"


# ---------------------------------------------------------------- data-source parity (cloud wiring)
def test_parquet_matches_sqlite():
    """The cloud reads daily bars from S3/local Parquet (env AGENT_DATA_URI, the same source the
    Agent Builder uses); local dev reads SQLite. This asserts the two branches are INDISTINGUISHABLE
    — identical date index (both ns) and byte-identical OHLCV+nifty — so wiring the cloud data changes
    the *source* only, never the numbers the detectors/evidence see. Skips if duckdb/pyarrow or the
    R&D DB is absent (both are present in the cloud image)."""
    if not data.db_available():
        print("SKIP test_parquet_matches_sqlite — DB absent")
        return "SKIP"
    try:
        import duckdb  # noqa: F401
        import pyarrow as pa
        import pyarrow.parquet as papq
    except Exception:  # noqa: BLE001
        print("SKIP test_parquet_matches_sqlite — duckdb/pyarrow not installed")
        return "SKIP"
    import sqlite3, tempfile, shutil, numpy as np

    sqlite_path = os.environ.get("AGENT_CHART_DB") or data.DEFAULT_DB
    tmp = tempfile.mkdtemp(prefix="chart_pq_")
    saved = {k: os.environ.get(k) for k in ("AGENT_DATA_URI", "AGENT_CHART_DB")}
    try:
        # build a tiny hive-partitioned parquet (symbol=<sym>/data.parquet) matching the cloud schema
        con = sqlite3.connect("file:" + sqlite_path.replace("\\", "/") + "?mode=ro", uri=True)
        try:
            for s in ("TITAN", data.NIFTY):
                d0 = pd.read_sql_query(
                    "SELECT substr(bar_time,1,10) date, open,high,low,close,volume "
                    "FROM ohlc_daily WHERE symbol=? ORDER BY bar_time", con, params=(s,))
                d0["date"] = pd.to_datetime(d0["date"]).dt.date
                part = os.path.join(tmp, "symbol=" + s)
                os.makedirs(part, exist_ok=True)
                papq.write_table(pa.Table.from_pandas(d0, preserve_index=False),
                                 os.path.join(part, "data.parquet"))
        finally:
            con.close()

        def _load(uri, sqlite):
            os.environ["AGENT_DATA_URI"] = uri
            if sqlite:
                os.environ["AGENT_CHART_DB"] = sqlite
            else:
                os.environ.pop("AGENT_CHART_DB", None)
            data.load_daily.cache_clear(); data._nifty_close.cache_clear()
            return data.load_daily("TITAN")

        a = _load(tmp, None)         # Parquet branch
        b = _load("", sqlite_path)   # SQLite branch
        assert a.index.equals(b.index), "date index differs between Parquet and SQLite"
        assert str(a.index.dtype) == "datetime64[ns]" == str(b.index.dtype), (a.index.dtype, b.index.dtype)
        for c in ("open", "high", "low", "close", "volume", "nifty"):
            assert np.array_equal(a[c].values, b[c].values), f"column {c} differs between sources"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        data.load_daily.cache_clear(); data._nifty_close.cache_clear()
        shutil.rmtree(tmp, ignore_errors=True)
    return f"parquet==sqlite over {len(a)} bars"


# ---------------------------------------------------------------- read-only endpoint smoke tests
def test_endpoints_return_valid_json():
    """The 3 portal endpoints (scan/decision/storyline) each return well-formed, honest JSON for
    date=2022-08-30 / symbol=TITAN on the R&D DB. Skips gracefully if the DB is absent (SPEC feeds).
    They are guarded — a failure returns ok=False JSON, never a 500 crash."""
    import json
    from agents import router as R

    if not data.db_available():
        print("SKIP test_endpoints_return_valid_json — DB absent")
        return "SKIP"

    # scan — point-in-time as-of the date; TITAN breakout must be among the occurrences
    s = R.chart_scan(date=TITAN_DATE, limit=40)
    assert json.dumps(s, default=str)              # JSON-serialisable
    assert s["ok"] is True and s["count"] >= 1, s
    syms = {r["stock"] for r in s["occurrences"]}
    assert "TITAN" in syms, syms
    titan = next(r for r in s["occurrences"] if r["stock"] == "TITAN")
    assert titan["stage"] == "BREAKOUT" and abs(titan["level"] - TITAN_LEVEL) / TITAN_LEVEL < 0.01

    # decision — honest WATCH at current N; both families present; basis stamped
    d = R.chart_decision(symbol="TITAN", date=TITAN_DATE)
    assert json.dumps(d, default=str)
    assert d["ok"] is True and d["decision"] == "WATCH" and "n=6" in d["reason"], (d["decision"], d["reason"])
    assert d["basis"] == "strategy_replay"
    assert d["strategy"] and d["strategy"]["n"] == 6
    assert d["pattern_forward"] and "3" in d["pattern_forward"]["horizons"]

    # storyline — ordered events ending in the WATCH decision
    st = R.chart_storyline(symbol="TITAN", date=TITAN_DATE)
    assert json.dumps(st, default=str)
    assert st["ok"] is True and st["decision"] == "WATCH"
    kinds = [e["kind"] for e in st["events"]]
    assert kinds[0] == "level" and kinds[-1] == "decision" and "breakout" in kinds, kinds

    # guard — an unknown symbol returns honest JSON (ok=False), never raises
    bad = R.chart_decision(symbol="NOTREAL", date=TITAN_DATE)
    assert bad["ok"] is False and bad.get("error"), bad
    return f"scan={s['count']} decision={d['decision']} storyline_events={len(st['events'])}"


if __name__ == "__main__":
    results = []
    for fn in (test_package_imports_and_registers, test_titan_breakout_detected,
               test_replay_exit_reasons, test_pattern_vs_strategy_separation,
               test_trade_emits_paper_intent, test_decide_returns_valid_dict,
               test_parquet_matches_sqlite, test_endpoints_return_valid_json):
        try:
            r = fn()
            results.append((fn.__name__, "PASS", r))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e)))
    print("\n=== Chart Agent smoke tests ===")
    for name, status, detail in results:
        print(f"  [{status}] {name}" + (f"  -> {detail}" if detail not in (None,) else ""))
    if any(s == "FAIL" for _, s, _ in results):
        sys.exit(1)
