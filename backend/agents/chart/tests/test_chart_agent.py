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

from agents import registry                    # noqa: E402
from agents.chart import data                  # noqa: E402
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
    import pandas as pd
    k = int(df.index.get_loc(pd.Timestamp(TITAN_DATE)))
    occ = HorizontalTrendlineDetector().detect(df, as_of_idx=k)
    assert occ
    res = agent.decide(occ[0].to_dict())
    assert res["decision"] in ("TRADE", "WATCH", "NO_TRADE"), res["decision"]
    assert isinstance(res["reason"], str) and res["reason"]
    assert res.get("basis") == "pattern_forward"
    if res["decision"] == "TRADE":
        assert "intent" in res and res["intent"].mode == "paper"
    return res["decision"], res["reason"]


if __name__ == "__main__":
    results = []
    for fn in (test_package_imports_and_registers, test_titan_breakout_detected,
               test_decide_returns_valid_dict):
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
