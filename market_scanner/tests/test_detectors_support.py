"""Shared fixtures/invariance helpers for detector tests (no TestCase here).

Run all detector suites from the repository root:
    market_scanner/.venv/Scripts/python.exe -m unittest discover -s market_scanner/tests -p "test_detectors_*.py"
"""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import math
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import numpy as np  # noqa: E402

EVENT_KEYS = {"signal_index", "episode", "state", "atr", "score", "direction", "pattern_start",
              "formation_start_index", "detected_index"}
OPTIONAL_KEYS = {"geometry", "alias_group", "library_value", "quality_tags", "confirmed_index"}


def make_bars(ohlc, timeframe="1D", start="2020-01-01", volume=1000.0, gaps=()):
    """ohlc: iterable of (open, high, low, close[, volume]).  Daily-spaced timestamps."""
    t0 = datetime.fromisoformat(start + " 09:15:00")
    bars = []
    for i, row in enumerate(ohlc):
        o, h, l, c = (float(x) for x in row[:4])
        vol = float(row[4]) if len(row) > 4 else volume
        t = t0 + timedelta(days=i)
        e = t.replace(hour=15, minute=30)
        bars.append({"time": t.isoformat(sep=" "), "end": e.isoformat(sep=" "), "open": o, "high": h,
                     "low": l, "close": c, "volume": vol, "gap": i in set(gaps)})
    return bars


def from_closes(closes, spread=0.6, seed=0, **kw):
    """Plausible OHLC around a close path (open = previous close + noise)."""
    rng = np.random.default_rng(seed)
    rows, prev = [], float(closes[0])
    for c in closes:
        c = float(c)
        o = prev + rng.normal(0, spread * 0.2)
        hi = max(o, c) + abs(rng.normal(0, spread * 0.5))
        lo = min(o, c) - abs(rng.normal(0, spread * 0.5))
        rows.append((o, hi, lo, c))
        prev = c
    return make_bars(rows, **kw)


def random_walk(n=600, seed=1, start=100.0, vol=1.0, **kw):
    rng = np.random.default_rng(seed)
    closes = np.maximum(start + np.cumsum(rng.normal(0, vol, n)), 5.0)
    return from_closes(closes, spread=vol, seed=seed + 1, **kw)


def flatten(result):
    return {k: list(v) for k, v in result.items()}


def assert_contract(tc, module, bars, timeframe="1D"):
    """Keys match specs, events well formed, sorted, JSON safe, no duplicates."""
    specs = module.specifications()
    keys = [(d["pattern_id"], d["variant"], d["side"]) for d in specs]
    tc.assertEqual(len(keys), len(set(keys)), "duplicate specification keys")
    by_key = {k: d for k, d in zip(keys, specs)}
    result = module.detect(bars, timeframe)
    json.dumps({"|".join(k): v for k, v in result.items()}, allow_nan=False)
    tc.assertTrue(set(result) <= set(keys), f"unknown keys {set(result) - set(keys)}")
    for key, events in result.items():
        seen = set()
        last = -1
        for e in events:
            tc.assertTrue(EVENT_KEYS <= set(e), f"{key} missing {EVENT_KEYS - set(e)}")
            tc.assertTrue(set(e) <= EVENT_KEYS | OPTIONAL_KEYS, f"{key} extra {set(e) - EVENT_KEYS - OPTIONAL_KEYS}")
            tc.assertIn(e["state"], by_key[key]["states"], key)
            tc.assertGreaterEqual(e["signal_index"], last, f"{key} not sorted")
            last = e["signal_index"]
            tc.assertLessEqual(e["formation_start_index"], e["detected_index"])
            tc.assertLessEqual(e["detected_index"], e["signal_index"])
            tc.assertLess(e["signal_index"], len(bars))
            tc.assertTrue(math.isfinite(e["atr"]) and e["atr"] > 0)
            tc.assertTrue(math.isfinite(e["score"]))
            tc.assertEqual(e["pattern_start"], bars[e["formation_start_index"]]["time"])
            marker = (e["episode"], e["state"])
            tc.assertNotIn(marker, seen, f"{key} repeated episode/state {marker}")
            seen.add(marker)
            if e["state"] == "confirmed" and "confirmed_index" in e:
                tc.assertEqual(e["confirmed_index"], e["signal_index"])
    return result


def assert_causal(tc, module, bars, timeframe="1D", cuts=None, seed=7):
    """Prefix truncation and future perturbation never change earlier events."""
    full = module.detect(bars, timeframe)
    n = len(bars)
    cuts = cuts or sorted({n // 3, n // 2, (2 * n) // 3, n - 5})
    rng = np.random.default_rng(seed)
    for cut in cuts:
        def upto(res):
            return {k: [e for e in v if e["signal_index"] < cut] for k, v in res.items()}
        prefix = module.detect(bars[:cut], timeframe)
        tc.assertEqual({k: v for k, v in upto(full).items() if v}, {k: v for k, v in prefix.items() if v},
                       f"prefix truncation at {cut} changed events")
        perturbed = [dict(b) for b in bars]
        for b in perturbed[cut:]:
            scale = float(rng.uniform(0.6, 1.6))
            o, c = b["open"] * scale, b["close"] * float(rng.uniform(0.6, 1.6))
            b.update(open=o, close=c, high=max(o, c) * 1.03, low=min(o, c) * 0.97,
                     volume=b["volume"] * float(rng.uniform(0.1, 5)))
        other = module.detect(perturbed, timeframe)
        tc.assertEqual({k: v for k, v in upto(full).items() if v}, {k: v for k, v in upto(other).items() if v},
                       f"future perturbation after {cut} changed events")


def events_for(result, pattern_id, variant=None, side=None, state=None):
    out = []
    for (pid, var, sd), events in result.items():
        if pid == pattern_id and (variant is None or var == variant) and (side is None or sd == side):
            out.extend(e for e in events if state is None or e["state"] == state)
    return out
