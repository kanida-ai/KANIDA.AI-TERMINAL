import json
import os
import time
import unittest

import numpy as np
import talib

from test_detectors_support import ROOT, make_bars, random_walk, assert_contract, assert_causal, events_for
from market_scanner.pattern_research import candlesticks as cdl
from market_scanner.pattern_research import common


def noisy_bars(n=1500, seed=11, doji_rate=0.12):
    """Random walk with injected dojis/equal opens so rare shapes appear."""
    rng = np.random.default_rng(seed)
    c = np.maximum(100 + np.cumsum(rng.normal(0, 1, n)), 5)
    o = c + rng.normal(0, 0.8, n)
    idx = rng.random(n) < doji_rate
    o[idx] = c[idx]
    h = np.maximum(o, c) + np.abs(rng.normal(0, 0.7, n)) * (rng.random(n) < 0.85)
    l = np.minimum(o, c) - np.abs(rng.normal(0, 0.7, n)) * (rng.random(n) < 0.85)
    return make_bars(list(zip(o, h, l, c)))


def declining(n=40, start=120.0, step=0.5):
    rows = []
    for k in range(n):
        o = start - step * k + 0.25
        c = o - 0.5
        rows.append((o, o + 0.25, c - 0.25, c))
    return rows


class CandleCatalogueTests(unittest.TestCase):
    def test_covers_all_61_catalogue_ids_with_pinned_library(self):
        self.assertEqual(talib.__version__, cdl.EXPECTED_TALIB_VERSION)
        with open(os.path.join(ROOT, "docs", "PATTERN_CATALOGUE_PROPOSAL.json"), encoding="utf-8") as fh:
            catalogue = {e["id"] for e in json.load(fh)["entries"] if e["id"].startswith("CDL")}
        self.assertEqual(len(catalogue), 61)
        self.assertEqual({r[0] for r in cdl.TABLE}, catalogue)
        self.assertEqual(set(talib.get_function_groups()["Pattern Recognition"]), catalogue)
        ids = {d["pattern_id"] for d in cdl.specifications()}
        self.assertEqual(ids, catalogue)

    def test_neutral_shapes_never_have_library_long_cells(self):
        for d in cdl.specifications():
            row = cdl._ROWS[d["pattern_id"]]
            if row[3] in ("neutral", "context", "color"):
                self.assertNotEqual(d["variant"], "canonical", d["pattern_id"])

    def test_contract_and_causality_random(self):
        bars = noisy_bars(900, seed=3)
        assert_contract(self, cdl, bars)
        assert_causal(self, cdl, bars, cuts=[300, 451, 777])

    def test_cross_check_against_library(self):
        bars = noisy_bars(3000, seed=5)
        res = cdl.detect(bars, "1D")
        s = common.prepare(bars, "1D")
        four = (s.o == s.h) & (s.h == s.l) & (s.l == s.c)
        for row in cdl.TABLE:
            fid, kind = row[0], row[3]
            kw = {"penetration": cdl.PENETRATION[fid]} if fid in cdl.PENETRATION else {}
            lib = getattr(talib, fid)(s.o, s.h, s.l, s.c, **kw)
            usable = np.isfinite(s.atr)
            if kind in ("directional", "intrinsic", "hikkake"):
                state = "confirmed" if kind == "intrinsic" else "setup"
                for sign, side in ((1, "long"), (-1, "short")):
                    mag = 100
                    want = {int(i) for i in np.flatnonzero((np.sign(lib) == sign) & usable & (np.abs(lib) <= mag))}
                    got = {e["signal_index"] for e in res.get((fid, "canonical", side), []) if e["state"] == state}
                    if (fid, "canonical", side) in res:
                        self.assertEqual(got, want, f"{fid} {side}")
                        for e in res[(fid, "canonical", side)]:
                            if e["state"] == state:
                                self.assertEqual(e["library_value"], int(lib[e["signal_index"]]))
                    else:
                        self.assertFalse(want, f"{fid} emits unsupported {side}")
                if kind == "hikkake":
                    for sign, side in ((1, "long"), (-1, "short")):
                        want = set()
                        for j in np.flatnonzero(lib == 200 * sign):
                            k = next(x for x in range(j - 1, j - 4, -1) if abs(lib[x]) == 100)
                            if np.isfinite(s.atr[k]):
                                want.add(int(j))
                        got = {e["signal_index"] for e in res[(fid, "canonical", side)] if e["state"] == "confirmed"}
                        self.assertEqual(got, want, f"{fid} confirmations {side}")
            else:
                want = {int(i) for i in np.flatnonzero((lib != 0) & usable & ~four)}
                got = {e["signal_index"] for e in res[(fid, "break_up", "long")] if e["state"] == "setup"}
                self.assertEqual(got, want, fid)

    def test_context_variant_subset_and_trend(self):
        bars = noisy_bars(2000, seed=8)
        res = cdl.detect(bars, "1D")
        s = common.prepare(bars, "1D")
        checked = 0
        for (fid, variant, side), events in res.items():
            if variant != "canonical_context":
                continue
            interp = cdl._ROWS[fid][4]
            base = {(e["signal_index"], e["state"]) for e in res.get((fid, "canonical", side), [])}
            for e in events:
                if base:
                    self.assertIn((e["signal_index"], e["state"]), base)
                if e["state"] == "setup":
                    need = ("down" if side == "long" else "up") if interp == "reversal" else ("up" if side == "long" else "down")
                    self.assertEqual(s.trend(e["formation_start_index"]), need)
                    checked += 1
        self.assertGreater(checked, 50)

    def test_context_shapes_emit_on_their_conventional_side(self):
        bars = noisy_bars(4000, seed=31, doji_rate=0.2)
        res = cdl.detect(bars, "1D")
        s = common.prepare(bars, "1D")
        lib = talib.CDLGRAVESTONEDOJI(s.o, s.h, s.l, s.c)
        self.assertTrue((lib[lib != 0] == 100).all())  # constant recognition code
        for fid, side, need in (("CDLGRAVESTONEDOJI", "short", "up"), ("CDLDRAGONFLYDOJI", "long", "down"),
                                ("CDLTAKURI", "long", "down")):
            ev = [e for e in res[(fid, "canonical_context", side)] if e["state"] == "setup"]
            self.assertTrue(ev, fid)
            for e in ev:
                self.assertEqual(s.trend(e["formation_start_index"]), need)
                self.assertEqual(e["direction"], "bullish" if side == "long" else "bearish")

    def test_bullish_engulfing_fixture_setup_context_and_confirmation(self):
        rows = declining(40)
        prev_c = rows[-1][3]
        rows.append((prev_c - 0.1, prev_c + 0.2, prev_c - 1.6, prev_c - 1.5))   # 40 down candle
        rows.append((prev_c - 1.7, prev_c + 0.6, prev_c - 1.8, prev_c + 0.5))   # 41 engulfing up candle
        rows.append((prev_c + 0.5, prev_c + 2.6, prev_c + 0.4, prev_c + 2.5))   # 42 confirming close
        rows.append((prev_c + 2.5, prev_c + 2.8, prev_c + 2.2, prev_c + 2.6))
        bars = make_bars(rows)
        self.assertEqual(talib.CDLENGULFING(*[np.array([b[k] for b in bars]) for k in ("open", "high", "low", "close")])[41], 100)
        res = cdl.detect(bars, "1D")
        for variant in ("canonical", "canonical_context"):
            ev = res[("CDLENGULFING", variant, "long")]
            self.assertEqual([(e["state"], e["signal_index"], e["episode"]) for e in ev],
                             [("setup", 41, 40), ("confirmed", 42, 40)], variant)
            self.assertEqual(ev[1]["detected_index"], 41)
            self.assertEqual(ev[1]["confirmed_index"], 42)
            self.assertEqual(ev[0]["pattern_start"], bars[40]["time"])
        assert_causal(self, cdl, bars, cuts=[41, 42, 43])
        # Missing-data flag inside the two-candle formation or the confirmation window.
        gapped = [dict(b) for b in bars]
        gapped[41]["gap"] = True
        self.assertFalse(cdl.detect(gapped, "1D")[("CDLENGULFING", "canonical", "long")])
        gapped = [dict(b) for b in bars]
        gapped[42]["gap"] = True
        ev = cdl.detect(gapped, "1D")[("CDLENGULFING", "canonical", "long")]
        self.assertEqual([e["state"] for e in ev], ["setup"])
        # A gap shortly before: segment too short for prior ATR -> no event.
        gapped = [dict(b) for b in bars]
        gapped[35]["gap"] = True
        self.assertFalse(cdl.detect(gapped, "1D")[("CDLENGULFING", "canonical", "long")])

    def test_engulfing_without_decline_fails_context_only(self):
        rows = [(100 + 0.3 * (k % 2), 100.6, 99.4, 100 - 0.3 * (k % 2)) for k in range(40)]
        rows.append((100.2, 100.4, 98.6, 98.7))
        rows.append((98.5, 100.9, 98.4, 100.8))
        rows.append((100.8, 101, 100.5, 100.9))
        res = cdl.detect(make_bars(rows), "1D")
        self.assertEqual([e["signal_index"] for e in res[("CDLENGULFING", "canonical", "long")]], [41])
        self.assertFalse(res[("CDLENGULFING", "canonical_context", "long")])

    def test_adjacent_dojis_are_distinct_episodes_and_four_price_excluded(self):
        rows = [(100 + 0.8 * ((-1) ** k), 101.5, 98.5, 100 - 0.8 * ((-1) ** k)) for k in range(40)]
        rows += [(100, 101, 99, 100), (100, 101.2, 98.9, 100), (100, 100, 100, 100), (100, 101, 99, 100.6)]
        bars = make_bars(rows)
        res = cdl.detect(bars, "1D")
        setups = [e for e in res[("CDLDOJI", "break_up", "long")] if e["state"] == "setup"]
        idx = [e["signal_index"] for e in setups]
        self.assertIn(40, idx)
        self.assertIn(41, idx)
        self.assertNotIn(42, idx)
        self.assertEqual(len({e["episode"] for e in setups}), len(setups))
        self.assertTrue(all(e["direction"] == "neutral" for e in setups))

    def test_hikkake_library_confirmation_links_episode(self):
        bars = noisy_bars(3000, seed=21)
        res = cdl.detect(bars, "1D")
        for side in ("long", "short"):
            ev = res[("CDLHIKKAKE", "canonical", side)]
            setups = {e["episode"]: e for e in ev if e["state"] == "setup"}
            confirms = [e for e in ev if e["state"] == "confirmed"]
            self.assertTrue(confirms)
            for e in confirms:
                self.assertIn(e["episode"], setups)
                self.assertEqual(setups[e["episode"]]["signal_index"], e["detected_index"])
                self.assertLessEqual(e["signal_index"] - e["detected_index"], 3)
                self.assertEqual(abs(e["library_value"]), 200)

    def test_intrinsic_three_inside_is_confirmed_only(self):
        bars = noisy_bars(3000, seed=4)
        res = cdl.detect(bars, "1D")
        ev = res[("CDL3INSIDE", "canonical", "long")] + res[("CDL3INSIDE", "canonical", "short")]
        self.assertTrue(ev)
        self.assertTrue(all(e["state"] == "confirmed" and e["detected_index"] == e["signal_index"] for e in ev))

    def test_candle_settings_are_restored(self):
        bars = noisy_bars(600, seed=2)
        before = cdl.detect(bars, "1D")
        from talib import _ta_lib
        _ta_lib._ta_set_candle_settings(3, 0, 10, 0.5)  # BodyDoji factor changed
        after = cdl.detect(bars, "1D")
        self.assertEqual(before, after)

    def test_settings_changed_before_import_do_not_contaminate_lookbacks(self):
        """Codex finding 2: isolated import after a non-default BodyDoji avgperiod."""
        import subprocess
        import sys
        tests_dir = os.path.dirname(os.path.abspath(__file__))
        code = f"""
import sys
sys.path[:0] = [{ROOT!r}, {tests_dir!r}]
from talib import _ta_lib
_ta_lib._ta_set_candle_settings(3, 1, 100, 0.1)   # BodyDoji avgperiod 100 BEFORE import
import numpy as np, talib
from market_scanner.pattern_research import candlesticks as cdl, common
from test_detectors_support import random_walk, make_bars
assert cdl._LIB_LOOKBACK['CDLGRAVESTONEDOJI'] == 10, cdl._LIB_LOOKBACK['CDLGRAVESTONEDOJI']
bars = random_walk(100, seed=3)
bars[60]['gap'] = True                       # 40-bar segment 60..99, then gravestone at 100
last = bars[-1]['close']
bars.append(dict(bars[-1], time='2099-01-01 09:15:00', end='2099-01-01 15:30:00', open=last, high=last + 3 * abs(last) * 0.01 + 2, low=last, close=last, gap=False))
s = common.prepare(bars, '1D')
raw = cdl.library_outputs(s)['CDLGRAVESTONEDOJI']
cdl._restore_default_candle_settings()
seg = slice(60, 101)
direct = talib.CDLGRAVESTONEDOJI(s.o[seg], s.h[seg], s.l[seg], s.c[seg])
assert direct[-1] == 100, direct[-1]
assert raw[100] == direct[-1], (raw[100], direct[-1])
assert np.array_equal(raw[seg], direct)
print('ok')
"""
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        self.assertIn("ok", proc.stdout)

    def test_stub_tag_intraday(self):
        bars = noisy_bars(300, seed=9)
        for i, b in enumerate(bars):
            b["end"] = b["time"][:11] + ("09:30:00" if i % 7 == 0 else "10:15:00")
        res = cdl.detect(bars, "1H")
        tagged = [e for v in res.values() for e in v if "session_stub" in e.get("quality_tags", [])]
        self.assertTrue(tagged)
        for e in tagged:
            self.assertTrue(any(k % 7 == 0 for k in range(e["formation_start_index"], e["signal_index"] + 1)))

    def test_many_gaps_never_create_spanning_patterns(self):
        bars = noisy_bars(2000, seed=13)
        rng = np.random.default_rng(1)
        for i in rng.choice(np.arange(1, 2000), 120, replace=False):
            bars[int(i)]["gap"] = True
        s = common.prepare(bars, "1D")
        res = assert_contract(self, cdl, bars)
        for events in res.values():
            for e in events:
                a = e["formation_start_index"]
                self.assertTrue(s.clean(a, e["signal_index"]))
                self.assertGreaterEqual(a - s.seg_start[a], 20 - (e["detected_index"] - a))

    def test_performance_20k(self):
        bars = random_walk(20000, seed=17)
        t0 = time.perf_counter()
        res = cdl.detect(bars, "1H")
        dt = time.perf_counter() - t0
        n = sum(len(v) for v in res.values())
        print(f"\n[candles] 20k bars: {dt:.2f}s, {n} events")
        self.assertLess(dt, 60)


if __name__ == "__main__":
    unittest.main()
