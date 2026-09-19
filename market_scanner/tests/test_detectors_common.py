import math
import unittest

import numpy as np

from test_detectors_support import make_bars, random_walk
from market_scanner.pattern_research import common
from market_scanner.historical import confirmed_pivot_positions


class CommonTests(unittest.TestCase):
    def test_prior_atr_excludes_signal_bar_and_is_prefix_invariant(self):
        bars = random_walk(300, seed=3)
        s = common.prepare(bars, "1D")
        self.assertTrue(np.isnan(s.atr[:20]).all())
        i = 150
        expected = max(np.mean(s.tr[i - 20:i]), s.c[i] * 0.001)
        self.assertAlmostEqual(s.atr[i], expected, places=9)
        short = common.prepare(bars[:i + 1], "1D")
        self.assertEqual(short.atr[i], s.atr[i])
        self.assertEqual(short.trend(i), s.trend(i))

    def test_gap_breaks_segments_atr_and_pivots(self):
        bars = random_walk(200, seed=4)
        bars[100]["gap"] = True
        s = common.prepare(bars, "1D")
        self.assertFalse(s.clean(99, 100))
        self.assertTrue(s.clean(100, 130))
        self.assertTrue(np.isnan(s.atr[100:120]).all())
        self.assertTrue(math.isfinite(s.atr[120]))
        self.assertEqual(s.tr[100], s.h[100] - s.l[100])
        for p in s.pivots(True):
            self.assertTrue(p + 3 < 100 or p - 3 >= 100)
        self.assertIsNone(s.trend(105))

    def test_pivots_match_legacy_on_clean_data(self):
        bars = random_walk(500, seed=5)
        s = common.prepare(bars, "1D")
        np.testing.assert_array_equal(s.pivots(True), confirmed_pivot_positions(s.h, True))
        np.testing.assert_array_equal(s.pivots(False), confirmed_pivot_positions(s.l, False))
        avail = s.pivots_available(200, True)
        self.assertTrue((avail + 3 <= 200).all())

    def test_trend_classification(self):
        up = make_bars([(100 + 2 * i, 101 + 2 * i, 99 + 2 * i, 100.5 + 2 * i) for i in range(60)])
        down = make_bars([(200 - 2 * i, 201 - 2 * i, 199 - 2 * i, 199.5 - 2 * i) for i in range(60)])
        flat = make_bars([(100, 101, 99, 100 + (i % 2) * 0.2) for i in range(60)])
        self.assertEqual(common.prepare(up, "1D").trend(40), "up")
        self.assertEqual(common.prepare(down, "1D").trend(40), "down")
        self.assertEqual(common.prepare(flat, "1D").trend(40), "neutral")
        self.assertIsNone(common.prepare(flat, "1D").trend(10))

    def test_invalid_bar_isolated(self):
        bars = random_walk(120, seed=6)
        bars[60]["high"] = bars[60]["low"] - 1
        s = common.prepare(bars, "1D")
        self.assertFalse(s.valid[60])
        self.assertFalse(s.clean(59, 61))
        self.assertTrue(s.clean(61, 80))

    def test_stub_flag_intraday(self):
        bars = make_bars([(10, 11, 9, 10)] * 3)
        bars[1]["end"] = bars[1]["time"][:11] + "09:30:00"
        bars[0]["end"] = bars[0]["time"][:11] + "10:15:00"
        bars[2]["end"] = bars[2]["time"][:11] + "10:15:00"
        s = common.prepare(bars, "1H")
        self.assertEqual(list(s.stub), [False, True, False])

    def test_event_and_collector(self):
        bars = random_walk(80, seed=8)
        s = common.prepare(bars, "1D")
        e = common.make_event(s, signal_index=50, episode=48, state="setup", direction="bullish",
                              formation_start=48, detected_index=50, geometry={"x": np.float64(1.5)})
        self.assertEqual(e["pattern_start"], bars[48]["time"])
        col = common.Collector([("A", "v", "long")])
        self.assertTrue(col.add(("A", "v", "long"), e))
        self.assertFalse(col.add(("A", "v", "long"), dict(e)))
        with self.assertRaises(ValueError):
            common.make_event(s, signal_index=5, episode=5, state="setup", direction="bullish",
                              formation_start=5, detected_index=5)  # no ATR yet
        with self.assertRaises(ValueError):
            common.json_safe(float("nan"))

    def test_confirm_close_beyond_stops_at_gap(self):
        rows = [(100, 101, 99, 100)] * 40 + [(100, 101, 99, 100), (100, 110, 100, 109)]
        bars = make_bars(rows)
        s = common.prepare(bars, "1D")
        self.assertEqual(common.confirm_close_beyond(s, 40, 101, 99, True), 41)
        bars[41]["gap"] = True
        self.assertIsNone(common.confirm_close_beyond(common.prepare(bars, "1D"), 40, 101, 99, True))

    def test_empty_input(self):
        s = common.prepare([], "1D")
        self.assertEqual(s.n, 0)


if __name__ == "__main__":
    unittest.main()
