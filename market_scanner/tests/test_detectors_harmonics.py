import math
import unittest

from test_detectors_support import make_bars, from_closes, random_walk, assert_contract, assert_causal, events_for
from market_scanner.pattern_research import harmonics

W = 0.2          # symmetric wick; turning-bar extreme equals the target price exactly
WARM = 35        # flat warm-up bars (ties -> no pivots) so ATR exists
PRE = 8          # bars from warm-up level into the first point
LEGS = (12, 9, 8, 11)
X_INDEX = WARM - 1 + PRE
MIRROR = 400.0

# (pattern_id, variant): (turning prices bull, near-miss prices bull, ratio name, expected in-band ratio)
FIXTURES = {
    ("HA01", "canonical"): ([200, 140, 176, 116], [200, 140, 176, 107], "CD/AB", 1.0),
    ("HA02", "ext_1_27"): ([200, 140, 176, 99.8], [200, 140, 176, 89], "CD/AB", 1.27),
    ("HA02", "ext_1_618"): ([200, 140, 176, 78.92], [200, 140, 176, 89], "CD/AB", 1.618),
    ("HA03", "canonical"): ([100, 200, 138.2, 183.2, 121.4], [100, 200, 130, 191.4, 121.4], "AD/XA", 0.786),
    ("HA04", "b_0_382"): ([100, 200, 160, 192, 111.4], [100, 200, 160, 192, 120], "AD/XA", 0.886),
    ("HA04", "b_0_50"): ([100, 200, 153, 190.6, 111.4], [100, 200, 153, 190.6, 120], "AD/XA", 0.886),
    ("HA05", "canonical"): ([100, 200, 162, 195.5, 87], [100, 200, 162, 195.5, 95], "AD/XA", 1.13),
    ("HA06", "ext_1_27"): ([100, 200, 121.4, 169.97, 73], [100, 200, 121.4, 169.97, 84], "AD/XA", 1.27),
    ("HA06", "ext_1_618"): ([100, 200, 121.4, 176.42, 38.2], [100, 200, 121.4, 176.42, 60], "AD/XA", 1.618),
    ("HA07", "canonical"): ([100, 200, 140, 188, 38.2], [100, 200, 140, 188, 55], "AD/XA", 1.618),
    ("HA08", "canonical"): ([100, 200, 111.4, 164.56, 38.2], [100, 200, 120, 168, 38.2], "AD/XA", 1.618),
    ("HA09", "canonical"): ([100, 200, 150, 213.5, 100], [100, 200, 150, 202.5, 100], "XC/OX", 1.0),
    ("HA10", "canonical"): ([100, 150, 86.5, 207.15, 146.825], [100, 150, 86.5, 207.15, 132.35], "CD/BC", 0.5),
}


def build(prices, bull=True, post=None, gaps=()):
    """Piecewise-linear close path through the turning prices (bull orientation), then mirrored for bear.

    Turning closes are offset by the wick so the pivot extreme equals the target price.
    Returns (bars, point indices)."""
    n = len(prices)
    first_low = n == 5                      # X/O low for 5-point bull; A high for 4-point bull
    kinds = [("L" if (k % 2 == 0) == first_low else "H") for k in range(n)]
    pre = prices[0] + 30 if kinds[0] == "L" else prices[0] - 30
    closes = [pre] * WARM
    idx = []
    cur = pre
    segs = [(PRE,)] + [(b,) for b in LEGS[:n - 1]]
    for k, (price, kind) in enumerate(zip(prices, kinds)):
        target = price + W if kind == "L" else price - W
        nb = segs[k][0]
        for j in range(1, nb + 1):
            closes.append(cur + (target - cur) * j / nb)
        cur = target
        idx.append(len(closes) - 1)
    last_low = kinds[-1] == "L"
    post = post if post is not None else [(60.0, 10)]
    for delta, nb in post:
        target = cur + (delta if last_low else -delta)
        for j in range(1, nb + 1):
            closes.append(cur + (target - cur) * j / nb)
        cur = target
    closes.extend([cur] * 5)
    if not bull:
        closes = [MIRROR - c for c in closes]
    rows = [(c, c + W, c - W, c) for c in closes]
    return make_bars(rows, gaps=gaps), idx


def five_point_ratios(p):
    legs = [abs(p[k + 1] - p[k]) for k in range(4)]
    return {"r21": legs[1] / legs[0], "r32": legs[2] / legs[1], "r43": legs[3] / legs[2],
            "r42": legs[3] / legs[1], "ret41": (p[1] - p[4]) / legs[0]}


MIN_CELLS = [("HA04", "b_0_382"), ("HA04", "b_0_50"), ("HA06", "ext_1_27"), ("HA06", "ext_1_618"),
             ("HA07", "canonical")]


def key_for(pid, variant, bull):
    return (pid, variant, "long" if bull else "short")


class HarmonicSpecTests(unittest.TestCase):
    def test_specifications_cover_all_cells(self):
        specs = harmonics.specifications()
        keys = {(d["pattern_id"], d["variant"], d["side"]) for d in specs}
        self.assertEqual(len(keys), len(specs))
        self.assertEqual(len(specs), 26)
        self.assertEqual({d["pattern_id"] for d in specs}, {f"HA{i:02d}" for i in range(1, 11)})
        for d in specs:
            self.assertEqual(d["family"], "harmonic")
            self.assertEqual(d["states"], ["setup", "confirmed"])
            self.assertIn("Confirmation", d["definition"])
            self.assertTrue(d["ratio_bands"])
        for pid, variant in FIXTURES:
            self.assertIn((pid, variant, "long"), keys)
            self.assertIn((pid, variant, "short"), keys)
        result = harmonics.detect([], "1D")
        self.assertEqual(set(result), keys)
        self.assertTrue(all(v == [] for v in result.values()))

    def test_flat_series_has_no_events(self):
        bars = make_bars([(100, 100.2, 99.8, 100)] * 400)
        self.assertFalse(any(harmonics.detect(bars, "1D").values()))


class HarmonicFixtureTests(unittest.TestCase):
    def test_positive_fixtures_setup_and_confirmation(self):
        for (pid, variant), (prices, _, ratio_name, ratio) in FIXTURES.items():
            for bull in (True, False):
                with self.subTest(pid=pid, variant=variant, bull=bull):
                    bars, idx = build(prices, bull)
                    result = assert_contract(self, harmonics, bars)
                    ev = result[key_for(pid, variant, bull)]
                    d = idx[-1]
                    setups = [e for e in ev if e["state"] == "setup"]
                    confirms = [e for e in ev if e["state"] == "confirmed"]
                    self.assertEqual(len(setups), 1, ev)
                    self.assertEqual(len(confirms), 1, ev)
                    su, co = setups[0], confirms[0]
                    self.assertEqual(su["signal_index"], d + 3)
                    self.assertEqual(su["detected_index"], d + 3)
                    self.assertEqual(su["episode"], idx[0])
                    self.assertEqual(su["formation_start_index"], idx[0])
                    self.assertEqual(su["direction"], "bullish" if bull else "bearish")
                    self.assertNotIn("confirmed_index", su)
                    self.assertTrue(0.0 <= su["score"] <= 1.0)
                    self.assertGreater(su["score"], 0.5)
                    geo = su["geometry"]
                    labels = list(geo["points"])
                    for lab, i, p in zip(labels, idx, prices):
                        self.assertEqual(geo["points"][lab]["index"], i)
                        self.assertEqual(geo["points"][lab]["available_index"], i + 3)
                        self.assertAlmostEqual(geo["points"][lab]["price"], p if bull else MIRROR - p, places=6)
                    self.assertAlmostEqual(geo["ratios"][ratio_name], ratio, delta=0.002)
                    lo, hi = geo["prz"]["price_band"]
                    dprice = geo["points"][labels[-1]]["price"]
                    self.assertTrue(lo - 1e-6 <= dprice <= hi + 1e-6)
                    self.assertEqual(co["signal_index"], d + 4)
                    self.assertEqual(co["confirmed_index"], d + 4)
                    self.assertEqual(co["detected_index"], d + 3)
                    self.assertEqual(co["episode"], su["episode"])
                    self.assertEqual(co["geometry"], su["geometry"])
                    # the opposite side never fires on the same terminal point
                    self.assertEqual(result[key_for(pid, variant, not bull)], [])

    def test_alias_groups(self):
        expected = {"HA01": "abcd", "HA02": "abcd", "HA03": "xabcd", "HA04": "xabcd", "HA05": "xabcd",
                    "HA06": "xabcd", "HA07": "xabcd", "HA08": "xabcd", "HA09": "shark", "HA10": "five_zero"}
        for (pid, variant), (prices, *_rest) in FIXTURES.items():
            bars, _ = build(prices)
            ev = harmonics.detect(bars, "1D")[key_for(pid, variant, True)]
            self.assertTrue(ev)
            self.assertTrue(all(e["alias_group"] == expected[pid] for e in ev))

    def test_near_misses_emit_nothing(self):
        for (pid, variant), (_, miss, *_rest) in FIXTURES.items():
            for bull in (True, False):
                with self.subTest(pid=pid, variant=variant, bull=bull):
                    bars, _ = build(miss, bull)
                    result = assert_contract(self, harmonics, bars)
                    self.assertEqual(result[key_for(pid, variant, bull)], [])

    def test_named_near_misses(self):
        # Gartley with B at 0.70 of XA, Crab with D at 1.45 of XA
        r = harmonics.detect(build([100, 200, 130, 191.4, 121.4])[0], "1D")
        self.assertEqual(events_for(r, "HA03"), [])
        r = harmonics.detect(build([100, 200, 140, 188, 55])[0], "1D")
        self.assertEqual(events_for(r, "HA07"), [])
        # AB=CD at 1.15 sits between HA01 and HA02 bands
        r = harmonics.detect(build([200, 140, 176, 107])[0], "1D")
        self.assertEqual(events_for(r, "HA01") + events_for(r, "HA02"), [])

    def test_abcd_confluence_gate(self):
        # Every other ratio in band; only CD/AB sits just outside [0.90, 1.10].
        cases = {
            "HA03": {1.20: [100, 200, 140, 192, 120], 0.80: [100, 200, 138.2, 170.84, 121.4]},
            "HA10": {1.20: [100, 150, 86.5, 226.2, 150], 0.80: [100, 150, 86.5, 194.45, 143.65]},
        }
        for pid, by_ratio in cases.items():
            for ratio, prices in by_ratio.items():
                cd = abs(prices[3] - prices[4]) / abs(prices[1] - prices[2])
                self.assertAlmostEqual(cd, ratio, delta=0.005)
                for bull in (True, False):
                    with self.subTest(pid=pid, ratio=ratio, bull=bull):
                        bars, _ = build(prices, bull)
                        result = assert_contract(self, harmonics, bars)
                        self.assertEqual(events_for(result, pid), [])
            prices = FIXTURES[(pid, "canonical")][0]
            for bull in (True, False):
                with self.subTest(pid=pid, positive=True, bull=bull):
                    ev = harmonics.detect(build(prices, bull)[0], "1D")[key_for(pid, "canonical", bull)]
                    self.assertEqual([e["state"] for e in ev], ["setup", "confirmed"])
                    self.assertTrue(0.90 <= ev[0]["geometry"]["ratios"]["CD/AB"] <= 1.10)
        spec_text = {d["pattern_id"]: d["definition"] for d in harmonics.specifications()}
        self.assertIn("CD/AB in [0.9, 1.1]", spec_text["HA03"])
        self.assertIn("CD/AB in [0.9, 1.1]", spec_text["HA10"])

    def test_abcd_minimum_completion_gate(self):
        templates = {(t["pattern_id"], t["variant"]): t for t in harmonics.TEMPLATES}
        specs = {(d["pattern_id"], d["variant"], d["side"]): d for d in harmonics.specifications()}
        for cell in MIN_CELLS:
            with self.subTest(cell=cell):
                t = templates[cell]
                mins = [c for c in t["constraints"] if c["ratio"] == "r42"]
                self.assertEqual([(c["lo"], c["hi"], c["ideal"]) for c in mins], [(0.90, None, None)])
                for side in ("long", "short"):
                    d = specs[cell + (side,)]
                    self.assertIn("CD/AB >= 0.9 (minimum only", d["definition"])
                    self.assertIn("never a gate", d["definition"])
                    self.assertEqual([b for b in d["ratio_bands"] if b["ratio"] == "CD/AB"],
                                     [{"ratio": "CD/AB", "lo": 0.9, "hi": None, "lo_open": False,
                                       "hi_open": False, "ideal": None, "bound": "minimum"}])
                prices = FIXTURES[cell][0]
                ratios = five_point_ratios(prices)
                self.assertGreaterEqual(ratios["r42"], 0.90)          # primary fixture unchanged and still valid
                self.assertIsNotNone(harmonics._check(t, ratios))
                self.assertIsNotNone(harmonics._check(t, dict(ratios, r42=50.0)))   # no upper limit
                self.assertIsNone(harmonics._check(t, dict(ratios, r42=0.80)))      # below minimum, rest in band
                self.assertIsNone(harmonics._check(t, dict(ratios, r42=0.899)))
                for bull in (True, False):
                    ev = harmonics.detect(build(prices, bull)[0], "1D")[key_for(*cell, bull)]
                    self.assertEqual([e["state"] for e in ev], ["setup", "confirmed"])
                    ext = ev[0]["geometry"]["abcd_extension"]
                    self.assertTrue(ext["descriptive_only"])
                    self.assertEqual(ext["typical_levels"], list(t["typical_ext"]))
                    self.assertIn(ext["nearest_typical"], ext["typical_levels"])
                    self.assertAlmostEqual(ext["cd_ab"], ratios["r42"], places=6)
                    self.assertAlmostEqual(ev[0]["geometry"]["ratios"]["CD/AB"], ratios["r42"], places=6)
        # Bar level, long and short.  Butterfly 1.27 is the only cell where CD/AB < 0.90 is reachable with every
        # other ratio in band (see test_abcd_minimum_reachability): CD/AB 0.893 rejected, 0.913 accepted.
        t = templates[("HA06", "ext_1_27")]
        below, above = [100, 200, 119.5, 151.7, 79.8], [100, 200, 119.5, 153.31, 79.8]
        rb = five_point_ratios(below)
        self.assertAlmostEqual(rb["r42"], 0.8932, delta=0.001)
        self.assertIsNotNone(harmonics._check(t, dict(rb, r42=1.0)))
        for bull in (True, False):
            with self.subTest(bull=bull):
                r = assert_contract(self, harmonics, build(below, bull)[0])
                self.assertEqual(events_for(r, "HA06"), [])
                r = harmonics.detect(build(above, bull)[0], "1D")
                self.assertEqual([e["state"] for e in r[key_for("HA06", "ext_1_27", bull)]], ["setup", "confirmed"])
        # Cells without the minimum carry no annotation
        ev = harmonics.detect(build(FIXTURES[("HA03", "canonical")][0])[0], "1D")[key_for("HA03", "canonical", True)]
        self.assertNotIn("abcd_extension", ev[0]["geometry"])

    def test_abcd_minimum_reachability(self):
        """CD/AB = (AD-AB)/AB + BC/AB in XA units.  Scan the other bands (incl. CD/BC) for the smallest CD/AB."""
        import numpy as np
        templates = {(t["pattern_id"], t["variant"]): t for t in harmonics.TEMPLATES}
        g = np.linspace(0.0, 1.0, 121)
        minima = {}
        for cell in MIN_CELLS:
            cons = {c["ratio"]: c for c in templates[cell]["constraints"]}
            axes = [cons[k]["lo"] + g * (cons[k]["hi"] - cons[k]["lo"]) for k in ("r21", "r32", "ret41")]
            ab, bc, ad = np.meshgrid(*axes, indexing="ij")
            cd_ab = (ad - ab) / ab + bc
            cd_bc = cd_ab / bc
            ok = (cd_bc >= cons["r43"]["lo"]) & (cd_bc <= cons["r43"]["hi"])
            minima[cell] = float(cd_ab[ok].min())
        for cell in [("HA04", "b_0_382"), ("HA04", "b_0_50"), ("HA06", "ext_1_618"), ("HA07", "canonical")]:
            self.assertGreater(minima[cell], 0.90, cell)       # gate implied; stated explicitly
        self.assertLess(minima[("HA06", "ext_1_27")], 0.90)      # gate binding here

    def test_variant_exclusivity(self):
        r = harmonics.detect(build(FIXTURES[("HA04", "b_0_382")][0])[0], "1D")
        self.assertTrue(events_for(r, "HA04", "b_0_382"))
        self.assertEqual(events_for(r, "HA04", "b_0_50"), [])
        r = harmonics.detect(build(FIXTURES[("HA02", "ext_1_27")][0])[0], "1D")
        self.assertEqual(events_for(r, "HA02", "ext_1_618") + events_for(r, "HA01"), [])
        r = harmonics.detect(build(FIXTURES[("HA07", "canonical")][0])[0], "1D")
        self.assertEqual(events_for(r, "HA08") + events_for(r, "HA06"), [])

    def test_gap_inside_window_blocks_pattern(self):
        for (pid, variant), (prices, *_rest) in FIXTURES.items():
            for pos in (1, len(prices) - 1):
                with self.subTest(pid=pid, variant=variant, pos=pos):
                    _, idx = build(prices)
                    bars, _ = build(prices, gaps=(idx[pos] - 2,))
                    result = assert_contract(self, harmonics, bars)
                    self.assertEqual(result[key_for(pid, variant, True)], [])

    def test_gap_in_confirmation_window_expires(self):
        for (pid, variant), (prices, *_rest) in FIXTURES.items():
            with self.subTest(pid=pid, variant=variant):
                _, idx = build(prices)
                bars, _ = build(prices, gaps=(idx[-1] + 4,))
                ev = harmonics.detect(bars, "1D")[key_for(pid, variant, True)]
                self.assertEqual([e["state"] for e in ev], ["setup"])
                self.assertEqual(ev[0]["signal_index"], idx[-1] + 3)

    def test_failure_blocks_confirmation(self):
        fail_post = [(0.5, 1), (0.5, 1), (0.5, 1), (-12.0, 2), (80.0, 6)]
        ok_post = [(0.5, 1), (0.5, 1), (0.5, 1), (80.0, 6)]
        for (pid, variant), (prices, *_rest) in FIXTURES.items():
            for bull in (True, False):
                with self.subTest(pid=pid, variant=variant, bull=bull):
                    bars, idx = build(prices, bull, post=fail_post)
                    ev = assert_contract(self, harmonics, bars)[key_for(pid, variant, bull)]
                    self.assertEqual([(e["state"], e["signal_index"]) for e in ev], [("setup", idx[-1] + 3)])
                    bars, idx = build(prices, bull, post=ok_post)
                    ev = harmonics.detect(bars, "1D")[key_for(pid, variant, bull)]
                    self.assertEqual([(e["state"], e["signal_index"]) for e in ev],
                                     [("setup", idx[-1] + 3), ("confirmed", idx[-1] + 4)])

    def test_setup_not_available_before_pivot_confirmation(self):
        prices = FIXTURES[("HA03", "canonical")][0]
        bars, idx = build(prices)
        d = idx[-1]
        for cut in (d + 1, d + 2, d + 3):
            self.assertEqual(events_for(harmonics.detect(bars[:cut], "1D"), "HA03"), [])
        ev = events_for(harmonics.detect(bars[:d + 4], "1D"), "HA03")
        self.assertEqual([e["state"] for e in ev], ["setup"])

    def test_span_gate(self):
        global LEGS
        saved = LEGS
        try:
            LEGS = (45, 40, 40, 40)   # X->D span 165 > 150
            bars, _ = build(FIXTURES[("HA03", "canonical")][0])
            self.assertEqual(events_for(harmonics.detect(bars, "1D"), "HA03"), [])
        finally:
            LEGS = saved

    def test_causal_on_fixtures(self):
        for (pid, variant), (prices, *_rest) in FIXTURES.items():
            for bull in (True, False):
                with self.subTest(pid=pid, variant=variant, bull=bull):
                    bars, idx = build(prices, bull)
                    d = idx[-1]
                    assert_causal(self, harmonics, bars, cuts=[d + 2, d + 3, d + 4, d + 5, len(bars) - 3])


class HarmonicRandomWalkTests(unittest.TestCase):
    def test_contract_and_causality_on_random_walks(self):
        total = 0
        for seed in (1, 5, 11, 23):
            bars = random_walk(3000, seed=seed)
            result = assert_contract(self, harmonics, bars)
            total += sum(len(v) for v in result.values())
            for events in result.values():
                for e in events:
                    self.assertTrue(0.0 <= e["score"] <= 1.0)
                    self.assertTrue(math.isfinite(e["atr"]))
                    g = e["geometry"]
                    pts = list(g["points"].values())
                    self.assertEqual(e["episode"], pts[0]["index"])
                    self.assertEqual(e["detected_index"], pts[-1]["available_index"])
                    self.assertTrue(20 <= g["span_bars"] <= 150)
            assert_causal(self, harmonics, bars)
        self.assertGreater(total, 0)

    def test_random_walk_with_quality_gaps(self):
        bars = random_walk(2000, seed=9)
        for i in (300, 700, 701, 1500):
            bars[i]["gap"] = True
        result = assert_contract(self, harmonics, bars)
        for events in result.values():
            for e in events:
                lo = e["formation_start_index"]
                hi = e["signal_index"]
                self.assertFalse(any(bars[i]["gap"] for i in range(lo + 1, hi + 1)))
        assert_causal(self, harmonics, bars)

    def test_from_closes_fixture_is_causal(self):
        closes = [100 + 20 * math.sin(i / 9.0) + 8 * math.sin(i / 3.1) for i in range(900)]
        bars = from_closes(closes, spread=0.4, seed=3)
        assert_contract(self, harmonics, bars)
        assert_causal(self, harmonics, bars)


if __name__ == "__main__":
    unittest.main()
