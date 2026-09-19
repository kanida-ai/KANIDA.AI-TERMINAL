"""CH11-CH28 chart-pattern detector tests (synthetic fixtures only).

Run from the repository root:
    market_scanner/.venv/Scripts/python.exe -m unittest discover -s market_scanner/tests -p "test_detectors_chart.py" -v
"""
import time
import unittest

import numpy as np

from test_detectors_support import make_bars, from_closes, random_walk, assert_contract, assert_causal, events_for
from market_scanner.pattern_research import chart_patterns

MIRROR = 240.0


def path_bars(knots, wick=0.3, mirror=None, gaps=(), vols=None):
    """Piecewise-linear close path; open = midpoint of previous and current close.

    Strictly monotone legs give unique radius-3 pivots exactly at the knots.
    mirror=X reflects prices (X - price) to build the bearish/top counterpart.
    """
    xs = [k[0] for k in knots]
    ys = [k[1] for k in knots]
    closes = np.interp(np.arange(xs[-1] + 1), xs, ys)
    if mirror is not None:
        closes = mirror - closes
    rows, prev = [], closes[0]
    for k, c in enumerate(closes):
        o = (prev + c) / 2
        row = (o, max(o, c) + wick, min(o, c) - wick, c)
        rows.append(row + ((vols(k),) if vols else ()))
        prev = c
    return make_bars(rows, gaps=gaps)


def island_bars(top=True, island=1, overlap=False, flat_prior=False, gaps=()):
    rows = []
    for k in range(30):
        o = 100.0 + (0 if flat_prior else k) + (0.5 * (k % 2) if flat_prior else 0)
        rows.append((o, o + 1.2, o - 0.2, o + 1))
    base = rows[-1][1]
    for m in range(island):
        rows.append((base + 2.8 + m * 0.2, base + 3.8 + m * 0.2, base + 2.3, base + 3.3))
    rows.append((base - 0.2, (base + 2.4) if overlap else (base + 0.3), base - 2.2, base - 1.7))
    for k in range(15):
        rows.append((base - 2.2 - k, base - 1.7 - k, base - 3.7 - k, base - 3.2 - k))
    if not top:
        rows = [(260 - o, 260 - l, 260 - h, 260 - c) for o, h, l, c in rows]
    return make_bars(rows, gaps=gaps)


# Positive fixtures: knots and expected exact indices (derived from the frozen contract).
DOUBLE = [(0, 130), (40, 100), (55, 112), (70, 100), (78, 110), (79, 118), (90, 122)]
TRIPLE = [(0, 130), (40, 100), (52, 111), (64, 100), (76, 111), (88, 100), (95, 109), (96, 116), (105, 120)]
ASC_UP = [(0, 100), (10, 104), (25, 110), (37, 100), (49, 110), (59, 104), (64, 109), (65, 114), (70, 116)]
ASC_DN = [(0, 100), (10, 104), (25, 110), (37, 100), (49, 110), (59, 104), (64, 108), (65, 100), (70, 98)]
BREAKDOWN = [(0, 110), (20, 100), (32, 110), (44, 100), (56, 110), (68, 100), (73, 104), (74, 95), (80, 93)]
RECT_UP = [(0, 110), (20, 100), (32, 110), (44, 100), (56, 110), (62, 104), (63, 114), (70, 116)]
RECT_DN = [(0, 110), (20, 100), (32, 110), (44, 100), (56, 110), (60, 106), (61, 96), (70, 94)]
PENNANT = [(0, 95), (10, 100), (20, 96), (30, 100), (38, 120), (42, 114), (46, 118.5), (50, 115), (53, 117),
           (54, 121), (60, 124)]
BOWL = [(0, 104), (10, 110)] + [(k, 100 + 10 * ((k - 55) / 45) ** 2) for k in range(11, 101)] + [(101, 113), (106, 115)]
CUP = ([(0, 80), (20, 100)] + [(k, 88 + 12 * ((k - 50) / 30) ** 2) for k in range(21, 80)]
       + [(80, 100), (86, 97), (89, 99), (90, 103), (95, 105)])
BROAD_UP = [(0, 100), (10, 104), (18, 97), (27, 106), (37, 94), (48, 108), (51, 105), (52, 112), (58, 114)]
BROAD_DN = [(0, 100), (10, 104), (18, 97), (27, 106), (37, 94), (48, 108), (51, 105), (52, 88), (58, 86)]
DIAMOND_UP = [(0, 80), (30, 100), (40, 104), (48, 97), (58, 110), (68, 90), (78, 104), (88, 97), (91, 99.5), (92, 104),
              (98, 106)]
DIAMOND_DN = DIAMOND_UP[:9] + [(92, 93), (98, 91)]
VEE = [(0, 100), (40, 110), (48, 94), (51, 100), (52, 106), (60, 108)]
MEASURED = [(0, 110), (20, 100), (40, 120), (52, 110), (58, 118), (59, 123), (65, 125)]
BUMP = [(0, 100), (10, 103), (16, 101), (28, 106), (40, 105.8), (56, 124), (59, 120), (62, 111), (63, 105), (70, 100)]
VCP = [(0, 95), (20, 110), (30, 100), (40, 109.5), (48, 103), (55, 109), (61, 105), (64, 108), (65, 113), (70, 115)]


def run(bars):
    return chart_patterns.detect(bars, "1D")


class ChartTestCase(unittest.TestCase):
    def one(self, result, pid, variant, side, state):
        found = events_for(result, pid, variant, side, state)
        self.assertEqual(len(found), 1, f"{pid}/{variant}/{side}/{state}: {found}")
        return found[0]

    def assert_pair(self, result, pid, variant, side, setup_index, confirm_index, episode, setup_key=None,
                    setup_direction=None, confirm_direction=None):
        """Setup (possibly in another key) and confirmation share the episode and indices."""
        skey = setup_key or (pid, variant, side)
        setup = self.one(result, skey[0], skey[1], skey[2], "setup")
        self.assertEqual((setup["signal_index"], setup["detected_index"], setup["episode"]),
                         (setup_index, setup_index, episode))
        self.assertEqual(setup["formation_start_index"], episode)
        if setup_direction:
            self.assertEqual(setup["direction"], setup_direction)
        conf = self.one(result, pid, variant, side, "confirmed")
        self.assertEqual((conf["signal_index"], conf["confirmed_index"], conf["detected_index"], conf["episode"]),
                         (confirm_index, confirm_index, setup_index, episode))
        if confirm_direction:
            self.assertEqual(conf["direction"], confirm_direction)
        self.assertIn("volume_ratio", conf["geometry"])
        return setup, conf

    def assert_quiet(self, result, pid, variant=None, side=None):
        self.assertEqual(events_for(result, pid, variant, side), [], f"{pid} unexpectedly fired")

    def check_invariants(self, bars):
        assert_contract(self, chart_patterns, bars)
        cuts = sorted({len(bars) // 2, len(bars) - 12, len(bars) - 5})
        assert_causal(self, chart_patterns, bars, cuts=cuts)


# ------------------------------------------------------------------ specifications
class SpecificationTests(ChartTestCase):
    def test_cells_cover_ch11_to_ch28(self):
        specs = chart_patterns.specifications()
        keys = [(d["pattern_id"], d["variant"], d["side"]) for d in specs]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual({d["pattern_id"] for d in specs}, {f"CH{i}" for i in range(11, 29)})
        for d in specs:
            self.assertEqual(d["family"], "chart")
            self.assertIn("0.12 ATR", d["definition"])
            self.assertTrue(set(d["states"]) <= {"setup", "confirmed"})
        confirmed_only = {k for k, d in zip(keys, specs) if d["states"] == ["confirmed"]}
        self.assertEqual(confirmed_only, {("CH11", "downside_resolution", "short"), ("CH19", "canonical", "long"),
                                          ("CH20", "canonical", "short"), ("CH24", "top_single", "short"),
                                          ("CH24", "top_multi", "short"), ("CH24", "bottom_single", "long"),
                                          ("CH24", "bottom_multi", "long")})

    def test_every_spec_key_present_on_short_and_empty_input(self):
        keys = {(d["pattern_id"], d["variant"], d["side"]) for d in chart_patterns.specifications()}
        for bars in ([], random_walk(30, seed=2)):
            result = run(bars)
            self.assertEqual(set(result), keys)
            self.assertTrue(all(v == [] for v in result.values()))

    def test_integration_gate(self):
        from market_scanner.pattern_research import runner
        runner.registry(("chart_patterns",))
        for scatter in (False, True):
            bars = random_walk(2500, seed=21)
            if scatter:
                for k in range(40, 2500, 97):
                    bars[k]["gap"] = True
            runner.validate_events(run(bars), chart_patterns.specifications(), bars)


# ------------------------------------------------------------------ CH11 / CH12 / CH13
class HorizontalBoundaryTests(ChartTestCase):
    def test_ch11_upside_and_downside_resolution(self):
        up = run(path_bars(ASC_UP))
        self.assert_pair(up, "CH11", "upside", "long", 62, 65, 25, setup_direction="bullish", confirm_direction="bullish")
        self.assert_quiet(up, "CH11", "downside_resolution")
        geo = self.one(up, "CH11", "upside", "long", "setup")["geometry"]
        self.assertEqual(geo["resistance_touches"], [25, 49])
        self.assertEqual(geo["support_troughs"], [37, 59])
        self.assertEqual(geo["support_available"], [40, 62])
        dn = run(path_bars(ASC_DN))
        self.assert_pair(dn, "CH11", "downside_resolution", "short", 62, 65, 25, setup_key=("CH11", "upside", "long"),
                         confirm_direction="bearish")
        self.assertEqual(events_for(dn, "CH11", "upside", state="confirmed"), [])
        self.check_invariants(path_bars(ASC_UP))

    def test_ch11_negative_flat_support_and_expiry(self):
        flat = [(0, 100), (10, 104), (25, 110), (37, 100), (49, 110), (59, 100), (64, 109), (65, 114), (70, 116)]
        self.assert_quiet(run(path_bars(flat)), "CH11")
        late = [(0, 100), (10, 104), (25, 110), (37, 100), (49, 110), (59, 104), (69, 110), (72, 107), (73, 114),
                (80, 116)]
        result = run(path_bars(late))
        self.one(result, "CH11", "upside", "long", "setup")
        self.assertEqual(events_for(result, "CH11", state="confirmed"), [])

    def test_ch11_gap(self):
        self.assert_quiet(run(path_bars(ASC_UP, gaps=(45,))), "CH11")

    def test_ch12_breakdown(self):
        bars = path_bars(BREAKDOWN)
        result = run(bars)
        setup, _ = self.assert_pair(result, "CH12", "support_breakdown", "short", 71, 74, 20,
                                    setup_direction="bearish", confirm_direction="bearish")
        self.assertEqual(setup["geometry"]["support_touches"], [20, 44, 68])
        self.check_invariants(bars)

    def test_ch12_negative_two_touches_and_gap(self):
        two = [(0, 110), (20, 100), (32, 110), (44, 100), (49, 104), (50, 95), (58, 93)]
        self.assert_quiet(run(path_bars(two)), "CH12")
        self.assert_quiet(run(path_bars(BREAKDOWN, gaps=(50,))), "CH12")

    def test_ch13_rectangle_both_sides(self):
        bars = path_bars(RECT_UP)
        up = run(bars)
        self.assert_pair(up, "CH13", "upside", "long", 59, 63, 20, setup_direction="neutral", confirm_direction="bullish")
        down_setup = self.one(up, "CH13", "downside", "short", "setup")
        self.assertEqual((down_setup["episode"], down_setup["direction"], down_setup["alias_group"]),
                         (20, "neutral", "horizontal_range"))
        self.assertEqual(events_for(up, "CH13", "downside", state="confirmed"), [])
        dn = run(path_bars(RECT_DN))
        self.assert_pair(dn, "CH13", "downside", "short", 59, 61, 20, confirm_direction="bearish")
        self.assertEqual(events_for(dn, "CH13", "upside", state="confirmed"), [])
        self.check_invariants(bars)

    def test_ch13_continuing_range_emits_once(self):
        knots = [(0, 110)] + [(20 + 12 * k, 100 if k % 2 == 0 else 110) for k in range(9)] + [(122, 105)]
        result = run(path_bars(knots))
        for variant, side in (("upside", "long"), ("downside", "short")):
            events = events_for(result, "CH13", variant, side)
            self.assertEqual([(e["state"], e["episode"]) for e in events], [("setup", 20)])

    def test_ch13_negative_unequal_tops_and_gap(self):
        unequal = [(0, 110), (20, 100), (32, 110), (44, 100), (56, 113), (62, 104), (63, 116), (70, 118)]
        self.assert_quiet(run(path_bars(unequal)), "CH13")
        self.assert_quiet(run(path_bars(RECT_UP, gaps=(40,))), "CH13")


# ------------------------------------------------------------------------ CH14
class PennantTests(ChartTestCase):
    def test_bull_and_bear(self):
        for mirror, variant, side, direction in ((None, "bull", "long", "bullish"), (MIRROR, "bear", "short", "bearish")):
            bars = path_bars(PENNANT, mirror=mirror)
            result = run(bars)
            setup, conf = self.assert_pair(result, "CH14", variant, side, 53, 54, 20, setup_direction=direction,
                                           confirm_direction=direction)
            self.assertEqual(setup["geometry"]["pole_top"], 38)
            self.assert_quiet(result, "CH14", "bear" if mirror is None else "bull")
            self.check_invariants(bars)

    def test_negative_weak_pole_and_gap(self):
        # Choppy pole: net 24 over a 44-point close path -> efficiency ~0.55 < frozen 0.72.
        choppy = [(0, 95), (10, 100), (20, 96), (26, 112), (31, 102), (38, 120), (42, 114), (46, 118.5), (50, 115),
                  (53, 117), (54, 121), (60, 124)]
        self.assert_quiet(run(path_bars(choppy)), "CH14")
        self.assert_quiet(run(path_bars(PENNANT, gaps=(45,))), "CH14")


# ------------------------------------------------------------------ CH15 - CH18
class DoubleTripleTests(ChartTestCase):
    def test_double_bottom_and_top(self):
        for mirror, pid, side, direction, alias in ((None, "CH16", "long", "bullish", "double_bottom"),
                                                    (MIRROR, "CH15", "short", "bearish", "double_top")):
            bars = path_bars(DOUBLE, mirror=mirror)
            result = run(bars)
            setup, conf = self.assert_pair(result, pid, "canonical", side, 73, 79, 40, setup_direction=direction,
                                           confirm_direction=direction)
            self.assertEqual(setup["geometry"]["troughs"], [40, 70])
            self.assertEqual(setup["geometry"]["troughs_available"], [43, 73])
            self.assertEqual(setup["alias_group"], alias)
            self.assertEqual(setup["quality_tags"], ["subtype:adam_adam"])
            self.check_invariants(bars)

    def test_eve_subtype_rule(self):
        rounded = [(0, 130), (37, 100.3), (40, 100), (43, 100.3), (55, 112), (70, 100), (78, 110), (79, 118), (90, 122)]
        setup = self.one(run(path_bars(rounded)), "CH16", "canonical", "long", "setup")
        self.assertEqual(setup["geometry"]["subtype"], "eve_adam")

    def test_double_negatives(self):
        unequal = [(0, 130), (40, 100), (55, 112), (70, 97), (78, 110), (79, 118), (90, 122)]
        self.assert_quiet(run(path_bars(unequal)), "CH16")
        no_trend = [(0, 101), (10, 99), (20, 101), (30, 99), (40, 100), (55, 112), (70, 100), (78, 110), (79, 118),
                    (90, 122)]
        self.assert_quiet(run(path_bars(no_trend)), "CH16")
        late = [(0, 130), (40, 100), (55, 110), (70, 100), (84, 111), (85, 118), (95, 122)]
        result = run(path_bars(late))
        self.one(result, "CH16", "canonical", "long", "setup")
        self.assertEqual(events_for(result, "CH16", state="confirmed"), [])
        self.assert_quiet(run(path_bars(DOUBLE, gaps=(60,))), "CH16")
        self.assert_quiet(run(path_bars(DOUBLE, mirror=MIRROR, gaps=(60,))), "CH15")

    def test_triple_bottom_links_double_without_rewriting(self):
        for mirror, pid, dpid, side, alias in ((None, "CH18", "CH16", "long", "double_bottom"),
                                               (MIRROR, "CH17", "CH15", "short", "double_top")):
            bars = path_bars(TRIPLE, mirror=mirror)
            result = run(bars)
            setup, _ = self.assert_pair(result, pid, "canonical", side, 91, 96, 40)
            self.assertEqual(setup["alias_group"], alias)
            self.assertEqual(setup["geometry"]["extends_double_episode"], 40)
            self.assertEqual(setup["geometry"]["troughs"], [40, 64, 88])
            double = self.one(result, dpid, "canonical", side, "setup")
            self.assertEqual((double["signal_index"], double["episode"]), (67, 40))
            prefix = run(bars[:80])
            self.assertEqual(events_for(prefix, dpid), events_for(result, dpid, state="setup")[:1])
            self.check_invariants(bars)

    def test_triple_negative_and_gap(self):
        lower = [(0, 130), (40, 100), (52, 111), (64, 100), (76, 111), (88, 97), (95, 109), (96, 116), (105, 120)]
        self.assert_quiet(run(path_bars(lower)), "CH18")
        self.assert_quiet(run(path_bars(TRIPLE, gaps=(70,))), "CH18")


# ------------------------------------------------------------------ CH19 / CH20 / CH21
class RoundedTests(ChartTestCase):
    def test_rounding_bottom_and_top(self):
        for mirror, pid, side, direction in ((None, "CH19", "long", "bullish"), (MIRROR, "CH20", "short", "bearish")):
            bars = path_bars(BOWL, mirror=mirror)
            result = run(bars)
            conf = self.one(result, pid, "canonical", side, "confirmed")
            self.assertEqual((conf["signal_index"], conf["detected_index"], conf["confirmed_index"], conf["episode"]),
                             (101, 101, 101, 10))
            self.assertEqual(conf["direction"], direction)
            self.assertGreaterEqual(conf["geometry"]["quadratic_r2"], 0.78)
            self.assertEqual(events_for(result, pid, state="setup"), [])
            self.check_invariants(bars)

    def test_rounding_negative_v_shape_and_gap(self):
        vee = [(0, 104), (10, 110), (55, 100), (100, 110), (101, 113), (106, 115)]
        self.assert_quiet(run(path_bars(vee)), "CH19")
        self.assert_quiet(run(path_bars(BOWL, gaps=(50,))), "CH19")

    def test_inverted_cup_and_handle(self):
        bars = path_bars(CUP, mirror=MIRROR)
        result = run(bars)
        setup, _ = self.assert_pair(result, "CH21", "canonical", "short", 89, 90, 20, setup_direction="bearish",
                                    confirm_direction="bearish")
        self.assertEqual((setup["geometry"]["left_rim"], setup["geometry"]["right_rim"],
                          setup["geometry"]["handle_extreme"]), (20, 80, 86))
        self.assert_quiet(run(path_bars(CUP)), "CH21")      # an ordinary cup is not an inverted cup
        self.check_invariants(bars)

    def test_inverted_cup_negative_deep_handle_and_gap(self):
        deep = CUP[:-4] + [(80, 100), (86, 93), (89, 96), (90, 103), (95, 105)]
        self.assert_quiet(run(path_bars(deep, mirror=MIRROR)), "CH21")
        self.assert_quiet(run(path_bars(CUP, mirror=MIRROR, gaps=(50,))), "CH21")


# ------------------------------------------------------------------ CH22 / CH23
class BroadeningDiamondTests(ChartTestCase):
    def test_broadening_both_breaks(self):
        bars = path_bars(BROAD_UP)
        up = run(bars)
        setup, _ = self.assert_pair(up, "CH22", "upside", "long", 51, 52, 10, setup_direction="neutral",
                                    confirm_direction="bullish")
        self.assertEqual(setup["geometry"]["anchors"], [10, 18, 27, 37, 48])
        self.assertIn(setup["geometry"]["slope"], ("ascending", "descending", "flat"))
        dn = run(path_bars(BROAD_DN))
        self.assert_pair(dn, "CH22", "downside", "short", 51, 52, 10, confirm_direction="bearish")
        self.assertEqual(events_for(dn, "CH22", "upside", state="confirmed"), [])
        self.check_invariants(bars)

    def test_broadening_negative_converging_and_gap(self):
        converging = [(0, 100), (10, 110), (18, 94), (27, 108), (37, 96), (48, 106), (51, 104), (52, 112), (58, 114)]
        self.assert_quiet(run(path_bars(converging)), "CH22")
        self.assert_quiet(run(path_bars(BROAD_UP, gaps=(30,))), "CH22")

    def test_diamond_contexts(self):
        bars = path_bars(DIAMOND_UP)
        up = run(bars)
        _, conf = self.assert_pair(up, "CH23", "upside", "long", 91, 92, 40, setup_direction="neutral",
                                   confirm_direction="bullish")
        self.assertEqual(conf["geometry"]["context"], "continuation")
        dn = run(path_bars(DIAMOND_DN))
        _, conf = self.assert_pair(dn, "CH23", "downside", "short", 91, 92, 40, confirm_direction="bearish")
        self.assertEqual(conf["geometry"]["context"], "top")
        self.check_invariants(bars)

    def test_diamond_negative_indistinct_middle_and_gap(self):
        flat_mid = DIAMOND_UP[:4] + [(58, 104.2)] + DIAMOND_UP[5:]
        self.assert_quiet(run(path_bars(flat_mid)), "CH23")
        self.assert_quiet(run(path_bars(DIAMOND_UP, gaps=(70,))), "CH23")


# ------------------------------------------------------------------------ CH24
class IslandTests(ChartTestCase):
    def test_single_and_multi_top_and_bottom(self):
        for top, pid_side, direction in ((True, ("top", "short"), "bearish"), (False, ("bottom", "long"), "bullish")):
            kind, side = pid_side
            for island, variant, j in ((1, f"{kind}_single", 31), (3, f"{kind}_multi", 33)):
                bars = island_bars(top, island)
                result = run(bars)
                conf = self.one(result, "CH24", variant, side, "confirmed")
                self.assertEqual((conf["signal_index"], conf["episode"], conf["detected_index"], conf["confirmed_index"]),
                                 (j, 29, j, j))
                self.assertEqual(conf["direction"], direction)
                self.assertEqual(conf["geometry"]["island_bars"], island)
                other = f"{kind}_multi" if island == 1 else f"{kind}_single"
                self.assert_quiet(result, "CH24", other)
                self.check_invariants(bars)

    def test_negatives_overlap_no_trend_and_quality_gap(self):
        self.assert_quiet(run(island_bars(True, 1, overlap=True)), "CH24")
        self.assert_quiet(run(island_bars(True, 1, flat_prior=True)), "CH24")
        # A quality-gap flag is never a price gap: flagging the island bar removes the event.
        self.assert_quiet(run(island_bars(True, 1, gaps=(30,))), "CH24")
        self.assert_quiet(run(island_bars(False, 3, gaps=(31,))), "CH24")

    def test_quality_flag_does_not_create_island(self):
        rows = [(100.0 + k, 101.2 + k, 99.8 + k, 101.0 + k) for k in range(45)]
        self.assert_quiet(run(make_bars(rows, gaps=(30, 31))), "CH24")


# ------------------------------------------------------------------ CH25 / CH26
class SwingTests(ChartTestCase):
    def test_v_bottom_and_inverted_v(self):
        for mirror, variant, side, direction in ((None, "v_bottom", "long", "bullish"),
                                                 (MIRROR, "inverted_v_top", "short", "bearish")):
            bars = path_bars(VEE, mirror=mirror)
            result = run(bars)
            setup, conf = self.assert_pair(result, "CH25", variant, side, 51, 52, 40, setup_direction=direction,
                                           confirm_direction=direction)
            self.assertEqual(setup["geometry"]["extreme"], 48)
            self.assertEqual(setup["geometry"]["extreme_available"], 51)
            self.check_invariants(bars)

    def test_v_negative_rounded_slow_and_gap(self):
        rounded = [(0, 100), (40, 110), (47, 96), (53, 96), (58, 104), (60, 108)]
        self.assert_quiet(run(path_bars(rounded)), "CH25")
        # Sharp start keeps the V setup at 51, then a stall: the level is first crossed at 63 (> 51 + 10) -> expired.
        slow = [(0, 100), (40, 110), (48, 94), (50, 98), (62, 101.8), (66, 108)]
        result = run(path_bars(slow))
        self.one(result, "CH25", "v_bottom", "long", "setup")
        self.assertEqual(events_for(result, "CH25", "v_bottom", state="confirmed"), [])
        self.assert_quiet(run(path_bars(VEE, gaps=(45,))), "CH25")

    def test_measured_move(self):
        for mirror, variant, side, direction in ((None, "bull", "long", "bullish"), (MIRROR, "bear", "short", "bearish")):
            bars = path_bars(MEASURED, mirror=mirror)
            result = run(bars)
            setup, _ = self.assert_pair(result, "CH26", variant, side, 55, 59, 20, setup_direction=direction,
                                        confirm_direction=direction)
            self.assertAlmostEqual(setup["geometry"]["retracement"], 0.5, delta=0.05)
            self.assertIn("projection_target", setup["geometry"])
            self.check_invariants(bars)

    def test_measured_move_negative_deep_correction_and_gap(self):
        deep = [(0, 110), (20, 100), (40, 120), (52, 102), (58, 118), (59, 123), (65, 125)]
        self.assert_quiet(run(path_bars(deep)), "CH26")
        self.assert_quiet(run(path_bars(MEASURED, gaps=(45,))), "CH26")


# ------------------------------------------------------------------ CH27 / CH28
class BumpContractionTests(ChartTestCase):
    def test_bump_and_run_top_and_bottom(self):
        for mirror, variant, side, direction in ((None, "top", "short", "bearish"), (MIRROR, "bottom", "long", "bullish")):
            bars = path_bars(BUMP, mirror=mirror)
            result = run(bars)
            setup, _ = self.assert_pair(result, "CH27", variant, side, 59, 63, 16, setup_direction=direction,
                                        confirm_direction=direction)
            self.assertEqual(setup["geometry"]["lead_in_lows"], [16, 40])
            self.assertGreaterEqual(setup["geometry"]["slope_multiple"], 2.0)
            self.check_invariants(bars)

    def test_bump_negative_no_acceleration_and_gap(self):
        mild = [(0, 100), (10, 103), (16, 101), (28, 106), (40, 105.8), (56, 109.5), (59, 108), (62, 105), (63, 100),
                (70, 98)]
        self.assert_quiet(run(path_bars(mild)), "CH27")
        self.assert_quiet(run(path_bars(BUMP, gaps=(30,))), "CH27")

    def test_vcp_base_and_volume_variant(self):
        for mirror, prefix, side, direction in ((None, "bullish", "long", "bullish"), (MIRROR, "bearish", "short", "bearish")):
            bars = path_bars(VCP, mirror=mirror)
            result = run(bars)
            setup, _ = self.assert_pair(result, "CH28", prefix, side, 64, 65, 20, setup_direction=direction,
                                        confirm_direction=direction)
            self.assertFalse(setup["geometry"]["volume_contracting"])
            self.assert_quiet(result, "CH28", f"{prefix}_volume_contraction")
            self.check_invariants(bars)
        vol = run(path_bars(VCP, vols=lambda k: 3000.0 - 30.0 * k))
        self.assert_pair(vol, "CH28", "bullish_volume_contraction", "long", 64, 65, 20)
        self.assert_pair(vol, "CH28", "bullish", "long", 64, 65, 20)

    def test_vcp_negative_expanding_pullback_and_gap(self):
        expanding = [(0, 95), (20, 110), (30, 100), (40, 109.5), (48, 98), (55, 109), (61, 105), (64, 108), (65, 113),
                     (70, 115)]
        self.assert_quiet(run(path_bars(expanding)), "CH28")
        self.assert_quiet(run(path_bars(VCP, gaps=(45,))), "CH28")


# ------------------------------------------------------------------ regressions
class RegressionTests(ChartTestCase):
    def test_flat_cluster_region_clipped_past_latest_pivot_returns_none(self):
        """Codex finding 4: a trigger bar whose high breaks the latest pivot band left an empty slice."""
        from market_scanner.pattern_research import common
        bars = path_bars(BREAKDOWN)
        bars[71]["high"] = 130.0           # trigger bar of low pivot 68 spikes far above every high pivot
        s = common.prepare(bars, "1D")
        view = chart_patterns._View(s, False)
        atr = s.atr_at(71)
        self.assertIsNone(chart_patterns._flat_cluster(view, 71, True, atr, 52))
        assert_contract(self, chart_patterns, bars)      # full detect must not raise either
        spike_last = path_bars(RECT_UP)
        spike_last[-1]["high"] = spike_last[-1]["close"] + 50
        run(spike_last)

    def test_ch28_both_keys_emit_and_mirrors_claim_independently(self):
        """Codex finding 5: volume-filtered keys were suppressed by a duplicate claim."""
        from market_scanner.pattern_research import common
        vols = lambda k: 3000.0 - 30.0 * k
        bull = run(path_bars(VCP, vols=vols))
        bear = run(path_bars(VCP, mirror=MIRROR, vols=vols))
        for result, prefix, side in ((bull, "bullish", "long"), (bear, "bearish", "short")):
            for variant in (prefix, f"{prefix}_volume_contraction"):
                self.assert_pair(result, "CH28", variant, side, 64, 65, 20)
        s = common.prepare(path_bars(VCP, vols=vols), "1D")
        ctx = chart_patterns._Ctx(s, chart_patterns.specifications())
        chart_patterns._vcp(ctx, False)
        self.assertFalse(ctx.claim(("CH28", False), 20, 64))
        self.assertTrue(ctx.claim(("CH28", True), 20, 64))
        self.assertNotIn(None, ctx.groups)

    def test_short_prefixes_are_causal(self):
        """A short history must not be skipped when a longer one would emit earlier events."""
        bars = island_bars(True, 1)
        full = run(bars)
        prefix = run(bars[:33])
        self.assertEqual(events_for(prefix, "CH24"), events_for(full, "CH24"))
        assert_causal(self, chart_patterns, bars, cuts=[22, 32, 35, 40])


# ------------------------------------------------------------------ random walks
class RandomWalkTests(ChartTestCase):
    def test_contract_and_causality_on_random_walks(self):
        for seed in (3, 17, 29):
            bars = random_walk(3000, seed=seed)
            assert_contract(self, chart_patterns, bars)
            assert_causal(self, chart_patterns, bars)

    def test_contract_and_causality_with_quality_gaps_and_noisy_fixture(self):
        bars = random_walk(2000, seed=41)
        for k in range(55, 2000, 123):
            bars[k]["gap"] = True
        assert_contract(self, chart_patterns, bars)
        assert_causal(self, chart_patterns, bars)
        noisy = from_closes(np.interp(np.arange(91), [x for x, _ in DOUBLE], [y for _, y in DOUBLE]), spread=0.4, seed=5)
        assert_contract(self, chart_patterns, noisy)
        assert_causal(self, chart_patterns, noisy)

    def test_confirmations_link_to_setups_on_random_walk(self):
        result = run(random_walk(6000, seed=8))
        specs = {(d["pattern_id"], d["variant"], d["side"]): d for d in chart_patterns.specifications()}
        setups = {}
        for key, events in result.items():
            for e in events:
                if e["state"] == "setup":
                    setups[(key[0], e["episode"])] = e["signal_index"]
        for key, events in result.items():
            for e in events:
                if e["state"] == "confirmed":
                    if "setup" in specs[key]["states"] or key == ("CH11", "downside_resolution", "short"):
                        self.assertEqual(setups[(key[0], e["episode"])], e["detected_index"], key)
                        self.assertLessEqual(e["signal_index"] - e["detected_index"], 40)

    def test_speed_20k_bars(self):
        bars = random_walk(20000, seed=5)
        start = time.perf_counter()
        run(bars)
        self.assertLess(time.perf_counter() - start, 5.0)


if __name__ == "__main__":
    unittest.main()
