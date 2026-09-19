import time
import unittest

from test_detectors_support import make_bars, from_closes, random_walk, assert_contract, assert_causal, events_for
from market_scanner.pattern_research import price_action, common

W = 30  # warm-up bars: fixture bars start at index 30 (ATR and 20-close trend available)


def up_warm(n=W):
    return [(100 + 0.5 * k - 0.25, 100 + 0.5 * k + 0.5, 100 + 0.5 * k - 0.5, 100 + 0.5 * k) for k in range(n)]


def down_warm(n=W):
    return [(200 - 0.5 * k + 0.25, 200 - 0.5 * k + 0.5, 200 - 0.5 * k - 0.5, 200 - 0.5 * k) for k in range(n)]


def flat_warm(n=W):
    return [(100.0, 100.5, 99.5, 100.0)] * n


def mirror(rows, axis=200.0):
    return [(axis - o, axis - l, axis - h, axis - c) for (o, h, l, c) in rows]


def run(rows, gaps=()):
    return price_action.detect(make_bars(rows, gaps=gaps), "1D")


def one(tc, result, pid, variant, side, state):
    ev = events_for(result, pid, variant, side, state)
    tc.assertEqual(len(ev), 1, f"{pid}/{variant}/{side}/{state}: {ev}")
    return ev[0]


def at(result, pid, index, variant=None):
    return [e for e in events_for(result, pid, variant) if e["signal_index"] == index]


# ------------------------------------------------------------------ fixtures
TWEEZER_TOP = up_warm() + [
    (114.6, 116.0, 114.4, 115.8),    # 30 up candle, tests the high
    (115.8, 116.05, 114.9, 115.0),   # 31 high within tol, closes away (lower half)
    (115.0, 115.1, 113.5, 113.8),    # 32 closes below pair low - 0.12 ATR -> confirmed
]
INSIDE = flat_warm() + [
    (100.0, 102.0, 98.0, 100.5),     # 30 mother
    (100.0, 101.5, 98.5, 100.2),     # 31 first inside
    (100.0, 101.0, 99.0, 100.1),     # 32 inside 31 -> cluster + nested (same mother)
    (100.1, 101.2, 99.2, 100.5),     # 33 inside mother only
    (100.5, 103.0, 100.0, 102.6),    # 34 close > mother high + buffer -> break_up
    (102.6, 104.0, 102.5, 103.8),
    (103.8, 105.0, 103.6, 104.8),
]
FAKEY = flat_warm() + [
    (100.0, 102.0, 98.0, 100.5),     # 30 mother
    (100.2, 101.5, 98.5, 100.0),     # 31 inside
    (99.8, 100.5, 97.3, 97.6),       # 32 breach below mother low, closes outside
    (97.7, 99.5, 97.5, 99.0),        # 33 failure close back inside -> setup
    (99.0, 101.0, 98.8, 100.8),      # 34
    (100.8, 102.8, 100.6, 102.6),    # 35 close > mother high + buffer -> confirmed
]
OUTSIDE = flat_warm() + [
    (99.6, 101.5, 99.0, 101.3),      # 30 outside, close location 0.92
    (101.3, 102.0, 101.0, 101.8),    # 31 confirms above 101.5 + 0.12 ATR
]
PIN = flat_warm() + [
    (100.3, 100.5, 98.0, 100.4),     # 30 lower shadow 92%, upper 4%
    (100.4, 101.0, 100.2, 100.9),    # 31 confirms above 100.5 + 0.12 ATR
]
NR = flat_warm() + [
    (100.0, 100.25, 99.75, 100.125),  # 30 range 0.5, inside bar 29 -> NR4, NR7, inside-NR4
    (100.1, 100.9, 100.0, 100.8),     # 31 close > 100.25 + 0.12 ATR -> break_up
]
WINDOW = flat_warm() + [
    (101.2, 101.8, 101.0, 101.6),    # 30 low > prior high 100.5
    (101.6, 102.2, 101.5, 102.0),    # 31 confirms above 101.8 + 0.12 ATR
]
GAP_REV = flat_warm() + [
    (98.8, 100.3, 98.6, 100.1),      # 30 opens below prior low, closes back inside, upper half
    (100.1, 101.0, 100.0, 100.9),    # 31 confirms above max(100.5, 100.3) + 0.12 ATR
]


class PA01Tweezer(unittest.TestCase):
    def test_top_positive_and_confirmation(self):
        r = run(TWEEZER_TOP)
        s = one(self, r, "PA01", "top", "short", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["formation_start_index"], s["direction"]),
                         (31, 30, 30, "bearish"))
        self.assertEqual(s["geometry"]["prior_trend"], "up")
        c = one(self, r, "PA01", "top", "short", "confirmed")
        self.assertEqual((c["signal_index"], c["episode"], c["detected_index"], c["confirmed_index"]), (32, 30, 31, 32))
        self.assertEqual(events_for(r, "PA01", "bottom"), [])

    def test_bottom_mirror(self):
        r = run(mirror(TWEEZER_TOP))
        s = one(self, r, "PA01", "bottom", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["direction"]), (31, 30, "bullish"))
        self.assertEqual(one(self, r, "PA01", "bottom", "long", "confirmed")["signal_index"], 32)

    def test_beyond_tolerance_and_no_trend(self):
        rows = list(TWEEZER_TOP)
        rows[31] = (115.8, 116.2, 114.9, 115.0)  # 0.2 > 0.1 ATR
        self.assertEqual(events_for(run(rows), "PA01"), [])
        rows = flat_warm() + TWEEZER_TOP[30:]      # no preceding advance
        rows = [(r[0] - 15, r[1] - 15, r[2] - 15, r[3] - 15) if i >= W else r for i, r in enumerate(rows)]
        self.assertEqual(events_for(run(rows), "PA01"), [])

    def test_close_not_away(self):
        rows = list(TWEEZER_TOP)
        rows[31] = (115.0, 116.05, 114.9, 115.9)  # closes in upper half
        self.assertEqual(events_for(run(rows), "PA01", "top"), [])

    def test_gap_flag(self):
        self.assertEqual(events_for(run(TWEEZER_TOP, gaps=(31,)), "PA01"), [])
        self.assertEqual(events_for(run(TWEEZER_TOP, gaps=(30,)), "PA01"), [])

    def test_adjacent_pairs_are_distinct_episodes(self):
        rows = TWEEZER_TOP[:32] + [(115.0, 116.08, 114.6, 114.8)]
        ev = events_for(run(rows), "PA01", "top", "short", "setup")
        self.assertEqual([(e["signal_index"], e["episode"]) for e in ev], [(31, 30), (32, 31)])

    def test_contract_and_causal(self):
        for rows in (TWEEZER_TOP, mirror(TWEEZER_TOP)):
            assert_contract(self, price_action, make_bars(rows))
            assert_causal(self, price_action, make_bars(rows))


class PA02InsideBar(unittest.TestCase):
    def test_nested_cluster_share_mother(self):
        r = run(INSIDE)
        for var, idx in (("first", 31), ("cluster", 32), ("nested", 32)):
            for brk, side in (("break_up", "long"), ("break_down", "short")):
                s = one(self, r, "PA02", f"{var}_{brk}", side, "setup")
                self.assertEqual((s["signal_index"], s["episode"], s["formation_start_index"], s["direction"]),
                                 (idx, 30, 30, "neutral"))
                self.assertEqual(s["alias_group"], "inside_range")
            c = one(self, r, "PA02", f"{var}_break_up", "long", "confirmed")
            self.assertEqual((c["signal_index"], c["episode"], c["detected_index"], c["direction"]),
                             (34, 30, idx, "bullish"))
            self.assertEqual(events_for(r, "PA02", f"{var}_break_down", "short", "confirmed"), [])
        self.assertEqual({e["episode"] for e in events_for(r, "PA02")}, {30})

    def test_break_down_mirror(self):
        r = run(mirror(INSIDE))
        c = one(self, r, "PA02", "first_break_down", "short", "confirmed")
        self.assertEqual((c["signal_index"], c["detected_index"], c["direction"]), (34, 31, "bearish"))
        self.assertEqual(events_for(r, "PA02", "first_break_up", "long", "confirmed"), [])

    def test_near_misses(self):
        rows = list(INSIDE)
        rows[31] = (100.0, 102.0, 98.0, 100.2)  # identical range: no strict edge
        rows[32] = (100.0, 102.5, 99.0, 100.1)  # not inside
        self.assertEqual([e for e in events_for(run(rows[:33]), "PA02") if e["signal_index"] >= W], [])
        rows = list(INSIDE[:31]) + [(98.5, 102.2, 98.2, 101.5)]  # body inside, range not (harami-only)
        self.assertEqual(events_for(run(rows), "PA02"), [])

    def test_breach_without_close_expires(self):
        rows = INSIDE[:34] + [(100.5, 102.4, 100.0, 101.9), (101.9, 103.5, 101.5, 103.2)]
        r = run(rows)
        self.assertEqual(events_for(r, "PA02", state="confirmed"), [])

    def test_gap_flag(self):
        r = run(INSIDE, gaps=(31,))
        self.assertEqual(events_for(r, "PA02"), [])
        r = run(INSIDE, gaps=(33,))  # cluster cut: confirmation cannot span the gap
        self.assertEqual(events_for(r, "PA02", state="confirmed"), [])
        self.assertEqual(len(events_for(r, "PA02", state="setup")), 6)

    def test_contract_and_causal(self):
        for rows in (INSIDE, mirror(INSIDE)):
            assert_contract(self, price_action, make_bars(rows))
            assert_causal(self, price_action, make_bars(rows))


class PA03OutsideBar(unittest.TestCase):
    def test_bullish_close(self):
        r = run(OUTSIDE)
        s = one(self, r, "PA03", "bullish_close", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["formation_start_index"]), (30, 29, 29))
        self.assertIn("close_top_quarter", s["quality_tags"])
        c = one(self, r, "PA03", "bullish_close", "long", "confirmed")
        self.assertEqual((c["signal_index"], c["detected_index"], c["episode"]), (31, 30, 29))

    def test_bearish_mirror(self):
        r = run(mirror(OUTSIDE))
        self.assertEqual(one(self, r, "PA03", "bearish_close", "short", "setup")["signal_index"], 30)
        self.assertEqual(one(self, r, "PA03", "bearish_close", "short", "confirmed")["signal_index"], 31)

    def test_near_misses(self):
        rows = list(OUTSIDE)
        rows[30] = (99.6, 101.5, 99.0, 100.8)  # close location 0.72 -> middle, not emitted
        self.assertEqual(events_for(run(rows), "PA03"), [])
        rows[30] = (99.6, 100.5, 99.0, 100.4)  # equal high: not outside
        self.assertEqual(events_for(run(rows), "PA03"), [])

    def test_gap_flag(self):
        self.assertEqual(events_for(run(OUTSIDE, gaps=(30,)), "PA03"), [])

    def test_contract_and_causal(self):
        assert_contract(self, price_action, make_bars(OUTSIDE))
        assert_causal(self, price_action, make_bars(OUTSIDE))


class PA04PinBar(unittest.TestCase):
    def test_lower_pin_no_context_in_flat(self):
        r = run(PIN)
        s = one(self, r, "PA04", "lower_shadow", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["alias_group"]), (30, 30, "lower_shadow_rejection"))
        self.assertEqual(s["geometry"]["prior_trend"], "neutral")
        self.assertEqual(one(self, r, "PA04", "lower_shadow", "long", "confirmed")["signal_index"], 31)
        self.assertEqual(events_for(r, "PA04", "lower_shadow_context"), [])

    def test_context_variant_after_decline_and_mirror(self):
        rows = down_warm() + [(185.3, 185.5, 183.0, 185.4), (185.4, 186.0, 185.2, 185.9)]
        r = run(rows)
        self.assertEqual(one(self, r, "PA04", "lower_shadow_context", "long", "setup")["signal_index"], 30)
        self.assertEqual(one(self, r, "PA04", "lower_shadow_context", "long", "confirmed")["signal_index"], 31)
        m = run(mirror(rows))
        self.assertEqual(one(self, m, "PA04", "upper_shadow_context", "short", "setup")["signal_index"], 30)
        self.assertEqual(one(self, m, "PA04", "upper_shadow", "short", "confirmed")["signal_index"], 31)

    def test_opposite_shadow_16_percent(self):
        rows = list(PIN)
        rows[30] = (100.0, 100.5, 98.0, 100.1)  # upper 0.4 / 2.5 = 16%
        self.assertEqual(events_for(run(rows), "PA04"), [])
        rows[30] = (100.0, 100.0, 100.0, 100.0)  # zero range is excluded
        self.assertEqual([e for e in events_for(run(rows), "PA04") if e["signal_index"] == 30], [])

    def test_gap_flag_and_adjacent(self):
        self.assertEqual(events_for(run(PIN, gaps=(30,)), "PA04"), [])
        rows = PIN[:31] + [PIN[30]]
        ev = events_for(run(rows), "PA04", "lower_shadow", "long", "setup")
        self.assertEqual([(e["signal_index"], e["episode"]) for e in ev], [(30, 30), (31, 31)])

    def test_contract_and_causal(self):
        assert_contract(self, price_action, make_bars(PIN))
        assert_causal(self, price_action, make_bars(PIN))


class PA05NarrowRange(unittest.TestCase):
    def test_nr4_nr7_inside(self):
        r = run(NR)
        for var, ep in (("nr4", 27), ("nr7", 24), ("inside_nr4", 27)):
            for brk, side in (("break_up", "long"), ("break_down", "short")):
                s = one(self, r, "PA05", f"{var}_{brk}", side, "setup")
                self.assertEqual((s["signal_index"], s["episode"], s["direction"]), (30, ep, "neutral"))
            c = one(self, r, "PA05", f"{var}_break_up", "long", "confirmed")
            self.assertEqual((c["signal_index"], c["detected_index"], c["episode"], c["direction"]),
                             (31, 30, ep, "bullish"))
            self.assertEqual(events_for(r, "PA05", f"{var}_break_down", "short", "confirmed"), [])

    def test_break_down_first_close_decides(self):
        rows = NR[:31] + [(100.0, 100.1, 99.4, 99.5), (99.5, 101.0, 99.4, 100.9)]
        r = run(rows)
        self.assertEqual(one(self, r, "PA05", "nr4_break_down", "short", "confirmed")["signal_index"], 31)
        self.assertEqual(events_for(r, "PA05", "nr4_break_up", "long", "confirmed"), [])

    def test_tie_and_not_inside(self):
        rows = list(NR)
        rows[30] = (100.25, 100.75, 99.75, 100.5)  # range 1.0 equals prior minimum -> tie, not emitted
        self.assertEqual(at(run(rows), "PA05", 30), [])
        rows[30] = (100.25, 100.625, 100.125, 100.5)  # range 0.5 but high above prior high: NR only, not inside
        r = run(rows)
        self.assertEqual(len(at(r, "PA05", 30, "nr4_break_up")), 1)
        self.assertEqual(at(r, "PA05", 30, "inside_nr4_break_up"), [])

    def test_gap_flag(self):
        self.assertEqual(at(run(NR, gaps=(30,)), "PA05", 30), [])
        self.assertEqual(at(run(NR, gaps=(28,)), "PA05", 30), [])

    def test_contract_and_causal(self):
        assert_contract(self, price_action, make_bars(NR))
        assert_causal(self, price_action, make_bars(NR))


class PA06Fakey(unittest.TestCase):
    def test_bull_fakey(self):
        r = run(FAKEY)
        s = one(self, r, "PA06", "bull", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["formation_start_index"]), (33, 30, 30))
        self.assertEqual((s["geometry"]["breach_index"], s["geometry"]["failure_index"]), (32, 33))
        self.assertEqual(s["alias_group"], "inside_bar_trap")
        c = one(self, r, "PA06", "bull", "long", "confirmed")
        self.assertEqual((c["signal_index"], c["detected_index"], c["episode"]), (35, 33, 30))
        self.assertEqual((c["geometry"]["failure_index"], c["geometry"]["confirmation_index"]), (33, 35))
        self.assertEqual(events_for(r, "PA06", "bear"), [])

    def test_bear_mirror(self):
        r = run(mirror(FAKEY))
        self.assertEqual(one(self, r, "PA06", "bear", "short", "setup")["signal_index"], 33)
        self.assertEqual(one(self, r, "PA06", "bear", "short", "confirmed")["signal_index"], 35)

    def test_failure_too_late_and_two_sided_breach(self):
        rows = FAKEY[:33] + [(97.6, 97.9, 97.0, 97.4), (97.4, 97.9, 97.1, 97.5), (97.5, 99.5, 97.4, 99.0)]
        self.assertEqual(events_for(run(rows), "PA06"), [])
        rows = list(FAKEY)
        rows[32] = (99.8, 102.5, 97.3, 99.6)  # breaches both mother edges
        self.assertEqual(events_for(run(rows), "PA06"), [])

    def test_invalidated_before_confirmation(self):
        rows = FAKEY[:34] + [(99.0, 99.2, 97.0, 97.5), (97.5, 102.8, 97.4, 102.6)]
        r = run(rows)
        # mother 30: setup at 33, then bar 34 closes back below mother low -> invalidated, never confirmed.
        # (bar 33 inside bar 32 forms an unrelated later mother-32 fakey; it stays a distinct episode.)
        ev30 = [e for e in events_for(r, "PA06", "bull", "long") if e["episode"] == 30]
        self.assertEqual([(e["state"], e["signal_index"]) for e in ev30], [("setup", 33)])

    def test_gap_flag(self):
        self.assertEqual(events_for(run(FAKEY, gaps=(32,)), "PA06"), [])
        r = run(FAKEY, gaps=(34,))
        self.assertEqual(len(events_for(r, "PA06", state="setup")), 1)
        self.assertEqual(events_for(r, "PA06", state="confirmed"), [])

    def test_contract_and_causal(self):
        for rows in (FAKEY, mirror(FAKEY)):
            assert_contract(self, price_action, make_bars(rows))
            assert_causal(self, price_action, make_bars(rows))


class PA07Window(unittest.TestCase):
    def test_rising(self):
        r = run(WINDOW)
        s = one(self, r, "PA07", "rising", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["formation_start_index"]), (30, 29, 29))
        c = one(self, r, "PA07", "rising", "long", "confirmed")
        self.assertEqual((c["signal_index"], c["detected_index"]), (31, 30))

    def test_falling_mirror(self):
        r = run(mirror(WINDOW))
        self.assertEqual(one(self, r, "PA07", "falling", "short", "setup")["signal_index"], 30)
        self.assertEqual(one(self, r, "PA07", "falling", "short", "confirmed")["signal_index"], 31)

    def test_overlap_and_gap_flag(self):
        rows = list(WINDOW)
        rows[30] = (101.2, 101.8, 100.4, 101.6)  # low below prior high: overlapping ranges
        self.assertEqual(events_for(run(rows), "PA07"), [])
        rows[30] = (110.0, 111.0, 109.0, 110.5)  # real price gap on a data-quality-gap bar
        self.assertEqual(events_for(run(rows, gaps=(30,)), "PA07"), [])
        self.assertEqual(events_for(run(WINDOW, gaps=(30,)), "PA07"), [])

    def test_adjacent_windows(self):
        rows = WINDOW[:31] + [(102.1, 102.6, 102.0, 102.5)]
        ev = events_for(run(rows), "PA07", "rising", "long", "setup")
        self.assertEqual([(e["signal_index"], e["episode"]) for e in ev], [(30, 29), (31, 30)])

    def test_contract_and_causal(self):
        assert_contract(self, price_action, make_bars(WINDOW))
        assert_causal(self, price_action, make_bars(WINDOW))


class PA08GapReversal(unittest.TestCase):
    def test_bullish(self):
        r = run(GAP_REV)
        s = one(self, r, "PA08", "bullish", "long", "setup")
        self.assertEqual((s["signal_index"], s["episode"], s["direction"]), (30, 29, "bullish"))
        c = one(self, r, "PA08", "bullish", "long", "confirmed")
        self.assertEqual((c["signal_index"], c["detected_index"], c["episode"]), (31, 30, 29))

    def test_bearish_mirror(self):
        r = run(mirror(GAP_REV))
        self.assertEqual(one(self, r, "PA08", "bearish", "short", "setup")["signal_index"], 30)
        self.assertEqual(one(self, r, "PA08", "bearish", "short", "confirmed")["signal_index"], 31)

    def test_near_misses(self):
        rows = list(GAP_REV)
        rows[30] = (98.8, 100.8, 98.6, 100.7)  # closes above prior high: outside the prior range
        self.assertEqual(events_for(run(rows), "PA08"), [])
        rows[30] = (98.8, 100.3, 98.6, 99.4)   # stays below prior low
        self.assertEqual(events_for(run(rows), "PA08"), [])
        rows[30] = (99.6, 100.3, 98.6, 100.1)  # open inside prior range: no opening gap
        self.assertEqual(events_for(run(rows), "PA08"), [])
        rows[30] = (98.8, 101.4, 98.6, 99.9)   # back inside but lower half of own range
        self.assertEqual(events_for(run(rows), "PA08"), [])

    def test_gap_flag(self):
        self.assertEqual(events_for(run(GAP_REV, gaps=(30,)), "PA08"), [])

    def test_contract_and_causal(self):
        assert_contract(self, price_action, make_bars(GAP_REV))
        assert_causal(self, price_action, make_bars(GAP_REV))


class RandomWalkInvariants(unittest.TestCase):
    def test_specifications(self):
        specs = price_action.specifications()
        self.assertEqual(len(specs), 26)
        self.assertEqual({d["pattern_id"] for d in specs}, {f"PA0{i}" for i in range(1, 9)})
        for d in specs:
            self.assertEqual(d["family"], "price_action")
            self.assertEqual(d["states"], ["setup", "confirmed"])
            self.assertGreater(len(d["definition"]), 60)
        r = price_action.detect([], "1D")
        self.assertEqual(set(r), set(common.spec_keys(specs)))

    def test_contract_causal_identity_and_gaps(self):
        for seed in (1, 2, 3):
            bars = random_walk(3000, seed=seed)
            for g in (400, 401, 1500, 2222):
                bars[g]["gap"] = True
            result = assert_contract(self, price_action, bars)
            assert_causal(self, price_action, bars, cuts=[700, 1501, 2223, 2995], seed=seed)
            s = common.prepare(bars, "1D")
            total = 0
            for key, events in result.items():
                setups = {e["episode"]: e for e in events if e["state"] == "setup"}
                for e in events:
                    total += 1
                    self.assertTrue(s.clean(e["formation_start_index"], e["signal_index"]), (key, e))
                    self.assertTrue(0.0 <= e["score"] <= 1.0)
                    if e["state"] == "confirmed":
                        su = setups.get(e["episode"])
                        self.assertIsNotNone(su, (key, e))
                        self.assertEqual(e["detected_index"], su["signal_index"])
                        self.assertGreater(e["signal_index"], su["signal_index"])
                        self.assertLessEqual(e["signal_index"] - su["signal_index"], 3)
                        self.assertIn(e["direction"], ("bullish", "bearish"))
                    if key[0] in ("PA07", "PA08"):
                        self.assertFalse(bars[e["signal_index"]]["gap"])
            for pid in ("PA02", "PA03", "PA04", "PA05"):
                self.assertTrue(events_for(result, pid), f"seed {seed}: no {pid} events on random walk")
            self.assertGreater(total, 100)

    def test_timing_20k(self):
        bars = random_walk(20000, seed=11)
        t0 = time.perf_counter()
        price_action.detect(bars, "1D")
        elapsed = time.perf_counter() - t0
        print(f"\nPA detect on 20,000 bars: {elapsed:.3f}s")
        self.assertLess(elapsed, 5.0)


if __name__ == "__main__":
    unittest.main()
