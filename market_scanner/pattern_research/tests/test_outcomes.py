"""Forward-outcome evidence: excursion arithmetic, point-in-time gates, honest selection."""
from datetime import date, timedelta
from pathlib import Path
import hashlib
import json
import tempfile
import unittest

import numpy as np

from market_scanner.pattern_research import outcomes, outcomes_cli
from market_scanner.pattern_research.outcomes import (
    COST_PCT, HORIZONS, flatten_horizon, horizon_grid, outcome_cell, sample_label,
)


# ------------------------------------------------------------------ fixtures
def bar(day, open_, high, low, close, gap=False):
    return dict(time=day + ' 09:15:00', end=day + ' 15:30:00', open=open_, high=high,
                low=low, close=close, volume=100., gap=gap)


def flat(n=40, price=100., start=date(2020, 1, 1)):
    """Perfectly flat daily bars; individual bars are overwritten by the tests."""
    return [bar((start + timedelta(days=i)).isoformat(), price, price, price, price)
            for i in range(n)]


def put(bars, i, open_, close, high=None, low=None, gap=False):
    day = bars[i]['time'][:10]
    high = max(open_, close) if high is None else high
    low = min(open_, close) if low is None else low
    bars[i] = bar(day, open_, high, low, close, gap)


def event(i, state='setup', episode=None, score=1.):
    return dict(signal_index=i, episode=i if episode is None else episode, state=state,
                atr=2., score=score, direction='bullish', pattern_start='2020-01-01 09:15:00')


def cell(bars, events, timeframe='1D', side='long', state='setup', horizon=None):
    return outcome_cell(bars, events, timeframe, side, state, horizon)


# ------------------------------------------------------------------ unit tests
class GridTests(unittest.TestCase):
    def test_default_grid_matches_the_contract(self):
        self.assertEqual(HORIZONS, {'1H': 30, '4H': 20, '1D': 10, '1W': 8})
        self.assertEqual([horizon_grid(t) for t in ('1H', '4H', '1D', '1W')], [30, 20, 10, 8])

    def test_grid_is_configurable_and_validated(self):
        self.assertEqual(horizon_grid('1D', {'1D': 15}), 15)
        self.assertEqual(horizon_grid('1D', {'1W': 15}), 10)
        self.assertEqual(horizon_grid('1W', 12), 12)
        with self.assertRaises(ValueError):
            horizon_grid('1D', {'1D': 0})
        with self.assertRaises(ValueError):
            horizon_grid('30m')

    def test_sample_labels_never_call_a_thin_sample_adequate(self):
        self.assertEqual([sample_label(n) for n in (0, 1, 9, 10, 29, 30, 99, 100)],
                         ['none', 'very_small', 'very_small', 'small', 'small',
                          'moderate', 'moderate', 'adequate'])

    def test_flatten_horizon_finds_the_plateau(self):
        self.assertEqual(flatten_horizon([1., 2., 3., 3.05, 3.05], 0.90), 3)
        self.assertEqual(flatten_horizon([1., 2., 3., 3.05, 3.05], 0.99), 4)
        self.assertIsNone(flatten_horizon([None, None]))
        self.assertIsNone(flatten_horizon([0., 0., 0.]))


class PathTests(unittest.TestCase):
    def test_next_open_entry_cost_and_horizon_indexing(self):
        bars = flat(20)
        put(bars, 0, 50., 50.)            # signal bar: must NOT be the entry price
        put(bars, 1, 100., 101.)          # entry bar: open 100 -> close 101 at h=1
        put(bars, 2, 101., 103.)          # h=2 closes at 103
        result = cell(bars, [event(0)])
        record = result['occurrence_records'][0]
        self.assertEqual(record['entry_index'], 1)
        self.assertEqual(record['entry_price'], 100.)
        self.assertEqual(record['entry_time'], bars[1]['time'])
        self.assertEqual(record['signal_time'], bars[0]['end'])
        self.assertEqual(record['cost_pct'], COST_PCT)
        self.assertAlmostEqual(record['net_return_pct'][0], 1.0 - COST_PCT)
        self.assertAlmostEqual(record['net_return_pct'][1], 3.0 - COST_PCT)
        self.assertAlmostEqual(record['net_return_pct'][2], -COST_PCT)   # back to flat 100
        self.assertEqual(result['horizons'][0]['h'], 1)
        self.assertAlmostEqual(result['horizons'][1]['mean_net_return_pct'], 3.0 - COST_PCT)

    def test_mfe_and_mae_use_highs_and_lows_and_are_cumulative(self):
        bars = flat(20)
        put(bars, 1, 100., 100., high=102., low=99.)    # h=1: +2 / -1
        put(bars, 2, 100., 100., high=101., low=96.)    # h=2: +2 / -4
        put(bars, 3, 100., 100., high=105., low=98.)    # h=3: +5 / -4
        record = cell(bars, [event(0)])['occurrence_records'][0]
        self.assertEqual(record['mfe_pct'][:3], [2., 2., 5.])
        self.assertEqual(record['mae_pct'][:3], [1., 4., 4.])
        # Excursions are gross price moves: costs are not deducted from them.
        self.assertNotAlmostEqual(record['mfe_pct'][0], 2. - COST_PCT)
        self.assertTrue(all(v >= 0 for v in record['mfe_pct'] if v is not None))
        self.assertTrue(all(v >= 0 for v in record['mae_pct'] if v is not None))

    def test_short_side_sign_conventions_mirror_the_long_side(self):
        bars = flat(20)
        put(bars, 1, 100., 98., high=102., low=96.)
        long_record = cell(bars, [event(0)], side='long')['occurrence_records'][0]
        short_record = cell(bars, [event(0)], side='short')['occurrence_records'][0]
        self.assertAlmostEqual(long_record['net_return_pct'][0], -2. - COST_PCT)
        self.assertAlmostEqual(short_record['net_return_pct'][0], 2. - COST_PCT)
        self.assertEqual((long_record['mfe_pct'][0], long_record['mae_pct'][0]), (2., 4.))
        self.assertEqual((short_record['mfe_pct'][0], short_record['mae_pct'][0]), (4., 2.))

    def test_availability_rule_blanks_horizons_that_do_not_exist_yet(self):
        bars = flat(14)                      # indices 0..13
        result = cell(bars, [event(10)])     # entry 11, only 3 bars exist (11,12,13)
        record = result['occurrence_records'][0]
        self.assertEqual(record['available_horizons'], 3)
        self.assertFalse(record['available_full_grid'])
        self.assertEqual(record['available_from_index'], 11 + 10 - 1)
        self.assertIsNone(record['available_from_time'])
        self.assertEqual(len(record['net_return_pct']), 10)
        self.assertTrue(all(v is not None for v in record['net_return_pct'][:3]))
        self.assertTrue(all(v is None for v in record['net_return_pct'][3:]))
        self.assertTrue(all(v is None for v in record['mfe_pct'][3:]))
        self.assertEqual([row['n'] for row in result['horizons']], [1, 1, 1] + [0] * 7)
        self.assertEqual(result['horizons'][5]['sample_label'], 'none')

    def test_no_look_ahead_truncating_the_future_cannot_change_a_past_record(self):
        bars = flat(40)
        for i in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
            put(bars, i, 100. + i, 100. + i, high=101. + i, low=99. + i)
        full = cell(bars, [event(0)])['occurrence_records'][0]
        truncated = cell(bars[:11], [event(0)])['occurrence_records'][0]
        for field in ('net_return_pct', 'mfe_pct', 'mae_pct'):
            self.assertEqual(full[field], truncated[field])
        self.assertTrue(truncated['available_full_grid'])
        # A shorter history must blank, never invent, the horizons it cannot see.
        shorter = cell(bars[:6], [event(0)])['occurrence_records'][0]
        self.assertEqual(shorter['net_return_pct'][:5], full['net_return_pct'][:5])
        self.assertTrue(all(v is None for v in shorter['net_return_pct'][5:]))

    def test_available_from_index_marks_the_full_grid(self):
        bars = flat(30)
        record = cell(bars, [event(0)])['occurrence_records'][0]
        self.assertEqual(record['available_from_index'], 10)
        self.assertEqual(record['available_from_time'], bars[10]['end'])
        self.assertTrue(record['available_full_grid'])


class GapTests(unittest.TestCase):
    def test_signal_or_entry_gap_disqualifies_the_occurrence(self):
        for flagged in (0, 1):
            bars = flat(20)
            bars[flagged]['gap'] = True
            result = cell(bars, [event(0)])
            self.assertEqual(result['occurrence_records'], [])
            self.assertEqual(result['exclusions'], {'signal_or_entry_gap': 1})
            self.assertEqual(result['admitted'], 0)

    def test_signal_on_the_last_bar_has_no_entry(self):
        bars = flat(20)
        result = cell(bars, [event(19)])
        self.assertEqual(result['exclusions'], {'no_entry_bar': 1})

    def test_gap_inside_the_hold_exits_at_that_open_and_flags_unknown_excursions(self):
        bars = flat(20)
        put(bars, 1, 100., 101., high=103., low=99.)      # +3 / -1 excursion
        put(bars, 2, 101., 101.)
        put(bars, 3, 104., 120., high=130., low=104., gap=True)   # gap: exit at open 104
        record = cell(bars, [event(0)])['occurrence_records'][0]
        # Bar 3 is entry+2, so a three-bar hold (h=3) is already the gap exit.
        self.assertEqual(record['gap_exit_h'], 3)
        self.assertFalse(record['excursion_bounded'])
        self.assertAlmostEqual(record['net_return_pct'][0], 1.0 - COST_PCT)   # h=1, pre-gap close
        self.assertAlmostEqual(record['net_return_pct'][1], 1.0 - COST_PCT)   # h=2, pre-gap close
        for h in range(2, 10):                                                # h=3..10 frozen
            self.assertAlmostEqual(record['net_return_pct'][h], 4.0 - COST_PCT)
        # The gap bar's 130 high and 120 close are never credited: only its open is observed.
        self.assertEqual(record['mfe_pct'][1], 3.)
        self.assertEqual(record['mfe_pct'][2], 4.)
        self.assertEqual(record['mfe_pct'][9], 4.)
        self.assertEqual(record['mae_pct'][9], 1.)
        # Every horizon at/after the gap settles on the same exit bar.
        self.assertEqual(record['exit_index'], [1, 2] + [3] * 8)


# ------------------------------------------------- fold-based horizon selection
def selection_series(n=2200, start=date(2015, 1, 1), flip=date(2018, 1, 1),
                     train_move=0.004, test_move=-0.006, spacing=12, first=30):
    """Daily bars where a five-bar hold earns +2% before ``flip`` and -3% after it."""
    bars = flat(n, start=start)
    events = []
    for signal in range(first, n - 20, spacing):
        entry = signal + 1
        move = train_move if date.fromisoformat(bars[entry]['time'][:10]) < flip else test_move
        put(bars, entry, 100., 100. * (1 + move))
        for k in range(2, 6):
            level = 100. * (1 + move * k)
            put(bars, entry + k - 1, level, level)
        settled = 100. * (1 + move * 5)
        for k in range(6, 12):
            put(bars, entry + k - 1, settled, settled)
        events.append(event(signal))
    return bars, events


class SelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bars, events = selection_series()
        cls.result = cell(bars, events)
        cls.selection = cls.result['selection']

    def test_folds_reuse_the_rolling_36_month_train_6_month_test_protocol(self):
        folds = self.selection['folds']
        self.assertGreater(len(folds), 4)
        self.assertEqual(folds[0]['train_start'], '2015-01-01')
        self.assertEqual(folds[0]['test_start'], '2018-01-01')
        self.assertEqual(folds[1]['test_start'], '2018-07-01')
        self.assertEqual(folds[0]['status'], 'frozen_for_test')

    def test_horizon_is_selected_on_training_folds_only(self):
        self.assertEqual(self.selection['folds'][0]['horizon'], 5)
        self.assertGreaterEqual(self.selection['folds'][0]['training_stats']['n'], 20)
        self.assertAlmostEqual(self.selection['folds'][0]['training_stats']['expectancy_pct'],
                               2.0 - COST_PCT, places=6)

    def test_in_sample_best_is_never_reported_as_the_out_of_sample_number(self):
        in_sample = self.selection['in_sample']
        out_of_sample = self.selection['out_of_sample']
        self.assertGreater(in_sample['stats']['expectancy_pct'], 0)
        self.assertLess(out_of_sample['stats']['expectancy_pct'], 0)
        self.assertNotAlmostEqual(in_sample['stats']['expectancy_pct'],
                                  out_of_sample['stats']['expectancy_pct'])
        # The two blocks are separate and the in-sample one says so.
        self.assertIn('never_an_expected_return', in_sample['scope'])
        self.assertEqual(out_of_sample['scope'],
                         'test_folds_at_the_training_selected_horizon')
        self.assertAlmostEqual(out_of_sample['stats']['expectancy_pct'], -3.0 - COST_PCT, places=6)

    def test_out_of_sample_sample_size_is_labelled(self):
        stats = self.selection['out_of_sample']['stats']
        self.assertEqual(self.selection['out_of_sample']['sample_label'], sample_label(stats['n']))
        self.assertIn(self.selection['status'],
                      ('tested', 'small_out_of_sample_sample', 'insufficient'))

    def test_too_few_trades_reports_insufficient_rather_than_a_pick(self):
        bars = flat(40)
        result = cell(bars, [event(0), event(12)])
        self.assertEqual(result['selection']['status'], 'insufficient_history')
        self.assertIsNone(result['selection']['horizon'])
        bars, events = selection_series(n=2200)
        thin = cell(bars, events[:5])
        self.assertEqual(thin['selection']['status'], 'insufficient')
        self.assertIsNone(thin['selection']['horizon'])

    def test_aggregates_cover_the_whole_grid_with_distribution_and_excursions(self):
        rows = self.result['horizons']
        self.assertEqual([r['h'] for r in rows], list(range(1, 11)))
        row = rows[4]
        self.assertEqual(sorted(row['percentiles']), ['p10', 'p25', 'p50', 'p75', 'p90'])
        self.assertIsNotNone(row['median_mfe_pct'])
        self.assertIsNotNone(row['win_rate_pct'])
        self.assertIsNotNone(row['median_span_calendar_days'])
        self.assertIsNotNone(self.result['mfe_terminal_median_pct'])
        json.dumps(self.result, allow_nan=False)

    def test_median_mfe_curve_flattening_is_measured_from_the_data(self):
        bars = flat(60)
        events = []
        for signal in (0, 12, 24, 36):
            for k in range(1, 11):
                level = 100. * (1 + 0.0075 * min(k, 4))
                put(bars, signal + k, 100. if k == 1 else level, level, high=level)
            put(bars, signal + 1, 100., 100.75, high=100.75)
            events.append(event(signal))
        result = cell(bars, events)
        curve = [row['median_mfe_pct'] for row in result['horizons']]
        self.assertAlmostEqual(curve[0], 0.75, places=4)
        self.assertAlmostEqual(curve[3], 3.0, places=4)
        self.assertAlmostEqual(curve[9], 3.0, places=4)
        self.assertEqual(result['mfe_flatten_h_90pct'], 4)
        self.assertEqual(result['mfe_flatten_h_95pct'], 4)


# ------------------------------------------------------ statistics primitives
class StatisticsTests(unittest.TestCase):
    def test_student_t_p_value_matches_published_critical_values(self):
        # t = 2.262157 at 9 d.f. is the classic two-sided 5% critical value.
        values = np.zeros(10)
        values[:] = 1.0
        values[0] = 1.0 + 1e-9                      # degenerate guard, exercised below
        self.assertAlmostEqual(outcomes._betainc(4.5, 0.5, 9 / (9 + 2.262157 ** 2)), 0.05, places=5)
        self.assertAlmostEqual(outcomes._betainc(15.0, 0.5, 30 / (30 + 4.0)), 0.0546250, places=6)
        self.assertIsNone(outcomes.t_test_p_value([1.0]))
        self.assertEqual(outcomes.t_test_p_value([2.0, 2.0, 2.0]), 0.0)
        self.assertEqual(outcomes.t_test_p_value([0.0, 0.0, 0.0]), 1.0)

    def test_t_p_value_separates_a_real_mean_from_noise(self):
        rng = np.random.default_rng(11)
        self.assertLess(outcomes.t_test_p_value(rng.normal(0.5, 1.0, 200)), 0.001)
        self.assertGreater(outcomes.t_test_p_value(rng.normal(0.0, 1.0, 200)), 0.05)

    def test_bootstrap_p_value_is_block_based_and_never_exactly_zero(self):
        rng = np.random.default_rng(3)
        strong = outcomes.bootstrap_p_value(rng.normal(1.0, 0.5, 200), 4, replicates=200)
        noise = outcomes.bootstrap_p_value(rng.normal(0.0, 1.0, 200), 4, replicates=200)
        self.assertGreater(strong, 0.0)
        self.assertLess(strong, 0.02)
        self.assertGreater(noise, 0.05)
        self.assertIsNone(outcomes.bootstrap_p_value([1.0], 4))

    def test_stationary_bootstrap_indices_are_in_range_and_reproducible(self):
        first = outcomes._stationary_indices(50, 8, 10, 7)
        second = outcomes._stationary_indices(50, 8, 10, 7)
        self.assertEqual(first.shape, (8, 50))
        self.assertTrue((first >= 0).all() and (first < 50).all())
        self.assertTrue((first == second).all())
        # Blocks mean consecutive draws are usually consecutive indices.
        contiguous = np.mean(np.diff(first, axis=1) == 1)
        self.assertGreater(contiguous, 0.5)

    def test_benjamini_hochberg_is_monotone_and_matches_a_worked_example(self):
        p = [0.001, 0.008, 0.039, 0.041, 0.042, 0.06, 0.074, 0.205, 0.212, 0.216]
        q = outcomes.benjamini_hochberg(p)
        self.assertEqual([round(v, 4) for v in q],
                         [0.01, 0.04, 0.084, 0.084, 0.084, 0.1, 0.1057, 0.216, 0.216, 0.216])
        self.assertEqual(q, sorted(q))
        self.assertEqual(outcomes.benjamini_hochberg([None, 0.01, None]), [None, 0.01, None])
        self.assertEqual(outcomes.benjamini_hochberg([]), [])


# ------------------------------------------------ unconditional baseline (Lo/M/W)
class BaselineTests(unittest.TestCase):
    def test_eligible_entries_apply_the_same_gap_gate_as_the_conditional_sample(self):
        gaps = np.array([False, False, True, False, False], dtype=bool)
        # entry 2 has gaps[2]; entry 3 has gaps[2] as its signal bar. Both are out.
        self.assertEqual(list(outcomes.eligible_entries(gaps)), [1, 4])
        self.assertEqual(list(outcomes.eligible_entries(np.zeros(0, bool))), [])

    def test_baseline_uses_the_same_cost_model_and_a_no_edge_pattern_shows_no_edge(self):
        bars = flat(300)
        events = [event(i) for i in range(0, 260, 10)]
        result = cell(bars, events)
        base = result['baseline']
        self.assertEqual(base['status'], 'tested')
        row = base['horizons'][0]
        # Flat prices: every entry, conditional or not, loses exactly the round trip.
        self.assertAlmostEqual(row['baseline_mean_net_return_pct'], -COST_PCT)
        self.assertAlmostEqual(row['diff_mean_net_return_pct'], 0.0)
        self.assertFalse(row['diff_excludes_zero'])
        self.assertGreater(row['baseline_n'], row['conditional_n'])

    def test_a_pattern_with_a_real_one_bar_edge_beats_the_unconditional_baseline(self):
        bars = flat(1000)
        events = []
        for signal in range(30, 960, 25):
            put(bars, signal + 1, 100., 103.)       # the move happens only after the signal
            put(bars, signal + 2, 100., 100.)       # and reverts before the next bar
            events.append(event(signal))
        base = cell(bars, events)['baseline']
        self.assertEqual(base['status'], 'tested')
        first, second = base['horizons'][0], base['horizons'][1]
        # The pattern makes +3% gross. The unconditional sample contains those same
        # bars, so the measured difference is slightly attenuated - the honest number.
        self.assertGreater(first['diff_mean_net_return_pct'], 2.5)
        self.assertLess(first['diff_mean_net_return_pct'], 3.0)
        self.assertGreater(first['baseline_mean_net_return_pct'], -COST_PCT)
        self.assertTrue(first['diff_excludes_zero'])
        self.assertGreater(first['diff_ci95_low'], 0)
        # The edge is exactly one bar wide.  By h=2 it is gone, and in fact slightly
        # negative: an entry one bar EARLIER captured the same move, so the
        # unconditional sample already contains it.  The baseline exposes that.
        self.assertLess(second['diff_mean_net_return_pct'], 0.5)
        self.assertGreater(first['diff_mean_net_return_pct'],
                           10 * abs(second['diff_mean_net_return_pct']))

    def test_baseline_reports_the_window_it_actually_used_and_flags_mismatch(self):
        bars = flat(1200)
        late = [event(i) for i in range(1000, 1180, 6)]
        base = cell(bars, late)['baseline']
        self.assertLess(base['window_coverage'], outcomes.BASELINE_COVERAGE_MIN)
        self.assertTrue(base['window_mismatch'])
        self.assertEqual(len(base['occurrence_window']), 2)
        self.assertLessEqual(base['baseline_window'][0], base['occurrence_window'][0])
        self.assertGreaterEqual(base['baseline_window'][1], base['occurrence_window'][1])

    def test_thin_sample_gets_no_interval_rather_than_a_fake_one(self):
        bars = flat(300)
        base = cell(bars, [event(0), event(20)])['baseline']
        self.assertEqual(base['status'], 'insufficient_sample_for_interval')
        self.assertIsNone(base['horizons'][0]['diff_ci95_low'])
        self.assertIsNotNone(base['horizons'][0]['diff_mean_net_return_pct'])

    def test_baseline_can_be_skipped(self):
        bars = flat(300)
        result = outcome_cell(bars, [event(0)], '1D', 'long', 'setup', baseline=False)
        self.assertEqual(result['baseline']['status'], 'skipped')


# ------------------------------------------------------- year-by-year stability
def yearly_series(profitable_years, n=1600, start=date(2015, 1, 1), spacing=14, gain=0.06):
    bars = flat(n, start=start)
    events = []
    for signal in range(30, n - 30, spacing):
        entry = signal + 1
        year = (start + timedelta(days=entry)).year
        move = gain if year in profitable_years else 0.0
        for k in range(1, 12):
            level = 100. * (1 + move)
            put(bars, entry + k - 1, 100. if k == 1 else level, level)
        # Varying detector scores so the quality-tertile dimension has three buckets.
        events.append(event(signal, score=0.2 + 0.3 * (len(events) % 3)))
    return bars, events


class StabilityTests(unittest.TestCase):
    def test_year_table_reports_n_mean_and_win_rate_per_calendar_year(self):
        bars, events = yearly_series({2015, 2016, 2017, 2018})
        decay = cell(bars, events)['stability']
        self.assertIn(decay['horizon_source'], ('selected', 'declared_baseline'))
        self.assertTrue(1 <= decay['horizon'] <= 10)
        years = {row['year']: row for row in decay['all_history']['by_year']}
        self.assertEqual(sorted(years), [2015, 2016, 2017, 2018, 2019])
        for year in (2015, 2016, 2017, 2018):
            self.assertGreater(years[year]['n'], 0)
            self.assertAlmostEqual(years[year]['mean_net_return_pct'], 6.0 - COST_PCT, places=6)
            self.assertEqual(years[year]['win_rate_pct'], 100.0)
        self.assertAlmostEqual(years[2019]['mean_net_return_pct'], -COST_PCT, places=6)
        self.assertEqual(years[2019]['win_rate_pct'], 0.0)
        self.assertEqual(decay['positive_years'], 4)
        self.assertFalse(decay['edge_concentrated_in_best_years'])

    def test_an_edge_that_lives_in_one_year_is_flagged_as_concentrated(self):
        bars, events = yearly_series({2015})
        decay = cell(bars, events)['stability']
        self.assertTrue(decay['edge_concentrated_in_best_years'])
        self.assertEqual(decay['positive_years'], 1)
        self.assertEqual(decay['concentration_years'], outcomes.CONCENTRATION_YEARS)
        rows = {row['year']: row['mean_net_return_pct'] for row in decay['all_history']['by_year']}
        self.assertAlmostEqual(rows[2015], 6.0 - COST_PCT, places=6)
        self.assertAlmostEqual(rows[2017], -COST_PCT, places=6)

    def test_pre_post_split_shows_decay_that_an_average_would_hide(self):
        bars, events = yearly_series({2015, 2016})
        split = cell(bars, events)['stability']['all_history']['split']
        self.assertGreater(split['first_half']['expectancy_pct'], 0)
        self.assertAlmostEqual(split['second_half']['expectancy_pct'], -COST_PCT, places=6)
        self.assertRegex(split['pivot_date'], r'^\d{4}-\d{2}-\d{2}$')

    def test_without_a_selected_horizon_the_declared_baseline_hold_is_used(self):
        bars, events = yearly_series({2015}, n=400)       # under 36 months: no folds, no pick
        decay = cell(bars, events)['stability']
        self.assertEqual(decay['horizon_source'], 'declared_baseline')
        self.assertEqual(decay['horizon'], 10)            # evaluation.BASELINE['1D']

    def test_stability_uses_the_selected_horizon_when_the_folds_chose_one(self):
        bars, events = selection_series()
        decay = cell(bars, events)['stability']
        self.assertEqual(decay['horizon_source'], 'selected')
        self.assertEqual(decay['horizon'], 5)
        self.assertEqual(decay['all_history']['overlap'],
                         'non_overlapping_single_admission_stream')


# ------------------------------------------------------- multiple-testing control
class InferenceTests(unittest.TestCase):
    def test_p_value_is_the_conservative_maximum_of_the_two_tests(self):
        bars, events = selection_series()
        inference = cell(bars, events)['inference']
        self.assertEqual(inference['status'], 'tested')
        self.assertEqual(inference['method'], 'max(student_t, stationary_bootstrap)')
        self.assertAlmostEqual(inference['p_value'],
                               max(inference['p_value_t'], inference['p_value_bootstrap']))
        self.assertEqual(inference['scope'], 'out_of_sample_non_overlapping_trades')
        # q-values are a run-level quantity; the JSON must point at the column, not guess.
        self.assertIsNone(inference['q_value'])
        self.assertIn('cell_outcomes.oos_q_value', inference['fdr'])

    def test_no_p_value_without_enough_out_of_sample_trades(self):
        bars = flat(300)
        inference = cell(bars, [event(0)])['inference']
        self.assertEqual(inference['status'], 'insufficient_out_of_sample_trades')
        self.assertIsNone(inference['p_value'])

    def test_assumptions_state_how_overlap_is_handled(self):
        assumptions = cell(flat(40), [event(0)])['assumptions']
        self.assertIn('non-overlapping admission stream', assumptions['overlap'])
        self.assertIn('no p-value', assumptions['overlap'])
        self.assertIn('conservative', assumptions['unconditional_baseline'])
        self.assertIn('Benjamini-Hochberg', assumptions['multiplicity'])


class FDRStorageTests(unittest.TestCase):
    def insert(self, con, run, rows):
        con.executemany(
            'INSERT INTO cell_outcomes(run,research_run,symbol,timeframe,pattern_id,variant,'
            'side,state,summary,oos_p_value) VALUES(?,?,?,?,?,?,?,?,?,?)',
            [(run, 'rr', f'S{i}', '1D', f'P{i}', 'v', 'long', 'setup', '{}', p)
             for i, p in enumerate(rows)])
        con.commit()

    def database(self):
        tmp = tempfile.TemporaryDirectory()
        con = outcomes.connect(tmp.name)
        self.addCleanup(tmp.cleanup)
        self.addCleanup(con.close)
        return con

    def test_fdr_is_applied_across_the_whole_run_and_is_idempotent(self):
        p_values = [0.001, 0.008, 0.039, 0.041, 0.042, 0.065, 0.074, 0.205, 0.212, 0.216, None]
        expected = outcomes.benjamini_hochberg(p_values[:10])
        con = self.database()
        self.insert(con, 'run1', p_values)
        summary = outcomes.apply_fdr(con, 'run1')
        self.assertEqual(summary['trials'], 10)              # the NULL cell is not a trial
        self.assertEqual(summary['discoveries'],
                         {'q<0.1': sum(1 for q in expected if q < 0.1),
                          'q<0.05': sum(1 for q in expected if q < 0.05)})
        stored = list(con.execute('SELECT oos_p_value,oos_q_value,fdr_trials,discovery_q10,'
                                  'discovery_q05 FROM cell_outcomes WHERE run=? ORDER BY rowid',
                                  ('run1',)))
        self.assertEqual([r[1] for r in stored[:10]], expected)
        self.assertEqual(stored[-1][1:], (None, None, None, None))
        self.assertTrue(all(r[2] == 10 for r in stored[:10]))
        self.assertEqual([r[3] for r in stored[:10]], [int(q < 0.1) for q in expected])
        self.assertEqual([r[4] for r in stored[:10]], [int(q < 0.05) for q in expected])
        self.assertEqual(outcomes.apply_fdr(con, 'run1'), summary)         # idempotent
        # A different run is a different family and must not be touched.
        self.insert(con, 'run2', [0.5])
        outcomes.apply_fdr(con, 'run2')
        self.assertEqual(con.execute('SELECT fdr_trials FROM cell_outcomes WHERE run=?',
                                     ('run2',)).fetchone()[0], 1)
        self.assertEqual(con.execute('SELECT fdr_trials FROM cell_outcomes WHERE run=? AND '
                                     'oos_p_value IS NOT NULL ORDER BY rowid LIMIT 1',
                                     ('run1',)).fetchone()[0], 10)

    def test_a_run_with_no_testable_cell_reports_zero_trials(self):
        con = self.database()
        self.insert(con, 'empty', [None, None])
        self.assertEqual(outcomes.apply_fdr(con, 'empty')['trials'], 0)


# ------------------------------------------------------------------- storage
class StorageTests(unittest.TestCase):
    def test_round_trip_keeps_provenance_and_both_tables(self):
        bars = flat(30)
        put(bars, 1, 100., 102.)
        payload = cell(bars, [event(0), event(12)])
        payload.update(pattern_id='CH01', variant='legacy_1.0.1', side='long', family='chart')
        result = dict(run='testrun', research_run='8ae6ddc251e80668239e', source_run='src',
                      symbol='TEST', snapshot_id='snap-1', history_sha256='deadbeef',
                      seconds=0.1, cells=[payload])
        with tempfile.TemporaryDirectory() as tmp:
            path, digest = outcomes.write_symbol_artifact('testrun', 'TEST', result, tmp)
            con = outcomes.connect(tmp)
            cells, rows = outcomes.commit_symbol(con, 'testrun', 'TEST', path, digest, tmp)
            self.assertEqual((cells, rows), (1, 2))
            stored = con.execute('SELECT snapshot_id,history_sha256,research_run,max_horizon,'
                                 'selection_status FROM cell_outcomes').fetchone()
            self.assertEqual(stored, ('snap-1', 'deadbeef', '8ae6ddc251e80668239e', 10,
                                      'insufficient_history'))
            net, exits = con.execute('SELECT net_return_pct,exit_index FROM occurrence_outcomes '
                                     'WHERE signal_index=0').fetchone()
            self.assertAlmostEqual(json.loads(net)[0], 2.0 - COST_PCT)
            self.assertEqual(json.loads(exits), list(range(1, 11)))
            self.assertEqual(outcomes.completed_symbols(con, 'testrun'), {'TEST'})
            con.close()
            self.assertTrue((Path(tmp) / outcomes.DB_NAME).exists())

    def test_artifact_digest_mismatch_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = dict(run='r', research_run='x', symbol='TEST', cells=[])
            path, _ = outcomes.write_symbol_artifact('r', 'TEST', result, tmp)
            con = outcomes.connect(tmp)
            with self.assertRaises(ValueError):
                outcomes.commit_symbol(con, 'r', 'TEST', path, '0' * 64, tmp)
            con.close()


class DetectionCodeGateTests(unittest.TestCase):
    """The re-detection gate: what may drift, what may never drift."""

    def closure(self):
        return outcomes_cli.detection_closure(['legacy', 'price_action'])

    def manifest(self, broken=()):
        package = outcomes_cli.PACKAGE
        hashes = {}
        for relative in sorted(self.closure() | {str(Path('pattern_research/runner.py'))}):
            path = package / relative
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            hashes[relative] = '0' * 64 if relative in broken else digest
        return dict(id='r', modules=['legacy', 'price_action'], code_hashes=hashes)

    def test_closure_is_walked_from_the_detectors_not_hard_coded(self):
        closure = self.closure()
        for expected in ('pattern_research\\common.py', 'pattern_research\\evaluation.py',
                         'pattern_research\\legacy.py', 'pattern_research\\price_action.py'):
            self.assertIn(expected, closure)
        # legacy.py reaches the replay chain through relative imports.
        self.assertIn('historical.py', closure)
        self.assertNotIn('pattern_research\\candlesticks.py', closure)  # module not requested
        self.assertNotIn('pattern_research\\runner.py', closure)        # orchestration only

    def test_a_changed_detection_module_is_always_fatal(self):
        with self.assertRaises(ValueError) as caught:
            outcomes_cli.verify_detector_code(self.manifest({'pattern_research\\price_action.py'}))
        self.assertIn('price_action', str(caught.exception))
        with self.assertRaises(ValueError):
            outcomes_cli.verify_detector_code(self.manifest({'pattern_research\\common.py'}))

    def test_drift_outside_the_closure_is_recorded_not_fatal(self):
        result = outcomes_cli.verify_detector_code(self.manifest({'pattern_research\\runner.py'}))
        self.assertEqual(result['detection_verified'], 'by_code_hash')
        self.assertEqual(result['code_drift_outside_detection'], ['pattern_research\\runner.py'])
        self.assertEqual(result['changed_in_detection_closure'], [])

    def test_indirect_drift_needs_a_reproduction_probe_and_is_fatal_without_one(self):
        with self.assertRaises(ValueError) as caught:
            outcomes_cli.verify_detector_code(self.manifest({'historical.py'}))
        self.assertIn('could not be shown to reproduce', str(caught.exception))
        self.assertIn('historical.py', str(caught.exception))


# --------------------------------------------------------- barriers (first touch)
def barrier(bars, events, pair_id, **kw):
    rows = cell(bars, events, **kw)['barriers']['rows']
    return next(r for r in rows if r['id'] == pair_id)


class BarrierTests(unittest.TestCase):
    def test_grid_is_declared_in_both_percent_and_atr_and_covers_the_brief(self):
        ids = [p['id'] for p in outcomes.BARRIER_GRID]
        self.assertEqual(ids, ['pct:1.0:1.0', 'pct:2.0:1.0', 'pct:3.0:1.5',
                               'atr:1.0:1.0', 'atr:2.0:1.0'])
        self.assertEqual({p['unit'] for p in outcomes.BARRIER_GRID}, {'pct', 'atr'})
        self.assertEqual(outcomes.DISPLAY_BARRIER_ID, 'pct:2.0:1.0')

    def test_target_first_and_stop_first_are_read_from_intrabar_extremes(self):
        bars = flat(30)
        put(bars, 1, 100., 100., high=100.5, low=99.8)     # neither
        put(bars, 2, 100., 100., high=102.5, low=99.8)     # target +2% touched
        winner = cell(bars, [event(0)])
        bars = flat(30)
        put(bars, 1, 100., 100., high=100.5, low=98.5)     # stop -1% touched first
        put(bars, 2, 100., 100., high=103.0, low=99.0)
        loser = cell(bars, [event(0)])
        first = next(r for r in winner['barriers']['rows'] if r['id'] == 'pct:2.0:1.0')
        second = next(r for r in loser['barriers']['rows'] if r['id'] == 'pct:2.0:1.0')
        self.assertEqual((first['n_target'], first['n_stop']), (1, 0))
        self.assertEqual(first['bars_to_target']['median'], 2)
        self.assertEqual((second['n_target'], second['n_stop']), (0, 1))
        self.assertEqual(second['bars_to_stop']['median'], 1)

    def test_both_touched_in_one_bar_counts_the_stop_and_says_so(self):
        bars = flat(30)
        put(bars, 1, 100., 100., high=103.0, low=98.0)     # +3% and -2% in the same bar
        row = barrier(bars, [event(0)], 'pct:2.0:1.0')
        self.assertEqual((row['n_stop'], row['n_target']), (1, 0))
        self.assertEqual(row['both_touched_same_bar_n'], 1)
        self.assertEqual(row['tie_rule'], 'stop_first_when_both_touched_in_the_same_bar')
        self.assertEqual(outcomes.BARRIER_TIE_RULE, row['tie_rule'])

    def test_an_opening_price_beyond_a_barrier_fills_at_that_open(self):
        bars = flat(30)
        put(bars, 1, 100., 100.)
        put(bars, 2, 97.0, 104.0, high=105., low=97.)      # opens through the stop
        row = barrier(bars, [event(0)], 'pct:2.0:1.0')
        self.assertEqual((row['n_stop'], row['n_target']), (1, 0))
        self.assertEqual(row['bars_to_stop']['median'], 2)

    def test_neither_barrier_touched_within_the_horizon(self):
        bars = flat(30)
        row = barrier(bars, [event(0)], 'pct:2.0:1.0')
        self.assertEqual(row['n_neither'], 1)
        self.assertEqual(row['p_neither'], 100.0)
        self.assertEqual(row['p_target'], 0.0)

    def test_probabilities_sum_to_one_hundred_and_short_side_mirrors(self):
        bars = flat(60)
        events = []
        for signal in range(0, 40, 6):
            put(bars, signal + 2, 100., 100., high=103.0, low=99.5)
            events.append(event(signal))
        for side in ('long', 'short'):
            row = barrier(bars, events, 'pct:2.0:1.0', side=side)
            total = row['p_target'] + row['p_stop'] + row['p_neither'] + row['p_gap_exit']
            self.assertAlmostEqual(total, 100.0, places=6)
        long_row = barrier(bars, events, 'pct:2.0:1.0', side='long')
        short_row = barrier(bars, events, 'pct:2.0:1.0', side='short')
        self.assertGreater(long_row['n_target'], 0)          # +3% high reaches the long target
        self.assertEqual(short_row['n_target'], 0)           # the same high is the short's stop
        self.assertGreater(short_row['n_stop'], 0)

    def test_an_undecided_occurrence_without_enough_history_is_excluded_not_guessed(self):
        bars = flat(14)                                       # entry 11, only 3 bars ahead
        row = barrier(bars, [event(10)], 'pct:2.0:1.0')
        self.assertEqual(row['n'], 0)
        self.assertEqual(row['undetermined_n'], 1)
        self.assertIsNone(row['p_target'])

    def test_atr_pairs_use_the_event_atr_and_skip_an_unusable_one(self):
        bars = flat(30)
        put(bars, 1, 100., 100., high=102.1, low=99.9)        # +2.1 = just over 1 ATR
        row = barrier(bars, [event(0)], 'atr:1.0:1.0')        # event atr = 2.0
        self.assertEqual((row['n_target'], row['n_stop']), (1, 0))
        tighter = barrier(bars, [event(0)], 'atr:2.0:1.0')    # +4.0 is not reached
        self.assertEqual(tighter['n_neither'], 1)

    def test_a_gap_inside_the_hold_ends_the_observation_rather_than_inventing_one(self):
        bars = flat(30)
        put(bars, 3, 100., 100., high=110., low=90., gap=True)
        row = barrier(bars, [event(0)], 'pct:2.0:1.0')
        self.assertEqual(row['n_gap_exit'], 1)
        self.assertEqual(row['n_target'], 0)
        self.assertEqual(row['n_stop'], 0)


class TimeToTouchTests(unittest.TestCase):
    def test_quartiles_are_reported_for_winners_and_losers_separately(self):
        bars = flat(120)
        events = []
        for k, signal in enumerate(range(0, 90, 12)):
            entry = signal + 1
            delay = 1 + k % 4
            if k % 2 == 0:
                put(bars, entry + delay - 1, 100., 100., high=102.5, low=99.5)
            else:
                put(bars, entry + delay - 1, 100., 100., high=100.5, low=98.5)
            events.append(event(signal))
        row = barrier(bars, events, 'pct:2.0:1.0')
        self.assertGreater(row['bars_to_target']['n'], 0)
        self.assertGreater(row['bars_to_stop']['n'], 0)
        for side in ('target', 'stop'):
            quartiles = row['bars_to_' + side]
            self.assertLessEqual(quartiles['p25'], quartiles['median'])
            self.assertLessEqual(quartiles['median'], quartiles['p75'])
            self.assertIsNotNone(row['calendar_days_to_' + side])

    def test_the_return_peak_horizon_is_where_the_median_stops_improving(self):
        bars = flat(60)
        events = []
        for signal in range(0, 40, 12):
            entry = signal + 1
            for k in range(1, 11):
                level = 100. * (1 + 0.01 * min(k, 4))      # rises to +4% by h=4, then flat
                put(bars, entry + k - 1, 100. if k == 1 else level, level)
            events.append(event(signal))
        peak = cell(bars, events)['return_peak']
        self.assertEqual(peak['median_peak_h'], 4)
        self.assertEqual(peak['mean_peak_h'], 4)
        self.assertAlmostEqual(peak['median_peak_net_return_pct'], 4.0 - COST_PCT, places=4)


class DisplayGridTests(unittest.TestCase):
    def test_declared_grids_match_the_owner_spec(self):
        self.assertEqual(outcomes.DISPLAY_GRID, {'1H': (1, 2, 4, 8, 12, 24),
                                                 '4H': (1, 2, 3, 5, 8, 10),
                                                 '1D': (1, 2, 3, 5, 10),
                                                 '1W': (1, 2, 4, 8, 12)})

    def test_values_beyond_the_horizon_are_dropped_and_reported(self):
        weekly = outcomes.display_grid('1W')                  # H = 8, grid asks for 12
        self.assertEqual(weekly['shown'], [1, 2, 4, 8])
        self.assertEqual(weekly['dropped_beyond_horizon'], [12])
        self.assertEqual(outcomes.display_grid('1W', 12)['shown'], [1, 2, 4, 8, 12])
        self.assertEqual(outcomes.display_grid('1D')['shown'], [1, 2, 3, 5, 10])

    def test_the_cell_carries_its_display_grid(self):
        grid = cell(flat(40), [event(0)])['display_grid']
        self.assertEqual(grid['max_horizon'], 10)
        self.assertEqual(grid['shown'], [1, 2, 3, 5, 10])


# ------------------------------------------------------------- condition buckets
class BucketTests(unittest.TestCase):
    def test_bucket_boundaries_are_declared_constants(self):
        self.assertEqual((outcomes.VOLUME_LOOKBACK, outcomes.VOLUME_HIGH, outcomes.VOLUME_LOW),
                         (20, 1.5, 0.7))
        self.assertEqual((outcomes.REGIME_LOOKBACK, outcomes.REGIME_BAND), (200, 0.05))
        self.assertEqual(outcomes.BUCKET_MIN_SAMPLE, 30)
        self.assertEqual(outcomes.QUALITY_TERTILES, 3)

    def test_volume_buckets_are_point_in_time_and_use_the_declared_thresholds(self):
        bars = flat(60)
        for i, b in enumerate(bars):
            b['volume'] = 100.
        bars[30]['volume'] = 200.        # 2.0x the prior median -> high
        bars[40]['volume'] = 50.         # 0.5x -> low
        prices, gaps, _, _ = outcomes._prepare(bars)
        labels = outcomes.bar_buckets(bars, prices, gaps)['volume']
        self.assertEqual(labels[30], 'high')
        self.assertEqual(labels[40], 'low')
        self.assertEqual(labels[35], 'normal')
        self.assertIsNone(labels[5])     # not enough history yet

    def test_regime_buckets_use_the_stock_trend_when_no_index_is_supplied(self):
        bars = flat(400)
        for i in range(400):
            level = 100. * (1 + 0.001 * i)           # +0.1%/bar: clearly up after 200 bars
            put(bars, i, level, level)
        prices, gaps, _, _ = outcomes._prepare(bars)
        assigned = outcomes.bar_buckets(bars, prices, gaps)
        self.assertEqual(assigned['regime'][350], 'up')
        self.assertIsNone(assigned['regime'][100])
        self.assertEqual(assigned['_regime_source'], 'stock_own_200_bar_trend')

    def test_an_index_series_is_used_for_the_regime_when_supplied(self):
        bars = flat(400)
        index = [dict(b, close=100. * (1 - 0.001 * i)) for i, b in enumerate(bars)]
        prices, gaps, _, _ = outcomes._prepare(bars)
        assigned = outcomes.bar_buckets(bars, prices, gaps, index_bars=index)
        self.assertEqual(assigned['_regime_source'], 'index_series')
        self.assertEqual(assigned['regime'][350], 'down')      # the stock itself is flat

    def test_a_thin_bucket_reports_insufficient_and_gets_no_p_value(self):
        bars, events = yearly_series({2015, 2016, 2017, 2018})
        rows = cell(bars, events)['buckets']['rows']
        self.assertTrue(rows)
        for row in rows:
            if row['n'] < outcomes.BUCKET_MIN_SAMPLE:
                self.assertEqual(row['status'], 'insufficient')
                self.assertIsNone(row['p_value'])
            else:
                self.assertEqual(row['status'], 'tested')
                self.assertIsNotNone(row['p_value'])

    def test_every_bucket_carries_a_baseline_in_the_same_bucket(self):
        bars, events = yearly_series({2015, 2016, 2017, 2018})
        block = cell(bars, events)['buckets']
        by_dimension = {row['dimension'] for row in block['rows']}
        self.assertIn('volume', by_dimension)
        self.assertIn('quality_tertile', by_dimension)
        for row in block['rows']:
            self.assertIn('baseline_scope', row)
            self.assertEqual(row['scope'], 'all_history_non_overlapping_not_out_of_sample')
            if row['dimension'] == 'quality_tertile':
                self.assertIn('no_score_exists', row['baseline_scope'])
                self.assertEqual(row['stratification'],
                                 'in_sample_score_tertiles_not_a_point_in_time_rule')
            else:
                self.assertEqual(row['baseline_scope'],
                                 'unconditional_entries_in_the_same_bucket')

    def test_a_bucket_that_only_rides_its_own_regime_shows_no_difference(self):
        # Every bar in the window moves the same way, pattern or not.
        bars = flat(1000)
        events = []
        for i in range(1, 1000):
            put(bars, i, 100., 102.)                 # +2% on every single bar
        for signal in range(30, 960, 12):
            events.append(event(signal))
        rows = cell(bars, events)['buckets']['rows']
        tested = [r for r in rows if r['status'] == 'tested' and r['dimension'] == 'volume']
        self.assertTrue(tested)
        for row in tested:
            self.assertGreater(row['mean_net_return_pct'], 1.0)      # looks great raw
            self.assertAlmostEqual(row['diff_mean_net_return_pct'], 0.0, places=6)
            # Beating ZERO is trivial here and must not be mistaken for an edge.
            self.assertLess(row['p_value_vs_zero'], 0.001)
            self.assertGreater(row['p_value'], 0.10)
            self.assertEqual(row['p_value_method'], 'stationary_bootstrap_of_the_difference')
            self.assertIn('NOT zero', row['null_hypothesis'])

    def test_a_bucket_that_genuinely_beats_its_own_baseline_is_detected(self):
        bars = flat(1000)
        events = []
        for i in range(1, 1000):
            put(bars, i, 100., 101.)                 # the market itself makes +1% a bar
        for signal in range(30, 960, 12):
            put(bars, signal + 1, 100., 104.)        # the pattern bar makes +4%
            events.append(event(signal))
        rows = cell(bars, events, horizon=1)['buckets']['rows']
        tested = [r for r in rows if r['status'] == 'tested' and r['dimension'] == 'volume']
        self.assertTrue(tested)
        for row in tested:
            self.assertGreater(row['diff_mean_net_return_pct'], 2.0)
            self.assertLess(row['p_value'], 0.05)


# ------------------------------------------------------- last N occurrences (UI)
class LastOccurrenceTests(unittest.TestCase):
    def test_the_most_recent_occurrences_are_returned_newest_first(self):
        bars = flat(200)
        events = [event(i) for i in range(0, 150, 10)]
        block = cell(bars, events)['last_occurrences']
        self.assertEqual(len(block['rows']), outcomes.LAST_N_OCCURRENCES)
        dates = [row['entry_date'] for row in block['rows']]
        self.assertEqual(dates, sorted(dates, reverse=True))
        self.assertEqual(block['barrier_id'], outcomes.DISPLAY_BARRIER_ID)
        self.assertEqual(block['tie_rule'], outcomes.BARRIER_TIE_RULE)

    def test_each_row_carries_what_the_app_has_to_show(self):
        bars = flat(60)
        put(bars, 41, 100., 100., high=102.5, low=99.5)    # entry bar of the last event
        events = [event(i) for i in range(0, 45, 10)]
        row = cell(bars, events)['last_occurrences']['rows'][0]
        for field in ('entry_date', 'net_return_pct', 'mfe_pct', 'mae_pct', 'bars_held',
                      'outcome', 'horizon'):
            self.assertIn(field, row)
        self.assertEqual(row['outcome'], 'target_hit')
        self.assertEqual(row['bars_held'], 1)
        self.assertEqual(row['mfe_pct'], 2.5)

    def test_outcome_labels_cover_target_stop_and_time_exits(self):
        bars = flat(60)
        put(bars, 1, 100., 100., high=103., low=99.9)      # target
        put(bars, 11, 100., 100., high=100.1, low=98.5)    # stop
        events = [event(0), event(10), event(20)]          # the third never touches
        labels = {row['entry_index']: row['outcome']
                  for row in cell(bars, events)['last_occurrences']['rows']}
        self.assertEqual(labels[1], 'target_hit')
        self.assertEqual(labels[11], 'stop_hit')
        self.assertEqual(labels[21], 'time_exit')

    def test_the_count_is_configurable(self):
        bars = flat(200)
        events = [event(i) for i in range(0, 150, 10)]
        result = outcome_cell(bars, events, '1D', 'long', 'setup', last_n=2)
        self.assertEqual(len(result['last_occurrences']['rows']), 2)


class BucketStorageTests(unittest.TestCase):
    def test_bucket_rows_are_stored_and_corrected_as_their_own_family(self):
        bars, events = yearly_series({2015, 2016, 2017, 2018})
        payload = cell(bars, events)
        payload.update(pattern_id='PA02', variant='v', side='long', family='price_action')
        result = dict(run='r', research_run='rr', source_run='s', symbol='TEST',
                      snapshot_id=None, history_sha256='abc', seconds=1.0,
                      labels=dict(sector='Information Technology', market_cap_tier='large',
                                  is_fno=1),
                      cells=[payload])
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path, digest = outcomes.write_symbol_artifact('r', 'TEST', result, tmp.name)
        con = outcomes.connect(tmp.name)
        self.addCleanup(con.close)
        outcomes.commit_symbol(con, 'r', 'TEST', path, digest, tmp.name)
        stored = list(con.execute('SELECT dimension,bucket,n,status,baseline_scope,p_value '
                                  'FROM bucket_outcomes WHERE run=?', ('r',)))
        self.assertTrue(stored)
        self.assertTrue(any(d == 'quality_tertile' for d, *_ in stored))
        tier, sector, fno, grid, recent = con.execute(
            'SELECT market_cap_tier,sector,is_fno,display_grid,last_occurrences '
            'FROM cell_outcomes WHERE run=?', ('r',)).fetchone()
        self.assertEqual((tier, sector, fno), ('large', 'Information Technology', 1))
        self.assertEqual(json.loads(grid)['shown'], [1, 2, 3, 5, 10])
        self.assertEqual(len(json.loads(recent)), outcomes.LAST_N_OCCURRENCES)
        # Two families, corrected separately, never pooled.
        summary = outcomes.apply_all_fdr(con, 'r')
        self.assertEqual(set(summary), {'cells', 'buckets'})
        self.assertEqual(summary['buckets']['family'], 'buckets')
        testable = sum(1 for row in stored if row[5] is not None)
        self.assertEqual(summary['buckets']['trials'], testable)
        cells_testable = con.execute('SELECT COUNT(*) FROM cell_outcomes WHERE run=? AND '
                                     'oos_p_value IS NOT NULL', ('r',)).fetchone()[0]
        self.assertEqual(summary['cells']['trials'], cells_testable)
        # A bucket q-value is never computed from the cell family, or vice versa.
        self.assertNotIn(summary['buckets']['trials'], (0,))
        trials = {r[0] for r in con.execute('SELECT fdr_trials FROM bucket_outcomes '
                                            'WHERE run=? AND p_value IS NOT NULL', ('r',))}
        self.assertEqual(trials, {testable})

    def test_the_display_barrier_is_flattened_onto_the_cell_row(self):
        bars = flat(60)
        put(bars, 1, 100., 100., high=103., low=99.9)
        payload = cell(bars, [event(0), event(20)])
        payload.update(pattern_id='P', variant='v', side='long', family='chart')
        result = dict(run='r2', research_run='rr', symbol='TEST', history_sha256='x',
                      cells=[payload])
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path, digest = outcomes.write_symbol_artifact('r2', 'TEST', result, tmp.name)
        con = outcomes.connect(tmp.name)
        self.addCleanup(con.close)
        outcomes.commit_symbol(con, 'r2', 'TEST', path, digest, tmp.name)
        row = con.execute('SELECT display_barrier_id,barrier_n,p_target_first,p_stop_first,'
                          'p_neither,median_bars_to_target,return_peak_h FROM cell_outcomes '
                          'WHERE run=?', ('r2',)).fetchone()
        self.assertEqual(row[0], outcomes.DISPLAY_BARRIER_ID)
        self.assertEqual(row[1], 2)
        self.assertAlmostEqual(row[2] + row[3] + row[4], 100.0, places=6)
        self.assertEqual(row[5], 1)


if __name__ == '__main__':
    unittest.main()

