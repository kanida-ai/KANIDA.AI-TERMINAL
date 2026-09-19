"""Execution and chronological selection gates; no source database needed."""
from datetime import date, timedelta
import copy
import json
import unittest

import numpy as np

from market_scanner.pattern_research.evaluation import (
    evaluate_cell, _prepare, _matrix, _choose, _stats, _excursions, HOLDS,
)


def bars(n=20, trend=0., start=date(2020, 1, 1)):
    result = []
    for i in range(n):
        day = (start + timedelta(days=i)).isoformat()
        price = 100. + i * trend
        result.append(dict(time=day+' 09:15:00', end=day+' 15:30:00', open=price,
                           high=price+.2, low=price-.2, close=price, volume=100., gap=False))
    return result


def event(i=0, state='setup'):
    return dict(signal_index=i, episode=i, state=state, atr=2., score=1.,
                direction='bullish', pattern_start='2020-01-01 09:15:00')


def one_matrix(bs, hold=3, stop=1., target=2., side=1):
    prices, gaps, starts, ends = _prepare(bs)
    matrix = _matrix(prices, gaps, np.array([0], dtype=np.int64), np.array([2.]),
                     np.array([0], dtype=np.int8), np.array([0], dtype=np.int8),
                     np.array([hold], dtype=np.int64), np.array([stop]), np.array([target]), side)
    return matrix


class EvaluationTests(unittest.TestCase):
    def test_baseline_trigger_is_predeclared_not_chosen_from_future_events(self):
        bs=bars(100)
        first=evaluate_cell(bs[:50],[event(0,'confirmed')],'1D','long',['setup','confirmed'])
        extended=evaluate_cell(bs,[event(0,'confirmed'),event(60)],'1D','long',['setup','confirmed'])
        self.assertEqual(first['reference']['rule'],extended['reference']['rule'])
        self.assertEqual(first['reference']['rule']['trigger'],'setup')
        intrinsic=evaluate_cell(bs,[event(0,'confirmed')],'1D','long',['confirmed'])
        self.assertEqual(intrinsic['reference']['rule']['trigger'],'confirmed')

    def test_baseline_costs_and_next_open_long_and_short(self):
        bs = bars()
        bs[0].update(open=50., high=50.2, low=49.8, close=50.)
        bs[10].update(open=108., high=108.2, low=107.8, close=108.)
        for side, expected in [('long', 7.6), ('short', -8.4)]:
            result = evaluate_cell(bs, [event()], '1D', side, ['setup'])
            trade = result['reference']['trades'][0]
            self.assertEqual(trade['entry_index'], 1)
            self.assertEqual(trade['exit_index'], 10)
            self.assertAlmostEqual(trade['net_return_pct'], expected)
            self.assertEqual(trade['cost_pct'], .4)
            json.dumps(result, allow_nan=False)

    def test_same_bar_stop_target_is_stop_first_for_both_sides(self):
        bs = bars(); bs[1].update(high=110., low=90.)
        for side, fill in [(1, 98.), (-1, 102.)]:
            exits, fills, codes, collision, returns = one_matrix(bs, side=side)
            self.assertEqual(int(exits[0, 0]), 1)
            self.assertEqual(float(fills[0, 0]), fill)
            self.assertEqual(int(codes[0, 0]), 2)
            self.assertTrue(bool(collision[0, 0]))
            self.assertAlmostEqual(float(returns[0, 0]), -2.4)

    def test_opening_price_gap_uses_open(self):
        bs = bars(); bs[2].update(open=90., high=91., low=89., close=90.)
        matrix = one_matrix(bs)
        self.assertEqual(int(matrix[2][0, 0]), 4)
        self.assertEqual(float(matrix[1][0, 0]), 90.)

    def test_future_quality_gap_retains_losing_position(self):
        bs = bars(); bs[2].update(open=80., high=81., low=79., close=80., gap=True)
        result = evaluate_cell(bs, [event()], '1D', 'long', ['setup'])
        trade = result['reference']['trades'][0]
        self.assertEqual(trade['exit_index'], 2)
        self.assertEqual(trade['exit_reason'], 'data_gap_first_available_open')
        self.assertAlmostEqual(trade['net_return_pct'], -20.4)
        self.assertEqual(trade['exit_time'], bs[2]['time'])

    def test_entry_quality_gap_skips_stale_signal(self):
        bs = bars(); bs[1]['gap'] = True
        result = evaluate_cell(bs, [event()], '1D', 'long', ['setup'])
        self.assertEqual(result['reference']['trades'], [])
        self.assertEqual(result['reference']['exclusions']['signal_or_entry_gap'], 1)

    def test_final_potential_horizon_required_even_if_target_would_hit_early(self):
        bs = bars(3); bs[1].update(high=120.)
        matrix = one_matrix(bs, hold=3)
        self.assertEqual(int(matrix[0][0, 0]), -1)
        self.assertEqual(int(matrix[2][0, 0]), -2)

    def test_baseline_distinct_occurrences_preserved_but_trades_do_not_overlap(self):
        result = evaluate_cell(bars(40), [event(0), event(1), event(10)], '1D', 'long', ['setup'])
        self.assertEqual(result['events'], 3)
        self.assertEqual([t['entry_index'] for t in result['reference']['trades']], [1, 11])
        self.assertEqual(result['reference']['exclusions']['overlap'], 1)

    def test_potential_horizon_and_embargo_are_purged_despite_early_exit(self):
        bs = bars(200)
        signals = np.arange(0, 100, 3, dtype=np.int64)
        entries = signals + 1
        # All outcomes appear profitable and exit immediately, but full horizons
        # (10 holding bars plus 40 embargo bars) must qualify independently.
        exits = entries.reshape(1, -1)
        returns = np.ones((1, len(signals)))
        starts, ends = _prepare(bs)[2:]
        cutoff = int(starts[100])
        chosen = _choose(exits, returns, signals, starts, ends, np.array([10]), int(starts[0]), cutoff, 40, 20)
        self.assertEqual(chosen, -1)  # Only 17 full horizons qualify; 34 early exits do not.

    def test_future_prices_cannot_change_first_frozen_rule(self):
        bs = bars(1500, trend=.5)
        events = [event(i) for i in range(40, len(bs)-1)]
        first = evaluate_cell(bs, events, '1D', 'long', ['setup'])
        changed = copy.deepcopy(bs)
        for i, b in enumerate(changed):
            if b['time'][:10] >= '2023-01-01':
                p = 1000. - (i-1096) * .9
                b.update(open=p, high=p+.2, low=p-.2, close=p)
        second = evaluate_cell(changed, events, '1D', 'long', ['setup'])
        self.assertIsNotNone(first['folds'][0]['rule'])
        self.assertEqual(first['folds'][0]['rule'], second['folds'][0]['rule'])
        self.assertEqual(first['folds'][0]['training_stats'], second['folds'][0]['training_stats'])

    def test_cross_fold_positions_keep_rule_and_block_new_entries(self):
        bs = bars(1500, trend=.5)
        result = evaluate_cell(bs, [event(i) for i in range(40, len(bs)-1)], '1D', 'long', ['setup'])
        ledger = result['walkforward']['trades']
        crossing = [t for t in ledger if t['entry_time'][:10] < '2023-07-01' <= t['exit_time'][:10]]
        self.assertTrue(crossing)
        trade = crossing[0]
        self.assertEqual(trade['rule'], result['folds'][trade['fold']]['rule'])
        for previous, current in zip(ledger, ledger[1:]):
            self.assertGreater(current['entry_index'], previous['exit_index'])
        self.assertTrue(any(t['fold'] > trade['fold'] for t in ledger))
        self.assertEqual(sum(f['stats']['n'] for f in result['folds']), len(ledger))
        self.assertAlmostEqual(sum(f['stats']['sum_net_return_pct'] for f in result['folds']),
                               result['walkforward']['stats']['sum_net_return_pct'])

    def test_no_event_history_and_bad_contracts(self):
        result = evaluate_cell([], [], '1W', 'short', ['confirmed'])
        self.assertEqual(result['status'], 'no_occurrences')
        self.assertEqual(result['candidate_count'], 28)
        self.assertEqual(result['folds'], [])
        json.dumps(result, allow_nan=False)
        with self.assertRaises(ValueError):
            evaluate_cell(bars(), [event(100)], '1D', 'long', ['setup'])

    def test_numpy_event_scalars_are_serialized_as_native_values(self):
        e = event(np.int64(0)); e['atr'] = np.float64(2.)
        result = evaluate_cell(bars(), [e], '1D', 'long', ['setup'])
        json.dumps(result, allow_nan=False)

    def test_intrabar_excursions_do_not_claim_post_exit_extremes(self):
        bs = bars(); bs[1].update(high=120., low=80.)
        result = _excursions(bs, 1, 1, 100., 98., 1, 2, 98., 104.)
        self.assertEqual(result['mfe_lower_pct'], 0.)
        self.assertEqual(result['mfe_upper_pct'], 4.)
        self.assertEqual(result['mae_lower_pct'], 2.)
        self.assertEqual(result['mae_upper_pct'], 2.)
        self.assertTrue(result['excursion_uncertain'])

    def test_data_gap_excursion_upper_bounds_are_unknown(self):
        bs = bars(); bs[2].update(open=80., high=81., low=79., close=80., gap=True)
        trade = evaluate_cell(bs, [event()], '1D', 'long', ['setup'])['reference']['trades'][0]
        self.assertEqual(trade['mae_lower_pct'], 20.)
        self.assertIsNone(trade['mae_upper_pct'])
        self.assertIsNone(trade['mfe_upper_pct'])

    def test_statistics_report_wilson_interval_and_breakeven(self):
        stats = _stats([1., -1., 0.])
        self.assertEqual(stats['breakeven'], 1)
        self.assertLess(stats['win_rate_ci95'][0], stats['win_rate_pct'])
        self.assertGreater(stats['win_rate_ci95'][1], stats['win_rate_pct'])


if __name__ == '__main__':
    unittest.main()
