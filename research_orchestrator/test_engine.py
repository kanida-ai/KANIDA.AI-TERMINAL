from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

from .adapters import Features, compile_mask, validate_data, RecordedSnapshotProvider
from .contracts import validate
from .engine import cache_identity, choose, objective_check, read_evidence, run, simulate, period_metrics, conclusions
from .accounting_checks import verify_account_strict
from .numerical_probe import digest


def request():
    return json.loads((Path(__file__).parent / 'examples/pilot-strategies.json').read_text())


def bars(n=1100):
    days = [];day = date(2015, 1, 1)
    while len(days) < n:
        if day.weekday() < 5:
            days.append(day.isoformat())
        day += timedelta(days=1)
    return [dict(time=d + ' 09:15:00', end=d + ' 15:30:00', open=100 + i * .02,
                 close=100 + i * .02, high=101 + i * .02, low=99 + i * .02,
                 volume=100000, gap=False) for i, d in enumerate(days)]


class ContractTests(unittest.TestCase):
    def test_omitted_exit_settings_do_not_invent_a_profit_target(self):
        r = request();r['strategies'][0]['exit'] = {'trailing': 5}
        with self.assertRaisesRegex(ValueError, 'explicit settings'):
            validate(r)

    def test_boolean_is_not_a_numeric_threshold(self):
        r = request();r['strategies'][0]['entry']['value'] = True
        with self.assertRaisesRegex(ValueError, 'JSON number'):
            validate(r)

    def test_entry_event_cannot_be_reused_as_continuing_qualification(self):
        r = request();r['strategies'][4]['exit']['disqualify'] = True
        with self.assertRaisesRegex(ValueError, 'entry pulse'):
            validate(r)

    def test_lucky_test_candidate_does_not_change_walk_forward_verdict(self):
        fail = {'metrics': {}, 'objective': {'numeric_constraints_met': False, 'historical_constraints_met': False}}
        win = {'metrics': {}, 'objective': {'numeric_constraints_met': True, 'historical_constraints_met': False}}
        fold = dict(test_start='2020-01-01', test_end='2020-12-31', selection={'strategy_id': 'selected'},
                    testing={'selected': fail, 'lucky': win})
        walk, posthoc, verdict = conclusions([fold])
        self.assertFalse(walk['numeric_criteria_met_all_folds'])
        self.assertEqual(posthoc, ['lucky'])
        self.assertIn('did not meet', verdict)

    def test_short_test_cannot_establish_annual_return_target(self):
        m = dict(trades=50, cagr=100, max_drawdown_pct=5, excess_cagr=70, period_days=60)
        result = objective_check(m, dict(cagr=50, drawdown=15, beat_nifty=True), 5, quality_verified=True)
        self.assertFalse(result['numeric_constraints_met'])

    def test_assert_optimization_cannot_disable_account_checks(self):
        account = dict(summary=dict(open_positions=0, ending_equity=10001, total_costs=0), trades=[], daily_curve=[])
        with self.assertRaisesRegex(ValueError, 'does not reconcile'):
            verify_account_strict(account, dict(capital=10000, max_positions=1))

    def test_fetch_timestamp_does_not_invalidate_identical_evidence(self):
        r = validate(request())
        a = cache_identity(r, {'content_hashes': {'TEST':'a'}, 'fetched_at':'first'}, {}, 'owner')
        b = cache_identity(r, {'content_hashes': {'TEST':'a'}, 'fetched_at':'second'}, {}, 'owner')
        self.assertEqual(a, b)

    def test_rejects_unsupported_constraint_instead_of_dropping_it(self):
        r = request();r['max_sector_pct'] = 25
        with self.assertRaisesRegex(ValueError, 'unsupported fields'):
            validate(r)

    def test_fno_never_becomes_explicit_surviving_stocks(self):
        r = request();r['universe'] = 'fno'
        with self.assertRaisesRegex(ValueError, 'membership is unverified'):
            validate(r)

    def test_training_cannot_overlap_its_test(self):
        r = request();r['folds'][0]['train_end'] = '2020-02-01'
        with self.assertRaisesRegex(ValueError, 'precede'):
            validate(r)

    def test_future_labels_not_allowed_as_features(self):
        r = request();r['strategies'][0]['entry'] = dict(engine='stock_miner', feature='y', op='>', value=0)
        with self.assertRaisesRegex(ValueError, 'future labels'):
            validate(r)

    def test_unknown_ndp_variant_not_guessed(self):
        r = request();r['strategies'][0]['entry'] = dict(engine='ndp', name='EMA_cross', params='17x42', signal=1)
        with self.assertRaisesRegex(ValueError, 'explicitly supported'):
            validate(r)

    def test_nested_and_or_semantics(self):
        n = {'all': [{'any': [{'kind': 'rsi_below', 'value': 30}, {'kind': 'momentum', 'value': 10}]}, {'kind': 'volume', 'value': 1}]}
        f = dict(close=np.ones(4), rsi=np.array([20, 40, 40, 20]), momentum=np.array([0, 20, 0, 0]), volume_ratio=np.array([2, 2, 2, .5]))
        np.testing.assert_array_equal(compile_mask(n, f, {}, {}), [True, True, False, False])

    def test_ndp_crossover_event_is_not_persistent_above_state(self):
        from .feature_probe import load_pure_module
        from .inventory import TERMINAL
        ndp, _ = load_pure_module(TERMINAL / 'ndp/indicator_library.py', 'test_ndp_cross')
        fast = np.array([0., 0., 2., 3., 3.]);slow = np.ones(5)
        signal = ndp.cross(fast, slow);signal[0] = 0
        n = dict(engine='ndp', name='EMA_cross', params='9x21', signal=1)
        np.testing.assert_array_equal(compile_mask(n, {}, {('EMA_cross', '9x21'): signal}, {}), [False, False, True, False, False])

    def test_target_not_clamped_or_lost(self):
        r = request();r['objectives']['cagr'] = 100
        self.assertEqual(validate(r)['objectives']['cagr'], 100)

    def test_cache_changes_with_costs_rules_data_engine_and_owner(self):
        r = validate(request());base = cache_identity(r, {'data': 'a'}, {'engine': 'a'}, 'owner-a')
        edited = deepcopy(r);edited['account']['fee_bps'] += 1
        for args in [(edited, {'data': 'a'}, {'engine': 'a'}, 'owner-a'),
                     (r, {'data': 'b'}, {'engine': 'a'}, 'owner-a'),
                     (r, {'data': 'a'}, {'engine': 'b'}, 'owner-a'),
                     (r, {'data': 'a'}, {'engine': 'a'}, 'owner-b')]:
            self.assertNotEqual(base, cache_identity(*args))

    def test_corrupt_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'bad.json'
            path.write_text(json.dumps(dict(sha256='invented', evidence={'cache_key': 'x'})))
            with self.assertRaisesRegex(ValueError, 'checksum'):
                read_evidence(path, 'x')

    def test_objectives_need_evidence_even_if_cagr_looks_good(self):
        m = dict(trades=1, cagr=100, max_drawdown_pct=5, excess_cagr=50)
        self.assertFalse(objective_check(m, dict(cagr=50, drawdown=15, beat_nifty=True), 5)['historical_constraints_met'])

    def test_training_selection_ignores_sparse_high_return(self):
        t = {'stable': {'metrics': dict(trades=20, cagr=20, max_drawdown_pct=10, period_days=730)},
             'lucky': {'metrics': dict(trades=1, cagr=200, max_drawdown_pct=0, period_days=730)}}
        self.assertEqual(choose(t, 5), 'stable')


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.bars = bars()
        self.r = validate(request())
        self.r['symbols'] = ['TEST']
        self.s = deepcopy(self.r['strategies'][0]);self.s['id'] = 'test'
        self.s['entry'] = {'kind': 'momentum', 'value': -90}
        self.r['strategies'] = [self.s]

    def prepared(self, cutoff):
        return Features([self.s]).prepare({'TEST': self.bars}, self.bars, cutoff, [self.s])

    def test_time_boundary_purges_max_horizon_not_realized_exit(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end)
        p['TEST'][2]['test'][:] = False;p['TEST'][2]['test'][895] = True
        # Immediate target would close inside training, but potential hold extends out.
        self.s['exit']['target'] = .1
        row = simulate(p, index, self.r, self.s, start, end, training=True)
        self.assertEqual(row['purged_candidate_count'], 1)
        self.assertEqual(row['metrics']['trades'], 0)

    def test_next_open_closed_test_window_and_conserved_cash(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end)
        p['TEST'][2]['test'][:] = False;p['TEST'][2]['test'][895] = True
        row = simulate(p, index, self.r, self.s, start, end)
        t = row['trades'][0]
        self.assertEqual(t['entry_time'], self.bars[896]['time'])
        self.assertEqual(t['exit_reason'], 'study_end')
        self.assertEqual(t['exit_time'], self.bars[900]['end'])
        self.assertAlmostEqual(row['metrics']['ending_equity'], self.r['account']['capital'] + t['net_pnl'], places=2)
        self.assertTrue(all(p['cash'] >= 0 for p in row['daily_curve']))

    def test_future_prices_do_not_change_earlier_masks(self):
        p, _ = self.prepared(self.bars[700]['end'][:10])
        later, _ = self.prepared(self.bars[900]['end'][:10])
        np.testing.assert_array_equal(p['TEST'][2]['test'], later['TEST'][2]['test'][:701])

    def test_no_trades_keeps_cash_and_includes_idle_market_sessions(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end);p['TEST'][2]['test'][:] = False
        row = simulate(p, index, self.r, self.s, start, end)
        self.assertEqual(row['metrics']['ending_equity'], self.r['account']['capital'])
        self.assertGreaterEqual(len(row['daily_curve']), 21)
        self.assertEqual(row['metrics']['trades'], 0)

    def test_calendar_start_is_not_invented_as_a_trading_session(self):
        i = next(i for i in range(800, 850) if date.fromisoformat(self.bars[i]['time'][:10]).weekday() == 0)
        start = (date.fromisoformat(self.bars[i]['time'][:10]) - timedelta(days=1)).isoformat()
        end = self.bars[i+10]['end'][:10]
        p, index = self.prepared(end);p['TEST'][2]['test'][:] = False
        row = simulate(p, index, self.r, self.s, start, end)
        self.assertEqual(len(row['daily_curve']), 12)  # initial cash plus 11 sessions
        self.assertEqual(row['daily_curve'][0]['time'], start+' 00:00:00')
        self.assertNotIn(start+' 15:30:00', [p['time'] for p in row['daily_curve']])

    def test_provider_to_report_and_exact_cache_reuse(self):
        r = deepcopy(self.r)
        r['folds'] = [dict(train_start=self.bars[300]['time'][:10], train_end=self.bars[500]['end'][:10],
                           test_start=self.bars[501]['time'][:10], test_end=self.bars[700]['end'][:10])]
        history = self.bars
        class Provider:
            def load(self, symbols, cutoff, as_of=None):
                selected = [b for b in history if b['end'][:10] <= cutoff]
                return {'TEST': selected}, selected, {'provider': 'synthetic_test_fixture'}
        with tempfile.TemporaryDirectory() as folder:
            first = run(r, provider=Provider(), output=folder)
            second = run(r, provider=Provider(), output=folder)
            self.assertFalse(first['cache_hit'])
            self.assertTrue(second['cache_hit'])
            self.assertEqual(first['evidence'], second['evidence'])
            self.assertEqual(first['evidence']['prefix_checks'], 1)
            self.assertFalse(first['evidence']['promotion_allowed'])
            refreshed = run(r, provider=Provider(), output=folder, refresh=True)
            self.assertTrue(refreshed['evidence']['determinism_check']['passed'])
            self.assertNotEqual(first['report'], refreshed['report'])
            self.assertTrue(Path(first['evidence']['input_artifact']['path']).is_file())
            replay = run(r, provider=RecordedSnapshotProvider(first['evidence']['input_artifact']['path']), output=folder)
            self.assertTrue(replay['cache_hit'])
            self.assertEqual(replay['evidence']['folds'], first['evidence']['folds'])

    def test_unclosed_candle_is_rejected(self):
        from datetime import datetime, timezone
        r = deepcopy(self.r)
        r['folds'] = [dict(train_start=self.bars[300]['time'][:10], train_end=self.bars[500]['end'][:10],
                           test_start=self.bars[501]['time'][:10], test_end=self.bars[700]['end'][:10])]
        cutoff = datetime.fromisoformat(self.bars[700]['time']).replace(tzinfo=timezone.utc) - timedelta(hours=8)
        with self.assertRaisesRegex(ValueError, 'has not completed'):
            validate_data({'TEST': self.bars[:701]}, self.bars[:701], r, as_of=cutoff)

    def test_future_gap_flag_does_not_censor_a_signal(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end)
        p['TEST'][2]['test'][:] = False;p['TEST'][2]['test'][895] = True
        p['TEST'][0][896]['gap'] = True
        row = simulate(p, index, self.r, self.s, start, end)
        self.assertEqual(row['metrics']['trades'], 1)

    def test_benchmark_uses_price_before_start_not_start_days_close(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end);p['TEST'][2]['test'][:] = False
        row = simulate(p, index, self.r, self.s, start, end)
        expected = (self.bars[900]['close'] / self.bars[879]['close'] - 1) * 100
        self.assertAlmostEqual(row['metrics']['benchmark_return_pct'], expected)

    def test_first_session_loss_is_included_in_volatility(self):
        end = self.bars[900]['end'][:10];start = self.bars[880]['time'][:10]
        p, index = self.prepared(end);p['TEST'][2]['test'][:] = False
        row = simulate(p, index, self.r, self.s, start, end)
        account = dict(summary=dict(starting_capital=30000, ending_equity=27000, return_pct=-10), trades=[], curve=[], daily_curve=[
            dict(time=start+' 00:00:00', equity=30000), dict(time=start+' 15:30:00', equity=27000),
            dict(time=self.bars[881]['end'], equity=27000)])
        metrics = period_metrics(account, index, start, end)
        self.assertGreater(metrics['volatility_pct'], 0)


if __name__ == '__main__':
    unittest.main()
