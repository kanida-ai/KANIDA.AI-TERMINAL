import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from market_scanner import backtest_store, capital, catalog, performance


def trade(index, entry=100, exit_price=110, cost=.4):
    return {'entry_index': index, 'exit_index': index+1, 'signal_index': index-1,
            'entry_time': f'2020-01-{index:02d} 09:15:00', 'exit_candle_end': f'2020-01-{index+1:02d} 15:30:00',
            'entry': entry, 'exit': exit_price, 'cost_pct': cost, 'exit_reason': 'time'}


class CapitalTests(unittest.TestCase):
    def test_whole_shares_cost_reserve_and_compounding(self):
        result = capital.simulate_account([trade(1), trade(3, exit_price=90)], 'long', 1000)
        a, b = result['ledger']
        self.assertEqual((a['shares'], a['costs'], a['reserved_cash'], a['balance_after']), (9, 3.6, 96.4, 1086.4))
        self.assertEqual((b['shares'], b['balance_after']), (10, 982.4))
        self.assertAlmostEqual(result['growth_pct'], -1.76)
        for row in result['ledger']:
            self.assertGreaterEqual(row['reserved_cash'], 0)
            self.assertAlmostEqual(row['balance_before']+row['gross_pnl']-row['costs'], row['balance_after'])
            self.assertAlmostEqual(row['notional']+row['costs']+row['reserved_cash'], row['balance_before'])

    def test_unaffordable_trade_is_skipped_without_cash_topup(self):
        result = capital.simulate_account([trade(1, 12000, 13000), trade(3, 1000, 1100)], 'long', 10000)
        self.assertEqual(result['skipped_trades'], 1)
        self.assertEqual(result['ledger'][0]['balance_after'], 10000)
        self.assertEqual(result['ending_capital'], 10864)
        self.assertEqual(result['executed_trades'], 1)

    def test_auto_budget_uses_first_entry_not_future_prices_or_returns(self):
        result = capital.simulate_account([trade(1, 12000, 1000), trade(3, 100000, 300000)], 'long')
        self.assertEqual(result['starting_capital'], 30000)
        changed = capital.simulate_account([trade(1, 12000, 100000), trade(3, 100, 500)], 'long')
        self.assertEqual(changed['starting_capital'], result['starting_capital'])
        self.assertEqual(capital.simulate_account([trade(1, 51000, 52000)], 'long')['starting_capital'], 52000)

    def test_short_proceeds_are_not_spendable_and_insolvency_stops_trading(self):
        win = capital.simulate_account([trade(1, 100, 90)], 'short', 1000)
        self.assertEqual(win['ending_capital'], 1086.4)
        loss = capital.simulate_account([trade(1, 100, 300), trade(3, 1, .5)], 'short', 1000)
        self.assertEqual(loss['ending_capital'], -803.6)
        self.assertTrue(loss['depleted'])
        self.assertEqual(loss['ledger'][1]['shares'], 0)
        self.assertEqual(loss['ledger'][1]['skipped_reason'], 'Account depleted')

    def test_empty_account_and_invalid_inputs(self):
        result = capital.simulate_account([], 'long', 10000)
        self.assertEqual(result['ending_capital'], 10000)
        self.assertIsNone(result['cash_trade_win_rate'])
        for value in ('nan', 'inf', 0, -100, 'abc', 1000000001):
            with self.subTest(value=value), self.assertRaises(ValueError):
                capital.simulate_account([], 'long', value)
        with self.assertRaises(ValueError):capital.simulate_account([trade(1), trade(2)], 'long')
        with self.assertRaises(ValueError):capital.simulate_account([trade(1, 0)], 'long')

    def test_segments_restart_cash_and_do_not_mutate_frozen_study(self):
        study = dict(run_id='frozen', symbol='AAA', pattern='channel', timeframe='1D', side='long',
                     rule={'hold': 3}, reference={'rule': {'hold': 2}}, reference_trades=[trade(1)],
                     trades=[dict(trade(1), split='train'), dict(trade(4, 100, 90), split='test')])
        original = copy.deepcopy(study)
        training = capital.study_account(study, 'train', 1000)
        test = capital.study_account(study, 'test', 1000)
        self.assertEqual(training['ending_capital'], 1086.4)
        self.assertEqual(test['starting_capital'], 1000)
        self.assertEqual(test['ending_capital'], 906.4)
        self.assertEqual(study, original)
        self.assertEqual(test['run_id'], 'frozen')
        study['rule'] = None
        with self.assertRaises(ValueError):capital.study_account(study, 'test')


def stats(n, expectancy, wr=65, lower=.2):
    return dict(n=n, expectancy_pct=expectancy, win_rate=wr, expectancy_ci95=[lower, 2])


class FilterTests(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            'AAA': dict(sector='Finance', universes=['nifty50', 'fno'], membership_unknown=False),
            'BBB': dict(sector='Technology', universes=['nifty100'], membership_unknown=False),
            'CCC': dict(sector='Unclassified', universes=[], membership_unknown=True)}
        self.catalog_patch = patch.object(catalog, 'labels', return_value=self.metadata)
        self.catalog_patch.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(backtest_store, 'DB', Path(self.tmp.name)/'research.sqlite3')
        self.db_patch.start()
        backtest_store.initialize()
        self.summaries = [
            dict(symbol='AAA', side='long', reference=stats(30, 1), test=stats(21, -.2, lower=-.5)),
            dict(symbol='BBB', side='long', reference=stats(19, 2), test=stats(25, 1, lower=.1)),
            dict(symbol='CCC', side='long', reference=stats(40, -1, 80), test=stats(0, None, None, None)),
        ]
        with backtest_store.connection() as con:
            con.execute("INSERT INTO settings VALUES('active_run','run1')")
            for summary in self.summaries:
                summary.update(pattern='channel', timeframe='1D', status='tested')
                con.execute('INSERT INTO cells VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                            ('run1', summary['symbol'], '1D', 'channel', 'long', 'tested', 50, summary['test']['n'],
                             summary['test']['expectancy_pct'], json.dumps(summary), b''))

    def tearDown(self):
        self.db_patch.stop();self.catalog_patch.stop();self.tmp.cleanup()

    def test_sector_and_universe_are_intersection_and_unknown_stays_unknown(self):
        self.assertEqual(catalog.selected_symbols({'sector': 'Finance', 'universe': 'nifty50'}), ['AAA'])
        self.assertEqual(catalog.selected_symbols({'sector': 'Finance', 'universe': 'nifty100'}), [])
        self.assertEqual(catalog.selected_symbols({'universe': 'unknown'}), ['CCC'])
        with self.assertRaises(ValueError):catalog.selected_symbols({'universe': 'smallcap'})
        with self.assertRaises(ValueError):catalog.selected_symbols({'market_cap': 'small'})

    def test_profit_filters_apply_before_pagination_and_require_sample(self):
        result = backtest_store.listing({'performance': 'high_wr', 'limit': 1, 'min_trades': 20})
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['rows'][0]['symbol'], 'AAA')
        self.assertEqual(backtest_store.listing({'performance': 'high_wr', 'offset': 1, 'min_trades': 20})['rows'], [])
        result = backtest_store.listing({'performance': 'high_wr', 'min_trades': 10})
        self.assertEqual(result['total'], 2)
        self.assertNotIn('CCC', [r['symbol'] for r in result['rows']])  # High WR alone can still lose money.

    def test_strong_screen_requires_heldout_evidence(self):
        result = backtest_store.listing({'performance': 'strong', 'mode': 'reference', 'min_trades': 1})
        self.assertEqual(result['mode'], 'test')
        self.assertEqual([r['symbol'] for r in result['rows']], ['BBB'])
        self.assertEqual(backtest_store.listing({'performance': 'positive', 'mode': 'test', 'sector': 'Finance'})['total'], 0)

    def test_sql_and_scanner_tags_agree_and_sort_uses_selected_basis(self):
        for mode in ('reference', 'test'):
            for profile in performance.PROFILES:
                actual = backtest_store.listing(dict(mode=mode, performance=profile, min_trades=20))
                effective = 'test' if profile == 'strong' else mode
                expected = [s['symbol'] for s in self.summaries if s[effective]['n'] >= 20 and profile in performance.profiles(s[effective], effective)]
                self.assertEqual([r['symbol'] for r in actual['rows']], expected)
        baseline = backtest_store.listing({'mode': 'reference', 'sort': 'expectancy'})
        self.assertEqual(baseline['rows'][0]['symbol'], 'BBB')
        self.assertEqual(backtest_store.listing({'sector': 'Finance', 'universe': 'nifty100'})['total'], 0)

    def test_invalid_screen_inputs(self):
        for filters in ({'mode': 'pooled'}, {'performance': 'best'}, {'min_trades': 'NaN'}, {'min_trades': '-1'}):
            with self.subTest(filters=filters), self.assertRaises(ValueError):backtest_store.listing(filters)


if __name__ == '__main__': unittest.main()
