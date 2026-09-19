import json
import unittest
from datetime import date

from market_scanner import performance, backtest_store
from market_scanner.backtest import Rule, simulate, rules_config
from market_scanner.data import Calendar, load_config
import test_filters_capital as fixtures
stats = fixtures.stats


class ReturnRangeTests(unittest.TestCase):
    def setUp(self):
        fixtures.FilterTests.setUp(self)
        self.values = [(4, .74), (5, .498399957), (5, 1), (6, 2), (8, 5), (10, 10),
                       (12, 10.01), (10, -.2), (0, None), (5, .1), (5, .995), (5, 4.995), (5, 0)]
        with backtest_store.connection() as con:
            con.execute('DELETE FROM cells')
            for i, (n, mean) in enumerate(self.values):
                symbol = f'R{i:02d}'
                row = dict(symbol=symbol, pattern='cup_handle', timeframe='1D', side='long', status='no_validated_rule',
                    reference=dict(stats(n, mean), rule={'hold': 10}), test=stats(0, None))
                con.execute('INSERT INTO cells VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    ('run1', symbol, '1D', 'cup_handle', 'long', 'no_validated_rule', n, 0, None, json.dumps(row), b''))

    def tearDown(self):fixtures.FilterTests.tearDown(self)

    def test_default_five_and_independent_count_filter(self):
        default = backtest_store.listing({})
        self.assertEqual(default['total'], 11)
        self.assertTrue(all(r['reference']['n'] >= 5 for r in default['rows']))
        self.assertEqual(backtest_store.listing({'min_trades': 0})['total'], 13)
        self.assertEqual(backtest_store.listing({'min_trades': 10})['total'], 3)
        self.assertEqual(backtest_store.listing({'min_trades': 20})['total'], 0)
        self.assertEqual(backtest_store.listing({'return_band': '0.5_1'})['total'], 1)
        self.assertEqual(backtest_store.listing({'return_band': '0.5_1', 'min_trades': 4})['total'], 2)

    def test_band_boundaries_are_disjoint_and_match_displayed_number(self):
        all_ids = set()
        for band in performance.RETURN_BANDS:
            rows = backtest_store.listing({'return_band': band, 'min_trades': 0})['rows']
            ids = {r['symbol'] for r in rows}
            self.assertFalse(all_ids & ids)
            all_ids |= ids
            for row in rows:
                s = row['reference']
                self.assertEqual(s['return_band'], band)
                self.assertEqual(performance.return_band(s['expectancy_pct']), band)
                self.assertTrue(performance.screen_stats(s, {'return_band': band, 'min_trades': 0}))
        self.assertEqual(len(all_ids), 11)  # Negative and missing returns are not positive-range results.
        self.assertEqual(performance.return_band(.498399957), '0.5_1')
        self.assertEqual(performance.return_band(.995), '1_2')
        self.assertEqual(performance.return_band(2), '2_5')
        self.assertEqual(performance.return_band(4.995), '5_10')
        self.assertEqual(performance.return_band(10), '5_10')
        self.assertEqual(performance.return_band(10.01), 'over_10')

    def test_filtering_precedes_pagination_and_invalid_values_fail(self):
        result = backtest_store.listing({'return_band': '5_10', 'limit': 1})
        self.assertEqual(result['total'], 3)
        next_page = backtest_store.listing({'return_band': '5_10', 'limit': 1, 'offset': 1})
        self.assertNotEqual(result['rows'][0]['symbol'], next_page['rows'][0]['symbol'])
        for query in ({'return_band': 'made_up'}, {'min_trades': -1}, {'min_trades': '5.5'}, {'min_trades': 'nan'}):
            with self.subTest(query=query), self.assertRaises(ValueError):backtest_store.listing(query)

    def test_unknown_history_does_not_become_zero_return(self):
        row = next(r for r in backtest_store.listing({'min_trades': 0})['rows'] if r['symbol'] == 'R08')
        self.assertIsNone(row['reference']['display_return_pct'])
        self.assertIsNone(row['reference']['return_band'])
        self.assertFalse(performance.screen_stats(row['reference'], {'return_band': '0_0.5', 'min_trades': 0}))


class HoldingExplanationTests(unittest.TestCase):
    def test_holding_descriptions_and_older_selected_rule_summary(self):
        for tf, hold, duration in [('1H', 6, '1–2 trading days'), ('4H', 6, '3–4 trading days'),
                                    ('1D', 10, '10 trading days'), ('1W', 4, 'About 4 weeks')]:
            value = performance.holding_details(tf, hold)
            self.assertEqual(value['duration'], duration)
            self.assertTrue(value['can_carry_overnight'])
        self.assertFalse(performance.holding_details('1H', 1)['can_carry_overnight'])
        old = dict(timeframe='1H', rule_description='setup; next open; 1 ATR stop / 1R target; max 6 candles',
                   test=stats(5, .75), reference=dict(stats(10, 1), rule={'hold': 6}))
        result = performance.decorate(old)
        self.assertEqual(result['test']['holding']['bars'], 6)
        self.assertTrue(result['test']['holding']['maximum'])
        self.assertNotIn('holding', old['test'])  # Presentation does not mutate saved research.

    def test_six_hourly_candles_can_exit_on_the_next_trading_day(self):
        cal = Calendar(load_config())
        bars = [dict(time=a.isoformat(' '), end=b.isoformat(' '), open=100, high=101, low=99,
                     close=100, volume=1, gap=False)
                for day in (date(2026, 7, 27), date(2026, 7, 28)) for a, b in cal.intervals(day, '1H')]
        event = dict(signal_index=3, state='setup', atr=1, episode=1, score=80,
                     direction='bullish', pattern_start=bars[0]['time'])
        trades, _ = simulate([event], bars, 'long', Rule('setup', 6), 0, len(bars), rules_config())
        self.assertEqual(trades[0]['entry_time'], '2026-07-27 13:15:00')
        self.assertEqual(trades[0]['exit_candle_end'], '2026-07-28 12:15:00')
        self.assertEqual(trades[0]['holding_bars'], 6)


if __name__ == '__main__': unittest.main()
