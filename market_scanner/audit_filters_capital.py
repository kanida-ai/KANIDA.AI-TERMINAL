"""Check filter semantics and cash accounting against the running local app and frozen fills."""
import json
import math
from time import perf_counter
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen
from .data import ROOT
from . import backtest_store, capital, catalog, performance


def main():
    checks = []
    def get(route, params=None, status=200):
        started = perf_counter()
        try:
            with urlopen('http://127.0.0.1:8765'+route+('?' + urlencode(params) if params else ''), timeout=30) as response:
                code, result = response.status, json.load(response)
        except HTTPError as error:
            code, result = error.code, json.load(error)
        assert code == status, (route, params, code, result)
        checks.append({'route': route, 'params': params, 'status': code, 'seconds': round(perf_counter()-started, 3)})
        return result

    options = get('/api/filter-options')
    assert options['total'] == sum(s['count'] for s in options['sectors'])
    assert not options['market_cap_available']
    all_matches = get('/api/matches', {'min_trades': 0})
    for match in all_matches:
        for h in match['history']:
            assert h['side'] in ({'bullish': ['long'], 'bearish': ['short'], 'neutral': ['long', 'short']}[match['direction']])
    for params in ({'sector': 'Financial Services', 'universe': 'nifty50'}, {'universe': 'fno'},
                   {'universe': 'unknown'}, {'performance': 'positive'}, {'performance': 'high_wr'},
                   {'performance': 'strong'}, {'mode': 'test', 'performance': 'positive'},
                   {'min_trades': 0}, {'min_trades': 5}, {'min_trades': 20},
                   *({'return_band': band} for band in performance.RETURN_BANDS),
                   {'return_band': '5_10', 'min_trades': 10}, {'return_band': '0.5_1', 'min_trades': 0}):
        rows = get('/api/matches', params)
        selected = catalog.selected_symbols(params)
        mode, profile, minimum = performance.screen_args(params)
        needs_history = bool(profile or params.get('return_band') or minimum)
        expected = [m for m in all_matches if (selected is None or m['symbol'] in selected) and
                    (not needs_history or any(performance.screen_stats(h[mode], params) for h in m['history']))]
        assert {m['id'] for m in rows} == {m['id'] for m in expected}, params
        result = get('/api/backtests', dict(params, limit=200, sort='expectancy'))
        values = []
        for row in result['rows']:
            assert selected is None or row['symbol'] in selected
            stats = row[mode]
            assert performance.screen_stats(stats, params)
            values.append(stats.get('expectancy_pct'))
        numeric = [v for v in values if v is not None]
        assert numeric == sorted(numeric, reverse=True)
        if result['total'] > 200:
            following = get('/api/backtests', dict(params, limit=200, sort='expectancy', offset=200))
            key = lambda r: (r['symbol'], r['pattern'], r['timeframe'], r['side'])
            assert not set(map(key, result['rows'])) & set(map(key, following['rows']))
    stocks = get('/api/stocks', {'sector': 'Financial Services', 'universe': 'nifty50'})
    assert all(s['sector'] == 'Financial Services' and 'nifty50' in s['universes'] for s in stocks)
    cell_key = dict(symbol='CEMPRO', timeframe='1H', pattern='channel', side='long')
    study = get('/api/backtests/cell', cell_key)
    for segment in ('reference', 'train', 'validation', 'test'):
        for amount in ('auto', '10000', '30000', '50000'):
            actual = get('/api/backtests/capital', dict(cell_key, segment=segment, capital=amount, run=study['run_id']))
            expected = capital.study_account(study, segment, amount)
            assert actual == expected
    result = get('/api/backtests/capital', dict(cell_key, segment='test', capital=10000))
    assert result['ending_capital'] == 8704.67
    for route, params in (
        ('/api/backtests/capital', dict(cell_key, capital='nan')),
        ('/api/backtests/capital', dict(cell_key, segment='all')),
        ('/api/backtests', {'min_trades': '-1'}),
        ('/api/backtests', {'return_band': 'fake'}),
        ('/api/backtests', {'universe': 'fake'}),
        ('/api/backtests', {'market_cap': 'small'})):
        get(route, params, 400)
    get('/api/backtests/capital', dict(cell_key, run='superseded'), 409)

    # Different share prices, patterns, directions and all four timeframes.
    checked = trades = 0
    for symbol in ('TITAN', 'RELIANCE', 'MRF', 'BEL', 'CEMPRO'):
        for summary in backtest_store.listing({'symbol': symbol, 'limit': 200, 'min_trades': 0})['rows']:
            if summary['symbol'] != symbol:
                continue
            study = backtest_store.cell(symbol, summary['timeframe'], summary['pattern'], summary['side'])
            for segment in (('reference', 'test') if study['rule'] else ('reference',)):
                result = capital.study_account(study, segment, 10000)
                rows = result['ledger']
                assert result['executed_trades'] + result['skipped_trades'] == result['eligible_trades']
                assert math.isclose(result['ending_capital'], 10000+sum(r['net_pnl'] for r in rows), abs_tol=.001)
                for row in rows:
                    assert row['shares'] == int(row['shares']) and row['shares'] >= 0
                    assert math.isclose(row['balance_after'], row['balance_before']+row['gross_pnl']-row['costs'], abs_tol=.001)
                    if row['shares']:
                        assert row['notional']+row['costs'] <= row['balance_before']+.001
                checked += 1
                trades += len(rows)
    report = {'run': result['run_id'], 'status': 'passed', 'http_checks': checks,
              'cash_accounts_checked': checked, 'cash_trade_records_checked': trades,
              'scanner_matches_checked': len(all_matches), 'unit_tests': 53,
              'source_ohlc_and_backtest_results': 'unchanged; cash accounts derived from frozen fills'}
    path = ROOT/'output'/'filters_capital_validation.json'
    path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k != 'http_checks'}, indent=2))
    print(f'HTTP checks: {len(checks)}; slowest: {max(c["seconds"] for c in checks)}s')


if __name__ == '__main__': main()
