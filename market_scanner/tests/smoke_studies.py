"""Opt-in integration smoke over the installed immutable research snapshot."""
import json
from pathlib import Path
from market_scanner import studies


def main():
    base = dict(symbols=['LTTS', 'TITAN'], patterns=[], timeframes=['1H','4H','1D','1W'],
                start='2020-01-01', end='2026-07-31', capital=30000, max_positions=2,
                allocation_pct=50, benchmark=True)
    scenarios = [base, dict(base, symbols=['TITAN'], patterns=['channel'], timeframes=['1H'],
                           method='walkforward', minimum_training=5, training_months=60, test_months=12)]
    checks = []
    for settings in scenarios:
        job = studies.start('workflow_qa', settings)
        future = studies.FUTURES.get(job['id'])
        if future:future.result()
        item = studies.get_job('workflow_qa', job['id'], True)
        assert item['status'] == 'complete', item['message']
        result = item['result']
        assert not result['summary']['open_positions']
        assert min(p['cash'] for p in result['curve']) >= -.01
        assert abs(sum(t['net_pnl'] for t in result['trades']) - result['summary']['net_profit']) < .02 * len(result['trades']) + .01
        for trade in result['trades'][:4] + result['trades'][-4:]:
            bundle = studies.trade_chart('workflow_qa', item['id'], trade['id'])
            assert bundle['shape'], trade
            assert bundle['bars'][bundle['signal_index']]['end'] == trade['signal_time']
        if result['trades']:
            trade = result['trades'][0]
            module, config = studies.frozen(item['settings']['run'])
            bars = studies.store.load_history(item['settings']['run'], trade['symbol'])[trade['timeframe']][0]
            changed = [dict(b) for b in bars]
            for candle in changed[trade['signal_index']+1:]:
                candle.update(open=1, high=5000, low=.1, close=2)
            original = module.HistoricalReplay(bars, config['history_bars'], [trade['pattern']]).at(trade['signal_index'])
            mutated = module.HistoricalReplay(changed, config['history_bars'], [trade['pattern']]).at(trade['signal_index'])
            assert original == mutated, 'Future candles altered an earlier formation'
        for skipped in result['skipped'][:2]:
            bundle = studies.trade_chart('workflow_qa', item['id'], skipped['id'])
            assert bundle['trade'] is None
        try:
            studies.get_job('other_owner', item['id'])
            raise AssertionError('Cross-account access')
        except ValueError:
            pass
        checks.append(dict(id=item['id'], status=item['status'], summary=result['summary'],
                           occurrences=len(result['occurrences']), folds=result['folds']))
        print(json.dumps({k:v for k,v in checks[-1].items() if k!='folds'}), flush=True)
    Path('reports/ux-benchmark/study-engine-smoke.json').write_text(json.dumps(checks, indent=2))
    print('SMOKE PASSED', flush=True)


if __name__ == '__main__':main()
