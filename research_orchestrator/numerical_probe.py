"""Bounded, local integration experiment. No strategy is promoted by this probe."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import time
import warnings

from .feature_probe import load_pure_module
from .inventory import TERMINAL, WORKSPACE, inventory

SYMBOLS = ('TITAN', 'ICICIBANK', 'MARUTI')
# Fixed before looking at results; this is a four-candidate integration test.
VARIANTS = (
    ('original', 'NDP RSI(14) < 30'),
    ('stock_trend', 'Original AND Stock Miner distance above SMA(200) > 0'),
    ('market_trend', 'Original AND Nifty 50 close > SMA(200)'),
    ('volume', 'Original AND volume >= prior 20-session mean'),
)
FOLDS = (
    ('2017-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
    ('2018-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
    ('2019-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def benchmark(cutoff):
    from market_scanner.data import ROOT, connect_source, load_config
    with connect_source(ROOT / load_config()['database']) as con:
        rows = con.execute('SELECT bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? AND bar_time<=? ORDER BY bar_time',
                           ('NIFTY 50', cutoff + ' 23:59:59')).fetchall()
    bars = [dict(time=r[0][:10]+' 09:15:00', end=r[0][:10]+' 15:30:00',
                 open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5] or 0, gap=False) for r in rows]
    if not bars or any(not all(isinstance(b[k], (int, float)) and math.isfinite(b[k]) and b[k]>0 for k in ('open','high','low','close')) for b in bars):
        raise ValueError('Benchmark is missing or has invalid prices')
    return bars


def prepare(histories, index, cutoff, miner, ndp):
    """Engines receive only bars on/before this cutoff, including warm-up history."""
    import numpy as np
    import pandas as pd
    from market_scanner import strategy_core as core
    bench = [b for b in index if b['end'][:10] <= cutoff]
    prepared = {}
    for symbol, history in histories.items():
        bars = [b for b in history if b['end'][:10] <= cutoff]
        frame = pd.DataFrame([dict(trade_date=b['end'][:10], **{k:b[k] for k in ('open','high','low','close','volume')}) for b in bars])
        stock = miner.compute_features(frame)
        # An explicit feature allowlist excludes Stock Miner's future labels.
        above_sma = stock['d_sma200'].to_numpy() > 0
        inputs = {k:frame[c].to_numpy(float) for k,c in [('o','open'),('h','high'),('l','low'),('c','close'),('v','volume')]}
        signals = {(name, params):values for name,params,values in ndp.build_all(inputs)}
        base = signals[('RSI', 'n=14,os=30')] == 1
        base[:252] = False
        f = core.features(bars, bench)
        masks = dict(original=base, stock_trend=base & above_sma,
                     market_trend=base & core.condition_mask(dict(kind='market_trend',value=200), f),
                     volume=base & (f['volume_ratio'] >= 1))
        for key in masks:masks[key] = masks[key] & np.isfinite(f['atr'])
        prepared[symbol] = (bars, f, masks)
    return prepared, bench


def simulate(prepared, index, spec, variant, training=False):
    from market_scanner import strategy_core as core
    candidates = []
    histories = {}
    for symbol, (bars, f, masks) in prepared.items():
        mask = masks[variant];histories[symbol+'|1D'] = bars
        for i in range(len(bars)-1):
            if not mask[i] or not spec['start'] <= bars[i+1]['time'][:10] <= spec['end']:continue
            # Purge by the maximum possible holding horizon, not the observed exit.
            if training and (i+spec['exit']['hold'] >= len(bars) or bars[i+spec['exit']['hold']]['end'][:10] > spec['end']):continue
            t = core.trade_path(spec, bars, f, mask, i, end=spec['end'])
            if t:
                t.update(symbol=symbol, timeframe='1D', pattern='indicator_strategy', variant=variant,
                         id=f'{variant}-{symbol}-{i}', horizon_time=t['exit_time'])
                candidates.append(t)
    account = core.run_account(candidates, histories, spec)
    verify_account(account, spec)
    metrics = core.metrics(account, index, spec['start'], spec['end'])
    evidence = dict(metrics=metrics, trades=account['trades'], daily_curve=account['daily_curve'],
                    skipped=[dict(symbol=t['symbol'],signal_time=t['signal_time'],entry_time=t['entry_time'],reason=t['skipped_reason']) for t in account['skipped']],
                    candidate_count=len(candidates), period=dict(start=spec['start'],end=spec['end']),
                    accounting_checked=True)
    return evidence


def verify_account(account, spec):
    s = account['summary'];trades = account['trades']
    assert s['open_positions'] == 0, 'All trades must close inside the bounded test period'
    assert all(p['cash'] >= -.01 for p in account['daily_curve']), 'Cash borrowing detected'
    assert all(p['positions'] <= spec['max_positions'] for p in account['daily_curve'])
    gross = sum(t['quantity']*(t['exit']-t['entry']) for t in trades)
    fees = sum(t['costs'] for t in trades)
    assert abs(s['ending_equity']-(spec['capital']+gross-fees)) <= .011, 'Account does not reconcile'
    assert abs(s['total_costs']-fees) <= .011
    for t in trades:
        assert t['quantity'] >= 1 and int(t['quantity']) == t['quantity']
        assert t['signal_time'] < t['entry_time'] <= t['exit_time']
        assert spec['start'] <= t['entry_time'][:10] <= t['exit_time'][:10] <= spec['end']


def select(training, minimum=5):
    """Declared diagnostic heuristic. It is not a statistically validated ranker."""
    scores = {key:(row['metrics']['cagr']-.5*row['metrics']['max_drawdown_pct'])
              for key,row in training.items() if row['metrics']['trades'] >= minimum}
    return max(scores, key=lambda key:(scores[key], key)) if scores else None


def assess(testing, chosen, minimum=5):
    if chosen is None:
        return dict(status='insufficient_training_evidence',promotion_allowed=False)
    original = testing['original']['metrics'];selected = testing[chosen]['metrics']
    reasons = []
    if selected['trades'] < minimum:reasons.append(f"Only {selected['trades']} selected-strategy test trades; the diagnostic minimum is {minimum}.")
    improvement = selected['return_pct']-original['return_pct']
    if improvement <= 0:reasons.append('The training-selected variant did not improve total return in this later period.')
    reasons.append('Independent final holdout and data-quality validation remain required.')
    return dict(status='insufficient_test_evidence' if selected['trades'] < minimum else 'not_validated',
                return_difference_percentage_points=round(improvement,4),reasons=reasons,promotion_allowed=False)


def run():
    began = time.perf_counter()
    from market_scanner import backtest_store as store, strategy_core as core
    before = inventory()
    with store.connection() as con:run_id = store.active_run(con)
    histories = {symbol:store.load_history(run_id, symbol)['1D'][0] for symbol in SYMBOLS}
    index = benchmark(min(bars[-1]['end'][:10] for bars in histories.values()))
    miner, miner_hash = load_pure_module(TERMINAL/'stock_miner/indicators.py', 'research_probe_stock_features')
    ndp, ndp_hash = load_pure_module(TERMINAL/'ndp/indicator_library.py', 'research_probe_ndp_features')
    spec = core.validate(dict(name='RSI integration probe', universe='', symbols=list(SYMBOLS),
        start=FOLDS[0][0], end=FOLDS[-1][-1], capital=30000, max_positions=3, allocation_pct=100/3,
        risk_pct=2, ranking='symbol', fee_bps=30, slippage_bps=5,
        conditions=[dict(kind='rsi_below',value=30)], exit=dict(hold=20,stop=7,target=15)))
    folds = [];prefix_checks = 0
    with warnings.catch_warnings(record=True) as observed:
        warnings.simplefilter('always')
        for train_start,train_end,test_start,test_end in FOLDS:
            assert train_end < test_start
            training_data, train_index = prepare(histories,index,train_end,miner,ndp)
            train_spec = dict(spec,start=train_start,end=train_end)
            training = {key:simulate(training_data,train_index,train_spec,key,True) for key,_ in VARIANTS}
            chosen = select(training)
            # Freeze the decision before computing any candidate's future outcomes.
            selection = dict(chosen=chosen,training_end=train_end,training_evidence_hash=digest(training))
            test_data, test_index = prepare(histories,index,test_end,miner,ndp)
            import numpy as np
            for symbol in SYMBOLS:
                n = len(training_data[symbol][0])
                for key,_ in VARIANTS:
                    prefix_checks += 1
                    assert np.array_equal(training_data[symbol][2][key],test_data[symbol][2][key][:n]), 'Later bars changed earlier qualification'
            test_spec = dict(spec,start=test_start,end=test_end)
            testing = {key:simulate(test_data,test_index,test_spec,key) for key in dict.fromkeys(['original']+([chosen] if chosen else []))}
            folds.append(dict(train_start=train_start,train_end=train_end,test_start=test_start,test_end=test_end,
                              selection=selection,training=training,testing=testing,assessment=assess(testing,chosen)))
    after = inventory();hashes = {row['id']:row.get('sha256') for row in before['engines']}
    changed = [row['id'] for row in after['engines'] if hashes[row['id']] != row.get('sha256')]
    assert not changed, 'Existing source engines changed during the probe'
    result = dict(created=datetime.now(timezone.utc).isoformat(),status='integration_probe_only',data_run=run_id,
        seconds=round(time.perf_counter()-began,3),timing_scope='One local run including imports, reads, features and tests; excludes report serialization and process startup. Not a p95 SLA.',
        symbols=list(SYMBOLS),data_hashes={**{s:digest(b) for s,b in histories.items()},'NIFTY 50':digest(index)},
        engine_hashes=dict(stock_features=miner_hash,ndp_signals=ndp_hash,strategy_core=hashlib.sha256((WORKSPACE/'market_scanner/strategy_core.py').read_bytes()).hexdigest(),
                           portfolio_accounting=hashes['portfolio_accounting']),spec=spec,
        signal_definition='Entry masks use the named NDP RSI variant, not strategy_core RSI. Stock SMA filter comes from Stock Miner. Both engine versions are fingerprinted.',
        variants=[dict(id=key,rule=label) for key,label in VARIANTS],folds=folds,
        prefix_checks=prefix_checks,existing_sources_changed=changed,warnings=sorted({str(w.message) for w in observed}),
        selection_rule='Highest training CAGR minus 0.5 times drawdown among candidates with >=5 trades. This heuristic is diagnostic, not a promotion rule.',
        promotion_allowed=False,limitations=[
            'Three manually selected surviving stocks, four fixed candidates and three folds. No whole-market or general profitability claim.',
            'Raw price adjustment, delisted stocks and historical constituent membership are unverified. No deployment qualification.',
            'Nifty 50 price index benchmark excludes dividends and index investment costs.',
            'Annual test accounts independently start with INR 30,000; folds must not be added or compounded into a continuous portfolio.',
            'Training purges entries whose maximum holding horizon crosses its end; test positions close at each fold end.',
            '30 bps fees per round trip charged against entry notional and 5 bps slippage on each side are illustrative, not a verified broker tariff.',
            'Daily candle stop/target ordering assumes stop first and flags uncertain excursions. Drawdown uses observed open/close marks, not all intraday movements.',
            'Repeated selection and small samples require a separate final holdout and broader robustness testing before recommending improvements.',
            'This command uses an explicit RSI experiment. It does not execute arbitrary natural-language requests.'])
    folder = WORKSPACE/'reports/research-orchestrator';folder.mkdir(parents=True,exist_ok=True)
    path = folder/('numerical-integration-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.json')
    path.write_text(json.dumps(result,indent=2,allow_nan=False),encoding='utf-8')
    compact = dict(seconds=result['seconds'],folds=len(folds),candidates_per_training=4,prefix_checks=prefix_checks,
        existing_sources_changed=changed,report=str(path),results=[dict(year=f['test_start'][:4],selected=f['selection']['chosen'],
            accounts={key:dict(ending_equity=value['metrics']['ending_equity'],trades=value['metrics']['trades'],return_pct=value['metrics']['return_pct']) for key,value in f['testing'].items()}) for f in folds])
    print(json.dumps(compact,indent=2));return result


if __name__ == '__main__':run()
