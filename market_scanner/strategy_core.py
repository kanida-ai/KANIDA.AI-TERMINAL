"""Auditable strategy DSL and causal daily-equity research for the local pilot.

No expressions are evaluated as code. All features use a completed candle or
earlier observations; order admission never sees a future trade outcome.
"""
from __future__ import annotations
from bisect import bisect_right
from collections import defaultdict
from copy import deepcopy
from datetime import date
import math
import statistics as stats

from .study_engine import portfolio, add_months

VERSION = 'strategy-1.0.0'
CONDITIONS = {
    'breakout': dict(label='Price breaks a prior high', value=252, unit='trading days', low=5, high=504),
    'dip': dict(label='Below the prior 52-week high', value=10, unit='% or more', low=.1, high=90),
    'rsi_below': dict(label='RSI (14) below', value=30, unit='RSI', low=1, high=99),
    'rsi_above': dict(label='RSI (14) above', value=55, unit='RSI', low=1, high=99),
    'above_sma': dict(label='Stock above its moving average', value=200, unit='trading days', low=5, high=504),
    'volume': dict(label='Volume / prior 20-day average', value=1.2, unit='times or more', low=.1, high=20),
    'momentum': dict(label='63-day price momentum', value=10, unit='% or more', low=-90, high=500),
    'liquidity': dict(label='Prior 20-day average turnover', value=1, unit='₹ crore or more', low=.01, high=10000),
    'market_trend': dict(label='Nifty 50 above its moving average', value=200, unit='trading days', low=5, high=504),
    'relative_strength': dict(label='63-day return above Nifty 50', value=0, unit='percentage points', low=-100, high=100),
    'volatility': dict(label='ATR (14) / price below', value=5, unit='%', low=.1, high=50),
    'pattern': dict(label='Chart pattern qualifies', value='cup_handle', unit='pattern'),
}
UNAVAILABLE = {
    'market_cap': 'Point-in-time market capitalization is not supplied.',
    'revenue_growth': 'Point-in-time reported revenue and publication dates are not supplied.',
    'quality': 'Point-in-time profitability, quality and valuation histories are not supplied.',
    'sector_strength': 'A verified historical sector-to-index mapping is not supplied.',
    'institutional': 'Point-in-time institutional flows are not supplied.',
}


def numeric(value, default, low, high, integer=False):
    try:x = float(default if value is None else value)
    except (TypeError, ValueError):raise ValueError('Enter a valid number') from None
    if not math.isfinite(x) or not low <= x <= high or integer and x != int(x):
        raise ValueError(f'Value must be {low}–{high}' + (' (whole number)' if integer else ''))
    return int(x) if integer else x


def validate(spec):
    from .detectors import NAMES
    if not isinstance(spec, dict):raise ValueError('A strategy is required')
    source = spec.get('conditions', [])
    if not isinstance(source, list) or not 1 <= len(source) <= 12:raise ValueError('Choose 1–12 entry conditions')
    conditions = []
    for i, b in enumerate(source):
        if not isinstance(b, dict):raise ValueError('Invalid condition block')
        kind = b.get('kind')
        if kind in UNAVAILABLE:raise ValueError(UNAVAILABLE[kind])
        if kind not in CONDITIONS:raise ValueError('Unsupported condition: ' + str(kind))
        definition = CONDITIONS[kind]
        v = b.get('value', definition['value'])
        if kind == 'pattern':
            if v not in NAMES:raise ValueError('Choose an approved chart pattern')
        else:v = numeric(v, definition['value'], definition['low'], definition['high'], kind in ('breakout','above_sma','market_trend'))
        conditions.append(dict(id=str(b.get('id', 'c'+str(i)))[:50], kind=kind, value=v))
    start, end = str(spec.get('start', '2016-07-29')), str(spec.get('end', '2026-07-29'))
    date.fromisoformat(start);date.fromisoformat(end)
    if start >= end:raise ValueError('Start must be before end')
    universe = str(spec.get('universe', 'nifty50'))
    if universe not in ('', 'nifty50','nifty100','nifty200','nifty500','fno'):raise ValueError('That universe is not available in the supplied labels')
    symbols = spec.get('symbols', [])
    if not isinstance(symbols, list) or len(symbols)>1500 or any(not isinstance(s,str) or len(s)>45 for s in symbols):raise ValueError('Invalid stock selection')
    join = spec.get('join', 'all')
    if join not in ('all','any'):raise ValueError('Choose all or any entry conditions')
    ranking = spec.get('ranking','momentum')
    if ranking not in ('momentum','liquidity','symbol'):raise ValueError('Choose a supported ranking')
    rebalance = spec.get('rebalance','daily')
    if rebalance not in ('daily','weekly','monthly'):raise ValueError('Choose a review schedule')
    exit_data = spec.get('exit', {})
    stop_kind = exit_data.get('stop_kind','percent')
    if stop_kind not in ('percent','atr'):raise ValueError('Choose percentage or ATR stop')
    out = dict(name=str(spec.get('name','My strategy'))[:100], universe=universe, symbols=sorted(set(s.upper().strip() for s in symbols if s.strip())),
        sector=str(spec.get('sector',''))[:80], timeframe='1D', product='CNC', side='long', conditions=conditions, join=join,
        ranking=ranking, rebalance=rebalance, start=start, end=end,
        capital=numeric(spec.get('capital'),100000,100,1e9), max_positions=numeric(spec.get('max_positions'),10,1,100,True),
        allocation_pct=numeric(spec.get('allocation_pct'),10,.1,100), risk_pct=numeric(spec.get('risk_pct'),2,.01,100),
        fee_bps=numeric(spec.get('fee_bps'),30,0,1000), slippage_bps=numeric(spec.get('slippage_bps'),5,0,1000),
        exit=dict(hold=numeric(exit_data.get('hold'),40,1,260,True),stop_kind=stop_kind,
            stop=numeric(exit_data.get('stop'),7,.1,50), target=numeric(exit_data.get('target'),15,.1,200),
            trailing=numeric(exit_data.get('trailing'),0,0,50), disqualify=exit_data.get('disqualify',False) is True),
        objectives=dict(cagr=numeric(spec.get('objectives',{}).get('cagr'),0,0,200), drawdown=numeric(spec.get('objectives',{}).get('drawdown'),15,1,100),
            beat_nifty=spec.get('objectives',{}).get('beat_nifty',True) is True),
        train_months=numeric(spec.get('train_months'),36,12,120,True),test_months=numeric(spec.get('test_months'),12,3,24,True),
        min_trades=numeric(spec.get('min_trades'),5,5,200,True))
    if spec.get('timeframe','1D')!='1D' or spec.get('product','CNC')!='CNC' or spec.get('side','long')!='long':
        raise ValueError('Strategy Lab currently executes daily CNC long equities. Intraday and short pattern studies remain in Simulate.')
    return out


def rolling(values, n, mode='mean', prior=False):
    """Finite, full-window only; no backfill at the beginning of history."""
    import numpy as np
    from numpy.lib.stride_tricks import sliding_window_view
    a=np.asarray(values,dtype=float);out=np.full(len(a),np.nan)
    if len(a)>=n:
        w=sliding_window_view(a,n)
        out[n-1:]=np.mean(w,axis=1) if mode=='mean' else np.max(w,axis=1)
    return np.r_[np.nan,out[:-1]] if prior else out


def features(bars, benchmark):
    import numpy as np
    c=np.array([b['close'] for b in bars],float);h=np.array([b['high'] for b in bars],float);l=np.array([b['low'] for b in bars],float);v=np.array([b['volume'] for b in bars],float)
    previous=np.r_[c[0],c[:-1]];tr=np.maximum(h-l,np.maximum(abs(h-previous),abs(l-previous)))
    delta=np.r_[0,np.diff(c)];g=rolling(np.maximum(delta,0),14);loss=rolling(np.maximum(-delta,0),14)
    # Wilder RSI seeded with 14 actual close-to-close changes, not a synthetic first gain.
    rsi=np.full(len(c),np.nan)
    if len(c)>14:
        ag=float(np.maximum(delta[1:15],0).mean());al=float(np.maximum(-delta[1:15],0).mean())
        for i in range(14,len(c)):
            if i>14:ag=(ag*13+max(delta[i],0))/14;al=(al*13+max(-delta[i],0))/14
            rsi[i]=50 if ag==al==0 else 100 if al==0 else 100-100/(1+ag/al)
    mom=np.full(len(c),np.nan)
    if len(c)>63:mom[63:]=(c[63:]/c[:-63]-1)*100
    times=[b['end'][:10] for b in benchmark];bc=np.array([b['close'] for b in benchmark],float)
    bm=np.full(len(bc),np.nan)
    if len(bc)>63:bm[63:]=(bc[63:]/bc[:-63]-1)*100
    indices=[bisect_right(times,b['end'][:10])-1 for b in bars]
    bmoment=np.array([bm[i] if i>=0 else np.nan for i in indices])
    return dict(close=c, high=h, volume=v, rsi=rsi, atr=rolling(tr,14), momentum=mom,
        volume_ratio=v/np.maximum(1,rolling(v,20,prior=True)), liquidity=rolling(v*c,20,prior=True)/1e7,
        dip=(1-c/rolling(h,252,'max',True))*100, relative_strength=mom-bmoment,
        benchmark_close=bc,benchmark_indices=indices,cache={}, dates=[b['end'][:10] for b in bars])


def condition_mask(block, f, patterns=None):
    import numpy as np
    kind,v=block['kind'],block['value'];c=f['close']
    if kind=='breakout':
        key=('high',int(v));a=f['cache'].setdefault(key,rolling(f['high'],int(v),'max',True));return c>a
    if kind=='above_sma':
        key=('sma',int(v));a=f['cache'].setdefault(key,rolling(c,int(v)));return c>a
    if kind=='market_trend':
        key=('market',int(v));a=f['cache'].setdefault(key,rolling(f['benchmark_close'],int(v)))
        return np.array([i>=0 and f['benchmark_close'][i]>a[i] for i in f['benchmark_indices']])
    if kind=='rsi_below':return f['rsi']<v
    if kind=='rsi_above':return f['rsi']>v
    if kind=='dip':return f['dip']>=v
    if kind=='momentum':return f['momentum']>=v
    if kind=='volume':return f['volume_ratio']>=v
    if kind=='liquidity':return f['liquidity']>=v
    if kind=='relative_strength':return f['relative_strength']>=v
    if kind=='volatility':return f['atr']/c*100<v
    if kind=='pattern':return np.array([i in (patterns or {}).get(v,set()) for i in range(len(c))])
    raise ValueError('Unsupported block')


def qualifying(spec,f,patterns=None):
    import numpy as np
    masks=[condition_mask(b,f,patterns) for b in spec['conditions']]
    out=np.logical_and.reduce(masks) if spec['join']=='all' else np.logical_or.reduce(masks)
    return out & np.isfinite(f['atr'])


def review_day(bars,i,schedule):
    if schedule=='daily' or i==0:return True
    previous=date.fromisoformat(bars[i-1]['end'][:10]);current=date.fromisoformat(bars[i]['end'][:10])
    return previous.isocalendar()[:2]!=current.isocalendar()[:2] if schedule=='weekly' else (previous.year,previous.month)!=(current.year,current.month)


def trade_path(spec,bars,f,mask,i,end=None):
    """Next-open entry. Trailing stops use only highs from prior completed bars."""
    entry_i=i+1;last_date=end or spec['end']
    if entry_i>=len(bars) or bars[entry_i]['end'][:10]>last_date:return None
    ex=spec['exit'];slip=spec['slippage_bps']/10000;entry=bars[entry_i]['open']*(1+slip)
    stop=entry-ex['stop']*f['atr'][i] if ex['stop_kind']=='atr' else entry*(1-ex['stop']/100)
    if stop<=0:return None
    target=entry*(1+ex['target']/100);initial_stop=stop;peak=entry;mfe=mae=0.;uncertain=False
    last=min(len(bars)-1,entry_i+ex['hold']-1)
    while last>=entry_i and bars[last]['end'][:10]>last_date:last-=1
    if last<entry_i:return None
    for j in range(entry_i,last+1):
        b=bars[j];o,h,l,c=(b[k] for k in ('open','high','low','close'));reason=None;raw=c;timing='close'
        if j>entry_i and b.get('gap'):reason='data_gap_first_available_open';raw=o;timing='open'
        elif j>entry_i and ex['disqualify'] and review_day(bars,j-1,spec['rebalance']) and not mask[j-1]:reason='no_longer_qualifies';raw=o;timing='open'
        elif o<=stop:reason='stop_gap';raw=o;timing='open'
        elif o>=target:reason='target_gap';raw=o;timing='open'
        elif l<=stop:reason='trailing_stop' if stop>initial_stop else 'stop';raw=stop;timing='intrabar';uncertain=True
        elif h>=target:reason='target';raw=target;timing='intrabar';uncertain=True
        observed=[o,raw] if reason else [h,l,c]
        for p in observed:mfe=max(mfe,(p/entry-1)*100);mae=max(mae,(1-p/entry)*100)
        if not reason and j==last:reason='time_exit' if j==entry_i+ex['hold']-1 else 'study_end'
        if reason:
            fill=raw*(1-slip);gross=(fill/entry-1)*100
            rank=f['momentum'][i] if spec['ranking']=='momentum' else f['liquidity'][i] if spec['ranking']=='liquidity' else 0
            return dict(signal_index=i,entry_index=entry_i,exit_index=j,signal_time=bars[i]['end'],entry_time=bars[entry_i]['time'],
                exit_time=b['time'] if timing=='open' else b['end'],exit_candle_end=b['end'],exit_timing=timing,
                entry=entry,exit=fill,raw_entry=bars[entry_i]['open'],raw_exit=raw,stop=initial_stop,target=target,side='long',product='CNC',
                net_return_pct=gross-spec['fee_bps']/100,gross_return_pct=gross,cost_pct=spec['fee_bps']/100,
                mfe_pct=mfe,mae_pct=mae,excursion_uncertain=uncertain,exit_reason=reason,holding_bars=j-entry_i+1,
                holding_days=(date.fromisoformat(b['end'][:10])-date.fromisoformat(bars[entry_i]['time'][:10])).days,
                overnight=j>entry_i,rule=deepcopy(ex),score=float(rank) if math.isfinite(rank) else -1e9,episode=i,
                features={k:round(float(f[k][i]),4) if math.isfinite(f[k][i]) else None for k in ('rsi','momentum','volume_ratio','relative_strength','liquidity')})
        peak=max(peak,h)
        if ex['trailing']:stop=max(stop,peak*(1-ex['trailing']/100))
    return None


def candidates_for(spec,bars,f,mask,symbol,variant):
    out=[];last_exit=-1
    for i in range(len(bars)-1):
        if i<=last_exit or not mask[i] or not review_day(bars,i,spec['rebalance']) or not spec['start']<=bars[i+1]['time'][:10]<=spec['end']:continue
        t=trade_path(spec,bars,f,mask,i)
        if t:
            horizon=i+spec['exit']['hold']+1
            t.update(symbol=symbol,timeframe='1D',pattern='strategy',variant=variant,id=f'{variant}-{symbol}-{i}',horizon_time=bars[horizon]['end'] if horizon<len(bars) else '9999-12-31')
            out.append(t);last_exit=t['exit_index']-1
    return out


def run_account(trades,histories,spec,start=None,end=None,purge=False):
    settings=dict(spec,start=start or spec['start'],end=end or spec['end'],reinvest=True)
    selected=[deepcopy(t) for t in trades if settings['start']<=t['entry_time'][:10]<=settings['end'] and (not purge or t.get('horizon_time',t['exit_time'])[:10]<=settings['end'])]
    # Admission order is based only on information known at the signal close.
    selected.sort(key=lambda t:(t['entry_time'],-t['score'],t['symbol'],t['id']))
    for i,t in enumerate(selected):t['id']=str(i)
    used={t['symbol'] for t in selected}
    return portfolio(selected,{k:v for k,v in histories.items() if k.split('|')[0] in used},settings)


def daily_curve(curve):
    by={}
    for p in curve:by[p['time'][:10]]=p
    return list(by.values())


def metrics(result,benchmark,start,end):
    curve=daily_curve(result.get('daily_curve',result['curve']));summary=result['summary'];initial=summary['starting_capital'];final=summary['ending_equity']
    years=max(1,(date.fromisoformat(end)-date.fromisoformat(start)).days)/365.25
    cagr=((final/initial)**(1/years)-1)*100 if final>0 else -100
    returns=[b['equity']/a['equity']-1 for a,b in zip(curve,curve[1:]) if a['equity']>0]
    volatility=stats.stdev(returns)*math.sqrt(252)*100 if len(returns)>1 else None
    sharpe=stats.mean(returns)/stats.stdev(returns)*math.sqrt(252) if len(returns)>1 and stats.stdev(returns)>0 else None
    grouped=defaultdict(list)
    for p in curve:grouped[p['time'][:4]].append(p)
    annual=[];prior=initial
    for year,points in sorted(grouped.items()):
        val=points[-1]['equity'];annual.append(dict(year=year,return_pct=(val/prior-1)*100,partial=year in (start[:4],end[:4])));prior=val
    times=[b['end'][:10] for b in benchmark];b0=bisect_right(times,start)-1;b1=bisect_right(times,end)-1
    # Benchmark uses last known close at the period start, never a future observation.
    available=b0>=0 and b1>b0 and (date.fromisoformat(start)-date.fromisoformat(times[b0])).days<=7 and (date.fromisoformat(end)-date.fromisoformat(times[b1])).days<=7
    br=(benchmark[b1]['close']/benchmark[b0]['close']-1)*100 if available else None
    bc=((1+br/100)**(1/years)-1)*100 if br is not None else None
    wins=[t['net_return_pct'] for t in result['trades'] if t['net_pnl']>0];losses=[t['net_return_pct'] for t in result['trades'] if t['net_pnl']<0]
    return dict(**summary,cagr=cagr,volatility_pct=volatility,sharpe=sharpe,benchmark_return_pct=br,benchmark_cagr=bc,
        excess_return_pct=summary['return_pct']-br if br is not None else None,excess_cagr=cagr-bc if bc is not None else None,
        average_win_pct=stats.mean(wins) if wins else None,average_loss_pct=stats.mean(losses) if losses else None,
        annual=annual,best_year=max(annual,key=lambda x:x['return_pct']) if annual else None,worst_year=min(annual,key=lambda x:x['return_pct']) if annual else None)


def benchmark_curve(benchmark,start,end,capital):
    times=[b['end'][:10] for b in benchmark];i=bisect_right(times,start)-1
    if i<0:return []
    base=benchmark[i]['close']
    return [dict(time=start+' 00:00:00',equity=capital)]+[dict(time=b['end'],equity=capital*b['close']/base) for b in benchmark[i+1:] if b['end'][:10]<=end]


def variants(spec):
    """Small declared search, not fabricated thousands of experiments."""
    out=[dict(id='original',name='Your original',spec=deepcopy(spec),change='Original declared rules',added=[])]
    existing={b['kind'] for b in spec['conditions']}
    additions=[('volume',1.2),('above_sma',200),('market_trend',200),('relative_strength',0),('liquidity',1)]
    for kind,value in additions:
        if kind in existing or len(spec['conditions'])>=12:continue
        v=deepcopy(spec);v['conditions'].append(dict(id='add-'+kind,kind=kind,value=value))
        # OR strategies require an explicit nested gate; do not silently change their Boolean meaning.
        if spec['join']=='any':continue
        out.append(dict(id=kind,name=CONDITIONS[kind]['label'],spec=v,change='Add '+CONDITIONS[kind]['label'],added=[kind]))
    for suffix,mult in [('shorter',.75),('longer',1.25)]:
        v=deepcopy(spec);v['exit']['hold']=max(1,min(260,round(spec['exit']['hold']*mult)))
        if v['exit']['hold']!=spec['exit']['hold']:out.append(dict(id=suffix,name=f"Hold up to {v['exit']['hold']} days",spec=v,change=f"Change maximum hold from {spec['exit']['hold']} to {v['exit']['hold']} trading days",added=[]))
    return out


def robustness(m,minimum):
    if m['trades']<minimum:return None
    return m['cagr']-m['max_drawdown_pct']*.5


def regime_at(day,benchmark):
    times=[b['end'][:10] for b in benchmark];i=bisect_right(times,day)-1
    if i<199:return 'Unknown'
    avg=sum(b['close'] for b in benchmark[i-199:i+1])/200;ratio=benchmark[i]['close']/avg
    return 'Bull' if ratio>1.02 else 'Bear' if ratio<.98 else 'Sideways'


def diagnostics(result,benchmark):
    groups=defaultdict(list)
    for t in result['trades']:groups[regime_at(t['signal_time'][:10],benchmark)].append(t)
    regimes=[dict(regime=k,trades=len(ts),win_rate=100*sum(t['net_pnl']>0 for t in ts)/len(ts),net_pnl=sum(t['net_pnl'] for t in ts),average_return_pct=stats.mean(t['net_return_pct'] for t in ts)) for k,ts in groups.items()]
    losses=[t for t in result['trades'] if t['net_pnl']<0];claims=[]
    for label,test in [('volume below its prior 20-day average',lambda t:t['features'].get('volume_ratio') is not None and t['features']['volume_ratio']<1),('negative 63-day relative strength',lambda t:t['features'].get('relative_strength') is not None and t['features']['relative_strength']<0)]:
        if losses:claims.append(f"{sum(test(t) for t in losses)} of {len(losses)} losing trades had {label}. This is an association to test, not a proven cause.")
    return dict(regimes=regimes,observations=claims,definition='Nifty price / trailing 200-day mean: above 1.02 = bull; below 0.98 = bear; otherwise sideways. Regime is measured at signal close.')
