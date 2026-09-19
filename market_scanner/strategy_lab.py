"""Owner-scoped intent, strategy experiments and evidence. Local pilot only."""
from __future__ import annotations
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date,timedelta
from functools import lru_cache
import hashlib
import json
import math
import re
import threading
import time
import uuid

from . import backtest_store as store, catalog, studies
from .data import ROOT,connect_source,load_config
from . import strategy_core as core

HOME=ROOT/'output'/'strategy-lab'
POOL=ThreadPoolExecutor(max_workers=1,thread_name_prefix='strategy-lab')
ACTIVE={}
LOCK=threading.RLock()


def active_run():
    with store.connection() as c:return store.active_run(c)


@lru_cache(maxsize=3)
def benchmark(run):
    file=HOME/'benchmarks'/(studies.identity(run)+'.json.gz')
    if file.exists():return studies.packed_read(file)
    # Freeze the real index once, with the same cutoff as the stored equity run.
    with store.connection() as c:meta=json.loads(c.execute('select payload from runs where id=?',(run,)).fetchone()[0])
    frames=store.load_history(run,'TITAN');cutoff=frames['1D'][0][-1]['end'][:10]
    with connect_source(ROOT/load_config()['database']) as c:
        rows=c.execute("select bar_time,open,high,low,close,volume from ohlc_daily where symbol='NIFTY 50' and bar_time<=? order by bar_time",(cutoff+' 23:59:59',)).fetchall()
    bars=[dict(time=r[0][:10]+' 09:15:00',end=r[0][:10]+' 15:30:00',open=r[1],high=r[2],low=r[3],close=r[4],volume=r[5] or 0,gap=False) for r in rows if all(v is not None and math.isfinite(v) and v>0 for v in r[1:5])]
    if not bars:raise ValueError('Nifty 50 history is unavailable; benchmark claims cannot be tested.')
    studies.packed_write(file,bars);return bars


def capabilities():
    run=active_run();bench=benchmark(run);opts=catalog.options()
    return dict(version=core.VERSION,run=run,first=bench[0]['end'][:10],last=bench[-1]['end'][:10],
        conditions=[dict(kind=k,**v) for k,v in core.CONDITIONS.items()],unavailable=core.UNAVAILABLE,
        universes=opts['universes'],sectors=opts['sectors'],benchmark='Nifty 50 price index (dividends excluded)',
        interpreter='Local rule interpreter: supported market phrases are compiled into visible rules; unsupported requirements need clarification.',
        limits=['Daily CNC long equities; intraday pattern studies remain available under Simulate.',
            'Current supplied membership labels, not historical constituents. Survivorship bias is possible.',
            'Raw source prices. Corporate-action adjustment is not verified; large split-related moves may distort results.',
            'Fundamental, quality, valuation, market-cap and institutional histories are unavailable.',
            'Cold research jobs may exceed a minute. Counts and progress reflect actual work.'])


def default_spec():
    caps=capabilities();end=caps['last'];year=int(end[:4])-10
    return core.validate(dict(name='Momentum with evidence',universe='nifty50',start=f'{year}{end[4:]}',end=end,
        conditions=[dict(id='momentum',kind='momentum',value=10)],capital=100000))


def interpret(text,previous=None):
    if not isinstance(text,str) or not 1<=len(text.strip())<=4000:raise ValueError('Describe your idea in 1–4,000 characters')
    q=text.lower().replace(',','').replace('₹','rs ').replace('–','-');spec=deepcopy(previous) if isinstance(previous,dict) else default_spec()
    blockers=[];notes=[];conditions=[];recognized=False
    if any(x in q for x in ['my portfolio','my holdings','portfolio risk','overexposed','analyze my','analyse my']):
        return dict(action='portfolio',message='Open Portfolio, import your holdings or connect Kite, and I can measure concentration, sector exposure and historical risk.',spec=spec,blockers=[],notes=[],runnable=False)
    if re.search(r'\b(deploy|invest|allocate)\b',q) and any(x in q for x in ['version','strategy','balanced','conservative','aggressive']):
        return dict(action='deploy',message='Choose a completed research candidate, review its evidence and capital, then create a paper portfolio. Live execution remains disabled in this pilot.',spec=spec,blockers=[],notes=[],runnable=False)
    for token,key in [('1000','unsupported'),('500','nifty500'),('200','nifty200'),('100','nifty100'),('50','nifty50')]:
        if re.search(r'nifty\s*'+token+r'\b',q):
            # Nifty 50 in a comparison is a benchmark, not an override of an explicit universe.
            if key=='unsupported':blockers.append('Nifty 1000 membership is not available. Select an available universe in Builder.')
            elif key!='nifty50' or not re.search(r'nifty\s*(100|200|500)\b',q):spec['universe']=key
            recognized=True;break
    symbols=re.search(r'\b(?:stocks?|symbols?)\s*[:=]\s*([A-Za-z0-9&., -]+)',text)
    if symbols:spec['symbols']=[s.strip().upper() for s in symbols.group(1).split(',')];spec['universe']='';recognized=True
    with store.connection() as c:available={r[0] for r in c.execute("select symbol from stocks where run=? and status='complete'",(active_run(),))}
    explicit=[s for s in available if re.search(r'(?<![A-Z0-9])'+re.escape(s)+r'(?![A-Z0-9])',text.upper()) and len(s)>2]
    if explicit and not symbols:spec['symbols']=sorted(explicit);spec['universe']='';recognized=True
    for words,kind in [(['large-cap','large cap','blue chip','market cap','market capitalization'],'market_cap'),(['quality','profitable','valuation','earnings'],'quality'),(['revenue','fundamental','growth stocks'],'revenue_growth'),(['sector strength','sector momentum','outperforming their sector'],'sector_strength'),(['institutional'],'institutional')]:
        if any(w in q for w in words):blockers.append(core.UNAVAILABLE[kind])
    if any(w in q for w in ['short sell','shorting','intraday','1h','4h','weekly candle','1w']):blockers.append('This strategy compiler supports daily long CNC equities. Use Simulate for intraday chart-pattern research.')
    def add(kind,value):
        nonlocal recognized
        conditions.append(dict(id='c'+str(len(conditions)),kind=kind,value=value));recognized=True
    if re.search(r'52[- ]week high|52 week highs',q) and not re.search(r'below|fall|dip|down|pullback',q):add('breakout',252)
    elif re.search(r'6[- ]month high',q):add('breakout',126)
    elif re.search(r'breakout|break out',q):add('breakout',63)
    m=re.search(r'(?:fall|fallen|dip|down|pullback|below)[^.!?]{0,35}?(\d+(?:\.\d+)?)\s*%',q)
    if not m:m=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:below|from (?:their|its|the) (?:recent|52))',q)
    if m:add('dip',float(m[1]))
    m=re.search(r'rsi(?:\s*\(?14\)?)?\s*(below|under|less than|<|above|over|>)\s*(\d+(?:\.\d+)?)',q)
    if m:add('rsi_below' if m[1] in ('below','under','less than','<') else 'rsi_above',float(m[2]))
    elif 'rsi' in q:add('rsi_below',30);notes.append('RSI threshold was not specified; initial hypothesis uses Wilder RSI (14) below 30.')
    if re.search(r'momentum',q) and not conditions:add('momentum',10)
    if re.search(r'increasing volume|high volume|volume confirmation|volume (?:>|above)',q):add('volume',1.2)
    if re.search(r'200[- ]day|200 dma',q):add('market_trend' if 'nifty' in q or 'market' in q else 'above_sma',200)
    if re.search(r'bull market|market regime|market trend',q) and not any(b['kind']=='market_trend' for b in conditions):add('market_trend',200)
    pattern_names={'cup and handle':'cup_handle','cup & handle':'cup_handle','flag and pole':'flag_pole','flag & pole':'flag_pole','symmetrical triangle':'symmetrical_triangle','falling wedge':'falling_wedge','horizontal breakout':'horizontal_breakout'}
    for name,key in pattern_names.items():
        if name in q:add('pattern',key)
    if conditions:spec['conditions']=conditions
    objective=re.search(r'(?:target(?:s|ing)?|annual return|cagr|return of)\s*(\d+(?:\.\d+)?)\s*%',q)
    if not objective:objective=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:annual|cagr)',q)
    if objective:spec['objectives']['cagr']=float(objective[1]);recognized=True
    dd=re.search(r'(?:drawdown(?: of| to)?|maximum drawdown(?: to)?)[^.!?\d]{0,24}(\d+(?:\.\d+)?)\s*%',q)
    if not dd:dd=re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:maximum )?drawdown',q)
    if dd:spec['objectives']['drawdown']=float(dd[1]);recognized=True
    if re.search(r'low[- ]risk|lower volatility|diversif|risk.adjusted|beat(?:en|s)?|strategies|strategy',q):recognized=True
    if not conditions and not previous:notes.append('You described an objective. I propose 63-day momentum ≥10% as the first testable hypothesis; you can edit it before running.')
    amount=re.search(r'(?:rs\.?\s*|capital(?: of)?\s*|i have\s*|invest\s*)(\d+(?:\.\d+)?)\s*(lakh|lac|crore|k|million)?',q)
    if amount:spec['capital']=float(amount[1])*{'lakh':1e5,'lac':1e5,'crore':1e7,'k':1e3,'million':1e6,None:1}[amount[2]];recognized=True
    number=re.search(r'(?:no more than|max(?:imum)?|up to)\s*(\d+)\s*(?:stocks|positions)',q)
    if number:spec['max_positions']=int(number[1]);spec['allocation_pct']=100/int(number[1]);recognized=True
    years=re.search(r'(?:last|past|over)\s*(\d+)\s*[- ]?years?',q)
    if years:
        end=spec['end'];spec['start']=f'{int(end[:4])-int(years[1]):04d}{end[4:]}';recognized=True
    dates=re.findall(r'\b\d{4}-\d{2}-\d{2}\b',q)
    if len(dates)>=2:spec['start'],spec['end']=dates[:2];recognized=True
    for field,pat in [('hold',r'(?:hold(?:ing)?(?: for| period)?|exit after)\s*(\d+)\s*(?:days|sessions)'),('stop',r'stop(?:[- ]loss)?\s*(?:of|at)?\s*-?(\d+(?:\.\d+)?)\s*%'),('target',r'(?:profit target|take profit)\s*(?:of|at)?\s*\+?(\d+(?:\.\d+)?)\s*%'),('trailing',r'trailing (?:stop|exit)\s*(\d+(?:\.\d+)?)\s*%')]:
        m=re.search(pat,q)
        if m:spec['exit'][field]=float(m[1]);recognized=True
    if 'weekly rebalance' in q:spec['rebalance']='weekly'
    if 'monthly rebalance' in q:spec['rebalance']='monthly'
    if not recognized:blockers.append('I could not map this request to a supported rule. Try “RSI below 30”, “52-week high with increasing volume”, or edit the Builder.')
    try:spec=core.validate(spec)
    except ValueError as e:blockers.append(str(e))
    first=capabilities()['first']
    if spec['start']<first:blockers.append(f"The request starts {spec['start']}; available Nifty history starts {first}. Change the test dates explicitly.")
    spec['name']=text.strip()[:90]
    return dict(action='research',spec=spec,blockers=list(dict.fromkeys(blockers)),notes=notes,runnable=not blockers,
        message='I translated your objective into the flow below. I will test the original, diagnose losses, and compare a bounded family of improvements on later data.' if not blockers else 'Parts of your request cannot be tested faithfully with the supplied data. Resolve the items below or edit the strategy explicitly.',
        interpretation='local_rules',prompt=text)


def path_for(kind,key):return HOME/kind/(studies.identity(key)+'.json')


def get(owner,key,result=False):
    f=path_for('jobs',key)
    if not f.exists():raise ValueError('Research not found')
    v=studies.packed_read(f)
    if v['owner']!=owner:raise ValueError('Research not found')
    if v['status'] in ('running','queued') and key not in ACTIVE:v.update(status='interrupted',message='Research worker restarted; run this saved idea again.')
    v.pop('owner',None)
    if result and v['status']=='complete':v['result']=studies.packed_read(HOME/'results'/(key+'.json.gz'))
    return v


def jobs(owner):return sorted([get(owner,p.stem) for p in (HOME/'jobs').glob('*.json') if studies.packed_read(p).get('owner')==owner],key=lambda x:x['created'],reverse=True)[:40]


def save_strategy(owner,data):
    owner=studies.identity(owner);spec=core.validate(data['spec']);key=uuid.uuid4().hex
    item=dict(id=key,owner=owner,spec=spec,created=time.time(),version=core.VERSION)
    studies.packed_write(path_for('strategies',key),item);return {k:v for k,v in item.items() if k!='owner'}


def saved(owner):return [{k:v for k,v in studies.packed_read(p).items() if k!='owner'} for p in (HOME/'strategies').glob('*.json') if studies.packed_read(p).get('owner')==owner]


def selected_symbols(spec):
    with store.connection() as c:available={r[0] for r in c.execute("select symbol from stocks where run=? and status='complete'",(active_run(),))}
    selected=set(spec['symbols']) if spec['symbols'] else available
    missing=selected-available
    if missing:raise ValueError('No frozen history for '+', '.join(sorted(missing)[:8]))
    allowed=catalog.selected_symbols(spec)
    if allowed is not None:selected&=set(allowed)
    if not selected:raise ValueError('No stocks match the selected universe and sector')
    return sorted(selected)


def start(owner,data):
    owner=studies.identity(owner);spec=core.validate(data['spec']);symbols=selected_symbols(spec);bench=benchmark(active_run())
    if spec['end']>bench[-1]['end'][:10] or spec['start']<bench[0]['end'][:10]:raise ValueError('Dates exceed the available Nifty 50 history. Review the explicit date range.')
    if (date.fromisoformat(spec['end'])-date.fromisoformat(spec['start'])).days<365*2:raise ValueError('Use at least two years for discovery and a separate final test.')
    with LOCK:
        if any(j['status'] in ('running','queued') for j in jobs(owner)):raise ValueError('One research experiment is already running. Finish or cancel it first.')
        key=uuid.uuid4().hex;event=threading.Event();ACTIVE[key]=event
        v=dict(id=key,owner=owner,created=time.time(),status='queued',message='Preparing the declared experiments',done=0,total=len(symbols),
            run=active_run(),version=core.VERSION,spec=spec,symbols=symbols,prompt=str(data.get('prompt',''))[:4000],variants=core.variants(spec))
        studies.packed_write(path_for('jobs',key),v);POOL.submit(run_job,v,event)
    return get(owner,key)


def cancel(owner,key):
    v=get(owner,key)
    if key in ACTIVE:ACTIVE[key].set()
    return v


def run_job(item,event):
    def update(**kw):item.update(**kw);studies.packed_write(path_for('jobs',item['id']),item)
    def check():
        if event.is_set():raise InterruptedError('Research cancelled')
    try:
        update(status='running',message='Reading frozen daily candles and calculating causal indicators')
        spec=item['spec'];variants=item['variants'];bench=benchmark(item['run']);histories={};trades={v['id']:[] for v in variants};coverage=[];latest=[]
        pattern_ids=list({b['value'] for v in variants for b in v['spec']['conditions'] if b['kind']=='pattern'})
        for n,symbol in enumerate(item['symbols']):
            check();frames=store.load_history(item['run'],symbol)
            if not frames or not frames.get('1D'):continue
            bars=frames['1D'][0];histories[symbol+'|1D']=studies.MarkHistory(item['run'],symbol,'1D',bars)
            features=core.features(bars,bench);patterns={}
            if pattern_ids:
                _,groups,_=studies.event_history(item['run'],symbol,'1D',pattern_ids,event.is_set)
                patterns={p:{x['signal_index'] for side in ('long',) for x in groups.get(p+'|'+side,[])} for p in pattern_ids}
            for v in variants:
                mask=core.qualifying(v['spec'],features,patterns)
                trades[v['id']].extend(core.candidates_for(v['spec'],bars,features,mask,symbol,v['id']))
                index=next((i for i in range(len(bars)-1,-1,-1) if bars[i]['end'][:10]<=spec['end']),-1)
                if index>=0 and mask[index]:latest.append(dict(variant=v['id'],symbol=symbol,price=bars[index]['close'],date=bars[index]['end'][:10],
                    score=float(features['momentum'][index]) if math.isfinite(features['momentum'][index]) else 0))
            coverage.append(dict(symbol=symbol,first=bars[0]['end'][:10],last=bars[-1]['end'][:10],bars=len(bars),
                full_requested_period=bars[0]['end'][:10]<=spec['start'] and bars[-1]['end'][:10]>=spec['end'],
                large_moves=sum(abs(bars[i]['close']/bars[i-1]['close']-1)>.35 for i in range(1,len(bars)))))
            update(done=n+1,message=f"Prepared {symbol} · {n+1}/{len(item['symbols'])} stocks × {len(variants)} declared variations")
        check();days=(date.fromisoformat(spec['end'])-date.fromisoformat(spec['start'])).days
        holdout=(date.fromisoformat(spec['start'])+timedelta(days=round(days*.8))).isoformat();discovery_end=(date.fromisoformat(holdout)-timedelta(days=1)).isoformat()
        results=[];full_by={}
        for v in variants:
            check();update(message='Testing '+v['name']+' against Nifty 50')
            full=core.run_account(trades[v['id']],histories,v['spec']);discovery=core.run_account(trades[v['id']],histories,v['spec'],end=discovery_end,purge=True)
            # No final-test result participates in candidate selection.
            dm=core.metrics(discovery,bench,spec['start'],discovery_end)
            results.append(dict(id=v['id'],name=v['name'],spec=v['spec'],change=v['change'],added=v['added'],
                full=core.metrics(full,bench,spec['start'],spec['end']),discovery=dm,selection_score=core.robustness(dm,spec['min_trades'])))
            full_by[v['id']]=full
        eligible=[r for r in results if r['selection_score'] is not None]
        ranked=sorted(eligible,key=lambda r:(-r['selection_score'],r['id']))
        choices=dict(balanced=ranked[0]['id'] if ranked else None,
            conservative=min(eligible,key=lambda r:(r['discovery']['max_drawdown_pct'],-r['discovery']['cagr']))['id'] if eligible else None,
            aggressive=max(eligible,key=lambda r:r['discovery']['cagr'])['id'] if eligible else None)
        for r in results:
            check();final=core.run_account(trades[r['id']],histories,r['spec'],start=holdout)
            r['holdout']=core.metrics(final,bench,holdout,spec['end'])
            r['meets_requested_objective']=r['holdout']['trades']>=spec['min_trades'] and r['holdout']['cagr']>=spec['objectives']['cagr'] and r['holdout']['max_drawdown_pct']<=spec['objectives']['drawdown'] and (not spec['objectives']['beat_nifty'] or (r['holdout']['excess_cagr'] or 0)>0)
        original=results[0];chosen=next((r for r in results if r['id']==choices['balanced']),None)
        improved=bool(chosen and chosen['id']!='original' and chosen['holdout']['trades']>=spec['min_trades'] and original['holdout']['trades']>=spec['min_trades'] and
            chosen['holdout']['cagr']>original['holdout']['cagr'] and chosen['holdout']['max_drawdown_pct']<=original['holdout']['max_drawdown_pct'])
        verdict=('The discovery-selected change also improved return without increasing drawdown in the final test. Treat it as a candidate for further paper observation.' if improved else
            'No improvement is established on the final test. Keep the original hypothesis or gather more evidence; do not select a new winner using this same final period.')
        # Rolling selection only on earlier fully completed potential holding horizons.
        update(message='Freezing rules in rolling learning windows and testing the following periods')
        folds=[];wf_trades=[];boundary=core.add_months(spec['start'],spec['train_months']);wf_start=boundary
        while boundary<=spec['end']:
            check();next_boundary=core.add_months(boundary,spec['test_months']);end=min(spec['end'],(date.fromisoformat(next_boundary)-timedelta(days=1)).isoformat());train_start=max(spec['start'],core.add_months(boundary,-spec['train_months']));train_end=(date.fromisoformat(boundary)-timedelta(days=1)).isoformat()
            best=None
            for v in variants:
                check();a=core.run_account(trades[v['id']],histories,v['spec'],train_start,train_end,purge=True);m=core.metrics(a,bench,train_start,train_end);score=core.robustness(m,spec['min_trades'])
                if score is not None and m['cagr']>0 and (best is None or score>best['score']):best=dict(id=v['id'],score=score,samples=m['trades'])
            if best:wf_trades.extend(dict(t,fold=len(folds)) for t in trades[best['id']] if boundary<=t['entry_time'][:10]<=end)
            folds.append(dict(train_start=train_start,train_end=train_end,start=boundary,end=end,selected=best))
            boundary=next_boundary
        wf=core.run_account(wf_trades,histories,spec,start=wf_start) if folds else None
        wf_result=dict(metrics=core.metrics(wf,bench,wf_start,spec['end']),curve=wf['curve'],folds=folds,trades=wf['trades']) if wf else dict(metrics=None,curve=[],folds=[],trades=[],note='The requested period is shorter than the initial learning window. Reduce the learning months or extend the study.')
        accounts={k:dict(curve=a['curve'],trades=a['trades'],skipped=a['skipped'],diagnostics=core.diagnostics(a,bench)) for k,a in full_by.items()}
        benchmark_hash=hashlib.sha256(json.dumps(bench,separators=(',',':')).encode()).hexdigest()
        result=dict(version=core.VERSION,run=item['run'],spec=spec,variation_count=len(variants),rows=results,choices=choices,verdict=verdict,improvement_supported=improved,
            split=dict(discovery_start=spec['start'],discovery_end=discovery_end,holdout_start=holdout,holdout_end=spec['end']),walkforward=wf_result,
            accounts=accounts,benchmark=core.benchmark_curve(bench,spec['start'],spec['end'],spec['capital']),coverage=coverage,latest=latest,
            benchmark_source=dict(name='Nifty 50 price index',first=bench[0]['end'],last=bench[-1]['end'],sha256=benchmark_hash),
            assumptions=capabilities()['limits']+['All fills are next-open, whole shares, no leverage. Stop wins an ambiguous stop/target candle. Trailing stops update after close.',
                'Fees are declared round-trip basis points on entry notional; slippage applies at both fills. These are assumptions, not broker-specific charges.',
                'Sharpe uses daily marked equity returns, 252 sessions/year and zero risk-free rate. Partial first/last years are labelled.',
                'Weekly/monthly review runs on the first available session of the new week/month and fills next open; existing holdings obey their frozen exits.',
                'Final 20% is excluded from selection. It is no longer an untouched test after you inspect it or repeatedly revise the strategy.',
                'The bounded search is not exhaustive. Three profile labels may point to the same candidate; there may be no supported improvement.'])
        check();studies.packed_write(HOME/'candidates'/(item['id']+'.json.gz'),trades)
        studies.packed_write(HOME/'results'/(item['id']+'.json.gz'),result);update(status='complete',message='Evidence ready',finished=time.time())
    except InterruptedError:update(status='cancelled',message='Research cancelled; no portfolio was deployed')
    except Exception as e:update(status='error',message=str(e)[:300])
    finally:ACTIVE.pop(item['id'],None)


def chart(owner,key,variant,trade_id):
    job=get(owner,key,True);result=job.get('result')
    if not result or variant not in result['accounts']:raise ValueError('Strategy evidence not found')
    trade=next((t for t in result['accounts'][variant]['trades'] if t['id']==trade_id),None)
    if not trade:raise ValueError('Trade not found')
    bars=store.load_history(job['run'],trade['symbol'])['1D'][0];f=core.features(bars,benchmark(job['run']));begin=max(0,trade['signal_index']-70);end=trade['exit_index']+1
    indicators={}
    for b in next(r for r in result['rows'] if r['id']==variant)['spec']['conditions']:
        if b['kind']=='above_sma':indicators['SMA '+str(b['value'])]=core.rolling(f['close'],int(b['value']))
        if b['kind']=='breakout':indicators['Prior '+str(b['value'])+'-day high']=core.rolling(f['high'],int(b['value']),'max',True)
    shape=None
    pattern=next((b['value'] for b in next(r for r in result['rows'] if r['id']==variant)['spec']['conditions'] if b['kind']=='pattern'),None)
    if pattern:
        try:return dict(studies.chart_bundle(job['run'],trade['symbol'],'1D',trade['signal_index'],pattern,trade),strategy=True,features=trade['features'])
        except ValueError:pass
    return dict(symbol=trade['symbol'],timeframe='1D',bars=bars[begin:end],signal_index=trade['signal_index']-begin,shape=shape,
        trade=dict(trade,entry_chart_index=trade['entry_index']-begin,exit_chart_index=trade['exit_index']-begin,signal_chart_index=trade['signal_index']-begin),
        overlays=[dict(name=k,values=[float(v) if math.isfinite(v) else None for v in a[begin:end]]) for k,a in indicators.items()],features=trade['features'])
