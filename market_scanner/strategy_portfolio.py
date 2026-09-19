"""Paper rehearsal and read-only holdings analytics; never submits broker orders."""
from __future__ import annotations
from collections import defaultdict
from copy import deepcopy
from datetime import date
import math
import statistics as stats
import time
import uuid

from . import strategy_lab as lab, strategy_core as core, studies, catalog, backtest_store as store


def analyse(data):
    holdings=data.get('holdings')
    if not isinstance(holdings,list) or not 1<=len(holdings)<=200:raise ValueError('Supply 1–200 holdings with symbol, quantity and average price')
    run=lab.active_run();bench=lab.benchmark(run);labels=catalog.labels();rows=[];histories={};seen=set();source=data.get('source','manual')
    for h in holdings:
        symbol=str(h.get('symbol','')).strip().upper()
        if not symbol or len(symbol)>45 or symbol in seen:raise ValueError('Use one combined row per stock symbol')
        seen.add(symbol);quantity=core.numeric(h.get('quantity'),0,1,1e9,True);avg=core.numeric(h.get('average_price'),0,.0001,1e8)
        frames=store.load_history(run,symbol)
        if not frames or '1D' not in frames:rows.append(dict(symbol=symbol,quantity=quantity,average_price=avg,available=False,reason='No frozen daily history'));continue
        bars=frames['1D'][0];histories[symbol]=bars;price=bars[-1]['close'];f=core.features(bars,bench);sma=core.rolling(f['close'],200)[-1]
        rows.append(dict(symbol=symbol,quantity=quantity,average_price=avg,available=True,price=price,price_date=bars[-1]['end'][:10],value=quantity*price,cost=quantity*avg,
            pnl=quantity*(price-avg),pnl_pct=(price/avg-1)*100,sector=labels.get(symbol,{}).get('sector','Unclassified'),
            momentum_63d=float(f['momentum'][-1]) if math.isfinite(f['momentum'][-1]) else None,
            above_200d=bool(price>sma) if math.isfinite(sma) else None))
    priced=[r for r in rows if r['available']];value=sum(r['value'] for r in priced);cost=sum(r['cost'] for r in priced)
    if value<=0:raise ValueError('None of these holdings has a usable stored price')
    sectors=defaultdict(float)
    for r in priced:r['weight']=r['value']/value*100;sectors[r['sector']]+=r['value']
    # Constant-current-share historical path, not a reconstruction of actual purchases.
    common=None;maps={}
    for r in priced:
        bars=histories[r['symbol']][-505:];maps[r['symbol']]={b['end'][:10]:b['close'] for b in bars};dates=set(maps[r['symbol']]);common=dates if common is None else common&dates
    dates=sorted(common or []);curve=[];peak=0.;maxdd=0
    daily={r['symbol']:[] for r in priced}
    for day in dates:
        total=sum(maps[r['symbol']][day]*r['quantity'] for r in priced);peak=max(peak,total);dd=100*(1-total/peak);maxdd=max(maxdd,dd)
        curve.append(dict(time=day+' 15:30:00',equity=total,drawdown_pct=dd,cash=0,unrealized=total-cost))
    for r in priced:daily[r['symbol']]=[maps[r['symbol']][b]/maps[r['symbol']][a]-1 for a,b in zip(dates,dates[1:])]
    covariance={};correlations=[];risk=[]
    if len(dates)>20:
        import numpy as np
        mat=np.array([daily[r['symbol']] for r in priced]);cov=np.atleast_2d(np.cov(mat,ddof=1));weights=np.array([r['weight']/100 for r in priced]);variance=float(weights@cov@weights)
        contributions=weights*(cov@weights)
        for i,r in enumerate(priced):risk.append(dict(symbol=r['symbol'],contribution_pct=float(contributions[i]/variance*100) if variance>0 else 0))
        for i in range(len(priced)):
            for j in range(i+1,len(priced)):
                den=math.sqrt(cov[i,i]*cov[j,j]);corr=cov[i,j]/den if den>0 else None
                if corr is not None:correlations.append(dict(a=priced[i]['symbol'],b=priced[j]['symbol'],correlation=float(corr)))
        volatility=math.sqrt(max(0,variance)*252)*100
    else:volatility=None
    top=max(priced,key=lambda r:r['weight']);sector_rows=sorted([dict(sector=k,value=v,weight=100*v/value) for k,v in sectors.items()],key=lambda r:-r['weight'])
    observations=[f"{top['symbol']} is {top['weight']:.1f}% of priced holdings.",f"{sector_rows[0]['sector']} accounts for {sector_rows[0]['weight']:.1f}% of priced holdings."]
    if top['weight']>25:observations.append('Single-stock concentration is above the 25% review threshold. Test smaller position limits before changing actual holdings.')
    below=[r['symbol'] for r in priced if r['above_200d'] is False]
    if below:observations.append(', '.join(below[:8])+' are below their 200-day mean at the stored price date. Review whether they still fit your objective; this alone is not a sell decision.')
    return dict(source=source,run=run,holdings=rows,value=value,cost=cost,pnl=value-cost,pnl_pct=(value/cost-1)*100,sectors=sector_rows,risk_contributions=sorted(risk,key=lambda r:-r['contribution_pct']),
        correlations=sorted(correlations,key=lambda r:-r['correlation'])[:12],volatility_pct=volatility,historical_drawdown_pct=maxdd if len(curve)>1 else None,
        curve=curve,observations=observations,stress=[dict(market_move_pct=x,portfolio_change=-value*x/100,remaining=value*(1-x/100)) for x in (10,20,30)],
        scenarios_note='Parallel price-shock scenario: every priced holding falls by the chosen percentage. This is not a forecast or a beta model.',
        upside=None,upside_note='A maximum potential upside cannot be inferred reliably from OHLC alone. No upside guarantee or return forecast is assigned.',
        methodology='P&L values your supplied quantities and average costs using stored closes. Risk uses the most recent common daily observations (up to 2 years) at current weights. The curve holds today’s shares constant; it is not your actual account history.',
        coverage=dict(priced=len(priced),total=len(rows),common_sessions=len(dates),first=dates[0] if dates else None,last=dates[-1] if dates else None,as_of=sorted({r['price_date'] for r in priced})),
        limitations=['Missing holdings are excluded from totals and exposures; totals are partial when coverage is incomplete.','Prices are historical, not broker live marks. Corporate-action adjustment is not verified.','No buy/sell or rebalance instruction is sent to a broker.'])


def paper_path(key):return lab.path_for('portfolios',key)


def get_paper(owner,key):
    f=paper_path(key)
    if not f.exists():raise ValueError('Paper portfolio not found')
    p=studies.packed_read(f)
    if p['owner']!=owner:raise ValueError('Paper portfolio not found')
    return p


def public(p):return {k:v for k,v in p.items() if k not in ('owner','replay')}


def papers(owner):return [public(studies.packed_read(p)) for p in (lab.HOME/'portfolios').glob('*.json') if studies.packed_read(p).get('owner')==owner]


def prepare(owner,data):
    job=lab.get(owner,data['research_id'],True);result=job.get('result');variant=data.get('variant')
    if not result:raise ValueError('Complete the research before preparing a portfolio')
    row=next((r for r in result['rows'] if r['id']==variant),None)
    if not row:raise ValueError('Choose a tested candidate')
    capital=core.numeric(data.get('capital'),row['spec']['capital'],100,1e9)
    # Re-price quantity and allocation at the chosen capital; never scale old fills blindly.
    spec=dict(row['spec'],capital=capital);asof=spec['end'];histories={}
    trades=studies.packed_read(lab.HOME/'candidates'/(job['id']+'.json.gz'))[variant]
    for symbol in {t['symbol'] for t in trades if t['entry_time'][:10]>=result['split']['holdout_start']}:
        bars=store.load_history(job['run'],symbol)['1D'][0];histories[symbol+'|1D']=studies.MarkHistory(job['run'],symbol,'1D',bars)
    replay=core.run_account(trades,histories,spec,start=result['split']['holdout_start'])
    current=[r for r in result['latest'] if r['variant']==variant and r['date']==asof];current.sort(key=lambda r:(-r['score'],r['symbol']))
    basket=[];cash=capital;rate=spec['fee_bps']/10000
    for r in current[:spec['max_positions']]:
        quantity=int(min(cash,capital*spec['allocation_pct']/100)/(r['price']*(1+rate)))
        if quantity<=0:continue
        value=quantity*r['price'];cash-=value*(1+rate);basket.append(dict(r,quantity=quantity,notional=value,side='BUY',product='CNC'))
    key=uuid.uuid4().hex;p=dict(id=key,owner=owner,research_id=job['id'],variant=variant,name=row['name'],spec=spec,capital=capital,created=time.time(),status='draft',
        mode='historical_paper_rehearsal',basket=basket,as_of=asof,remaining_cash=cash,step=0,replay=replay,observations=[],
        note='Draft basket uses stored closing prices, not executable live quotes. Rehearsal replays the final historical period using this candidate, whole shares and the chosen capital. It does not submit orders.',
        live_enabled=False)
    p=marked(p);studies.packed_write(paper_path(key),p);return public(p)


def marked(p):
    replay=p['replay'];curve=replay.get('daily_curve',replay['curve']);step=min(p['step'],len(curve)-1);point=curve[step];clock=point['time']
    trades=replay['trades'];positions=[t for t in trades if t['entry_time']<=clock<t['exit_time']];closed=[t for t in trades if t['exit_time']<=clock]
    # No future exit/P&L fields are exposed for a still-open rehearsal position.
    p['account']=dict(point,positions=[{k:t[k] for k in ('symbol','quantity','entry','entry_time','stop','target','notional')} for t in positions],closed=closed,
        total_steps=len(curve),curve=curve[:step+1],current_step=step,completed=step==len(curve)-1)
    p['observations']=[f"{len(positions)} positions open; {len(closed)} historical exits completed.",f"Marked account decline: {point['drawdown_pct']:.2f}%. Risk review threshold: {p['spec']['objectives']['drawdown']}%."]
    return p


def action(owner,key,data):
    with lab.LOCK:
        p=get_paper(owner,key);cmd=data.get('action')
        if cmd=='start':
            if p['status'] not in ('draft','paused'):raise ValueError('Only a draft or paused rehearsal can start')
            p['status']='active'
        elif cmd=='pause':p['status']='paused'
        elif cmd=='stop':p['status']='stopped'
        elif cmd=='advance':
            if p['status']!='active':raise ValueError('Start the paper rehearsal before advancing')
            p['step']=min(p['step']+int(core.numeric(data.get('sessions'),1,1,21,True)),len(p['replay'].get('daily_curve',p['replay']['curve']))-1);p=marked(p)
            if p['account']['drawdown_pct']>p['spec']['objectives']['drawdown']:p['status']='risk_paused';p['observations'].append('Risk threshold crossed. Rehearsal paused for review; no silent override.')
            elif p['account']['completed']:p['status']='complete'
        else:raise ValueError('Choose start, pause, stop or advance')
        p=marked(p);studies.packed_write(paper_path(key),p);return public(p)
