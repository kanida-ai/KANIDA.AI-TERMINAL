"""Evidence-linked exits; frozen selected rules are never re-selected on test data.

The fallback is an untested structural 1:2 paper benchmark. It is not assigned
the fixed-hold baseline's win rate. Nothing here submits or authorizes orders.
"""
import hashlib
import json
import math
from statistics import mean, stdev, NormalDist
from .performance import holding_details

VERSION = 'exit-policy-1'


def current_atr(bars):
    # Same lagged 20-bar true range used by the frozen research at signal time.
    end = len(bars)-1
    values = []
    for i in range(max(1,end-20),end):
        b, previous = bars[i], bars[i-1]['close']
        values.append(max(b['high']-b['low'],abs(b['high']-previous),abs(b['low']-previous)))
    return max(mean(values) if values else 0, bars[-1]['close']*.001)


def structural_stop(bars, found, side):
    price=bars[-1]['close']; atr=current_atr(bars); long=side=='long'
    lines=found.get('lines',[]); boundaries=[l for l in lines if l['role']=='boundary']
    opposite=[l for l in boundaries if ('support' if long else 'resistance') in l['label'].lower()]
    level=None; basis='Recent swing with a volatility buffer'
    if found['pattern']=='cup_handle':
        handle=next((l for l in lines if l['label']=='Handle'),None)
        if handle: level=min(p['value'] for p in handle['points']);basis='Below the handle low, with a volatility buffer'
    elif found['pattern'] in ('head_shoulders','inverse_head_shoulders'):
        shoulder=next((l for l in lines if l['label']=='Right shoulder'),None)
        if shoulder:level=shoulder['points'][0]['value'];basis='Beyond the right shoulder, with a volatility buffer'
    elif opposite:
        line=opposite[0]; first,last=line['points'][0],line['points'][-1]
        slope=(last['value']-first['value'])/max(1,last['index']-first['index'])
        level=last['value']+slope*(len(bars)-1-last['index'])
        basis='Beyond pattern '+('support' if long else 'resistance')+', with a volatility buffer'
    if level is None:
        start=max(found.get('start_index',0),len(bars)-24)
        candidates=[]
        for i in range(max(3,start),len(bars)-3):
            value=bars[i]['low' if long else 'high']
            window=[b['low' if long else 'high'] for b in bars[i-3:i+4]]
            if value==(min(window) if long else max(window)) and window.count(value)==1:candidates.append(value)
        valid=[v for v in candidates if v<price] if long else [v for v in candidates if v>price]
        level=valid[-1] if valid else (min(b['low'] for b in bars[start:]) if long else max(b['high'] for b in bars[start:]))
    distance=max((price-level if long else level-price)+atr*.25,atr)
    return distance,basis,atr


def assess(study):
    """Assess the ONE previously selected rule. Never fall back to another test winner."""
    reference=study.get('reference',{}); rule=study.get('rule'); splits=study.get('splits',{})
    status='limited' if reference.get('n',0)>0 else 'default'
    why='No exit rule has enough independent support. The 1:2 plan remains a paper benchmark.'
    test=splits.get('test',{}); trades=sorted([t for t in study.get('trades',[]) if t.get('split')=='test'],key=lambda t:t['entry_index'])
    values=[t['net_return_pct'] for t in trades]; n=len(values)
    periods=[values[:n//2],values[n//2:]]
    period_means=[mean(p) if p else None for p in periods]
    # A conservative search-adjusted lower bound supplements the frozen split.
    trials=max(1,study.get('candidates_tested',1))
    bound=mean(values)-NormalDist().inv_cdf(1-.05/trials)*stdev(values)/math.sqrt(n) if n>1 else None
    minimum=study.get('assumptions',{}).get('minimum_test_trades_for_estimate',20)
    supported=bool(rule and rule.get('stop_atr',0)>0 and rule.get('target_r',0)>0 and n==test.get('n') and n>=minimum
                   and bound is not None and bound>0 and all(p is not None and p>0 for p in period_means)
                   and all((splits.get(k,{}).get('selection_score') or 0)>0 for k in ('train','validation')))
    ci=test.get('expectancy_ci95')
    if rule and test.get('n',0)>=5 and ci and ci[1]<0:
        status='failed';why='The selected exit rule lost money on later data after costs. Keep this setup out of automatic execution.'
    elif supported:
        status='supported';why='The frozen exit rule stayed positive on later data, after costs and uncertainty checks.'
    elif rule and not rule.get('stop_atr'):
        why='The selected study uses a timed exit, without a stop/target ratio. Adding either creates a new, untested rule.'
    elif rule:
        why='A candidate passed the earlier selection, but its later evidence is too weak or inconsistent to support a customised exit.'
    candidates=sorted([r for r in study.get('training_candidates',[]) if r.get('train',{}).get('n',0)>0],
                      key=lambda r:-(r['train'].get('selection_score') if r['train'].get('selection_score') is not None else -1e20))[:3]
    return dict(status=status,why=why,selected_rule=rule,selected_description=study.get('rule_description'),
                test=test,period_means=period_means,search_adjusted_lower_pct=bound,minimum_later_trades=minimum,
                candidates_tested=trials,candidates=candidates,baseline_n=reference.get('n',0),
                test_period=study.get('date_spans',{}).get('test'),splits={k:splits.get(k,{'n':0}) for k in ('train','validation','test')})


def describe(match,side,chart,study):
    if study and any(study.get(k)!=v for k,v in [('symbol',match['symbol']),('pattern',match['pattern']),('timeframe',match['timeframe']),('side',side)]):
        raise ValueError('Exit evidence belongs to a different stock, pattern, timeframe or direction')
    bars=chart.get('bars',[]); found=next((m for m in chart.get('matches',[]) if m['pattern']==match['pattern']),None)
    if not bars or not found or bars[-1]['end']!=match['candle_end']:raise ValueError('Chart snapshot changed. Reopen this setup.')
    evidence=assess(study or {}); status=evidence['status']; price=bars[-1]['close']
    distance,basis,atr=structural_stop(bars,found,side)
    rule=dict(kind='structure',trigger='confirmed',hold={'1H':6,'4H':6,'1D':10,'1W':4}[match['timeframe']],target_r=2,stop_atr=None)
    if status=='supported':
        selected=evidence['selected_rule']; selected_distance=selected['stop_atr']*atr
        if selected_distance+1e-8<distance:
            status='limited';evidence['why']='The tested stop sits inside this setup’s structural risk. The wider structural benchmark is a different rule and needs testing.'
        else:
            rule={**selected,'kind':'atr'};distance=selected_distance;basis=f"Exact tested rule: {selected['stop_atr']:g} × recent average candle range"
    sign=1 if side=='long' else -1; stop_pct=distance/price*100; target_pct=stop_pct*rule['target_r']
    stop=price-sign*distance;target=price+sign*distance*rule['target_r']
    usable=0<stop_pct<=20 and min(stop,target)>0
    labels={'default':'Default benchmark','limited':'Limited evidence','supported':'History supported','failed':'Exit test failed'}
    evidence['status']=status
    cost=(study or {}).get('assumptions',{});cost_pct=(cost.get('round_trip_fee_bps',30)+cost.get('round_trip_slippage_bps',10))/100
    selected_matches=status=='supported'
    result=dict(version=VERSION,match_id=match['id'],side=side,run=(study or {}).get('run_id'),snapshot=match['candle_end'],
                status=status,label=labels[status],why=evidence['why'],evidence=evidence,rule=rule,stop_basis=basis,
                price=price,stop=stop,target=target,stop_pct=stop_pct,target_pct=target_pct,reward=rule['target_r'],atr=atr,
                holding=holding_details(match['timeframe'],rule['hold'],True),cost_pct=cost_pct,usable=usable,
                evidence_applies=selected_matches,rule_metrics=evidence['test'] if selected_matches else None,
                entry=f"After a new {'confirmed breakout' if rule['trigger']=='confirmed' else 'qualifying setup'} candle, enter at the next candle open",
                execution_ready=False,screen='pass' if status=='failed' else 'review' if selected_matches and match['direction']!='neutral' else 'watch',
                note='Levels illustrate the rule from the stored close. Recheck the signal and size at the actual next-open entry. Paths are not forecasts.')
    result['id']=hashlib.sha256(json.dumps(result,sort_keys=True,allow_nan=False).encode()).hexdigest()[:24]
    return result
