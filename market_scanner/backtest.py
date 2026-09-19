"""Per-stock rule mining with purged chronological validation and untouched test.

No signal, trade, selection statistic or probability is pooled across stocks.
All returns are percent of entry notional, unlevered, after assumed costs.
"""
from __future__ import annotations
import json
import math
from dataclasses import dataclass, asdict
import numpy as np
from .data import ROOT


def rules_config():
    return json.loads((ROOT/'backtest_rules.json').read_text(encoding='utf-8'))


@dataclass(frozen=True)
class Rule:
    trigger: str
    hold: int
    stop_atr: float=0
    target_r: float=0

    @property
    def id(self):
        return f'{self.trigger}:{self.hold}:{self.stop_atr:g}:{self.target_r:g}'

    def describe(self):
        return f'{self.trigger}; next open; '+(f'{self.stop_atr:g} ATR stop / {self.target_r:g}R target; ' if self.stop_atr else 'time exit; ')+f'max {self.hold} candles'


def candidates(timeframe,config):
    return [Rule(trigger,hold,stop,r) for trigger in ('setup','confirmed')
            for hold in config['holding_bars'][timeframe]
            for stop,r in [(0,0)]+[(s,r) for s in config['stop_atr'] for r in config['target_r']]]


def episodes(replay,masks=None,rearm=3):
    """One setup and one confirmation per episode per direction study.

    Three consecutive absent closes are necessary to rearm. A neutral setup is
    tested in separate long and hypothetical short studies, never silently
    assigned a favorable future direction.
    """
    groups={}; active={}; observations=0
    for index in range(39,len(replay.bars)):
        matches=replay.at(index,63 if masks is None else int(masks[index]))
        observations+=len(matches)
        for m in matches:
            sides=('long','short') if m['direction']=='neutral' else ('long',) if m['direction']=='bullish' else ('short',)
            for side in sides:
                key=(m['pattern'],side)
                previous=active.get(key)
                if previous is None or index-previous['last']>rearm:
                    previous={'last':index,'seen':set(),'id':index}
                    active[key]=previous
                previous['last']=index
                if m['state'] in previous['seen']:continue
                previous['seen'].add(m['state'])
                atr=max(float(replay.tr[index-20:index].mean()),float(replay.c[index])*.001)
                groups.setdefault(key,[]).append({'signal_index':index,'episode':previous['id'],'state':m['state'],
                    'atr':atr,'score':m['score'],'direction':m['direction'],'pattern_start':m['pattern_start']})
    return groups,observations


def simulate(events,bars,side,rule,lo,hi,config):
    """Fill next open; never use a signal candle as an executable entry.

    Entry eligibility requires the entire maximum horizon inside the split,
    even if a target would have been hit early. Censor gaps while in position.
    Exit-bar OHLC cannot establish intrabar ordering: excursion bounds retain
    that uncertainty instead of claiming a precise maximum after an early exit.
    """
    trades=[]; excluded={'boundary':0,'gap':0,'overlap':0,'invalid_price':0}
    last_exit=-1; sign=1 if side=='long' else -1
    cost=(config['round_trip_fee_bps']+config['round_trip_slippage_bps'])/100
    for e in events:
        if e['state']!=rule.trigger:continue
        entry_index=e['signal_index']+1
        if entry_index<lo or entry_index>=hi:continue
        if entry_index+rule.hold>hi:
            excluded['boundary']+=1;continue
        if entry_index<=last_exit:
            excluded['overlap']+=1;continue
        entry=float(bars[entry_index]['open'])
        stop=entry-sign*rule.stop_atr*e['atr'] if rule.stop_atr else None
        target=entry+sign*rule.stop_atr*rule.target_r*e['atr'] if rule.stop_atr else None
        if entry<=0 or (stop is not None and min(stop,target)<=0):
            excluded['invalid_price']+=1;continue
        best=0.; worst=0.; best_upper=0.; worst_lower=0.; ambiguity=False; failed=False
        for j in range(entry_index,entry_index+rule.hold):
            bar=bars[j]
            if bar['gap']:
                excluded['gap']+=1;failed=True;last_exit=j;break
            o,h,l,c=(float(bar[k]) for k in ('open','high','low','close'))
            favorable=(h-entry)/entry*100 if sign==1 else (entry-l)/entry*100
            adverse=(entry-l)/entry*100 if sign==1 else (h-entry)/entry*100
            reason=None; fill=c; uncertain=False
            if stop is not None:
                if sign*(o-stop)<=0:reason='stop_gap';fill=o
                elif sign*(o-target)>=0:reason='target_gap';fill=o
                else:
                    stop_hit=l<=stop if sign==1 else h>=stop
                    target_hit=h>=target if sign==1 else l<=target
                    if stop_hit:reason='stop';fill=stop;ambiguity=target_hit;uncertain=True
                    elif target_hit:reason='target';fill=target;uncertain=True
            if reason is None and j==entry_index+rule.hold-1:reason='time';fill=c
            if reason and reason.endswith('_gap'):
                observed=sign*(o-entry)/entry*100
                best=max(best,observed);worst=max(worst,-observed)
                best_upper=best;worst_lower=worst
            elif uncertain:
                observed=[sign*(o-entry)/entry*100,sign*(fill-entry)/entry*100]
                best=max(best,*observed);worst=max(worst,*[-v for v in observed])
                # Stop-first convention resolves P&L, while excursion ordering
                # remains unknown. Possible bounds stay within resting barriers.
                best_upper=max(best,min(max(0.,favorable),abs(target-entry)/entry*100))
                worst_lower=worst
                worst=max(worst,min(max(0.,adverse),abs(stop-entry)/entry*100))
            else:
                best=max(best,favorable);worst=max(worst,adverse)
                best_upper=best;worst_lower=worst
            if reason:
                gross=sign*(fill-entry)/entry*100
                trades.append({'signal_index':e['signal_index'],'episode':e['episode'],'signal_time':bars[e['signal_index']]['end'],
                    'entry_index':entry_index,'exit_index':j,'entry_time':bars[entry_index]['time'],
                    'exit_candle_start':bar['time'],'exit_candle_end':bar['end'],'exit_timing':'open' if reason.endswith('_gap') else 'close' if reason=='time' else 'intrabar, exact time unknown',
                    'entry':entry,'exit':fill,'stop':stop,'target':target,'atr_at_signal':e['atr'],
                    'gross_return_pct':gross,'cost_pct':cost,'net_return_pct':gross-cost,
                    'mfe_pct':best,'mfe_upper_pct':best_upper,'mae_pct':worst,'mae_lower_pct':worst_lower,
                    'excursion_uncertain':uncertain,'same_bar_stop_target':ambiguity,'exit_reason':reason,
                    'holding_bars':j-entry_index+1,'signal_direction':e['direction'],'pattern_start':e['pattern_start'],'score':e['score']})
                last_exit=j;break
        if failed:continue
    return trades,excluded


def statistics(trades):
    n=len(trades)
    if not n:return {'n':0,'win_rate':None,'win_rate_ci95':None,'expectancy_pct':None,'expectancy_ci95':None,
        'avg_win_pct':None,'avg_loss_pct':None,'profit_factor':None,'max_favorable_pct':None,'max_adverse_pct':None,
        'mean_favorable_pct':None,'mean_adverse_pct':None,'max_favorable_upper_pct':None,'same_bar_ambiguous':0,'excursion_uncertain':0,'selection_score':None}
    r=np.array([t['net_return_pct'] for t in trades]); winners=r[r>0]; losers=r[r<0]
    wr=len(winners)/n; z=1.959963984540054; d=1+z*z/n
    mid=(wr+z*z/(2*n))/d; half=z*math.sqrt(wr*(1-wr)/n+z*z/(4*n*n))/d
    mean=float(r.mean()); se=float(r.std(ddof=1)/math.sqrt(n)) if n>1 else None
    return {'n':n,'wins':len(winners),'losses':len(losers),'breakeven':int((r==0).sum()),'win_rate':wr*100,
        'win_rate_ci95':[(mid-half)*100,(mid+half)*100], 'expectancy_pct':mean,
        'expectancy_ci95':[mean-z*se,mean+z*se] if se is not None else None,
        'avg_win_pct':float(winners.mean()) if len(winners) else None,
        'avg_loss_pct':float(losers.mean()) if len(losers) else None,
        'profit_factor':float(winners.sum()/-losers.sum()) if len(losers) else None,
        'max_favorable_pct':max(t['mfe_pct'] for t in trades),'max_adverse_pct':max(t['mae_pct'] for t in trades),
        'max_favorable_upper_pct':max(t['mfe_upper_pct'] for t in trades),
        'mean_favorable_pct':float(np.mean([t['mfe_pct'] for t in trades])),
        'mean_adverse_pct':float(np.mean([t['mae_pct'] for t in trades])),
        'same_bar_ambiguous':sum(t['same_bar_stop_target'] for t in trades),
        'excursion_uncertain':sum(t['excursion_uncertain'] for t in trades),
        'selection_score':mean-se if se is not None else None}


def mine_cell(events,bars,timeframe,side,config=None):
    config=config or rules_config(); n=len(bars); horizon=max(config['holding_bars'][timeframe])
    c1=int(n*config['training_fraction']); c2=int(n*(config['training_fraction']+config['validation_fraction']))
    spans={'train':(40,c1),'validation':(c1+horizon,c2),'test':(c2+horizon,n)}
    eligible=[]; tried=[]
    # Test data is not accessed by scoring/selection. Two-stage selection is
    # frozen before the chosen rule is evaluated on the final segment.
    for rule in candidates(timeframe,config):
        ts,excluded=simulate(events,bars,side,rule,*spans['train'],config)
        stats=statistics(ts)
        tried.append({'id':rule.id,'rule':asdict(rule),'train':stats})
        if stats['n']>=config['minimum_training_trades'] and stats['selection_score']>0:
            eligible.append((stats['selection_score'],rule,stats))
    eligible.sort(key=lambda v:(-v[0],v[1].id))
    shortlist=[]
    for _,rule,train in eligible[:config['training_shortlist']]:
        ts,excluded=simulate(events,bars,side,rule,*spans['validation'],config)
        stats=statistics(ts)
        shortlist.append({'id':rule.id,'rule':asdict(rule),'train':train,'validation':stats})
    passed=[r for r in shortlist if r['validation']['n']>=config['minimum_validation_trades'] and r['validation']['selection_score']>0]
    passed.sort(key=lambda r:(-r['validation']['selection_score'],r['id']))
    chosen=Rule(**passed[0]['rule']) if passed else None
    splits={}; ledger=[]
    if chosen:
        for name,span in spans.items():
            trades,excluded=simulate(events,bars,side,chosen,*span,config)
            splits[name]={**statistics(trades),'excluded':excluded}
            ledger.extend([{**t,'split':name} for t in trades])
    # A predeclared reference is shown even when evidence cannot support mining.
    # It never becomes the mined rule and is never labeled a forward estimate.
    reference_rule=Rule('setup',config['holding_bars'][timeframe][1])
    if not any(e['state']=='setup' for e in events):reference_rule=Rule('confirmed',reference_rule.hold)
    reference_trades,excluded=simulate(events,bars,side,reference_rule,40,n,config)
    reference={**statistics(reference_trades),'rule':asdict(reference_rule),'description':reference_rule.describe(),'excluded':excluded,
               'scope':'Whole-history descriptive baseline; not an out-of-sample prediction'}
    sufficient=bool(chosen and splits['test']['n']>=config['minimum_test_trades_for_estimate'])
    status='tested' if sufficient else 'small_test_sample' if chosen else 'no_validated_rule' if events else 'no_occurrences'
    date_spans={name:{'start':bars[lo]['time'] if lo<n and lo<hi else None,'end':bars[hi-1]['end'] if hi>lo else None,
                            'start_index':lo,'end_index_exclusive':hi} for name,(lo,hi) in spans.items()}
    return {'status':status,'bars':n,'events':len(events),'episodes':len({e['episode'] for e in events}),
        'first_candle':bars[0]['time'] if bars else None,'last_candle':bars[-1]['end'] if bars else None,
        'rule':asdict(chosen) if chosen else None,'rule_description':chosen.describe() if chosen else None,
        'splits':splits,'date_spans':date_spans,'embargo_bars':horizon,'candidates_tested':len(tried),
        'shortlist':shortlist,'training_candidates':tried,'reference':reference,
        'next_expectancy_pct':splits['test']['expectancy_pct'] if sufficient else None,
        'historical_win_probability_pct':splits['test']['win_rate'] if sufficient else None,
        'estimate_note':'Held-out historical estimate; not a guarantee of the next trade' if sufficient else 'Insufficient evidence for a next-trade estimate',
        'trades':ledger,'reference_trades':reference_trades}
