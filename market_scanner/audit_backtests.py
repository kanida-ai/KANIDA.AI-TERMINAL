"""Audit frozen, independently reproducible cells and selected trade ledgers."""
from __future__ import annotations
import hashlib
import json
import math
import time
import zlib
from .backtest import Rule,statistics,simulate,rules_config
from .backtest_store import connection,state,load_history,dumps
from .data import ROOT
from .detectors import detect
from .historical import HistoricalReplay


def close(a,b):return a==b or (a is not None and b is not None and math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-10))


def audit(require_complete=False):
    began=time.monotonic();s=state();run=s['id'];rules=s['rules']
    if require_complete:assert s['status']=='complete',s['status']
    count=0;trades_checked=0;selected=[];chosen_studies=[];coverage={};violations=[]
    with connection() as con:
        con.execute('BEGIN')
        assert con.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        for symbol,tf,pattern,side,status,summary,payload in con.execute('SELECT symbol,timeframe,pattern,side,status,summary,payload FROM cells WHERE run=?',(run,)):
            count+=1;d=json.loads(zlib.decompress(payload));small=json.loads(summary)
            try:
                assert (d['symbol'],d['timeframe'],d['pattern'],d['side'])==(symbol,tf,pattern,side)
                assert d['status']==status
                assert d['reference']['n']==len(d['reference_trades'])
                assert d['episodes']>=0
                reference_stats=statistics(d['reference_trades'])
                for key in ('n','win_rate','expectancy_pct','max_favorable_pct','max_adverse_pct'):
                    assert close(d['reference'][key],reference_stats[key]),key
                for split,stat in d['splits'].items():
                    ts=[t for t in d['trades'] if t['split']==split];computed=statistics(ts)
                    for key in ('n','win_rate','expectancy_pct','max_favorable_pct','max_adverse_pct'):
                        assert close(stat[key],computed[key]),(split,key)
                    lo=d['date_spans'][split]['start_index'];hi=d['date_spans'][split]['end_index_exclusive']
                    for t in ts:
                        assert lo<=t['entry_index']<=t['exit_index']<hi
                        assert t['entry_index']+d['rule']['hold']<=hi
                    assert all(a['exit_index']<b['entry_index'] for a,b in zip(ts,ts[1:]))
                for t in d['trades']+d['reference_trades']:
                    trades_checked+=1
                    assert t['entry_index']==t['signal_index']+1
                    assert t['entry_time']>=t['signal_time']
                    assert close(t['net_return_pct'],t['gross_return_pct']-t['cost_pct'])
                    assert -1e-9<=t['mfe_pct']<=t['mfe_upper_pct']+1e-9
                    assert -1e-9<=t['mae_lower_pct']<=t['mae_pct']+1e-9
                    assert t['holding_bars']==t['exit_index']-t['entry_index']+1
                if status=='tested':
                    assert d['splits']['test']['n']>=rules['minimum_test_trades_for_estimate']
                    assert d['next_expectancy_pct']==d['splits']['test']['expectancy_pct']
                    assert d['historical_win_probability_pct']==d['splits']['test']['win_rate']
                else:assert d['next_expectancy_pct'] is None and d['historical_win_probability_pct'] is None
                if d['rule']:
                    assert d['splits']['train']['n']>=rules['minimum_training_trades']
                    assert d['splits']['validation']['n']>=rules['minimum_validation_trades']
                    assert d['splits']['train']['selection_score']>0 and d['splits']['validation']['selection_score']>0
                    chosen_studies.append(d)
                coverage[status]=coverage.get(status,0)+1
                if (symbol in ('TITAN','BEL','HEG') or status=='tested') and d['reference_trades'] and len(selected)<100:
                    selected.append(d)
            except AssertionError as error:violations.append({'cell':[symbol,tf,pattern,side],'error':str(error)})
        stocks=con.execute("SELECT count(*) FROM stocks WHERE run=? AND status='complete'",(run,)).fetchone()[0]
        assert count==stocks*52,(count,stocks)
    charts_checked=0;checksums=set()
    for d in selected:
        history=load_history(run,d['symbol']);bars=history[d['timeframe']][0]
        if d['symbol'] not in checksums:
            assert hashlib.sha256(dumps(history).encode()).hexdigest()==d['history_sha256']
            checksums.add(d['symbol'])
        for t in [d['reference_trades'][0],d['reference_trades'][-1]]:
            index=t['signal_index'];window=bars[max(0,index-259):index+1]
            matches=detect(window);m=next((m for m in matches if m['pattern']==d['pattern']),None)
            assert m is not None,(d['symbol'],d['timeframe'],d['pattern'],index)
            assert m['candle_end']==t['signal_time'] and m['pattern_start']==t['pattern_start']
            event={'signal_index':index,'episode':t['episode'],'state':d['reference']['rule']['trigger'],
                'atr':t['atr_at_signal'],'score':t['score'],'direction':t['signal_direction'],'pattern_start':t['pattern_start']}
            replayed,_=simulate([event],bars,d['side'],Rule(**d['reference']['rule']),40,len(bars),rules)
            assert replayed and replayed[0]==t,(d['symbol'],d['timeframe'],index)
            charts_checked+=1
    selected_rule_fills=0
    for d in sorted(chosen_studies,key=lambda d:d['symbol']):
        history=load_history(run,d['symbol']);bars=history[d['timeframe']][0]
        assert hashlib.sha256(dumps(history).encode()).hexdigest()==d['history_sha256']
        for t in d['trades']:
            span=d['date_spans'][t['split']]
            event={'signal_index':t['signal_index'],'episode':t['episode'],'state':d['rule']['trigger'],
                'atr':t['atr_at_signal'],'score':t['score'],'direction':t['signal_direction'],'pattern_start':t['pattern_start']}
            replayed,_=simulate([event],bars,d['side'],Rule(**d['rule']),span['start_index'],span['end_index_exclusive'],rules)
            assert replayed and replayed[0]=={k:v for k,v in t.items() if k!='split'},(d['symbol'],d['timeframe'],t['signal_index'])
            selected_rule_fills+=1
    report={'run':run,'status':s['status'],'stocks':stocks,'cells':count,'ledger_trades_checked':trades_checked,
        'historical_charts_and_fills_reproduced':charts_checked,'selected_rule_fills_reproduced':selected_rule_fills,'frozen_stock_checksums':len(checksums),
        'coverage':coverage,'violations':violations,'passed':not violations,'seconds':round(time.monotonic()-began,2)}
    (ROOT/'output'/'backtest_validation.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2),flush=True)
    assert not violations,violations[:10]
    return report


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--require-complete',action='store_true');a=p.parse_args();audit(a.require_complete)
