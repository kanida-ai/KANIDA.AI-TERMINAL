"""Read-only whole-snapshot audit of the customer exit-plan endpoint."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import json
import math
from urllib.request import urlopen
from urllib.parse import urlencode
from .data import ROOT

def get(path):
    with urlopen('http://127.0.0.1:8082'+path,timeout=45) as response:return json.load(response)

def main():
    matches=get('/api/matches?min_trades=0')
    tasks=[(m,h) for m in matches for h in m['history']]
    def check(item):
        m,h=item
        p=get('/api/product/exit-plan?'+urlencode({'match_id':m['id'],'side':h['side'],'run':h['run'],'snapshot':m['candle_end']}))
        assert p['match_id']==m['id'] and p['side']==h['side'] and p['run']==h['run']
        assert all(math.isfinite(p[k]) for k in ('price','stop','target','stop_pct','target_pct','reward'))
        assert not p['execution_ready']
        assert p['evidence_applies']==(p['status']=='supported')
        if not p['evidence_applies']:assert p['rule_metrics'] is None
        if p['usable']:
            assert 0<p['stop_pct']<=20 and min(p['stop'],p['target'])>0
            assert p['stop']<p['price']<p['target'] if h['side']=='long' else p['target']<p['price']<p['stop']
        return dict(id=m['id'],side=h['side'],timeframe=m['timeframe'],status=p['status'],usable=p['usable'],stop_pct=p['stop_pct'],reward=p['reward'])
    with ThreadPoolExecutor(max_workers=4) as pool:rows=list(pool.map(check,tasks))
    result=dict(setups=len(matches),direction_studies=len(rows),statuses=dict(Counter(r['status'] for r in rows)),timeframes=dict(Counter(r['timeframe'] for r in rows)),unusable=sum(not r['usable'] for r in rows),rows=rows)
    (ROOT/'output'/'exit_plan_validation.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='rows'}),flush=True)

if __name__=='__main__':main()
