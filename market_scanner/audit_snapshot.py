"""Validate a persisted scan against source OHLC and report local API timings."""
import json
import time
from collections import Counter
from datetime import datetime
from urllib.request import urlopen

from .data import ROOT, Calendar, aggregate, connect_source, load_config, load_rows, observed_sessions
from .engine import Scanner


def main():
    config=load_config()
    scanner=Scanner(config)
    counts=Counter()
    for (symbol,tf),cell in scanner.cells.items():
        bars=cell['bars']
        assert all(a['time']<b['time'] for a,b in zip(bars,bars[1:])), (symbol,tf,'ordering')
        for match in cell['matches']:
            assert match['timeframe']==tf and match['symbol']==symbol
            assert match['candle_end']==bars[-1]['end']
            assert not any(b['gap'] for b in bars[match['start_index']+1:])
            assert 72<=match['score']<=99
            for line in match['lines']:
                assert all(0<=p['index']<len(bars) for p in line['points'])
            counts[f"{match['pattern']}:{tf}"]+=1
    source_comparisons=[]
    with connect_source(scanner.source) as source:
        calendar=Calendar(config,observed_sessions(source))
        for symbol,tf in [('HEG','4H'),('TITAN','1H'),('TITAN','1D'),('TITAN','1W')]:
            rows=load_rows(source,symbol,tf in ('1H','4H'),datetime(2026,9,11,18))
            bars,_=aggregate(rows,tf,calendar,datetime(2026,9,11,18))
            actual=scanner.cells[(symbol,tf)]['bars']
            assert actual==bars,(symbol,tf,'source mismatch')
            source_comparisons.append({'symbol':symbol,'timeframe':tf,'bars':len(bars),'last_close':bars[-1]['end']})
    timings={}
    for endpoint in ('/api/state','/api/matches','/api/chart?symbol=HEG&timeframe=4H','/api/stock?symbol=TITAN'):
        samples=[]
        for _ in range(3):
            start=time.perf_counter()
            with urlopen(f'http://127.0.0.1:{config["port"]}'+endpoint) as response:
                data=json.load(response)
            samples.append(round((time.perf_counter()-start)*1000,2))
        timings[endpoint]=samples
    with urlopen(f'http://127.0.0.1:{config["port"]}/api/matches?current=true') as response:
        assert json.load(response)==[], 'Stale July data was incorrectly labeled current'
    result={'snapshot':scanner.metadata,'cells':len(scanner.cells),'matrix':dict(sorted(counts.items())),
            'source_comparisons':source_comparisons,'api_milliseconds':timings,'result':'passed'}
    (ROOT/'output'/'validation.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__': main()
