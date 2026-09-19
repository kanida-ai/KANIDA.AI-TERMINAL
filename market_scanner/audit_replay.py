"""Compare every historical close with and without compiled rejection gates."""
import json
import time
from .backtest_store import state,load_history
from .historical import HistoricalReplay
from .replay_filter import masks
from .data import ROOT


def audit(symbols=('TITAN','BEL','HEG','CEMPRO')):
    run=state()['id'];report={'run':run,'passed':True,'stocks':{},'closed_candles_compared':0,'matches_compared':0}
    started=time.monotonic()
    for symbol in symbols:
        frames=load_history(run,symbol);report['stocks'][symbol]={}
        for tf,(bars,quality) in frames.items():
            r=HistoricalReplay(bars);m=masks(r.h,r.l,r.c,r.v,r.tr,r.tops,r.bottoms)
            found=0
            for i in range(39,len(bars)):
                plain=r.at(i);fast=r.at(i,int(m[i]))
                assert plain==fast,(symbol,tf,i,plain,fast)
                found+=len(plain)
            comparisons=max(0,len(bars)-39)
            report['stocks'][symbol][tf]={'closes':comparisons,'matches':found}
            report['closed_candles_compared']+=comparisons;report['matches_compared']+=found
            print(symbol,tf,comparisons,'closes',found,'matches: exact equivalence',flush=True)
    report['seconds']=round(time.monotonic()-started,2)
    (ROOT/'output'/'replay_validation.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2),flush=True)


if __name__=='__main__':audit()
