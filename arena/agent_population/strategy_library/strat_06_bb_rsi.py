"""STRATEGY #6 — Bollinger + RSI Double (ChartArt). LONG when RSI crosses ABOVE 50 AND close crosses back
ABOVE the lower Bollinger band (same bar); SHORT when RSI crosses BELOW 50 AND close crosses below the upper
band. Per-stock config: RSI period {6,14} x BB period {50,200} (mult 2)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
def _rsi(c,n):
    d=np.diff(c,prepend=c[0]); up=np.where(d>0,d,0.0); dn=np.where(d<0,-d,0.0)
    ru=pd.Series(up).ewm(alpha=1/n,adjust=False).mean().values; rd=pd.Series(dn).ewm(alpha=1/n,adjust=False).mean().values
    rs=ru/np.where(rd==0,np.nan,rd); return 100-100/(1+rs)
def bbrsi_positions(g,rsin,bbn,mult,side):
    n=len(g); c=g.close.values.astype(float)
    if n<bbn+30: return np.array([],dtype=int)
    rsi=_rsi(c,rsin); basis=pd.Series(c).rolling(bbn).mean().values; sd=pd.Series(c).rolling(bbn).std().values
    up=basis+mult*sd; lo=basis-mult*sd; out=[]
    for t in range(bbn,n-1):
        if side=="bull":
            if rsi[t]>50 and rsi[t-1]<=50 and c[t]>lo[t] and c[t-1]<=lo[t-1]: out.append(t)
        else:
            if rsi[t]<50 and rsi[t-1]>=50 and c[t]<up[t] and c[t-1]>=up[t-1]: out.append(t)
    return np.array(out,dtype=int)
def mk(r,b,side): return lambda g,_r=r,_b=b,_s=side: bbrsi_positions(g,_r,_b,2.0,_s)
LONG={f"rsi{r}_bb{b}":mk(r,b,"bull") for r in (6,14) for b in (50,200)}
SHORT={f"rsi{r}_bb{b}":mk(r,b,"bear") for r in (6,14) for b in (50,200)}
if __name__=="__main__":
    evaluate_family("S06_bb_rsi","Bollinger + RSI Double (per-stock config)",
        "RSI-50 cross + BB band recross; RSI{6,14} x BB{50,200}; LONG 1D+swing, SHORT 1D.",
        LONG, short_configs=SHORT)
