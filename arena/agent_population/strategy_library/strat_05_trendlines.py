"""STRATEGY #5 — Trendlines with Breaks [LuxAlgo]. Pivot(len,len) anchored trendlines with a decaying slope
(Atr or Stdev based). LONG = price breaks up through the descending upper trendline; SHORT = breaks down
through the ascending lower trendline. Per-stock config: length {10,14,20} x method {Atr,Stdev} (mult=1)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
def _atr(h,l,c,n):
    pc=np.roll(c,1); pc[0]=c[0]
    tr=np.maximum(h-l,np.maximum(np.abs(h-pc),np.abs(l-pc)))
    return pd.Series(tr).rolling(n).mean().values
def trend_positions(g,length,method,mult,side):
    n=len(g)
    if n<2*length+30: return np.array([],dtype=int)
    h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float)
    slope=(_atr(h,l,c,length) if method=="Atr" else pd.Series(c).rolling(length).std().values)/length*mult
    cmax=pd.Series(h).rolling(2*length+1,center=True).max().values; cmin=pd.Series(l).rolling(2*length+1,center=True).min().values
    is_ph=h==cmax; is_pl=l==cmin
    upper=0.0; lower=0.0; sph=0.0; spl=0.0; upos=0; dnos=0; seen_h=False; seen_l=False; out=[]
    for t in range(length,n-1):
        j=t-length; ph=j>=0 and is_ph[j]; pl=j>=0 and is_pl[j]; s=slope[t]
        if s!=s: s=0.0
        if ph: sph=s; seen_h=True
        if pl: spl=s; seen_l=True
        upper=h[j] if ph else upper-sph
        lower=l[j] if pl else lower+spl
        pu=upos; pd_=dnos
        upos=0 if ph else (1 if c[t]>upper-sph*length else upos)
        dnos=0 if pl else (1 if c[t]<lower+spl*length else dnos)
        if side=="bull" and seen_h and upos>pu: out.append(t)
        if side=="bear" and seen_l and dnos>pd_: out.append(t)
    return np.array(out,dtype=int)
def mk(L,m,side): return lambda g,_L=L,_m=m,_s=side: trend_positions(g,_L,_m,1.0,_s)
LONG={f"L{L}_{m}":mk(L,m,"bull") for L in (10,14,20) for m in ("Atr","Stdev")}
SHORT={f"L{L}_{m}":mk(L,m,"bear") for L in (10,14,20) for m in ("Atr","Stdev")}
if __name__=="__main__":
    evaluate_family("S05_trendlines","Trendlines with Breaks (per-stock config)",
        "Pivot trendline breakouts, decaying slope Atr/Stdev; length{10,14,20}; LONG 1D+swing, SHORT 1D.",
        LONG, short_configs=SHORT)
