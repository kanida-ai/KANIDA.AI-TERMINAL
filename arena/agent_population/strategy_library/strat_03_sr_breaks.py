"""STRATEGY #3 — LuxAlgo Support/Resistance Levels with Breaks. Pivot(L,L) levels; break confirmed by a
volume oscillator osc = 100*(ema5(vol)-ema10(vol))/ema10(vol) > threshold. LONG = close crosses above
resistance (+vol); SHORT = close crosses below support (+vol). Per-stock config: L {10,15,20} x volThresh {0,20}."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
def _osc(v):
    s=pd.Series(v.astype(float)); return (100*(s.ewm(span=5,adjust=False).mean()-s.ewm(span=10,adjust=False).mean())/s.ewm(span=10,adjust=False).mean()).values
def sr_positions(g, L, volth, side):
    n=len(g)
    if n<2*L+30: return np.array([],dtype=int)
    h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float); osc=_osc(g.volume.values)
    cmax=pd.Series(h).rolling(2*L+1,center=True).max().values; cmin=pd.Series(l).rolling(2*L+1,center=True).min().values
    is_ph=h==cmax; is_pl=l==cmin
    hp=np.nan; lp=np.nan; out=[]
    for t in range(L,n-1):
        j=t-L
        if j>=0 and is_ph[j]: hp=h[j]
        if j>=0 and is_pl[j]: lp=l[j]
        if side=="bull":
            if not np.isnan(hp) and c[t]>hp and c[t-1]<=hp and osc[t]>volth: out.append(t)
        else:
            if not np.isnan(lp) and c[t]<lp and c[t-1]>=lp and osc[t]>volth: out.append(t)
    return np.array(out,dtype=int)
def mk(L,vt,side): return lambda g,_L=L,_v=vt,_s=side: sr_positions(g,_L,_v,_s)
LONG={f"L{L}_v{vt}":mk(L,vt,"bull") for L in (10,15,20) for vt in (0,20)}
SHORT={f"L{L}_v{vt}":mk(L,vt,"bear") for L in (10,15,20) for vt in (0,20)}
if __name__=="__main__":
    evaluate_family("S03_sr_breaks","S/R Levels with Breaks + Volume (per-stock config)",
        "Pivot(L,L) break confirmed by volume oscillator; L{10,15,20} x volThresh{0,20}; LONG 1D+swing, SHORT 1D.",
        LONG, short_configs=SHORT)
