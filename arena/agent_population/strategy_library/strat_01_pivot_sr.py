"""STRATEGY #1 — High-Volume Pivot S/R Zones [BigBeluga], PER-STOCK config tuning, long + short.
LONG (bull): resistance breakout / support-hold retest / flipped-res retest.
SHORT (bear): support breakdown / resistance-reject retest / flipped-support retest.
Config family per stock: pivot length {20,40} x volume multiplier {1.2,1.5}. Leak-safe (40/20-bar pivot acts
after confirmation). LONG scored on 3-5d swing, SHORT on 1D."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
def pivot_positions(g, pivlen, volmult, side):
    n=len(g)
    if n<pivlen*2+60: return np.array([],dtype=int)
    o=g.open.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float)
    v=g.volume.values.astype(float); atr=g.atr.values.astype(float); volsma=g.sma20vol.values.astype(float)
    hivol=v>volsma*volmult
    cmax=pd.Series(h).rolling(pivlen*2+1,center=True).max().values
    cmin=pd.Series(l).rolling(pivlen*2+1,center=True).min().values
    is_ph=(h==cmax)&hivol; is_pl=(l==cmin)&hivol
    r_top=r_bot=np.nan; r_broken=False; r_active=False
    s_top=s_bot=np.nan; s_broken=False; s_active=False
    out=[]
    for t in range(pivlen,n-1):
        j=t-pivlen
        if j>=0 and is_ph[j] and not np.isnan(atr[j]):
            bt=max(o[j],c[j]); r_bot=bt; r_top=bt+atr[j]; r_broken=False; r_active=True
        if j>=0 and is_pl[j] and not np.isnan(atr[j]):
            bb=min(o[j],c[j]); s_top=bb; s_bot=bb-atr[j]; s_broken=False; s_active=True
        if side=="bull":
            if r_active and not r_broken and c[t]>r_top: out.append(t); r_broken=True
            elif r_active and r_broken and l[t-1]<=r_top and l[t]>r_top: out.append(t)
            elif s_active and c[t]>s_bot and l[t-1]<=s_top and l[t]>s_top: out.append(t)
        else:  # bear
            if s_active and not s_broken and c[t]<s_bot: out.append(t); s_broken=True
            elif s_active and s_broken and h[t-1]>=s_bot and h[t]<s_bot: out.append(t)
            elif r_active and c[t]<r_top and h[t-1]>=r_bot and h[t]<r_bot: out.append(t)
    return np.array(out,dtype=int)
def mk(pl,vm,side): return lambda g,_p=pl,_v=vm,_s=side: pivot_positions(g,_p,_v,_s)
LONG={f"pl{pl}_v{vm}":mk(pl,vm,"bull") for pl in (20,40) for vm in (1.2,1.5)}
SHORT={f"pl{pl}_v{vm}":mk(pl,vm,"bear") for pl in (20,40) for vm in (1.2,1.5)}
if __name__=="__main__":
    evaluate_family("S01_pivot_sr","High-Volume Pivot S/R (per-stock config)",
        "Res breakout / support retest / flipped retest; pivot len {20,40} x vol mult {1.2,1.5}; LONG 1D+swing, SHORT 1D; best config per stock.",
        LONG, short_configs=SHORT)
