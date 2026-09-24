"""STRATEGY #4 — CM Ultimate MACD. MACD = ema(fast)-ema(slow); signal = sma(macd, sig).
LONG = MACD crosses ABOVE signal (or above zero); SHORT = crosses BELOW. Per-stock config:
param sets {(12,26,9),(8,21,5),(5,35,5)} x {signal-cross, zero-cross}."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
def macd_positions(g, fast, slow, sig, mode, side):
    n=len(g)
    if n<slow+sig+30: return np.array([],dtype=int)
    c=pd.Series(g.close.values.astype(float))
    macd=(c.ewm(span=fast,adjust=False).mean()-c.ewm(span=slow,adjust=False).mean()).values
    signal=pd.Series(macd).rolling(sig).mean().values
    out=[]
    for t in range(slow+sig,n-1):
        if mode=="sig":
            up=macd[t]>signal[t] and macd[t-1]<=signal[t-1]; dn=macd[t]<signal[t] and macd[t-1]>=signal[t-1]
        else:
            up=macd[t]>0 and macd[t-1]<=0; dn=macd[t]<0 and macd[t-1]>=0
        if side=="bull" and up: out.append(t)
        if side=="bear" and dn: out.append(t)
    return np.array(out,dtype=int)
def mk(f,s,sg,mode,side): return lambda g,_f=f,_s=s,_g=sg,_m=mode,_d=side: macd_positions(g,_f,_s,_g,_m,_d)
PARAMS=[(12,26,9),(8,21,5),(5,35,5)]
LONG={}; SHORT={}
for f,s,sg in PARAMS:
    LONG[f"{f}_{s}_{sg}_sig"]=mk(f,s,sg,"sig","bull"); SHORT[f"{f}_{s}_{sg}_sig"]=mk(f,s,sg,"sig","bear")
LONG["12_26_9_zero"]=mk(12,26,9,"zero","bull"); SHORT["12_26_9_zero"]=mk(12,26,9,"zero","bear")
if __name__=="__main__":
    evaluate_family("S04_macd","CM Ultimate MACD (per-stock config)",
        "MACD signal-cross / zero-cross; params {(12,26,9),(8,21,5),(5,35,5)}; LONG 1D+swing, SHORT 1D.",
        LONG, short_configs=SHORT)
