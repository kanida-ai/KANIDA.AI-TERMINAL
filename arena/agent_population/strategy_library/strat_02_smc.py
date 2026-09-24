"""STRATEGY #2 — LuxAlgo Smart Money Concepts (bullish structure), next-day LONG, PER-STOCK config tuning.
Ports the leg/pivot + BOS/CHoCH structure-break logic. Bullish signal = close breaks above the last
internal/swing pivot high; tag = CHoCH (prior trend down = reversal) or BOS (prior trend up = continuation).
Config family iterated per stock: {structure = internal(size 5/10) or swing(size 25/50)} x {BOS / CHoCH / ALL}.
The library keeps each stock's BEST config. Leak-safe: pivot confirmed `size` bars later; act on the break bar."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd
from harness import evaluate_family
BULL,BEAR=1,-1
def smc_bull_positions(g, size, types):
    """Return row positions where a bullish BOS/CHoCH break occurs. types subset of {'BOS','CHoCH'}."""
    n=len(g)
    if n<size*2+30: return np.array([],dtype=int)
    h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float)
    rollmax=pd.Series(h).rolling(size).max().values; rollmin=pd.Series(l).rolling(size).min().values
    h_shift=pd.Series(h).shift(size).values; l_shift=pd.Series(l).shift(size).values
    newLegHigh=h_shift>rollmax; newLegLow=l_shift<rollmin
    # leg state machine
    leg=np.zeros(n,dtype=int); cur=0
    for t in range(n):
        if newLegHigh[t]: cur=0
        elif newLegLow[t]: cur=1
        leg[t]=cur
    legprev=np.roll(leg,1); legprev[0]=leg[0]
    startBear=(leg==0)&(legprev==1)   # pivot HIGH made at t-size
    startBull=(leg==1)&(legprev==0)   # pivot LOW  made at t-size
    swingHigh=np.nan; swingLow=np.nan; trend=0; hi_crossed=True; lo_crossed=True
    out=[]
    for t in range(size,n):
        if startBear[t] and t-size>=0: swingHigh=h[t-size]; hi_crossed=False
        if startBull[t] and t-size>=0: swingLow=l[t-size]; lo_crossed=False
        if not np.isnan(swingHigh) and not hi_crossed and c[t]>swingHigh:
            tag="CHoCH" if trend==BEAR else "BOS"; trend=BULL; hi_crossed=True
            if tag in types: out.append(t)
        if not np.isnan(swingLow) and not lo_crossed and c[t]<swingLow:
            trend=BEAR; lo_crossed=True   # bearish break (not a long signal)
    return np.array(out,dtype=int)
def smc_bear_positions(g, size, types):
    """Bearish structure break: close crosses BELOW last pivot low. tag CHoCH (prior up) / BOS (prior down)."""
    n=len(g)
    if n<size*2+30: return np.array([],dtype=int)
    h=g.high.values.astype(float); l=g.low.values.astype(float); c=g.close.values.astype(float)
    rollmax=pd.Series(h).rolling(size).max().values; rollmin=pd.Series(l).rolling(size).min().values
    h_shift=pd.Series(h).shift(size).values; l_shift=pd.Series(l).shift(size).values
    newLegHigh=h_shift>rollmax; newLegLow=l_shift<rollmin
    leg=np.zeros(n,dtype=int); cur=0
    for t in range(n):
        if newLegHigh[t]: cur=0
        elif newLegLow[t]: cur=1
        leg[t]=cur
    legprev=np.roll(leg,1); legprev[0]=leg[0]
    startBear=(leg==0)&(legprev==1); startBull=(leg==1)&(legprev==0)
    swingHigh=np.nan; swingLow=np.nan; trend=0; hi_crossed=True; lo_crossed=True
    out=[]
    for t in range(size,n):
        if startBear[t] and t-size>=0: swingHigh=h[t-size]; hi_crossed=False
        if startBull[t] and t-size>=0: swingLow=l[t-size]; lo_crossed=False
        if not np.isnan(swingHigh) and not hi_crossed and c[t]>swingHigh:
            trend=BULL; hi_crossed=True
        if not np.isnan(swingLow) and not lo_crossed and c[t]<swingLow:
            tag="CHoCH" if trend==BULL else "BOS"; trend=BEAR; lo_crossed=True
            if tag in types: out.append(t)
    return np.array(out,dtype=int)
def mk(size,types): return lambda g,_s=size,_t=types: smc_bull_positions(g,_s,_t)
def mkS(size,types): return lambda g,_s=size,_t=types: smc_bear_positions(g,_s,_t)
CONFIGS={
 "int5_choch":  mk(5,{"CHoCH"}),
 "int5_bos":    mk(5,{"BOS"}),
 "int5_all":    mk(5,{"BOS","CHoCH"}),
 "int10_choch": mk(10,{"CHoCH"}),
 "int10_all":   mk(10,{"BOS","CHoCH"}),
 "sw25_choch":  mk(25,{"CHoCH"}),
 "sw25_all":    mk(25,{"BOS","CHoCH"}),
 "sw50_choch":  mk(50,{"CHoCH"}),
 "sw50_bos":    mk(50,{"BOS"}),
 "sw50_all":    mk(50,{"BOS","CHoCH"}),
}
SHORT_CONFIGS={
 "int5_choch":  mkS(5,{"CHoCH"}),  "int5_bos": mkS(5,{"BOS"}),  "int5_all": mkS(5,{"BOS","CHoCH"}),
 "int10_all":   mkS(10,{"BOS","CHoCH"}), "sw25_all": mkS(25,{"BOS","CHoCH"}),
 "sw50_choch":  mkS(50,{"CHoCH"}), "sw50_all": mkS(50,{"BOS","CHoCH"}),
}
if __name__=="__main__":
    evaluate_family("S02_smc","Smart Money Concepts (BOS/CHoCH, per-stock config)",
        "Structure-break: internal(5/10) & swing(25/50) pivots x BOS/CHoCH/ALL; next-day entry; LONG 1D+3-5d swing, SHORT 1D; best config per stock.",
        CONFIGS, short_configs=SHORT_CONFIGS)
