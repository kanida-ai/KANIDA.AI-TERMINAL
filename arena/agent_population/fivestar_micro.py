"""FIVESTAR micro-diagnostic — signal 2025-01-23 (the winning trade 01-24, +5.58% intraday, Falcon rank 24).
Read every micro-element: which patterns it FIRES (and their lift), and which HIGH-lift patterns it MISSES by
exactly ONE condition (near-miss) + which feature/threshold is the blocker. Then: relax which parameter lifts
its avg_lift past the #15 cut (12.72, gap 0.91)? Leak-free PIT. Read-only."""
import os, sys, sqlite3, warnings
from collections import defaultdict
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0,os.path.join(ROOT,"scripts")); import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
SD=sys.argv[2] if len(sys.argv)>2 else "2025-01-23"; SYM=sys.argv[1] if len(sys.argv)>1 else "FIVESTAR"; CUT15=float(sys.argv[3]) if len(sys.argv)>3 else 12.72
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query(f"SELECT * FROM falcon_features WHERE symbol='{SYM}' AND trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query(f"SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE symbol='{SYM}' AND trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY trade_date",con); con.close()
g=oh.copy(); dt=pd.to_datetime(g.trade_date); g["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
wk=pd.DataFrame(dict(trade_date=g.trade_date.values,
    weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
    weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan)))
FC=feat.drop(columns=WEEKLY).merge(wk,on="trade_date",how="left")
r=FC[FC.trade_date==SD].iloc[0]
Xd={col:(float(r[col]) if col in FC.columns and pd.notna(r[col]) else np.nan) for col in FR.FEATURE_COLS}
print("  FIVESTAR micro-elements on signal 2025-01-23 (leak-free week-to-date):")
for k in ["atr_20_pct","weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","roc_20","roc_60","rsi_14","dist_high_20","dist_sma_200","slope_sma_50","close_loc","roc_5"]:
    print(f"     {k:<22}{Xd.get(k,float('nan')):.3f}")
def passes(op,v,th):
    if v!=v: return False
    return (op==">" and v>th) or (op==">=" and v>=th) or (op=="<=" and v<=th) or (op=="<" and v<th)
fired=[]; near=defaultdict(list)
for p in pats:
    if int(p["mined_year"])>=2025: continue
    fails=[(f,op,th) for f,op,th in p["rule"] if not passes(op,Xd.get(f,np.nan),th)]
    if len(fails)==0: fired.append((p["pattern_id"],p["oos_lift"] or 0))
    elif len(fails)==1:
        f,op,th=fails[0]; v=Xd.get(f,np.nan)
        gap=(th-v) if op in(">",">=") else (v-th)   # how far X must move
        near[f].append((p["pattern_id"],p["oos_lift"] or 0,op,th,v,gap))
n=len(fired); s=sum(l for _,l in fired); avg=s/max(n,1)
print(f"\n  FIRES {n} patterns · sum_lift {s:.1f} · avg_lift {avg:.2f}  (Falcon rank 24; #15 cut = {CUT15}; needs +{CUT15-avg:.2f})")
lows=sorted(fired,key=lambda x:x[1])[:8]; his=sorted(fired,key=lambda x:-x[1])[:6]
print(f"    top fires:  "+", ".join(f"{p}({l:.0f})" for p,l in his))
print(f"    DRAG (lowest-lift fires diluting the avg): "+", ".join(f"{p}({l:.1f})" for p,l in lows))
print(f"\n  NEAR-MISS high-lift patterns (miss by ONE condition) grouped by BLOCKING feature:")
print(f"  {'feature (parameter)':<24}{'#miss':>6}{'#lift>=15':>10}{'sum lift>=15':>13}   examples (need X move)")
rankrows=[]
for f,lst in sorted(near.items(),key=lambda kv:-sum(l for _,l,_,_,_,_ in kv[1] if l>=15)):
    hi15=[x for x in lst if x[1]>=15]
    if not hi15: continue
    ex="; ".join(f"{p}(lift{l:.0f},{op}{th}, X={v:.2f}, +{gap:.2f})" for p,l,op,th,v,gap in sorted(hi15,key=lambda x:-x[1])[:2])
    print(f"  {f:<24}{len(lst):>6}{len(hi15):>10}{sum(l for _,l,_,_,_,_ in hi15):>13.1f}   {ex[:70]}")
    rankrows.append((f,hi15))
# simulate: relax feature f to capture its near-misses -> new avg_lift for FIVESTAR
print(f"\n  IF we relax ONE parameter for FIVESTAR (add its near-miss fires) -> new avg_lift:")
print(f"  {'parameter relaxed':<24}{'new n':>7}{'new avg_lift':>14}{'>= 12.72 cut?':>16}")
best=[]
for f,hi15 in rankrows:
    add=[(p,l) for p,l,_,_,_,_ in hi15]           # add all lift>=15 near-misses on this feature
    nn=n+len(add); ns=s+sum(l for _,l in add); na=ns/nn
    ok="YES -> top15" if na>=CUT15 else ""
    print(f"  {f:<24}{nn:>7}{na:>14.2f}{ok:>16}")
    best.append((f,na,ok))
