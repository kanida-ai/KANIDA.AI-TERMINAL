"""WALK-FORWARD (leak-free) for S05 Trendlines, 2026 test. Select each stock's confident config using ONLY
2023-2025 (train); freeze it; apply to 2026 (out-of-sample). Monthly view, LONG & SHORT separate.
LONG: metric 3d swing (report 1D + 3d). SHORT: metric 1D. Capital Rs5,00,000/day equal-weight basket, 1x."""
import os, sys, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from harness import load_cache, _gen_signals
from strat_05_trendlines import LONG, SHORT
CAP=500000.0
D=load_cache()
AL=_gen_signals(D, LONG, +1.0); AS=_gen_signals(D, SHORT, -1.0)
def select_train(A, metric):
    A=A.copy(); A["yr"]=A.entry_date.str[:4].astype(int); tr=A[A.yr<=2025]; chosen={}
    for sym,ds in tr.groupby("symbol"):
        best=None
        for cfg,d in ds.groupby("config"):
            if len(d)<6: continue
            wy=sum(1 for y in (2023,2024,2025) if len(d[d.yr==y])>=3 and (d[d.yr==y][metric]>0).mean()>=0.60 and d[d.yr==y][metric].mean()>1.0)
            if (d[metric]>0).mean()>=0.60 and d[metric].mean()>1.0 and wy>=2:
                key=(wy, d[metric].mean())
                if best is None or key>best[0]: best=(key,cfg)
        if best: chosen[sym]=best[1]
    return chosen
def test_2026(A, chosen):
    A=A.copy(); A["mo"]=A.entry_date.str[:7]
    keep=A[(A.entry_date>="2026-01-01")&(A.apply(lambda r: chosen.get(r.symbol)==r.config,axis=1))]
    return keep
def monthly(df, horizon):
    rc="ret_nextday" if horizon=="1D" else "ret_3d"
    rows=[]
    for mo in sorted(df.mo.unique()):
        d=df[df.mo==mo]
        daily=d.groupby("entry_date")[rc].mean()
        ret1x=( (1+d.groupby("entry_date")["ret_nextday"].mean()/100).prod()-1 )*100
        rows.append(dict(month=mo,stocks=len(d),days=d.entry_date.nunique(),stk_per_day=round(len(d)/max(d.entry_date.nunique(),1),1),
            days_pos=int((d.groupby("entry_date")["ret_nextday"].mean()>0).sum()),
            ret1x_pct=round(ret1x,2),ret_1D_avg=round(d.ret_nextday.mean(),2),ret_3d_avg=round(d.ret_3d.mean(),2),
            pnl_rs_1x=int(CAP*ret1x/100)))
    return pd.DataFrame(rows)
chL=select_train(AL,"ret_3d"); chS=select_train(AS,"ret_nextday")
tL=test_2026(AL,chL); tS=test_2026(AS,chS)
print(f"  S05 WALK-FORWARD 2026 (config chosen on 2023-2025, frozen, applied to 2026) · Rs5L/day 1x basket\n")
print(f"  LONG: {len(chL)} stocks selected on train · SHORT: {len(chS)} stocks selected on train\n")
print("  === LONG (2026, out-of-sample) ===")
mL=monthly(tL,"3d")
print(f"  {'month':<9}{'stk/day':>8}{'days':>6}{'days_pos':>9}{'ret1x%(1D)':>11}{'1D avg%':>9}{'3d avg%':>9}{'pnl_rs(1x)':>12}")
for _,r in mL.iterrows():
    print(f"  {r['month']:<9}{r['stk_per_day']:>8}{int(r['days']):>6}{int(r['days_pos']):>9}{r['ret1x_pct']:>10}%{r['ret_1D_avg']:>+8.2f}%{r['ret_3d_avg']:>+8.2f}%{int(r['pnl_rs_1x']):>12,}")
if len(mL): print(f"  {'TOTAL':<9}{'':<8}{int(mL.days.sum()):>6}{int(mL.days_pos.sum()):>9}{((np.prod(1+mL.ret1x_pct/100)-1)*100):>10.1f}%{mL.ret_1D_avg.mean():>+8.2f}%{mL.ret_3d_avg.mean():>+8.2f}%{int(mL.pnl_rs_1x.sum()):>12,}")
print("\n  === SHORT (2026, out-of-sample, 1D) ===")
mS=monthly(tS,"1D")
print(f"  {'month':<9}{'stk/day':>8}{'days':>6}{'days_pos':>9}{'ret1x%(1D)':>11}{'1D avg%':>9}{'pnl_rs(1x)':>12}")
for _,r in mS.iterrows():
    print(f"  {r['month']:<9}{r['stk_per_day']:>8}{int(r['days']):>6}{int(r['days_pos']):>9}{r['ret1x_pct']:>10}%{r['ret_1D_avg']:>+8.2f}%{int(r['pnl_rs_1x']):>12,}")
if len(mS): print(f"  {'TOTAL':<9}{'':<8}{int(mS.days.sum()):>6}{int(mS.days_pos.sum()):>9}{((np.prod(1+mS.ret1x_pct/100)-1)*100):>10.1f}%{mS.ret_1D_avg.mean():>+8.2f}%{int(mS.pnl_rs_1x.sum()):>12,}")
