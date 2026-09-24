"""Jan-2025 ALL leak-free Falcon signals (the ranked top-100 each day), tiered (Falcon-free), counted by
Falcon RANK band x tier. Focus tiers: PREMIUM-Pullback, GOLD, PREMIUM-Compression. Summed across all Jan
signal days. Read-only. Production untouched."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique())
# tier features per (symbol, date)
trows=[]
for s,g in oh.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True)
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    pc=np.roll(c,1); pc[0]=np.nan; c2=np.roll(c,2); c2[:2]=np.nan
    sret=(c/pc-1)*100; twoday=(c/c2-1)*100; rng=(h-l)/pc*100
    trend=pd.Series(v).rolling(3).mean().values/pd.Series(v).rolling(20).mean().values
    turn=c*v; turnpct=pd.Series(turn).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i in range(len(g)): trows.append((s,g.trade_date.values[i],sret[i],twoday[i],rng[i],trend[i],turnpct[i]))
TF=pd.DataFrame(trows,columns=["symbol","trade_date","sret","twoday","rng","trend3_20","turn_pct"]).set_index(["symbol","trade_date"])
# leak-free weekly + rank
o2=oh[["symbol","trade_date","high","low","close"]].copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
def rank_all(sd):
    fd=BASE[BASE.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in pats:
        if int(p["mined_year"])>=yr: continue
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*(p["oos_lift"] or 0)
    cands=[{"symbol":syms[i],"nf":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return [c["symbol"] for c in ranked]
jan_sigs=[d for d in cal if "2025-01-01"<=d<="2025-01-31"]
def band(r): return "01-15" if r<=15 else "16-30" if r<=30 else "31-50" if r<=50 else "51-100"
rec=[]
for sd in jan_sigs:
    for rank,sym in enumerate(rank_all(sd),1):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else "NA"
        rec.append((sd,sym,rank,band(rank),tier))
R=pd.DataFrame(rec,columns=["signal_date","symbol","falcon_rank","rank_band","tier"])
FOCUS=["PREMIUM-Pullback","GOLD","PREMIUM-Compression"]
print(f"  Jan-2025: {len(jan_sigs)} signal days · {len(R)} total ranked signals (top-100/day)\n")
print(f"  === COUNT of signals in each tier, grouped by Falcon rank band (summed over {len(jan_sigs)} days) ===")
ct=pd.crosstab(R["rank_band"],R["tier"])
order=["01-15","16-30","31-50","51-100"]
print(f"  {'rank band':<12}"+"".join(f"{t.replace('PREMIUM-','P-'):>16}" for t in FOCUS)+f"{'3-tier total':>14}{'all signals':>13}")
for b in order:
    row=ct.loc[b] if b in ct.index else pd.Series(dtype=int)
    vals=[int(row.get(t,0)) for t in FOCUS]
    print(f"  {b:<12}"+"".join(f"{v:>16}" for v in vals)+f"{sum(vals):>14}{int(row.sum()):>13}")
tot=[int(ct.get(t,pd.Series()).sum()) for t in FOCUS]
print(f"  {'TOTAL':<12}"+"".join(f"{v:>16}" for v in tot)+f"{sum(tot):>14}{len(R):>13}")
print(f"\n  per-day average (3 focus tiers): {sum(tot)/len(jan_sigs):.1f} signals/day"
      f"  |  in top-15: {sum(int((R['rank_band']=='01-15').sum() and ct.loc['01-15'].get(t,0)) for t in FOCUS)/len(jan_sigs):.1f}/day")
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_TIER_BY_FALCONRANK.xlsx")
with pd.ExcelWriter(out) as xw:
    pd.crosstab(R["rank_band"],R["tier"]).reindex(order).to_excel(xw,sheet_name="rank_x_tier_count")
    R.to_excel(xw,sheet_name="all_signals",index=False)
print(f"\n  full detail -> {out}")
