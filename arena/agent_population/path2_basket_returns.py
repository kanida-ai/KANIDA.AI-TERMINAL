"""PATH 2 — does a Falcon top-N basket (which contains all your picks) reproduce your intraday returns?
Entry next-day 09:15 open, exit intraday (EOD / operator trail). Equal-weight basket. 1x and 5x MIS.
Compares: YOUR actual Jan-2025 picks vs Falcon top-15 / top-20 / top-30. Leak-free rebuild. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from weekly_dayofweek_fix import OPJAN
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")  # 1-min
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
parts=[]
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); c=g.close.values.astype(float); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    parts.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
BASE=feat.drop(columns=WEEKLY).merge(pd.concat(parts,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
def topN(sd,N):
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
    return [c["symbol"] for c in ranked[:N]]
# ---- 1-min intraday sim ----
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def bars(sym,date):
    df=pd.read_sql_query("SELECT open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                         mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    return df
def sim(sym,date,rule):
    df=bars(sym,date)
    if len(df)<5: return None
    entry=float(df.open.iloc[0])
    if entry<=0: return None
    if rule=="EOD": return (float(df.close.iloc[-1])-entry)/entry
    arm,gb,hard=rule  # price-fraction terms
    peak=entry
    for o,h,l,c in df[["open","high","low","close"]].itertuples(index=False):
        if (l-entry)/entry <= -hard: return -hard
        peak=max(peak,h); gain=(peak-entry)/entry
        if gain>=arm:
            trail=entry+(peak-entry)*(1-gb)
            if l<=trail: return (trail-entry)/entry
    return (float(df.close.iloc[-1])-entry)/entry
def basket_month(daymap, rule, N=None):
    """daymap: {trade_date: [symbols]}. Returns per-day mean price return + coverage."""
    rows=[]
    for td in sorted(daymap):
        syms=daymap[td][:N] if N else daymap[td]
        rs=[sim(s,td,rule) for s in syms]; rs=[r for r in rs if r is not None]
        if rs: rows.append((td,np.mean(rs),len(rs),len(syms)))
    return rows
def summarize(rows,lev):
    if not rows: return None
    r=np.array([x[1] for x in rows])
    dailycap=lev*r
    comp=np.prod(1+dailycap)-1
    return dict(days=len(rows),mean_day_pct=r.mean()*100,win=(r>0).mean()*100,
                monthly_comp_pct=comp*100,sum_pct=dailycap.sum()*100)
# trade days = operator's actual Jan-2025 trade days
trade_days=[td for td in sorted(OPJAN) if cidx.get(td) and cidx[td]-1>=0]
op_map={td:OPJAN[td] for td in trade_days}
fal={N:{td:topN(cal[cidx[td]-1],N) for td in trade_days} for N in (15,20,30)}
RULES=[("EOD","EOD"),("trail 6/60/3 (price)",(0.06,0.60,0.03)),("trail 3/50/2 (price)",(0.03,0.50,0.02))]
print(f"  Jan-2025 · {len(trade_days)} operator trade days · entry 09:15 open, intraday exit · equal-weight\n")
for rlab,rule in RULES:
    print(f"  === exit rule: {rlab} ===")
    print(f"  {'basket':<20}{'1x monthly':>12}{'5x monthly':>12}{'mean/day':>10}{'win%':>7}{'avg cov':>9}")
    defs=[("YOUR picks",op_map,None)]+[(f"Falcon top-{N}",fal[N],None) for N in (15,20,30)]
    for lab,dm,N in defs:
        rows=basket_month(dm,rule,N)
        s1=summarize(rows,1); s5=summarize(rows,5)
        cov=np.mean([x[2]/x[3] for x in rows])*100 if rows else 0
        print(f"  {lab:<20}{s1['monthly_comp_pct']:>11.1f}%{s5['monthly_comp_pct']:>11.1f}%{s1['mean_day_pct']:>9.2f}%{s1['win']:>6.0f}%{cov:>8.0f}%")
    print()
mcon.close()
