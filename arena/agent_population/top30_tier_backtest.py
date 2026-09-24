"""BACKTEST: trade Falcon TOP-30 (leak-free) filtered to the 3 winning tiers (PREMIUM-Pullback / GOLD /
PREMIUM-Compression). Entry next-day 09:15 open, exit 15:29 close (EOD intraday). 5x MIS, Rs5,00,000 basket
capital equal-weighted. Emits Excel: 'trades' (per-trade entry/exit/pnl), 'daily' (per-day P&L + cumulative),
'summary'. Jan-2025. Leak-free, read-only, production untouched."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from tier_no_falcon import classify_tier_no_falcon
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
WIN_TIERS={"PREMIUM-Pullback","GOLD","PREMIUM-Compression"}
CAPITAL=500000.0; LEV=5.0; TOPN=30
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
# tier features
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
def top_ranked(sd,N):
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
    return [(r,c["symbol"]) for r,c in enumerate(ranked[:N],1)]
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def entryexit(sym,date):
    df=pd.read_sql_query("SELECT bar_time,open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<5: return None
    e=float(df.open.iloc[0]); x=float(df.close.iloc[-1])
    if e<=0: return None
    return (df.bar_time.iloc[0][11:16],round(e,2),df.bar_time.iloc[-1][11:16],round(x,2),(x-e)/e*100)
trade_days=[d for d in cal if "2025-01-01"<=d<="2025-01-31" and cidx[d]-1>=0]
trades=[]
for td in trade_days:
    sd=cal[cidx[td]-1]
    for rank,sym in top_ranked(sd,TOPN):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        if tier not in WIN_TIERS: continue
        ee=entryexit(sym,td)
        if ee is None: continue
        et,ep,xt,xp,ret=ee
        trades.append(dict(trade_date=td,signal_date=sd,symbol=sym,falcon_rank=rank,tier=tier,
            entry_time=et,entry_px=ep,exit_time=xt,exit_px=xp,stock_ret_pct=round(ret,2),ret_5x_pct=round(ret*LEV,2)))
mcon.close()
T=pd.DataFrame(trades)
# per-trade rupee pnl: equal split of (CAPITAL*LEV) across that day's N trades
n_by_day=T.groupby("trade_date").size().to_dict()
T["day_n"]=T.trade_date.map(n_by_day)
T["pnl_5x_rs"]=(CAPITAL*LEV/T["day_n"])*(T["stock_ret_pct"]/100)
T["pnl_5x_rs"]=T["pnl_5x_rs"].round(0)
# daily
D=T.groupby("trade_date").agg(n_trades=("symbol","size"),basket_ret_1x_pct=("stock_ret_pct","mean"),
    win_stocks=("stock_ret_pct",lambda s:(s>0).sum())).reset_index()
D["basket_ret_5x_pct"]=(D["basket_ret_1x_pct"]*LEV).round(2); D["basket_ret_1x_pct"]=D["basket_ret_1x_pct"].round(2)
D["pnl_5x_rs"]=(CAPITAL*D["basket_ret_5x_pct"]/100).round(0)
D["cum_pnl_5x_rs"]=D["pnl_5x_rs"].cumsum()
mret=D["basket_ret_1x_pct"].values/100
summary=pd.DataFrame([dict(trade_days=len(D),total_trades=len(T),avg_trades_per_day=round(len(T)/len(D),1),
    win_days=int((D["basket_ret_1x_pct"]>0).sum()),win_day_pct=round((D["basket_ret_1x_pct"]>0).mean()*100,1),
    stock_win_pct=round((T["stock_ret_pct"]>0).mean()*100,1),
    month_ret_1x_pct=round((np.prod(1+mret)-1)*100,1),month_ret_5x_pct=round((np.prod(1+LEV*mret)-1)*100,1),
    total_pnl_5x_rs=int(D["pnl_5x_rs"].sum()),capital=CAPITAL,leverage=LEV,exit_rule="EOD 15:29 close")])
out=os.path.join(os.path.expanduser("~"),"Downloads","JAN2025_TOP30_TIER_TRADELOG.xlsx")
with pd.ExcelWriter(out) as xw:
    T.sort_values(["trade_date","falcon_rank"]).to_excel(xw,sheet_name="trades",index=False)
    D.to_excel(xw,sheet_name="daily",index=False)
    summary.to_excel(xw,sheet_name="summary",index=False)
print(f"  Top-30 ∩ (PREMIUM-Pullback/GOLD/PREMIUM-Compression) · entry 09:15 open · exit 15:29 close · 5x · Rs5L\n")
print(f"  {'date':<12}{'#tr':>4}{'basket 1x%':>11}{'basket 5x%':>11}{'pnl 5x Rs':>12}{'cum 5x Rs':>13}")
for _,r in D.iterrows():
    print(f"  {r['trade_date']:<12}{int(r['n_trades']):>4}{r['basket_ret_1x_pct']:>11}{r['basket_ret_5x_pct']:>11}{int(r['pnl_5x_rs']):>12,}{int(r['cum_pnl_5x_rs']):>13,}")
s=summary.iloc[0]
print(f"\n  {s['trade_days']} days · {s['total_trades']} trades ({s['avg_trades_per_day']}/day) · win-days {s['win_day_pct']}% · stock win {s['stock_win_pct']}%")
print(f"  MONTH: {s['month_ret_1x_pct']}% (1x)  {s['month_ret_5x_pct']}% (5x)   total P&L 5x = Rs{int(s['total_pnl_5x_rs']):,}")
print(f"\n  full trade log -> {out}")
