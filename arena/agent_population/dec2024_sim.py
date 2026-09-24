"""DEC-2024 SIMULATION. Strategy: previous-day leak-free Falcon EOD signal -> keep only the 3 high tiers
(PREMIUM-Pullback/GOLD/PREMIUM-Compression) -> observe first 15 min on trade day -> TRADE only the GREEN ones
(09:29 close > 09:15 open): enter at 09:29 close, exit 15:29 close (EOD). Skip reds. 5x MIS, Rs5L basket, equal wt.
Also reports the all-candidates 09:15->EOD baseline for contrast. Leak-free, read-only, production untouched."""
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
CAPITAL=500000.0; LEV=5.0; TOPN=100
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2024-12-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2024-12-31' ORDER BY symbol,trade_date",con); con.close()
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}
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
def ranked(sd,N):
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
    cands.sort(key=lambda c:-c["score"]); rk=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["nf"],1)))
    return [(r,c["symbol"]) for r,c in enumerate(rk[:N],1)]
mcon=sqlite3.connect("file:"+MDB.replace("\\","/")+"?mode=ro",uri=True)
def daybars(sym,date):
    return pd.read_sql_query("SELECT open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
trade_days=[d for d in cal if "2024-12-01"<=d<="2024-12-31" and cidx[d]-1>=0]
trades=[]
for td in trade_days:
    sd=cal[cidx[td]-1]
    for rank,sym in ranked(sd,TOPN):
        tf=TF.loc[(sym,sd)] if (sym,sd) in TF.index else None
        tier=classify_tier_no_falcon(tf["sret"],tf["twoday"],tf["rng"],tf["trend3_20"],tf["turn_pct"]) if tf is not None else None
        if tier not in WIN_TIERS: continue
        df=daybars(sym,td)
        if len(df)<16: continue
        o915=float(df.open.iloc[0]); c0929=float(df.close.iloc[14]); eod=float(df.close.iloc[-1])
        if o915<=0: continue
        first15=(c0929-o915)/o915*100; green=c0929>o915
        ret_green=(eod-c0929)/c0929*100 if green else None       # enter 09:29 close, exit EOD (only greens)
        ret_allin=(eod-o915)/o915*100                             # baseline: all candidates 09:15->EOD
        trades.append(dict(trade_date=td,signal_date=sd,symbol=sym,falcon_rank=rank,tier=tier,
            open_0915=round(o915,2),close_0929=round(c0929,2),first15_pct=round(first15,2),
            green=green,eod_close=round(eod,2),ret_green_pct=round(ret_green,2) if green else None,
            ret_allin_pct=round(ret_allin,2)))
mcon.close()
T=pd.DataFrame(trades)
G=T[T.green].copy()
# GREEN strategy daily
def daily_pnl(df,retcol):
    d=df.groupby("trade_date")[retcol].mean().reset_index().rename(columns={retcol:"ret_1x"})
    d["n"]=df.groupby("trade_date")[retcol].size().values
    d["ret_5x"]=(d.ret_1x*LEV).round(2); d["ret_1x"]=d.ret_1x.round(2)
    d["pnl_5x"]=(CAPITAL*d.ret_5x/100).round(0); d["cum_pnl_5x"]=d.pnl_5x.cumsum()
    return d
DG=daily_pnl(G,"ret_green_pct")
def month(d): m=d.ret_1x.values/100; return (np.prod(1+m)-1)*100,(np.prod(1+LEV*m)-1)*100,(d.ret_1x>0).mean()*100
g1,g5,gw=month(DG)
print(f"  DEC-2024 SIM · prev-day Falcon signal -> 3 high tiers -> first-15 GREEN -> enter 09:29, exit EOD · 5x Rs5L\n")
print(f"  {'date':<12}{'cand':>5}{'green':>6}{'basket1x%':>11}{'basket5x%':>11}{'pnl 5x Rs':>12}{'cum 5x Rs':>13}")
ncand=T.groupby("trade_date").size().to_dict()
for _,r in DG.iterrows():
    print(f"  {r['trade_date']:<12}{ncand.get(r['trade_date'],0):>5}{int(r['n']):>6}{r['ret_1x']:>11}{r['ret_5x']:>11}{int(r['pnl_5x']):>12,}{int(r['cum_pnl_5x']):>13,}")
# all-in baseline + V2 (enter 09:15, ride greens to EOD, cut reds at 09:29)
DA=daily_pnl(T,"ret_allin_pct"); a1,a5,aw=month(DA)
T["ret_v2"]=np.where(T.green,T.ret_allin_pct,T.first15_pct)
DV=daily_pnl(T,"ret_v2"); v1,v5,vw=month(DV)
print(f"\n  V1 GREEN-only (enter 09:29->EOD) : {len(G)} trades ({round(len(G)/len(DG),1)}/day) · win-day {gw:.0f}% · stock-win {(G.ret_green_pct>0).mean()*100:.0f}%")
print(f"                                     MONTH {g1:+.1f}% (1x)   {g5:+.1f}% (5x)   P&L 5x Rs{int(DG.pnl_5x.sum()):,}")
print(f"  V2 enter 09:15, ride greens/cut reds@09:29: MONTH {v1:+.1f}% (1x)  {v5:+.1f}% (5x)   P&L 5x Rs{int(DV.pnl_5x.sum()):,}")
print(f"  BASE all candidates 09:15->EOD (no filter): MONTH {a1:+.1f}% (1x)  {a5:+.1f}% (5x)")
# ---- clean EVERYDAY trade log (BASE strategy = hold all good-tier candidates 09:15 -> 15:29 EOD) ----
L=T.copy()
L["nday"]=L.trade_date.map(L.groupby("trade_date").size().to_dict())
L["entry_time"]="09:15"; L["entry_px"]=L["open_0915"]; L["exit_time"]="15:29"; L["exit_px"]=L["eod_close"]
L["stock_ret_pct"]=L["ret_allin_pct"]; L["ret_5x_pct"]=(L["ret_allin_pct"]*LEV).round(2)
L["pnl_5x_rs"]=((CAPITAL*LEV/L["nday"])*(L["ret_allin_pct"]/100)).round(0)
L["first15_dir"]=np.where(L.green,"GREEN","RED"); L["outcome"]=np.where(L.ret_allin_pct>0,"WIN","LOSS")
LOG=L[["trade_date","signal_date","symbol","tier","falcon_rank","entry_time","entry_px","first15_pct","first15_dir",
       "exit_time","exit_px","stock_ret_pct","ret_5x_pct","pnl_5x_rs","outcome"]].sort_values(["trade_date","falcon_rank"])
DAILY=L.groupby("trade_date").agg(n_trades=("symbol","size"),win_stocks=("outcome",lambda s:(s=="WIN").sum()),
    basket_ret_1x_pct=("stock_ret_pct","mean")).reset_index()
DAILY["basket_ret_5x_pct"]=(DAILY.basket_ret_1x_pct*LEV).round(2); DAILY["basket_ret_1x_pct"]=DAILY.basket_ret_1x_pct.round(2)
DAILY["day_pnl_5x_rs"]=(CAPITAL*DAILY.basket_ret_5x_pct/100).round(0); DAILY["cum_pnl_5x_rs"]=DAILY.day_pnl_5x_rs.cumsum()
out=os.path.join(os.path.expanduser("~"),"Downloads","DEC2024_DAILY_TRADELOG.xlsx")
with pd.ExcelWriter(out) as xw:
    LOG.to_excel(xw,sheet_name="trade_log_everyday",index=False)
    DAILY.to_excel(xw,sheet_name="daily_pnl",index=False)
    G.sort_values(["trade_date","falcon_rank"]).to_excel(xw,sheet_name="green_only_variant",index=False)
print(f"\n  everyday trade log -> {out}")
