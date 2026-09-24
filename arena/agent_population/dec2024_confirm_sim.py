"""DEC-2024 · Falcon TOP-50 EOD (leak-free) + price-only every-minute BUY-confirmation entry.
Rule (LONG only, since Falcon top-50 is a long ranked list):
  O = 09:15 open. From the 09:17 candle onward, when a completed candle's CLOSE is:
    (1) > O, (2) > max(high of prev 2 candles), (3) > prev candle close  -> BUY confirmed.
  Enter at the OPEN of the next minute (earliest 09:18). Take the FIRST confirmation.
  REJECT if the confirming candle's own move (close-open)/open > 0.5% (move likely exhausted).
  Exit EOD 15:29 close. No volume/indicators/VWAP. Skip stocks that never confirm.
5x MIS, Rs5L basket, equal-weight across entered names. Read-only, leak-free, production untouched."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); MDB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
CAPITAL=500000.0; LEV=5.0; TOPN=50; MAXCANDLE=0.5  # reject confirming candle move > 0.5%
con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2024-12-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2024-12-31' ORDER BY symbol,trade_date",con); con.close()
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
def top(sd,N):
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
def confirm_trade(sym,date):
    df=pd.read_sql_query("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",mcon,params=(sym,f"{date} 09:15:00",f"{date} 15:29:00"))
    if len(df)<20: return None
    bt=df.bar_time.values; o=df.open.values.astype(float); h=df.high.values.astype(float); l=df.low.values.astype(float); c=df.close.values.astype(float)
    O=o[0]
    if O<=0: return None
    for i in range(2,len(df)-1):                      # first decision after 09:17 close (i=2); need next candle i+1
        prev2_high=max(h[i-1],h[i-2]); cmove=(c[i]-o[i])/o[i]*100
        if c[i]>O and c[i]>prev2_high and c[i]>c[i-1]:
            if abs(cmove)>MAXCANDLE: continue          # reject explosive confirming candle, keep scanning
            ep=o[i+1]                                   # enter at open of next minute
            if ep<=0: continue
            xp=c[-1]                                     # exit EOD 15:29 close
            return dict(entry_time=bt[i+1][11:16],entry_px=round(ep,2),confirm_time=bt[i][11:16],
                        exit_time=bt[-1][11:16],exit_px=round(xp,2),ret_pct=round((xp-ep)/ep*100,2),
                        confirm_move_pct=round(cmove,2))
    return None
trade_days=[d for d in cal if "2024-12-01"<=d<="2024-12-31" and cidx[d]-1>=0]
trades=[]; skipped=0; total=0
for td in trade_days:
    sd=cal[cidx[td]-1]
    for rank,sym in top(sd,TOPN):
        total+=1; t=confirm_trade(sym,td)
        if t is None: skipped+=1; continue
        t.update(trade_date=td,signal_date=sd,symbol=sym,falcon_rank=rank,direction="LONG"); trades.append(t)
mcon.close()
T=pd.DataFrame(trades)
T["ret_5x_pct"]=(T.ret_pct*LEV).round(2); T["outcome"]=np.where(T.ret_pct>0,"WIN","LOSS")
nby=T.groupby("trade_date").size().to_dict(); T["pnl_5x_rs"]=((CAPITAL*LEV/T.trade_date.map(nby))*(T.ret_pct/100)).round(0)
D=T.groupby("trade_date").agg(n_entered=("symbol","size"),wins=("outcome",lambda s:(s=="WIN").sum()),
    basket_ret_1x=("ret_pct","mean")).reset_index()
D["basket_ret_5x"]=(D.basket_ret_1x*LEV).round(2); D["basket_ret_1x"]=D.basket_ret_1x.round(2)
D["pnl_5x_rs"]=(CAPITAL*D.basket_ret_5x/100).round(0); D["cum_pnl_5x_rs"]=D.pnl_5x_rs.cumsum()
m=D.basket_ret_1x.values/100; mo1=(np.prod(1+m)-1)*100; mo5=(np.prod(1+LEV*m)-1)*100
print(f"  DEC-2024 · Falcon TOP-50 + price-only minute BUY-confirmation (long) · exit EOD · 5x Rs5L\n")
print(f"  {'date':<12}{'entered':>8}{'wins':>6}{'basket1x%':>11}{'basket5x%':>11}{'pnl 5x Rs':>12}{'cum 5x Rs':>13}")
for _,r in D.iterrows():
    print(f"  {r['trade_date']:<12}{int(r['n_entered']):>8}{int(r['wins']):>6}{r['basket_ret_1x']:>11}{r['basket_ret_5x']:>11}{int(r['pnl_5x_rs']):>12,}{int(r['cum_pnl_5x_rs']):>13,}")
print(f"\n  candidates {total} (top-50 x {len(trade_days)} days) · confirmed/entered {len(T)} ({round(len(T)/total*100)}%) · never-confirmed skipped {skipped}")
print(f"  entered stock-win {round((T.ret_pct>0).mean()*100)}% · win-days {round((D.basket_ret_1x>0).mean()*100)}% · avg entry {T.entry_time.mode().iloc[0]}")
print(f"  MONTH: {mo1:+.1f}% (1x)   {mo5:+.1f}% (5x)   total P&L 5x = Rs{int(D.pnl_5x_rs.sum()):,}")
out=os.path.join(os.path.expanduser("~"),"Downloads","DEC2024_TOP50_MINUTE_CONFIRM.xlsx")
with pd.ExcelWriter(out) as xw:
    T[["trade_date","signal_date","symbol","falcon_rank","direction","confirm_time","confirm_move_pct","entry_time","entry_px","exit_time","exit_px","ret_pct","ret_5x_pct","pnl_5x_rs","outcome"]].sort_values(["trade_date","entry_time"]).to_excel(xw,sheet_name="trade_log",index=False)
    D.to_excel(xw,sheet_name="daily_pnl",index=False)
print(f"\n  trade log -> {out}")
