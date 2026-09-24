"""JAN 2026 · DYNAMIC stop/target scaled to each stock's ATR (volatility). Falcon Top-20, ₹25k/stock,
buy 9:15, max hold 20 sessions, cash, 0.30% cost. target = T*ATR%, stop = S*ATR% (ATR = atr_20_pct, PIT).
Sweep T/S combos, report trader journal (WR, PF, avg win/loss, target/stop hits, P&L, winning days).
Compares to the fixed +10%/-8%. Leak-free (week-to-date features). Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HZ=20; COST=0.30; ALLOC=25000.0; LTP_MAX=25000.0; TOPN=20

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-04-15' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; SYM={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s]=dict(o=g.open.values.astype(float),h=g.high.values.astype(float),l=g.low.values.astype(float),c=cl,idx={d:i for i,d in enumerate(g.trade_date)},n=len(g))
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def picks(day):
    fd=FCpit[FCpit.trade_date==day]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    atr=dict(zip(fd.symbol, pd.to_numeric(fd.get("atr_20_pct"),errors="coerce")))
    c=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    c.sort(key=lambda x:-x["score"]); c=sorted(c[:150],key=lambda x:-(x["score"]/max(x["nf"],1)))[:TOPN]
    return [(x["symbol"], atr.get(x["symbol"], np.nan)) for x in c]

def trade(sym, d1, atrp, tmult, smult, fixed=None):
    S=SYM.get(sym); i=S["idx"].get(d1) if S else None
    if i is None or i+HZ-1>=S["n"]: return None
    e=S["o"][i]
    if e<=0 or e>LTP_MAX: return None
    sh=int(ALLOC//e)
    if sh<1: return None
    if fixed: tp=e*(1+fixed[0]/100); sp=e*(1+fixed[1]/100)
    else:
        if not np.isfinite(atrp) or atrp<=0: return None
        tp=e*(1+tmult*atrp/100); sp=e*(1-smult*atrp/100)
    ex=None; rsn=None
    for k in range(HZ):
        j=i+k; o,h,l=S["o"][j],S["h"][j],S["l"][j]
        if k>0:
            if o>=tp: ex=o; rsn="target"; break
            if o<=sp: ex=o; rsn="stop"; break
        if l<=sp and h>=tp: ex=sp; rsn="stop"; break   # both same day -> conservative
        if h>=tp: ex=tp; rsn="target"; break
        if l<=sp: ex=sp; rsn="stop"; break
    if ex is None: ex=S["c"][i+HZ-1]; rsn="timeout"
    ret=(ex/e-1)*100-COST; pnl=sh*(ex-e)-COST/100*sh*e
    return dict(symbol=sym,basket_day=d1,ret=ret,pnl=pnl,reason=rsn)

jan=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) is not None and cidx[d]+1<len(cal)]
PICKS={sd:picks(sd) for sd in jan}

def run(tmult,smult,fixed=None):
    rows=[]
    for sd in jan:
        d1=cal[cidx[sd]+1]
        for sym,atrp in PICKS[sd]:
            t=trade(sym,d1,atrp,tmult,smult,fixed)
            if t: rows.append(t)
    T=pd.DataFrame(rows); won=T[T.pnl>0]; lost=T[T.pnl<=0]
    gp=won.pnl.sum(); gl=abs(lost.pnl.sum())
    byday=T.groupby("basket_day").pnl.sum()
    return dict(T=T,n=len(T),wr=len(won)/len(T)*100,pf=(gp/gl if gl else 9.9),pnl=T.pnl.sum(),
                aw=won.ret.mean(),al=lost.ret.mean(),tgt=(T.reason=="target").mean()*100,stp=(T.reason=="stop").mean()*100,
                wdays=(byday>0).sum(),ndays=len(byday))

print("\n  DYNAMIC stop/target sweep · Falcon Top-20 · Jan 2026 · target=T×ATR, stop=S×ATR (ATR=each stock's daily range%)\n")
print(f"  {'config':<22}{'trades':>7}{'WR':>6}{'PF':>6}{'avgWin%':>9}{'avgLoss%':>10}{'tgtHit%':>9}{'stopHit%':>10}{'P&L ₹':>11}{'winDays':>9}")
grid=[("+10% / -8% FIXED",None,None,(10,-8))]
for tm in [2.5,3.0,4.0]:
    for sm in [1.5,2.0,2.5]:
        grid.append((f"tgt {tm}xATR / stop {sm}xATR", tm, sm, None))
res={}
for lab,tm,sm,fx in grid:
    r=run(tm,sm,fx); res[lab]=r
    print(f"  {lab:<22}{r['n']:>7}{r['wr']:>5.0f}%{r['pf']:>6.2f}{r['aw']:>+9.2f}{r['al']:>+10.2f}{r['tgt']:>8.0f}%{r['stp']:>9.0f}%{r['pnl']:>+11,.0f}{r['wdays']:>4}/{r['ndays']}")

best=max(res, key=lambda k: res[k]['pf'])
r=res[best]; T=r['T']; won=T[T.pnl>0]; lost=T[T.pnl<=0]
print("\n"+"="*80); print(f"  BEST CONFIG: {best}   (profit factor {r['pf']:.2f})"); print("="*80)
print(f"  Trades {r['n']}   ·   Stocks WON {len(won)}  /  LOST {len(lost)}   ·   WIN RATE {r['wr']:.0f}%")
print(f"  Total P&L ₹{T.pnl.sum():+,.0f}   ·   avg/trade {T.ret.mean():+.2f}%")
print(f"  Avg WIN +{won.ret.mean():.2f}%   Avg LOSS {lost.ret.mean():.2f}%   ·   PROFIT FACTOR {r['pf']:.2f}")
print(f"  Hit target {r['tgt']:.0f}%  ·  hit stop {r['stp']:.0f}%  ·  timed out {(T.reason=='timeout').mean()*100:.0f}%")
print(f"  Winning days {r['wdays']} of {r['ndays']}")
print(f"  Best: {T.loc[T.pnl.idxmax(),'symbol']} ₹{T.pnl.max():+,.0f}   Worst: {T.loc[T.pnl.idxmin(),'symbol']} ₹{T.pnl.min():+,.0f}")
