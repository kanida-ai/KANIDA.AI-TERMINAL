"""JAN 2026 · plain trade journal. Config: buy 9:15, sell at +10% target OR after 20 trading days, cash,
integer shares ₹25k/stock, 0.30% round-trip, NO stop. Compare Falcon Top-20 vs the WHOLE Falcon list.
Leak-free (week-to-date features). Reports P&L, win rate, stocks won/lost, profit factor, winning days. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HZ=20; TGT=10.0; COST=0.30; ALLOC=25000.0; LTP_MAX=25000.0

con = sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-04-15' ORDER BY symbol,trade_date", con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; SYM={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s]=dict(o=g.open.values.astype(float),h=g.high.values.astype(float),c=cl,idx={d:i for i,d in enumerate(g.trade_date)},n=len(g))
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def score_day(day):
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
    c=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    c.sort(key=lambda x:-x["score"]); c=sorted(c[:150],key=lambda x:-(x["score"]/max(x["nf"],1)))
    return [x["symbol"] for x in c]

def trade(sym,d1):
    S=SYM.get(sym); i=S["idx"].get(d1) if S else None
    if i is None or i+HZ-1>=S["n"]: return None
    e=S["o"][i]
    if e<=0 or e>LTP_MAX: return None
    sh=int(ALLOC//e)
    if sh<1: return None
    tp=e*(1+TGT/100); ex=None; hit=0
    for k in range(HZ):
        if S["h"][i+k]>=tp: ex=tp; hit=1; break
    if ex is None: ex=S["c"][i+HZ-1]
    ret=(ex/e-1)*100-COST; pnl=sh*(ex-e)-COST/100*sh*e
    return dict(symbol=sym,entry_date=d1,shares=sh,entry=round(e,2),exit=round(ex,2),ret=round(ret,2),pnl=round(pnl),hit=hit)

jan=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) is not None and cidx[d]+1<len(cal)]

def run(topn, label):
    rows=[]
    for sd in jan:
        d1=cal[cidx[sd]+1]; names=score_day(sd)
        pick=names if topn is None else names[:topn]
        for s in pick:
            t=trade(s,d1)
            if t: t["basket_day"]=sd; rows.append(t)
    T=pd.DataFrame(rows)
    won=T[T.pnl>0]; lost=T[T.pnl<=0]
    gp=won.pnl.sum(); gl=abs(lost.pnl.sum()); pf=gp/gl if gl else float('inf')
    byday=T.groupby("basket_day").pnl.sum(); wdays=(byday>0).sum()
    print("="*88); print(f"  {label}"); print("="*88)
    print(f"  Trades: {len(T)}   Stocks WON: {len(won)}   Stocks LOST: {len(lost)}")
    print(f"  WIN RATE: {len(won)/len(T)*100:.0f}%   ·   hit +10% target: {T.hit.sum()} ({T.hit.mean()*100:.0f}% of trades)")
    print(f"  TOTAL P&L: ₹{T.pnl.sum():+,.0f}   (avg per trade ₹{T.pnl.mean():+,.0f} = {T.ret.mean():+.2f}% on ₹25k)")
    print(f"  Avg WIN:  +₹{won.pnl.mean():,.0f}  (+{won.ret.mean():.2f}%)     Avg LOSS: -₹{abs(lost.pnl.mean()):,.0f}  ({lost.ret.mean():.2f}%)")
    print(f"  PROFIT FACTOR: {pf:.2f}   (₹{gp:,.0f} won / ₹{gl:,.0f} lost)")
    print(f"  Winning days: {wdays} of {len(byday)}   Losing days: {len(byday)-wdays}")
    print(f"  Best stock: {T.loc[T.pnl.idxmax(),'symbol']} ₹{T.pnl.max():+,.0f}   Worst: {T.loc[T.pnl.idxmin(),'symbol']} ₹{T.pnl.min():+,.0f}\n")
    return T

print("\nCONFIG: buy 9:15 · sell at +10% target OR after 20 trading days · ₹25,000/stock · cash · NO stop · 0.30% cost · Jan 2026\n")
T20=run(20, "FALCON TOP-20 (the ranked picks)")
TALL=run(None, "WHOLE FALCON LIST (~all eligible each day) — to show ranking barely matters")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN_TRADE_JOURNAL.xlsx")
with pd.ExcelWriter(out,engine="openpyxl") as w: T20.to_excel(w,"Top20",index=False); TALL.to_excel(w,"WholeList",index=False)
print(f"trade logs -> {out}")
