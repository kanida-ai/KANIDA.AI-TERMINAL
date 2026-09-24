"""JAN 2026 · daily overlapping 2-day scalp basket · Top-15 · +7% target / -5% stop / day-2 close.
Leak-free rebuild. Entry = signal-next-day 09:15 (1-min open). Cash only, integer shares, LTP<=33,333.
Exit logic walked on the REAL 1-min path (D1 09:15 -> D2 15:29), gap-aware, target-vs-stop by time order.
Reports: trade journal (win%, exit mix, expectancy, PF) + portfolio (return on 2-sleeve committed, idle, DD).
Read-only. Jan fully inside 1-min window (ends 2026-07-10)."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
TOPN=15; ALLOC=33333.0; TGT=7.0; STOP=-5.0; COST=0.30; LTP_MAX=33333.0

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-28' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s]=dict(c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, dates=list(g.trade_date), n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}

def top15(day):
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
    cands=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:120],key=lambda c:-(c["score"]/max(c["nf"],1)))[:TOPN]
    return [c["symbol"] for c in ranked]

oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
sig=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) is not None and cidx[d]+2<len(cal)]
trades=[]
for sd in sig:
    ci=cidx[sd]; d1=cal[ci+1]; d2=cal[ci+2]
    for s in top15(sd):
        S=SYM.get(s); i=S["idx"].get(d1) if S else None
        if i is None: continue
        b=oc.execute("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",(s,d1+" 09:15",d2+" 15:59")).fetchall()
        if len(b)<5: continue
        e=b[0][1]
        if e is None or e<=0 or e>LTP_MAX: continue
        sh=int(ALLOC//e)
        if sh<1: continue
        tp=e*(1+TGT/100); spx=e*(1+STOP/100); ex=None; rsn=None
        for t,o,h,l,c in b:
            if o>=tp: ex=o; rsn="target"; break
            if o<=spx: ex=o; rsn="stop"; break
            ht=h>=tp; hs=l<=spx
            if ht and hs: ex=spx; rsn="stop"; break     # same-bar both -> conservative stop
            if ht: ex=tp; rsn="target"; break
            if hs: ex=spx; rsn="stop"; break
        if ex is None: ex=b[-1][4]; rsn="timeout"
        ret=(ex/e-1)*100-COST; pnl=sh*(ex-e)-COST/100*sh*e
        trades.append(dict(signal_date=sd, entry_date=d1, exit_date=d2, symbol=s, shares=sh, cap=sh*e,
                           entry=round(e,2), exit=round(ex,2), reason=rsn, ret_pct=round(ret,2), pnl=round(pnl)))
oc.close()
T=pd.DataFrame(trades)

print("="*92)
print("  JAN 2026 · Top-15 daily 2-day scalp · +7% target / -5% stop / day-2 close · ₹33,333/slot · cash")
print("="*92)
nb=T.signal_date.nunique()
print(f"  baskets(days) {nb}  ·  trades {len(T)}  ·  avg {len(T)/nb:.1f} fills/day  ·  avg capital/trade ₹{T.cap.mean():,.0f} (idle {(1-T.cap.mean()/ALLOC)*100:.1f}%)")
def stat(g):
    w=g[g.ret_pct>0]; l=g[g.ret_pct<=0]; return len(g),(len(w)/len(g)*100 if len(g) else 0),(w.ret_pct.mean() if len(w) else 0),(l.ret_pct.mean() if len(l) else 0)
print("\n  TRADE JOURNAL")
print(f"  {'exit':<10}{'n':>5}{'%ofall':>8}{'win%':>7}{'avgWin%':>9}{'avgLoss%':>10}{'P&L ₹':>12}")
for r in ["target","stop","timeout"]:
    g=T[T.reason==r]
    if not len(g): continue
    nn,w,aw,al=stat(g); print(f"  {r:<10}{nn:>5}{len(g)/len(T)*100:>7.0f}%{w:>6.0f}%{aw:>+9.2f}{al:>+10.2f}{g.pnl.sum():>+12,.0f}")
n,wr,aw,al=stat(T); exp=(wr/100*aw)+((1-wr/100)*al); pf=T[T.pnl>0].pnl.sum()/max(abs(T[T.pnl<=0].pnl.sum()),1)
print(f"  {'ALL':<10}{n:>5}{100:>7.0f}%{wr:>6.0f}%{aw:>+9.2f}{al:>+10.2f}{T.pnl.sum():>+12,.0f}")
print(f"  expectancy/trade {exp:+.2f}%  ·  profit factor {pf:.2f}")

# ---- portfolio equity on 2-sleeve committed capital ----
COMMIT=1000000.0
byday={}
for _,x in T.iterrows(): byday.setdefault(x.entry_date,[]).append(x)
run_dates=[d for d in cal if sig[0] < d <= cal[cidx[sig[-1]]+2]]
cash=COMMIT; realized=0.0; eq=[]; openp=[]
for D in run_dates:
    # entries
    for x in byday.get(D,[]): cash-=x.cap; openp.append(x)
    # exits at their exit_date close-of-cycle
    still=[]
    for x in openp:
        if x.exit_date==D or x.exit_date<D:
            cash+=x.shares*x.exit + x.pnl - x.shares*(x.exit-x.entry)  # = cash + sh*exit - cost
            realized+=x.pnl
        else: still.append(x)
    openp=still
    mtm=sum(p.shares*SYM[p.symbol]["c"][SYM[p.symbol]["idx"][D]] for p in openp if D in SYM[p.symbol]["idx"])
    eq.append(dict(date=D, equity=cash+mtm, deployed=COMMIT-cash))
E=pd.DataFrame(eq); E["peak"]=E.equity.cummax(); E["dd"]=(E.equity/E.peak-1)*100
print("\n  PORTFOLIO (2-sleeve ₹10L committed)")
print(f"  Jan realized P&L ₹{T.pnl.sum():+,.0f}  ·  return on ₹10L {T.pnl.sum()/COMMIT*100:+.2f}%  ·  per-basket avg {T.groupby('signal_date').pnl.sum().mean()/500000*100:+.2f}% on ₹5L")
print(f"  peak deployed ₹{E.deployed.max():,.0f}  ·  avg deployed ₹{E.deployed.mean():,.0f}  ·  avg idle {(1-E.deployed.mean()/COMMIT)*100:.0f}%  ·  max DD {E.dd.min():+.2f}%")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN2026_SCALP_TARGET.xlsx")
T.to_excel(out,index=False); print(f"\n  trade log -> {out}")
