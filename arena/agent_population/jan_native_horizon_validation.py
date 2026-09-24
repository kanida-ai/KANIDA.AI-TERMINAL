"""JAN 2026 · NATIVE-horizon rank validation — grade the ranking the way patterns were mined:
does each pick TOUCH +10% within 20 trading sessions? Leak-free. Entry = signal-next-day 09:15 open.
Compare Top-10/20/30/50/75/100 hit-rate vs the UNIVERSE base rate (all Nifty-500 stocks that day).
LIFT = bucket hit-rate / base hit-rate (>1 => ranking adds value). Also +15% hit, 20d MFE, 20d return,
and marginal rank bands to see the gradient. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HZ = 20  # trading sessions

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-04-15' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values; cl=g.close.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(cl-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(cl>0,(hi-lo)/cl*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(cl/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(cl>ph).astype(float),np.nan))))
    SYM[s]=dict(o=g.open.values.astype(float), h=g.high.values.astype(float), c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}

def rank100(day):
    fd=FCpit[FCpit.trade_date==day]
    if fd.empty: return {}
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
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:150],key=lambda c:-(c["score"]/max(c["nf"],1)))[:100]
    return {c["symbol"]: r for r,c in enumerate(ranked,1)}

def metrics(sym, d1):
    S=SYM.get(sym); i=S["idx"].get(d1) if S else None
    if i is None or i+HZ-1>=S["n"]: return None
    e=S["o"][i]
    if e<=0: return None
    hi=S["h"][i:i+HZ].max()
    return dict(hit10=int(hi>=e*1.10), hit15=int(hi>=e*1.15), mfe=(hi/e-1)*100, fwd=(S["c"][i+HZ-1]/e-1)*100)

jan=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) is not None and cidx[d]+1<len(cal)]
rows=[]
for sd in jan:
    d1=cal[cidx[sd]+1]; rk=rank100(sd)
    daysyms=FCpit[FCpit.trade_date==sd].symbol.unique()   # universe eligible that day
    for s in daysyms:
        m=metrics(s,d1)
        if m is None: continue
        m.update(day=sd, symbol=s, rank=rk.get(s, np.nan)); rows.append(m)
R=pd.DataFrame(rows)

base=R  # all eligible stocks = base rate
b10=base.hit10.mean()*100; b15=base.hit15.mean()*100
print("="*100)
print(f"  JAN 2026 · NATIVE 20-session / +10% rank validation · {len(jan)} signal days · {len(R):,} stock-instances")
print("="*100)
print(f"  BASE RATE (all {R.symbol.nunique()} eligible stocks): hit +10%/20d = {b10:.1f}%  ·  hit +15% = {b15:.1f}%  ·  avg 20d MFE {base.mfe.mean():+.2f}%  ·  avg 20d ret {base.fwd.mean():+.2f}%")
print("  " + "-"*98)
print(f"  {'bucket':<9}{'n':>7}{'hit+10%':>9}{'LIFT':>7}{'hit+15%':>9}{'20d MFE%':>10}{'20d ret%':>10}")
for N in [10,20,30,50,75,100]:
    g=R[R['rank']<=N]
    h=g.hit10.mean()*100
    print(f"  Top-{N:<5}{len(g):>7}{h:>8.1f}%{h/b10:>7.2f}{g.hit15.mean()*100:>8.1f}%{g.mfe.mean():>+10.2f}{g.fwd.mean():>+10.2f}")
print(f"  {'BASE':<9}{len(base):>7}{b10:>8.1f}%{1.00:>7.2f}{b15:>8.1f}%{base.mfe.mean():>+10.2f}{base.fwd.mean():>+10.2f}")
print("\n  marginal rank bands (gradient check):")
print(f"  {'band':<10}{'n':>7}{'hit+10%':>9}{'LIFT':>7}{'20d ret%':>10}")
for a,b in [(1,10),(11,20),(21,30),(31,50),(51,75),(76,100)]:
    g=R[(R['rank']>=a)&(R['rank']<=b)]
    h=g.hit10.mean()*100
    print(f"  {str(a)+'-'+str(b):<10}{len(g):>7}{h:>8.1f}%{h/b10:>7.2f}{g.fwd.mean():>+10.2f}")
print("\n  verdict:")
t10=R[R['rank']<=10].hit10.mean()*100; t100=R[R['rank']<=100].hit10.mean()*100
print(f"    Top-10 lift {t10/b10:.2f}x  ·  Top-100 lift {t100/b10:.2f}x  ·  gradient Top-10>Top-100: {'YES' if t10>t100 else 'NO'}  ·  ranking beats base: {'YES' if t10>b10 else 'NO'}")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_JAN2026_NATIVE_20D_VALIDATION.xlsx")
R.to_excel(out,index=False); print(f"\n  per-instance table -> {out}")
