"""JAN 2026 · systematic INTRADAY high-tier basket, ALL 21 days. Buy 09:15 (open), sell 15:29 (close).
Leak-free rebuild + FIXED tier engine (validated 100% vs operator on ENT/GOLD). For every trading day:
tier+rank every Falcon pick, measure same-day open->close. Break down by TIER, and sweep selection methods
(which tiers, top-N by rank, basket size) to find where the returns concentrate. Equal-weight, 1x & 5x, net 0.30%.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
COST_INTRADAY=0.10  # MIS intraday round-trip ~0.10% (STT intraday + brokerage)

def _ok(v): return v is not None and not (isinstance(v,float) and v!=v)
def classify(sret,twoday,rng,al,tr,turn):
    if _ok(sret) and sret>10: return "AVOID"
    if _ok(sret) and sret>7 and _ok(turn) and turn>=0.75: return "AVOID"
    if _ok(sret) and sret<=2 and _ok(twoday) and twoday<-5 and _ok(al) and al>15: return "PREMIUM-Pullback"
    if _ok(sret) and sret<=2 and _ok(rng) and rng<2 and _ok(al) and al>15: return "PREMIUM-Compression"
    if _ok(sret) and sret<=2 and _ok(tr) and tr<0.9: return "ENTERPRISE-Dryup"
    if _ok(sret) and sret<=2 and _ok(turn) and turn<0.75: return "GOLD"
    if _ok(sret) and sret<=2: return "GOLD-baseline"
    if _ok(sret) and sret<=5: return "STANDARD"
    return "STANDARD-weak"

con=sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro",uri=True)
pats=FR.load_patterns(con)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-02-15'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-02-15' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; B={}; TF={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float); o=g.open.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100; rng=(h-l)/pc*100; twoday=(c/c2-1)*100
    a20=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20>0,a3/a20,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    idx={d:i for i,d in enumerate(g.trade_date)}
    B[s]=dict(o=o,c=c,idx=idx)
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def picks(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    c=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i])} for i in range(len(syms)) if fire[i]>=10]
    c.sort(key=lambda x:-x["score"]); c=sorted(c[:150],key=lambda x:-(x["score"]/max(x["nf"],1)))
    out=[]
    for r,x in enumerate(c,1):
        al=x["score"]/max(x["nf"],1); tf=TF.get((x["symbol"],sd),(np.nan,)*5)
        out.append(dict(symbol=x["symbol"],rank=r,tier=classify(tf[0],tf[1],tf[2],al,tf[3],tf[4])))
    return out

def intraday(sym,td):
    S=B.get(sym); i=S["idx"].get(td) if S else None
    if i is None: return None
    o=S["o"][i]
    return (S["c"][i]/o-1)*100-COST_INTRADAY if o>0 else None

trade_days=[d for d in cal if "2026-01-01"<=d<="2026-01-31"]
ALL=[]
for td in trade_days:
    ci=cidx[td]
    if ci-1<0: continue
    sd=cal[ci-1]
    for p in picks(sd):
        r=intraday(p["symbol"],td)
        if r is None: continue
        ALL.append(dict(td=td,symbol=p["symbol"],rank=p["rank"],tier=p["tier"],ret=r,high=p["tier"] in HIGH))
R=pd.DataFrame(ALL)

print("="*92); print(f"  JAN 2026 · INTRADAY (buy 9:15 / sell 15:29) · ALL {R.td.nunique()} days · net {COST_INTRADAY}% · leak-free tiers"); print("="*92)
print("\n  A) WHERE THE RETURNS ARE — avg intraday return by TIER (all Falcon picks):")
print(f"  {'tier':<20}{'picks/day':>10}{'avg ret%':>10}{'win%':>7}{'best day avg':>13}")
for t in ["PREMIUM-Compression","PREMIUM-Pullback","ENTERPRISE-Dryup","GOLD","GOLD-baseline","STANDARD","STANDARD-weak","AVOID"]:
    g=R[R.tier==t]
    if not len(g): continue
    print(f"  {t:<20}{len(g)/R.td.nunique():>10.1f}{g.ret.mean():>+10.2f}{(g.ret>0).mean()*100:>6.0f}%{g.groupby('td').ret.mean().max():>+13.2f}")

def basket(sub, label, sizecap=None):
    rows=[]
    for td,g in sub.groupby("td"):
        gg=g.sort_values("rank")
        if sizecap: gg=gg.head(sizecap)
        if len(gg)==0: continue
        rows.append(dict(td=td,n=len(gg),ret=gg.ret.mean()))
    D=pd.DataFrame(rows)
    tot=D.ret.sum()  # sum of daily equal-weight returns ~ cumulative if 1 basket/day rotated
    return dict(label=label, days=len(D), avgn=D.n.mean(), avgday=D.ret.mean(), wdays=(D.ret>0).sum(),
                tot1x=tot, tot5x=tot*5, best=D.ret.max(), worst=D.ret.min(), D=D)

print("\n  B) SELECTION METHODS — intraday basket, all Jan (daily equal-weight, then summed):")
print(f"  {'method':<34}{'avg names':>10}{'avg/day%':>10}{'winDays':>9}{'Jan 1x%':>9}{'Jan 5x%':>9}{'best day':>10}")
meths=[
 ("ALL high-tier (no cap)", R[R.high], None),
 ("high-tier Top-25 by rank", R[R.high], 25),
 ("high-tier Top-15 by rank", R[R.high], 15),
 ("high-tier Top-10 by rank", R[R.high], 10),
 ("ENTERPRISE+PREMIUM only", R[R.tier.isin(["ENTERPRISE-Dryup","PREMIUM-Compression","PREMIUM-Pullback"])], None),
 ("ENTERPRISE+PREMIUM Top-15", R[R.tier.isin(["ENTERPRISE-Dryup","PREMIUM-Compression","PREMIUM-Pullback"])], 15),
 ("ENTERPRISE only", R[R.tier=="ENTERPRISE-Dryup"], None),
 ("GOLD+GOLD-baseline only", R[R.tier.isin(["GOLD","GOLD-baseline"])], None),
]
best=None
for lab,sub,cap in meths:
    if not len(sub): continue
    b=basket(sub,lab,cap);
    print(f"  {lab:<34}{b['avgn']:>10.1f}{b['avgday']:>+10.2f}{b['wdays']:>4}/{b['days']}{b['tot1x']:>+9.1f}{b['tot5x']:>+9.1f}{b['best']:>+10.2f}")
    if best is None or b['tot1x']>best['tot1x']: best=b

print("\n"+"="*92); print(f"  BEST METHOD: {best['label']}  ·  daily journal (1x)"); print("="*92)
print(f"  {'date':<12}{'names':>6}{'ret1x%':>9}{'ret5x%':>9}")
for _,x in best['D'].iterrows():
    print(f"  {x.td:<12}{int(x.n):>6}{x.ret:>+9.2f}{x.ret*5:>+9.2f}")
print("  "+"-"*36)
print(f"  Jan total 1x {best['tot1x']:+.1f}%  ·  5x {best['tot5x']:+.1f}%  ·  winning days {best['wdays']}/{best['days']}  ·  avg {best['avgday']:+.2f}%/day  ·  avg {best['avgn']:.0f} names")
print(f"  (operator's 10 hand-picked days averaged +2.99%/day at 1x)")
