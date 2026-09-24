"""Reconstruct the systematic high-tier INTRADAY basket for Mar-May 2025 (operator's 3-month log) and compare.
Leak-free rebuild + real tier engine. Buy 09:15 (open), EOD exit 15:29 (close), net 0.10%. Equal-weight.
Variants: all high-tier / Top-30/20/15/10 by rank. Monthly SUM of daily 1x returns + winning days.
Operator actual (1x): Mar +42.3% (14/18), Apr +34.2% (11/19), May +44.2% (14/21). Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
COST=0.10
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
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
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
    B[s]=dict(o=o,c=c,idx={d:i for i,d in enumerate(g.trade_date)})
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
        t=classify(tf[0],tf[1],tf[2],al,tf[3],tf[4])
        if t in HIGH: out.append(x["symbol"])
    return out

def intraday(sym,td):
    S=B.get(sym); i=S["idx"].get(td) if S else None
    if i is None: return None
    o=S["o"][i]; return (S["c"][i]/o-1)*100-COST if o>0 else None

trade_days=[d for d in cal if "2025-03-01"<=d<="2025-05-31"]
recs=[]
for td in trade_days:
    ci=cidx[td]
    if ci-1<0: continue
    sd=cal[ci-1]; hp=picks(sd)
    if not hp: continue
    rmap={s:r for r,s in enumerate(hp,1)}   # already rank-ordered
    for s in hp:
        r=intraday(s,td)
        if r is not None: recs.append(dict(td=td,mo=td[:7],symbol=s,rank=rmap[s],ret=r))
R=pd.DataFrame(recs)

OP={"2025-03":(42.3,"14/18"),"2025-04":(34.2,"11/19"),"2025-05":(44.2,"14/21")}
def basket(cap):
    rows=[]
    for td,g in R.groupby("td"):
        gg=g.sort_values("rank")
        if cap: gg=gg.head(cap)
        rows.append(dict(td=td,mo=td[:7],n=len(gg),ret=gg.ret.mean()))
    return pd.DataFrame(rows)

print("="*88); print("  SYSTEMATIC high-tier INTRADAY basket · Mar-May 2025 · EOD (9:15->15:29) · 1x · net 0.1%"); print("="*88)
print(f"  {'method':<22}{'month':<9}{'avgNames':>9}{'sumRet1x%':>11}{'winDays':>9}   operator")
for cap,lab in [(None,"ALL high-tier"),(30,"Top-30"),(20,"Top-20"),(15,"Top-15"),(10,"Top-10")]:
    D=basket(cap)
    for mo in ["2025-03","2025-04","2025-05"]:
        g=D[D.mo==mo]; op=OP[mo]
        print(f"  {lab:<22}{mo:<9}{g.n.mean():>9.0f}{g.ret.sum():>+11.1f}{(g.ret>0).sum():>4}/{len(g)}   op +{op[0]}% ({op[1]})")
    print()
# best variant overall
best=None
for cap,lab in [(None,"ALL"),(30,"Top-30"),(20,"Top-20"),(15,"Top-15"),(10,"Top-10")]:
    D=basket(cap); tot=D.ret.sum()
    if best is None or tot>best[1]: best=(lab,tot,D)
print(f"  best systematic (EOD, no stop/trail): {best[0]}  ·  3-mo sum {best[1]:+.1f}%/1x  ·  operator 3-mo sum +120.7%/1x")
print(f"  NOTE: systematic here has NO hard-stop / NO trail yet — operator's capped losing days are not modeled.")
