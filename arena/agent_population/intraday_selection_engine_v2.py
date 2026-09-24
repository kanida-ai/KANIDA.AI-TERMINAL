"""SELECTION ENGINE v2 — push toward operator returns. Separate engine. Leak-free tiers.
Pass 1: refine SELECTION (vol/lowpx/pullback blends, tier-restriction, size) with exit=arm6/gb60/hard-3.
Pass 2: tune EXIT (arm/gb/hard) on the best selection. Mar-May 2025, 5x. Operator = +603.4%.
Reuses 1-min pool cache. Read-only."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
ENTPREM={"ENTERPRISE-Dryup","PREMIUM-Pullback","PREMIUM-Compression"}
COST5X=0.5
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
oh=pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}; PX={}
for s,g in o2.groupby("symbol",sort=False):
    g=g.sort_values("trade_date").reset_index(drop=True); hi=g.groupby("wk").high.cummax().values; lo=g.groupby("wk").low.cummin().values
    c=g.close.values.astype(float); h=g.high.values.astype(float); l=g.low.values.astype(float); v=g.volume.values.astype(float)
    wb=g.groupby("wk").agg(wc=("close","last"),wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm=g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph=g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s,trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan),weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan),weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100; rng=(h-l)/pc*100; twoday=(c/c2-1)*100
    a20=pd.Series(v).rolling(20).mean().values; a3=pd.Series(v).rolling(3).mean().values; tr=np.where(a20>0,a3/a20,np.nan)
    turn=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i]); PX[(s,d)]=c[i]
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}

def pool(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return pd.DataFrame()
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms))
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
    rows=[]
    for i in range(len(syms)):
        if fire[i]<10: continue
        al=score[i]/max(fire[i],1); tf=TF.get((syms[i],sd),(np.nan,)*5)
        t=classify(tf[0],tf[1],tf[2],al,tf[3],tf[4])
        if t not in HIGH: continue
        r=fd.iloc[i]
        rows.append(dict(symbol=syms[i], tier=t, price=PX.get((syms[i],sd),np.nan),
            atr=float(r.get("atr_20_pct",np.nan)), disthi60=float(r.get("dist_high_60",np.nan))))
    return pd.DataFrame(rows)

oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
trade_days=[d for d in cal if "2025-03-01"<=d<="2025-05-31"]
POOLS={}; DAYCACHE={}
for td in trade_days:
    ci=cidx[td]
    if ci-1<0: continue
    P=pool(cal[ci-1])
    if P.empty: continue
    POOLS[td]=P; syms=list(P.symbol)
    q="SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(syms))
    permin={}; cur=None; ent=None
    for sym,t,o,c in oc.execute(q,(td+" 09:15",td+" 15:35",*syms)).fetchall():
        if sym!=cur: cur=sym; ent=o; permin[sym]={}
        if ent and ent>0 and c and c>0: permin[sym][t]=(c/ent-1)
    DAYCACHE[td]=permin
oc.close()

def path5x(td,names):
    permin=DAYCACHE.get(td,{}); have=[s for s in names if s in permin and permin[s]]
    if len(have)<3: return None
    bsum=defaultdict(float); bcnt=defaultdict(int)
    for s in have:
        for t,r in permin[s].items(): bsum[t]+=r; bcnt[t]+=1
    times=sorted(bsum); p=np.array([5*100*bsum[t]/bcnt[t] for t in times if bcnt[t]>=max(3,len(have)//2)])
    return p if len(p) else None

def apply_exit(p,arm,gb,hard):
    armed=False; peak=0.0
    for r in p:
        if r<=hard: return hard-COST5X
        if not armed and r>=arm: armed=True
        if armed:
            peak=max(peak,r)
            if r<=peak*(1-gb): return r-COST5X
    return p[-1]-COST5X

def select(P, name, sz):
    d=P
    if name.endswith("|EP"): d=d[d.tier.isin(ENTPREM)]; name=name[:-3]
    if len(d)==0: return []
    if name=="ATR+lowpx": d=d.assign(k=d.atr.rank(pct=True)+(-d.price).rank(pct=True)).sort_values("k",ascending=False)
    elif name=="ATR+pullbk": d=d.assign(k=d.atr.rank(pct=True)+(-d.disthi60).rank(pct=True)).sort_values("k",ascending=False)
    elif name=="3way": d=d.assign(k=d.atr.rank(pct=True)+(-d.price).rank(pct=True)+(-d.disthi60).rank(pct=True)).sort_values("k",ascending=False)
    elif name=="hi_ATR": d=d.sort_values("atr",ascending=False)
    return list(d.head(sz).symbol)

def run(selname,sz,arm,gb,hard):
    mo=defaultdict(list); allr=[]
    for td in trade_days:
        if td not in POOLS: continue
        p=path5x(td, select(POOLS[td],selname,sz))
        if p is None: continue
        r=apply_exit(p,arm,gb,hard); mo[td[:7]].append(r); allr.append(r)
    allr=np.array(allr); won=allr[allr>0]; lost=allr[allr<=0]
    pf=won.sum()/abs(lost.sum()) if len(lost) and lost.sum()!=0 else 9.9
    return dict(tot=allr.sum(),pf=pf,wd=int((allr>0).sum()),n=len(allr),
                mar=sum(mo['2025-03']),apr=sum(mo['2025-04']),may=sum(mo['2025-05']))

print("="*92); print("  PASS 1 — SELECTION (exit arm6/gb60/hard-3) · 5x · Mar-May25 · operator +603%"); print("="*92)
print(f"  {'selection':<16}{'size':>5}{'3mo%':>9}{'PF':>6}{'winD':>7}{'Mar/Apr/May':>22}")
p1=[]
for sel in ["ATR+lowpx","ATR+pullbk","3way","3way|EP","ATR+lowpx|EP","hi_ATR|EP"]:
    for sz in [12,15,18]:
        r=run(sel,sz,6,0.6,-3); p1.append((sel,sz,r))
        mam=f"{r['mar']:+.0f}/{r['apr']:+.0f}/{r['may']:+.0f}"
        print(f"  {sel:<16}{sz:>5}{r['tot']:>+9.1f}{r['pf']:>6.2f}{r['wd']:>4}/{r['n']}{mam:>22}")
best=max(p1,key=lambda z:z[2]['tot']); bsel,bsz=best[0],best[1]
print(f"\n  best selection: {bsel} size {bsz}  ->  {best[2]['tot']:+.1f}%")

print("\n"+"="*92); print(f"  PASS 2 — EXIT tune on {bsel}-{bsz} · 5x"); print("="*92)
print(f"  {'arm':>4}{'gb':>5}{'hard':>6}{'3mo%':>9}{'PF':>6}{'winD':>7}")
p2=[]
for arm in [4,5,6,7,8]:
    for gb in [0.4,0.5,0.6,0.7]:
        for hard in [-2.0,-2.5,-3.0,-4.0]:
            r=run(bsel,bsz,arm,gb,hard); p2.append((arm,gb,hard,r))
p2.sort(key=lambda z:-z[3]['tot'])
for arm,gb,hard,r in p2[:12]:
    print(f"  {arm:>4}{gb:>5.1f}{hard:>6.1f}{r['tot']:>+9.1f}{r['pf']:>6.2f}{r['wd']:>4}/{r['n']}")
b=p2[0]
print(f"\n  BEST OVERALL: {bsel}-{bsz} arm{b[0]}/gb{int(b[1]*100)}/hard{b[2]}  ->  3mo 5x {b[3]['tot']:+.1f}%  PF {b[3]['pf']:.2f}  ·  {b[3]['tot']/603.4*100:.0f}% of operator")
print(f"    monthly: Mar {b[3]['mar']:+.0f}% · Apr {b[3]['apr']:+.0f}% · May {b[3]['may']:+.0f}%")
