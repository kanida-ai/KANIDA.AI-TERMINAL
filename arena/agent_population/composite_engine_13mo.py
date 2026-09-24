"""FINAL composite engine, run straight through Oct2024->Oct2025 (train 8mo + forward 5mo vs summaries).
High-tier pool; rank by composite = capitulation(down-streak x vol>1.5) + confluence(nf) + mover(ATR,low-price);
top-15; enter 09:15; exit arm6/gb60/hard-3 on 1-min; 5x. Compare monthly to operator. Leak-free. Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
from collections import defaultdict
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0,os.path.join(ROOT,"scripts")); import falcon_signal_replay as FR
UDB=os.path.join(ROOT,"data","db","kanida_universe.db"); ODB=os.path.join(ROOT,"universe_engine","data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
ARM=6.0; GB=0.60; HARD=-3.0; COST5X=0.5; SIZE=15; ONEMIN_END="2026-07-10"
OP={"2024-10":104.4,"2024-11":141.3,"2024-12":163.9,"2025-01":170.4,"2025-02":59.2,"2025-03":211.7,"2025-04":170.9,"2025-05":220.8,
    "2025-06":174.6,"2025-07":175.9,"2025-08":126.7,"2025-09":158.6,"2025-10":165.7}
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
rows865=con.execute("""SELECT c.mined_year,c.rule_json FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for my,rj in rows865:
    try: PATS.append((my,[(f,op,th) for f,op,th in json.loads(rj)]))
    except: pass
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-10-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-10-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}; DAILY={}
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
    down=(c<pc).astype(int); ds=np.zeros(len(c))
    for i in range(1,len(c)): ds[i]=ds[i-1]+1 if down[i] else 0
    volr=v/np.where(a20==0,1e-9,a20)
    DAILY[s]=dict(ds=ds,volr=volr,idx={d:i for i,d in enumerate(g.trade_date)})
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}
def rankpct(a):
    a=np.asarray(a,float); o=np.argsort(np.argsort(a)); return o/max(len(a)-1,1)
def pool(sd):
    fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=np.zeros(len(syms),np.int32)
    for (my,rule) in PATS:
        if int(my)>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in rule:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        fire+=m.astype(np.int32)
    atr=pd.to_numeric(fd.get("atr_20_pct"),errors="coerce").values
    cand=[]
    for i in range(len(syms)):
        if fire[i]<10: continue
        s=syms[i]; tf=TF.get((s,sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) not in HIGH: continue
        D=DAILY.get(s); k=D["idx"].get(sd) if D else None
        if k is None: continue
        cand.append((s, fire[i], atr[i], D["ds"][k], D["volr"][k]))
    if not cand: return []
    nf=rankpct([c[1] for c in cand]); at=rankpct([c[2] for c in cand]); dstr=rankpct([min(c[3],5) for c in cand]); vr=rankpct([min(c[4],3) for c in cand])
    comp=nf+at+dstr+vr   # confluence + mover + capitulation(down-streak+vol)
    order=np.argsort(-comp)
    return [cand[i][0] for i in order[:SIZE]]
oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
days=[d for d in cal if "2024-10-01"<=d<="2025-10-31" and d<=ONEMIN_END]
monthly=defaultdict(list)
for td in days:
    ci=cidx[td]
    if ci-1<0: continue
    names=pool(cal[ci-1])
    if not names: continue
    q="SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN (%s) ORDER BY symbol,bar_time"%",".join("?"*len(names))
    permin={}; cur=None; ent=None
    for sym,t,o,c in oc.execute(q,(td+" 09:15",td+" 15:35",*names)).fetchall():
        if sym!=cur: cur=sym; ent=o; permin[sym]={}
        if ent and ent>0 and c and c>0: permin[sym][t]=(c/ent-1)
    have=[s for s in names if s in permin and permin[s]]
    if len(have)<3: continue
    bsum=defaultdict(float); bcnt=defaultdict(int)
    for s in have:
        for t,r in permin[s].items(): bsum[t]+=r; bcnt[t]+=1
    times=sorted(bsum); p=np.array([5*100*bsum[t]/bcnt[t] for t in times if bcnt[t]>=max(3,len(have)//2)])
    if len(p)==0: continue
    armed=False;peak=0.0;ex=None
    for r in p:
        if r<=HARD: ex=HARD;break
        if not armed and r>=ARM: armed=True
        if armed:
            peak=max(peak,r)
            if r<=peak*(1-GB): ex=r;break
    if ex is None: ex=p[-1]
    monthly[td[:7]].append(ex-COST5X)
oc.close()
print("="*80); print("  COMPOSITE ENGINE (capitulation+confluence+mover) · 5x · Oct24-Oct25"); print("  train Oct24-May25 | FORWARD Jun-Oct25 (vs summary)"); print("="*80)
print(f"  {'month':<9}{'ENGINE 5x%':>12}{'OPERATOR':>11}{'winDays':>9}{'phase':>8}")
te=to=0
for mo in sorted(monthly):
    e=sum(monthly[mo]); te+=e; to+=OP.get(mo,0)
    ph="TRAIN" if mo<="2025-05" else "FWD"
    print(f"  {mo:<9}{e:>+12.1f}{OP.get(mo,np.nan):>+11.1f}{int((np.array(monthly[mo])>0).sum())}/{len(monthly[mo]):<3}{ph:>8}")
print("  "+"-"*78)
print(f"  ENGINE {te:+.1f}%  ·  OPERATOR {to:+.1f}%  ·  {te/to*100:.0f}% of operator")
