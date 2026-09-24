"""Deployable systematic strategy: high-tier intraday basket + operator's exit (arm6/gb60/hard-3) at 5x.
Mar-May 2025. Buy 09:15, basket-level proportional trail + hard stop on the 1-MINUTE path, else EOD 15:29.
Config (interpreted in 5x-return space, matching operator's screen): arm=+6% (arm trail), giveback=60% of
peak gain, hard=-3% (hard stop). Top-20 high-tier by rank, equal-weight, cost 0.5% (5x round-trip).
Leak-free tiers. Reports monthly P&L, win-days, profit factor vs operator. Read-only."""
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
TOPN=20; ARM=6.0; GB=0.60; HARD=-3.0; COST5X=0.5   # 5x-return space
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
rec=[]; TF={}
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
    for x in c:
        al=x["score"]/max(x["nf"],1); tf=TF.get((x["symbol"],sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],al,tf[3],tf[4]) in HIGH: out.append(x["symbol"])
        if len(out)>=TOPN: break
    return out

oc=sqlite3.connect("file:"+ODB.replace("\\","/")+"?mode=ro",uri=True)
trade_days=[d for d in cal if "2025-03-01"<=d<="2025-05-31"]
days=[]
for td in trade_days:
    ci=cidx[td]
    if ci-1<0: continue
    names=picks(cal[ci-1])
    if not names: continue
    # per-minute basket path from 1-min
    bsum=defaultdict(float); bcnt=defaultdict(int); ent={}
    for s in names:
        b=oc.execute("SELECT bar_time,open,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                     (s, td+" 09:15", td+" 15:35")).fetchall()
        if len(b)<5: continue
        e=b[0][1]
        if e is None or e<=0: continue
        ent[s]=e
        for t,o,c in b:
            if c and c>0: bsum[t]+=(c/e-1); bcnt[t]+=1
    if not ent: continue
    times=sorted(bsum)
    path5x=np.array([5*100*bsum[t]/bcnt[t] for t in times if bcnt[t]>=max(3,len(ent)//2)])
    if len(path5x)==0: continue
    # apply arm6/gb60/hard-3 on path5x
    armed=False; peak=0.0; exit_ret=None; reason=None
    for r in path5x:
        if r<=HARD: exit_ret=HARD; reason="HARD_STOP"; break
        if not armed and r>=ARM: armed=True
        if armed:
            peak=max(peak,r)
            if r<=peak*(1-GB): exit_ret=r; reason="TRAIL"; break
    if exit_ret is None: exit_ret=path5x[-1]; reason="EOD"
    net5x=exit_ret-COST5X
    days.append(dict(td=td,mo=td[:7],n=len(ent),ret5x=net5x,ret1x=net5x/5,reason=reason))
oc.close()
D=pd.DataFrame(days)

OP={"2025-03":(211.7,"14/18"),"2025-04":(170.9,"11/19"),"2025-05":(220.8,"14/21")}
print("="*90)
print(f"  DEPLOYABLE STRATEGY · high-tier Top-{TOPN} intraday · arm{int(ARM)}/gb{int(GB*100)}/hard{int(HARD)} · 5x · net {COST5X}%")
print("="*90)
print(f"  {'month':<9}{'days':>6}{'sysRet5x%':>11}{'winDays':>9}{'PF':>7}{'EOD/TRAIL/STOP':>18}   operator5x")
for mo in ["2025-03","2025-04","2025-05"]:
    g=D[D.mo==mo]; won=g[g.ret5x>0]; lost=g[g.ret5x<=0]
    pf=won.ret5x.sum()/abs(lost.ret5x.sum()) if len(lost) and lost.ret5x.sum()!=0 else float('inf')
    mix=g.reason.value_counts().to_dict()
    mixs=f"{mix.get('EOD',0)}/{mix.get('TRAIL',0)}/{mix.get('HARD_STOP',0)}"
    op=OP[mo]
    print(f"  {mo:<9}{len(g):>6}{g.ret5x.sum():>+11.1f}{len(won):>4}/{len(g)}{pf:>7.2f}{mixs:>18}   +{op[0]}% ({op[1]})")
won=D[D.ret5x>0]; lost=D[D.ret5x<=0]
pf=won.ret5x.sum()/abs(lost.ret5x.sum())
print("  "+"-"*88)
print(f"  3-MONTH systematic 5x: {D.ret5x.sum():+.1f}%  (1x {D.ret1x.sum():+.1f}%)  ·  win-days {len(won)}/{len(D)}  ·  PF {pf:.2f}")
print(f"  operator 3-month 5x: +603.4%  (1x +120.7%)")
print(f"  avg win day +{won.ret5x.mean():.1f}%  ·  avg loss day {lost.ret5x.mean():.1f}%  ·  worst {D.ret5x.min():.1f}%")
out=os.path.join(os.path.expanduser("~"),"Downloads","FALCON_DEPLOY_STRATEGY_3MONTH.xlsx")
D.to_excel(out,index=False); print(f"\n  daily log -> {out}")
