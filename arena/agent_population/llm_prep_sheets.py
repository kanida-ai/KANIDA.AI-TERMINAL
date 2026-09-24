"""Prepare candidate 'reading sheets' for the LLM pattern-chart approach. For a sample of operator days,
write one text sheet per day (all high-tier candidates: recent price/volume action + fired pattern rules +
technicals) + ground truth (operator picks + next-day intraday return per candidate). Leak-free. Read-only."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
OUT=os.path.join(AP,"llm_sheets"); os.makedirs(OUT,exist_ok=True)
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
SAMPLE=["2024-10-08","2024-11-14","2024-12-19","2025-01-23","2025-02-19","2025-03-04","2025-03-21","2025-04-07","2025-05-16"]
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
rows865=con.execute("""SELECT c.pattern_id,c.mined_year,c.rule_json,c.outcome_target,p.avg_oos_year_lift_pp
    FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for pid,my,rj,tgt,lift in rows865:
    try: PATS.append((pid,my,[(f,op,th) for f,op,th in json.loads(rj)],tgt,lift if lift is not None else 0.0))
    except: pass
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-01-01' AND trade_date<='2025-05-31' ORDER BY symbol,trade_date",con); con.close()
o2=oh.copy(); dt=pd.to_datetime(o2.trade_date); o2["wk"]=dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec=[]; TF={}; DAILY={}
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
    down=(c<pc).astype(int); ds=np.zeros(len(c))
    for i in range(1,len(c)): ds[i]=ds[i-1]+1 if down[i] else 0
    volr=v/np.where(a20==0,1e-9,a20)
    DAILY[s]=dict(c=c,o=o,v=v,a20=a20,ds=ds,volr=volr,idx={d:i for i,d in enumerate(g.trade_date)},n=len(c),dates=list(g.trade_date))
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}
def rulestr(rule): return " & ".join(f"{f}{op}{round(th,2)}" for f,op,th in rule)
GT={}
for td in SAMPLE:
    ci=cidx[td]; sd=cal[ci-1]
    fd=FCpit[FCpit.trade_date==sd]
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(sd[:4]); fire=[[] for _ in range(len(syms))]
    for (pid,my,rule,tgt,lift) in PATS:
        if int(my)>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in rule:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok or not m.any(): continue
        for i in np.where(m)[0]: fire[i].append((lift,tgt,rule))
    fdi=fd.set_index("symbol"); lines=[]; rets={}; opset=PICKS.get(td,[])
    for i in range(len(syms)):
        if len(fire[i])<10: continue
        s=syms[i]; tf=TF.get((s,sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) not in HIGH: continue
        D=DAILY.get(s); k=D["idx"].get(sd) if D else None
        if k is None or k<12 or k+1>=D["n"]: continue
        c10=",".join(f"{x:.0f}" for x in D["c"][k-9:k+1]); vrec=",".join(f"{D['v'][j]/max(D['a20'][j],1):.1f}x" for j in range(k-4,k+1))
        tier=classify(tf[0],tf[1],tf[2],0,tf[3],tf[4])
        fps=sorted(fire[i],key=lambda x:-x[0])[:5]; pstr=" | ".join(f"{t}:{rulestr(r)}" for _,t,r in fps)
        r=fdi.loc[s]
        lines.append(f"{s} [{tier}] nf={len(fire[i])} down_streak={int(D['ds'][k])} vol={D['volr'][k]:.1f}x sret={tf[0]:+.1f}% "
                     f"rsi={r.get('rsi_14',np.nan):.0f} distSMA200={r.get('dist_sma_200',np.nan):+.0f}% distHigh60={r.get('dist_high_60',np.nan):+.0f}% "
                     f"atr={r.get('atr_20_pct',np.nan):.1f}% | close10d[{c10}] vol5d[{vrec}] | patterns: {pstr}")
        e=D["o"][k+1]; rets[s]=(D["c"][k+1]/e-1)*100 if e>0 else 0
    open(os.path.join(OUT,f"day_{td}.txt"),"w",encoding="utf-8").write("\n".join(lines))
    GT[td]={"operator_picks":opset,"returns":rets,"n_candidates":len(lines)}
    print(f"{td}: {len(lines)} candidates, {len(opset)} operator picks")
json.dump(GT, open(os.path.join(OUT,"ground_truth.json"),"w"))
print(f"\nsheets + ground truth -> {OUT}")
