"""STAGE 1: create a NEW versioned copy of the 865 patterns (originals untouched) with per-pattern weights
RE-CALIBRATED to the operator's selection + intraday horizon. Leak-proof (week-to-date features; weights fit
on Oct2024-May2025, to be applied forward to Jun-Oct2025). For each pattern compute:
  op_lift    = P(operator picks stock | pattern fires, high-tier) / base pick-rate  (reproduces SELECTION)
  intra_lift = avg next-day open->close return when it fires  (reproduces the intraday edge / horizon)
  intra_pup  = P(next-day up) when it fires
Writes arena/agent_population/falcon_patterns_v2.json  (NEW file, does not overwrite any table). Read-only on DB."""
import os, sys, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP=os.path.join(ROOT,"arena","agent_population"); sys.path.insert(0,AP); sys.path.insert(0,os.path.join(ROOT,"scripts"))
import falcon_signal_replay as FR
from operator_picks_8mo import PICKS
UDB=os.path.join(ROOT,"data","db","kanida_universe.db")
WEEKLY=["weekly_close_loc","weekly_range_pct","weekly_close_vs_sma20","weekly_breakout_20w"]
HIGH={"PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup","GOLD","GOLD-baseline"}
OPS={"<=":np.less_equal,"<":np.less,">":np.greater,">=":np.greater_equal}
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
# COPY all 865 patterns (rules + original meta) — originals in DB untouched
rows=con.execute("""SELECT c.pattern_id,c.mined_year,c.rule_json,c.outcome_target,p.classification,p.avg_oos_year_lift_pp
    FROM falcon_promoted_patterns p JOIN falcon_pattern_candidates c ON p.pattern_id=c.pattern_id""").fetchall()
PATS=[]
for pid,my,rj,tgt,cls,lift in rows:
    try: PATS.append({"pattern_id":pid,"mined_year":my,"rule":[(f,op,th) for f,op,th in json.loads(rj)],
                      "target":tgt,"classification":cls,"orig_lift":lift if lift is not None else 0.0})
    except: pass
print(f"copied {len(PATS)} patterns -> building v2", flush=True)
feat=pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-01' AND trade_date<='2025-05-31'",con)
oh=pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2025-06-15' ORDER BY symbol,trade_date",con); con.close()
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
    DAILY[s]=dict(o=o,c=c,idx={d:i for i,d in enumerate(g.trade_date)},n=len(c))
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],twoday[i],rng[i],tr[i],turn[i])
FCpit=feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal=sorted(oh.trade_date.unique()); cidx={d:i for i,d in enumerate(cal)}; FIDX={c:i for i,c in enumerate(FR.FEATURE_COLS)}
# accumulate per-pattern stats over TRAIN window (operator days Oct24-May25)
NP=len(PATS)
op_fire=np.zeros(NP); pool_fire=np.zeros(NP); intra_sum=np.zeros(NP); intra_up=np.zeros(NP); intra_n=np.zeros(NP)
op_total=0; pool_total=0
tdays=[td for td in sorted(PICKS) if cidx.get(td) and cidx[td]-1>=0]
for td in tdays:
    sd=cal[cidx[td]-1]; fd=FCpit[FCpit.trade_date==sd]
    if fd.empty: continue
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    # high-tier mask + intraday next-day ret + operator label
    ht=np.zeros(len(syms),bool); ret=np.full(len(syms),np.nan); opl=np.zeros(len(syms),bool)
    opset=set(PICKS[td])
    for i,s in enumerate(syms):
        tf=TF.get((s,sd),(np.nan,)*5)
        if classify(tf[0],tf[1],tf[2],0,tf[3],tf[4]) in HIGH: ht[i]=True
        D=DAILY.get(s); k=D["idx"].get(td) if D else None
        if k is not None and D["o"][k]>0: ret[i]=(D["c"][k]/D["o"][k]-1)*100
        opl[i]=s in opset
    valid=ht&~np.isnan(ret)
    pool_total+=int(valid.sum()); op_total+=int((valid&opl).sum())
    yr=int(sd[:4])
    for pi,p in enumerate(PATS):
        if int(p["mined_year"])>=yr: continue
        m=np.ones(len(syms),bool); ok=True
        for f,op,th in p["rule"]:
            idx=FIDX.get(f)
            if idx is None: ok=False; break
            col=X[:,idx]; m&=OPS[op](col,th)&~np.isnan(col)
        if not ok: continue
        mm=m&valid
        if not mm.any(): continue
        pool_fire[pi]+=int(mm.sum()); op_fire[pi]+=int((mm&opl).sum())
        intra_sum[pi]+=ret[mm].sum(); intra_up[pi]+=int((ret[mm]>0).sum()); intra_n[pi]+=int(mm.sum())
base_op=op_total/max(pool_total,1)
for pi,p in enumerate(PATS):
    orate=op_fire[pi]/pool_fire[pi] if pool_fire[pi] else 0
    p["op_lift"]=round(orate/base_op,3) if base_op>0 and pool_fire[pi]>=20 else 1.0
    p["intra_lift"]=round(intra_sum[pi]/intra_n[pi],3) if intra_n[pi]>=20 else 0.0
    p["intra_pup"]=round(intra_up[pi]/intra_n[pi]*100,1) if intra_n[pi]>=20 else 0.0
    p["train_fires"]=int(pool_fire[pi])
    p["rule"]=[[f,op,th] for f,op,th in p["rule"]]
out=os.path.join(AP,"falcon_patterns_v2.json")
json.dump({"version":"v2_recalibrated","base_op_rate":round(base_op*100,2),"fit_window":"2024-10..2025-05","patterns":PATS}, open(out,"w"))
V=pd.DataFrame(PATS)
print(f"\n  base operator pick-rate in high-tier pool: {base_op*100:.1f}%")
print(f"  patterns with op_lift>1.5 (strongly operator-predictive): {(V.op_lift>1.5).sum()}")
print(f"  patterns with intra_lift>0 (positive intraday edge): {(V.intra_lift>0).sum()}/{len(V)}  ·  intra_pup>52%: {(V.intra_pup>52).sum()}")
print("\n  TOP 12 patterns by op_lift (most predictive of YOUR picks):")
for _,x in V[V.train_fires>=30].sort_values("op_lift",ascending=False).head(12).iterrows():
    rule=" & ".join(f"{f}{op}{round(th,2)}" for f,op,th in x['rule'])
    print(f"    FALCPAT_{x.pattern_id:<5} op_lift={x.op_lift:>4.1f} intra={x.intra_lift:+.2f}% pup={x.intra_pup:.0f}% fires={x.train_fires}  {rule[:60]}")
print(f"\n  v2 saved -> {out} (originals untouched)")
