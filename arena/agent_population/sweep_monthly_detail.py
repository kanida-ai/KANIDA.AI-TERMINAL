"""Month-by-month capital-normalized return for the sweep FINALISTS (leak-free, point-in-time)."""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
GOLD = {"GOLD", "GOLD-baseline"}; ENTPREM = {"ENTERPRISE-Dryup", "PREMIUM-Pullback", "PREMIUM-Compression"}; HIGH = GOLD | ENTPREM
STOPPX = -6.0


def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"


con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-31' ORDER BY symbol,trade_date", con); con.close()
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date); o2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close","last"), wh=("high","max")).reset_index(); wb["sm"]=wb.wc.rolling(20).mean().shift(1); wb["ph"]=wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk,wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk,wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi>lo,(c-lo)/(hi-lo),np.nan), weekly_range_pct=np.where(c>0,(hi-lo)/c*100,np.nan),
        weekly_close_vs_sma20=np.where((sm==sm)&(sm>0),(c/sm-1)*100,np.nan), weekly_breakout_20w=np.where(ph==ph,(c>ph).astype(float),np.nan))))
    pc=np.roll(c,1);pc[0]=np.nan;c2=np.roll(c,2);c2[:2]=np.nan
    sret=(c/pc-1)*100;rng=(h-l)/pc*100;twoday=(c/c2-1)*100
    av20=pd.Series(v).rolling(20).mean().values;av3=pd.Series(v).rolling(3).mean().values;tr3=np.where(av20>0,av3/av20,np.nan)
    tp=pd.Series(c*v).rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True).values
    for i,d in enumerate(g.trade_date.values): TF[(s,d)]=(sret[i],rng[i],twoday[i],tr3[i],tp[i])
    SYM[s]=dict(o=g.open.values.astype(float),h=h,l=l,c=c,idx={d:i for i,d in enumerate(g.trade_date)},n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")


def basket(day):
    fd = FCpit[FCpit.trade_date==day]
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
    cands=[{"symbol":syms[i],"n_fires":int(fire[i]),"score":float(score[i])} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:100],key=lambda c:-(c["score"]/max(c["n_fires"],1)))[:15]
    out=[]
    for rk,c in enumerate(ranked,1):
        al=c["score"]/max(c["n_fires"],1); tf=TF.get((c["symbol"],day),(np.nan,)*5)
        out.append((c["symbol"],rk,classify(tf[0],tf[2],tf[1],al,tf[3],tf[4])))
    return out


def longret(sym,day,hh):
    S=SYM.get(sym); i=S["idx"].get(day) if S else None
    if i is None or i+hh>=S["n"]: return None
    e=S["o"][i+1]
    if e<=0: return None
    return (S["c"][i+hh]/e-1)*100 - (0.15 if hh==1 else 0.30)

def shortret(sym,day):
    S=SYM.get(sym); i=S["idx"].get(day) if S else None
    if i is None or i+1>=S["n"]: return None
    e=S["o"][i+1]
    if e<=0: return None
    return (e/S["c"][i+1]-1)*100 - 0.15

FILT={"high_tier":lambda rk,ti:ti in HIGH,"GOLD_only":lambda rk,ti:ti in GOLD,"TOP5_rank":lambda rk,ti:rk<=5,"ENT_PREM":lambda rk,ti:ti in ENTPREM}
# (name, filter, dir, hold)
FINAL=[("TOP5 short intraday","TOP5_rank","short",1),
       ("high_tier short intraday","high_tier","short",1),
       ("GOLD short intraday","GOLD_only","short",1),
       ("ENT_PREM short intraday","ENT_PREM","short",1),
       ("GOLD long 20-session","GOLD_only","long",20),
       ("high_tier long 5-session","high_tier","long",5),
       ("high_tier long 2 (BASELINE)","high_tier","long",2)]
months=[f"2026-{m:02d}" for m in range(1,8)]
sigdays=sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-07-31")].trade_date.unique())
# monthly sum of basket returns per config
from collections import defaultdict
data={nm:defaultdict(list) for nm,_,_,_ in FINAL}
for d in sigdays:
    mo=d[:7]; bk=basket(d)
    if not bk: continue
    for nm,fk,dr,hh in FINAL:
        names=[s for (s,rk,ti) in bk if FILT[fk](rk,ti)]
        if not names: continue
        if dr=="short": vals=[shortret(s,d) for s in names]
        else: vals=[longret(s,d,hh) for s in names]
        vals=[x for x in vals if x is not None]
        if vals: data[nm][mo].append(float(np.mean(vals)))
print("MONTHLY capital-normalized return %  (sum of basket% in month / hold)  ·  leak-free point-in-time\n")
print(f"  {'config':<30}" + "".join(f"{m[5:]:>8}" for m in months) + f"{'7mo':>9}{'posMo':>7}")
for nm,_,_,hh in FINAL:
    cells=[]; tot=0; pos=0; nm_present=0
    for m in months:
        if m in data[nm] and data[nm][m]:
            roc=sum(data[nm][m])/hh; cells.append(f"{roc:>+8.1f}"); tot+=roc; pos+= (roc>0); nm_present+=1
        else: cells.append(f"{'—':>8}")
    print(f"  {nm:<30}" + "".join(cells) + f"{tot:>+9.1f}{pos:>4}/{nm_present}")
