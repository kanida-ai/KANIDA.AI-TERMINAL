"""RESTART · JAN 2026 · (A) what patterns fired & their HORIZON, (B) rank monotonicity next-day.
Leak-free rebuild. Read-only.
A) For the 2026-01-02 signal: which Falcon patterns fired on the top stocks, and what each pattern TARGETS
   (outcome_target like hit_10pc_20d = +10% within 20 days). Aggregate horizon + magnitude distribution.
B) Rank buckets Top-10/20/30/50/75/100: % of stocks that CLOSED POSITIVE next day (both vs signal-close and
   next-day intraday), avg return, and a 5-day column to expose the horizon mismatch. Jan-2 AND full-Jan avg.
"""
import os, sys, sqlite3, warnings, re
from collections import Counter
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]

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
    SYM[s]=dict(o=g.open.values.astype(float), c=cl, idx={d:i for i,d in enumerate(g.trade_date)}, n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec,ignore_index=True),on=["symbol","trade_date"],how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d:i for i,d in enumerate(cal)}

def rank_full(day, topk=100, want_patterns=False):
    fd=FCpit[FCpit.trade_date==day]
    if fd.empty: return []
    syms=fd.symbol.values; X=np.full((len(syms),len(FR.FEATURE_COLS)),np.nan)
    for j,col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:,j]=pd.to_numeric(fd[col],errors="coerce").values
    yr=int(day[:4]); elig=[p for p in pats if int(p["mined_year"])<yr]
    fire=np.zeros(len(syms),np.int32); score=np.zeros(len(syms)); fired=[[] for _ in range(len(syms))]
    for p in elig:
        m=FR.rule_mask(p["rule"],X)
        if not m.any(): continue
        fire+=m.astype(np.int32); score+=m.astype(np.float64)*p["oos_lift"]
        if want_patterns:
            for idx in np.where(m)[0]: fired[idx].append(p)
    cands=[{"symbol":syms[i],"score":float(score[i]),"nf":int(fire[i]),"pats":fired[i] if want_patterns else None} for i in range(len(syms)) if fire[i]>=10]
    cands.sort(key=lambda c:-c["score"]); ranked=sorted(cands[:150],key=lambda c:-(c["score"]/max(c["nf"],1)))[:topk]
    for r,c in enumerate(ranked,1): c["rank"]=r
    return ranked

# ================= PART A — patterns & horizons on Jan 2 =================
SD="2026-01-02"
rk=rank_full(SD, topk=100, want_patterns=True)
print("="*94); print(f"  PART A — 2026-01-02 signal: WHAT fired and its HORIZON"); print("="*94)
tgt=Counter()
for c in rk[:20]:
    for p in c["pats"]: tgt[p["target"]]+=1
def parse(t):
    m=re.match(r"hit_(\d+)pc_(\d+)d", t or "");
    return (int(m.group(1)), int(m.group(2))) if m else (None,None)
horizons=Counter(); mags=Counter()
for t,n in tgt.items():
    mg,hz=parse(t); horizons[hz]+=n; mags[mg]+=n
tot=sum(tgt.values())
print(f"  across the Top-20 picks, {tot} pattern-fires. Their TARGET horizons:")
for hz,n in sorted(horizons.items(), key=lambda x:-x[1]):
    if hz: print(f"     +move within {hz} days : {n/tot*100:>4.0f}% of fires")
print(f"  their TARGET magnitudes:")
for mg,n in sorted(mags.items(), key=lambda x:-x[1]):
    if mg: print(f"     +{mg}% move          : {n/tot*100:>4.0f}% of fires")
print(f"\n  concrete — the #1 pick ({rk[0]['symbol']}, {rk[0]['nf']} fires) top patterns:")
for p in sorted(rk[0]["pats"], key=lambda p:-(p['oos_lift'] or 0))[:5]:
    rule=" AND ".join(f"{f}{op}{th}" for f,op,th in p["rule"])
    print(f"     FALCPAT_{p['pattern_id']}  target={p['target']}  oos_lift={p['oos_lift']}  | {rule[:80]}")
print(f"\n  >> the ranking is optimized to predict a ~{max(horizons,key=horizons.get)}-day, +{max(mags,key=mags.get)}% move — NOT a next-day move.")

# ================= PART B — rank monotonicity next-day =================
def fwd(day, ranked):
    ci=cidx[day]; d1=cal[ci+1]
    out=[]
    for c in ranked:
        S=SYM.get(c["symbol"]); i=S["idx"].get(d1) if S else None; i0=S["idx"].get(day) if S else None
        if i is None or i0 is None or i+5>=S["n"]: continue
        e=S["o"][i]  # next-day open (entry)
        if e<=0: continue
        out.append(dict(rank=c["rank"],
            cc=(S["c"][i]/S["c"][i0]-1)*100,       # signal-close -> next-day close ("closed positive next day")
            oc=(S["c"][i]/e-1)*100,                # next-day intraday (buy 9:15 -> next-day close)
            d5=(S["c"][i+4]/e-1)*100))             # entry -> 5th session close (nearer the pattern horizon)
    return pd.DataFrame(out)

BUCKETS=[10,20,30,50,75,100]
print("\n"+"="*94); print("  PART B — does the ranking sort next-day winners?  (% of picks that closed positive)"); print("="*94)
F2=fwd(SD, rk)
print(f"  Jan-2 signal (entry Jan-5):")
print(f"  {'bucket':<8}{'n':>4}{'%+ close-to-close':>19}{'%+ next-day intraday':>22}{'avg cc%':>10}{'avg 5d%':>10}")
for b in BUCKETS:
    g=F2[F2['rank']<=b]
    print(f"  Top-{b:<4}{len(g):>4}{(g.cc>0).mean()*100:>17.0f}%{(g.oc>0).mean()*100:>21.0f}%{g.cc.mean():>+10.2f}{g.d5.mean():>+10.2f}")

# full January average
jan=[d for d in sorted(FCpit[(FCpit.trade_date>="2026-01-01")&(FCpit.trade_date<="2026-01-31")].trade_date.unique()) if cidx.get(d) and cidx[d]+1<len(cal)]
allf=[]
for d in jan:
    r=rank_full(d, topk=100); ff=fwd(d,r); ff["day"]=d; allf.append(ff)
A=pd.concat(allf, ignore_index=True)
print(f"\n  FULL JANUARY ({len(jan)} signal days, robust):")
print(f"  {'bucket':<8}{'n':>6}{'%+ close-to-close':>19}{'%+ next-day intraday':>22}{'avg cc%':>10}{'avg 5d%':>10}")
for b in BUCKETS:
    g=A[A['rank']<=b]
    print(f"  Top-{b:<4}{len(g):>6}{(g.cc>0).mean()*100:>17.0f}%{(g.oc>0).mean()*100:>21.0f}%{g.cc.mean():>+10.2f}{g.d5.mean():>+10.2f}")
# marginal deciles to see the gradient
print(f"\n  marginal rank bands (full Jan) — is rank 1-10 better than 41-50, 91-100?")
print(f"  {'band':<10}{'n':>6}{'%+ close-to-close':>19}{'avg cc%':>10}{'avg 5d%':>10}")
for a,b in [(1,10),(11,20),(21,30),(41,50),(76,100)]:
    g=A[(A['rank']>=a)&(A['rank']<=b)]
    print(f"  {str(a)+'-'+str(b):<10}{len(g):>6}{(g.cc>0).mean()*100:>17.0f}%{g.cc.mean():>+10.2f}{g.d5.mean():>+10.2f}")
