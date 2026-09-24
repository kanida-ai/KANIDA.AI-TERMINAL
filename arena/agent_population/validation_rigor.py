"""Institutional validation for the agent population — quantifies overfitting / multiple-testing risk.
Builds an (agents × month) OOS return matrix (Swing hold, ALL universe, true-OOS after mining year, net cost) and computes:
  • TRIAL COUNT (how many agents were tested — the multiple-testing multiplier).
  • DEFLATED SHARPE RATIO (Bailey & López de Prado 2014) — is the BEST agent's Sharpe real once you deflate for N trials,
    track length, skew and kurtosis? DSR is a probability; >0.95 = survives.
  • PROBABILITY OF BACKTEST OVERFITTING via CSCV (Combinatorially-Symmetric Cross-Validation) — the chance that an
    in-sample winner is an out-of-sample below-median performer. PBO < 0.5 good; the lower the better.
Writes validation_summary + agent_sharpe to arena_metrics.db. Read-only sources; non-destructive to other tables."""
import os, sqlite3, json, itertools
import numpy as np, pandas as pd
from math import erf, sqrt, log, exp
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
COST = 0.15; HOLD = 15                     # Swing
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
NORM_CDF = lambda x: 0.5*(1+erf(x/sqrt(2)))
GAMMA = 0.5772156649                        # Euler–Mascheroni
def norm_ppf(p):                            # inverse normal (Acklam approx)
    a=[-3.969683028665376e+01,2.209460984245205e+02,-2.759285104469687e+02,1.383577518672690e+02,-3.066479806614716e+01,2.506628277459239e+00]
    b=[-5.447609879822406e+01,1.615858368580409e+02,-1.556989798598866e+02,6.680131188771972e+01,-1.328068155288572e+01]
    c=[-7.784894002430293e-03,-3.223964580411365e-01,-2.400758277161838e+00,-2.549732539343734e+00,4.374664141464968e+00,2.938163982698783e+00]
    d=[7.784695709041462e-03,3.224671290700398e-01,2.445134137142996e+00,3.754408661907416e+00]
    pl=0.02425
    if p<pl:
        q=sqrt(-2*log(p)); return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p<=1-pl:
        q=p-0.5; r=q*q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q/(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    q=sqrt(-2*log(1-p)); return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)

# ---- build agents × month OOS return matrix (Swing, ALL) ----
uc = sqlite3.connect("file:"+UDB.replace("\\","/")+"?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2024-06-01' ORDER BY symbol,trade_date", uc)
tax = pd.read_sql_query("SELECT pattern_id,mined_year,rule_json FROM falcon_pattern_taxonomy", uc); uc.close()
rc = sqlite3.connect(os.path.join(AP,"falcon_pattern_registry.db"))
aa = pd.read_sql_query("SELECT agent_id,pattern_id FROM arena_agents", rc); rc.close()
agents = aa.merge(tax, on="pattern_id")
cn = dict(pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", sqlite3.connect(os.path.join(AP,"arena_metrics.db"))).values)

outs=[]
for s,g in oh.groupby("symbol",sort=False):
    O=g.open.values.astype(float); C=g.close.values.astype(float); entry=np.roll(O,-1); entry[-1]=np.nan
    exitc=pd.Series(C).shift(-HOLD).values
    outs.append(pd.DataFrame({"symbol":s,"trade_date":g.trade_date.values,"r":(exitc-entry)/entry*100-COST}))
M=feat.merge(pd.concat(outs,ignore_index=True),on=["symbol","trade_date"],how="inner")
M["ym"]=M.trade_date.str[:7]; M["yr"]=M.trade_date.str[:4].astype(int)
cal=pd.DataFrame({"d":sorted(M.trade_date.unique())}); dd=pd.to_datetime(cal.d)
cal["wk"]=dd.dt.isocalendar().year.astype(str)+"-"+dd.dt.isocalendar().week.astype(str); cal["we"]=cal.wk!=cal.wk.shift(-1)
WE=dict(zip(cal.d,cal.we)); M["we"]=M.trade_date.map(WE)
INWIN=(M.trade_date>="2025-01-01")&(M.trade_date<="2026-07-31")
months=sorted(M.loc[INWIN,"ym"].unique())
def uses_weekly(rj): return any(f.startswith("weekly_") for f,_,_ in json.loads(rj))

cols={}; sharpe_rows=[]
for _,a in agents.iterrows():
    m=np.ones(len(M),bool)
    try:
        for f,op,thr in json.loads(a.rule_json): m&=OPS[op](M[f].values,thr)
    except Exception: continue
    wk=uses_weekly(a.rule_json); oos=M.yr.values>int(a.mined_year)
    idx=m & INWIN.values & oos & (M.we.values if wk else True)
    sub=M.loc[idx,["ym","r"]].dropna()
    if sub.ym.nunique()<6 or len(sub)<20: continue
    mret=sub.groupby("ym").r.mean().reindex(months).fillna(0.0)   # month = equal-weight avg signal return (%)
    cols[a.agent_id]=mret.values
    mu=mret.mean(); sd=mret.std(ddof=1)
    sr_m=mu/sd if sd>1e-9 else 0.0
    sharpe_rows.append(dict(agent_id=a.agent_id, codename=cn.get(a.agent_id,a.agent_id),
                            sharpe_ann=round(sr_m*np.sqrt(12),2), months=int((mret!=0).sum()), mean_m=round(mu,2)))

R=pd.DataFrame(cols, index=months)                # T months × N agents
N=R.shape[1]; T=R.shape[0]
SH=pd.DataFrame(sharpe_rows)

# ---- Deflated Sharpe Ratio for the best agent ----
sr_monthly=(R.mean()/R.std(ddof=1)).replace([np.inf,-np.inf],np.nan).dropna()
best_id=sr_monthly.idxmax(); sr0_hat=sr_monthly.max()          # best monthly Sharpe (non-annualised)
varSR=sr_monthly.var(ddof=1)
# expected max Sharpe under the null across N trials (Bailey–LdP)
emax=sqrt(varSR)*((1-GAMMA)*norm_ppf(1-1.0/N)+GAMMA*norm_ppf(1-1.0/(N*exp(1))))
br=R[best_id]; g1=br.skew(); g2=br.kurt()+3.0                  # skew, kurtosis (pandas kurt is excess)
denom=sqrt(max(1e-9,1 - g1*sr0_hat + (g2-1)/4.0*sr0_hat**2))
DSR=NORM_CDF((sr0_hat-emax)*sqrt(T-1)/denom)

# ---- PBO via CSCV ----
def cscv_pbo(Rm, S=8):
    T=Rm.shape[0]; S=min(S, T-(T%2)) ; S-= S%2
    if S<4: return np.nan,0
    bl=np.array_split(np.arange(T), S); idx=list(range(S)); lam=[]
    for combo in itertools.combinations(idx, S//2):
        is_rows=np.concatenate([bl[i] for i in combo]); oos_rows=np.concatenate([bl[i] for i in idx if i not in combo])
        Ris=Rm.iloc[is_rows]; Roos=Rm.iloc[oos_rows]
        sr_is=(Ris.mean()/Ris.std(ddof=1)).replace([np.inf,-np.inf],np.nan)
        sr_oos=(Roos.mean()/Roos.std(ddof=1)).replace([np.inf,-np.inf],np.nan)
        if sr_is.dropna().empty: continue
        star=sr_is.idxmax()
        rank=sr_oos.rank(pct=True).get(star,np.nan)
        if pd.isna(rank): continue
        w=min(max(rank,1e-6),1-1e-6); lam.append(log(w/(1-w)))
    lam=np.array(lam); return float((lam<0).mean()) if len(lam) else np.nan, len(lam)
PBO,ncombo=cscv_pbo(R, S=8)

summary=dict(trials_N=int(N), months_T=int(T),
             best_agent=cn.get(best_id,best_id), best_sharpe_ann=round(float(sr0_hat*np.sqrt(12)),2),
             expected_max_sharpe_ann_under_null=round(float(emax*np.sqrt(12)),2),
             deflated_sharpe_ratio=round(float(DSR),3), dsr_verdict=("SURVIVES (>0.95)" if DSR>0.95 else "NOT SIGNIFICANT"),
             pbo=round(float(PBO),3) if PBO==PBO else None, pbo_combos=int(ncombo),
             pbo_verdict=("LOW overfitting" if (PBO==PBO and PBO<0.5) else "HIGH overfitting" if PBO==PBO else "n/a"),
             agents_positive_sharpe=int((sr_monthly>0).sum()))
con=sqlite3.connect(os.path.join(AP,"arena_metrics.db"))
pd.DataFrame([summary]).to_sql("validation_summary", con, if_exists="replace", index=False)
SH.sort_values("sharpe_ann",ascending=False).to_sql("agent_sharpe", con, if_exists="replace", index=False); con.close()

print("VALIDATION RIGOR (Swing hold · ALL · true-OOS)")
for k,v in summary.items(): print(f"  {k:<38}: {v}")
print("\nInterpretation:")
print(f"  • Tested {N} agents over {T} OOS months. Best = {summary['best_agent']} (Sharpe {summary['best_sharpe_ann']} ann).")
print(f"  • Under the null, the BEST of {N} random trials would score ~{summary['expected_max_sharpe_ann_under_null']} Sharpe by luck.")
print(f"  • Deflated Sharpe = {summary['deflated_sharpe_ratio']} → {summary['dsr_verdict']} (prob the edge is real after multiple-testing).")
print(f"  • PBO = {summary['pbo']} over {ncombo} CSCV splits → {summary['pbo_verdict']} (chance an IS winner is an OOS loser).")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["validation_summary","agent_sharpe"], rows=len(SH),
       notes="Deflated Sharpe + PBO/CSCV on Swing hold, ALL universe")
