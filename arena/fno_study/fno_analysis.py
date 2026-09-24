"""STAGE 3 — analysis + per-stock profitability profiles from the Stage-2 records.
Relationships: tier vs profit, rank-band vs profit, pattern vs profit, long vs short, MFE->EOD capture, MAE vs outcome.
Per-stock profiles (long & short): win-rate, avg/median return, Sharpe, maxDD, monthly consistency, MFE/MAE, edge.
-> arena/fno_study/FNO_LONG_SHORT_STUDY.xlsx + console summary. Read-only; Falcon untouched."""
import os, glob
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
REC = os.path.join(ROOT, "arena", "fno_study", "records")
files = sorted(glob.glob(os.path.join(REC, "*.csv")))
D = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
print(f"loaded {len(D):,} records | {D.symbol.nunique()} stocks | {D.month.nunique()} months {D.month.min()}..{D.month.max()}")
ANN = np.sqrt(252)

def dd(series):                       # max drawdown on sum-of-daily equity (return points)
    eq = series.cumsum().values
    return float((np.maximum.accumulate(eq) - eq).max()) if len(eq) else 0.0

def side_profile(g, ret, mfe, mae):
    s = g[ret]
    return pd.Series({"days": len(s), "win%": (s > 0).mean()*100, "avg%": s.mean(), "median%": s.median(),
                      "std%": s.std(), "sharpe": (s.mean()/s.std()*ANN) if s.std() else 0.0,
                      "maxDD%": dd(s), "avgMFE%": g[mfe].mean(), "avgMAE%": g[mae].mean(),
                      "capture%": (s.mean()/g[mfe].mean()*100) if g[mfe].mean() else np.nan,
                      "posMonths%": (g.groupby("month")[ret].mean() > 0).mean()*100})

def build_profiles(side_ret, side_mfe, side_mae):
    prof = D.groupby("symbol").apply(lambda g: side_profile(g, side_ret, side_mfe, side_mae)).reset_index()
    prof = prof.merge(D.groupby("symbol").sector.agg(lambda x: x.dropna().mode().iloc[0] if x.dropna().size else None).reset_index(), on="symbol")
    return prof.sort_values("sharpe", ascending=False)

LONG = build_profiles("L_eod_n", "L_mfe", "L_mae")
SHORT = build_profiles("S_eod_n", "S_mfe", "S_mae")

# relationship tables
def by(col, ret):
    t = D.groupby(col)[ret].agg(n="count", avg="mean", med="median", win=lambda x: (x > 0).mean()*100)
    return t.sort_values("avg", ascending=False)
TIER_L = by("tier", "L_eod_n"); TIER_S = by("tier", "S_eod_n")
D["rankband"] = pd.cut(D["rank"], [0,10,25,50,100,200,500], labels=["1-10","11-25","26-50","51-100","101-200","201-500"])
RANK_L = D.groupby("rankband", observed=True).L_eod_n.agg(n="count", avg="mean", win=lambda x:(x>0).mean()*100)
RANK_S = D.groupby("rankband", observed=True).S_eod_n.agg(n="count", avg="mean", win=lambda x:(x>0).mean()*100)
PAT_L = by("entry_context", "L_eod_n").head(20)
# monthly long vs short
MON = D.groupby("month").agg(L_avg=("L_eod_n","mean"), L_win=("L_eod_n",lambda x:(x>0).mean()*100),
                             S_avg=("S_eod_n","mean"), S_win=("S_eod_n",lambda x:(x>0).mean()*100), recs=("symbol","count"))
# MFE capture & MAE-vs-outcome
CAP = pd.DataFrame({"long":[D.L_eod_g.mean(), D.L_mfe.mean(), D.L_eod_g.mean()/D.L_mfe.mean()*100, D.L_mae.mean()],
                    "short":[D.S_eod_g.mean(), D.S_mfe.mean(), D.S_eod_g.mean()/D.S_mfe.mean()*100, D.S_mae.mean()]},
                   index=["avg EOD gross%","avg MFE%","capture% (EOD/MFE)","avg MAE%"])

xls = os.path.join(ROOT, "arena", "fno_study", "FNO_LONG_SHORT_STUDY.xlsx")
with pd.ExcelWriter(xls, engine="openpyxl") as w:
    LONG.round(3).to_excel(w, "LongProfiles", index=False)
    SHORT.round(3).to_excel(w, "ShortProfiles", index=False)
    TIER_L.round(3).to_excel(w, "Tier_Long"); TIER_S.round(3).to_excel(w, "Tier_Short")
    RANK_L.round(3).to_excel(w, "RankBand_Long"); RANK_S.round(3).to_excel(w, "RankBand_Short")
    PAT_L.round(3).to_excel(w, "Pattern_Long")
    MON.round(3).to_excel(w, "Monthly_LongShort")
    CAP.round(3).to_excel(w, "MFE_MAE_capture")
print(f"\n-> {xls}")

pd.set_option("display.width", 200)
print("\n================ TIER vs PROFITABILITY (net %/day) ================")
print("LONG:\n", TIER_L.round(3).to_string()); print("SHORT:\n", TIER_S.round(3).to_string())
print("\n================ RANK-BAND vs PROFITABILITY ================")
print("LONG:\n", RANK_L.round(3).to_string()); print("SHORT:\n", RANK_S.round(3).to_string())
print("\n================ MONTHLY LONG vs SHORT ================\n", MON.round(3).to_string())
print("\n================ MFE / MAE capture ================\n", CAP.round(3).to_string())
print("\n================ TOP 15 LONG stocks (by Sharpe) ================\n",
      LONG.head(15)[["symbol","days","win%","avg%","sharpe","maxDD%","posMonths%","sector"]].round(2).to_string(index=False))
print("\n================ TOP 15 SHORT stocks (by Sharpe) ================\n",
      SHORT.head(15)[["symbol","days","win%","avg%","sharpe","maxDD%","posMonths%","sector"]].round(2).to_string(index=False))
