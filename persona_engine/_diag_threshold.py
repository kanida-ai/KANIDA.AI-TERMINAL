"""Feasibility of the NEW threshold objective: predict next-day moves >=+5% (long)
and <=-5% (short). Base rates + walk-forward HistGBM precision@10 vs baselines."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

con = db.connect()
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
feats = pd.read_sql_query(
    "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>='2022-01-01'"
    % ",".join("?"*len(fo)), con, params=fo)
openf = pd.read_sql_query(
    "SELECT symbol,trade_date,prev_close,day_close,day_open,oc_full FROM persona_open_features "
    "WHERE symbol IN (%s) AND trade_date>='2022-01-01'" % ",".join("?"*len(fo)), con, params=fo)

# next trading date map per symbol
cal = sorted(feats["trade_date"].unique())
nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
feats["odate"] = feats["trade_date"].map(nxt)
o = openf.rename(columns={"trade_date":"odate"})
df = feats.merge(o, on=["symbol","odate"], how="inner")
df["full_move"] = (df["day_close"]/df["prev_close"]-1)*100   # prev_close -> next close (chart move)
df = df.dropna(subset=["full_move"])
df["up5"] = (df["full_move"] >= 5).astype(int)
df["dn5"] = (df["full_move"] <= -5).astype(int)
df["oc5u"] = (df["oc_full"] >= 5).astype(int)  # capturable open->close
df["oc5d"] = (df["oc_full"] <= -5).astype(int)
df["year"] = df["odate"].str[:4]

print("rows:", len(df))
print("BASE RATES (share of F&O stock-days):")
print(f"  next-day >=+5% (chart): {df['up5'].mean()*100:.2f}%   <=-5%: {df['dn5'].mean()*100:.2f}%")
print(f"  open->close >=+5%:      {df['oc5u'].mean()*100:.2f}%   <=-5%: {df['oc5d'].mean()*100:.2f}%")

FEATS = [f for f in ALL_FEATURES if f in df.columns]
X = df[FEATS].replace([np.inf,-np.inf], np.nan)

def walkforward(label):
    out = {}
    for ty in ["2023","2024","2025","2026"]:
        tr = df["year"] < ty
        te = df["year"] == ty
        if tr.sum() < 5000 or te.sum() < 500: continue
        m = HistGradientBoostingClassifier(max_iter=200, max_depth=4, learning_rate=0.06,
                                           l2_regularization=1.0, random_state=0)
        m.fit(X[tr], df.loc[tr, label])
        p = m.predict_proba(X[te])[:,1]
        d = df.loc[te, ["odate", label]].copy(); d["p"] = p
        # precision @ top-10 per day
        prec=[]; nsig=[]
        for dt,g in d.groupby("odate"):
            g=g.sort_values("p",ascending=False)
            top=g.head(10)
            prec.append(top[label].mean()); nsig.append(top[label].sum())
        out[ty]=(np.mean(prec)*100, df.loc[te,label].mean()*100)
    return out

for label,name in [("up5","LONG >=+5% (chart)"),("dn5","SHORT <=-5% (chart)"),
                   ("oc5u","LONG >=+5% (open->close)"),("oc5d","SHORT <=-5% (open->close)")]:
    res = walkforward(label)
    print(f"\n{name}: precision@10/day  (vs base rate)")
    for ty,(prec,base) in res.items():
        print(f"   {ty}: {prec:.1f}%  (base {base:.2f}%, lift {prec/base:.1f}x)")
con.close()
