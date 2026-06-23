"""Top-5 precision framing (operator 2026-06-22):
 base rate = how often any F&O stock moves >=X% (open->close) next day;
 model = of the top-5 highest-confidence picks, how many actually moved >=X%.
 Target: >=3 of 5 (precision@5 >= 60%). Bars X = 3,4,5%. Direction-matched. Capturable
 (open->close). Walk-forward HistGBM, retrained each year on strictly-past data."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

con = db.connect(read_only=True)
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")

feats = pd.read_sql_query(
    "SELECT * FROM persona_signal_features WHERE symbol IN (%s) AND trade_date>='2021-01-01'"
    % ",".join("?"*len(fo)), con, params=fo)
ev = pd.read_sql_query(
    "SELECT symbol,trade_date,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum "
    "FROM persona_event_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo)
feats = feats.merge(ev, on=["symbol","trade_date"], how="left")
op = pd.read_sql_query(
    "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
    % ",".join("?"*len(fo)), con, params=fo)
cal = sorted(feats["trade_date"].unique())
nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
feats["odate"] = feats["trade_date"].map(nxt)
o = op.rename(columns={"trade_date":"odate"})
df = feats.merge(o, on=["symbol","odate"], how="inner").dropna(subset=["oc_full"])
df["year"] = df["odate"].str[:4]
FEATS = [f for f in ALL_FEATURES if f in df.columns] + \
        [f for f in ["earn_next1","earn_recent2","deliv_pct","deliv_z20","accum"] if f in df.columns]
X = df[FEATS].replace([np.inf,-np.inf], np.nan)

def run(thresh, direction):
    if direction=="LONG":
        df["lab"]=(df["oc_full"]>=thresh).astype(int)
    else:
        df["lab"]=(df["oc_full"]<=-thresh).astype(int)
    out={}
    for ty in ["2023","2024","2025","2026"]:
        tr=df["year"]<ty; te=df["year"]==ty
        if tr.sum()<5000 or te.sum()<300: continue
        m=HistGradientBoostingClassifier(max_iter=250,max_depth=4,learning_rate=0.06,
                                         l2_regularization=1.0,random_state=0)
        m.fit(X[tr], df.loc[tr,"lab"])
        p=m.predict_proba(X[te])[:,1]
        d=df.loc[te,["odate","lab"]].copy(); d["p"]=p
        hits=[]
        for dt,g in d.groupby("odate"):
            g=g.sort_values("p",ascending=False).head(5)
            hits.append(g["lab"].sum())   # how many of top-5 hit
        out[ty]=(np.mean(hits), np.mean(hits)/5*100, df.loc[te,"lab"].mean()*100)
    return out

print("base = unconditional %% of F&O stock-days with the move (open->close)")
print("p@5  = avg hits out of 5 top-confidence picks ; prec = p@5 as %% ; TARGET = 3/5 = 60%%\n")
for direction in ["LONG","SHORT"]:
    print(f"================== {direction} (open->close) ==================")
    for thr in [3.0,4.0,5.0]:
        res=run(thr,direction)
        print(f"  >= {thr:.0f}% move:")
        for ty,(hits,prec,base) in res.items():
            flag=" <-- 3/5 reached" if hits>=3 else ""
            print(f"     {ty}:  p@5 = {hits:.2f}/5  ({prec:.0f}%)   base = {base:.2f}%   lift = {prec/base:.1f}x{flag}")
    print()
con.close()
print("PREC5_DONE")
