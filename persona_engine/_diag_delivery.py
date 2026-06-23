"""Does delivery% (conviction) predict next-day or 4-8wk returns?
Tests raw delivery%, delivery z-score vs 20d, and delivery*direction (accumulation)."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from persona_engine import db, outcomes, universe

con = db.connect()
fo, lt = universe.get_universes(con, as_of_date="2026-06-22")
uni = sorted(set(fo) | set(lt))
dv = pd.read_sql_query(
    "SELECT symbol, trade_date, deliv_pct FROM delivery_daily WHERE symbol IN (%s)"
    % ",".join("?"*len(uni)), con, params=uni)
print("delivery rows:", len(dv), "dates:", dv.trade_date.nunique(),
      "range:", dv.trade_date.min(), dv.trade_date.max())

dv = dv.sort_values(["symbol","trade_date"])
g = dv.groupby("symbol", group_keys=False)
dv["deliv_z20"] = g["deliv_pct"].transform(lambda s:(s - s.rolling(20).mean())/s.rolling(20).std())
dv["deliv_chg"] = g["deliv_pct"].transform(lambda s: s - s.rolling(20).mean())

# forward returns
fwd = outcomes.forward_returns(con, symbols=uni)
d = dv.merge(fwd[["symbol","trade_date","fwd_nd","ret_20"]], on=["symbol","trade_date"], how="left")
# accumulation = delivery z * sign of today's return (proxy via fwd? no -> use sig ret from features)
feats = pd.read_sql_query("SELECT symbol,trade_date,sig_ret_pct,close_loc FROM persona_signal_features", con)
d = d.merge(feats, on=["symbol","trade_date"], how="left")
d["accum"] = d["deliv_z20"] * np.sign(d["sig_ret_pct"].fillna(0))
d["deliv_up"] = d["deliv_z20"] * (d["close_loc"]>0.6).astype(int)  # high delivery + strong close

fo_set=set(fo); lt_set=set(lt)
def pooled_ic(df, feat, ret):
    s=df.dropna(subset=[feat,ret]); s=s[s.groupby("trade_date")[ret].transform("size")>=30]
    if len(s)<1000: return np.nan
    fr=s.groupby("trade_date")[feat].rank(pct=True); rr=s.groupby("trade_date")[ret].rank(pct=True)
    return np.corrcoef(fr,rr)[0,1]
fo_d=d[d.symbol.isin(fo_set)]; lt_d=d[d.symbol.isin(lt_set)]
print("\nFEATURE          IC_nextday(FO)   IC_20d(LT)")
for f in ["deliv_pct","deliv_z20","deliv_chg","accum","deliv_up"]:
    print(f"  {f:12s}   {pooled_ic(fo_d,f,'fwd_nd'):+.4f}        {pooled_ic(lt_d,f,'ret_20'):+.4f}")
con.close()
