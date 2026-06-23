"""Diagnostic: predictive power of opening signals (gap, 9:15->9:45 momentum) for
the rest-of-day (09:45->close) and full-day (09:15->close) returns, plus the
top-10 overlap a pure opening-confirmation model would achieve. Intraday period only."""
import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
from persona_engine import db, universe

con = db.connect()
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
df = pd.read_sql_query(
    "SELECT * FROM persona_open_features WHERE has_intraday=1 AND symbol IN (%s)"
    % ",".join("?" * len(fo)), con, params=fo)
print("intraday rows:", len(df), "dates:", df.trade_date.nunique(),
      "range:", df.trade_date.min(), df.trade_date.max())


def pooled_ic(d, feat, ret):
    s = d.dropna(subset=[feat, ret])
    s = s[s.groupby("trade_date")[ret].transform("size") >= 30]
    if len(s) < 1000:
        return np.nan
    fr = s.groupby("trade_date")[feat].rank(pct=True)
    rr = s.groupby("trade_date")[ret].rank(pct=True)
    return np.corrcoef(fr, rr)[0, 1]


print("\nIC vs ret_0945_close (capturable) / oc_full (spec):")
for f in ["gap_pct", "ret_o_0930", "ret_o_0945", "ret_o_1000", "evol_0945"]:
    print(f"  {f:12s} {pooled_ic(df,f,'ret_0945_close'):+.4f}   {pooled_ic(df,f,'oc_full'):+.4f}")


def overlap(d, sel_col, tgt_col, k=10, asc=False):
    """For each date: top-k by sel_col, measure overlap with actual top-k by tgt_col."""
    res = []
    for dt, g in d.groupby("trade_date"):
        g = g.dropna(subset=[sel_col, tgt_col])
        if len(g) < 30:
            continue
        sel = set(g.sort_values(sel_col, ascending=asc).head(k)["symbol"])
        act = set(g.sort_values(tgt_col, ascending=asc).head(k)["symbol"])
        res.append(len(sel & act))
    return np.mean(res), len(res)


print("\nTop-10 overlap (pure opening-confirmation model):")
for sel, tgt, asc, lbl in [
    ("ret_o_0945", "ret_0945_close", False, "LONG: early-mom -> actual top gainers (rest-of-day)"),
    ("ret_o_0945", "ret_0945_close", True,  "SHORT: early-weak -> actual top losers (rest-of-day)"),
    ("gap_pct", "oc_full", False, "LONG: gap -> actual top gainers (open->close, spec)"),
    ("gap_pct", "oc_full", True,  "SHORT: gap -> actual top losers (open->close, spec)"),
    ("ret_o_0945", "oc_full", False, "LONG: early-mom -> actual top gainers (open->close)"),
]:
    m, n = overlap(df, sel, tgt, asc=asc)
    print(f"  {lbl:55s}: {m:.2f}/10 = {m*10:.0f}%  (n={n})")
con.close()
