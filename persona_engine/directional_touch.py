"""
DIRECTIONAL target as the operator actually asked:
  LONG list  -> of 5 picks, how many go UP   >= +X% (intraday high reaches +X%)
  SHORT list -> of 5 picks, how many go DOWN <= -X% (intraday low reaches -X%)
Best directional model (full features: morning microstructure + market context +
prior-day) + the volatility-screen baseline. precision@5, by year, X in {1,2}.
Target: 4.5/5 @ +/-1%.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod_v2 import build, stack_predict
from persona_engine.touch_tradelog import touch_window

NPICK = 5
FOLDS = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]


def run(con, fo):
    df, FEATS = build(con, fo)                         # features incl m_volat, market ctx
    tw = touch_window(con, fo)                         # entry, hi, lo per symbol/date
    tw["up_touch"] = (tw["hi"]/tw["entry"]-1)*100
    tw["dn_touch"] = (tw["lo"]/tw["entry"]-1)*100
    d = df.merge(tw[["symbol", "date", "up_touch", "dn_touch"]], on=["symbol", "date"], how="inner")
    d = d.dropna(subset=["up_touch", "m_volat"])
    X = d[FEATS].replace([np.inf, -np.inf], np.nan)
    print(f"rows={len(d)} dates={d['date'].nunique()}\n")

    def screen_p5(rankcol, touchcol, X_thr, asc=False):
        hits = []
        for dt, g in d.groupby("date"):
            g = g.dropna(subset=[rankcol])
            if len(g) < 30: continue
            pk = g.sort_values(rankcol, ascending=asc).head(NPICK)
            hits.append((pk[touchcol] >= X_thr).sum() if touchcol == "up_touch"
                        else (pk[touchcol] <= -X_thr).sum())
        return round(np.mean(hits), 2)

    def model_p5(direction, X_thr):
        col = "up_touch" if direction == "LONG" else "dn_touch"
        lab = ((d[col] >= X_thr) if direction == "LONG" else (d[col] <= -X_thr)).astype(int).values
        res = []
        for tr_end, lo, hi in FOLDS:
            tr = (d["date"] < tr_end).values; te = ((d["date"] >= lo) & (d["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300: continue
            p = stack_predict(X, lab[tr], tr, te)
            g = d.loc[te, ["date", col]].copy(); g["p"] = p
            hh = []
            for dt, gg in g.groupby("date"):
                pk = gg.sort_values("p", ascending=False).head(NPICK)
                hh.append((pk[col] >= X_thr).sum() if direction == "LONG" else (pk[col] <= -X_thr).sum())
            res.append(np.mean(hh))
        return round(np.mean(res), 2) if res else None

    print("DIRECTIONAL precision@5 (target 4.5/5 @ +/-1%):")
    print(f"  LONG up>=1%  : volatility-screen={screen_p5('m_volat','up_touch',1)}  model={model_p5('LONG',1)}")
    print(f"  LONG up>=2%  : volatility-screen={screen_p5('m_volat','up_touch',2)}  model={model_p5('LONG',2)}")
    print(f"  SHORT dn>=1% : volatility-screen={screen_p5('m_volat','dn_touch',1)}  model={model_p5('SHORT',1)}")
    print(f"  SHORT dn>=2% : volatility-screen={screen_p5('m_volat','dn_touch',2)}  model={model_p5('SHORT',2)}")
    # base rates
    print(f"\nbase rate: up>=1% {(d['up_touch']>=1).mean()*100:.0f}%  dn<=-1% {(d['dn_touch']<=-1).mean()*100:.0f}%")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("DIRTOUCH_DONE")
