"""
TOUCH / MFE test (operator 2026-06-23 'push harder'): measure whether a pick HITS the
+/-X% target at ANY point 10:00->15:15 (intraday high for long, low for short), not
whether it CLOSED there. This is how a real target-order trade books a winner, and is
the likely basis for higher hit-rates quoted elsewhere.

Entry = 10:00 price. up_touch = intraday HIGH >= entry*(1+X%); dn_touch = LOW <= entry*(1-X%).
Screen = rank by morning volatility (best from setup screen) + a model. Report precision@5.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

NPICK = 5
FOLDS = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]


def touch_frame(con, fo):
    # per (symbol,date): entry at 10:00, then HIGH/LOW/last-close over 10:00..15:15
    rows = con.execute(
        "SELECT symbol, substr(bar_time,1,10) d, "
        "MIN(CASE WHEN substr(bar_time,12,5)='10:00' THEN open END) entry, "
        "MAX(high) hi, MIN(low) lo, "
        "MAX(CASE WHEN substr(bar_time,12,5)='15:15' THEN close END) c1515 "
        "FROM ohlc_1min WHERE substr(bar_time,12,5) >= '10:00' AND substr(bar_time,12,5) <= '15:15' "
        "AND symbol IN (%s) GROUP BY symbol, d" % ",".join("?"*len(fo)), list(fo)).fetchall()
    t = pd.DataFrame(rows, columns=["symbol", "date", "entry", "hi", "lo", "c1515"]).dropna(subset=["entry"])
    t["up_touch"] = (t["hi"]/t["entry"]-1)*100
    t["dn_touch"] = (t["lo"]/t["entry"]-1)*100
    t["close_move"] = (t["c1515"]/t["entry"]-1)*100
    return t


def run(con, fo):
    mf = morning_features(con, fo)[["symbol", "date", "m_volat", "m_vol", "m_ret"]]
    t = touch_frame(con, fo).merge(mf, on=["symbol", "date"], how="inner").dropna(subset=["m_volat"])
    print(f"rows={len(t)} dates={t['date'].nunique()} ({t['date'].min()}..{t['date'].max()})\n")

    # base rates (touch vs close)
    for X in (1, 2, 3):
        print(f"base rate TOUCH up>= {X}%: {(t['up_touch']>=X).mean()*100:4.1f}%   "
              f"down<=-{X}%: {(t['dn_touch']<=-X).mean()*100:4.1f}%   |   "
              f"CLOSE >= {X}%: {(t['close_move']>=X).mean()*100:4.1f}%")
    print()

    # volatility screen -> precision@5 on TOUCH (magnitude: either side touches X)
    def screen_p5(col, kind, Xs=(1, 2, 3)):
        out = {}
        for X in Xs:
            hits = []
            for dt, g in t.groupby("date"):
                g = g.dropna(subset=[col])
                if len(g) < 30: continue
                picks = g.sort_values(col, ascending=False).head(NPICK)
                if kind == "mag":
                    h = ((picks["up_touch"] >= X) | (picks["dn_touch"] <= -X)).sum()
                elif kind == "long":
                    h = (picks["up_touch"] >= X).sum()
                else:
                    h = (picks["dn_touch"] <= -X).sum()
                hits.append(h)
            out[X] = round(np.mean(hits), 2)
        return out

    print("VOLATILITY screen, precision@5 on TOUCH:")
    print("  magnitude (either side touches):", screen_p5("m_volat", "mag"))
    print("  long (high touches +X):         ", screen_p5("m_volat", "long"))
    print("  short (low touches -X):         ", screen_p5("m_volat", "short"))

    # ML model for up_touch>=1 and a magnitude touch model
    import lightgbm as lgb
    daily = pd.read_sql_query("SELECT symbol,trade_date,atr_20_pct,vol_ratio_20d FROM persona_signal_features "
                              "WHERE symbol IN (%s)" % ",".join("?"*len(fo)), con, params=fo).sort_values(["symbol","trade_date"])
    daily["date"] = daily.groupby("symbol")["trade_date"].shift(-1)
    t2 = t.merge(daily[["symbol","date","atr_20_pct","vol_ratio_20d"]], on=["symbol","date"], how="left")
    t2["abs_mret"] = t2["m_ret"].abs()
    feats = ["m_volat","m_vol","abs_mret","atr_20_pct","vol_ratio_20d"]
    def model_touch(kind, X):
        if kind=="mag": lab=((t2["up_touch"]>=X)|(t2["dn_touch"]<=-X)).astype(int).values
        elif kind=="long": lab=(t2["up_touch"]>=X).astype(int).values
        else: lab=(t2["dn_touch"]<=-X).astype(int).values
        Xm=t2[feats].replace([np.inf,-np.inf],np.nan); res=[]
        for tr_end,lo,hi in FOLDS:
            tr=(t2["date"]<tr_end).values; te=((t2["date"]>=lo)&(t2["date"]<=hi)).values
            if tr.sum()<3000 or te.sum()<300: continue
            m=lgb.LGBMClassifier(n_estimators=350,max_depth=6,learning_rate=0.04,subsample=0.8,
                                 colsample_bytree=0.8,n_jobs=8,verbose=-1,random_state=0)
            m.fit(Xm[tr],lab[tr])
            d=t2.loc[te,["date","up_touch","dn_touch"]].copy(); d["p"]=m.predict_proba(Xm[te])[:,1]
            hh=[]
            for dt,g in d.groupby("date"):
                pk=g.sort_values("p",ascending=False).head(NPICK)
                if kind=="mag": hh.append(((pk["up_touch"]>=X)|(pk["dn_touch"]<=-X)).sum())
                elif kind=="long": hh.append((pk["up_touch"]>=X).sum())
                else: hh.append((pk["dn_touch"]<=-X).sum())
            res.append(np.mean(hh))
        return round(np.mean(res),2) if res else None
    print("\nML model precision@5 on TOUCH:")
    print(f"  magnitude @1%={model_touch('mag',1)}  @2%={model_touch('mag',2)}  @3%={model_touch('mag',3)}")
    print(f"  long      @1%={model_touch('long',1)}  @2%={model_touch('long',2)}")
    print(f"  short     @1%={model_touch('short',1)}  @2%={model_touch('short',2)}")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("MFE_DONE")
