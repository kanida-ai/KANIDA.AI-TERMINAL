"""
Multi-indicator + pattern directional model (operator: use historical patterns +
multiple indicators, stocks only). Adds a full classic TA suite (point-in-time, from
prior close) + candlestick patterns to the morning microstructure + market/sector
regime, then a 4-model stacked ensemble. Directional +-1% touch, top-5, walk-forward.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features
from persona_engine.touch_tradelog import touch_window
from persona_engine.combined_model import stack

NPICK = 5
FOLDS = [("2025-07-01", "2025-07-01", "2025-12-31"), ("2026-01-01", "2026-01-01", "2099")]


def _ema(s, n): return s.ewm(span=n, adjust=False).mean()


def daily_indicators(con, fo):
    df = pd.read_sql_query(
        "SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
        "WHERE symbol IN (%s) AND trade_date>='2023-06-01' AND quality_flag!='rejected'"
        % ",".join("?"*len(fo)), con, params=fo).sort_values(["symbol", "trade_date"])
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.copy(); c, h, l, o, v = g["close"], g["high"], g["low"], g["open"], g["volume"]
        d = pd.DataFrame(index=g.index); d["symbol"] = sym; d["trade_date"] = g["trade_date"]
        delta = c.diff(); up = delta.clip(lower=0).rolling(14).mean(); dn = (-delta.clip(upper=0)).rolling(14).mean()
        d["rsi14"] = 100-100/(1+up/dn.replace(0, np.nan))
        macd = _ema(c, 12)-_ema(c, 26); d["macd_hist"] = macd-_ema(macd, 9)
        sma20 = c.rolling(20).mean(); sd20 = c.rolling(20).std()
        d["bb_pctb"] = (c-(sma20-2*sd20))/(4*sd20).replace(0, np.nan); d["bb_bw"] = (4*sd20)/sma20
        hh14, ll14 = h.rolling(14).max(), l.rolling(14).min()
        d["stoch_k"] = (c-ll14)/(hh14-ll14).replace(0, np.nan)*100
        d["williams_r"] = -100*(hh14-c)/(hh14-ll14).replace(0, np.nan)
        tp = (h+l+c)/3; d["cci"] = (tp-tp.rolling(20).mean())/(0.015*tp.rolling(20).apply(lambda x: np.abs(x-x.mean()).mean(), raw=True))
        pc = c.shift(1); tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
        d["atr14_pct"] = tr.rolling(14).mean()/c*100
        # ADX
        upm = h.diff(); dnm = -l.diff()
        plus = np.where((upm > dnm) & (upm > 0), upm, 0.0); minus = np.where((dnm > upm) & (dnm > 0), dnm, 0.0)
        atr = tr.rolling(14).mean()
        pdi = 100*pd.Series(plus, index=g.index).rolling(14).mean()/atr.replace(0, np.nan)
        mdi = 100*pd.Series(minus, index=g.index).rolling(14).mean()/atr.replace(0, np.nan)
        dx = 100*(pdi-mdi).abs()/(pdi+mdi).replace(0, np.nan); d["adx14"] = dx.rolling(14).mean()
        d["di_diff"] = pdi-mdi
        obv = (np.sign(delta).fillna(0)*v).cumsum(); d["obv_slope"] = (obv-obv.shift(10))/(v.rolling(20).mean()*10).replace(0, np.nan)
        d["ema_cross"] = (_ema(c, 5)-_ema(c, 20))/c*100
        d["roc10"] = c.pct_change(10)*100
        mf_tp = tp*v; pos = mf_tp.where(tp > tp.shift(1), 0).rolling(14).sum(); neg = mf_tp.where(tp < tp.shift(1), 0).rolling(14).sum()
        d["mfi14"] = 100-100/(1+pos/neg.replace(0, np.nan))
        d["dist_sma50"] = (c/c.rolling(50).mean()-1)*100
        # candlestick
        rng = (h-l).replace(0, np.nan)
        d["body"] = (c-o)/o*100; d["upwick"] = (h-np.maximum(o, c))/rng; d["lowick"] = (np.minimum(o, c)-l)/rng
        d["gap"] = (o/pc-1)*100
        out.append(d)
    full = pd.concat(out)
    full = full.sort_values(["symbol", "trade_date"])
    full["date"] = full.groupby("symbol")["trade_date"].shift(-1)   # known at next day's open
    return full


def build(con, fo):
    mf = morning_features(con, fo)
    sect = {r["symbol"]: (r["sector"] or "UNK") for r in con.execute("SELECT symbol,sector FROM falcon_sectors")}
    mf["sector"] = mf["symbol"].map(sect).fillna("UNK")
    g = mf.groupby("date")["m_ret"]
    mf["mkt_m_ret"] = g.transform("mean"); mf["mkt_breadth"] = g.transform(lambda s: (s > 0).mean()); mf["mkt_disp"] = g.transform("std")
    mf["rel_mkt"] = mf["m_ret"]-mf["mkt_m_ret"]; mf["mret_x_mkt"] = mf["m_ret"]*mf["mkt_m_ret"]
    ind = daily_indicators(con, fo)
    icols = [c for c in ind.columns if c not in ("symbol", "trade_date", "date")]
    mf = mf.merge(ind[["symbol", "date"]+icols], on=["symbol", "date"], how="left")
    tw = touch_window(con, fo)
    tw["up_touch"] = (tw["hi"]/tw["entry"]-1)*100; tw["dn_touch"] = (tw["lo"]/tw["entry"]-1)*100
    d = mf.merge(tw[["symbol", "date", "up_touch", "dn_touch"]], on=["symbol", "date"], how="inner")
    d = d.dropna(subset=["up_touch", "m_volat"]).reset_index(drop=True)
    mcols = ["m_ret", "m_range", "m_loc", "m_vol", "m_volat", "m_vwap_dev", "m_upbars", "m_last15",
             "mkt_m_ret", "mkt_breadth", "mkt_disp", "rel_mkt", "mret_x_mkt"]
    return d, mcols+icols


def run(con, fo):
    d, FEATS = build(con, fo)
    X = d[FEATS].replace([np.inf, -np.inf], np.nan)
    print(f"rows={len(d)} feats={len(FEATS)} (incl {len([f for f in FEATS if f in ('rsi14','macd_hist','adx14','cci','mfi14')])} TA core)\n")
    for direction in ("LONG", "SHORT"):
        col = "up_touch" if direction == "LONG" else "dn_touch"
        lab = ((d[col] >= 1) if direction == "LONG" else (d[col] <= -1)).astype(int).values
        hits = []
        for tr_end, lo, hi in FOLDS:
            tr = (d["date"] < tr_end).values; te = ((d["date"] >= lo) & (d["date"] <= hi)).values
            if tr.sum() < 3000 or te.sum() < 300: continue
            p = stack(X[tr], lab[tr], X[te])
            g = d.loc[te, ["date", col]].copy(); g["p"] = p
            for dt, gg in g.groupby("date"):
                pk = gg.sort_values("p", ascending=False).head(NPICK)
                hits.append((dt[:4], (pk[col] >= 1).sum() if direction == "LONG" else (pk[col] <= -1).sum()))
        df = pd.DataFrame(hits, columns=["yr", "h"])
        print(f"{direction} +/-1% directional (multi-indicator+pattern multi-ML): "
              f"p@5={df['h'].mean():.2f}/5  by year={df.groupby('yr')['h'].mean().round(2).to_dict()}")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("MULTIIND_DONE")
