"""Barrier tuning.

Guessing a target/stop pair is a bad way to design a trade. On a 4.5%-range
stock a 0.5% stop is noise; on a 1.7%-range stock a 2.5% stop is never reached.
So sweep the geometry and let each symbol's own history pick.

Three structures are compared, all with entry at the open:

  bracket      target T, stop S, exit at close if neither is touched
  close_only   no stop at all, exit at the close
  hybrid       wide disaster stop only, otherwise exit at the close

The third one matters here. When a stock has high RETENTION -- it touches a
level and holds the direction into the close -- a tight target caps the winners
while the stop still cuts the losers. Exiting at the close instead can beat any
bracket. The retention column in the baseline report is what flags this.

Daily bars cannot order two touches, so bracket results use the conservative
reading and are a LOWER bound. `close_only` and `hybrid` have no such ambiguity
(only one barrier exists), which is a real advantage of those structures.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

COST = 0.0011   # round trip. Indian intraday equity, retail, all-in:
# brokerage ~0.03%/leg capped, STT 0.025% on the sell leg, exchange txn ~0.003%,
# GST 18% on those, stamp 0.003% buy side. ~11bps BEFORE slippage. Wide-spread
# names (a Rs 7 stock with a 1-paisa tick) will cost considerably more.


# --------------------------------------------------------------------------- #
# volatility                                                                    #
# --------------------------------------------------------------------------- #
def atr_pct(panel: pd.DataFrame, window: int = 20) -> pd.Series:
    """ATR as a fraction of price, causal: value on day D uses data through D."""
    d = panel.sort_values(["symbol", "date"])
    g = d.groupby("symbol", sort=False)
    prev_close = g["close"].shift(1)
    tr = pd.concat([
        d["high"] - d["low"],
        (d["high"] - prev_close).abs(),
        (d["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.groupby(d["symbol"], sort=False).transform(
        lambda s: s.rolling(window, min_periods=window // 2).mean())
    return (atr / d["close"]).reindex(panel.index)


def add_trade_frame(panel: pd.DataFrame, atr_window: int = 20) -> pd.DataFrame:
    """One row per symbol-day: the trade you could take at the NEXT open."""
    d = panel.sort_values(["symbol", "date"]).copy()
    d["atr_pct"] = atr_pct(d, atr_window)
    g = d.groupby("symbol", sort=False)
    entry = g["open"].shift(-1)
    d["t_entry"] = entry
    d["t_up"] = g["high"].shift(-1) / entry - 1.0      # best case long
    d["t_dn"] = g["low"].shift(-1) / entry - 1.0       # worst case long
    d["t_close"] = g["close"].shift(-1) / entry - 1.0
    # barriers are sized from ATR known at THIS close, applied to tomorrow
    return d[d["t_up"].notna() & d["atr_pct"].notna()]


# --------------------------------------------------------------------------- #
# the three structures                                                          #
# --------------------------------------------------------------------------- #
def _oriented(tf: pd.DataFrame, side: str):
    s = 1.0 if side == "long" else -1.0
    fav = tf["t_up"] * s if side == "long" else -tf["t_dn"]
    adv = tf["t_dn"] * s if side == "long" else -tf["t_up"]
    return fav, adv, tf["t_close"] * s


def eval_bracket(tf: pd.DataFrame, target, stop, side: str) -> dict:
    fav, adv, clo = _oriented(tf, side)
    hit = (fav >= target) & (adv > -stop)
    stopped = adv <= -stop
    timeout = ~hit & ~stopped
    pnl = np.where(hit, target, np.where(stopped, -stop, clo))
    return {"structure": "bracket", "p_target": float(hit.mean()),
            "p_stop": float(stopped.mean()), "p_timeout": float(timeout.mean()),
            "expectancy": float(np.mean(pnl) - COST)}


def eval_close_only(tf: pd.DataFrame, side: str) -> dict:
    _, _, clo = _oriented(tf, side)
    return {"structure": "close_only", "p_target": float((clo > 0).mean()),
            "p_stop": np.nan, "p_timeout": np.nan,
            "expectancy": float(clo.mean() - COST)}


def eval_hybrid(tf: pd.DataFrame, stop, side: str) -> dict:
    """Disaster stop only; otherwise ride to the close."""
    _, adv, clo = _oriented(tf, side)
    stopped = adv <= -stop
    pnl = np.where(stopped, -stop, clo)
    return {"structure": "hybrid", "p_target": float((~stopped & (clo > 0)).mean()),
            "p_stop": float(stopped.mean()), "p_timeout": np.nan,
            "expectancy": float(np.mean(pnl) - COST)}


# --------------------------------------------------------------------------- #
# the sweep                                                                     #
# --------------------------------------------------------------------------- #
def sweep(panel: pd.DataFrame, side: str = "long", atr_window: int = 20,
          verbose: bool = True, cost: float | None = None) -> pd.DataFrame:
    """Every structure x geometry, per symbol. Fixed AND ATR-scaled."""
    global COST
    if cost is not None:
        COST = cost
    tf = add_trade_frame(panel, atr_window)
    rows = []

    fixed_stops = [0.005, 0.0075, 0.010, 0.015, 0.020, 0.025, 0.030]
    ratios = [1.0, 1.5, 2.0]
    atr_stop_mults = [0.20, 0.30, 0.40, 0.50, 0.75]

    for sym, g in tf.groupby("symbol", sort=True):
        med_atr = float(g["atr_pct"].median())

        for s in fixed_stops:
            for r in ratios:
                rows.append({"symbol": sym, "side": side, "sizing": "fixed",
                             "stop": s, "target": s * r, "ratio": r, "n": len(g),
                             **eval_bracket(g, s * r, s, side)})
            rows.append({"symbol": sym, "side": side, "sizing": "fixed",
                         "stop": s, "target": np.nan, "ratio": np.nan, "n": len(g),
                         **eval_hybrid(g, s, side)})

        # ATR-scaled: every day gets its own barrier, sized on that day's vol
        for m in atr_stop_mults:
            stop_v = g["atr_pct"] * m
            for r in ratios:
                tgt_v = stop_v * r
                fav, adv, clo = _oriented(g, side)
                hit = (fav >= tgt_v) & (adv > -stop_v)
                stopped = adv <= -stop_v
                pnl = np.where(hit, tgt_v, np.where(stopped, -stop_v, clo))
                rows.append({"symbol": sym, "side": side, "sizing": "atr",
                             "stop": m, "target": m * r, "ratio": r, "n": len(g),
                             "structure": "bracket", "p_target": float(hit.mean()),
                             "p_stop": float(stopped.mean()),
                             "p_timeout": float((~hit & ~stopped).mean()),
                             "expectancy": float(np.mean(pnl) - COST)})
            fav, adv, clo = _oriented(g, side)
            stopped = adv <= -(g["atr_pct"] * m)
            pnl = np.where(stopped, -(g["atr_pct"] * m), clo)
            rows.append({"symbol": sym, "side": side, "sizing": "atr",
                         "stop": m, "target": np.nan, "ratio": np.nan, "n": len(g),
                         "structure": "hybrid", "p_target": float((~stopped & (clo > 0)).mean()),
                         "p_stop": float(stopped.mean()), "p_timeout": np.nan,
                         "expectancy": float(np.mean(pnl) - COST)})

        rows.append({"symbol": sym, "side": side, "sizing": "none",
                     "stop": np.nan, "target": np.nan, "ratio": np.nan,
                     "n": len(g), "med_atr": med_atr, **eval_close_only(g, side)})

    return pd.DataFrame(rows)


def summarise(sw: pd.DataFrame, top: int = 12) -> pd.DataFrame:
    """Rank geometries by how they do across the WHOLE universe.

    Ranked on the median symbol, not the mean, and the worst symbol is shown
    beside it. A geometry that only works on one name is not a geometry.
    """
    key = ["side", "sizing", "structure", "stop", "target", "ratio"]
    agg = (sw.groupby(key, dropna=False)
             .agg(n_symbols=("symbol", "nunique"),
                  median_exp=("expectancy", "median"),
                  worst_exp=("expectancy", "min"),
                  best_exp=("expectancy", "max"),
                  median_p_target=("p_target", "median"),
                  median_p_stop=("p_stop", "median"))
             .reset_index())
    agg["n_positive"] = [
        int((sw[(sw["sizing"] == r["sizing"]) & (sw["structure"] == r["structure"]) &
                (sw["stop"].fillna(-1) == (r["stop"] if pd.notna(r["stop"]) else -1)) &
                (sw["ratio"].fillna(-1) == (r["ratio"] if pd.notna(r["ratio"]) else -1))]
             ["expectancy"] > 0).sum())
        for _, r in agg.iterrows()]
    return agg.sort_values("median_exp", ascending=False).head(top).reset_index(drop=True)


def run_tuning(panel: pd.DataFrame, out_dir: str = "outputs",
               cost: float | None = None) -> dict:
    import os
    os.makedirs(out_dir, exist_ok=True)
    res = {}
    for side in ("long", "short"):
        sw = sweep(panel, side, cost=cost)
        sw.to_csv(f"{out_dir}/barriers_{side}.csv", index=False)
        top = summarise(sw)
        res[side] = top
        print(f"\n{'=' * 78}\nBEST GEOMETRIES, {side.upper()}   "
              f"(median symbol, no signal, cost={COST*10000:.0f}bps)\n{'=' * 78}")
        cols = ["sizing", "structure", "stop", "target", "ratio", "median_p_target",
                "median_p_stop", "median_exp", "worst_exp", "n_positive"]
        print(top[cols].round(4).to_string(index=False))
        print("\n  n_positive = how many of the symbols had positive expectancy")
        print("  with NO signal at all. This is the null. A mined state has to")
        print("  beat it, so pick a geometry that is near break-even here --")
        print("  not one that is already profitable (that is usually a")
        print("  volatility artefact) nor one deeply negative (unfixable).")
    return res
