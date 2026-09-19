"""
Strategy engine (self-contained) for the Agent Builder backend. A strategy is a JSON DSL:
  { entry:{logic:AND|OR, conditions:[{indicator,params,op,value}...]}, direction, exit:{...}, cost_bps }
Computes indicators point-in-time, evaluates the entry signal LEAK-FREE (enter next open), backtests
across full history + 5 Market Worlds, returns an evidence card. Token cost = real compute processed.
Reads market data via data.wide() (Parquet/S3/SQLite). No dependency on the research repo.
"""
from __future__ import annotations
import math
import numpy as np, pandas as pd
from . import data

COST_BPS_DEFAULT = 30
UNIT = 100_000                         # 1 KANIDA token = 100k evaluated data-cells (like an LLM token = a text chunk)
BARS_MULT = {"daily": 1, "1min": 375}  # 1-min has ~375x more bars -> ~375x the tokens

IND_META = {
    "rsi": ({"period": 14}, "RSI"), "close_vs_sma": ({"period": 200}, "% above SMA"),
    "sma_slope": ({"period": 50}, "SMA slope %"), "ema_cross": ({"fast": 12, "slow": 26}, "EMA fast−slow %"),
    "macd_hist": ({}, "MACD histogram"), "gap": ({}, "Gap % from prev close"),
    "return_n": ({"period": 20}, "N-day return %"), "vol_ratio": ({"period": 20}, "Volume vs N-day avg"),
    "atr_pct": ({"period": 14}, "ATR % of price"), "bb_pctb": ({"period": 20}, "Bollinger %B"),
    "nd_high_dist": ({"period": 60}, "% from N-day high"), "streak_up": ({"period": 5}, "Up days in last N"),
    "rs_nifty": ({"period": 20}, "Relative strength vs NIFTY"),
}
OPS = {">": lambda x, y: x > y, "<": lambda x, y: x < y, ">=": lambda x, y: x >= y, "<=": lambda x, y: x <= y}


def indicator(name, **p):
    w = data.wide(); o, h, l, c, v = w["o"], w["h"], w["l"], w["c"], w["v"]
    p = {**IND_META[name][0], **p}
    if name == "rsi":
        d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / p["period"], adjust=False).mean()
        dn = (-d.clip(upper=0)).ewm(alpha=1 / p["period"], adjust=False).mean()
        return 100 - 100 / (1 + up / dn.replace(0, np.nan))
    if name == "close_vs_sma": return (c / c.rolling(p["period"]).mean() - 1) * 100
    if name == "sma_slope":
        s = c.rolling(p["period"]).mean(); return (s / s.shift(5) - 1) * 100
    if name == "ema_cross":
        return (c.ewm(span=p["fast"], adjust=False).mean() / c.ewm(span=p["slow"], adjust=False).mean() - 1) * 100
    if name == "macd_hist":
        macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        return macd - macd.ewm(span=9, adjust=False).mean()
    if name == "gap": return (o / c.shift(1) - 1) * 100
    if name == "return_n": return (c / c.shift(p["period"]) - 1) * 100
    if name == "vol_ratio": return v / v.rolling(p["period"]).mean()
    if name == "atr_pct":
        pc = c.shift(1); tr = np.maximum(h - l, np.maximum((h - pc).abs(), (l - pc).abs()))
        return tr.rolling(p["period"]).mean() / c * 100
    if name == "bb_pctb":
        m = c.rolling(p["period"]).mean(); s = c.rolling(p["period"]).std()
        return (c - (m - 2 * s)) / ((m + 2 * s) - (m - 2 * s)).replace(0, np.nan) * 100
    if name == "nd_high_dist": return (c / c.shift(1).rolling(p["period"]).max() - 1) * 100
    if name == "streak_up": return (c.diff() > 0).rolling(p["period"]).sum()
    if name == "rs_nifty":
        nf = data.wide()["nifty"]; nr = nf / nf.shift(p["period"]) - 1
        return (c / c.shift(p["period"]) - 1).sub(nr, axis=0) * 100
    raise ValueError(name)


def worlds():
    w = data.wide(); c = w["c"]; nf = w["nifty"]; idx = c.index
    ma200 = nf.rolling(200).mean(); rv = nf.pct_change().rolling(20).std() * np.sqrt(252)
    mvol = w["v"].sum(axis=1)
    above = (nf > ma200); hv = rv > rv.quantile(0.75); lowliq = mvol < mvol.quantile(0.25)
    unseen = (idx >= "2018-01-01") & (idx < "2020-01-01")
    return {"Normal": (above.values & ~hv.values & ~unseen), "High-Vol": hv.values,
            "Bear": ~above.values, "Low-Liquidity": lowliq.values, "Unseen 18-19": unseen}


def _entry(strat):
    conds = strat["entry"]["conditions"]; logic = strat["entry"].get("logic", "AND")
    mats = [OPS[cd["op"]](indicator(cd["indicator"], **cd.get("params", {})), cd["value"]).fillna(False) for cd in conds]
    if not mats: return None
    sig = mats[0]
    for m in mats[1:]:
        sig = (sig & m) if logic == "AND" else (sig | m)
    return sig


def _card(rets, base=None):
    r = rets[np.isfinite(rets)]
    if len(r) < 20: return None
    wins = r[r > 0]; loss = r[r <= 0]
    card = {"n": int(len(r)), "win": round(float((r > 0).mean()) * 100, 1),
            "expct": round(float(r.mean()) * 100, 2), "med": round(float(np.median(r)) * 100, 2),
            "pf": round(float(wins.sum() / -loss.sum()), 2) if loss.sum() < 0 else None}
    if base is not None:
        b = base[np.isfinite(base)]
        card["edge"] = round(float(r.mean() - b.mean()) * 100, 2) if len(b) else None
    return card


def backtest(strat, world_mask=None):
    w = data.wide(); o, c = w["o"], w["c"]
    sig = _entry(strat); ex = strat.get("exit", {"type": "horizon", "days": 5})
    cost = strat.get("cost_bps", COST_BPS_DEFAULT) / 10000
    long = strat["direction"] == "long"
    if ex["type"] == "horizon":
        raw = c.shift(-ex["days"]) / o.shift(-1) - 1
        net = ((raw if long else -raw) - cost).values
        m = sig.values & np.isfinite(net)
        if world_mask is not None: m = m & world_mask[:, None]
        base = net[(np.isfinite(net)) & (world_mask[:, None] if world_mask is not None else True)]
        return _card(net[m], base)
    return _path_card(strat, sig, world_mask, cost, long)


def _path_card(strat, sig, world_mask, cost, long):
    w = data.wide(); o, h, l, c = w["o"].values, w["h"].values, w["l"].values, w["c"].values
    ex = strat["exit"]; md = ex.get("max_days", 20)
    tgt = ex.get("target", 999) / 100; stp = ex.get("stop", 999) / 100; trail = ex.get("pct", None)
    S = sig.values; nd = S.shape[0]; dmask = world_mask if world_mask is not None else np.ones(nd, bool)
    rets = []
    for t, s in np.argwhere(S & dmask[:, None]):
        if t + 1 >= nd or not (o[t + 1, s] > 0): continue
        e = o[t + 1, s]; peak = e; r = None
        for d in range(t + 1, min(t + 1 + md, nd)):
            hi, lo = h[d, s], l[d, s]
            if hi > peak: peak = hi
            if long:
                if trail is not None and lo <= peak * (1 - trail / 100): r = peak * (1 - trail / 100) / e - 1; break
                if hi >= e * (1 + tgt): r = tgt; break
                if lo <= e * (1 - stp): r = -stp; break
            else:
                if lo <= e * (1 - tgt): r = tgt; break
                if hi >= e * (1 + stp): r = -stp; break
        if r is None: r = (c[min(t + md, nd - 1), s] / e - 1) * (1 if long else -1)
        if np.isfinite(r): rets.append(r - cost)
    return _card(np.array(rets)) if rets else None


def token_cost(strat, granularity="daily"):
    conds = strat["entry"]["conditions"]
    n_cond = len(conds); n_ind = len(set(cd["indicator"] for cd in conds))
    exit_w = {"horizon": 1, "target_stop": 3, "trail": 3}.get(strat.get("exit", {}).get("type", "horizon"), 2)
    uni = data.n_symbols(); bars = data.n_bars() * BARS_MULT.get(granularity, 1)
    ops = n_ind + n_cond + exit_w
    inp = math.ceil(uni * bars * ops / UNIT)
    worlds_t = math.ceil(inp * 0.4); out = 25
    return {"input": inp, "market_worlds": worlds_t, "output": out, "total": inp + worlds_t + out,
            "stocks": uni, "bars": bars, "ops_per_cell": ops, "granularity": granularity}


def run(strat, granularity="daily"):
    overall = backtest(strat)
    wmap = {name: backtest(strat, mask) for name, mask in worlds().items()}
    return {"strategy": strat, "overall": overall, "market_worlds": wmap,
            "tokens": token_cost(strat, granularity)}
