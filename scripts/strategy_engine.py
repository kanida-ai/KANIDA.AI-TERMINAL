"""
KANIDA STRATEGY ENGINE — the brain behind the custom agent builder.
A user's strategy is a JSON DSL: any set of INDICATORS (with params) combined by AND/OR conditions,
a direction, and an exit rule. The engine computes the indicators point-in-time, evaluates the entry
signal leak-free, backtests across 13 years + the 5 Market Worlds, and returns an evidence card.
Because ANY strategy can be composed, results cannot be precomputed — they are backtested on demand,
so TOKEN COST scales with complexity (indicators x conditions x universe x exit x granularity).

Run: python scripts/strategy_engine.py     (demos 3 complex custom strategies)
"""
from __future__ import annotations
import sys, json
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
import agent_arena as AA           # reuse worlds() + _card()
COST_BPS_DEFAULT = 30

# ---------------- INDICATOR LIBRARY (each returns a date x symbol matrix, point-in-time at close t) ----------------
_F = {}
def F():
    if not _F:
        f, _ = DC.wide_all(); _F.update(o=f["o"], h=f["h"], l=f["l"], c=f["c"], v=f["v"])
    return _F

IND_META = {   # name -> (default params, human label)  — what the builder exposes
    "rsi": ({"period": 14}, "RSI"),
    "close_vs_sma": ({"period": 200}, "% above SMA"),
    "sma_slope": ({"period": 50}, "SMA slope %"),
    "ema_cross": ({"fast": 12, "slow": 26}, "EMA fast-slow %"),
    "macd_hist": ({}, "MACD histogram"),
    "gap": ({}, "Gap % from prev close"),
    "return_n": ({"period": 20}, "N-day return %"),
    "vol_ratio": ({"period": 20}, "Volume vs N-day avg"),
    "atr_pct": ({"period": 14}, "ATR % of price"),
    "bb_pctb": ({"period": 20}, "Bollinger %B"),
    "nd_high_dist": ({"period": 60}, "% from N-day high"),
    "streak_up": ({"period": 5}, "Up days in last N"),
    "rs_nifty": ({"period": 20}, "Relative strength vs NIFTY"),
}


def indicator(name, **p):
    f = F(); o, h, l, c, v = f["o"], f["h"], f["l"], f["c"], f["v"]
    dp = IND_META[name][0]; p = {**dp, **p}
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
        pc = c.shift(1); tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()]).groupby(level=0).max()
        tr = np.maximum(h - l, np.maximum((h - pc).abs(), (l - pc).abs()))
        return tr.rolling(p["period"]).mean() / c * 100
    if name == "bb_pctb":
        m = c.rolling(p["period"]).mean(); s = c.rolling(p["period"]).std()
        lo = m - 2 * s; hi = m + 2 * s; return (c - lo) / (hi - lo).replace(0, np.nan) * 100
    if name == "nd_high_dist": return (c / c.shift(1).rolling(p["period"]).max() - 1) * 100
    if name == "streak_up": return (c.diff() > 0).rolling(p["period"]).sum()
    if name == "rs_nifty":
        import sqlite3
        con = sqlite3.connect("file:" + str((ROOT / "db" / "kanida.db")).replace("\\", "/") + "?mode=ro", uri=True)
        nf = pd.read_sql_query("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time", con); con.close()
        nf.index = pd.to_datetime(nf["bar_time"].str[:10]); nf = nf["close"].reindex(c.index).ffill()
        nr = (nf / nf.shift(p["period"]) - 1)
        return (c / c.shift(p["period"]) - 1).sub(nr, axis=0) * 100
    raise ValueError(name)


OPS = {">": lambda x, y: x > y, "<": lambda x, y: x < y, ">=": lambda x, y: x >= y, "<=": lambda x, y: x <= y}


def entry_signal(strat):
    conds = strat["entry"]["conditions"]; logic = strat["entry"].get("logic", "AND")
    mats = []
    for cd in conds:
        ind = indicator(cd["indicator"], **cd.get("params", {}))
        mats.append(OPS[cd["op"]](ind, cd["value"]).fillna(False))
    if not mats: return None
    sig = mats[0]
    for m in mats[1:]:
        sig = (sig & m) if logic == "AND" else (sig | m)
    return sig


def backtest(strat, world_mask=None):
    f = F(); o, c = f["o"], f["c"]
    sig = entry_signal(strat)
    ex = strat.get("exit", {"type": "horizon", "days": 5})
    cost = strat.get("cost_bps", COST_BPS_DEFAULT) / 10000    # 30 bps = 0.30% round-trip
    if ex["type"] == "horizon":
        raw = c.shift(-ex["days"]) / o.shift(-1) - 1
        net = (raw if strat["direction"] == "long" else -raw) - cost
        return AA._card(sig.values, net.values, world_mask)
    # target/stop or trail -> per-event forward sim (bounded by max_days)
    return _card_pathexit(strat, sig, world_mask, cost)


def _card_pathexit(strat, sig, world_mask, cost):
    f = F(); o, h, l, c = f["o"].values, f["h"].values, f["l"].values, f["c"].values
    ex = strat["exit"]; long = strat["direction"] == "long"; md = ex.get("max_days", 20)
    tgt = ex.get("target", 999) / 100; stp = ex.get("stop", 999) / 100; trail = ex.get("pct", None)
    S = sig.values; nd, ns = S.shape; rets = []
    dmask = world_mask if world_mask is not None else np.ones(nd, bool)
    ev = np.argwhere(S & dmask[:, None])
    for t, s in ev:
        if t + 1 >= nd or not (o[t + 1, s] > 0): continue
        e = o[t + 1, s]; peak = e; r = None
        for d in range(t + 1, min(t + 1 + md, nd)):
            hi = h[d, s]; lo = l[d, s]
            if hi > peak: peak = hi
            if long:
                if trail is not None and lo <= peak * (1 - trail / 100): r = (peak * (1 - trail / 100)) / e - 1; break
                if hi >= e * (1 + tgt): r = tgt; break
                if lo <= e * (1 - stp): r = -stp; break
            else:
                if trail is not None and hi >= peak_low(e, peak, trail): pass
                if lo <= e * (1 - tgt): r = tgt; break
                if hi >= e * (1 + stp): r = -stp; break
        if r is None: r = (c[min(t + md, nd - 1), s] / e - 1) * (1 if long else -1)
        if np.isfinite(r): rets.append(r - cost)
    if len(rets) < 20: return None
    r = np.array(rets); wins = r[r > 0]; loss = r[r <= 0]
    return {"n": len(r), "win": round(float((r > 0).mean()) * 100, 1), "expct": round(float(r.mean()) * 100, 2),
            "med": round(float(np.median(r)) * 100, 2), "pf": round(float(wins.sum() / -loss.sum()), 2) if loss.sum() < 0 else None,
            "edge": None}


def peak_low(e, peak, trail):
    return e  # (short-trail placeholder; long path is the demo)


# ---------------- TOKEN METER: measured like Claude tokens = units of real compute processed ----------------
# 1 KANIDA token = 100,000 evaluated market-data cells (a stock-day passed through one operation),
# exactly analogous to an LLM token = a chunk of text processed. INPUT tokens = the size of the backtest
# job (universe x bars x operations); OUTPUT tokens = the results returned. Total is deducted from the wallet.
UNIT = 100_000
BARS = {"daily": 3361, "1min": 3361 * 375}       # 1-minute data has ~375x more bars -> ~375x the tokens


def token_cost(strat, granularity="daily", universe_n=441):
    import math
    conds = strat["entry"]["conditions"]
    n_cond = len(conds); n_ind = len(set(cd["indicator"] for cd in conds))
    exit_w = {"horizon": 1, "target_stop": 3, "trail": 3}.get(strat.get("exit", {}).get("type", "horizon"), 2)
    uni = universe_n if strat.get("universe", "all") == "all" else max(1, int(universe_n * 0.4))
    ops = n_ind + n_cond + exit_w                                    # operations applied to every cell
    input_tokens = math.ceil(uni * BARS[granularity] * ops / UNIT)
    worlds_tokens = math.ceil(input_tokens * 0.4)                    # 5 Market Worlds reuse cached indicators (+40%)
    output_tokens = 25                                              # evidence card + 5 world cards returned
    total = input_tokens + worlds_tokens + output_tokens
    return total, {"input": input_tokens, "market_worlds": worlds_tokens, "output": output_tokens,
                   "stocks": uni, "bars": BARS[granularity], "ops_per_cell": ops, "granularity": granularity}


class Wallet:
    """Prepaid token balance — buy a pack, spend on backtests, top up when low (like Claude credits)."""
    def __init__(self, balance): self.balance = balance
    def charge(self, tokens):
        if tokens > self.balance:
            return False
        self.balance -= tokens; return True
    def topup(self, tokens): self.balance += tokens


def run(strat):
    overall = backtest(strat)
    worlds = {w: backtest(strat, mask) for w, mask in AA.worlds().items()}
    tok, br = token_cost(strat)
    return overall, worlds, tok, br


# ---------------- DEMOS: genuinely complex custom strategies ----------------
DEMOS = [
    {"name": "OversoldTrend", "direction": "long",
     "entry": {"logic": "AND", "conditions": [
         {"indicator": "rsi", "params": {"period": 14}, "op": "<", "value": 30},
         {"indicator": "close_vs_sma", "params": {"period": 200}, "op": ">", "value": 0}]},
     "exit": {"type": "horizon", "days": 5}},
    {"name": "MacdVolMomentum", "direction": "long",
     "entry": {"logic": "AND", "conditions": [
         {"indicator": "macd_hist", "op": ">", "value": 0},
         {"indicator": "vol_ratio", "params": {"period": 20}, "op": ">", "value": 1.5},
         {"indicator": "return_n", "params": {"period": 20}, "op": ">", "value": 5},
         {"indicator": "rs_nifty", "params": {"period": 20}, "op": ">", "value": 0}]},
     "exit": {"type": "target_stop", "target": 10, "stop": 5, "max_days": 20}},
    {"name": "GapExhaustionFade", "direction": "short",
     "entry": {"logic": "AND", "conditions": [
         {"indicator": "gap", "op": ">", "value": 3},
         {"indicator": "rsi", "params": {"period": 14}, "op": ">", "value": 70},
         {"indicator": "bb_pctb", "params": {"period": 20}, "op": ">", "value": 95}]},
     "exit": {"type": "horizon", "days": 1}},
]


def main():
    import time
    wallet = Wallet(1000)                                            # user pre-purchased 1,000 tokens
    print(f"WALLET: pre-purchased balance = {wallet.balance} tokens\n")
    for s in DEMOS:
        t0 = time.time()
        overall, worlds, tok, br = run(s)
        dt = time.time() - t0
        ok = wallet.charge(tok)
        conds = " " + s["entry"]["logic"] + " ".join([""] + [f"[{c['indicator']}{c['op']}{c['value']}]" for c in s["entry"]["conditions"]])
        print(f"### {s['name']}  ({s['direction']}, exit={s['exit']['type']})")
        print(f"  strategy:{conds}")
        if overall:
            print(f"  RESULT: n={overall['n']} win={overall['win']}% exp={overall['expct']:+.2f}%/trade PF={overall['pf']}  [{dt:.1f}s]")
        print(f"  TOKENS: {tok}  = input {br['input']} + market-worlds {br['market_worlds']} + output {br['output']}"
              f"  ({br['stocks']} stocks × {br['bars']} bars × {br['ops_per_cell']} ops, {br['granularity']})")
        print(f"  WALLET: {'charged ' + str(tok) if ok else 'INSUFFICIENT — top up'} -> balance {wallet.balance}\n")
    # show the 1-minute price of the same first strategy
    tok1m, br1m = token_cost(DEMOS[0], granularity="1min")
    print(f"Same OversoldTrend on 1-MINUTE data would cost {tok1m:,} tokens (vs {token_cost(DEMOS[0])[0]} daily) — "
          f"~{round(tok1m/token_cost(DEMOS[0])[0])}x, because 1-min has ~375x more bars. Compute = cost, like Claude.")


if __name__ == "__main__":
    main()
