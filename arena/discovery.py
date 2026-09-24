"""STRATEGY-DISCOVERY ENGINE (Arena Addendum-A/B, corrected mandate: DISCOVER, don't imitate).
An agent equipped with a BROAD indicator library that INVENTS its own composite signal and evolves it to BEAT
Magnifier on the leaderboard — with NO Falcon dependence. Cross-sectional space first: each day the invented signal
ranks the Nifty-500 (using ONLY data through yesterday), goes long the top-N / short the bottom-N, enters at today's
open, exits at close (intraday). Genetic search over {which indicators, weights, direction, basket size, leverage}.
Anti-lookahead: all features shifted 1 day; execution at next open. Honest cost model per name; winner re-validated
on the full 1-min arena engine afterward. Train on 2024-2025, confirm OUT-OF-SAMPLE on 2026-H1. READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
RNG = np.random.RandomState(7)                       # fixed seed (Math.random-free determinism)
COST = 0.0012                                        # per-name round-trip: brokerage+STT+exch+GST+slippage (~12bps)
TRAIN0, TRAIN1, HOLD0, HOLD1 = "2024-03-01", "2025-12-31", "2026-01-01", "2026-06-30"

# ---------------- data ----------------
def load_daily():
    c = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = [r[0] for r in c.execute("SELECT DISTINCT o.symbol FROM ohlc_daily o JOIN universe_master u ON o.symbol=u.symbol "
                                    "WHERE u.in_nifty500=1 AND o.trade_date BETWEEN ? AND ?", ("2026-01-01", "2026-06-30"))]
    df = pd.read_sql_query("SELECT symbol, trade_date, open, high, low, close, volume FROM ohlc_daily "
                           "WHERE trade_date BETWEEN ? AND ? ORDER BY symbol, trade_date", c, params=("2024-01-01", HOLD1))
    c.close()
    df = df[df.symbol.isin(syms)]
    return {s: g.set_index("trade_date") for s, g in df.groupby("symbol") if len(g) > 120}

# ---------------- indicator library (the agent's "complete skills") ----------------
def indicators(g):
    o, h, l, c, v = g.open, g.high, g.low, g.close, g.volume
    r1 = c.pct_change(); sma20 = c.rolling(20).mean(); sma50 = c.rolling(50).mean()
    e12, e26 = c.ewm(span=12).mean(), c.ewm(span=26).mean(); macd = e12 - e26; sig = macd.ewm(span=9).mean()
    d = c.diff(); up = d.clip(lower=0).rolling(14).mean(); dn = (-d.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1); atr = tr.rolling(14).mean()
    bbm = sma20; bbs = c.rolling(20).std(); pctb = (c - (bbm - 2 * bbs)) / (4 * bbs).replace(0, np.nan)
    F = pd.DataFrame(index=g.index)
    F["mom5"] = c.pct_change(5); F["mom20"] = c.pct_change(20); F["accel"] = F["mom5"] - F["mom5"].shift(5)
    F["rsi"] = rsi; F["macdh"] = (macd - sig) / c; F["dsma20"] = c / sma20 - 1; F["dsma50"] = c / sma50 - 1
    F["pctb"] = pctb; F["atrn"] = atr / c; F["rvol"] = r1.rolling(20).std()
    F["volz"] = (v - v.rolling(20).mean()) / v.rolling(20).std().replace(0, np.nan)
    F["hi20"] = c / h.rolling(20).max(); F["lo20"] = c / l.rolling(20).min()
    return F.shift(1)                                # SHIFT: ranking on day t uses only data <= t-1 (anti-lookahead)

FEATS = ["mom5", "mom20", "accel", "rsi", "macdh", "dsma20", "dsma50", "pctb", "atrn", "rvol", "volz", "hi20", "lo20"]

def build_matrices(daily):
    feat = {s: indicators(g) for s, g in daily.items()}
    Z = {}
    for f in FEATS:
        M = pd.DataFrame({s: feat[s][f] for s in daily})
        Z[f] = M.sub(M.mean(axis=1), axis=0).div(M.std(axis=1).replace(0, np.nan), axis=0)   # cross-sectional z per day
    RET = pd.DataFrame({s: daily[s].close / daily[s].open - 1 for s in daily})                # intraday open->close
    return Z, RET

# ---------------- genome + vectorized backtest ----------------
def rand_genome():
    k = RNG.randint(2, 6); fs = list(RNG.choice(FEATS, k, replace=False))
    return dict(w={f: round(RNG.uniform(-1, 1), 2) for f in fs}, dir=RNG.choice(["long", "ls"]),
                N=int(RNG.choice([5, 10, 15, 20])), lev=int(RNG.choice([3, 5])))

def signal(gen, Z):
    s = None
    for f, w in gen["w"].items():
        s = w * Z[f] if s is None else s + w * Z[f]
    return s

def backtest(gen, Z, RET, d0, d1):
    sig = signal(gen, Z); idx = [d for d in sig.index if d0 <= d <= d1 and d in RET.index]
    sig, ret = sig.loc[idx], RET.loc[idx]
    rnk = sig.rank(axis=1, ascending=False); cnt = sig.notna().sum(axis=1)
    N = gen["N"]; lmask = rnk <= N; smask = rnk.gt(cnt - N, axis=0)
    Lret = (ret.where(lmask)).mean(axis=1); Sret = (ret.where(smask)).mean(axis=1)
    if gen["dir"] == "ls":
        day = 0.5 * (Lret - COST) + 0.5 * (-Sret - COST)
    else:
        day = Lret - COST
    day = (day * gen["lev"] * 100).dropna()
    if len(day) < 40: return None
    mret = day.groupby([t[:7] for t in day.index]).sum()
    dd = []
    for m, grp in day.groupby([t[:7] for t in day.index]):
        eq = (1 + grp / 100).cumprod(); dd.append(float(((eq.cummax() - eq) / eq.cummax() * 100).max()))
    avgdd = np.mean(dd) if dd else 99
    return dict(mean=mret.mean(), worst=mret.min(), pos=(mret > 0).mean() * 100, avgdd=avgdd,
                calmar=mret.mean() / max(avgdd, 1.0), monthly=mret)

FITFOLDS = [("2024-03-01", "2024-08-31"), ("2024-09-01", "2025-02-28"), ("2025-03-01", "2025-08-31"), ("2025-09-01", "2025-12-31")]
def fitness(gen, Z, RET):
    cs = []
    for a, b in FITFOLDS:
        r = backtest(gen, Z, RET, a, b)
        if not r: return -99
        cs.append(r["calmar"])
    cs = np.array(cs)
    return cs.mean() - 0.7 * cs.std() - (2.0 if (cs < 0).any() else 0)   # reward CONSISTENT edge across all regimes

# ---------------- evolutionary search ----------------
def crossover(a, b):
    w = {};
    for f in set(a["w"]) | set(b["w"]):
        if f in a["w"] and f in b["w"]: w[f] = round((a["w"][f] + b["w"][f]) / 2, 2)
        elif RNG.rand() < 0.5: w[f] = a["w"].get(f, b["w"].get(f))
    if not w: w = dict(a["w"])
    return dict(w=w, dir=RNG.choice([a["dir"], b["dir"]]), N=RNG.choice([a["N"], b["N"]]), lev=RNG.choice([a["lev"], b["lev"]]))

def mutate(gen):
    g = dict(w=dict(gen["w"]), dir=gen["dir"], N=gen["N"], lev=gen["lev"])
    roll = RNG.rand()
    if roll < 0.4 and g["w"]: f = RNG.choice(list(g["w"])); g["w"][f] = round(np.clip(g["w"][f] + RNG.uniform(-0.4, 0.4), -1, 1), 2)
    elif roll < 0.6: f = RNG.choice(FEATS); g["w"][f] = round(RNG.uniform(-1, 1), 2)
    elif roll < 0.75 and len(g["w"]) > 2: del g["w"][RNG.choice(list(g["w"]))]
    elif roll < 0.9: g["N"] = int(RNG.choice([5, 10, 15, 20]))
    else: g["dir"] = RNG.choice(["long", "ls"])
    return g

def evolve(Z, RET, pop=90, gens=16):
    P = [rand_genome() for _ in range(pop)]
    best = None
    for gen in range(gens):
        scored = sorted(((fitness(g, Z, RET), g) for g in P), key=lambda x: -x[0])
        best = scored[0]
        print(f"  gen {gen+1}/{gens}: best robust-fit {best[0]:.2f}  ({len(best[1]['w'])} feats, {best[1]['dir']}, N={best[1]['N']}, {best[1]['lev']}x)", flush=True)
        elite = [g for _, g in scored[:max(4, pop // 5)]]
        P = list(elite)
        while len(P) < pop:
            a, b = elite[RNG.randint(len(elite))], elite[RNG.randint(len(elite))]
            P.append(mutate(crossover(a, b)))
    return best[1]

def describe(gen):
    terms = sorted(gen["w"].items(), key=lambda x: -abs(x[1]))
    desc = " + ".join(f"{w:+.2f}*{f}" for f, w in terms)
    return f"signal = {desc}   |  {'LONG-SHORT' if gen['dir']=='ls' else 'LONG-only'}, basket {gen['N']}, {gen['lev']}x MIS, intraday (open->close)"

if __name__ == "__main__":
    print("loading daily bars (Nifty-500, 2024-2026) ...", flush=True)
    daily = load_daily(); Z, RET = build_matrices(daily)
    print(f"universe {len(daily)} names | features {len(FEATS)} | train {TRAIN0}..{TRAIN1}  holdout {HOLD0}..{HOLD1}", flush=True)
    print("\n[Discovery] evolving invented signals (train = 2024-2025) ...", flush=True)
    champ = evolve(Z, RET)
    tr = backtest(champ, Z, RET, TRAIN0, TRAIN1); ho = backtest(champ, Z, RET, HOLD0, HOLD1)
    print("\n" + "=" * 88); print("DISCOVERED STRATEGY (invented by the engine, NO Falcon)"); print("=" * 88)
    print("PRINCIPLE:", describe(champ))
    print(f"\nTRAIN 2024-2025:  mean {tr['mean']:+.1f}%/mo  worst {tr['worst']:+.1f}%  avgDD {tr['avgdd']:.1f}%  Calmar {tr['calmar']:.2f}  %pos {tr['pos']:.0f}")
    print(f"HOLDOUT 2026-H1:  mean {ho['mean']:+.1f}%/mo  worst {ho['worst']:+.1f}%  avgDD {ho['avgdd']:.1f}%  Calmar {ho['calmar']:.2f}  %pos {ho['pos']:.0f}   (OUT-OF-SAMPLE)")
    print("\nMONTHLY VIEW — HOLDOUT 2026 (out-of-sample):")
    for m, r in ho["monthly"].items(): print(f"  {m}   {r:+6.1f}%")
    print("\nBENCHMARK to beat: Magnifier +107%/mo (5x, Falcon+trail, 1-min) / SignalLab-hold +39%/mo (5x EOD).")
    print("NOTE: fast daily open->close cost model; the winner will be re-validated on the full 1-min arena engine next.")
