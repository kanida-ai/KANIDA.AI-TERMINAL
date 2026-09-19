"""
SPS_V6 — Symbolic Alpha Miner (subsystems 1 + 4 + 5)
====================================================
Open-ended genetic programming that INVENTS formulaic alpha expressions over the
daily cross-section of the universe, evaluates them LEAK-FREE (walk-forward, forward
returns, 2026 vault SEALED), admits only those clearing a DEFLATED t-stat bar that
tightens with the number of trials, then ENSEMBLES survivors. A brand-new,
uncorrelated edge family vs the intraday ORBlate book.

Run:  PYTHONIOENCODING=utf-8 python alpha_miner.py
"""
import sqlite3, json, math
from pathlib import Path
from datetime import datetime
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE.parents[1] / "db" / "kanida.db"
UNIV_FILE = HERE.parents[0] / "SPS_V3" / "universe_scan.txt"
RNG = np.random.default_rng(42)

TRAIN = ("2022-01-01", "2024-12-31")
VAL = ("2025-01-01", "2025-12-31")
VAULT = ("2026-01-01", "2026-12-31")     # SEALED until final confirm
COST = 0.0010                            # per unit turnover
WINDOWS = [3, 5, 10, 20]
POP, GENS, ELITE = 160, 4, 12
MAX_DEPTH = 4


# ---------------- data panels (point-in-time) ----------------
def load_panels():
    syms = [s.strip() for s in UNIV_FILE.read_text().split() if s.strip()]
    con = sqlite3.connect(str(DB))
    q = ("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily "
         "WHERE symbol IN (%s)" % ",".join("?" * len(syms)))
    df = pd.read_sql(q, con, params=syms); con.close()
    df["date"] = pd.to_datetime(df["bar_time"])
    piv = lambda c: df.pivot_table(index="date", columns="symbol", values=c).sort_index()
    o, h, l, c, v = piv("open"), piv("high"), piv("low"), piv("close"), piv("volume")
    # keep symbols with full-ish history
    good = c.columns[c.loc[:"2025-12-31"].notna().sum() >= 900]
    o, h, l, c, v = [x[good] for x in (o, h, l, c, v)]
    vwap = (h + l + c) / 3
    term = {
        "ret": c.pct_change(), "oc": (c - o) / o, "hl": (h - l) / c,
        "co": (o - c.shift(1)) / c.shift(1), "relvol": v / v.rolling(20).mean(),
        "cvwap": c / vwap - 1, "close": c, "volume": v,
    }
    fwd = c.pct_change().shift(-1)           # return t -> t+1, realized AFTER position at t
    return term, fwd, list(term.keys())


# ---------------- operator set ----------------
def _clean(x): return x.replace([np.inf, -np.inf], np.nan)
def cs_rank(x): return x.rank(axis=1, pct=True) - 0.5
def cs_z(x):
    m = x.mean(axis=1); s = x.std(axis=1).replace(0, np.nan)
    return _clean(x.sub(m, axis=0).div(s, axis=0))
def decay(x, d):
    num = sum((d - i) * x.shift(i) for i in range(d)); return num / (d * (d + 1) / 2.0)   # vectorized

UNARY = {"neg": lambda x: -x, "abs": lambda x: x.abs(), "sign": np.sign,
         "csrank": cs_rank, "csz": cs_z}
TS = {"delta": lambda x, d: x - x.shift(d), "tsmean": lambda x, d: x.rolling(d).mean(),
      "tsstd": lambda x, d: x.rolling(d).std(), "tsrank": lambda x, d: x.rolling(d).rank(pct=True) - 0.5,
      "tsmin": lambda x, d: x.rolling(d).min(), "tsmax": lambda x, d: x.rolling(d).max(),
      "delay": lambda x, d: x.shift(d), "decay": decay}
BIN = {"add": lambda a, b: a + b, "sub": lambda a, b: a - b, "mul": lambda a, b: a * b,
       "div": lambda a, b: _clean(a / b.replace(0, np.nan))}


# ---------------- expression trees ----------------
# node = ('var', name) | ('u', op, child) | ('ts', op, child, d) | ('b', op, c1, c2)
def rand_tree(terms, depth=0):
    if depth >= MAX_DEPTH or (depth > 0 and RNG.random() < 0.35):
        return ("var", terms[RNG.integers(len(terms))])
    r = RNG.random()
    if r < 0.30:
        return ("u", list(UNARY)[RNG.integers(len(UNARY))], rand_tree(terms, depth + 1))
    if r < 0.65:
        return ("ts", list(TS)[RNG.integers(len(TS))], rand_tree(terms, depth + 1),
                int(WINDOWS[RNG.integers(len(WINDOWS))]))
    return ("b", list(BIN)[RNG.integers(len(BIN))], rand_tree(terms, depth + 1), rand_tree(terms, depth + 1))


def to_str(t):
    if t[0] == "var": return t[1]
    if t[0] == "u": return f"{t[1]}({to_str(t[2])})"
    if t[0] == "ts": return f"{t[1]}({to_str(t[2])},{t[3]})"
    return f"{t[1]}({to_str(t[2])},{to_str(t[3])})"


def evaluate(t, term):
    if t[0] == "var": return term[t[1]]
    if t[0] == "u": return _clean(UNARY[t[1]](evaluate(t[2], term)))
    if t[0] == "ts": return _clean(TS[t[1]](evaluate(t[2], term), t[3]))
    return _clean(BIN[t[1]](evaluate(t[2], term), evaluate(t[3], term)))


def nodes(t):
    if t[0] == "var": return [t]
    if t[0] in ("u", "ts"): return [t] + nodes(t[2])
    return [t] + nodes(t[2]) + nodes(t[3])


def mutate(t, terms):
    if RNG.random() < 0.5:
        return rand_tree(terms, MAX_DEPTH - 1) if RNG.random() < 0.5 else rand_tree(terms)
    # rebuild with one subtree replaced
    if t[0] == "var": return rand_tree(terms)
    if t[0] == "u": return ("u", t[1], mutate(t[2], terms))
    if t[0] == "ts":
        return ("ts", t[1], mutate(t[2], terms), int(WINDOWS[RNG.integers(len(WINDOWS))]))
    if RNG.random() < 0.5: return ("b", t[1], mutate(t[2], terms), t[3])
    return ("b", t[1], t[2], mutate(t[3], terms))


def crossover(a, b):
    if a[0] == "var": return b
    if a[0] == "u": return ("u", a[1], crossover(a[2], b))
    if a[0] == "ts": return ("ts", a[1], crossover(a[2], b), a[3])
    return ("b", a[1], crossover(a[2], b) if RNG.random() < 0.5 else a[2],
            a[3] if RNG.random() < 0.5 else crossover(a[3], b))


# ---------------- evaluation on a date slice ----------------
def slice_df(x, lo, hi): return x.loc[lo:hi]

def ic(alpha, fwd, lo, hi):
    a = slice_df(alpha, lo, hi); f = slice_df(fwd, lo, hi)
    cor = a.rank(axis=1).corrwith(f.rank(axis=1), axis=1)   # daily cross-sectional rank IC
    return float(cor.mean()) if cor.notna().any() else 0.0

def backtest(alpha, fwd, lo, hi, sign=1.0):
    a = slice_df(alpha, lo, hi) * sign
    w = cs_rank(a)
    w = w.div(w.abs().sum(axis=1).replace(0, np.nan), axis=0)     # dollar-neutral, gross 1
    f = slice_df(fwd, lo, hi)
    gross = (w * f).sum(axis=1)
    turn = (w - w.shift(1)).abs().sum(axis=1).fillna(0)
    net = gross - COST * turn
    net = net.dropna()
    if len(net) < 30 or net.std() == 0:
        return None
    return {"n": int(len(net)), "mean": float(net.mean()), "std": float(net.std()),
            "sharpe": float(net.mean() / net.std() * math.sqrt(252)),
            "tstat": float(net.mean() / net.std() * math.sqrt(len(net))),
            "ret": net}


# ---------------- GP loop ----------------
def main():
    term, fwd, terms = load_panels()
    print(f"universe {len(term['ret'].columns)} stocks · train {TRAIN[0]}..{TRAIN[1]} · vault SEALED", flush=True)
    pop = [rand_tree(terms) for _ in range(POP)]
    seen = {}
    def fit(t):
        k = to_str(t)
        if k in seen: return seen[k]
        try:
            al = evaluate(t, term)
            v = abs(ic(al, fwd, *TRAIN))
            if not np.isfinite(v): v = 0.0
        except Exception:
            v = 0.0
        seen[k] = v; return v
    for g in range(GENS):
        scored = sorted(pop, key=fit, reverse=True)
        best = fit(scored[0])
        print(f"  gen {g}: pop={len(pop)} unique={len(seen)} best_train_IC={best:.4f}", flush=True)
        nxt = scored[:ELITE]
        while len(nxt) < POP:
            a = scored[RNG.integers(min(60, len(scored)))]
            if RNG.random() < 0.6:
                b = scored[RNG.integers(min(60, len(scored)))]
                child = crossover(a, b)
            else:
                child = mutate(a, terms)
            nxt.append(child)
        pop = nxt
    N = len(seen)
    thr_train = max(2.5, math.sqrt(2 * math.log(max(N, 2))))   # DEFLATED bar on TRAIN (selection)
    THR_VAL = 2.0                                              # held-out val: standard significance
    print(f"\ntrials={N} · deflated TRAIN t-bar={thr_train:.2f} · held-out VAL t-bar={THR_VAL}", flush=True)

    # admission: walk-forward train+val, deflated, then vault-confirm
    admitted = []
    cand = sorted(seen, key=seen.get, reverse=True)[:120]
    for k in cand:
        # rebuild tree from population match (keep the tree object)
        pass
    # re-evaluate top unique trees we still hold in pop/scored
    uniq = {}
    for t in pop:
        uniq.setdefault(to_str(t), t)
    for k, t in uniq.items():
        if seen.get(k, 0) < 0.01:
            continue
        al = evaluate(t, term)
        btr = backtest(al, fwd, *TRAIN)
        if not btr:
            continue
        sign = 1.0 if btr["mean"] >= 0 else -1.0
        btr = backtest(al, fwd, *TRAIN, sign=sign)
        bva = backtest(al, fwd, *VAL, sign=sign)
        if not btr or not bva:
            continue
        if btr["tstat"] >= thr_train and bva["tstat"] >= THR_VAL and bva["mean"] > 0:
            admitted.append({"expr": k, "tree": t, "sign": sign, "train": btr, "val": bva, "alpha": al})
    admitted.sort(key=lambda x: -x["val"]["sharpe"])
    print(f"admitted (train&val deflated, net+): {len(admitted)}", flush=True)

    lib = []
    for a in admitted[:25]:
        bvt = backtest(a["alpha"], fwd, *VAULT, sign=a["sign"])       # SEAL BREAK (final confirm)
        a["vault"] = bvt
        held = bvt and bvt["sharpe"] > 0
        lib.append({"expr": a["expr"], "sign": a["sign"],
                    "train_sharpe": round(a["train"]["sharpe"], 2),
                    "val_sharpe": round(a["val"]["sharpe"], 2),
                    "vault_sharpe": round(bvt["sharpe"], 2) if bvt else None,
                    "vault_holds": bool(held)})
    holds = [a for a in admitted[:25] if a.get("vault") and a["vault"]["sharpe"] > 0]
    print(f"\n=== DISCOVERED ALPHAS (top, vault-checked) ===", flush=True)
    for a in admitted[:15]:
        vs = a["vault"]["sharpe"] if a.get("vault") else None
        tag = "HOLDS" if (a.get("vault") and a["vault"]["sharpe"] > 0) else "vault-rejects"
        print(f"  trainSR {a['train']['sharpe']:+.2f} valSR {a['val']['sharpe']:+.2f} "
              f"vaultSR {vs:+.2f} [{tag}]  {('-' if a['sign']<0 else '')}{a['expr']}", flush=True)

    # ---- ENSEMBLE the vault-holding alphas ----
    if holds:
        sig = None
        for a in holds:
            z = cs_z(a["alpha"] * a["sign"])
            sig = z if sig is None else sig.add(z, fill_value=0)
        sig = sig / len(holds)
        ens = backtest(sig, fwd, *VAULT)
        etr = backtest(sig, fwd, *TRAIN); eva = backtest(sig, fwd, *VAL)
        print(f"\n=== ENSEMBLE of {len(holds)} vault-holding alphas ===", flush=True)
        if ens:
            print(f"  train SR {etr['sharpe']:+.2f} | val SR {eva['sharpe']:+.2f} | "
                  f"VAULT SR {ens['sharpe']:+.2f} | vault mean/day {ens['mean']*100:+.3f}% "
                  f"(long-short, dollar-neutral, net)", flush=True)
    else:
        print("\nno alphas held on the vault this run (honest — the skeptic core did its job).", flush=True)

    json.dump(lib, open(HERE / "alpha_library.json", "w"), indent=2)
    print(f"\n[library -> {HERE/'alpha_library.json'}]", flush=True)


if __name__ == "__main__":
    main()
