"""
DAILY-resolution multi-fold persistence pre-check. Runs SAFELY alongside the 1-min backfill:
reads the (already-complete) daily history ONCE into memory, then computes off that copy — no
ongoing contention with the fetch, no Kite calls, single-process (CPU is free).

Answers the core question with ~9 folds (2018..2026) instead of 1:
  (A) Stock-level: does a stock's year Y-1 ROC predict year Y? (expect ~0, as before, now across a decade)
  (B) Setup-level: is each (direction x hold) setup CONSISTENTLY positive across the folds? (the
      actionable persistence signal — long AND short, even-handed)

Per fold Y: mine train<=Y-2 -> promote on Y-1 -> trade Y (daily sim: enter next open, exit on
target touch via daily H/L within window else time-stop). Fixed Rs1L/trade, small cost.
Shorts' P&L is the SIGNAL quality (live execution needs futures; noted).

Run: PYTHONIOENCODING=utf-8 python daily_persistence.py   (log: db/daily_persistence.out)
"""
import os, sys
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"; os.environ["MKL_NUM_THREADS"] = "1"
import sqlite3, json, multiprocessing as mp
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\daily_persistence.out")
REP = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports")
CAP = 100_000.0
COST = 0.10
FOLDS = list(range(2018, 2027))                      # test years
TARGETS = [("up", 2, 2), ("up", 3, 3), ("up", 5, 5), ("dn", 2, 2), ("dn", 3, 3), ("dn", 5, 5)]
MIN_TR, LIFT_TR, MIN_VA, LIFT_VA = 40, 5.0, 12, 2.0


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(OUT, "a", encoding="utf-8") as f: f.write(line + "\n")


def features(d, mk):
    O, H, L, C, V = d["open"], d["high"], d["low"], d["close"], d["volume"]
    pc = C.shift(1); tr = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    F = pd.DataFrame(index=C.index)
    F["ret1"] = C.pct_change() * 100
    F["ret5"] = C.pct_change(5) * 100
    F["ret20"] = C.pct_change(20) * 100
    F["atr20"] = (tr.rolling(20).mean() / C * 100)
    F["gap"] = (O / pc - 1) * 100
    F["c_vs_ma20"] = (C / C.rolling(20).mean() - 1) * 100
    F["c_vs_ma50"] = (C / C.rolling(50).mean() - 1) * 100
    F["ma20_vs_ma50"] = (C.rolling(20).mean() / C.rolling(50).mean() - 1) * 100
    F["dist_hi20"] = (C / H.rolling(20).max() - 1) * 100
    F["dist_lo20"] = (C / L.rolling(20).min() - 1) * 100
    F["vol_ratio"] = V / V.rolling(20).mean()
    up = (C.diff().clip(lower=0)).rolling(14).mean(); dn = (-C.diff().clip(upper=0)).rolling(14).mean()
    F["rsi14"] = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    F["rng5"] = ((H.rolling(5).max() - L.rolling(5).min()) / C) * 100
    rm = mk.reindex(C.index).ffill()
    F["mkt_ret5"] = rm.pct_change(5) * 100
    F["_o"] = O; F["_h"] = H; F["_l"] = L; F["_c"] = C; F["year"] = C.index.year
    return F


def label(F, d, p, w):
    C, H, L = F["_c"], F["_h"], F["_l"]
    fh = H.shift(-1).rolling(w).max().shift(-(w - 1)) if w > 1 else H.shift(-1)
    fl = L.shift(-1).rolling(w).min().shift(-(w - 1)) if w > 1 else L.shift(-1)
    return ((fh / C - 1) * 100 >= p).astype(float) if d == "up" else ((fl / C - 1) * 100 <= -p).astype(float)


def leaf_rules(rf, feats):
    out = []
    for est in rf.estimators_:
        t = est.tree_
        def rec(n, conds):
            if t.feature[n] != -2:
                f = feats[t.feature[n]]; thr = t.threshold[n]
                rec(t.children_left[n], conds + [(f, "<=", thr)])
                rec(t.children_right[n], conds + [(f, ">", thr)])
            else:
                if conds: out.append(conds)
        rec(0, [])
    return out


def apply_rule(df, conds):
    m = pd.Series(True, index=df.index)
    for f, op, thr in conds:
        m &= (df[f] <= thr) if op == "<=" else (df[f] > thr)
    return m.fillna(False)


def sim_trade(F, idx_positions, d, p, w):
    """Daily sim from signal rows -> list of net_roc% per trade (enter next open, target/time exit)."""
    O, H, L, C = F["_o"], F["_h"], F["_l"], F["_c"]
    idx = list(F.index); pos = {t: i for i, t in enumerate(idx)}
    long = (d == "up"); rets = []
    for t in idx_positions:
        i = pos[t]
        if i + 1 >= len(idx): continue
        entry = O.iloc[i + 1]
        if not np.isfinite(entry) or entry <= 0: continue
        tgt = entry * (1 + p / 100) if long else entry * (1 - p / 100)
        wdays = idx[i + 1:i + 1 + w]; ex = None
        for wd in wdays:
            hi = H.loc[wd]; lo = L.loc[wd]
            if long and hi >= tgt: ex = tgt; break
            if not long and lo <= tgt: ex = tgt; break
        if ex is None: ex = C.loc[wdays[-1]]
        g = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        rets.append(g - COST)
    return rets


def load_feats():
    kc = sqlite3.connect(KDB, timeout=180)
    fno = [r[0] for r in kc.execute("SELECT symbol FROM instrument_labels WHERE is_fno=1").fetchall()]
    cov = {r[0]: r[1][:10] for r in kc.execute("SELECT symbol,max(bar_time) FROM ohlc_daily GROUP BY symbol").fetchall()}
    stocks = sorted([s for s in fno if cov.get(s, "") >= "2026-07-25"])
    q = "SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol IN (%s)" % ",".join("?" * len(stocks))
    df = pd.read_sql(q, kc, params=stocks)
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", kc); kc.close()
    df["date"] = pd.to_datetime(df["bar_time"]); mk["date"] = pd.to_datetime(mk["bar_time"])
    MK = mk.set_index("date")["close"]
    feats = {}
    for s, g in df.groupby("symbol"):
        gg = g.set_index("date").sort_index()
        if len(gg) < 400: continue
        feats[s] = features(gg[["open", "high", "low", "close", "volume"]], MK)
    return feats


def run_fold(Y):
    """One fold, all stocks. Returns (stock_year_partial, setup_year_partial, counts)."""
    feats = load_feats()
    fcols = [c for c in next(iter(feats.values())).columns if not c.startswith("_") and c != "year"]
    sy = {}; su = {}; nmined = npromo = ntrades = 0
    for s, F in feats.items():
        X = F[fcols].replace([np.inf, -np.inf], np.nan)
        tr = F["year"] <= (Y - 2); va = F["year"] == (Y - 1); te = F["year"] == Y
        if te.sum() < 30 or tr.sum() < 300: continue
        stock_rets = []
        for d, p, w in TARGETS:
            y = label(F, d, p, w); valid = y.notna() & X.notna().all(axis=1)
            ytr = y[tr & valid]
            if len(ytr) < 150 or ytr.nunique() < 2: continue
            base_tr = ytr.mean() * 100
            rf = RandomForestClassifier(n_estimators=40, max_depth=3, min_samples_leaf=25, random_state=7, n_jobs=1)
            rf.fit(X[tr & valid], ytr)
            seen = set()
            for conds in leaf_rules(rf, fcols):
                key = tuple(sorted(conds))
                if key in seen: continue
                seen.add(key)
                mtr = apply_rule(F[tr & valid], conds); ntr = int(mtr.sum())
                if ntr < MIN_TR: continue
                ptr = y[tr & valid][mtr].mean() * 100
                if ptr - base_tr < LIFT_TR: continue
                nmined += 1
                yv = y[va & valid]; mva = apply_rule(F[va & valid], conds); nva = int(mva.sum())
                base_va = yv.mean() * 100 if len(yv) else 0
                if nva < MIN_VA: continue
                pva = yv[mva].mean() * 100 if nva else 0
                if pva - base_va < LIFT_VA or pva <= base_va: continue
                npromo += 1
                mte = apply_rule(F[te & valid], conds)
                sig = F[te & valid].index[mte.values]
                rets = sim_trade(F, sig, d, p, w)
                if rets:
                    stock_rets += rets
                    su.setdefault(f"{d}_{p}pct_{w}d", []).extend(rets); ntrades += len(rets)
        if stock_rets:
            sy[s] = float(np.sum(stock_rets))
    return (Y, sy, su, (nmined, npromo, ntrades))


def main():
    OUT.write_text("", encoding="utf-8")
    log(f"DAILY persistence | folds {FOLDS[0]}..{FOLDS[-1]} | parallel by fold (CPU; reads daily only, fetch-safe)")
    stock_year = {}; setup_year = {}
    nw = int(sys.argv[1]) if len(sys.argv) > 1 else 2      # low worker count to protect the concurrent fetch
    log(f"workers={nw} (kept low so the 1-min fetch keeps CPU/disk)")
    with mp.get_context("spawn").Pool(nw) as pool:
        for Y, sy, su, cnt in pool.map(run_fold, FOLDS):
            for s, v in sy.items(): stock_year[(s, Y)] = v
            for setup, rets in su.items(): setup_year[(setup, Y)] = rets
            log(f"  fold {Y}: mined {cnt[0]} promoted {cnt[1]} trades {cnt[2]}")

    # ---- (A) stock-level persistence across folds ----
    sy = pd.DataFrame([(s, Y, v) for (s, Y), v in stock_year.items()], columns=["stock", "year", "roc"])
    sy.to_csv(REP / "daily_persistence_stock_year.csv", index=False)
    pairs = []
    for Y in FOLDS[1:]:
        a = sy[sy.year == Y - 1].set_index("stock")["roc"]; b = sy[sy.year == Y].set_index("stock")["roc"]
        j = pd.concat([a, b], axis=1, keys=["prev", "cur"]).dropna()
        if len(j) > 20: pairs.append((Y, len(j), j["prev"].corr(j["cur"], method="spearman")))
    log("\n(A) STOCK-LEVEL persistence (Y-1 -> Y Spearman):")
    for Y, n, c in pairs: log(f"    {Y-1}->{Y}: n={n} spearman={c:+.3f}")
    if pairs: log(f"    MEAN spearman across folds = {np.mean([c for _,_,c in pairs]):+.3f}  (~0 => stock selection stays dead)")

    # ---- (B) setup-level persistence across folds ----
    rows = []
    for (setup, Y), rets in setup_year.items():
        rows.append((setup, Y, len(rets), float(np.mean(rets)), float(np.sum(rets))))
    sv = pd.DataFrame(rows, columns=["setup", "year", "n", "avg_ret", "sum_ret"])
    piv = sv.pivot_table(index="setup", columns="year", values="avg_ret")
    piv["folds_positive"] = (piv[FOLDS] > 0).sum(axis=1)
    piv["mean_avg_ret"] = piv[FOLDS].mean(axis=1)
    piv.to_csv(REP / "daily_persistence_setup.csv")
    log("\n(B) SETUP-LEVEL persistence — avg net ret% per trade, by fold year (long & short):")
    log(piv.round(2).to_string())
    log("\nDAILY PERSISTENCE PRE-CHECK COMPLETE")


if __name__ == "__main__":
    main()
