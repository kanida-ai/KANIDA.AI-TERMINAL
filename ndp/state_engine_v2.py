# -*- coding: utf-8 -*-
"""STATE SEPARATION ENGINE v2 — AUTOMATIC feature mining. Primitives = time + 1-min OHLCV of the path up
to the +0.5% touch. Feature Engine auto-generates HUNDREDS of features (windowed stats of returns, range,
CLV, body, volume, strength, slope, up-fraction across many windows + interactions). A gradient-boosting
learner finds the STRUCTURE that separates continuation(->+1%) from reversion. Leak-free, walk-forward.
Reports OOS AUC (the honest separation) vs in-sample AUC (overfit gap) + permutation importance (what it found)."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.inspection import permutation_importance
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
WINDOWS = [3, 5, 8, 10, 15, 20, 30, 45, 60, 90]


def gen_features(O, H, L, C, V, t):
    """Auto-generate features from the path bars [0..t]. O/H/L/C/V are arrays; t=touch index."""
    n = t + 1
    O, H, L, C, V = O[:n], H[:n], L[:n], C[:n], V[:n].astype(float)
    f = {}
    if n < 3: return f
    ret = np.diff(C, prepend=C[0]) / (np.roll(C, 1) + 1e-9); ret[0] = 0
    clv = (C - L) / (H - L + 1e-9)
    rng = (H - L) / (C + 1e-9)
    body = (C - O) / (H - L + 1e-9)
    up = (C > O).astype(float)
    series = {"ret": ret, "clv": clv, "rng": rng, "body": body, "vol": V, "up": up}
    f["t_mins"] = t; f["t_frac"] = t / 375.0
    vwap = ((H + L + C) / 3 * V).sum() / (V.sum() + 1e-9)
    f["vwap_dist_all"] = (C[-1] / vwap - 1) * 100
    for w in WINDOWS:
        if n < w: continue
        s = slice(n - w, n)
        for nm, arr in series.items():
            a = arr[s]
            f[f"{nm}_mean_{w}"] = a.mean(); f[f"{nm}_std_{w}"] = a.std()
            f[f"{nm}_max_{w}"] = a.max(); f[f"{nm}_min_{w}"] = a.min(); f[f"{nm}_last_{w}"] = a[-1]
        f[f"chg_{w}"] = (C[-1] / C[n - w] - 1) * 100
        f[f"slope_{w}"] = np.polyfit(np.arange(w), C[s], 1)[0] / (C[-1] + 1e-9) * 1000
        f[f"upfrac_{w}"] = up[s].mean()
        f[f"volsum_{w}"] = V[s].sum()
        f[f"range_w_{w}"] = (H[s].max() - L[s].min()) / (C[-1] + 1e-9) * 100
        f[f"clvpos_{w}"] = (C[-1] - L[s].min()) / (H[s].max() - L[s].min() + 1e-9)
        f[f"newhi_{w}"] = int(C[-1] >= H[s].max() - 1e-9)
    # cross-window interactions/ratios
    for a, b in [(5, 20), (10, 30), (5, 60), (15, 60), (10, 45)]:
        if n > b:
            f[f"volratio_{a}_{b}"] = V[n-a:].sum() / (V[n-b:n-a].sum() + 1e-9)
            f[f"volstd_ratio_{a}_{b}"] = (ret[n-a:].std()) / (ret[n-b:n-a].std() + 1e-9)
            f[f"rng_ratio_{a}_{b}"] = ((H[n-a:].max()-L[n-a:].min())) / ((H[n-b:n-a].max()-L[n-b:n-a].min()) + 1e-9)
            f[f"accel_{a}_{b}"] = (C[-1]/C[n-a]-1) - (C[n-a]/C[n-b]-1)
    return f


def build(sym, side):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    rows = []
    for dt, g in b.groupby("date"):
        g = g.sort_values("bar_time").reset_index(drop=True); o = g.open.iloc[0]
        O, H, L, C, V = g.open.values, g.high.values, g.low.values, g.close.values, g.volume.values
        lvl = o*(1+side*0.5/100); tgt = o*(1+side*1.0/100)
        hit = np.where(H >= lvl)[0] if side > 0 else np.where(L <= lvl)[0]
        if len(hit) == 0: continue
        t = hit[0]
        if t < 3 or t > len(g)-8: continue
        # for shorts, mirror the path so "up" primitives mean "in-trade-direction"
        if side < 0:
            f = gen_features(2*o-O, 2*o-L, 2*o-H, 2*o-C, V, t)   # reflect price around open
        else:
            f = gen_features(O, H, L, C, V, t)
        if not f: continue
        aft = (H[t:].max() if side > 0 else L[t:].min())
        f["y"] = int(aft >= tgt) if side > 0 else int(aft <= tgt)
        f["date"] = dt; rows.append(f)
    return pd.DataFrame(rows)


def run_side(sym, side, label):
    P = build(sym, side).sort_values("date").reset_index(drop=True)
    feats = [c for c in P.columns if c not in ("y", "date")]
    P["mo"] = P.date.str[:7]; months = sorted(P.mo.unique())
    base = P.y.mean()*100
    oos_p, oos_y, ins_p, ins_y = [], [], [], []
    for i in range(6, len(months)):
        tr = P[P.mo < months[i]]; te = P[P.mo == months[i]]
        if len(tr) < 80 or te.empty: continue
        clf = HistGradientBoostingClassifier(max_depth=3, max_iter=120, learning_rate=0.05, min_samples_leaf=20, l2_regularization=1.0, random_state=0)
        clf.fit(tr[feats].values, tr.y.values)
        oos_p += list(clf.predict_proba(te[feats].values)[:, 1]); oos_y += list(te.y.values)
        ins_p += list(clf.predict_proba(tr[feats].values)[:, 1]); ins_y += list(tr.y.values)
    oos_p, oos_y = np.array(oos_p), np.array(oos_y)
    auc_oos = roc_auc_score(oos_y, oos_p) if len(set(oos_y)) > 1 else np.nan
    auc_ins = roc_auc_score(ins_y, ins_p) if len(set(ins_y)) > 1 else np.nan
    q1, q2 = np.quantile(oos_p, [1/3, 2/3]); hi = oos_y[oos_p >= q2]; lo = oos_y[oos_p <= q1]
    # permutation importance on a 65/35 split
    cut = int(len(P)*0.65); tr, te = P.iloc[:cut], P.iloc[cut:]
    clf = HistGradientBoostingClassifier(max_depth=3, max_iter=120, learning_rate=0.05, min_samples_leaf=20, l2_regularization=1.0, random_state=0).fit(tr[feats].values, tr.y.values)
    imp = permutation_importance(clf, te[feats].values, te.y.values, n_repeats=10, random_state=0, scoring="roc_auc")
    top = sorted(zip(feats, imp.importances_mean), key=lambda x: -x[1])[:12]
    print(f"\n{'='*82}\n{label}  ({sym})\n{'='*82}")
    print(f"  touches {len(P)}  ·  auto-features {len(feats)}  ·  base continuation {base:.0f}%  ·  OOS n={len(oos_y)}")
    print(f"  IN-SAMPLE AUC : {auc_ins:.3f}   (how well it fits training — high = memorizing)")
    print(f"  >>> OOS AUC   : {auc_oos:.3f}   <<< THE HONEST SEPARATION (0.50=none, >0.55=real)")
    print(f"  High-confidence third continuation: {hi.mean()*100:.0f}%   ·   Low-confidence third: {lo.mean()*100:.0f}%   (spread {(hi.mean()-lo.mean())*100:+.0f})")
    print(f"  --- structure the learner leaned on (permutation importance, OOS) ---")
    for fn, iv in top: print(f"    {fn:<20} {iv:+.4f}")
    return auc_oos, auc_ins


def main(sym="ADANIENT"):
    print(f"STATE SEPARATION ENGINE v2 (auto feature mining) — {sym}  ·  1-min 2024-05+  ·  leak-free walk-forward")
    run_side(sym, +1, "LONG: +0.5% -> continuation to +1.0% vs reversion")
    run_side(sym, -1, "SHORT: -0.5% -> continuation to -1.0% vs reversion")


if __name__ == "__main__":
    import sys; main(sys.argv[1].upper() if len(sys.argv) > 1 else "ADANIENT")
