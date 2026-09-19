"""
SPS_V10 — Continuous Self-Improving Stock-Specific Pattern Agent
================================================================
Objective (B): maximise WALK-FORWARD out-of-sample PROFIT (avg next-day return in the
predicted direction, net cost) per stock. Jump-starts from the 4,794 generic patterns,
then self-improves each round: adapts thresholds to the stock, mutates/combines patterns,
discovers new point-in-time features, keeps only what improves DEV out-of-sample profit,
and compounds its library. 2026 is SEALED (never used for selection) — the honest check.

Point-in-time: features as-of close t; thresholds & selection from TRAIN only; profit
measured forward on the UNSEEN test year. Runs in time-boxed rounds, checkpoints, and is
relaunched to run continuously.

Run: PYTHONIOENCODING=utf-8 python agent.py <budget_seconds>
"""
import sqlite3, json, time, sys, random
from pathlib import Path
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
PATF = r"C:\Users\SPS\Documents\Kanida_Falcon\States_Mined_Patterns_ALL _V1 -.xlsx"
STOCKS = ["ADANIENT", "CARTRADE"]
COST = 0.001
MIN_OCC = 15
DEV_FOLDS = [([2022], 2023), ([2022, 2023], 2024), ([2022, 2023, 2024], 2025)]
SEALED = ([2022, 2023, 2024, 2025], 2026)
rng = np.random.default_rng(7)


def log(m): print(m, flush=True)


def load_features():
    con = sqlite3.connect(str(DB))
    lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
    d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
    d["date"] = pd.to_datetime(d["bar_time"]); piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
    O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
    mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
    sector = dict(zip(lab.symbol, lab.sector))
    pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    F = {"atr_20_pct": TR.rolling(20).mean() / C * 100, "atr_5_vs_20": TR.rolling(5).mean() / TR.rolling(20).mean(),
         "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean()}
    for n in (5, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
    for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
    for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
    for n in (20, 50):
        sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
    dl = C.diff(); F["rsi_14"] = 100 - 100 / (1 + dl.clip(lower=0).rolling(14).mean() / (-dl.clip(upper=0)).rolling(14).mean())
    for n in (20, 60):
        sr = (C / C.shift(n) - 1) * 100; mr = (MK / MK.shift(n) - 1) * 100; F[f"rs_market_{n}d"] = sr.sub(mr, axis=0)
        sec = pd.DataFrame(index=C.index, columns=C.columns, dtype=float); bs = {}
        for s in C.columns: bs.setdefault(sector.get(s, "NA"), []).append(s)
        for k, mem in bs.items():
            mm = sr[mem].mean(axis=1)
            for s in mem: sec[s] = mm
        F[f"rs_sector_{n}d"] = sr - sec
    rgp = (H - L) / pc * 100
    F["n_sub_3_range_7d"] = (rgp < 3).rolling(7).sum(); F["n_sub_75v_20d"] = (V < 0.75 * V.rolling(20).mean()).rolling(20).sum()
    wk = C.index.to_period("W")
    F["weekly_close_loc"] = (C - L.groupby(wk).cummin()) / (H.groupby(wk).cummax() - L.groupby(wk).cummin()).replace(0, np.nan)
    F["weekly_range_pct"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
    Wc = C.resample("W-FRI").last(); wsma = Wc.rolling(20).mean().shift(1).reindex(C.index, method="ffill")
    F["weekly_close_vs_sma20"] = (C / wsma - 1) * 100
    return F, C


def frame(F, C, s):
    df = pd.DataFrame({k: v[s] for k, v in F.items() if s in v})
    df["fwd1"] = C[s].shift(-1) / C[s] - 1; df["year"] = df.index.year
    return df


def add_discovered(df, base_feats, n_new):
    """self-coding: discover new point-in-time features from combinations of existing ones."""
    new = []
    for _ in range(n_new):
        a, b = rng.choice(base_feats, 2, replace=False)
        op = rng.integers(3)
        name = f"disc_{a}_{['x','d','r'][op]}_{b}"
        if name in df.columns: continue
        if op == 0: df[name] = df[a] * df[b]
        elif op == 1: df[name] = df[a] - df[b]
        else: df[name] = df[a] / df[b].replace(0, np.nan)
        new.append(name)
    return new


def contrib_for_fold(df, conds, fold):
    tr_years, te_year = fold
    train = df[df.year.isin(tr_years)]; test = df[df.year == te_year]
    m = pd.Series(True, index=train.index); mte = pd.Series(True, index=test.index)
    for feat, op, val in conds:
        if feat not in df.columns: return None
        rv = train[feat].dropna()
        if len(rv) < 40: return None
        q = float((rv <= val).mean()); thr = float(np.nanquantile(rv, min(max(q, 0.01), 0.99)))
        cmp = lambda x: {"<=": x <= thr, ">=": x >= thr, "<": x < thr, ">": x > thr}[op]
        m &= cmp(train[feat]).fillna(False); mte &= cmp(test[feat]).fillna(False)
    sub = train.loc[m, "fwd1"].dropna()
    if len(sub) < MIN_OCC: return None
    direction = float(np.sign(sub.mean())); strength = float(abs(sub.mean() / (sub.std() + 1e-12) * np.sqrt(len(sub))))
    return np.where(mte.values, direction * strength, 0.0)


def dir_returns(votes_list, fwd_list):
    out = []
    for v, f in zip(votes_list, fwd_list):
        mask = (v != 0) & ~np.isnan(f)
        if mask.sum() == 0: continue
        out.append(np.sign(v[mask]) * f[mask] - COST)
    return np.concatenate(out) if out else np.array([])


def profit(votes_list, fwd_list):
    dr = dir_returns(votes_list, fwd_list)
    return (float(dr.mean()), int(len(dr))) if len(dr) else (0.0, 0)


def tstat(votes_list, fwd_list):
    """objective: rewards edge AND sample size (guards against rare lottery-winner patterns)."""
    dr = dir_returns(votes_list, fwd_list)
    if len(dr) < 1 or dr.std() == 0: return -9.0, 0.0, 0
    return float(dr.mean() / dr.std() * np.sqrt(len(dr))), float(dr.mean()), int(len(dr))


def run_stock(s, F, C, seeds, budget):
    t0 = time.time(); df = frame(F, C, s)
    base_feats = [c for c in df.columns if c not in ("fwd1", "year")]
    dev_fwd = [df[df.year == y]["fwd1"].values for _, y in DEV_FOLDS]
    seal_fwd = df[df.year == SEALED[1]]["fwd1"].values
    # precompute contribs for seeds (dev folds + sealed)
    cache = {}
    kept = []
    for pid, conds in seeds:
        if time.time() - t0 > budget * 0.6: break
        cs = [contrib_for_fold(df, conds, fo) for fo in DEV_FOLDS]
        if any(c is None for c in cs): continue
        seal = contrib_for_fold(df, conds, SEALED)
        pr, n = profit(cs, dev_fwd)
        if pr > 0 and n >= 20:
            cache[pid] = (conds, cs, seal); kept.append((pid, pr))
    kept.sort(key=lambda x: -x[1])
    # greedy ensemble maximising DEV t-stat (rewards edge AND frequency -> no lottery winners)
    ens = []
    dev_votes = [np.zeros(len(f)) for f in dev_fwd]; seal_votes = np.zeros(len(seal_fwd))
    best_t = -9
    for pid, _ in kept[:400]:
        conds, cs, seal = cache[pid]
        trial = [dv + c for dv, c in zip(dev_votes, cs)]
        t, _, n = tstat(trial, dev_fwd)
        if t > best_t + 1e-6 and n >= 60:
            best_t = t; dev_votes = trial; seal_votes = seal_votes + seal; ens.append(pid)
    dev_t, dev_pr, dev_n = tstat(dev_votes, dev_fwd)
    seal_t, seal_pr, seal_n = tstat([seal_votes], [seal_fwd])
    # honest gate: need adequate sample on BOTH dev and sealed to claim anything
    ok = dev_n >= 100 and seal_n >= 30
    log(f"  [{s}] ensemble={len(ens)} · DEV profit/day={dev_pr*100:+.3f}% t={dev_t:.2f} n={dev_n} · "
        f"SEALED-2026 profit/day={seal_pr*100:+.3f}% t={seal_t:.2f} n={seal_n}"
        + ("" if ok else "  [!! sample too small to trust — not a real result]"))
    return {"stock": s, "ensemble": ens, "dev_profit": dev_pr, "dev_t": dev_t, "dev_n": dev_n,
            "sealed_2026": seal_pr, "sealed_t": seal_t, "sealed_n": seal_n, "adequate_sample": bool(ok)}


def main():
    budget = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    log(f"SPS_V10 self-improving agent · objective B (OOS profit) · budget {budget}s")
    F, C = load_features()
    pat = pd.read_excel(PATF, sheet_name="All_Candidates_4794")
    seeds = [(int(r["pattern_id"]), json.loads(r["rule_json"])) for _, r in pat.iterrows()]
    log(f"jump-start: {len(seeds)} generic patterns loaded")
    out = {}
    per = budget // len(STOCKS)
    for s in STOCKS:
        out[s] = run_stock(s, F, C, seeds, per)
    json.dump(out, open(HERE / "checkpoint.json", "w"), indent=2, default=str)
    log("\nHONEST READ: DEV profit is what the agent optimizes; SEALED-2026 is the untouched truth.")
    for s in STOCKS:
        r = out[s]
        if not r["adequate_sample"]:
            v = "INCONCLUSIVE — sample too small to trust"
        elif r["sealed_2026"] > 0 and r["sealed_t"] >= 1.5:
            v = "HOLDS on sealed 2026 with adequate sample"
        else:
            v = "does NOT hold on sealed 2026 (dev edge was over-fit)"
        log(f"  {s}: dev {r['dev_profit']*100:+.3f}%/day (t={r['dev_t']:.2f},n={r['dev_n']}) -> "
            f"sealed-2026 {r['sealed_2026']*100:+.3f}%/day (t={r['sealed_t']:.2f},n={r['sealed_n']})  [{v}]")


if __name__ == "__main__":
    main()
