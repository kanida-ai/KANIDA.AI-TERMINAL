"""
SPS_V9 — Stock-Specific Pattern Agent (autonomous, walk-forward, point-in-time)
===============================================================================
Takes the 4,794 GENERIC market-state patterns and, per stock, autonomously:
  1. re-calibrates each generic threshold to THAT stock's own distribution
     (percentile-mapped on TRAIN data only — so "high ATR" means high *for this stock*),
  2. measures every pattern's forward 1/2/3-day outcome on TRAIN (occurrences, hit-rate,
     avg return, MFE/MAE, t-stat/confidence),
  3. LEARNS which patterns carry a real directional edge for this stock (selection),
  4. combines the firing patterns into a weighted directional vote (multi-pattern),
  5. FORWARD-TESTS on the next unseen year — expanding walk-forward: 2022->2023,
     2022-23->2024, 2022-24->2025, 2022-25->2026.
Strict point-in-time: features as-of close t; thresholds & selection from TRAIN only;
outcomes measured forward. Honest OOS reporting (vs base rate) — no in-sample fit.

Run: PYTHONIOENCODING=utf-8 python pattern_agent.py
"""
import sqlite3, json
from pathlib import Path
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
PATF = r"C:\Users\SPS\Downloads\States_Mined_Patterns_ALL _V1 -.xlsx"
STOCKS = ["ADANIENT", "CARTRADE"]
MIN_OCC = 20            # min train occurrences for a pattern to be considered
T_BAR = 2.5            # train t-stat bar for a pattern to be "learned" (per stock)


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
    rng = (H - L) / pc * 100
    F["n_sub_3_range_7d"] = (rng < 3).rolling(7).sum(); F["n_sub_2_5_range_7d"] = (rng < 2.5).rolling(7).sum()
    lv = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lv.rolling(7).sum(); F["n_sub_75v_20d"] = lv.rolling(20).sum()
    wk = C.index.to_period("W")
    F["weekly_close_loc"] = (C - L.groupby(wk).cummin()) / (H.groupby(wk).cummax() - L.groupby(wk).cummin()).replace(0, np.nan)
    F["weekly_range_pct"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
    Wc = C.resample("W-FRI").last(); wsma = Wc.rolling(20).mean().shift(1).reindex(C.index, method="ffill")
    F["weekly_close_vs_sma20"] = (C / wsma - 1) * 100
    return F, O, H, L, C


def stock_frame(F, O, H, L, C, s):
    df = pd.DataFrame({k: v[s] for k, v in F.items() if s in v})
    df["close"] = C[s]
    df["fwd1"] = C[s].shift(-1) / C[s] - 1
    df["fwd2"] = C[s].shift(-2) / C[s] - 1
    df["fwd3"] = C[s].shift(-3) / C[s] - 1
    df["year"] = df.index.year
    return df


def adapt_mask(df, conds, ref):
    """conds: list of [feat,op,val]; ref: TRAIN slice used to percentile-map each threshold to the stock."""
    m = pd.Series(True, index=df.index)
    for feat, op, val in conds:
        if feat not in df.columns:
            return None
        rv = ref[feat].dropna()
        if len(rv) < 30:
            return None
        q = float((rv <= val).mean())                 # percentile of generic threshold in THIS stock's train dist
        thr = float(np.nanquantile(rv, min(max(q, 0.01), 0.99)))   # stock-specific threshold
        x = df[feat]
        c = {"<=": x <= thr, ">=": x >= thr, "<": x < thr, ">": x > thr}[op]
        m = m & c.fillna(False)
    return m


def main():
    F, O, H, L, C = load_features()
    pat = pd.read_excel(PATF, sheet_name="All_Candidates_4794")
    pats = [(r["pattern_id"], json.loads(r["rule_json"])) for _, r in pat.iterrows()]
    folds = [([2022], 2023), ([2022, 2023], 2024), ([2022, 2023, 2024], 2025), ([2022, 2023, 2024, 2025], 2026)]

    for s in STOCKS:
        df = stock_frame(F, O, H, L, C, s)
        print(f"\n================  {s}  ================")
        print(f"{'train->test':16}{'learned':>9}{'sig days':>10}{'OOS hit%':>10}{'base%':>8}{'OOS avg%':>10}{'edge':>8}")
        for tr_years, te_year in folds:
            train = df[df.year.isin(tr_years)]; test = df[df.year == te_year]
            if len(train) < 150 or len(test) < 50:
                continue
            # LEARN: adapt each pattern to the stock (train-calibrated), keep those with a real train edge
            learned = []
            for pid, conds in pats:
                m = adapt_mask(train, conds, train)
                if m is None: continue
                sub = train.loc[m, "fwd1"].dropna()
                if len(sub) < MIN_OCC: continue
                t = sub.mean() / (sub.std() + 1e-12) * np.sqrt(len(sub))
                if abs(t) >= T_BAR:
                    learned.append((pid, conds, np.sign(sub.mean()), abs(t)))
            # FORWARD TEST: weighted directional vote on the unseen test year
            votes = pd.Series(0.0, index=test.index); wsum = pd.Series(0.0, index=test.index)
            for pid, conds, direction, strength in learned:
                m = adapt_mask(test, conds, train)                # thresholds calibrated on TRAIN (point-in-time)
                if m is None: continue
                votes[m] += direction * strength; wsum[m] += strength
            net = (votes / wsum.replace(0, np.nan))                # signed confidence in [-1,1]
            lbl = f"{tr_years[0]}-{tr_years[-1]}->{te_year}"
            def score(horizon, conf):
                sig = net.dropna(); sig = sig[sig.abs() >= conf]
                act = test[horizon].reindex(sig.index); valid = sig.index[~act.isna()]
                sig = sig[valid]; act = act[valid]
                if len(sig) == 0: return None
                dr = act * np.sign(sig)
                base = max((test[horizon] > 0).mean(), (test[horizon] < 0).mean()) * 100
                return len(sig), (dr > 0).mean() * 100, base, dr.mean() * 100
            r1 = score("fwd1", 0); r2 = score("fwd2", 0); r3 = score("fwd3", 0); rc = score("fwd1", 0.5)
            def fmt(r): return f"{r[1]:.0f}/{r[2]:.0f}%({r[3]:+.2f})" if r else "-"
            print(f"  {lbl:16} learned={len(learned):>3}  1d[{fmt(r1)}] 2d[{fmt(r2)}] 3d[{fmt(r3)}]  "
                  f"hi-conf({rc[0] if rc else 0}d)[{fmt(rc)}]")


if __name__ == "__main__":
    main()
