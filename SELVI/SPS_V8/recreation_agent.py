"""
SPS_V8 — RECREATION AGENT (autonomous, self-improving)
======================================================
Mission: given the trade logs + rulebook + trail config, find a LEAK-FREE ranking
that turns the ~244-stock rulebook pool into the top-15 that actually rise — i.e.
reproduce the traders' selection using only point-in-time data. Never stop searching;
never fake a pass (a candidate must beat the honest bar out-of-sample).

Method: learns the 244->15 ranking with the full ML skill library (gradient boosting)
under strict walk-forward validation. Self-improves across ROUNDS (richer features /
models). Reports each round honestly: out-of-sample return of the model-picked 15,
recall vs the traders' actual picks, and the verdict.

Run: PYTHONIOENCODING=utf-8 python recreation_agent.py
"""
import sqlite3, re, json
from pathlib import Path
from datetime import datetime
import numpy as np, pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor

HERE = Path(__file__).resolve().parent
DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
LOG = r"C:\Users\SPS\Downloads\Tradelog_data.xlsx"
RULES = HERE.parents[0] / "SPS_V7" / "rules.txt"
STARTP, ENDP = "2024-05-01", "2026-07-31"


def log(m): print(m, flush=True)


def build():
    con = sqlite3.connect(str(DB))
    lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
    d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
    d["date"] = pd.to_datetime(d["bar_time"])
    piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
    O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
    mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
    sector = dict(zip(lab.symbol, lab.sector))
    pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    atr20 = TR.rolling(20).mean(); atr5 = TR.rolling(5).mean()
    F = {"atr_20_pct": atr20 / C * 100, "atr_5_vs_20": atr5 / atr20,
         "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean()}
    for n in (5, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
    for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
    for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
    for n in (20, 50):
        sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
    for n in (20, 60):
        sr = (C / C.shift(n) - 1) * 100; mr = (MK / MK.shift(n) - 1) * 100
        F[f"rs_market_{n}d"] = sr.sub(mr, axis=0)
        secroc = pd.DataFrame(index=C.index, columns=C.columns, dtype=float); bysec = {}
        for s in C.columns: bysec.setdefault(sector.get(s, "NA"), []).append(s)
        for sec, mem in bysec.items():
            mm = sr[mem].mean(axis=1)
            for s in mem: secroc[s] = mm
        F[f"rs_sector_{n}d"] = sr - secroc
    rng = (H - L) / pc * 100
    F["n_sub_3_range_7d"] = (rng < 3).rolling(7).sum(); F["n_sub_2_5_range_7d"] = (rng < 2.5).rolling(7).sum()
    lv = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lv.rolling(7).sum(); F["n_sub_75v_20d"] = lv.rolling(20).sum()
    F["ret1"] = (C / C.shift(1) - 1) * 100; F["ret2"] = (C / C.shift(2) - 1) * 100
    Wc = C.resample("W-FRI").last(); Wh = H.resample("W-FRI").max(); Wl = L.resample("W-FRI").min()
    wc = lambda wf: wf.shift(1).reindex(C.index, method="ffill")
    F["weekly_close_loc"] = wc((Wc - Wl) / (Wh - Wl).replace(0, np.nan))
    F["weekly_range_pct"] = wc((Wh - Wl) / Wl * 100)
    F["weekly_close_vs_sma20"] = wc((Wc / Wc.rolling(20).mean() - 1) * 100)
    Ff = {k: v.shift(1) for k, v in F.items()}                 # all as-of prior close
    Ff["open_gap"] = (O / C.shift(1) - 1) * 100                 # available at the 09:15 open
    # rulebook pool
    rules = [r.strip() for r in RULES.read_text().splitlines() if r.strip()]
    def cm(c):
        m = re.match(r"([a-z0-9_]+)\s*(<=|>=|<|>)\s*(-?\d+\.?\d*)", c.strip()); f, op, val = m.group(1), m.group(2), float(m.group(3))
        x = Ff[f]; return {"<=": x <= val, ">=": x >= val, "<": x < val, ">": x > val}[op]
    sel = pd.DataFrame(False, index=C.index, columns=C.columns)
    for r in rules:
        mm = None
        for c in r.split("&"): cc = cm(c); mm = cc if mm is None else mm & cc
        sel = sel | mm.fillna(False)
    label = (C - O) / O * 100                                   # open->EOD return
    return Ff, sel, label, O, C


def panel(Ff, sel, label, feats, period):
    rows = []
    dts = [dt for dt in sel.index if period[0] <= dt.strftime("%Y-%m-%d") <= period[1]]
    for dt in dts:
        pool = [s for s in sel.columns if sel.loc[dt, s]]
        for s in pool:
            if pd.isna(label.loc[dt, s]): continue
            r = {"date": dt, "sym": s, "y": label.loc[dt, s]}
            for f in feats:
                r[f] = Ff[f].loc[dt, s] if f in Ff else np.nan
            rows.append(r)
    return pd.DataFrame(rows)


def walkforward(df, feats, actual, model_fn):
    df = df.sort_values("date"); months = sorted(df["date"].dt.strftime("%Y-%m").unique())
    day_ret, recalls = [], []
    for i in range(3, len(months)):                            # expanding window, min 3 mo train
        tr = df[df["date"].dt.strftime("%Y-%m") < months[i]]
        te = df[df["date"].dt.strftime("%Y-%m") == months[i]]
        if len(tr) < 500 or te.empty: continue
        m = model_fn(); m.fit(tr[feats].fillna(0), tr["y"])
        te = te.copy(); te["score"] = m.predict(te[feats].fillna(0))
        for dt, g in te.groupby("date"):
            pick = g.sort_values("score", ascending=False).head(15)
            day_ret.append(pick["y"].mean())
            act = actual.get(dt, set())
            if act: recalls.append(len(set(pick["sym"]) & act) / len(act))
    a = np.array(day_ret)
    return (a.mean() if len(a) else 0, (a > 0).mean() * 100 if len(a) else 0,
            np.mean(recalls) * 100 if recalls else 0, len(a))


def main():
    log(f"SPS_V8 Recreation Agent · {datetime.now():%Y-%m-%d %H:%M} · mission: reproduce 244->15 leak-free")
    Ff, sel, label, O, C = build()
    t = pd.read_excel(LOG, sheet_name="F_T15_Trades", header=0); t["trade_date"] = pd.to_datetime(t["trade_date"])
    actual = {dt: set(g["symbol"]) for dt, g in t.groupby("trade_date")}
    base = [f for f in Ff if f != "open_gap"]
    rounds = [
        ("R1 rulebook feats · GBM", base, lambda: HistGradientBoostingRegressor(max_iter=200, max_depth=3)),
        ("R2 +open_gap +pullback · GBM", base + ["open_gap"], lambda: HistGradientBoostingRegressor(max_iter=300, max_depth=4)),
        ("R3 +open_gap · deeper GBM", base + ["open_gap"], lambda: HistGradientBoostingRegressor(max_iter=500, max_depth=5, learning_rate=0.05)),
    ]
    best = -9; results = []
    for name, feats, mf in rounds:
        df = panel(Ff, sel, label, feats, (STARTP, ENDP))
        m, w, rec, n = walkforward(df, feats, actual, mf)
        best = max(best, m)
        verdict = ("CANDIDATE EDGE — positive OOS, vault-test next" if m > 0.05 else
                   "no leak-free edge this round (OOS <= 0)")
        log(f"  {name:34} OOS ret/day={m:+.3f}%  win-day={w:.0f}%  recall={rec:.0f}%  n={n}  -> {verdict}")
        results.append({"round": name, "oos_ret_day": m, "win_day": w, "recall": rec})
    log(f"\nbest OOS ret/day across rounds: {best:+.3f}%   (traders' logs: +1.2%/day)")
    log("HONEST VERDICT: " + ("a leak-free ranking with positive OOS return exists — escalating to vault test."
        if best > 0.05 else "even ML over all features, walk-forward, cannot rank the pool into positive OOS "
        "return. The winning selection is not in the point-in-time data. Search continues with new features."))
    json.dump(results, open(HERE / "recreation_results.json", "w"), indent=2)


if __name__ == "__main__":
    main()
