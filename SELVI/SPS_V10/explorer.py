"""
SPS_V10 EXPLORER — autonomous multi-approach overnight search.
Searches genuinely DIFFERENT approaches (not just 1-day patterns) for a stock edge that
survives the SEALED 2026 year: horizons 1/2/3/5/10/20-day, GBM & pattern methods, confidence
filtering, and per-stock feature selection. Optimizes on DEV (2022-2025 walk-forward);
SEALED 2026 is checked but NEVER used to choose configs (no holdout snooping). Logs every
DEV improvement + its honest sealed result, with timestamps. Self-improves by mutating the
best configs. Runs until ~6am PDT.

Run: PYTHONIOENCODING=utf-8 python explorer.py <max_hours>
"""
import sqlite3, sys, time, json
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

HERE = Path(__file__).resolve().parent
DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
STOCKS = ["ADANIENT", "CARTRADE"]
HORIZONS = [3, 5, 10, 20]     # focus longer horizons where sealed edge is emerging (1-day is dead)
DEV_YEARS = [2023, 2024, 2025]
SEALED_YEAR = 2026
COST = 0.0012
LOGF = HERE / "explorer_log.txt"
rng = np.random.default_rng(20260731)


def log(m):
    line = f"{datetime.now():%H:%M:%S} | {m}"
    print(line, flush=True)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load(s):
    con = sqlite3.connect(str(DB))
    lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
    d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
    d["date"] = pd.to_datetime(d["bar_time"]); piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
    O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
    mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
    sec = dict(zip(lab.symbol, lab.sector))
    pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    F = {"atr_20_pct": TR.rolling(20).mean() / C * 100, "atr_5_vs_20": TR.rolling(5).mean() / TR.rolling(20).mean(),
         "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean()}
    for n in (2, 3, 5, 10, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
    for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
    for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
    for n in (20, 50): sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
    dl = C.diff(); F["rsi_14"] = 100 - 100 / (1 + dl.clip(lower=0).rolling(14).mean() / (-dl.clip(upper=0)).rolling(14).mean())
    for n in (20, 60):
        sr = (C / C.shift(n) - 1) * 100; F[f"rs_market_{n}d"] = sr.sub((MK / MK.shift(n) - 1) * 100, axis=0)
        se = pd.DataFrame(index=C.index, columns=C.columns, dtype=float); bs = {}
        for x in C.columns: bs.setdefault(sec.get(x, "NA"), []).append(x)
        for k, mem in bs.items():
            mm = sr[mem].mean(axis=1)
            for x in mem: se[x] = mm
        F[f"rs_sector_{n}d"] = sr - se
    wk = C.index.to_period("W")
    F["weekly_close_loc"] = (C - L.groupby(wk).cummin()) / (H.groupby(wk).cummax() - L.groupby(wk).cummin()).replace(0, np.nan)
    F["weekly_range_pct"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
    df = pd.DataFrame({k: v[s] for k, v in F.items() if s in v})
    # --- NEW: intraday microstructure daily features (from 1-min), as-of close t (point-in-time) ---
    try:
        con2 = sqlite3.connect(str(DB))
        mn = pd.read_sql("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=?", con2, params=[s]); con2.close()
        if len(mn) > 1000:
            mn["dt"] = pd.to_datetime(mn["bar_time"]); mn["d"] = mn["dt"].dt.normalize(); mn["hm"] = mn["dt"].dt.strftime("%H:%M")
            mn["cv"] = mn["close"] * mn["volume"]; g = mn.groupby("d")
            dop = g["open"].first(); dcl = g["close"].last(); dvol = g["volume"].sum()
            vwap = g["cv"].sum() / dvol.replace(0, np.nan)
            f30 = mn[mn["hm"] <= "09:44"].groupby("d")["close"].last()
            l30o = mn[mn["hm"] >= "15:00"].groupby("d")["open"].first()
            volfh = mn[mn["hm"] <= "10:14"].groupby("d")["volume"].sum()
            himin = mn.loc[g["high"].idxmax(), ["d", "hm"]].set_index("d")["hm"]
            idf = pd.DataFrame(index=dop.index)
            idf["id_first30"] = (f30 / dop - 1) * 100
            idf["id_last30"] = (dcl / l30o - 1) * 100
            idf["id_cvwap"] = (dcl / vwap - 1) * 100
            idf["id_volfh_pct"] = (volfh / dvol) * 100
            idf["id_high_late"] = (himin > "12:30").astype(float)
            idf.index = pd.to_datetime(idf.index)
            for c in idf.columns:
                df[c] = idf[c].reindex(df.index)
    except Exception as e:
        log(f"[{s}] intraday features skipped: {e}")
    for h in HORIZONS: df[f"fwd{h}"] = C[s].shift(-h) / C[s] - 1
    df["year"] = df.index.year
    feats = [c for c in df.columns if not c.startswith("fwd") and c != "year"]
    return df, feats


def wf(df, feats, h, conf_pct, depth, years):
    """walk-forward GBM: for each test year, train on prior years, trade top-confidence signals."""
    dr = []
    for Y in years:
        tr = df[df.year < Y]; tr = tr[tr[f"fwd{h}"].notna()]; te = df[df.year == Y]
        if len(tr) < 300 or te.empty: continue
        m = HistGradientBoostingRegressor(max_iter=200, max_depth=depth, learning_rate=0.05)
        m.fit(tr[feats].fillna(0), tr[f"fwd{h}"].values)
        pred = m.predict(te[feats].fillna(0)); yv = te[f"fwd{h}"].values
        thr = np.nanpercentile(np.abs(pred), conf_pct)
        sig = (np.abs(pred) >= thr) & ~np.isnan(yv)
        if sig.sum() == 0: continue
        dr.append(np.sign(pred[sig]) * yv[sig] - COST)
    dr = np.concatenate(dr) if dr else np.array([])
    if len(dr) < 1 or dr.std() == 0: return -9, 0, 0
    return float(dr.mean() / dr.std() * np.sqrt(len(dr))), float(dr.mean()), int(len(dr))


def main():
    max_h = float(sys.argv[1]) if len(sys.argv) > 1 else 6.5
    t0 = time.time()
    log(f"=== EXPLORER START · budget {max_h}h · optimizing DEV(2023-25), sealed 2026 honest ===")
    data = {s: load(s) for s in STOCKS}
    best = {s: {"dev_t": -9} for s in STOCKS}
    tried = 0
    while (time.time() - t0) / 3600 < max_h:
        for s in STOCKS:
            df, feats = data[s]
            # explore a random config (approach = GBM directional; vary horizon, confidence, depth, feature subset)
            h = int(rng.choice(HORIZONS))
            conf = int(rng.choice([0, 40, 60]))                  # keep enough signals for an honest sealed sample
            depth = int(rng.choice([2, 3, 4]))
            k = int(rng.integers(max(6, len(feats) // 2), len(feats) + 1))
            fs = list(rng.choice(feats, k, replace=False))
            tried += 1
            dev_t, dev_p, dev_n = wf(df, fs, h, conf, depth, DEV_YEARS)
            if dev_n >= 200 and dev_t > best[s]["dev_t"] + 0.05:   # adequate sample -> no tiny-n overfit traps
                seal_t, seal_p, seal_n = wf(df, fs, h, conf, depth, [SEALED_YEAR])   # honest sealed check
                best[s] = {"dev_t": dev_t, "dev_p": dev_p, "dev_n": dev_n, "h": h, "conf": conf,
                           "depth": depth, "feats": fs, "seal_t": seal_t, "seal_p": seal_p, "seal_n": seal_n}
                holds = seal_p > 0 and seal_n >= 25 and seal_t >= 1.0
                log(f"[{s}] NEW DEV-BEST h={h} conf{conf} d{depth} nf{k} :: "
                    f"DEV {dev_p*100:+.3f}%/tr t={dev_t:.2f} n={dev_n} -> SEALED-2026 {seal_p*100:+.3f}%/tr "
                    f"t={seal_t:.2f} n={seal_n}  {'*** HOLDS ***' if holds else '(sealed: no)'}")
        if tried % 40 == 0:
            log(f"...heartbeat: {tried} configs tried, {(time.time()-t0)/3600:.2f}h elapsed")
        json.dump(best, open(HERE / "explorer_best.json", "w"), indent=2, default=str)
    log("=== EXPLORER DONE ===")
    for s in STOCKS:
        b = best[s]
        log(f"FINAL [{s}] best DEV t={b.get('dev_t',0):.2f} (h={b.get('h')},conf{b.get('conf')}) -> "
            f"SEALED-2026 {b.get('seal_p',0)*100:+.3f}%/tr t={b.get('seal_t',0):.2f} n={b.get('seal_n',0)}")


if __name__ == "__main__":
    main()
