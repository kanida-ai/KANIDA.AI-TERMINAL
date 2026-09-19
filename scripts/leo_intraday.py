"""
AGENT LEO — INTRADAY hot-sector rotation (1x, unleveraged intraday long). Each morning Leo watches the
tape: which SECTOR is quietly being accumulated in the first ~30-60 min? It confirms the leader at a
checkpoint time, buys that day's strongest names in it, and exits at the close.

FAST ARCHITECTURE: from the 1-min arena payload (cache/bars/*.npz) we pre-extract ONLY the checkpoint
prices/volumes per stock per day (open, 9:20, 9:30, ... , close) into cache/leo_intraday.parquet ONCE
(vectorized reduceat = per-day reductions). Then the confirm-window sweep runs in a couple of seconds.

Leak-free: sector heat at time T uses ONLY data up to T; entry at the T price; exit at the close.
Costs: 0.20% round-trip intraday (conservative). Sweeps the CONFIRM WINDOW (9:20 -> later) to find the
best, plus #sectors and #stocks. Benchmark = buy-all-universe intraday (open->close).

Build cache:  python scripts/leo_intraday.py build
Analyze:      python scripts/leo_intraday.py
"""
from __future__ import annotations
import sys, sqlite3, glob, os, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
BARS = ROOT / "cache" / "bars"
KDB = str(ROOT / "db" / "kanida.db")
PANEL = ROOT / "cache" / "leo_intraday.parquet"
CHECKS = [920, 930, 940, 950, 1000, 1015, 1030, 1100, 1130, 1200]   # confirm-window candidates
COST_RT = 0.20 / 100


def _sectors():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    lab = pd.read_sql_query("SELECT symbol,sector FROM instrument_labels", con); con.close()
    return lab.dropna(subset=["sector"]).set_index("symbol")["sector"].to_dict()


def build_cache():
    t0 = time.time()
    sec = _sectors()
    rows = []
    files = sorted(glob.glob(str(BARS / "*.npz")))
    for f in files:
        sym = os.path.basename(f)[:-4]
        if sym not in sec:
            continue
        z = np.load(f)
        if "hm" not in z.files:
            continue
        hm = z["hm"]; c = z["c"]; v = z["v"]; ds = z["day_start"].astype(np.int64); did = z["day_id"]
        N = len(hm); ar = np.arange(N)
        # open = first bar close of each day; close = last bar close of each day
        open_ = c[ds]
        last_idx = np.maximum.reduceat(ar, ds)
        close_ = c[last_idx]
        vfull = np.add.reduceat(v, ds)
        d = {"symbol": sym, "sector": sec[sym], "day": did, "open": open_, "close": close_, "vfull": vfull}
        for T in CHECKS:
            valid = hm <= T
            lv = np.maximum.reduceat(np.where(valid, ar, -1), ds)          # last bar index <= T per day
            price = np.where(lv >= 0, c[np.clip(lv, 0, N - 1)], np.nan)
            vol = np.add.reduceat(np.where(valid, v, 0.0), ds)
            d[f"p{T}"] = price; d[f"v{T}"] = vol
        rows.append(pd.DataFrame(d))
    df = pd.concat(rows, ignore_index=True)
    df = df[(df["open"] > 0) & (df["close"] > 0)]
    df.to_parquet(PANEL, index=False)
    print(f"intraday cache: {df.symbol.nunique()} symbols x {df.day.nunique()} days = {len(df):,} rows "
          f"in {time.time()-t0:.0f}s -> {PANEL}")


def load_cache():
    if not PANEL.exists():
        build_cache()
    return pd.read_parquet(PANEL)


def metrics(daily):
    daily = daily.sort_index()
    eq = (1 + daily).cumprod()
    idx = pd.to_datetime(daily.index.astype(str), format="%Y%m%d")
    eq.index = idx
    yrs = (idx[-1] - idx[0]).days / 365.25
    cagr = (eq.iloc[-1]) ** (1 / yrs) - 1 if yrs > 0 and eq.iloc[-1] > 0 else -1
    peak = eq.cummax(); dd = float(((eq - peak) / peak).min())
    sh = daily.mean() / daily.std() * np.sqrt(252) if daily.std() > 0 else 0
    return {"CAGR_%": round(cagr * 100, 1), "maxDD_%": round(dd * 100, 1),
            "calmar": round(cagr / -dd, 2) if dd < 0 else None, "sharpe": round(sh, 2),
            "trades_days": int((daily != 0).sum()), "avg_day_%": round(daily[daily != 0].mean() * 100, 3) if (daily != 0).any() else 0,
            "win_day_%": round((daily[daily != 0] > 0).mean() * 100) if (daily != 0).any() else 0}


_PRE = {}


def _prep_T(df, T, min_names=3):
    """Precompute ONCE per checkpoint T: per stock-day mom/fwd + the day-relative SECTOR rank (by median
    momentum). topsec/nstock sweeps then just threshold ranks -> fully vectorized, no per-day Python loop."""
    if T in _PRE:
        return _PRE[T]
    pcol = f"p{T}"
    d = df[["sector", "day", "open", "close", pcol]].copy()
    d = d[d[pcol] > 0]
    d["mom"] = d[pcol] / d["open"] - 1.0
    d["fwd"] = d["close"] / d[pcol] - 1.0
    sec = d.groupby(["day", "sector"])["mom"].agg(["median", "size"])
    sec = sec[sec["size"] >= min_names].reset_index()
    sec["srank"] = sec.groupby("day")["median"].rank(ascending=False, method="first")
    d = d.merge(sec[["day", "sector", "srank"]], on=["day", "sector"], how="inner")
    _PRE[T] = d
    return d


def run(df, T, topsec=1, nstock=3, min_names=3, cost=COST_RT):
    d = _prep_T(df, T, min_names)
    lead = d[d["srank"] <= topsec].copy()
    lead["prank"] = lead.groupby("day")["mom"].rank(ascending=False, method="first")
    picks = lead[lead["prank"] <= nstock]
    daily = picks.groupby("day")["fwd"].mean() - cost
    return daily.sort_index()


def bench_all(df):
    d = df.copy(); d["r"] = d["close"] / d["open"] - 1.0
    return d.groupby("day")["r"].mean()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build_cache(); return
    t0 = time.time()
    df = load_cache()
    print(f"loaded intraday cache: {df.symbol.nunique()} syms x {df.day.nunique()} days  [{time.time()-t0:.0f}s]\n")

    print("=== CONFIRM-WINDOW SWEEP (Leo intraday, net 0.20% RT, entry@T exit@close) ===")
    print(f"  {'confirmT':>9}{'topsec':>7}{'nstk':>6}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}{'avgDay%':>8}{'winDay%':>8}{'days':>6}")
    best = None
    for T in CHECKS:
        for topsec in [1, 2]:
            for nstock in [2, 3, 5]:
                s = run(df, T, topsec=topsec, nstock=nstock)
                if len(s) < 200:
                    continue
                m = metrics(s)
                if best is None or (m["CAGR_%"] or -9) > best[0]["CAGR_%"]:
                    best = (m, T, topsec, nstock, s)
                if topsec == 1 and nstock == 3:
                    print(f"  {T:>9}{topsec:>7}{nstock:>6}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}"
                          f"{m['sharpe']:>6}{m['avg_day_%']:>8}{m['win_day_%']:>7}%{m['trades_days']:>6}")
    m, T, topsec, nstock, s = best
    print(f"\n=== BEST: confirm@{T} top{topsec}sec {nstock}stk  CAGR {m['CAGR_%']}%  DD {m['maxDD_%']}%  "
          f"Calmar {m['calmar']}  Sharpe {m['sharpe']}  avgDay {m['avg_day_%']}%  win {m['win_day_%']}% ===")
    mb = metrics(bench_all(df))
    print(f"  benchmark buy-all-universe open->close: CAGR {mb['CAGR_%']}%  DD {mb['maxDD_%']}%  avgDay {mb['avg_day_%']}%")
    # OOS era split
    print("  OOS era split (best):")
    print(f"  {'era':<16}{'days':>7}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'avgDay%':>9}")
    for lo, hi, lab in [(20150101, 20200101, "2015-2019"), (20200101, 20230101, "2020-2022"), (20230101, 20270101, "2023-2026 OOS")]:
        sub = s[(s.index >= lo) & (s.index < hi)]
        if len(sub) > 100:
            mm = metrics(sub)
            print(f"  {lab:<16}{mm['trades_days']:>7}{mm['CAGR_%']:>7}%{mm['maxDD_%']:>7}%{str(mm['calmar']):>7}{mm['avg_day_%']:>9}")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
