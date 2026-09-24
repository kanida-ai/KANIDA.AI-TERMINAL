"""
Focused efficiency check: tight trail givebacks 0.75 / 1.0 / 1.3 (user-specified),
no profit cap, breakeven-protected. Stop -1.5%, arm at +1% (product-realistic).
Caches the Top-5 1-min paths to disk so re-sweeps are instant.
Output: outputs/Falcon_Top5_Trail_Tight.xlsx
"""
import sqlite3, bisect, pickle
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date
from falcon_intraday_backtest import load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PKL = ROOT / "outputs" / "_top5_paths.pkl"
OUT = ROOT / "outputs" / "Falcon_Top5_Trail_Tight.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
TOPN = 5
TRAILS = [0.75, 1.0, 1.3, 1.5, 2.0, 2.5, 3.0]
SLS = [2.0]
ARM = 1.0


def paths(df):
    grid = [m for m in df.index if m >= "09:15"]
    if not grid or "09:15" not in df.index:
        return None
    o = df["open"].reindex(grid).to_numpy(float)
    c = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
    o = np.where(np.isfinite(o) & (o > 0), o, c)
    return (o, c) if o[0] > 0 else None


def build_cache():
    sc = sqlite3.connect(str(SLIM)); rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    onemin = [r[0] for r in rc.execute("SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min ORDER BY 1")]
    onemset = set(onemin)
    nx = lambda d: (onemin[bisect.bisect_right(onemin, d)] if bisect.bisect_right(onemin, d) < len(onemin) else None)
    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>='2024-05-10' ORDER BY 1")]
    cache = []
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(sc, patterns, sd, min_fires=10)
        if not rk:
            continue
        ed = nx(sd)
        if ed is None or ed not in onemset:
            continue
        syms = [c["symbol"] for c in rk[:TOPN]]
        day = load_ohlc_1min_day(rc, ed, syms, DEFAULT_ALIASES)
        pp = [paths(day[s]) for s in syms if day.get(s) is not None]
        pp = [p for p in pp if p]
        if len(pp) >= 3:
            cache.append((ed, pp))
        if (k + 1) % 100 == 0:
            print(f"  pass1 [{k+1}/{len(feat_days)}]", flush=True)
    sc.close(); rc.close()
    with open(PKL, "wb") as f:
        pickle.dump(cache, f)
    return cache


def hold(o, c):
    return (c[-1] / o[0] - 1) * 100


def trail(o, c, sl, arm, tr):
    e = o[0]; armed = False; peak = 0.0
    for i in range(len(c) - 1):
        r = (c[i] / e - 1) * 100
        if not armed:
            if r <= -sl:
                return (o[i + 1] / e - 1) * 100
            if r >= arm:
                armed = True; peak = r
        if armed:
            if r > peak:
                peak = r
            floor = max(0.0, peak - tr)        # breakeven, NO profit cap
            if r <= floor:
                return (o[i + 1] / e - 1) * 100
    return (c[-1] / e - 1) * 100


def metrics(port):
    p = np.asarray(port, float); eq = np.cumprod(1 + p / 100)
    dd = (eq / np.maximum.accumulate(eq) - 1) * 100; vol = p.std(ddof=0); mdd = dd.min()
    w = p[p > 0]; l = p[p < 0]
    return {"Avg/day%": round(p.mean(), 3), "WR%": round((p > 0).mean() * 100, 1),
            "AvgWin%": round(w.mean(), 3) if len(w) else None,
            "AvgLoss%": round(l.mean(), 3) if len(l) else None,
            "Vol%": round(vol, 3), "MaxDD%": round(mdd, 2),
            "Sharpe": round(p.mean() / vol * np.sqrt(252), 2) if vol > 0 else None,
            "Calmar": round(p.mean() * 252 / abs(mdd), 1) if mdd < 0 else None}


def main():
    if PKL.exists():
        print("[*] loading cached paths", flush=True)
        cache = pickle.load(open(PKL, "rb"))
    else:
        print("[*] building path cache (first run)", flush=True)
        cache = build_cache()
    print(f"[*] {len(cache)} days", flush=True)

    BH = metrics([float(np.mean([hold(*pair) for pair in pp])) for _, pp in cache])
    rows = [{"strategy": "BUY & HOLD", "trail": "-", "SL": "-", **BH}]
    for sl in SLS:
        for tr in TRAILS:
            td = [float(np.mean([trail(*p, sl, ARM, tr) for p in pp])) for _, pp in cache]
            rows.append({"strategy": "TRAIL (no cap)", "trail": tr, "SL": sl, **metrics(td)})
    R = pd.DataFrame(rows)
    # efficiency vs sized-down B&H at matched drawdown
    R["sizedBH@DD"] = (R["MaxDD%"].abs() / abs(BH["MaxDD%"]) * BH["Avg/day%"]).round(3)
    R["beats_sizing"] = R["Avg/day%"] > R["sizedBH@DD"]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        R.to_excel(xl, "Tight_Trail_Efficiency", index=False)
    pd.set_option("display.width", 200)
    print("\n=== TIGHT TRAIL EFFICIENCY (arm +1%, no profit cap, breakeven) ===")
    print(R.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
