"""
Top-20 pool -> pick best 5 by OPENING price-action/volume at 09:16/09:18/09:20.
One consistent signal source (regenerated Falcon engine). Entry on the next trading
day; decide at checkpoint T, fill next minute open, hold to 15:29 close.
Compares selection methods x entry times vs the plain Falcon Top-5 @ 09:15.
Output: outputs/Falcon_Top20_OpenEntry.xlsx
"""
import sqlite3, bisect, pickle
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PKL = ROOT / "outputs" / "_top20_open_cache.pkl"
OUT = ROOT / "outputs" / "Falcon_Top20_OpenEntry.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
POOL = 20
PICK = 5
CKPTS = ["09:16", "09:18", "09:20"]
ALIASES = {"ZOMATO": "ETERNAL"}


def nextmin(t):
    h, m = int(t[:2]), int(t[3:]); m += 1
    if m == 60: h += 1; m = 0
    return f"{h:02d}:{m:02d}"


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
        top = rk[:POOL]
        syms = [c["symbol"] for c in top]
        fscore = {c["symbol"]: c["score"] for c in top}
        # prior-day total volume (for volume pace)
        ph = ",".join("?" * len(syms))
        pv = {r[0]: r[1] for r in sc.execute(
            f"SELECT symbol, volume FROM ohlc_daily WHERE trade_date=? AND symbol IN ({ph})", [sd, *syms])}
        # 1-min OHLCV on entry day
        q = f"""SELECT symbol, substr(bar_time,12,5) hm, open, close, volume FROM ohlc_1min
                WHERE substr(bar_time,1,10)=? AND symbol IN ({ph})"""
        df = pd.read_sql_query(q, rc, params=[ed, *syms])
        if df.empty:
            continue
        df["symbol"] = df["symbol"].replace(ALIASES)
        recs = []
        for sym, g in df.groupby("symbol"):
            gi = g.sort_values("hm").set_index("hm")
            if "09:15" not in gi.index:
                continue
            o = gi.at["09:15", "open"]
            o = o.iloc[0] if isinstance(o, pd.Series) else o
            if not np.isfinite(o) or o <= 0:
                continue
            bars = list(gi.index)
            eod = gi["close"].iloc[-1]
            rec = {"sym": sym, "fscore": fscore.get(sym, 0.0), "open": float(o), "eod": float(eod)}
            for T in CKPTS:
                upto = [b for b in bars if "09:15" <= b <= T]
                fillbar = nextmin(T)
                if not upto or fillbar not in gi.index:
                    rec[f"ret_{T}"] = np.nan; rec[f"vp_{T}"] = np.nan; rec[f"held_{T}"] = np.nan
                    continue
                pxT = gi.loc[upto, "close"].iloc[-1]
                cumv = gi.loc[upto, "volume"].sum()
                fill = gi.at[fillbar, "open"]
                fill = fill.iloc[0] if isinstance(fill, pd.Series) else fill
                rec[f"ret_{T}"] = (pxT / o - 1) * 100
                rec[f"vp_{T}"] = (cumv / pv[sym]) if pv.get(sym) else np.nan
                rec[f"held_{T}"] = (eod / fill - 1) * 100 if fill and fill > 0 else np.nan
            recs.append(rec)
        if len(recs) >= PICK:
            cache.append((ed, recs))
        if (k + 1) % 100 == 0:
            print(f"  build [{k+1}/{len(feat_days)}]", flush=True)
    sc.close(); rc.close()
    pickle.dump(cache, open(PKL, "wb"))
    return cache


def z(a):
    a = np.asarray(a, float); mu = np.nanmean(a); sd = np.nanstd(a)
    return (a - mu) / sd if sd and sd > 0 else np.zeros_like(a)


def select(recs, T, method):
    valid = [r for r in recs if np.isfinite(r.get(f"held_{T}", np.nan))]
    if len(valid) < PICK:
        return None
    fs = np.array([r["fscore"] for r in valid]); ret = np.array([r[f"ret_{T}"] for r in valid])
    vp = np.array([r.get(f"vp_{T}", np.nan) for r in valid])
    if method == "falcon5":
        score = fs
    elif method == "mom":
        score = ret
    elif method == "mom_vol":
        score = z(ret) + z(vp)
    elif method == "falcon_mom":
        score = z(fs) + z(ret)
    elif method == "mom_pos":
        score = np.where(ret > 0, ret, -1e9)        # only up-at-open names
    else:
        score = fs
    idx = np.argsort(-score)[:PICK]
    return float(np.mean([valid[i][f"held_{T}"] for i in idx]))


def main():
    if PKL.exists():
        print("[*] loading cache", flush=True); cache = pickle.load(open(PKL, "rb"))
    else:
        print("[*] building cache", flush=True); cache = build_cache()
    print(f"[*] {len(cache)} days", flush=True)

    methods = ["falcon5", "mom", "mom_pos", "mom_vol", "falcon_mom"]
    avg_tbl, wr_tbl = {}, {}
    for meth in methods:
        for T in CKPTS:
            series = [select(recs, T, meth) for _, recs in cache]
            series = [s for s in series if s is not None]
            avg_tbl[(meth, T)] = round(float(np.mean(series)), 3)
            wr_tbl[(meth, T)] = round(float(np.mean([s > 0 for s in series])) * 100, 1)

    # baseline: Falcon Top-5 entered at 09:15 open -> close
    base = []
    for _, recs in cache:
        v = sorted(recs, key=lambda r: -r["fscore"])[:PICK]
        rs = [(r["eod"] / r["open"] - 1) * 100 for r in v if r["open"] > 0]
        if rs:
            base.append(float(np.mean(rs)))
    base_avg = round(float(np.mean(base)), 3); base_wr = round(float(np.mean([b > 0 for b in base])) * 100, 1)

    avg_df = pd.DataFrame([{"method": m, **{T: avg_tbl[(m, T)] for T in CKPTS}} for m in methods])
    wr_df = pd.DataFrame([{"method": m, **{T: wr_tbl[(m, T)] for T in CKPTS}} for m in methods])

    print(f"\n=== BASELINE: Falcon Top-5 @ 09:15 -> close: avg {base_avg}%/day, WR {base_wr}% ===")
    print("\n=== AVG %/day  (pick 5 of Top-20 by opening action) ===")
    print(avg_df.to_string(index=False))
    print("\n=== WIN RATE % (days positive) ===")
    print(wr_df.to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        pd.DataFrame([{"baseline": "Falcon Top-5 @09:15->close", "avg/day%": base_avg, "WR%": base_wr}]).to_excel(xl, "0_Baseline", index=False)
        avg_df.to_excel(xl, "1_Avg_per_day", index=False)
        wr_df.to_excel(xl, "2_Win_rate", index=False)
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
