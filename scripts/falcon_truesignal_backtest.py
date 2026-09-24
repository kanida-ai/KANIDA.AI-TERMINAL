"""
DEFINITIVE validation (option #2): regenerate the TRUE live-equivalent Falcon Top-10
for the whole backtest window by replaying the production engine over the slim DB's
feature history, then run the Trailing strategy (Top-5, lock+1/trail0.75/stop-1.5/15:29)
on those true signals using the 1-minute data. Compare to the research-signal backtest.
Read-only on all source tables. Output: outputs/Falcon_TrueSignal_Backtest.xlsx
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_signal_replay import load_patterns, rank_for_date
from falcon_intraday_backtest import load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"                  # features + patterns
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"  # 1-min OHLC
OUT = ROOT / "outputs" / "Falcon_TrueSignal_Backtest.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
CAP, TARGET, STOP, GIVEBACK, BASKET = 500_000.0, 1.0, -1.5, 0.75, 5
WIN_S, WIN_E = "2024-05-13", "2026-06-24"      # signal dates (entry = next day)


def basket_trail(con1m, entry_date, picks):
    syms = [s for _, s in picks]
    day = load_ohlc_1min_day(con1m, entry_date, syms, DEFAULT_ALIASES)
    grid = None
    for _, s in picks:
        df = day.get(s)
        if df is None or "09:15" not in df.index:
            continue
        g = [m for m in df.index if m >= "09:15"]
        if grid is None or len(g) > len(grid):
            grid = g
    if grid is None:
        return None
    cc, oo, ep = [], [], []
    for _, s in picks:
        df = day.get(s)
        if df is None or "09:15" not in df.index:
            continue
        eo = df.at["09:15", "open"]
        if not np.isfinite(eo) or eo <= 0:
            continue
        close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
        o = df["open"].reindex(grid).to_numpy(float)
        oo.append(np.where(np.isfinite(o) & (o > 0), o, close)); cc.append(close); ep.append(float(eo))
    if not ep:
        return None
    ep = np.array(ep); qty = np.floor((CAP / len(ep)) / ep); keep = qty > 0
    if not keep.any():
        return None
    close = np.column_stack(cc)[:, keep]; openexec = np.column_stack(oo)[:, keep]
    qty = qty[keep]; ep = ep[keep]
    deployed = float((qty * ep).sum())
    port = close @ qty; ret = (port - deployed) / deployed * 100.0
    openval = openexec @ qty
    nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    armed = False; peak = None
    for i in range(len(ret) - 1):
        r = ret[i]
        if r <= STOP:
            return (nxt[i] - deployed) / deployed * 100.0, int(keep.sum())
        if not armed:
            if r >= TARGET:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - GIVEBACK):
            return (nxt[i] - deployed) / deployed * 100.0, int(keep.sum())
    return (float(port[-1]) - deployed) / deployed * 100.0, int(keep.sum())


def main():
    sc = sqlite3.connect(str(SLIM))
    rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    print(f"[*] patterns: {len(patterns)}", flush=True)

    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        (WIN_S, WIN_E))]
    onemin_days = [r[0] for r in rc.execute(
        "SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min ORDER BY 1")]

    def next_1m(d):
        i = bisect.bisect_right(onemin_days, d)
        return onemin_days[i] if i < len(onemin_days) else None

    print(f"[*] regenerating TRUE signals for {len(feat_days)} signal days ...", flush=True)
    rows = []
    sig_records = []
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(sc, patterns, sd, min_fires=10)
        if not rk:
            continue
        ed = next_1m(sd)
        if ed is None:
            continue
        top5 = [(c["rank"], c["symbol"]) for c in rk[:5]]
        sig_records.append({"signal_date": sd, "entry_date": ed,
                            "top10": ", ".join(c["symbol"] for c in rk[:10])})
        res = basket_trail(rc, ed, top5)
        if res is None:
            continue
        r, n = res
        rows.append({"signal_date": sd, "entry_date": ed,
                     "stocks": ", ".join(s for _, s in top5), "n_stocks": n,
                     "return_pct": round(r, 4), "result": "WIN" if r > 0 else "LOSS"})
        if (k + 1) % 100 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {sd}", flush=True)
    sc.close(); rc.close()

    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["entry_date"]); df["year"] = df.dt.dt.year; df["month"] = df.dt.dt.month

    def agg(g):
        w = g[g.return_pct > 0]; l = g[g.return_pct < 0]
        return pd.Series({"Days": len(g), "Win": len(w), "Loss": len(l),
                          "Win%": round(len(w) / len(g) * 100, 1),
                          "Avg_win%": round(w.return_pct.mean(), 2) if len(w) else None,
                          "Avg_loss%": round(l.return_pct.mean(), 2) if len(l) else None,
                          "Avg_day%": round(g.return_pct.mean(), 3),
                          "Sum%": round(g.return_pct.sum(), 1)})

    monthly = df.groupby(["year", "month"]).apply(agg, include_groups=False).reset_index()
    yearly = df.groupby("year").apply(agg, include_groups=False).reset_index()
    overall = agg(df)

    comp = pd.DataFrame([
        {"backtest": "TRUE live-equivalent signals (regenerated production engine)",
         "days": int(overall["Days"]), "win_rate_%": overall["Win%"],
         "avg_day_%": overall["Avg_day%"], "sum_%": overall["Sum%"]},
        {"backtest": "RESEARCH signals (falcon_signal_day_study) — prior FINAL file",
         "days": 517, "win_rate_%": 75.2, "avg_day_%": 1.004, "sum_%": 519.1},
    ])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        comp.to_excel(xl, "1_TrueSignal_vs_Research", index=False)
        yearly.to_excel(xl, "2_Yearly", index=False)
        monthly.to_excel(xl, "3_Monthly", index=False)
        df[["entry_date", "stocks", "n_stocks", "return_pct", "result"]].to_excel(
            xl, "4_Daily_Log", index=False)
        pd.DataFrame(sig_records).to_excel(xl, "5_Regenerated_Top10", index=False)

    pd.set_option("display.width", 200)
    print("\n=== DEFINITIVE: strategy on TRUE live-equivalent signals vs research ===")
    print(comp.to_string(index=False))
    print("\n=== Yearly (true signals) ===")
    print(yearly.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
