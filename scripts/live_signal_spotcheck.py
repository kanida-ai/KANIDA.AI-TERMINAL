"""
Spot check: run the Trailing-Agent (Top-5, lock+1%/trail0.75/stop-1.5/15:29) on the
ACTUAL LIVE production signals (falcon_signals_live, live DB) vs the research signals
(falcon_signal_day_study) over the SAME entry dates. 1-min OHLC from the research DB.
Read-only on all source tables. Output: outputs/Live_Signal_SpotCheck.xlsx
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_intraday_backtest import load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
LIVE_DB = ROOT / "data" / "db" / "kanida_universe.db"
RND_DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Live_Signal_SpotCheck.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
CAP, TARGET, STOP, GIVEBACK, BASKET = 500_000.0, 1.0, -1.5, 0.75, 5

rc = sqlite3.connect(str(RND_DB))
days = [r[0] for r in rc.execute(
    "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min ORDER BY d")]
dayset = set(days)


def next_td(d):
    i = bisect.bisect_right(days, d)
    return days[i] if i < len(days) else None


# live signals (production)
lc = sqlite3.connect(str(LIVE_DB))
live = {}
for sd, rk, sym in lc.execute(
        "SELECT signal_date, rank, symbol FROM falcon_signals_live WHERE rank<=5 ORDER BY signal_date, rank"):
    live.setdefault(sd, []).append((rk, sym))
lc.close()

# research study signals (what the backtests used)
study = {}
for sd, rk, sym in rc.execute(
        "SELECT signal_date, engine_rank, symbol FROM falcon_signal_day_study "
        "WHERE persona='falcon_top10_daily' AND engine_rank<=5"):
    study.setdefault(sd, []).append((rk, sym))


def basket_trail(con, entry_date, picks):
    syms = [s for _, s in picks]
    day = load_ohlc_1min_day(con, entry_date, syms, DEFAULT_ALIASES)
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
    cc, oo, ep, used = [], [], [], []
    for _, s in picks:
        df = day.get(s)
        if df is None or "09:15" not in df.index:
            continue
        eo = df.at["09:15", "open"]
        if not np.isfinite(eo) or eo <= 0:
            continue
        close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
        o = df["open"].reindex(grid).to_numpy(float)
        oo.append(np.where(np.isfinite(o) & (o > 0), o, close)); cc.append(close)
        ep.append(float(eo)); used.append(s)
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
    n = len(ret); armed = False; peak = None
    for i in range(n - 1):
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


def run(sig_map, label):
    rows = []
    for sd in sorted(sig_map):
        ed = next_td(sd)
        if ed is None or ed not in dayset:
            continue
        res = basket_trail(rc, ed, sig_map[sd])
        if res is None:
            continue
        r, n = res
        rows.append({"signal_date": sd, "entry_date": ed,
                     "stocks": ", ".join(s for _, s in sig_map[sd]),
                     "n_stocks": n, "return_pct": round(r, 3),
                     "result": "WIN" if r > 0 else "LOSS", "src": label})
    return pd.DataFrame(rows)


live_df = run(live, "LIVE")
# research signals restricted to the live signal-date span (apples-to-apples window)
lo, hi = min(live), max(live)
study_span = {d: v for d, v in study.items() if lo <= d <= hi}
study_df = run(study_span, "RESEARCH")


def summ(df, label):
    if df.empty:
        return {"source": label, "days": 0}
    r = df["return_pct"]
    return {"source": label, "days": len(df), "win_rate_%": round((r > 0).mean() * 100, 1),
            "avg_day_%": round(r.mean(), 3), "median_%": round(float(r.median()), 3),
            "worst_%": round(r.min(), 2), "best_%": round(r.max(), 2),
            "sum_%": round(r.sum(), 2), "avg_n_stocks": round(df["n_stocks"].mean(), 2)}


comp = pd.DataFrame([summ(live_df, "LIVE production signals (falcon_signals_live)"),
                     summ(study_df, "RESEARCH signals (falcon_signal_day_study)")])

# day-by-day overlap on common entry dates
merged = live_df.merge(study_df, on="entry_date", suffixes=("_live", "_research"), how="inner")
merged["return_diff"] = (merged["return_pct_live"] - merged["return_pct_research"]).round(3)

OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
    comp.to_excel(xl, "1_LIVE_vs_RESEARCH", index=False)
    live_df.to_excel(xl, "2_LIVE_daily_log", index=False)
    study_df.to_excel(xl, "3_RESEARCH_daily_log", index=False)
    merged[["entry_date", "stocks_live", "return_pct_live", "stocks_research",
            "return_pct_research", "return_diff"]].to_excel(xl, "4_DayByDay_compare", index=False)
rc.close()

pd.set_option("display.width", 200)
print("=== SPOT CHECK: trailing strategy on LIVE vs RESEARCH signals (same window) ===")
print(comp.to_string(index=False))
print(f"\ncommon entry dates compared: {len(merged)}")
print(f"[*] wrote {OUT}")
if DESK.exists():
    import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")
