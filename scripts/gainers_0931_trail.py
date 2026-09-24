"""
Same-day Top-5 Gainers (09:15->09:30) entered 09:31, with the FALCON portfolio
trail config — apples-to-apples vs the Falcon Top-5 trailing study.

Selection : top 5 by intraday return 09:15->09:30 (no other filter).
Entry     : OPEN of the 09:31 candle (next candle after the 09:30 observation).
Exit      : PORTFOLIO trail — hard stop -1.5%; arm at +1% then trail `giveback`
            below the basket peak (floor +1%); else 15:29 close. Identical to
            research_trailing_top5. Triggered exits fill at next-candle open.

Sweeps giveback {0.3,0.5,0.75,1.0}. Parallel over days. Compares to Falcon Top-5.
"""
from __future__ import annotations
import argparse, sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd

from intraday_gainers_research import load_day, DB

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Gainers_0931_Trail_vs_Falcon.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
START, END = "2024-05-14", "2026-06-15"
CAPITAL = 500_000.0
OBS = "09:30"          # rank gainers on 09:15->09:30
TARGET, STOP = 1.0, -1.5
GIVEBACKS = [0.30, 0.50, 0.75, 1.00]
BASKET = 5


def sim_trail(close, openexec, qty, e, giveback):
    """close/openexec: (minutes,k) from full grid. e=entry idx. Portfolio trail."""
    n = close.shape[0]
    entry = openexec[e]
    deployed = float((qty * entry).sum())
    port = close[e:] @ qty
    ret = (port - deployed) / deployed * 100.0
    openval = openexec[e:] @ qty
    nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    m = len(ret)
    armed = False; peak = None
    for i in range(m - 1):
        r = ret[i]
        if r <= STOP:
            return (nxt[i] - deployed) / deployed * 100.0, "STOP"
        if not armed:
            if r >= TARGET:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - giveback):
            return (nxt[i] - deployed) / deployed * 100.0, "TRAIL"
    return (float(port[-1]) - deployed) / deployed * 100.0, "CLOSE"


def process_chunk(days_chunk):
    con = sqlite3.connect(str(DB))
    out = defaultdict(list)        # giveback -> [records]
    for day in days_chunk:
        dd = load_day(con, day)
        if dd is None:
            continue
        gi = dd["gi"]
        if OBS not in gi:
            continue
        it = gi[OBS]
        e = it + 1
        if e >= len(dd["grid"]):
            continue
        # rank top-5 gainers by 09:15->09:30 return
        cand = []
        for sym, d in dd["syms"].items():
            if not np.isfinite(d["close"][it]) or d["o0"] <= 0:
                continue
            cand.append(((d["close"][it] / d["o0"] - 1), sym))
        cand.sort(reverse=True)
        picks = [s for _, s in cand[:BASKET]]
        # build arrays for picks with a valid 09:31 entry open
        cc, oo, eps = [], [], []
        for sym in picks:
            d = dd["syms"][sym]
            ep = d["open"][e]
            if not np.isfinite(ep) or ep <= 0:
                continue
            cc.append(d["close"]); oo.append(d["open"]); eps.append(ep)
        if not eps:
            continue
        close = np.column_stack(cc); openexec = np.column_stack(oo)
        eps = np.array(eps)
        alloc = CAPITAL / len(eps)
        qty = np.floor(alloc / eps)
        keep = qty > 0
        if not keep.any():
            continue
        close = close[:, keep]; openexec = openexec[:, keep]; qty = qty[keep]
        for g in GIVEBACKS:
            r, reason = sim_trail(close, openexec, qty, e, g)
            out[g].append({"date": day, "ret": r, "reason": reason, "n": int(keep.sum())})
    con.close()
    return dict(out)


def agg_block(df):
    win = df[df.ret > 0]; loss = df[df.ret < 0]
    return {
        "trading_days": len(df), "winning_days": int((df.ret > 0).sum()),
        "losing_days": int((df.ret < 0).sum()),
        "win_rate_%": round((df.ret > 0).mean() * 100, 1),
        "avg_ret_win_%": round(win.ret.mean(), 3) if len(win) else None,
        "avg_ret_loss_%": round(loss.ret.mean(), 3) if len(loss) else None,
        "avg_ret_all_%": round(df.ret.mean(), 3),
        "hit_2pct_%": round((df.ret >= 2.0).mean() * 100, 1),
        "best_day_%": round(df.ret.max(), 2), "worst_day_%": round(df.ret.min(), 2),
        "sum_return_%": round(df.ret.sum(), 2),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=10)
    args = ap.parse_args()
    con = sqlite3.connect(str(DB))
    days = [r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
        "WHERE bar_time BETWEEN ? AND ? ORDER BY d", (START, END + " 23:59:59"))]
    con.close()
    print(f"[*] days {len(days)} workers {args.workers}", flush=True)

    merged = defaultdict(list)
    chunks = [days[i::args.workers] for i in range(args.workers)]
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        for part in ex.map(process_chunk, chunks):
            for g, recs in part.items():
                merged[g].extend(recs)

    comp_rows = []
    monthly_by_g, yearly_by_g = {}, {}
    for g in GIVEBACKS:
        df = pd.DataFrame(merged[g])
        df["dt"] = pd.to_datetime(df["date"])
        df["year"] = df.dt.dt.year; df["month"] = df.dt.dt.month
        row = {"strategy": f"Gainers09:31 trail {g:.2f}"}; row.update(agg_block(df))
        comp_rows.append(row)
        monthly_by_g[g] = (df.groupby(["year", "month"]).apply(
            lambda x: pd.Series(agg_block(x)), include_groups=False).reset_index())
        yearly_by_g[g] = (df.groupby("year").apply(
            lambda x: pd.Series(agg_block(x)), include_groups=False).reset_index())

    # Falcon Top-5 trailing reference (from research_trailing_top5, 509 days)
    falcon_ref = [
        {"strategy": "Falcon Top5 trail 0.50 (ref)", "win_rate_%": 75.8, "avg_ret_win_%": 1.568,
         "avg_ret_loss_%": -1.398, "avg_ret_all_%": 0.851, "worst_day_%": -4.68, "sum_return_%": 433.2},
        {"strategy": "Falcon Top5 trail 0.75 (ref)", "win_rate_%": 75.8, "avg_ret_win_%": 1.782,
         "avg_ret_loss_%": -1.398, "avg_ret_all_%": 1.014, "worst_day_%": -4.68, "sum_return_%": 516.1},
        {"strategy": "Falcon Top5 trail 1.00 (ref)", "win_rate_%": 75.8, "avg_ret_win_%": 1.930,
         "avg_ret_loss_%": -1.398, "avg_ret_all_%": 1.125, "worst_day_%": -4.68, "sum_return_%": 572.9},
        {"strategy": "Falcon Top5 +1% hard (ref)", "win_rate_%": 80.6, "avg_ret_win_%": 1.137,
         "avg_ret_loss_%": -1.521, "avg_ret_all_%": 0.620, "worst_day_%": -5.88, "sum_return_%": 315.4},
    ]
    comp = pd.concat([pd.DataFrame(comp_rows), pd.DataFrame(falcon_ref)], ignore_index=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        comp.to_excel(xl, sheet_name="Comparison", index=False)
        for g in GIVEBACKS:
            monthly_by_g[g].to_excel(xl, sheet_name=f"M_gain_{g:.2f}", index=False)
            yearly_by_g[g].to_excel(xl, sheet_name=f"Y_gain_{g:.2f}", index=False)

    print("\n=== COMPARISON (Gainers 09:31 trail vs Falcon Top-5) ===")
    print(comp.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESKTOP.exists():
        import shutil; shutil.copy(OUT, DESKTOP / OUT.name)
        print(f"[*] copied to {DESKTOP / OUT.name}")


if __name__ == "__main__":
    main()
