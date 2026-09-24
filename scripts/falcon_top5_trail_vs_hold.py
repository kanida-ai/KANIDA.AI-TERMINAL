"""
CONTROLLED head-to-head: Falcon Top-5 Buy&Hold vs Trailing.
SAME selection (regenerated true Falcon Top-5), SAME entry (next trading day 09:15
1-min open), SAME equal capital, SAME 1-min window. ONLY the exit differs:
  - Buy&Hold : hold each stock to 15:29 close
  - Trailing : per-stock initial stop-loss + profit lock + trailing-stop + 15:29 time exit
Sweeps realistic trailing params; reports return AND risk-adjusted (maxDD, vol, Sharpe, Calmar).
Read-only. Output: outputs/Falcon_Top5_Trail_vs_Hold.xlsx
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_signal_replay import load_patterns, rank_for_date
from falcon_intraday_backtest import load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Falcon_Top5_Trail_vs_Hold.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
TOPN = 5
# trailing parameter grid (per-stock, %)
SLS = [1.5, 2.0, 3.0]
LOCKS = [1.0, 2.0]
TRAILS = [0.75, 1.5, 2.0]


def paths(df):
    grid = [m for m in df.index if m >= "09:15"]
    if not grid or "09:15" not in df.index:
        return None
    o = df["open"].reindex(grid).to_numpy(float)
    c = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
    o = np.where(np.isfinite(o) & (o > 0), o, c)
    return o, c


def hold_ret(o, c):
    e = o[0]
    return (c[-1] / e - 1) * 100 if e > 0 else None


def trail_ret(o, c, sl, lock, trail):
    e = o[0]
    if e <= 0:
        return None, "bad"
    armed = False; peak = 0.0
    for i in range(len(c) - 1):
        r = (c[i] / e - 1) * 100
        if not armed:
            if r <= -sl:
                return (o[i + 1] / e - 1) * 100, "stop"
            if r >= lock:
                armed = True; peak = r
        if armed:
            peak = max(peak, r)
            floor = max(lock, peak - trail)
            if r <= floor:
                return (o[i + 1] / e - 1) * 100, ("lock" if floor == lock else "trail")
    return (c[-1] / e - 1) * 100, "time"


def metrics(port):
    port = np.asarray(port, float)
    if len(port) == 0:
        return {}
    eq = np.cumprod(1 + port / 100)
    dd = (eq / np.maximum.accumulate(eq) - 1) * 100
    vol = port.std(ddof=0)
    w = port[port > 0]; l = port[port < 0]
    return {
        "Days": len(port),
        "Avg/day %": round(port.mean(), 3),
        "Win rate %": round((port > 0).mean() * 100, 1),
        "Avg win %": round(w.mean(), 3) if len(w) else None,
        "Avg loss %": round(l.mean(), 3) if len(l) else None,
        "Daily vol %": round(vol, 3),
        "Max DD %": round(dd.min(), 2),
        "Sharpe (ann)": round(port.mean() / vol * np.sqrt(252), 2) if vol > 0 else None,
        "Calmar (ann)": round((port.mean() * 252) / abs(dd.min()), 2) if dd.min() < 0 else None,
        "Sum %": round(port.sum(), 1),
    }


def main():
    sc = sqlite3.connect(str(SLIM)); rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    onemin = [r[0] for r in rc.execute("SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min ORDER BY 1")]
    onemset = set(onemin)

    def next_1m(d):
        i = bisect.bisect_right(onemin, d); return onemin[i] if i < len(onemin) else None

    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>='2024-05-10' ORDER BY 1")]

    combos = [(sl, lk, tr) for sl in SLS for lk in LOCKS for tr in TRAILS]
    # per-day portfolio returns
    hold_days = []
    trail_days = {cmb: [] for cmb in combos}
    exit_counts = {cmb: {"stop": 0, "lock": 0, "trail": 0, "time": 0} for cmb in combos}
    daily_rows = []
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(sc, patterns, sd, min_fires=10)
        if not rk:
            continue
        ed = next_1m(sd)
        if ed is None or ed not in onemset:
            continue
        syms = [c["symbol"] for c in rk[:TOPN]]
        day = load_ohlc_1min_day(rc, ed, syms, DEFAULT_ALIASES)
        pth = {}
        for s in syms:
            df = day.get(s)
            if df is not None:
                p = paths(df)
                if p:
                    pth[s] = p
        if len(pth) < 3:
            continue
        # buy & hold portfolio (equal weight)
        h = [hold_ret(*pth[s]) for s in pth]
        h = [x for x in h if x is not None]
        hold_port = float(np.mean(h))
        hold_days.append(hold_port)
        row = {"date": ed, "n": len(h), "buyhold%": round(hold_port, 3)}
        # trailing for each combo
        for cmb in combos:
            sl, lk, tr = cmb
            rs = []
            for s in pth:
                r, reason = trail_ret(*pth[s], sl, lk, tr)
                if r is not None:
                    rs.append(r); exit_counts[cmb][reason] += 1
            tp = float(np.mean(rs))
            trail_days[cmb].append(tp)
            if cmb == (2.0, 1.0, 0.75):
                row["trail(2.0/1.0/0.75)%"] = round(tp, 3)
        daily_rows.append(row)
        if (k + 1) % 100 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {sd}", flush=True)
    sc.close(); rc.close()

    # metrics
    bh = metrics(hold_days); bh["strategy"] = "BUY & HOLD (to 15:29 close)"; bh["params"] = "-"
    rows = [bh]
    for cmb in combos:
        m = metrics(trail_days[cmb])
        m["strategy"] = "TRAILING (per-stock)"
        m["params"] = f"SL{cmb[0]}/lock{cmb[1]}/trail{cmb[2]}"
        ec = exit_counts[cmb]; tot = sum(ec.values()) or 1
        m["exit_mix"] = f"stop{ec['stop']*100//tot}/lock{ec['lock']*100//tot}/trail{ec['trail']*100//tot}/time{ec['time']*100//tot}"
        rows.append(m)
    res = pd.DataFrame(rows)
    cols = ["strategy", "params", "Days", "Avg/day %", "Win rate %", "Avg win %", "Avg loss %",
            "Daily vol %", "Max DD %", "Sharpe (ann)", "Calmar (ann)", "Sum %", "exit_mix"]
    res = res[[c for c in cols if c in res.columns]]

    # best trailing by Sharpe and by Calmar
    tr_only = res[res.strategy.str.startswith("TRAILING")]
    best_sharpe = tr_only.sort_values("Sharpe (ann)", ascending=False).iloc[0]
    best_calmar = tr_only.sort_values("Calmar (ann)", ascending=False).iloc[0]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        res.to_excel(xl, "1_AllConfigs", index=False)
        verdict = pd.DataFrame([
            res[res.strategy.str.startswith("BUY")].iloc[0],
            best_sharpe, best_calmar,
        ])
        verdict.insert(0, "label", ["Buy&Hold", "Best-Sharpe Trailing", "Best-Calmar Trailing"])
        verdict[[c for c in ["label"] + cols if c in verdict.columns]].to_excel(xl, "2_Verdict", index=False)
        pd.DataFrame(daily_rows).to_excel(xl, "3_Daily_compare", index=False)

    pd.set_option("display.width", 240)
    print("\n=== TRAIL vs HOLD (same Top-5, entry, capital, window; only exit differs) ===")
    print(res.to_string(index=False))
    print("\n=== VERDICT ===")
    print(f"Buy&Hold : avg {bh['Avg/day %']}%/day, Sharpe {bh['Sharpe (ann)']}, Calmar {bh['Calmar (ann)']}, MaxDD {bh['Max DD %']}%")
    print(f"Best-Sharpe trailing [{best_sharpe['params']}]: avg {best_sharpe['Avg/day %']}%/day, "
          f"Sharpe {best_sharpe['Sharpe (ann)']}, Calmar {best_sharpe['Calmar (ann)']}, MaxDD {best_sharpe['Max DD %']}%")
    print(f"[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
