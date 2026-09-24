"""
Optimise the Falcon Top-5 trailing exit to KEEP risk low but MAXIMISE profit.
Redesigned per-stock trailing (NO profit cap): cut losers with an initial stop,
let winners RUN with a WIDE trailing stop + breakeven protection.

  exit = initial stop (-SL%) before arming
       | once price >= ARM%, trail: exit when price <= peak - TRAIL%
         (breakeven-protected: an armed winner never becomes a loss)
       | else 15:29 close

Honest bar: a config is only "better" if it beats SIZED-DOWN Buy&Hold at the same
drawdown (sizing trades return for DD at constant Sharpe). Sweep wide; report the
frontier + any config that dominates. Same Top-5/entry/capital/window as the controlled test.
Output: outputs/Falcon_Top5_Trail_Optimized.xlsx
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
OUT = ROOT / "outputs" / "Falcon_Top5_Trail_Optimized.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
TOPN = 5
SLS = [2.0, 3.0, 4.0, 5.0]
ARMS = [1.0, 2.0, 3.0]
TRAILS = [2.0, 3.0, 4.0, 6.0]


def paths(df):
    grid = [m for m in df.index if m >= "09:15"]
    if not grid or "09:15" not in df.index:
        return None
    o = df["open"].reindex(grid).to_numpy(float)
    c = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
    o = np.where(np.isfinite(o) & (o > 0), o, c)
    if o[0] <= 0:
        return None
    return o, c


def hold_ret(o, c):
    return (c[-1] / o[0] - 1) * 100


def trail_v2(o, c, sl, arm, trail):
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
            floor = peak - trail
            if floor < 0:
                floor = 0.0                      # breakeven protection (no winner -> loss)
            if r <= floor:
                return (o[i + 1] / e - 1) * 100
    return (c[-1] / e - 1) * 100


def metrics(port):
    port = np.asarray(port, float)
    eq = np.cumprod(1 + port / 100)
    dd = (eq / np.maximum.accumulate(eq) - 1) * 100
    vol = port.std(ddof=0)
    mdd = dd.min()
    return {"avg": port.mean(), "wr": (port > 0).mean() * 100, "vol": vol, "mdd": mdd,
            "sharpe": port.mean() / vol * np.sqrt(252) if vol > 0 else np.nan,
            "calmar": (port.mean() * 252) / abs(mdd) if mdd < 0 else np.nan,
            "sum": port.sum()}


def main():
    sc = sqlite3.connect(str(SLIM)); rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    onemin = [r[0] for r in rc.execute("SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min ORDER BY 1")]
    onemset = set(onemin)

    def next_1m(d):
        i = bisect.bisect_right(onemin, d); return onemin[i] if i < len(onemin) else None

    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>='2024-05-10' ORDER BY 1")]

    # PASS 1: cache Top-5 1-min paths per day + buy&hold daily return
    cache = []
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(sc, patterns, sd, min_fires=10)
        if not rk:
            continue
        ed = next_1m(sd)
        if ed is None or ed not in onemset:
            continue
        syms = [c["symbol"] for c in rk[:TOPN]]
        day = load_ohlc_1min_day(rc, ed, syms, DEFAULT_ALIASES)
        pp = [paths(day[s]) for s in syms if day.get(s) is not None]
        pp = [p for p in pp if p]
        if len(pp) < 3:
            continue
        cache.append((ed, pp))
        if (k + 1) % 100 == 0:
            print(f"  pass1 [{k+1}/{len(feat_days)}] {sd}", flush=True)
    sc.close(); rc.close()
    print(f"[*] cached {len(cache)} days", flush=True)

    bh_days = [float(np.mean([hold_ret(*p) for p in pp])) for _, pp in cache]
    BH = metrics(bh_days)

    # PASS 2: sweep redesigned trailing
    combos = [(sl, ar, tr) for sl in SLS for ar in ARMS for tr in TRAILS]
    rows = [{"strategy": "BUY & HOLD", "params": "-", **BH}]
    for (sl, ar, tr) in combos:
        td = [float(np.mean([trail_v2(*p, sl, ar, tr) for p in pp])) for _, pp in cache]
        m = metrics(td); m["strategy"] = "TRAIL v2"; m["params"] = f"SL{sl}/arm{ar}/trail{tr}"
        rows.append(m)
    R = pd.DataFrame(rows)

    # dominance vs SIZED-DOWN buy&hold at matched drawdown (sizing = constant Sharpe)
    R["sized_BH_ret@sameDD"] = R["mdd"].abs() / abs(BH["mdd"]) * BH["avg"]
    R["beats_sized_BH"] = R["avg"] > R["sized_BH_ret@sameDD"] + 1e-9
    R["beats_BH_sharpe"] = R["sharpe"] > BH["sharpe"]
    R["beats_BH_calmar"] = R["calmar"] > BH["calmar"]

    for col in ("avg", "wr", "vol", "mdd", "sharpe", "calmar", "sum", "sized_BH_ret@sameDD"):
        R[col] = R[col].round(3)
    show = ["strategy", "params", "avg", "wr", "vol", "mdd", "sharpe", "calmar",
            "sized_BH_ret@sameDD", "beats_sized_BH", "beats_BH_sharpe", "beats_BH_calmar"]

    tr = R[R.strategy == "TRAIL v2"]
    winners = tr[tr.beats_sized_BH | tr.beats_BH_sharpe]
    print("\n=== BUY&HOLD baseline ===")
    print(f"  avg {BH['avg']:.3f}%/day  Sharpe {BH['sharpe']:.2f}  Calmar {BH['calmar']:.2f}  MaxDD {BH['mdd']:.2f}%")
    print("\n=== TOP TRAIL v2 configs by Calmar ===")
    print(tr.sort_values("calmar", ascending=False)[show].head(8).to_string(index=False))
    print("\n=== TOP TRAIL v2 configs by Sharpe ===")
    print(tr.sort_values("sharpe", ascending=False)[show].head(8).to_string(index=False))
    print(f"\nConfigs that BEAT sized-down Buy&Hold (more return at same DD): {len(winners)}")
    if len(winners):
        print(winners.sort_values("avg", ascending=False)[show].to_string(index=False))

    # sized-down buy&hold frontier for reference
    frontier = pd.DataFrame([{"capital_%": int(f*100), "avg/day%": round(BH["avg"]*f, 3),
                              "maxDD%": round(BH["mdd"]*f, 2), "sharpe": round(BH["sharpe"], 2)}
                             for f in (1.0, 0.8, 0.7, 0.6, 0.5, 0.4)])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        R[show].to_excel(xl, "1_AllConfigs", index=False)
        frontier.to_excel(xl, "2_SizedDown_BuyHold_Frontier", index=False)
        (winners[show] if len(winners) else pd.DataFrame([{"note": "no trailing config beat sized-down Buy&Hold"}])).to_excel(
            xl, "3_Winners_vs_sizedBH", index=False)
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
