# -*- coding: utf-8 -*-
"""FINALIZED SETUP end-to-end. Enter the ±X% break in the direction of prior-day momentum (yesterday up->
long only, down->short only). Target ±1.0% from open, stop at open, else square-off 15:29. 1-min race
(does target arrive before stop?). Rs1L/trade, 1X and 5X views. Sweeps entry X to test 'earlier=more'.
Full trade log with date/time. 1-min 2024-05+."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
CAP = 100000; C1, C5 = 0.06, 0.30          # round-trip cost % of capital at 1x / 5x
TARGET = 1.0                                 # target milestone from open (%)


def load(sym):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT trade_date,close FROM ohlc_daily WHERE symbol=? ORDER BY trade_date", con, params=[sym]); con.close()
    d["pret"] = d.close.pct_change()*100; return b, d.set_index("trade_date").pret.to_dict()


def backtest(b, pret, X, use_filter=True, slip=0.0):
    trades = []
    for dt, g in b.groupby("date"):
        pr = pret.get(dt, np.nan)
        if pr != pr: continue
        g = g.sort_values("bar_time").reset_index(drop=True); o = g.open.iloc[0]
        H, L, C, T = g.high.values, g.low.values, g.close.values, g.bar_time.values
        side = 1 if pr > 0 else -1                      # prior-day momentum decides side
        lvl = o*(1+side*X/100)
        hit = np.where(H >= lvl)[0] if side > 0 else np.where(L <= lvl)[0]
        if len(hit) == 0: continue
        t = hit[0]
        if use_filter and side*pr <= 0: continue        # (already aligned by construction, kept for clarity)
        entry = lvl; tgt = o*(1+side*TARGET/100); stp = o
        exit_p = C[-1]; exit_t = T[-1]; reason = "CLOSE"
        for i in range(t+1, len(g)):
            if side > 0:
                if L[i] <= stp: exit_p, exit_t, reason = stp, T[i], "STOP"; break     # conservative: stop checked first
                if H[i] >= tgt: exit_p, exit_t, reason = tgt, T[i], "TARGET"; break
            else:
                if H[i] >= stp: exit_p, exit_t, reason = stp, T[i], "STOP"; break
                if L[i] <= tgt: exit_p, exit_t, reason = tgt, T[i], "TARGET"; break
        gross = side*(exit_p-entry)/entry*100
        gross -= slip + (slip if reason == "STOP" else 0.0)      # slip: entry chase on all; extra on market stops
        trades.append(dict(date=dt, entry_time=T[t][11:16], side="LONG" if side > 0 else "SHORT",
                           entry=round(entry, 2), exit_time=exit_t[11:16], exit=round(exit_p, 2), reason=reason,
                           gross_pct=round(gross, 3), pnl_1x=round((gross-C1)/100*CAP), pnl_5x=round((gross*5-C5)/100*CAP),
                           status="WIN" if gross > 0 else "LOSS"))
    return pd.DataFrame(trades)


def dd(series):
    if series.empty: return 0
    cum = series.cumsum(); return float((cum.cummax()-cum).max())


def main(sym="ICICIBANK"):
    import sys
    if len(sys.argv) > 1: sym = sys.argv[1].upper()
    b, pret = load(sym)
    print(f"FINALIZED SETUP — {sym}  ·  ±X break + prior-day momentum  ·  target ±{TARGET}% / stop open  ·  Rs1L/trade")
    print(f"{'='*96}")
    print(f"{'Entry X':<9}{'trades':>7}{'WR':>7}{'avg%':>7}{'1X total':>12}{'1X maxDD':>11}{'5X total':>12}{'5X maxDD':>11}{'ret/DD 5X':>10}")
    best = None
    for X in [0.2, 0.25, 0.3, 0.35, 0.4, 0.5]:
        tr = backtest(b, pret, X)
        if tr.empty: continue
        t1, t5 = tr.pnl_1x.sum(), tr.pnl_5x.sum(); dd1, dd5 = dd(tr.pnl_1x), dd(tr.pnl_5x)
        wr = (tr.status == "WIN").mean()*100
        rdd = t5/dd5 if dd5 else 0
        print(f"±{X:<8}{len(tr):>7}{wr:>6.1f}%{tr.gross_pct.mean():>+7.3f}{t5*0+t1:>+12,.0f}{dd1:>11,.0f}{t5:>+12,.0f}{dd5:>11,.0f}{rdd:>10.2f}")
        tr.insert(0, "entry_X", X)
        if best is None or t5 > best[1]: best = (X, t5, tr)
    # full trade log for the best-P&L X
    Xb, _, trb = best
    out = os.path.expanduser("~")+f"/Downloads/FINAL_TRADELOG_{sym}.csv"
    alltr = pd.concat([backtest(b, pret, X).assign(entry_X=X) for X in [0.2, 0.25, 0.3, 0.35, 0.4, 0.5]], ignore_index=True)
    alltr.to_csv(out, index=False)
    print(f"\nBest by 5X P&L: entry ±{Xb}%   ({len(trb)} trades)")
    print("\n----- sample of the trade log (entry ±%s%%, first 12 + last 3) -----" % Xb)
    show = trb[["date", "entry_time", "side", "entry", "exit_time", "exit", "reason", "gross_pct", "pnl_1x", "pnl_5x", "status"]]
    print(show.head(12).to_string(index=False)); print("   ..."); print(show.tail(3).to_string(index=False))
    yr = trb.copy(); yr["y"] = yr.date.str[:4]
    print("\nyearly (best X, 5X):");
    for y, gg in yr.groupby("y"): print(f"  {y}: {len(gg)} trades  WR {(gg.status=='WIN').mean()*100:.0f}%  1X Rs {gg.pnl_1x.sum():>+10,.0f}  5X Rs {gg.pnl_5x.sum():>+12,.0f}")
    # ---- SLIPPAGE STRESS on the best X (the make-or-break test) ----
    print(f"\n===== SLIPPAGE STRESS (entry ±{Xb}%, the deployable number) =====")
    print(f"  {'stop slip':<12}{'WR':>7}{'avg%':>8}{'1X total':>12}{'5X total':>12}{'5X maxDD':>11}{'5X ret/DD':>10}")
    for sl in [0.0, 0.05, 0.10, 0.15]:
        ts = backtest(b, pret, Xb, slip=sl)
        t1, t5, d5 = ts.pnl_1x.sum(), ts.pnl_5x.sum(), dd(ts.pnl_5x)
        print(f"  {sl:<12.2f}{(ts.status=='WIN').mean()*100:>6.1f}%{ts.gross_pct.mean():>+8.3f}{t1:>+12,.0f}{t5:>+12,.0f}{d5:>11,.0f}{(t5/d5 if d5 else 0):>10.2f}")
    print(f"\nsaved FULL trade log (all X) -> {out}")


if __name__ == "__main__":
    main()
