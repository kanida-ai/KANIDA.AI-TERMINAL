"""
Convert the unified engine's edge -> TRADED P&L book (honest OOS). Trades PROMOTED patterns
(selected <=2025) on sealed 2026. Product-type routing + leverage + costs, daily-resolution execution
(enter next-day open, exit on target touch within window via daily H/L, else time-stop). Sequential
single-book per stock. Builds: Overall book, Best-Trader (by segment), Monthly total, Monthly ROC.

Segments/routing:  1D -> MIS 5x (long/short) | long 2D/5D -> CNC 1x | short 2D/5D -> NRML 5x (futures;
approximated on cash move, execution needs futures). Rs1,00,000 = margin/position.

Run: python engine_trade.py SYM1 SYM2 ...   (default: all stocks in unified_patterns)   -> reports/engine_book*.xlsx
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports")
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}
COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}


def route(direction, w):
    if w == 1: return "MIS", ("LONG" if direction == "up" else "SHORT")
    if direction == "up": return "CNC", "LONG"
    return "NRML", "SHORT"


def trades_for(symbol, con):
    rows = con.execute("SELECT target,rule_text,rule_json,lift_te FROM unified_patterns "
                       "WHERE symbol=? AND promoted=1", (symbol,)).fetchall()
    if not rows: return []
    pats = [{"t": t, "rule": rt, "dir": TGT[t][0], "pct": TGT[t][1], "w": TGT[t][2],
             "conds": [tuple(c) for c in json.loads(rj)], "lift": lf or 0} for t, rt, rj, lf in rows if t in TGT]
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return []
    f26 = frame[frame["year"] == 2026]
    days = list(f26.index); trades = []; i = 0
    O, H, L, C = frame["_o"], frame["_h"], frame["_l"], frame["_c"]
    if not pats: return []
    fire = np.zeros((len(days), len(pats)), dtype=bool)          # vectorise firing once (not per day)
    for pi, pp in enumerate(pats):
        fire[:, pi] = apply_rule(f26, pp["conds"]).values
    lifts = np.array([pp["lift"] for pp in pats])
    while i < len(days) - 1:
        fired = np.where(fire[i])[0]
        if fired.size == 0:
            i += 1; continue
        t = days[i]; p = pats[int(fired[np.argmax(lifts[fired])])]; d, pct, w = p["dir"], p["pct"], p["w"]
        ed = days[i + 1]; entry = float(O.loc[ed])
        if not np.isfinite(entry) or entry <= 0:
            i += 1; continue
        long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
        wdays = days[i + 1: i + 1 + w]; ex = None; exd = wdays[-1]; reason = "time"; mfe = -1e9; mae = 1e9
        for wd in wdays:
            hi = float(H.loc[wd]); lo = float(L.loc[wd])
            if long:
                mfe = max(mfe, (hi / entry - 1) * 100); mae = min(mae, (lo / entry - 1) * 100)
                if hi >= tgt: ex = tgt; exd = wd; reason = "target"; break
            else:
                mfe = max(mfe, (entry / lo - 1) * 100); mae = min(mae, (entry / hi - 1) * 100)
                if lo <= tgt: ex = tgt; exd = wd; reason = "target"; break
        if ex is None: ex = float(C.loc[exd])
        order, dirlbl = route(d, w); lev = LEV[order]; cost = COST[order]
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        pnl = CAP * lev * (gross - cost) / 100.0
        trades.append({"symbol": symbol, "segment": f"{order} {dirlbl} {w}D", "order": order, "direction": dirlbl,
                       "hold": f"{w}D", "target": p["t"], "month": ed.strftime("%Y-%m"),
                       "signal_date": t.strftime("%Y-%m-%d"), "entry_date": ed.strftime("%Y-%m-%d"),
                       "entry_px": round(entry, 2), "target_px": round(tgt, 2),
                       "exit_date": exd.strftime("%Y-%m-%d"), "exit_px": round(ex, 2), "exit_reason": reason,
                       "holding_days": wdays.index(exd) + 1, "gross_move_pct": round(gross, 3),
                       "leverage": lev, "cost_pct": cost, "net_roc_pct": round((gross - cost) * lev, 3),
                       "capital": CAP, "pnl_rs": round(pnl, 1), "mfe_pct": round(mfe, 2), "mae_pct": round(mae, 2),
                       "win": 1 if pnl > 0 else 0, "rule": p["rule"], "entry_d": ed, "exit_d": exd, "pnl": pnl})
        i = days.index(exd) + 1
    return trades


def peak_capital(df):
    ev = []
    for _, r in df.iterrows():
        ev.append((r.entry_d, 1)); ev.append((r.exit_d + pd.Timedelta(days=1), -1))
    ev.sort(); cur = mx = 0
    for _, dl in ev: cur += dl; mx = max(mx, cur)
    return mx * CAP


def build_views(trades, tag):
    df = pd.DataFrame(trades)
    if df.empty:
        print("no trades"); return None
    overall = pd.DataFrame([{"View": "All Tradeable", "Trades": len(df), "Profit": round(df.pnl.sum()),
                             "Capital Needed": round(peak_capital(df)),
                             "Return %": round(df.pnl.sum() / peak_capital(df) * 100, 1)}])
    seg = df.groupby("segment").apply(lambda g: pd.Series({
        "Trades": len(g), "Profit": round(g.pnl.sum()), "Capital Needed": round(peak_capital(g)),
        "Return %": round(g.pnl.sum() / peak_capital(g) * 100, 1),
        "Avg Profit/Trade": round(g.pnl.mean()), "Win Rate %": round(g.win.mean() * 100, 1)}),
        include_groups=False).reset_index().sort_values("Profit", ascending=False)
    mtot = df.groupby("month").apply(lambda g: pd.Series({
        "Trades": len(g), "Profit": round(g.pnl.sum()), "Capital Needed": round(peak_capital(g)),
        "Monthly Return %": round(g.pnl.sum() / peak_capital(g) * 100, 1)}), include_groups=False).reset_index()
    mroc = df.pivot_table(index="segment", columns="month", values="pnl", aggfunc="sum").round(0)
    jstock = df.groupby("symbol").apply(lambda g: pd.Series({
        "Trades": len(g), "Profit": round(g.pnl.sum()), "Return %": round(g.pnl.sum() / peak_capital(g) * 100, 1),
        "Win Rate %": round(g.win.mean() * 100, 1), "Avg MFE %": round(g.mfe_pct.mean(), 2),
        "Avg MAE %": round(g.mae_pct.mean(), 2)}), include_groups=False).reset_index().sort_values("Profit", ascending=False)
    log_cols = ["symbol", "segment", "target", "direction", "hold", "month", "signal_date", "entry_date",
                "entry_px", "target_px", "exit_date", "exit_px", "exit_reason", "holding_days",
                "gross_move_pct", "leverage", "cost_pct", "net_roc_pct", "capital", "pnl_rs",
                "mfe_pct", "mae_pct", "win", "rule"]
    tl = df[log_cols].sort_values(["symbol", "entry_date"])
    tl.to_csv(OUT / f"engine_trade_log_2026{tag}.csv", index=False)
    with pd.ExcelWriter(OUT / f"engine_book{tag}.xlsx", engine="openpyxl") as xl:
        overall.to_excel(xl, "Overall_Book", index=False)
        seg.to_excel(xl, "Journal_by_Segment", index=False)
        jstock.to_excel(xl, "Journal_by_Stock", index=False)
        mtot.to_excel(xl, "Monthly_Total", index=False)
        mroc.to_excel(xl, "Monthly_Profit_by_Segment")
        tl.head(50000).to_excel(xl, "Trade_Log", index=False)
    print(f"\n=== OVERALL TRADEABLE BOOK ({tag or 'full'}) ===")
    print(overall.to_string(index=False))
    print("\n=== BEST TRADER VIEW (by order/direction/hold) ===")
    print(seg.to_string(index=False))
    print("\n=== MONTHLY TOTAL ===")
    print(mtot.to_string(index=False))
    print(f"\nwritten: reports/engine_book{tag}.xlsx")
    return df


def main():
    con = sqlite3.connect(SNR)
    syms = sys.argv[1:] or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    tag = "_preview" if sys.argv[1:] else ""
    print(f"trading {len(syms)} stocks' promoted patterns on sealed 2026 ...")
    trades = []
    for n, s in enumerate(syms, 1):
        try:
            trades += trades_for(s, con)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:80]}")
        if n % 25 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    build_views(trades, tag)


if __name__ == "__main__":
    main()
