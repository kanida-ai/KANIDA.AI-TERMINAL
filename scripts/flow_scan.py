"""Live order-flow scanner over the whole captured universe (mkt_orderflow_1min).
Finds where BIG PLAYERS are pushing: gainers being lifted (price up + buy-heavy book +
volume = buyers eating offers) vs losers being pressed (price down + sell-heavy book +
volume = sellers hitting bids). Uses move + volume + total-book imbalance + top-5 depth
lean + (futures) OI buildup. Cross-sectional z-scores -> ride-the-flow ranking.

Run anytime during the session for a fresh read: `python flow_scan.py`
"""
import sqlite3
from datetime import datetime, timezone, timedelta
import numpy as np
from pathlib import Path

DB = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine") / "universe_engine" / "data" / "db" / "kanida_universe.db"
IST = timezone(timedelta(hours=5, minutes=30))
NAMED = {"AEGISVOPAK", "CUB", "NAUKRI", "RITES", "WELCORP"}


def load():
    c = sqlite3.connect(str(DB))
    day = datetime.now(IST).strftime("%Y-%m-%d")
    rows = c.execute(
        "SELECT symbol,segment,bar_time,open,close,volume,total_buy_qty,total_sell_qty,"
        "b1q,b2q,b3q,b4q,b5q,a1q,a2q,a3q,a4q,a5q,oi FROM mkt_orderflow_1min "
        "WHERE bar_time>=? ORDER BY symbol,segment,bar_time", (day + " 09:15",)).fetchall()
    c.close()
    return rows


def zsc(x):
    x = np.asarray(x, float); s = x.std()
    return (x - x.mean()) / s if s > 0 else x * 0


def main():
    rows = load()
    # group by (symbol,segment)
    g = {}
    for r in rows:
        g.setdefault((r[0], r[1]), []).append(r)
    # OI change per FUT symbol
    oichg = {}
    for (sym, seg), rs in g.items():
        if seg == "FUT" and len(rs) >= 2 and rs[0][18] and rs[-1][18]:
            oichg[sym] = (rs[-1][18] - rs[0][18]) / rs[0][18] * 100
    recs = []
    for (sym, seg), rs in g.items():
        if seg != "CASH" or len(rs) < 3:
            continue
        opn = rs[0][3]; cls = rs[-1][4]
        if not opn or not cls:
            continue
        move = (cls / opn - 1) * 100
        vol = sum(x[5] or 0 for x in rs)
        imb = np.mean([(x[6] or 0) / ((x[6] or 0) + (x[7] or 0)) for x in rs if (x[6] or x[7])] or [0.5]) * 100
        imb_last = (rs[-1][6] or 0) / ((rs[-1][6] or 0) + (rs[-1][7] or 1)) * 100
        bid5 = np.mean([sum(x[8:13]) for x in rs]); ask5 = np.mean([sum(x[13:18]) for x in rs])
        depth_lean = bid5 / (bid5 + ask5) * 100 if (bid5 + ask5) else 50
        recs.append(dict(sym=sym, move=move, vol=vol, imb=imb, imb_last=imb_last,
                         depth_lean=depth_lean, oi=oichg.get(sym)))
    if not recs:
        print("no cash data yet"); return
    import pandas as pd
    df = pd.DataFrame(recs)
    df["mz"] = zsc(df.move); df["vz"] = zsc(np.log1p(df.vol)); df["iz"] = zsc(df.imb - 50)
    # ride-the-flow: momentum CONFIRMED by book imbalance + volume
    df["long_score"] = df.mz + df.iz + 0.5 * df.vz
    df["short_score"] = -df.mz - df.iz + 0.5 * df.vz
    df["confirmed"] = np.sign(df.move) == np.sign(df.imb - 50)
    now = datetime.now(IST)
    n_min = df_minutes = len(set(r[2] for r in rows if r[1] == "CASH")) if rows else 0

    def show(title, d, sc):
        print(f"\n{title}")
        print(f"  {'stock':<12}{'move%':>7}{'buy%':>6}{'depthBid%':>10}{'vol':>10}{'OIΔ%':>7}  flow")
        for _, r in d.iterrows():
            oi = f"{r.oi:+.1f}" if r.oi is not None and np.isfinite(r.oi) else "  -"
            flow = ("BUY-push" if r.long_score == sc(r) and False else
                    ("↑buyers lifting" if r.move > 0 and r.imb > 50 else
                     "↓sellers hitting" if r.move < 0 and r.imb < 50 else
                     "DIVERGENCE"))
            tag = "*" if r.sym in NAMED else " "
            print(f" {tag}{r.sym:<12}{r.move:>+6.2f}%{r.imb:>5.0f}%{r.depth_lean:>9.0f}%{int(r.vol):>10,}{oi:>7}  {flow}")

    print(f"LIVE FLOW SCAN @ {now:%H:%M IST}  |  {len(df)} cash names  |  ~{n_min} min since 09:15")
    show("=== BIG-PLAYER UP-PUSH (ride LONG): rising + buy-heavy book + volume ===",
         df.sort_values("long_score", ascending=False).head(12), lambda r: r.long_score)
    show("=== BIG-PLAYER DOWN-PUSH (ride SHORT): falling + sell-heavy book + volume ===",
         df.sort_values("short_score", ascending=False).head(12), lambda r: r.short_score)
    # divergences: price vs book disagree (absorption / distribution -> reversal risk or setup)
    dv = df[(~df.confirmed) & (df.vol > df.vol.median())].copy()
    dv["absads"] = (df.imb - 50).abs() + df.move.abs()
    show("=== DIVERGENCES (price vs book disagree — absorption/distribution) ===",
         dv.sort_values("absads", ascending=False).head(8), lambda r: 0)
    # the 5 named
    show("=== your 5 named stocks ===", df[df.sym.isin(NAMED)].sort_values("move", ascending=False), lambda r: 0)


if __name__ == "__main__":
    main()
