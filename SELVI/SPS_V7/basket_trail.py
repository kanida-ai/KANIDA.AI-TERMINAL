"""
Basket-trail engine (their exact rules) applied to a pick source.
Entry: 50% @ 09:15 open + 50% @ 09:16 open (blended). MIS 5x. Square-off 15:29.
Basket trail on CAPITAL basis (5x): arm +6%, floor-lock +2%, giveback 5% from peak,
hard-stop -3%. Uses 1-min intraday path.

MODE:  python basket_trail.py their   -> their actual logged picks (validation)
       python basket_trail.py mom     -> my momentum screen picks (control)
"""
import sys, sqlite3
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
LOG = r"C:\Users\SPS\Downloads\Tradelog_data.xlsx"
CAP = 500_000.0
ARM, FLOOR, GIVEBACK, HARD = 0.06, 0.02, 0.05, 0.03   # capital basis
RT_COST = 0.0012                                       # ~ intraday round-trip

def basket_exit(cap_path):
    """apply trail/stop to a basket CAPITAL-P&L path (array). returns exit capital return."""
    peak = -9; armed = False
    for x in cap_path:
        peak = max(peak, x)
        if not armed and x <= -HARD:
            return -HARD                       # hard stop
        if x >= ARM:
            armed = True
        if armed:
            floor = max(FLOOR, peak - GIVEBACK)
            if x <= floor:
                return floor                   # trail / floor lock
    return cap_path[-1] if len(cap_path) else 0.0   # square-off 15:29


def picks_their():
    t = pd.read_excel(LOG, sheet_name="F_T15_Trades", header=0)
    t["trade_date"] = pd.to_datetime(t["trade_date"])
    return {d: list(g["symbol"]) for d, g in t.groupby("trade_date")}


def picks_mom():
    con = sqlite3.connect(str(DB)); d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con); con.close()
    d["date"] = pd.to_datetime(d["bar_time"])
    piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
    C, V, O = piv("close"), piv("volume"), piv("open")
    ret5 = C.pct_change(5).shift(1); ret20 = C.pct_change(20).shift(1)
    adv = (C * V).rolling(20).mean().shift(1); score = ret5.rank(axis=1) + 0.5 * ret20.rank(axis=1)
    out = {}
    for dt in C.index:
        if not ("2024-05-01" <= dt.strftime("%Y-%m-%d") <= "2026-07-31"): continue
        elig = (adv.loc[dt] >= 3e7) & ret5.loc[dt].notna() & O.loc[dt].notna()
        out[dt] = list(score.loc[dt][elig].sort_values(ascending=False).head(15).index)
    return out


def run(mode):
    picks = picks_their() if mode == "their" else picks_mom()
    con = sqlite3.connect(str(DB))
    rows = []
    for dt, syms in sorted(picks.items()):
        ds = dt.strftime("%Y-%m-%d")
        q = ("SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE symbol IN (%s) "
             "AND bar_time>=? AND bar_time<=? ORDER BY bar_time" % ",".join("?" * len(syms)))
        m = pd.read_sql(q, con, params=list(syms) + [ds + " 09:15:00", ds + " 15:29:00"])
        if m.empty: continue
        m["t"] = pd.to_datetime(m["bar_time"])
        paths = []
        for s in syms:
            g = m[m.symbol == s].set_index("t")
            if (ds + " 09:15:00") not in g.index.astype(str).tolist():
                pass
            try:
                o915 = g.loc[ds + " 09:15:00", "open"]; o916 = g.loc[ds + " 09:16:00", "open"]
            except KeyError:
                continue
            entry = (float(o915) + float(o916)) / 2.0
            path = g.loc[ds + " 09:16:00":, "close"].astype(float) / entry - 1.0   # price-return path from 09:16
            paths.append(path)
        if not paths: continue
        P = pd.concat(paths, axis=1).ffill()
        basket_price = P.mean(axis=1).to_numpy()          # equal-weight basket price return
        cap_path = 5.0 * basket_price                     # capital basis (5x)
        exit_cap = basket_exit(cap_path) - RT_COST * 5    # net of cost (5x turnover)
        rows.append({"date": dt, "ret5x": exit_cap, "ret1x": exit_cap / 5, "pnl": CAP * exit_cap})
    con.close()
    bt = pd.DataFrame(rows); bt["month"] = bt["date"].dt.strftime("%Y-%m")
    print(f"MODE={mode} · basket-trail (arm{ARM*100:.0f}/floor{FLOOR*100:.0f}/give{GIVEBACK*100:.0f}/hard{HARD*100:.0f}) · {len(bt)} days")
    print(f"{'month':9}{'days':>5}{'days_pos':>10}{'ret5x_pct':>11}{'ret1x_pct':>11}{'pnl_rs':>12}{'cum5x_pct':>11}")
    cum = 0
    for mo, g in bt.groupby("month"):
        r5 = g["ret5x"].sum() * 100; r1 = g["ret1x"].sum() * 100; cum += r5
        print(f"{mo:9}{len(g):>5}{int((g['ret1x']>0).sum()):>10}{r5:>10.1f}{r1:>10.1f}{g['pnl'].sum():>12,.0f}{cum:>10.1f}")
    print("-" * 69)
    print(f"{'TOTAL':9}{len(bt):>5}{int((bt['ret1x']>0).sum()):>10}{bt['ret5x'].sum()*100:>10.1f}{bt['ret1x'].sum()*100:>10.1f}{bt['pnl'].sum():>12,.0f}")
    print(f"avg/mo 1x: {bt['ret1x'].sum()*100/len(bt.groupby('month')):.1f}%  (their logs ~23-26%/mo)")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "their")
