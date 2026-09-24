"""TEST the hypothesis: is the cost stack driven by the COUNT of short trades?
Split the FULL cost stack by LEG (long 10/day vs short ~268/day): trades, turnover, brokerage, statutory, slippage.
Combo 70/30, L:MIS5X + S:MIS5X, across the capital ladder. Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume FROM ohlc_daily WHERE trade_date BETWEEN '2026-01-01' AND '2026-07-17'", uc); uc.close()
adv = od.assign(tv=od.close * od.volume).groupby("symbol").tv.median().to_dict()
op = od.pivot_table(index="trade_date", columns="symbol", values="open"); cp = od.pivot_table(index="trade_date", columns="symbol", values="close")
cal = sorted(od.trade_date.unique()); nextd = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date,rank,symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2025-12-31' AND '2026-07-16'", rc); rc.close()
byday = {sd: g for sd, g in rk.groupby("signal_date")}

def leg(names, ed, notional):
    names = [s for s in names if ed in op.index and s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s] and adv.get(s, 0) > 0]
    if not names or notional <= 0: return dict(n=0, turn=0, brok=0, stat=0, slip=0)
    per = notional / len(names); brok = stat = slip = turn = 0.0
    for s in names:
        o = op.at[ed, s]; c = cp.at[ed, s]; qty = per / o; tb = o * qty; ts = c * qty; turn += tb + ts
        br = min(20, 0.0003 * tb) + min(20, 0.0003 * ts); brok += br
        txn = 0.0000297 * (tb + ts); sebi = 0.000001 * (tb + ts)
        stat += 0.00025 * ts + txn + 0.00003 * tb + sebi + 0.18 * (br + txn + sebi)
        part = per / adv[s]; slip += 2 * (2 + 800 * part) / 1e4 * per
    return dict(n=len(names), turn=turn, brok=brok, stat=stat, slip=slip)

for cap, tag in [(1e6, "₹10L"), (1e7, "₹1cr"), (5e7, "₹5cr"), (1e8, "₹10cr")]:
    L = dict(n=0, turn=0, brok=0, stat=0, slip=0); S = dict(n=0, turn=0, brok=0, stat=0, slip=0)
    for sd, g in byday.items():
        ed = nextd.get(sd)
        if not ed: continue
        l = leg(list(g[g["rank"] <= 10].symbol), ed, cap * 0.7 * 5)
        s = leg(list(g[g["rank"] >= 201].symbol), ed, cap * 0.3 * 5)
        for k in L: L[k] += l[k]; S[k] += s[k]
    print("=" * 96 + f"\n{tag}   (Combo 70/30 · L:MIS5X + S:MIS5X)")
    print(f"{'leg':<8}{'#trades':>9}{'turnover Rs':>16}{'brokerage':>13}{'statutory':>13}{'slippage':>14}{'LEG COST':>14}{'%oftot':>8}")
    tot = (L["brok"] + L["stat"] + L["slip"]) + (S["brok"] + S["stat"] + S["slip"])
    for nm, X in [("LONG(10)", L), ("SHORT(268)", S)]:
        lc = X["brok"] + X["stat"] + X["slip"]
        print(f"{nm:<8}{X['n']:>9,}{X['turn']:>16,.0f}{X['brok']:>13,.0f}{X['stat']:>13,.0f}{X['slip']:>14,.0f}{lc:>14,.0f}{lc/tot*100:>7.0f}%")
    print(f"{'TOTAL':<8}{L['n']+S['n']:>9,}{L['turn']+S['turn']:>16,.0f}{L['brok']+S['brok']:>13,.0f}{L['stat']+S['stat']:>13,.0f}{L['slip']+S['slip']:>14,.0f}{tot:>14,.0f}{'100%':>8}")
    print(f"   -> brokerage per trade: LONG Rs{L['brok']/max(L['n'],1):.1f}  SHORT Rs{S['brok']/max(S['n'],1):.1f}   | slippage per trade: LONG Rs{L['slip']/max(L['n'],1):,.0f}  SHORT Rs{S['slip']/max(S['n'],1):,.0f}")
