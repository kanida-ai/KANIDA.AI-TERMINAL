"""Decompose WHY Net ROC + day-win fall as capital rises (same combo 70/30, same MIS5X+MIS5X leverage).
Split the cost stack into: brokerage (has Rs20 floor), statutory (STT+txn+stamp+SEBI+GST, pure %), slippage (market impact).
Also report per-name participation (position / ADV) per leg, and the per-day cost hurdle. Read-only; Falcon untouched."""
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

def leg(names, ed, sgn, notional):
    names = [s for s in names if ed in op.index and s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s] and adv.get(s, 0) > 0]
    if not names or notional <= 0: return dict(gross=0, brok=0, stat=0, slip=0, part=[], n=0)
    per = notional / len(names); gross = brok = stat = slip = 0.0; parts = []
    for s in names:
        o = op.at[ed, s]; c = cp.at[ed, s]; qty = per / o; tb = o * qty; ts = c * qty
        gross += sgn * (c - o) * qty
        brok += min(20, 0.0003 * tb) + min(20, 0.0003 * ts)
        st = 0.00025 * ts; txn = 0.0000297 * (tb + ts); stamp = 0.00003 * tb; sebi = 0.000001 * (tb + ts)
        gst = 0.18 * (min(20, 0.0003 * tb) + min(20, 0.0003 * ts) + txn + sebi)
        stat += st + txn + stamp + sebi + gst
        part = per / adv[s]; sbps = (2 + 800 * part) / 1e4; slip += 2 * sbps * per; parts.append(part)
    return dict(gross=gross, brok=brok, stat=stat, slip=slip, part=parts, n=len(names))

def run(cap):
    tot = dict(gross=0, brok=0, stat=0, slip=0); Lpart = []; Spart = []; net_days = []
    for sd, g in byday.items():
        ed = nextd.get(sd)
        if not ed: continue
        L = leg(list(g[g["rank"] <= 10].symbol), ed, 1, cap * 0.7 * 5)
        S = leg(list(g[g["rank"] >= 201].symbol), ed, -1, cap * 0.3 * 5)
        for k in tot: tot[k] += L[k] + S[k]
        Lpart += L["part"]; Spart += S["part"]
        net_days.append(L["gross"] + S["gross"] - (L["brok"] + S["brok"] + L["stat"] + S["stat"] + L["slip"] + S["slip"]))
    nd = np.array(net_days); dwin = (nd > 0).mean() * 100
    return tot, np.median(Lpart) * 100, np.median(Spart) * 100, dwin, len(nd)

print(f"{'Capital':>8}{'GrossROC%':>10}{'Brok%':>8}{'Statut%':>9}{'Slip%':>9}{'TotCost%':>10}{'NetROC%':>9}"
      f"{'LongPart%':>11}{'ShortPart%':>12}{'Daywin%':>9}{'hurdle/day%':>13}")
for cap, tag in [(1e6, "₹10L"), (1e7, "₹1cr"), (5e7, "₹5cr"), (1e8, "₹10cr")]:
    t, lp, sp, dwin, ndays = run(cap)
    g = t["gross"] / cap * 100; b = t["brok"] / cap * 100; st = t["stat"] / cap * 100; sl = t["slip"] / cap * 100
    tc = b + st + sl; hurdle = tc / ndays
    print(f"{tag:>8}{g:>+10.1f}{b:>8.1f}{st:>9.1f}{sl:>9.1f}{tc:>10.1f}{g-tc:>+9.1f}{lp:>10.2f}%{sp:>11.3f}%{dwin:>8.0f}%{hurdle:>12.2f}%")
print("\nLongPart% = median per-name position as % of that stock's daily traded value (the 10 Falcon longs).")
print("ShortPart% = same for the ~268 tail-short names. hurdle/day% = total cost drag a day must clear to be a NET win.")
print("Note: brokerage has a Rs20/order FLOOR -> penalises TINY positions most (small capital, esp. the 268 tiny shorts).")
