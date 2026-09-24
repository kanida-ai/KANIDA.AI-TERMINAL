"""MONTHLY CAPACITY / REAL-MONEY view, Jan-Jul 2026 — ORDER-TYPE CORRECT leverage.
Zerodha reality: CNC = 1X (cash/delivery, LONG ONLY — no shorting). MIS = 5X intraday (the ONLY way to short).
  -> Long leg (Falcon top-10): can be CNC 1X  OR  MIS 5X.
  -> Short leg (tail-short 201-500): MIS 5X ONLY. There is no 1X short.
month | ROC | Rs gross P&L | cost | slippage | NET Rs P&L | NET ROC + aggregates. Slippage grows with position/ADV.
Costs = real Zerodha stack. Prices+volume from data/db ohlc_daily (fresh, to 2026-07-17). Read-only; Falcon untouched."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RDB = os.path.join(ROOT, "data", "db", "falcon_research.db")

def cost_rs(tb, ts):
    brok = min(20, 0.0003 * tb) + min(20, 0.0003 * ts); stt = 0.00025 * ts; txn = 0.0000297 * (tb + ts)
    stamp = 0.00003 * tb; sebi = 0.000001 * (tb + ts); gst = 0.18 * (brok + txn + sebi)
    return brok + stt + txn + stamp + sebi + gst

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
od = pd.read_sql_query("SELECT symbol, trade_date, open, close, volume FROM ohlc_daily WHERE trade_date BETWEEN '2026-01-01' AND '2026-07-17'", uc); uc.close()
adv = od.assign(tv=od.close * od.volume).groupby("symbol").tv.median().to_dict()
op = od.pivot_table(index="trade_date", columns="symbol", values="open"); cp = od.pivot_table(index="trade_date", columns="symbol", values="close")
cal = sorted(od.trade_date.unique()); nextd = {cal[i]: cal[i + 1] for i in range(len(cal) - 1)}
rc = sqlite3.connect("file:" + RDB.replace("\\", "/") + "?mode=ro", uri=True)
rk = pd.read_sql_query("SELECT signal_date, rank, symbol FROM falcon_full_ranking WHERE signal_date BETWEEN '2025-12-31' AND '2026-07-16'", rc); rc.close()
byday = {sd: g for sd, g in rk.groupby("signal_date")}

def leg(names, ed, sgn, notional):
    """notional = total Rs exposure this leg (already = capital_slice * leverage)."""
    names = [s for s in names if ed in op.index and s in op.columns and op.at[ed, s] == op.at[ed, s] and cp.at[ed, s] == cp.at[ed, s] and adv.get(s, 0) > 0]
    if not names: return 0, 0, 0
    per = notional / len(names); gross = cost = slip = 0.0
    for s in names:
        o = op.at[ed, s]; c = cp.at[ed, s]; qty = per / o
        gross += sgn * (c - o) * qty; cost += cost_rs(o * qty, c * qty)
        part = per / adv[s]; sbps = (2 + 800 * part) / 1e4; slip += 2 * sbps * per
    return gross, cost, slip

# strat -> (long_capital_frac, long_lev, short_capital_frac, short_lev). short_lev is always 5 (MIS).
def run(strat, cap, long_lev):
    daily = []
    for sd, g in byday.items():
        ed = nextd.get(sd)
        if not ed: continue
        top = list(g[g["rank"] <= 10].symbol); tail = list(g[g["rank"] >= 201].symbol)
        if strat == "Falcon":                       # long only
            G, C, S = leg(top, ed, 1, cap * long_lev)
        elif strat == "TailShort":                  # short only, MIS 5x FORCED
            G, C, S = leg(tail, ed, -1, cap * 5)
        else:                                        # Combo 70/30: long leg @ long_lev, short leg @ 5x
            g1 = leg(top, ed, 1, cap * 0.7 * long_lev); g2 = leg(tail, ed, -1, cap * 0.3 * 5)
            G, C, S = g1[0] + g2[0], g1[1] + g2[1], g1[2] + g2[2]
        daily.append((ed, G, C, S, G - C - S))
    D = pd.DataFrame(daily, columns=["d", "gross", "cost", "slip", "net"]); D["ym"] = D.d.str[:7]
    return D

def summ(D, cap):
    m = D.groupby("ym").agg(gross=("gross", "sum"), cost=("cost", "sum"), slip=("slip", "sum"), net=("net", "sum"), days=("d", "count"))
    m["roc"] = m.gross / cap * 100; m["net_roc"] = m.net / cap * 100
    eq = D.net.cumsum(); mdd = (eq.cummax() - eq).max() / cap * 100
    return m, dict(net=D.net.sum(), net_roc=D.net.sum()/cap*100, gross_roc=D.gross.sum()/cap*100, cost=D.cost.sum(),
                   slip=D.slip.sum(), maxdd=mdd, posmonths=int((m.net>0).sum()), totmonths=len(m), days=len(D), daywin=int((D.net>0).mean()*100))

CAPS = [(1e6, "10L"), (1e7, "1cr"), (1e8, "10cr")]
print("=" * 104 + "\nCAPACITY LADDER — Jan-Jul 2026 (order-type-correct). CNC=1X long-only; MIS=5X; shorts are MIS-5X only.\n" + "=" * 104)

def ladder(key, title, rows):   # key = run() strategy key; rows = list of (long_lev, order_label)
    print(f"\n### {title}")
    print(f"{'capital':>8}{'order/lev':>12}{'gross ROC%':>12}{'cost%':>8}{'slip%':>8}{'NET ROC%':>10}{'maxDD%':>8}{'+mo':>6}{'daywin%':>9}")
    for cap, ctag in CAPS:
        for ll, lbl in rows:
            D = run(key, cap, ll); m, s = summ(D, cap)
            print(f"{ctag:>8}{lbl:>12}{s['gross_roc']:>+12.0f}{s['cost']/cap*100:>8.1f}{s['slip']/cap*100:>8.1f}{s['net_roc']:>+10.0f}{s['maxdd']:>8.0f}{s['posmonths']:>4}/{s['totmonths']}{s['daywin']:>8}%")

ladder("Falcon", "Falcon (LONG)", [(1, "CNC 1X"), (5, "MIS 5X")])
ladder("TailShort", "TailShort (SHORT)", [(5, "MIS 5X")])            # only valid config
ladder("Combo", "Combo 70/30", [(1, "L1X+S5X"), (5, "L5X+S5X")])     # long leg CNC or MIS; short always MIS 5x

print("\n" + "=" * 104 + "\nMONTHLY VIEW (requested columns) — the DEPLOYABLE configs\n" + "=" * 104)
VIEWS = [("Falcon", 1e7, 5, "Falcon LONG · 1cr · MIS 5X"),
         ("Falcon", 1e8, 1, "Falcon LONG · 10cr · CNC 1X"),
         ("TailShort", 1e8, 5, "TailShort SHORT · 10cr · MIS 5X (only config)"),
         ("Combo", 1e7, 5, "Combo 70/30 · 1cr · L5X+S5X"),
         ("Combo", 1e8, 1, "Combo 70/30 · 10cr · L1X(CNC)+S5X(MIS)")]
for strat, cap, ll, title in VIEWS:
    D = run(strat, cap, ll); m, s = summ(D, cap)
    print(f"\n-- {title} --")
    print(f"{'month':<9}{'ROC%':>8}{'gross Rs':>15}{'cost Rs':>12}{'slip Rs':>13}{'NET Rs':>15}{'netROC%':>9}{'days':>6}")
    for ym, r in m.iterrows():
        print(f"{ym:<9}{r.roc:>+8.1f}{r.gross:>+15,.0f}{r.cost:>12,.0f}{r.slip:>13,.0f}{r.net:>+15,.0f}{r.net_roc:>+9.1f}{int(r.days):>6}")
    print(f"{'TOTAL':<9}{s['gross_roc']:>+8.1f}{'':<15}{s['cost']:>12,.0f}{s['slip']:>13,.0f}{s['net']:>+15,.0f}{s['net_roc']:>+9.1f}{s['days']:>6}"
          f"  | maxDD {s['maxdd']:.0f}% | +mo {s['posmonths']}/{s['totmonths']} | day-win {s['daywin']}%")
