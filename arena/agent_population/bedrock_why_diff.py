"""WHY do the two F&O Swing numbers differ (+9.31% vs +4.2%)? Same agent, same signals, same window.
Run the SAME 66 F&O Swing signals under two position-sizing rules and print them month by month:
   A) SLEEVE   : ticket = (available cash / hold) / (signals that day)   <- what bedrock_monthly.py used
   B) UNIFORM  : ticket = flat Rs25,000 per signal                        <- what bedrock_capital.py used
Everything else identical: entry next open, exit close after 15 sessions, 0.15% cost."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.0015; LO, HI = "2026-01-01", "2026-07-31"; PID = 8787; HOLD = 15; CAP = 500000.0; FLAT = 25000.0
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date", uc, params=(LO, HI))
um = pd.read_sql_query("SELECT symbol,in_nifty200 FROM universe_master WHERE is_active=1", uc); uc.close()
FO = set(um[um.in_nifty200 == 1].symbol)
days = sorted(feat.trade_date.unique())
OPEN = {(r.symbol, r.trade_date): r.open for r in oh.itertuples()}
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
mask = np.ones(len(feat), bool)
for c, o, t in rule: mask &= OPS[o](feat[c].values, t)
S = feat.loc[mask, ["trade_date", "symbol"]]; S = S[S.symbol.isin(FO)]
byday = {d: sorted(g.symbol.tolist()) for d, g in S.groupby("trade_date")}

def run(mode):
    cash = CAP; openp = []; recs = []
    for i, d in enumerate(days):
        prev = days[i-1] if i > 0 else None
        cands = [x for x in (byday.get(prev, []) if prev else []) if OPEN.get((x, d), 0) > 0]
        if cands:
            n = len(cands)
            per = (min(cash/HOLD, cash) / n) if mode == "sleeve" else FLAT
            ei = min(i-1+HOLD, len(days)-1)
            for sym in cands:
                if mode == "sleeve" and per > cash: break
                px = OPEN[(sym, d)]
                openp.append(dict(sym=sym, qty=per/px, entry=px, exit=days[ei], ticket=per, ent_ym=d[:7]))
                cash -= per
        keep = []
        for p in openp:
            if p["exit"] <= d and (p["sym"], d) in CLOSE:
                px = CLOSE[(p["sym"], d)]
                pl = p["qty"]*px*(1-COST) - p["qty"]*p["entry"]
                cash += p["qty"]*px*(1-COST)
                recs.append(dict(ym=p["ent_ym"], sym=p["sym"], ticket=p["ticket"],
                                 ret_pct=(px/p["entry"]-1)*100 - COST*100, pnl=pl))
            else: keep.append(p)
        openp = keep
    return pd.DataFrame(recs)

A = run("sleeve"); B = run("flat")
print("BEDROCK · F&O · SWING (15d) · 2026 — SAME 66 signals, two sizing rules\n")
print(f"{'Month':<9}{'Sigs':>6} | {'SLEEVE avg tkt':>15}{'SLEEVE P&L':>13} | {'FLAT avg tkt':>14}{'FLAT P&L':>11}")
for m in sorted(set(A.ym) | set(B.ym)):
    a = A[A.ym == m]; b = B[B.ym == m]
    print(f"{m:<9}{len(a):>6} | {a.ticket.mean():>15,.0f}{a.pnl.sum():>13,.0f} | {b.ticket.mean():>14,.0f}{b.pnl.sum():>11,.0f}")
print(f"{'TOTAL':<9}{len(A):>6} | {A.ticket.mean():>15,.0f}{A.pnl.sum():>13,.0f} | {B.ticket.mean():>14,.0f}{B.pnl.sum():>11,.0f}")
print(f"\n  SLEEVE : P&L Rs{A.pnl.sum():,.0f} on Rs{CAP:,.0f} committed = {A.pnl.sum()/CAP*100:+.2f}%")
peak = 24*FLAT
print(f"  FLAT   : P&L Rs{B.pnl.sum():,.0f} on Rs{peak:,.0f} peak deployed = {B.pnl.sum()/peak*100:+.2f}%")
print(f"           (same P&L over Rs5L would be {B.pnl.sum()/CAP*100:+.2f}%)")
print("\n--- WHERE THE GAP COMES FROM ---")
print(f"{'Month':<9}{'Sigs':>6}{'avg ret/trade':>15}{'SLEEVE tkt':>13}{'FLAT tkt':>11}{'sizing effect':>15}")
for m in sorted(set(A.ym)):
    a = A[A.ym == m]; b = B[B.ym == m]
    if not len(a): continue
    print(f"{m:<9}{len(a):>6}{a.ret_pct.mean():>14.2f}%{a.ticket.mean():>13,.0f}{b.ticket.mean():>11,.0f}"
          f"{('BIG bet' if a.ticket.mean()>FLAT else 'small bet'):>15}")
print("\n  The sleeve rule sizes INVERSELY to how many signals fire that day:")
print("  few signals -> huge ticket; many signals -> tiny ticket. That is NOT a strategy, it is an")
print("  accident of the formula (cash/hold)/n. It happened to bet big on winning months here.")
