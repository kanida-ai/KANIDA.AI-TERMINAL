"""RECONCILE the Bedrock podium card against the Rs5L portfolio simulation. SWING (15d), ALL stocks, 2026 OOS.
The card and the simulation answer DIFFERENT questions:
  CARD  = signal-level statistics: every signal counted, equal weight, capital assumed unlimited.
  SIM   = portfolio-level: Rs5L, sleeve budget (cash/hold), Rs5,000 min ticket -> most signals cannot be funded.
This prints the bridge between them, number by number."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP = 500000.0; COST = 0.0015; MIN_TICKET = 5000.0; HOLD = 15
LO, HI = "2026-01-01", "2026-07-31"
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
PID = 8787

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date", uc, params=(LO, HI)); uc.close()
USES_WEEKLY = any(f.startswith("weekly_") for f, _, _ in rule)
print(f"Bedrock rule uses weekly features: {USES_WEEKLY}  ({[f for f,_,_ in rule]})")

days = sorted(feat.trade_date.unique())
_dt = pd.to_datetime(pd.Series(days)); _wk = _dt.dt.isocalendar().year.astype(str)+"-"+_dt.dt.isocalendar().week.astype(str)
WEEKEND = dict(zip(days, (_wk.values != np.roll(_wk.values, -1))))
OPEN = {(r.symbol, r.trade_date): r.open for r in oh.itertuples()}
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
IDX = {d: i for i, d in enumerate(days)}

mask = np.ones(len(feat), bool)
for col, op, thr in rule: mask &= OPS[op](feat[col].values, thr)
sig = feat.loc[mask, ["trade_date", "symbol"]].copy()
if USES_WEEKLY: sig = sig[sig.trade_date.map(lambda d: WEEKEND.get(d, False))]

# ---------- A) SIGNAL-LEVEL (what the CARD reports) ----------
rows = []
for r in sig.itertuples():
    i = IDX[r.trade_date]
    if i+1 >= len(days): continue
    ed = days[min(i+HOLD, len(days)-1)]
    e = OPEN.get((r.symbol, days[i+1])); x = CLOSE.get((r.symbol, ed))
    if e and x and e > 0:
        rows.append(dict(ym=days[i+1][:7], sym=r.symbol, ret=(x-e)/e*100 - COST*100))
SIGL = pd.DataFrame(rows)
print("\n" + "="*84)
print("A)  SIGNAL-LEVEL  — what the podium card shows (every signal, equal weight, NO capital limit)")
print("="*84)
print(f"  signals in window          : {len(SIGL)}")
print(f"  signals per month          : {len(SIGL)/7:.1f}      <-- card says 'TRADES / MONTH  108'")
print(f"  AVERAGE RETURN PER SIGNAL  : {SIGL.ret.mean():+.2f}%   <-- card says 'AVG / TRADE  +5.63%'")
print(f"  win rate                   : {(SIGL.ret>0).mean()*100:.0f}%      <-- card says 'WIN RATE  61%'")

# ---------- B) PORTFOLIO-LEVEL (Rs5L, sleeve) ----------
cash = CAP; open_pos = []; execd = []; dropped = 0; sig_seen = 0
byday = {d: sorted(g.symbol.tolist()) for d, g in sig.groupby("trade_date")}
eqs = []
for i, d in enumerate(days):
    prev = days[i-1] if i > 0 else None
    cands = byday.get(prev, []) if prev else []
    cands = [s for s in cands if (s, d) in OPEN and OPEN[(s, d)] > 0]
    sig_seen += len(cands)
    if cands and cash > MIN_TICKET:
        budget = min(cash / HOLD, cash); n = len(cands)
        if budget / n < MIN_TICKET: n = max(int(budget // MIN_TICKET), 0)
        dropped += len(cands) - n
        if n > 0:
            per = budget / n; ei = min(i-1+HOLD, len(days)-1)
            for s in cands[:n]:
                px = OPEN[(s, d)]
                open_pos.append(dict(sym=s, qty=per/px, entry_px=px, exit_date=days[ei], ticket=per))
                cash -= per
    else:
        dropped += len(cands)
    keep = []
    for p in open_pos:
        if p["exit_date"] <= d and (p["sym"], d) in CLOSE:
            px = CLOSE[(p["sym"], d)]; proceeds = p["qty"]*px*(1-COST)
            cash += proceeds
            execd.append(dict(ym=d[:7], ret=(px/p["entry_px"]-1)*100 - COST*100, ticket=p["ticket"],
                              pnl=proceeds - p["qty"]*p["entry_px"]))
        else: keep.append(p)
    open_pos = keep
    eqs.append(cash + sum(p["qty"]*CLOSE.get((p["sym"], d), p["entry_px"]) for p in open_pos))
EX = pd.DataFrame(execd)
final = eqs[-1]
print("\n" + "="*84)
print("B)  PORTFOLIO-LEVEL — Rs5,00,000, sleeve budget (cash/15), Rs5,000 min ticket")
print("="*84)
print(f"  signals presented          : {sig_seen}")
print(f"  signals FUNDED (executed)  : {len(EX)}")
print(f"  signals DROPPED - no cash  : {dropped}   ({dropped/max(sig_seen,1)*100:.0f}% of all signals)")
print(f"  average ticket per trade   : Rs{EX.ticket.mean():,.0f}  (vs Rs5,00,000 if you could bet it all)")
print(f"  avg return per EXECUTED tr : {EX.ret.mean():+.2f}%")
print(f"  total P&L                  : Rs{final-CAP:+,.0f}")
print(f"  PORTFOLIO RETURN (7 months): {(final/CAP-1)*100:+.2f}%")

# ---------- C) THE BRIDGE ----------
print("\n" + "="*84)
print("C)  THE BRIDGE — why +5.63%/signal does NOT become +5.63% on your money")
print("="*84)
avg_tkt = EX.ticket.mean()
print(f"  a signal earns on average          {EX.ret.mean():+.2f}%  of ITS OWN ticket")
print(f"  but a ticket is only               Rs{avg_tkt:,.0f}  = {avg_tkt/CAP*100:.2f}% of the Rs5L book")
print(f"  so one signal moves the book by    {EX.ret.mean()*avg_tkt/CAP:+.3f}%")
print(f"  x {len(EX)} funded trades  ->  approx {EX.ret.mean()*avg_tkt/CAP*len(EX):+.1f}% (before compounding)")
print(f"  actual compounded portfolio return {(final/CAP-1)*100:+.2f}%")
print("\n  CONCLUSION: the card's '108 trades/month' and '+5.63%/trade' are SIGNAL statistics.")
print("  With Rs5L you can only fund a fraction of them, at a small slice each.")
print(f"  Same agent, same signals, honest money: {(final/CAP-1)*100:+.2f}% over 7 months.")
