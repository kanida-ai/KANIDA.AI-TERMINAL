"""BEDROCK — plain month-by-month P&L across all 5 hold periods, on Rs5,00,000, BASKET/SLEEVE capital.
Rules (locked with the user):
  * signal formed at Day-T CLOSE (weekly-feature agents fire only on the week's last session)  -> no lookahead
  * ENTRY  at Day-T+1 OPEN
  * EXIT   at CLOSE of (signal day + hold trading days)   [matches the persona definition exactly]
  * CAPITAL: each day deploy (available cash / hold_days) split EQUALLY across that day's signals; compounds on
             available cash only; min ticket Rs5,000 - if the slice is smaller, take fewer names (never fake size)
  * cash from an exit (at close) is usable from the NEXT day
  * cost 0.15% per trade (round trip), applied on exit
  * WINDOW 2026-01-01..2026-07-31 = TRUE OOS (Bedrock was mined in 2025)
Prints F&O (Nifty 200) first, then All stocks."""
import os, sqlite3, json
import numpy as np, pandas as pd

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP = 500000.0; COST = 0.0015; MIN_TICKET = 5000.0
LO, HI = "2026-01-01", "2026-07-31"
PERSONAS = {"INTRADAY": 1, "BTST": 2, "WEEKLY": 5, "SWING": 15, "MONTHLY": 25}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
AGENT, PID = "FALCPAT_8787", 8787

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date",
                       uc, params=(LO, HI))
um = pd.read_sql_query("SELECT symbol,in_nifty200 FROM universe_master WHERE is_active=1", uc); uc.close()
FO = set(um[um.in_nifty200 == 1].symbol)
USES_WEEKLY = any(f.startswith("weekly_") for f, _, _ in rule)

days = sorted(feat.trade_date.unique())
_dt = pd.to_datetime(pd.Series(days)); _wk = _dt.dt.isocalendar().year.astype(str) + "-" + _dt.dt.isocalendar().week.astype(str)
WEEKEND = dict(zip(days, (_wk.values != np.roll(_wk.values, -1))))          # last session of each ISO week
OPEN = {(r.symbol, r.trade_date): r.open for r in oh.itertuples()}
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}

# signals per day (point-in-time: uses only that day's own feature row)
mask = np.ones(len(feat), bool)
for col, op, thr in rule: mask &= OPS[op](feat[col].values, thr)
sig_all = feat.loc[mask, ["trade_date", "symbol"]]


def simulate(hold, universe_set, label):
    sig = sig_all if universe_set is None else sig_all[sig_all.symbol.isin(universe_set)]
    byday = {d: sorted(g.symbol.tolist()) for d, g in sig.groupby("trade_date")}
    cash = CAP; open_pos = []            # each: dict(sym, qty, entry_px, exit_date)
    rows = []; entries_m = {}; realized_m = {}
    for i, d in enumerate(days):
        # 1) ENTRIES at today's OPEN, from signals formed at the PREVIOUS session's close
        prev = days[i-1] if i > 0 else None
        cands = byday.get(prev, []) if prev else []
        if USES_WEEKLY and prev and not WEEKEND.get(prev, False): cands = []
        cands = [s for s in cands if (s, d) in OPEN and OPEN[(s, d)] > 0]
        if cands and cash > MIN_TICKET:
            budget = cash if hold <= 1 else cash / hold          # rolling sleeve
            budget = min(budget, cash)
            n = len(cands)
            if budget / n < MIN_TICKET: n = max(int(budget // MIN_TICKET), 0); cands = cands[:n]
            if n > 0:
                per = budget / n
                ei = min(i - 1 + hold, len(days) - 1)            # exit = close of (signal day + hold sessions)
                for s in cands:
                    px = OPEN[(s, d)]; qty = per / px
                    open_pos.append(dict(sym=s, qty=qty, entry_px=px, exit_date=days[ei]))
                    cash -= per
                entries_m[d[:7]] = entries_m.get(d[:7], 0) + n
        # 2) EXITS at today's CLOSE (cash usable next day)
        keep = []
        for p in open_pos:
            if p["exit_date"] <= d and (p["sym"], d) in CLOSE:
                px = CLOSE[(p["sym"], d)]
                proceeds = p["qty"] * px * (1 - COST)
                cash += proceeds
                realized_m[d[:7]] = realized_m.get(d[:7], 0.0) + (proceeds - p["qty"] * p["entry_px"])
            else:
                keep.append(p)
        open_pos = keep
        # 3) mark to market
        mtm = sum(p["qty"] * CLOSE.get((p["sym"], d), p["entry_px"]) for p in open_pos)
        rows.append(dict(date=d, ym=d[:7], equity=cash + mtm, cash=cash, open_n=len(open_pos)))
    eq = pd.DataFrame(rows)
    mo = eq.groupby("ym").agg(end_equity=("equity", "last")).reset_index()
    starts = [CAP] + mo.end_equity.tolist()[:-1]
    mo["start_equity"] = starts
    mo["pnl"] = mo.end_equity - mo.start_equity
    mo["ret_pct"] = mo.pnl / mo.start_equity * 100
    mo["trades"] = mo.ym.map(lambda m: entries_m.get(m, 0))
    total = (eq.equity.iloc[-1] / CAP - 1) * 100
    months = len(mo); ann = ((eq.equity.iloc[-1] / CAP) ** (12.0 / months) - 1) * 100
    peak = eq.equity.cummax(); mdd = ((peak - eq.equity) / peak * 100).max()
    return mo, total, ann, mdd, int(sum(entries_m.values())), eq


def report(universe_set, label):
    print("\n" + "=" * 96)
    print(f"BEDROCK  ·  {label}  ·  Rs5,00,000  ·  basket/sleeve capital  ·  2026-01..2026-07 (true OOS)")
    print("=" * 96)
    summary = []
    for pname, hold in PERSONAS.items():
        mo, total, ann, mdd, ntr, eq = simulate(hold, universe_set, label)
        print(f"\n--- {pname}  (hold {hold} session{'s' if hold>1 else ''}) ---")
        print(f"{'Month':<9}{'Trades':>8}{'Return %':>11}{'P&L Rs':>14}{'Capital Rs':>15}")
        for _, r in mo.iterrows():
            print(f"{r.ym:<9}{int(r.trades):>8}{r.ret_pct:>10.2f}%{r.pnl:>14,.0f}{r.end_equity:>15,.0f}")
        print(f"{'TOTAL':<9}{ntr:>8}{total:>10.2f}%{eq.equity.iloc[-1]-CAP:>14,.0f}{eq.equity.iloc[-1]:>15,.0f}")
        print(f"   annualised {ann:+.1f}%   ·   max drawdown {mdd:.1f}%   ·   {ntr} trades over {len(mo)} months")
        summary.append(dict(persona=pname, hold=hold, trades=ntr, total_pct=total, ann_pct=ann, maxdd=mdd,
                            final=eq.equity.iloc[-1]))
    S = pd.DataFrame(summary)
    print(f"\n>>> SUMMARY — {label}")
    print(f"{'Persona':<10}{'Hold':>6}{'Trades':>8}{'7-mo Return':>14}{'Annualised':>13}{'MaxDD':>9}{'Final Rs':>14}")
    for _, r in S.iterrows():
        print(f"{r.persona:<10}{int(r.hold):>6}{int(r.trades):>8}{r.total_pct:>13.2f}%{r.ann_pct:>12.1f}%{r.maxdd:>8.1f}%{r.final:>14,.0f}")
    return S


s_fo = report(FO, "F&O  (Nifty 200)")
s_all = report(None, "ALL STOCKS")
print("\n" + "=" * 96)
print("SIDE BY SIDE — 7-month return on Rs5L")
print("=" * 96)
print(f"{'Persona':<10}{'Hold':>6}{'F&O ret':>11}{'F&O trades':>12}{'ALL ret':>11}{'ALL trades':>12}")
for p in PERSONAS:
    a = s_fo[s_fo.persona == p].iloc[0]; b = s_all[s_all.persona == p].iloc[0]
    print(f"{p:<10}{int(a.hold):>6}{a.total_pct:>10.2f}%{int(a.trades):>12}{b.total_pct:>10.2f}%{int(b.trades):>12}")
