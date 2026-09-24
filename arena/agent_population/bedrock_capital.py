"""BEDROCK — how much capital is needed to trade EVERY signal? (no signal ever dropped)
Two questions answered per persona, for F&O and ALL stocks, 2026 true-OOS:
  Q1  CAPACITY  : take EVERY signal at a fixed ticket. Measure PEAK concurrent deployed capital
                  -> that peak IS the capital requirement. Return = P&L / capital required.
  Q2  Rs5L FIX  : re-run Rs5L but PROPORTIONALLY SIZED (per the user's spec: never skip a signal,
                  shrink the ticket instead) and compare with the old drop-signals behaviour.
Entry = next open, exit = close after `hold` sessions, cost 0.15%."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.0015; LO, HI = "2026-01-01", "2026-07-31"; PID = 8787
PERSONAS = {"INTRADAY": 1, "BTST": 2, "WEEKLY": 5, "SWING": 15, "MONTHLY": 25}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
TICKET = 25000.0        # a realistic per-name ticket for Indian equities

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date", uc, params=(LO, HI))
um = pd.read_sql_query("SELECT symbol,in_nifty200 FROM universe_master WHERE is_active=1", uc); uc.close()
FO = set(um[um.in_nifty200 == 1].symbol)
days = sorted(feat.trade_date.unique()); IDX = {d: i for i, d in enumerate(days)}
OPEN = {(r.symbol, r.trade_date): r.open for r in oh.itertuples()}
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
mask = np.ones(len(feat), bool)
for c, o, t in rule: mask &= OPS[o](feat[c].values, t)
SIG = feat.loc[mask, ["trade_date", "symbol"]]


def capacity(hold, uset):
    """Every signal taken at a fixed TICKET. Returns peak deployed capital, P&L, concurrency."""
    s = SIG if uset is None else SIG[SIG.symbol.isin(uset)]
    byday = {d: sorted(g.symbol.tolist()) for d, g in s.groupby("trade_date")}
    openp = []; deployed = 0.0; peak = 0.0; pnl = 0.0; n_tr = 0; conc = []
    for i, d in enumerate(days):
        prev = days[i-1] if i > 0 else None
        for sym in (byday.get(prev, []) if prev else []):
            px = OPEN.get((sym, d))
            if not px or px <= 0: continue
            ei = min(i-1+hold, len(days)-1)
            openp.append(dict(sym=sym, qty=TICKET/px, entry=px, exit=days[ei]))
            deployed += TICKET; n_tr += 1
        peak = max(peak, deployed)
        keep = []
        for p in openp:
            if p["exit"] <= d and (p["sym"], d) in CLOSE:
                px = CLOSE[(p["sym"], d)]
                pnl += p["qty"]*px*(1-COST) - p["qty"]*p["entry"]
                deployed -= TICKET
            else: keep.append(p)
        openp = keep; conc.append(len(openp))
    return dict(trades=n_tr, peak_capital=peak, peak_positions=int(max(conc)), avg_positions=float(np.mean(conc)),
                pnl=pnl, ret_on_peak=(pnl/peak*100 if peak else 0.0))


def rs5L(hold, uset, proportional):
    """Rs5L sleeve. proportional=True -> never drop a signal, shrink the ticket instead."""
    CAP = 500000.0; MIN_T = 5000.0
    s = SIG if uset is None else SIG[SIG.symbol.isin(uset)]
    byday = {d: sorted(g.symbol.tolist()) for d, g in s.groupby("trade_date")}
    cash = CAP; openp = []; eq = []; funded = 0; dropped = 0; tickets = []
    for i, d in enumerate(days):
        prev = days[i-1] if i > 0 else None
        cands = [x for x in (byday.get(prev, []) if prev else []) if OPEN.get((x, d), 0) > 0]
        if cands and cash > 100:
            budget = min(cash/hold if hold > 1 else cash, cash); n = len(cands)
            if proportional:
                per = budget/n                      # shrink, never skip
            else:
                if budget/n < MIN_T: n = max(int(budget//MIN_T), 0); dropped += len(cands)-n; cands = cands[:n]
                per = budget/n if n else 0
            ei = min(i-1+hold, len(days)-1)
            for sym in cands[:n] if not proportional else cands:
                if per <= 0: break
                px = OPEN[(sym, d)]
                openp.append(dict(sym=sym, qty=per/px, entry=px, exit=days[ei]))
                cash -= per; funded += 1; tickets.append(per)
        elif cands: dropped += len(cands)
        keep = []
        for p in openp:
            if p["exit"] <= d and (p["sym"], d) in CLOSE:
                cash += p["qty"]*CLOSE[(p["sym"], d)]*(1-COST)
            else: keep.append(p)
        openp = keep
        eq.append(cash + sum(p["qty"]*CLOSE.get((p["sym"], d), p["entry"]) for p in openp))
    return dict(funded=funded, dropped=dropped, ret=(eq[-1]/CAP-1)*100, final=eq[-1],
                avg_ticket=float(np.mean(tickets)) if tickets else 0.0)


for label, uset in [("F&O (Nifty 200)", FO), ("ALL STOCKS", None)]:
    print("\n" + "="*100)
    print(f"BEDROCK · {label} · 2026-01..2026-07 · EVERY SIGNAL TRADED (ticket Rs{TICKET:,.0f}/name)")
    print("="*100)
    print(f"{'Persona':<10}{'Hold':>5}{'Signals':>9}{'Peak open':>11}{'Avg open':>10}{'CAPITAL NEEDED':>17}{'P&L':>14}{'Return':>9}")
    for p, h in PERSONAS.items():
        c = capacity(h, uset)
        print(f"{p:<10}{h:>5}{c['trades']:>9}{c['peak_positions']:>11}{c['avg_positions']:>10.1f}"
              f"{c['peak_capital']:>17,.0f}{c['pnl']:>14,.0f}{c['ret_on_peak']:>8.1f}%")
    print(f"\n  Rs5,00,000 — old behaviour (DROP signals) vs FIXED (proportional sizing, never drop):")
    print(f"{'Persona':<10}{'drop:funded':>13}{'drop:ret':>10}{'prop:funded':>13}{'prop:ret':>10}{'prop avg ticket':>17}")
    for p, h in PERSONAS.items():
        a = rs5L(h, uset, proportional=False); b = rs5L(h, uset, proportional=True)
        print(f"{p:<10}{a['funded']:>13}{a['ret']:>9.2f}%{b['funded']:>13}{b['ret']:>9.2f}%{b['avg_ticket']:>17,.0f}")
