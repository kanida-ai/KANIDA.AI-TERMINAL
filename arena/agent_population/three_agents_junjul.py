"""Standalone paper-trade of 3 Falcon pattern-agents (7619, 8584, 7164) over Jun-Jul 2026, day by day.
Each = target-hit swing (buy next open on rule-match; WIN if +10% within 20 trading days, else exit at 20d close;
positions still open at data-end are marked-to-market). Rs5L per position. Read-only; Falcon untouched."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
CAP = 5e5; PCT = 10; H = 20; COST = 0.15
AGENTS = [7619, 8584, 7164]
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rules = {pid: json.loads(uc.execute("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", (pid,)).fetchone()[0]) for pid in AGENTS}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2026-04-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2026-04-01' ORDER BY symbol,trade_date", uc)
uc.close()
CAL = sorted(oh.trade_date.unique()); POS = {d: i for i, d in enumerate(CAL)}
OHS = {s: g.reset_index(drop=True) for s, g in oh.groupby("symbol")}
SYMDATE = {(s, d): i for s, g in OHS.items() for i, d in enumerate(g.trade_date.values)}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}

def fires(rule):
    m = np.ones(len(feat), dtype=bool)
    for f, op, thr in rule:
        m &= OPS[op](feat[f].values, thr)
    return feat.loc[m, ["symbol", "trade_date"]]

def simulate(pid):
    trades = []
    for _, r in fires(rules[pid]).iterrows():
        s = r.symbol; sd = r.trade_date
        g = OHS.get(s)
        if g is None: continue
        si = SYMDATE.get((s, sd))
        if si is None or si + 1 >= len(g): continue
        ed = g.trade_date.values[si + 1]
        if not ("2026-06-01" <= ed <= "2026-07-20"): continue          # ENTRIES in Jun-Jul
        entry = g.open.values[si + 1]; tgt = entry * (1 + PCT / 100)
        exit_d = exit_p = reason = None
        for j in range(si + 1, min(si + 1 + H, len(g))):
            if g.high.values[j] >= tgt: exit_d = g.trade_date.values[j]; exit_p = tgt; reason = "target+10%"; break
            if j == si + H: exit_d = g.trade_date.values[j]; exit_p = g.close.values[j]; reason = "20d-exit"; break
        if exit_d is None:                                              # still open at data end
            exit_d = g.trade_date.values[-1]; exit_p = g.close.values[-1]; reason = "OPEN(mtm)"
        trades.append(dict(agent=pid, stock=s, entry_date=ed, entry=entry, exit_date=exit_d, exit=exit_p,
                           ret=(exit_p - entry) / entry * 100 - COST, reason=reason))
    return pd.DataFrame(trades)

RAW = pd.concat([simulate(p) for p in AGENTS], ignore_index=True)
# DE-DUP: one open position per stock at a time (chronological; re-entry allowed after the prior position exits)
RAW = RAW.sort_values("entry_date").reset_index(drop=True); held = {}; keep = []
for _, t in RAW.iterrows():
    if t.stock in held and t.entry_date < held[t.stock]: continue      # still holding this stock -> skip duplicate
    keep.append(t); held[t.stock] = t.exit_date
ALL = pd.DataFrame(keep).reset_index(drop=True)
print(f"[dedup] {len(RAW)} raw signals -> {len(ALL)} unique-stock positions ({len(RAW)-len(ALL)} duplicates skipped)\n")
win = [d for d in CAL if "2026-06-01" <= d <= "2026-07-20"]
print("STANDALONE PAPER-TRADE — agents 7619, 8584, 7164 — Jun-Jul 2026 (Rs5L/position, +10% target / 20d)\n")
for pid in AGENTS:
    T = ALL[ALL.agent == pid]
    res = T[T.reason != "OPEN(mtm)"]
    print(f"AGENT FALCPAT_{pid}: {len(T)} trades | target-hit {int((T.reason=='target+10%').sum())} | 20d-exit {int((T.reason=='20d-exit').sum())} | still-open {int((T.reason=='OPEN(mtm)').sum())}")
    print(f"   avg ret {T.ret.mean():+.2f}% | win% {(T.ret>0).mean()*100:.0f} | total P&L (Rs5L/pos) Rs{(T.ret/100*CAP).sum():,.0f}")
print(f"\nCOMBINED (all 3): {len(ALL)} trades | avg ret {ALL.ret.mean():+.2f}% | win% {(ALL.ret>0).mean()*100:.0f}% | total P&L Rs{(ALL.ret/100*CAP).sum():,.0f}")

# DAY BY DAY: clean mark-to-market. cumP&L(d) = sum over trades entered<=d of frozen(realized) or unrealized(close) P&L
print("\n" + "="*104 + "\nDAY-BY-DAY (combined book, Rs5L/position) — cumP&L reconciles to the trade total\n" + "="*104)
print(f"{'date':<12}{'new':>4}{'stocks bought':<40}{'open':>5}{'realized₹':>12}{'openMTM₹':>12}{'cumP&L₹':>13}{'dayΔ₹':>12}")
def price_on(s, d):
    g = OHS.get(s); i = SYMDATE.get((s, d))
    return g.close.values[i] if (g is not None and i is not None) else None
prev = 0.0
for d in win:
    new = ALL[ALL.entry_date == d]
    stocks = ",".join(new.stock.values[:6]) + ("..." if len(new) > 6 else "")
    live = ALL[ALL.entry_date <= d]
    real = open_mtm = 0.0; open_ct = 0
    for _, t in live.iterrows():
        if d >= t.exit_date: real += t.ret / 100 * CAP                 # frozen realized P&L
        else:
            p = price_on(t.stock, d)
            if p is not None: open_mtm += (p - t.entry) / t.entry * CAP; open_ct += 1
    cum = real + open_mtm
    print(f"{d:<12}{len(new):>4} {stocks:<39}{open_ct:>5}{real:>+12,.0f}{open_mtm:>+12,.0f}{cum:>+13,.0f}{cum-prev:>+12,.0f}")
    prev = cum
ALL.to_csv(os.path.join(ROOT, "docs", "reports", "THREE_AGENTS_junjul_trades.csv"), index=False)
print(f"\ntrade blotter -> docs/reports/THREE_AGENTS_junjul_trades.csv")
