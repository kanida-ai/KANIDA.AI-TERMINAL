"""Union multi-screener: all DIP-family Falcon patterns + quality gate (above 200MA & 50MA rising), 2022+,
held 6 days, 1X. Builds a PROPER daily portfolio return (equal-weight open positions each day). Reports monthly
daily-return, win-rate, and coverage. Leak-free (trailing features).
"""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLD = 6
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
prows = con.execute("SELECT c.pattern_id, c.rule_json FROM falcon_pattern_candidates c JOIN falcon_promoted_patterns p ON c.pattern_id=p.pattern_id").fetchall()
def is_dip(rule):
    for f, op, th in rule:
        if f == "rsi_14" and op == "<=" and th <= 50: return True
        if f in ("roc_5", "roc_20", "roc_60") and op == "<=" and th <= 3: return True
        if f in ("weekly_close_loc", "close_loc") and op == "<=" and th <= 0.5: return True
        if f in ("dist_high_10", "dist_high_20", "dist_high_60", "dist_high_120", "dist_high_252") and op == "<=" and th <= -5: return True
    return False
DIP = {pid for pid, rj in prows if is_dip(json.loads(rj))}
print(f"dip-family patterns: {len(DIP)} of {len(prows)}", flush=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2021-06-01' AND trade_date<='2025-02-28' ORDER BY symbol,trade_date", con)
qf = pd.read_sql_query("SELECT symbol,trade_date,dist_sma_200,slope_sma_50 FROM falcon_features WHERE trade_date>='2022-01-01' AND trade_date<='2024-12-31'", con); con.close()

L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
L = L[(L.pattern_id.isin(DIP)) & (L.signal_date >= "2022-01-01")]
trades = L[["symbol", "signal_date"]].drop_duplicates()
# quality gate
q = qf[(qf.dist_sma_200 > 0) & (qf.slope_sma_50 > 0)][["symbol", "trade_date"]].rename(columns={"trade_date": "signal_date"})
trades = trades.merge(q, on=["symbol", "signal_date"], how="inner")
print(f"qualifying trades (dip + quality, 2022+): {len(trades):,} · stocks {trades.symbol.nunique()}", flush=True)

# per symbol: r6 (per qualification) + DEDUPED held-day returns (one position per stock per day)
qset = set(zip(trades.symbol, trades.signal_date))
r6rows = []; dayrows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values; n = len(c)
    qpos = [p for p in range(n) if (s, td[p]) in qset]
    for p in qpos:                                    # per-qualification 6-day return (trade-level WR)
        if p + HOLD < n and o[p + 1] > 0: r6rows.append((c[p + HOLD] - o[p + 1]) / o[p + 1] * 100)
    held = np.zeros(n, bool)                          # a stock is HELD (once) if any entry's window covers the day
    for p in qpos:
        for k in range(1, HOLD + 1):
            if p + k < n: held[p + k] = True
    for j in range(n):
        if not held[j]: continue
        if j - 1 >= 0 and held[j - 1] and c[j - 1] > 0:   # mid-hold: close-to-close
            dayrows.append((td[j], (c[j] - c[j - 1]) / c[j - 1] * 100))
        elif o[j] > 0:                                    # first day of a hold block: entered at open
            dayrows.append((td[j], (c[j] - o[j]) / o[j] * 100))
r6 = np.array(r6rows)
DD = pd.DataFrame(dayrows, columns=["date", "ret"])
print(f"\n== TRADE-LEVEL (6-day hold, 1X) ==")
print(f"  trades {len(r6):,} · WR {(r6>0).mean()*100:.0f}% · avg 6-day {r6.mean():+.2f}% · median {np.median(r6):+.2f}%")

# portfolio DAILY return = equal-weight mean of open positions each day
port = DD.groupby("date").ret.agg(["mean", "size"]).rename(columns={"mean": "day_ret", "size": "positions"}).reset_index()
port["mo"] = port.date.str[:7]
print(f"  avg positions open/day {port.positions.mean():.0f} (max {port.positions.max()})")
print(f"\n== DAILY PORTFOLIO RETURN (1X, equal-weight, hold 6d) ==")
print(f"  {'month':<9}{'days':>5}{'avg daily%':>12}{'month %':>10}{'pos/day':>9}")
for mo, m in port.groupby("mo"):
    monthret = (np.prod(1 + m.day_ret / 100) - 1) * 100
    print(f"  {mo:<9}{len(m):>5}{m.day_ret.mean():>+11.2f}%{monthret:>+9.1f}%{m.positions.mean():>8.0f}")
ann = (np.prod(1 + port.day_ret / 100) ** (252 / len(port)) - 1) * 100
print(f"\n  overall avg daily {port.day_ret.mean():+.2f}%  ·  day win-rate {(port.day_ret>0).mean()*100:.0f}%  ·  annualized(1X) ~{ann:.0f}%")
print(f"  cumulative (compounded, 1X) over 2022-2024: {(np.prod(1+port.day_ret/100)-1)*100:+.0f}%")
