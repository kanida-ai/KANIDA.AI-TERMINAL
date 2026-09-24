"""Delayed-entry test on Falcon signals. Signal on S. Test entering at open(S+d) for d=1..4, held H days,
plus the single-day intraday return of each day S+1..S+6. Over all 14.9M leak-free fires. Reports win rate,
avg return, and consistency by year. Answers: does the edge improve if you enter later than T+1?
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
HOLD = 3   # days held after entry
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["pattern_id", "symbol", "signal_date"]]
print(f"fires {len(L):,}", flush=True)

con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE trade_date>='2018-12-01' AND trade_date<='2025-03-31' ORDER BY symbol,trade_date", con); con.close()

# per (symbol, signal_date): open(S+k), close(S+k) for k=1..8
rows = []
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); o = g.open.values.astype(float); c = g.close.values.astype(float); td = g.trade_date.values
    d = {"symbol": s, "signal_date": td}
    for k in range(1, 9):
        ok = np.roll(o, -k); ok[-k:] = np.nan; ck = np.roll(c, -k); ck[-k:] = np.nan
        d[f"o{k}"] = ok; d[f"c{k}"] = ck
    rows.append(pd.DataFrame(d))
PX = pd.concat(rows, ignore_index=True)
L = L.merge(PX, on=["symbol", "signal_date"], how="left")
L["yr"] = L.signal_date.str[:4]
# single-day intraday return of each day after signal: open(S+k)->close(S+k)
for k in range(1, 7): L[f"id{k}"] = (L[f"c{k}"] - L[f"o{k}"]) / L[f"o{k}"] * 100
# delayed entry d, hold HOLD days: open(S+d) -> close(S+d+HOLD-1)
for d in range(1, 5): L[f"de{d}"] = (L[f"c{d+HOLD-1}"] - L[f"o{d}"]) / L[f"o{d}"] * 100
print("returns computed\n", flush=True)

print("== SINGLE-DAY intraday return of each day after the signal (open->close), all fires ==")
print(f"  {'day':<8}{'win%':>8}{'avg%':>9}")
for k in range(1, 7):
    x = L[f"id{k}"].dropna()
    print(f"  S+{k:<6}{(x>0).mean()*100:>7.0f}%{x.mean():>+8.3f}%")
print(f"\n== DELAYED ENTRY: enter open(S+d), hold {HOLD} days, exit close(S+d+{HOLD-1}) ==")
print(f"  {'entry':<10}{'win%':>8}{'avg%':>9}{'avg 5x%':>10}")
for d in range(1, 5):
    x = L[f"de{d}"].dropna()
    print(f"  T+{d} (S+{d}){'':<1}{(x>0).mean()*100:>7.0f}%{x.mean():>+8.3f}%{x.mean()*5:>+9.2f}%")
print("\n== by YEAR (delayed-entry avg %, consistency check) ==")
print(f"  {'year':<7}" + "".join(f"{'T+'+str(d):>9}" for d in range(1, 5)))
for yr, g in L.groupby("yr"):
    print(f"  {yr:<7}" + "".join(f"{g[f'de{d}'].mean():>+8.2f}%" for d in range(1, 5)))
