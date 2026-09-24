import pandas as pd, sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
nt = pd.read_excel(ROOT / "outputs" / "Trade_Log_Reconciliation_AfterFix.xlsx", "3_Signals_Not_Traded")
print("not-traded count:", len(nt))
print(nt[["date", "rank", "symbol", "reason"]].to_string(index=False))
con = sqlite3.connect(str(ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"))
print("\navg daily close (₹) of these symbols:")
for s in sorted(set(nt["symbol"])):
    r = con.execute("SELECT avg(close) FROM ohlc_daily WHERE symbol=? AND trade_date BETWEEN ? AND ?",
                    (s, "2024-05-14", "2026-05-11")).fetchone()
    print(f"  {s:14} ~Rs {(r[0] or 0):,.0f}")
con.close()
