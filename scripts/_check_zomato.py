import pandas as pd, sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
tl = pd.read_csv(ROOT / "outputs" / "Trailing_TradeLog_Exploded.csv")
con = sqlite3.connect(str(ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"))
for d in ("2024-11-18", "2025-07-22", "2025-09-09"):
    sig = con.execute("SELECT engine_rank, symbol FROM falcon_signal_day_study "
                      "WHERE persona='falcon_top10_daily' AND engine_rank<=5 AND entry_date=? "
                      "ORDER BY engine_rank", (d,)).fetchall()
    logged = list(tl[tl.date == d]["stock"])
    print(f"{d}\n  signal rank1-5: {sig}\n  logged stocks : {logged}\n")
con.close()
