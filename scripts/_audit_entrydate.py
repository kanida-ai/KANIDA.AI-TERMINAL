import sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
c = sqlite3.connect(str(DB))
have = sorted(r[0] for r in c.execute("select distinct symbol from ohlc_1min"))
for needle in ("LTI", "GSP", "GUJ", "MIND", "GAS"):
    hits = [s for s in have if needle in s]
    print(f"  contains {needle!r}: {hits}")
# how many entry-day-symbol rows does ZOMATO->ETERNAL alias recover?
n = c.execute("""select count(*) from falcon_signal_day_study
        where engine_rank<=10 and entry_date>'2024-05-13' and symbol='ZOMATO'""").fetchone()[0]
print(f"\nZOMATO pick-rows in window (recovered via ETERNAL alias): {n}")
for s in ("GSPL", "LTIM"):
    n = c.execute("""select count(*) from falcon_signal_day_study
        where engine_rank<=10 and entry_date>'2024-05-13' and symbol=?""", (s,)).fetchone()[0]
    print(f"{s} pick-rows in window (will be dropped/reallocated): {n}")
