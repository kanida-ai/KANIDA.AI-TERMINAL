"""Step 0 audit for the 95%-hit-rate / Top-25 research run. Read-only."""
import sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
c = sqlite3.connect(str(DB))


def schema(t):
    print(f"\n--- {t} ---")
    cols = c.execute(f"PRAGMA table_info({t})").fetchall()
    if not cols:
        print("   (no such table)"); return
    for cid, name, typ, *_ in cols:
        print(f"    {name:32} {typ}")


# 1) Confirm ranked-history depth in the canonical table.
print("=== falcon_signal_day_study rank depth (persona falcon_top10_daily) ===")
print("  max rank:", c.execute(
    "select max(engine_rank) from falcon_signal_day_study where persona='falcon_top10_daily'").fetchone()[0])
print("  rows-per-date distribution (top few):")
for r in c.execute("""select cnt, count(*) ndates from (
        select signal_date, count(*) cnt from falcon_signal_day_study
        where persona='falcon_top10_daily' group by signal_date) group by cnt order by cnt desc limit 8"""):
    print("    picks/date =", r[0], " -> ", r[1], "dates")

# 2) Is engine_rank ordered by avg_lift or sum_lift? Inspect one recent date.
print("\n=== ordering check: one recent signal_date, all rows ===")
sd = c.execute("""select max(signal_date) from falcon_signal_day_study
                  where persona='falcon_top10_daily'""").fetchone()[0]
print("  date:", sd)
for r in c.execute("""select engine_rank, symbol, avg_lift, sum_lift, n_fires
        from falcon_signal_day_study where persona='falcon_top10_daily' and signal_date=?
        order by engine_rank""", (sd,)):
    print("   ", r)

# 3) Candidate score sources for reconstructing the FULL-universe ranking.
for t in ("falcon_pattern_contributions", "falcon_features", "ohlc_daily",
          "falcon_signal_day_context"):
    schema(t)

# 4) Coverage of contribution/feature tables (can we rank full universe per day?)
print("\n=== falcon_pattern_contributions coverage ===")
try:
    r = c.execute("""select count(*), count(distinct signal_date),
            min(signal_date), max(signal_date),
            count(distinct symbol) from falcon_pattern_contributions""").fetchone()
    print("   rows,dates,min,max,symbols:", r)
except Exception as e:
    print("   err:", e)
