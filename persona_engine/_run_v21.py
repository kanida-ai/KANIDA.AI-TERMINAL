"""Final integrated v2.1 run: build event features (earnings+delivery), then re-run
the two-stage engine with the new non-price signals and print the summary."""
import warnings; warnings.filterwarnings("ignore")
from persona_engine import db, event_features, engine_v2, universe

con = db.connect()
n = event_features.build(con, start="2022-01-01")
print("persona_event_features rows:", n)
print("  earn_next1=1:", con.execute("SELECT SUM(earn_next1) FROM persona_event_features").fetchone()[0])
print("  w/ delivery:", con.execute("SELECT COUNT(*) FROM persona_event_features WHERE deliv_pct IS NOT NULL").fetchone()[0])
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
print("\n--- v2.1 (with earnings + delivery) ---")
engine_v2.run_v2(con, fo, start="2024-01-01", end=None, intraday_only=False, verbose=True)
con.close()
print("V21_DONE")
