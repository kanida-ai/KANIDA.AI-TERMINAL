import sqlite3
conn = sqlite3.connect("data/db/kanida_quant.db")
tables = [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
for name in tables:
    cols = [c[1] for c in conn.execute(f"PRAGMA table_info([{name}])").fetchall()]
    if "date" in cols:
        row = conn.execute(f"SELECT MAX(date) FROM [{name}]").fetchone()
        print(f"  {name}: latest date = {row[0]}")
conn.close()
