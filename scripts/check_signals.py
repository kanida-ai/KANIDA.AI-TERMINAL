import sqlite3
DB = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_quant.db"
conn = sqlite3.connect(DB)

# Check live_opportunities
cols = [c[1] for c in conn.execute("PRAGMA table_info(live_opportunities)").fetchall()]
print("live_opportunities cols:", cols)
rows = conn.execute("SELECT * FROM live_opportunities ORDER BY rowid DESC LIMIT 20").fetchall()
print(f"\nLive opportunities ({len(rows)}):")
for r in rows:
    print(" ", r)

conn.close()
