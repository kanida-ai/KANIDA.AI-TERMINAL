import sqlite3
db = r'C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\data\db\kanida_fingerprints.db'
conn = sqlite3.connect(db)

print('--- DISTINCT bias values ---')
for r in conn.execute('SELECT DISTINCT bias, COUNT(*) as n FROM fingerprints GROUP BY bias ORDER BY n DESC').fetchall():
    print(' ', r)

print()
print('--- DISTINCT timeframe values ---')
for r in conn.execute('SELECT DISTINCT timeframe, COUNT(*) as n FROM fingerprints GROUP BY timeframe ORDER BY n DESC').fetchall():
    print(' ', r)

print()
print('--- DISTINCT qualifies values ---')
for r in conn.execute('SELECT DISTINCT qualifies, COUNT(*) as n FROM fingerprints GROUP BY qualifies').fetchall():
    print(' ', r)

print()
print('--- Sample rows (first 5) ---')
for r in conn.execute('SELECT ticker, market, bias, timeframe, appearances, win_rate, qualifies FROM fingerprints LIMIT 5').fetchall():
    print(' ', r)

conn.close()
