import importlib.util
import sqlite3
from pathlib import Path

print('LIBRARIES', [(x, bool(importlib.util.find_spec(x))) for x in ['numpy', 'pandas', 'scipy', 'fastapi', 'uvicorn', 'pytest']])
con = sqlite3.connect(f'file:{(Path(__file__).resolve().parent.parent / "db/kanida.db").as_posix()}?mode=ro', uri=True)
tables = [x[0] for x in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print('TABLES', tables)
for table in ['ohlc_daily', 'ohlc_5min', 'ohlc_1min', 'instrument_labels', 'instruments']:
    if table not in tables:
        continue
    print('SCHEMA', table, con.execute('SELECT sql FROM sqlite_master WHERE name=?', (table,)).fetchone())
    print('INDEXES', con.execute('SELECT sql FROM sqlite_master WHERE tbl_name=? AND type=?', (table, 'index')).fetchall())
    con.row_factory = sqlite3.Row
    print('SAMPLE', [dict(x) for x in con.execute(f'SELECT * FROM {table} LIMIT 2')])
    con.row_factory = None
print('LABEL BREAKDOWN', con.execute('SELECT exchange, instrument_type, count(*) FROM instrument_labels GROUP BY 1,2').fetchall())
rows = con.execute("SELECT symbol, instrument_type, is_active, (SELECT max(bar_time) FROM ohlc_daily d WHERE d.symbol=l.symbol), (SELECT max(bar_time) FROM ohlc_5min i WHERE i.symbol=l.symbol) FROM instrument_labels l WHERE exchange='NSE' AND instrument_type IN ('STOCK','EQ')").fetchall()
from collections import Counter
print('COVERAGE', Counter((r[1], r[2], r[3][:10] if r[3] else None, r[4][:10] if r[4] else None) for r in rows))
print('LATEST', sorted(rows, key=lambda r:r[3] or '', reverse=True)[:8])
print('DAILY_TIMESTAMP', con.execute("SELECT bar_time FROM ohlc_daily WHERE symbol='RELIANCE' ORDER BY bar_time DESC LIMIT 4").fetchall())
print('INTRA_TIMESTAMP', con.execute("SELECT bar_time FROM ohlc_5min WHERE symbol='RELIANCE' ORDER BY bar_time DESC LIMIT 4").fetchall())
