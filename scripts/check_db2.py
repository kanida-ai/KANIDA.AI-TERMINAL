import sqlite3, sys
DB = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_quant.db"
conn = sqlite3.connect(DB)
key_tables = {
    "ohlc_daily":    ["date", "trade_date", "candle_date"],
    "trade_log":     ["signal_date", "entry_date", "date"],
    "signal_events": ["date", "signal_date", "event_date"],
    "ingestion_log": ["date", "run_date", "fetched_date"],
}
for table, candidates in key_tables.items():
    cols = [c[1] for c in conn.execute(f"PRAGMA table_info([{table}])").fetchall()]
    print(f"\n{table} columns: {cols[:10]}")
    for col in candidates:
        if col in cols:
            row = conn.execute(f"SELECT MAX([{col}]) FROM [{table}]").fetchone()
            print(f"  MAX({col}) = {row[0]}")
            break
conn.close()
