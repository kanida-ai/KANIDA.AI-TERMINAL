"""
KANIDA — Initialize kanida_signals.db
Run once. Safe to rerun (all tables use CREATE IF NOT EXISTS).

Usage:
    python backend/scripts/init_signals_db.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backend.signals.db import init_db, SIGNALS_DB_PATH
import sqlite3

def main():
    print(f"\nInitializing kanida_signals.db")
    print(f"  Path: {SIGNALS_DB_PATH}\n")
    init_db()
    conn = sqlite3.connect(SIGNALS_DB_PATH)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    conn.close()
    print(f"  Tables created ({len(tables)}):")
    for t in tables:
        print(f"    {t}")
    print("\nDone. Next: python backend/scripts/backfill_signals.py")

if __name__ == "__main__":
    main()
