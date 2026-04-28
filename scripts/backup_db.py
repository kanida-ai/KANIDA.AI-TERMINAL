"""
KANIDA.AI — SQLite DB backup
Usage:
    python scripts/backup_db.py              # backup to data/db/backups/
    python scripts/backup_db.py --keep 7     # keep only last 7 backups (default: 14)
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).parent.parent
DB_PATH = Path(os.environ.get("KANIDA_DB_PATH", ROOT / "data" / "db" / "kanida_quant.db"))
BACKUP_DIR = DB_PATH.parent / "backups"

KEEP = 14
for i, arg in enumerate(sys.argv):
    if arg == "--keep" and i + 1 < len(sys.argv):
        KEEP = int(sys.argv[i + 1])


def backup() -> Path:
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        sys.exit(1)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest    = BACKUP_DIR / f"kanida_quant_{stamp}.db"

    # Use SQLite online backup API — safe even if DB is being written
    src_conn  = sqlite3.connect(str(DB_PATH))
    dest_conn = sqlite3.connect(str(dest))
    src_conn.backup(dest_conn)
    dest_conn.close()
    src_conn.close()

    size_mb = dest.stat().st_size / 1_048_576
    print(f"Backup written: {dest}  ({size_mb:.1f} MB)")

    # Prune old backups
    backups = sorted(BACKUP_DIR.glob("kanida_quant_*.db"))
    while len(backups) > KEEP:
        old = backups.pop(0)
        old.unlink()
        print(f"Pruned old backup: {old.name}")

    return dest


if __name__ == "__main__":
    backup()
