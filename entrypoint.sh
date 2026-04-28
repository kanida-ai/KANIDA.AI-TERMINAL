#!/bin/sh
# KANIDA.AI entrypoint — initialise persistent DB volume then start API

DB_PATH="${KANIDA_DB_PATH:-/app/data/db/kanida_quant.db}"
DB_DIR=$(dirname "$DB_PATH")
BUNDLE="/app/data/db_bundle/kanida_quant.db"

db_has_schema() {
    # Returns 0 (true) if DB has the instruments table
    python3 -c "
import sqlite3, sys
try:
    con = sqlite3.connect('$DB_PATH')
    rows = con.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name='instruments'\").fetchall()
    sys.exit(0 if rows else 1)
except Exception:
    sys.exit(1)
"
}

seed_db() {
    mkdir -p "$DB_DIR"
    if [ -f "$BUNDLE" ]; then
        echo "[entrypoint] Seeding DB from bundle -> $DB_PATH"
        cp "$BUNDLE" "$DB_PATH"
    else
        echo "[entrypoint] No bundle found at $BUNDLE — starting with empty DB"
    fi
}

if [ ! -f "$DB_PATH" ]; then
    echo "[entrypoint] Fresh volume — no DB found"
    seed_db
elif ! db_has_schema; then
    echo "[entrypoint] DB exists but schema is missing — reseeding from bundle"
    seed_db
else
    echo "[entrypoint] DB OK at $DB_PATH ($(du -sh "$DB_PATH" | cut -f1))"
fi

exec sh -c "cd /app/backend && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
