#!/bin/sh
# KANIDA.AI entrypoint — initialise persistent DB volume then start API

DB_PATH="${KANIDA_DB_PATH:-/app/data/db/kanida_quant.db}"
DB_DIR=$(dirname "$DB_PATH")
BUNDLE="/app/data/db/_bundle/kanida_quant.db"

# If DB is missing (first deploy on a fresh volume), copy the bundled version
if [ ! -f "$DB_PATH" ]; then
    mkdir -p "$DB_DIR"
    if [ -f "$BUNDLE" ]; then
        echo "[entrypoint] Fresh volume detected — copying bundled DB to $DB_PATH"
        cp "$BUNDLE" "$DB_PATH"
    else
        echo "[entrypoint] No bundled DB found — starting with empty DB"
    fi
else
    echo "[entrypoint] DB exists at $DB_PATH ($(du -sh $DB_PATH | cut -f1))"
fi

exec sh -c "cd /app/backend && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
