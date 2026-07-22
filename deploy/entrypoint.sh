#!/bin/sh
# entrypoint (v2) -- run the SQLite DBs on the container LOCAL disk, not EFS.
# EFS/NFS pays a network round-trip + file-lock cost per op; under live load the
# session drivers pile that onto the event loop, the / health check times out,
# and ECS kills the task. Local disk removes that cost (laptop-SSD-like).
# Persistence: copy EFS->local on boot; sync local->EFS every SYNC_SECS and on
# SIGTERM via SQLite online-backup. Bridge until RDS Postgres; broker is truth.
# NOTE: no "set -e" on purpose -- a transient copy hiccup must not block boot;
# the app's own verify_falcon_db() fails loud if a DB is genuinely missing.

echo "entrypoint: v2 start (local-disk DB bridge)"

EFS_DIR="${KANIDA_EFS_DB_DIR:-/data/db}"
LOCAL_DIR="${KANIDA_LOCAL_DB_DIR:-/localdb}"
WRITE_DBS="kanida_universe.db kanida_quant.db"
SYNC_SECS="${KANIDA_DB_SYNC_SECS:-300}"

mkdir -p "$LOCAL_DIR"

echo "entrypoint: copying DB files $EFS_DIR -> $LOCAL_DIR"
for f in "$EFS_DIR"/*; do
  [ -f "$f" ] || continue
  cp -f "$f" "$LOCAL_DIR/" && echo "  copied $(basename "$f")"
done

export FALCON_DB_PATH="$LOCAL_DIR/kanida_universe.db"
export POWER_DB_PATH="$LOCAL_DIR/kanida_universe.db"
export KANIDA_DB_PATH="$LOCAL_DIR/kanida_quant.db"
export FALCON_OUTCOMES_ARTIFACT="$LOCAL_DIR/falcon_serve_evidence.db"
export FALCON_SIM_PATTERNS_ARTIFACT="$LOCAL_DIR/falcon_sim_patterns.db"
export RUPEEZY_INSTRUMENT_MASTER="$LOCAL_DIR/rupeezy_instruments.json"
# R&D DB is absent in the cloud; the serving DB carries ohlc_daily + features +
# signals + taxonomy, so point the "R&D" read path (Top-10/Top-50 explainer,
# persona sim, admin market-data) at the local serving DB. (#3 serve-time dep.)
export POWER_RND_DB_PATH="$LOCAL_DIR/kanida_universe.db"
# Cloud mints its OWN Zerodha token in-process (Playwright + TOTP, creds from
# Secrets) — no laptop. The laptop's KanidaZerodhaAuth task must be DISABLED when
# this is on (one minter per Kite account).
export FALCON_INPROCESS_AUTH="true"
echo "entrypoint: app DB paths -> $LOCAL_DIR (sync back every ${SYNC_SECS}s)"

sync_back() {
  for db in $WRITE_DBS; do
    [ -f "$LOCAL_DIR/$db" ] || continue
    if python -c "import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()" "$LOCAL_DIR/$db" "$EFS_DIR/$db.tmp" 2>/dev/null; then
      mv -f "$EFS_DIR/$db.tmp" "$EFS_DIR/$db" && echo "entrypoint: synced $db -> EFS"
    else
      rm -f "$EFS_DIR/$db.tmp" 2>/dev/null
      echo "entrypoint: WARN sync $db failed (retry next cycle)"
    fi
  done
}

( while true; do sleep "$SYNC_SECS"; sync_back; done ) &
SYNC_LOOP_PID=$!

uvicorn main:app --host 0.0.0.0 --port "${PORT:-8001}" &
APP_PID=$!

on_term() {
  echo "entrypoint: SIGTERM -- final sync then shutdown"
  kill "$SYNC_LOOP_PID" 2>/dev/null
  sync_back
  kill -TERM "$APP_PID" 2>/dev/null
  wait "$APP_PID" 2>/dev/null
  exit 0
}
trap on_term TERM INT

wait "$APP_PID"
