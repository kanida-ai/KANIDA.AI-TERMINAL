#!/bin/sh
# ============================================================================
# entrypoint.sh — run the SQLite DBs on the container's LOCAL disk, not EFS.
#
# WHY: SQLite over EFS (NFS) pays a network round-trip + NFS file-lock cost on
# every read/write. Under live load (session tick/ws drivers doing frequent
# small writes) that latency + lock contention piles up, blocks the event loop,
# and the ALB /-health check times out -> ECS kills the task (crash-loop). It
# also caused "locking protocol" / "unable to open" failures. Running the DB on
# the task's LOCAL ephemeral disk removes the network + lock cost entirely
# (laptop-SSD-like), which is what makes it hold under trading load.
#
# PERSISTENCE: local disk is lost when the task stops, so we (a) copy the DBs
# from EFS -> local on boot, (b) sync local -> EFS every SYNC_SECS via SQLite's
# online-backup (consistent while the app writes), and (c) do a final sync on
# SIGTERM (graceful deploy/stop). Worst case (ungraceful crash) loses writes
# since the last periodic sync — acceptable for the bridge; the real end-state
# is RDS Postgres. The broker remains the source of truth for positions.
# ============================================================================
set -eu

EFS_DIR="${KANIDA_EFS_DB_DIR:-/data/db}"
LOCAL_DIR="${KANIDA_LOCAL_DB_DIR:-/localdb}"
# DBs the app WRITES (get synced back to EFS). Space-separated basenames.
WRITE_DBS="kanida_universe.db kanida_quant.db"
SYNC_SECS="${KANIDA_DB_SYNC_SECS:-300}"

mkdir -p "$LOCAL_DIR"

# ── Boot: copy EVERYTHING in the EFS db dir to local (app not running yet, so a
#    plain cp is consistent). This includes the read-only artifacts + the
#    rupeezy instrument master, so all serve-time reads are local/fast too.
echo "entrypoint: copying DB files $EFS_DIR -> $LOCAL_DIR ..."
for f in "$EFS_DIR"/*; do
  [ -f "$f" ] || continue
  cp -f "$f" "$LOCAL_DIR/" && echo "  copied $(basename "$f") ($(wc -c < "$f") bytes)"
done

# ── Point the app at the LOCAL copies (override the EFS paths from the task def).
export FALCON_DB_PATH="$LOCAL_DIR/kanida_universe.db"
export POWER_DB_PATH="$LOCAL_DIR/kanida_universe.db"
export KANIDA_DB_PATH="$LOCAL_DIR/kanida_quant.db"
export FALCON_OUTCOMES_ARTIFACT="$LOCAL_DIR/falcon_serve_evidence.db"
export FALCON_SIM_PATTERNS_ARTIFACT="$LOCAL_DIR/falcon_sim_patterns.db"
export RUPEEZY_INSTRUMENT_MASTER="$LOCAL_DIR/rupeezy_instruments.json"
echo "entrypoint: app DB paths -> $LOCAL_DIR (writes synced back to $EFS_DIR every ${SYNC_SECS}s)"

# ── Consistent local -> EFS sync of the WRITE DBs via SQLite online-backup.
sync_back() {
  for db in $WRITE_DBS; do
    src="$LOCAL_DIR/$db"; dst="$EFS_DIR/$db"
    [ -f "$src" ] || continue
    if python -c "import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()" \
         "$src" "$dst.tmp" 2>/dev/null; then
      mv -f "$dst.tmp" "$dst" && echo "entrypoint: synced $db -> EFS"
    else
      echo "entrypoint: WARN sync $db failed (will retry next cycle)"; rm -f "$dst.tmp" 2>/dev/null || true
    fi
  done
}

# periodic background sync
( while true; do sleep "$SYNC_SECS"; sync_back; done ) &
SYNC_LOOP_PID=$!

# ── Start the app in the background so we can trap SIGTERM for a final sync.
uvicorn main:app --host 0.0.0.0 --port "${PORT:-8001}" &
APP_PID=$!

on_term() {
  echo "entrypoint: SIGTERM — final sync then shutdown"
  kill "$SYNC_LOOP_PID" 2>/dev/null || true
  sync_back
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  exit 0
}
trap on_term TERM INT

wait "$APP_PID"
