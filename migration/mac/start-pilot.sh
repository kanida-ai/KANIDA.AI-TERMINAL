#!/usr/bin/env bash
# macOS port of kanida-app/scripts/start-pilot.ps1.   bash start-pilot.sh [port] [--build]
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/config.sh"
PORT="${1:-8082}"; APP="$FALCON/kanida-app"; PY="$APP/.pilot-venv/bin/python"
cd "$APP"
[ -x "$PY" ] || die "Create .pilot-venv first (3_build_envs.sh)."
if [ "${2:-}" = "--build" ] || [ "${1:-}" = "--build" ]; then npx expo export --platform web --output-dir dist-pilot; fi
[ -f dist-pilot/index.html ] || die "Build the web app first: start-pilot.sh $PORT --build"
lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 && die "Port $PORT already has a listener."
mkdir -p var
export PYTHONPATH="$APP/server" PILOT_WEB_DIRECTORY="$APP/dist-pilot" PILOT_PORT="$PORT" PILOT_BIND=0.0.0.0
"$PY" -m kanida_pilot.bootstrap
nohup "$PY" -m kanida_pilot > var/pilot.stdout.log 2> var/pilot.stderr.log &
echo $! > var/pilot-launcher.pid
echo "Pilot starting at http://127.0.0.1:$PORT/welcome. Private owner invitation: var/OWNER_INVITATION.txt"
