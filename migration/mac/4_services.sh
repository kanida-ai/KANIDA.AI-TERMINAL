#!/usr/bin/env bash
# Step 4: the Windows scheduled tasks as launchd agents (com.kanida.*).
#   bash 4_services.sh plan        show what would be installed (writes nothing)
#   bash 4_services.sh install     write ~/Library/LaunchAgents/com.kanida.*.plist and start them
#   bash 4_services.sh install-only <name...>   install just these jobs (e.g. the capture set; no broker-auth mint, no tunnel)
#   bash 4_services.sh status      what's loaded, last exit code
#   bash 4_services.sh uninstall   stop and remove every com.kanida.* agent
#   bash 4_services.sh logs <name> tail one job's log
#
# Only ONE machine may run the broker-auth / capture jobs at a time: two auth workers
# minting Kite/Vortex tokens can invalidate each other's session. Disable the Windows
# tasks first (MAC_MIGRATION.md step 5); `install` asks you to confirm that.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; source "$HERE/config.sh"
LA="$HOME/Library/LaunchAgents"; UIDN=$(id -u)
FPY="$FALCON/market_scanner/.venv/bin/python"; EPY="$ENGINE/.venv/bin/python"; TPY="$TERMINAL/.venv/bin/python"
RUNAT="$HERE/run_at.sh"
EL="$ENGINE/logs"

# name | schedule | working dir | command   (schedule: keepalive | every:<sec> | at:<TZ>:<HH:MM>:<days>)
JOBS=$(cat <<EOF
equity-prices|keepalive|$FALCON|exec "$FPY" -u -m market_data.live.cli --workers 4 run --interval 300
fno-capture|keepalive|$FALCON|exec "$FPY" -u -m market_data.derivatives.cli --log-file logs/derivatives_capture_service.log run
fno-metrics|keepalive|$FALCON|exec "$FPY" -u scripts/metrics_loop.py logs/metrics_loop_service.log
backend|keepalive|$ENGINE/backend|( for i in \$(seq 60); do curl -sf -m 2 -o /dev/null http://127.0.0.1:8001/openapi.json && break; sleep 1; done; "$EPY" "$ENGINE/scripts/warm_cache.py" >> "$EL/warmer.log" 2>&1 ) & exec "$EPY" -m uvicorn main:app --port 8001 --host 127.0.0.1 >> "$EL/backend.log" 2>&1
api-tunnel|keepalive|$HOME|exec cloudflared --config "$HOME/.cloudflared/config.yml" tunnel run kanida-api
keep-awake|keepalive|$K|exec /usr/bin/caffeinate -i -s
capture-watchdog|every:600|$FALCON|exec "$FPY" -u "$HERE/capture_watchdog.py"
zerodha-auth|every:1800|$ENGINE/backend|exec "$EPY" "$ENGINE/scripts/auth_worker.py" >> "$EL/auth_worker.log" 2>&1
vortex-auth|every:1800|$ENGINE/backend|exec "$EPY" "$ENGINE/scripts/vortex_auth_worker.py" >> "$EL/vortex_auth.log" 2>&1
mkt-poller|every:1800|$ENGINE/backend|exec "$EPY" -u "$ENGINE/scripts/mkt_poller.py" >> "$EL/mkt_poller.log" 2>&1
mkt-tick|every:1800|$ENGINE/backend|exec "$EPY" -u "$ENGINE/scripts/mkt_tick_capture.py" >> "$EL/mkt_tick.log" 2>&1
workflow-watchdog|every:900|$ENGINE/backend|exec "$EPY" "$ENGINE/scripts/workflow_watchdog.py" >> "$EL/workflow_watchdog.log" 2>&1
flow-eod|at:Asia/Kolkata:15:45:*|$ENGINE/scripts|exec "$RUNAT" Asia/Kolkata 15:45 '*' flow-eod -- "$EPY" -u "$ENGINE/scripts/flow_eod_report.py" >> "$EL/flow_eod.log" 2>&1
mkt-backfill|at:Asia/Kolkata:16:30:*|$ENGINE/backend|exec "$RUNAT" Asia/Kolkata 16:30 '*' mkt-backfill -- "$EPY" -u "$ENGINE/scripts/mkt_backfill_ohlc.py" >> "$EL/mkt_backfill.log" 2>&1
backend-restart|at:Asia/Kolkata:03:00:*|$K|exec "$RUNAT" Asia/Kolkata 03:00 '*' backend-restart -- /bin/launchctl kickstart -k gui/$UIDN/com.kanida.backend
weekly-learner|at:Asia/Kolkata:18:30:1|$ENGINE|exec "$RUNAT" Asia/Kolkata 18:30 1 weekly-learner -- "$EPY" "$ENGINE/scripts/tier_weekly_learner.py" >> "$DEPLOY/learner.log" 2>&1
nse-nightly|at:Asia/Kolkata:18:30:1-5|$TERMINAL|exec "$RUNAT" Asia/Kolkata 18:30 1-5 nse-nightly -- "$TPY" "$TERMINAL/backend/scripts/run_nightly_worker.py" --market NSE >> "$TERMINAL/logs/nightly_nse.log" 2>&1
us-nightly|at:America/Los_Angeles:17:00:1-5|$TERMINAL|exec "$RUNAT" America/Los_Angeles 17:00 1-5 us-nightly -- "$TPY" "$TERMINAL/backend/scripts/run_nightly_worker.py" --market US >> "$TERMINAL/logs/nightly_us.log" 2>&1
EOF
)

xml() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g'; }

plist() {  # name sched cwd cmd
  local name="$1" sched="$2" cwd="$3" cmd="$4" when
  case "$sched" in
    keepalive) when="<key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>30</integer>" ;;
    every:*)   when="<key>RunAtLoad</key><true/><key>StartInterval</key><integer>${sched#every:}</integer>" ;;
    at:*)      when="<key>RunAtLoad</key><true/><key>StartInterval</key><integer>300</integer>" ;;
  esac
  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.kanida.$name</string>
  <key>ProgramArguments</key><array><string>/bin/bash</string><string>-c</string><string>$(printf '%s' "$cmd" | xml)</string></array>
  <key>WorkingDirectory</key><string>$(printf '%s' "$cwd" | xml)</string>
  <key>EnvironmentVariables</key><dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>TZ</key><string>Asia/Kolkata</string>
    <key>PYTHONIOENCODING</key><string>utf-8</string>
    <key>PYTHONUNBUFFERED</key><string>1</string>
    <key>KANIDA_ENGINE_ROOT</key><string>$ENGINE</string>
    <key>KANIDA_AUTH_PYTHON</key><string>$EPY</string>
    <key>PLAYWRIGHT_BROWSERS_PATH</key><string>$HOME/Library/Caches/ms-playwright</string>
  </dict>
  $when
  <key>StandardOutPath</key><string>$LOGS/$name.log</string>
  <key>StandardErrorPath</key><string>$LOGS/$name.log</string>
</dict></plist>
EOF
}

each() { printf '%s\n' "$JOBS" | while IFS='|' read -r n s c cmd; do [ -n "$n" ] && "$@" "$n" "$s" "$c" "$cmd"; done; }

case "${1:-plan}" in
  plan)
    each bash -c 'printf "%-18s %-32s %s\n" "$0" "$1" "$2"' ;;
  install)
    for f in "$FPY" "$EPY" "$TPY"; do [ -x "$f" ] || die "missing $f — run 3_build_envs.sh first"; done
    read -r -p "Are the Windows KANIDA tasks disabled (MAC_MIGRATION.md step 5)? [y/N] " ok
    [ "$ok" = "y" ] || die "Disable them first — two machines minting broker tokens will fight."
    mkdir -p "$LA" "$LOGS" "$EL" "$TERMINAL/logs" "$FALCON/logs"; chmod +x "$RUNAT"
    write() { plist "$1" "$2" "$3" "$4" > "$LA/com.kanida.$1.plist"
              plutil -lint -s "$LA/com.kanida.$1.plist"
              launchctl bootout "gui/$UIDN/com.kanida.$1" 2>/dev/null || true
              launchctl bootstrap "gui/$UIDN" "$LA/com.kanida.$1.plist" && echo "   loaded com.kanida.$1"; }
    each write ;;
  install-only)
    shift; [ $# -gt 0 ] || die "name the jobs: $0 install-only fno-capture fno-metrics ..."
    for f in "$FPY" "$EPY"; do [ -x "$f" ] || die "missing $f - run 3_build_envs.sh first"; done
    for w in "$@"; do case "$w" in zerodha-auth|vortex-auth|api-tunnel|backend)
      read -r -p "$w mints broker tokens or takes live traffic. Is the Windows copy of it disabled? [y/N] " ok
      [ "$ok" = "y" ] || die "Disable it on Windows first.";; esac; done
    mkdir -p "$LA" "$LOGS" "$EL" "$FALCON/logs"; chmod +x "$RUNAT"
    printf '%s\n' "$JOBS" | while IFS='|' read -r n s_ c cmd; do
      for w in "$@"; do [ "$n" = "$w" ] || continue
        plist "$n" "$s_" "$c" "$cmd" > "$LA/com.kanida.$n.plist"; plutil -lint -s "$LA/com.kanida.$n.plist"
        launchctl bootout "gui/$UIDN/com.kanida.$n" 2>/dev/null || true
        launchctl bootstrap "gui/$UIDN" "$LA/com.kanida.$n.plist" && echo "   loaded com.kanida.$n"; done; done ;;
  status)
    launchctl list | awk 'NR==1 || /com\.kanida\./' ;;
  uninstall)
    for p in "$LA"/com.kanida.*.plist; do [ -e "$p" ] || continue; l=$(basename "$p" .plist)
      launchctl bootout "gui/$UIDN/$l" 2>/dev/null || true; rm -f "$p"; echo "   removed $l"; done ;;
  logs)
    tail -n 60 -f "$LOGS/${2:?name}.log" ;;
  *) die "usage: $0 plan|install|status|uninstall|logs <name>" ;;
esac
