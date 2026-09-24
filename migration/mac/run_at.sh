#!/usr/bin/env bash
# Run a command once per day at a wall-clock time in a given time zone.
# launchd calls this every 5 minutes; it exits quietly unless it's time.
#   run_at.sh <TZ> <HH:MM> <days: 1-5 | 1 | *> <name> -- <command...>
# Late start (Mac was asleep) still runs, up to 3 hours after the slot.
set -u
tz="$1" at="$2" days="$3" name="$4"; shift 5
now=$(TZ="$tz" date +%H:%M); dow=$(TZ="$tz" date +%u); today=$(TZ="$tz" date +%F)
case "$days" in
  '*') ;;
  *-*) [ "$dow" -ge "${days%-*}" ] && [ "$dow" -le "${days#*-}" ] || exit 0 ;;
  *) [ "$dow" = "$days" ] || exit 0 ;;
esac
m() { echo $(( 10#${1%:*} * 60 + 10#${1#*:} )); }
d=$(( $(m "$now") - $(m "$at") )); [ "$d" -ge 0 ] && [ "$d" -le 180 ] || exit 0
stamp="$HOME/Kanida/logs/.run_at/$name"; mkdir -p "$(dirname "$stamp")"
[ "$(cat "$stamp" 2>/dev/null)" = "$today" ] && exit 0
echo "$today" > "$stamp"
echo "[run_at] $name firing at $now $tz ($(date))"
exec "$@"
