#!/bin/bash
# Robust supervisor: every 10 min, ping (exit) on COMPLETE / process-death / stall.
# Detects PROCESS death (not just log staleness) so a startup crash can't slip past.
LOG="C:/Users/SPS/Documents/Kanida_Falcon/db/backfill_maxlookback.log"
ERR="C:/Users/SPS/Documents/Kanida_Falcon/db/backfill_maxlookback.launch"
while true; do
  sleep 600
  if grep -q "MAX-LOOKBACK BACKFILL COMPLETE" "$LOG" 2>/dev/null; then
    echo "STATUS=COMPLETE"; grep -E 'DONE:|COMPLETE' "$LOG" | tail -4; exit 0
  fi
  cnt=$(powershell -NoProfile -Command "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { \$_.CommandLine -like '*backfill_maxlookback*' }).Count" 2>/dev/null | tr -dc '0-9')
  [ -z "$cnt" ] && cnt=0
  if [ "$cnt" -eq 0 ]; then
    echo "STATUS=PROCESS_DEAD"; echo "--- stderr tail ---"; tail -8 "$ERR" 2>/dev/null; exit 2
  fi
  if [ -z "$(find "$LOG" -mmin -22 2>/dev/null)" ]; then
    echo "STATUS=STALLED_22MIN"; tail -6 "$LOG"; exit 3
  fi
done
