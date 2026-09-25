#!/usr/bin/env bash
# One-time catch-up after the Mac cutover (launchd com.kanida.catchup-once, first weekday 07:15 IST, after the 06:00
# Zerodha login). Runs ONCE - a done-marker stops it from repeating - and never mints a token itself.
#   1. F&O: re-fetch the recent sessions' 15-minute candles with OI (recovers 24-25 Sep as candles, before the
#      29 Sep expiry removes those contracts), then rebuild those two sessions' marks from the candles (labelled "seeded").
#   2. Stock OHLC: finish the daily/5m/1m refresh into db/kanida.db that the token loss stopped on 25 Sep.
# Snapshots (bid/ask, minute-by-minute) for 24-25 Sep cannot be recovered by any of this.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"; source "$HERE/config.sh"
FPY="$FALCON/market_scanner/.venv/bin/python"
DONE="$HOME/Kanida/logs/.run_at/catchup-once.done"
[ -e "$DONE" ] && { echo "[catchup] already done $(cat "$DONE") - nothing to do"; exit 0; }
cd "$FALCON" || exit 1
echo "[catchup] start $(date)"
"$FPY" -u -m market_data.derivatives.cli --log-file logs/derivatives_backfill_catchup.log backfill --rate 1 ; rc1=$?
for d in 2026-09-24 2026-09-25; do "$FPY" -u -m market_data.derivatives.cli seed --date "$d" > "logs/seed_$d.json" 2>&1; echo "[catchup] seed $d rc=$?"; done
cd "$FALCON/scripts" && KANIDA_NO_AUTH_MINT=1 "$FPY" -u fetch_universe.py >> "$FALCON/logs/fetch_universe_catchup.log" 2>&1 ; rc2=$?
echo "[catchup] backfill rc=$rc1 ohlc rc=$rc2 $(date)"
[ "$rc1" = 0 ] && [ "$rc2" = 0 ] && date +%F > "$DONE"
exit 0
