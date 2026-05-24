#!/bin/sh
# Railway cron entrypoint. Receives the job name via $FALCON_JOB env var.
# Single-shot: runs the job, exits with status code.
#
# Configure 4 separate cron services in Railway (or one with multiple schedules):
#   FALCON_JOB=daily_data_refresh   schedule: 30 11 * * 1-5   (16:30 IST = 11:00 UTC)
#   FALCON_JOB=daily_features       schedule: 32 11 * * 1-5   (16:32 IST)
#   FALCON_JOB=daily_signals        schedule: 35 11 * * 1-5   (16:35 IST)
#   FALCON_JOB=weekly_remine        schedule: 30 12 * * 0     (Sun 18:00 IST)

set -e

JOB="${FALCON_JOB:-daily_signals}"
echo "[cron] Falcon job: $JOB at $(date -u)"

cd /app/backend
exec python3 -m falcon.jobs.${JOB}
