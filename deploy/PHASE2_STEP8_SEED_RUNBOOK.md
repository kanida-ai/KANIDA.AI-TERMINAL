# PHASE 2 · STEP 8 — Seed the serving DB onto EFS

**Goal:** put the 4 serving-slice files onto the EFS volume at `/data/db` so the
cloud app boots (A2 fail-loud passes) and never reaches for the 38 GB R&D DB.

**Seed contents** (packaged into `kanida-seed.tar.gz`, 223 MB gz / 735 MB raw):

| file (→ /data/db/) | size | how made | flag it satisfies |
|---|---|---|---|
| `kanida_universe.db` | 636 MB | `VACUUM INTO` (compacted hot copy of live) | FALCON_DB_PATH + POWER_DB_PATH |
| `kanida_quant.db` | 80 MB | `VACUUM INTO` | KANIDA_DB_PATH |
| `falcon_serve_evidence.db` | 54 MB | copy (leg-1 artifact) | FALCON_OUTCOMES_ARTIFACT |
| `falcon_sim_patterns.db` | 0.3 MB | copy (leg-2 artifact) | FALCON_SIM_PATTERNS_ARTIFACT |
| `SHA256SUMS.txt` | — | integrity manifest | (verified in-container) |

Local snapshot was read-only — the live Monday DB was never modified.

## Mechanism (zero new infra)
Seed by running ONE off task of the EXISTING `kanida-prod-app` task definition,
with its command overridden to download+untar into the already-mounted EFS
(`/data/db`). Download uses a short-lived S3 **presigned URL** via stdlib
`urllib` (image has no awscli/boto3; this needs no new IAM). Integrity checked
in-container with `sha256sum -c`.

## 8A — Upload the tarball to S3
AWS Console → S3 → bucket `kanida-prod-artifacts-389642461326` → **Create folder**
`seed` → open it → **Upload** → add `C:\Users\SPS\Desktop\kanida-seed.tar.gz` →
Upload. (Key must end up as `seed/kanida-seed.tar.gz`.)

## 8B — Seed EFS (CloudShell)
Create the script:
```bash
cat > ~/seed_efs.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""
REGION=ap-south-1
CLUSTER=kanida-prod-cluster
TASKDEF=kanida-prod-app
CONTAINER=app
BUCKET=kanida-prod-artifacts-389642461326
KEY=seed/kanida-seed.tar.gz
SUBNETS="subnet-0d9bb03d5128f1f83,subnet-0ef882670447608e9"
SG=sg-00a155bbb2c054ede
LOGGROUP=/ecs/kanida-prod-app

URL="$(aws s3 presign "s3://$BUCKET/$KEY" --expires-in 3600 --region $REGION)"
echo "presigned URL ready (len ${#URL})"

python3 - "$URL" > /tmp/ov.json <<'PY'
import json, sys
url = sys.argv[1]
cmd = ('python -c "import os,urllib.request; urllib.request.urlretrieve(os.environ[\'SEED_URL\'],\'/tmp/s.tgz\')" '
       '&& tar xzf /tmp/s.tgz -C /data/db && rm -f /tmp/s.tgz '
       '&& cd /data/db && sha256sum -c SHA256SUMS.txt && ls -la /data/db')
print(json.dumps({"containerOverrides":[{"name":"app","command":["sh","-lc",cmd],
      "environment":[{"name":"SEED_URL","value":url}]}]}))
PY

TASK_ARN="$(aws ecs run-task --cluster $CLUSTER --task-definition $TASKDEF \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --overrides file:///tmp/ov.json --region $REGION \
  --query 'tasks[0].taskArn' --output text)"
echo "task started: $TASK_ARN"
echo "waiting for it to finish (pulls image + downloads seed, ~2-4 min)..."
aws ecs wait tasks-stopped --cluster $CLUSTER --tasks "$TASK_ARN" --region $REGION
echo "=== result ==="
aws ecs describe-tasks --cluster $CLUSTER --tasks "$TASK_ARN" --region $REGION \
  --query 'tasks[0].containers[0].{exitCode:exitCode,reason:reason}' --output table
TASK_ID="${TASK_ARN##*/}"
echo "=== container log ==="
aws logs get-log-events --log-group-name $LOGGROUP \
  --log-stream-name "app/$CONTAINER/$TASK_ID" --region $REGION \
  --limit 200 --query 'events[].message' --output text || echo "(logs not ready; re-run the get-log-events line)"
SH
echo "seed_efs.sh written"
```
Run it:
```bash
bash ~/seed_efs.sh
```

**Success:** `exitCode = 0`, and the log shows every file `... OK` from
`sha256sum -c`, plus an `ls -la /data/db` listing all 4 `.db` files.

## Step 9 (NEXT, not here)
Add artifact-flag env to the task def (terraform `modules/compute`):
`FALCON_OUTCOMES_ARTIFACT=/data/db/falcon_serve_evidence.db`,
`FALCON_SIM_PATTERNS_ARTIFACT=/data/db/falcon_sim_patterns.db`. Then set
`app_desired_count=1`, `terraform apply`, and smoke-test via the ALB DNS with
all live-trading gates OFF. No DNS cutover.
