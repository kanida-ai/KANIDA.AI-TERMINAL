# Phase 2 & 3 — Stand up the ONE cloud server (copy-paste runbook)

**What this is:** the numbered, plain-language guide that takes you from
"Phase-1 `terraform plan` is clean" to **one Fargate server in AWS running the
backend off an EFS-mounted copy of the production database** — so all ~10 users
can be served from the cloud instead of the laptop. No Terraform knowledge
needed; every command is copy-paste.

> **Honesty note (read this):** everything here was **authored on a machine with
> no Terraform, no Docker, and no AWS access** — so it has **never been run**.
> Individual steps have "what success looks like" and "paste this back to Claude
> if it fails" so we fix residual issues together. That is expected and fine.

> **The GOLDEN RULE about money.** Phase 1 (`init`/`validate`/`plan`) created
> **nothing** and cost nothing. **Phase 2 is where real, billable AWS resources
> get created.** The single command that does it is **`terraform apply`** — it is
> called out in BIG letters below. Everything before that first `apply` is still
> free/reversible.

> **The OTHER golden rule — laptop stays primary.** Nothing in this runbook
> points real users or DNS at the cloud. The laptop remains the live trading
> system the whole time. The cloud server boots in **paper / gates-OFF** mode so
> we can prove it matches the laptop before anyone relies on it. See the
> **guardrail** at the very end.

---

## Before you start — the map of what happens

```
PHASE 2  (creates cloud resources + costs money)
  1. Back up the VAULT KEY off-cloud            (free, do FIRST)
  2. Create the ECR image repository            (free)
  3. Build the app image + push to ECR          (free-ish; CloudShell or CodeBuild)
  4. Point terraform at the image, tasks = 0    (free; edit tfvars)
  5. Review the plan                            (free; terraform plan)
  6. >>> terraform apply <<<                    (THE MONEY STEP — creates EFS, secrets, ALB, RDS, etc.)
  7. Set the secret VALUES (vault key first)    (all of them — see the gotcha)
  8. Seed the prod DB onto the empty EFS        (laptop -> S3 -> one-off ECS task)
  9. Flip tasks = 1 and apply again             (launches the app against the seeded DB)

PHASE 3  (prove it, keep it safe)
 10. Confirm the task booted + A2 preflight passed + health endpoint answers
 11. Keep ALL live-trading gates OFF; check paper-parity vs the laptop
 GUARDRAIL. Do NOT cut over DNS. Laptop stays primary.
```

**Why the "tasks = 0 first, tasks = 1 later" dance?** The moment the ECS service
has `desired_count = 1`, it tries to start the app — and the app's A2 safety gate
**refuses to boot** if the database volume is empty (that is the whole point of
the gate). So we create everything with **0** app tasks running, quietly seed the
database onto the EFS volume, and only THEN turn the app on. This avoids a
crash-loop and is much calmer to watch.

---

## Prerequisites (confirm before Phase 2)

- Phase 1 passed: `terraform validate` = `Success!` and `terraform plan` =
  `Plan: NN to add, 0 to change, 0 to destroy` in **ap-south-1**.
- You are working in the **same `terraform` folder** where Phase 1 ran, and its
  remote state / backend is set up (if you used local state in CloudShell, keep
  using the SAME CloudShell so the state file persists — losing state loses track
  of what was created).
- You have the **production `kanida_universe.db`** (~683 MB) available on the
  laptop (it lives at `data/db/kanida_universe.db` in the live repo).
- HTTPS is **optional** for this runbook. Without an ACM certificate the app is
  reachable only over the internal HTTP listener (fine for the paper-parity
  check; you are NOT exposing it publicly yet).

---

# PHASE 2

## Step 1 — Back up the VAULT KEY off-cloud (do this FIRST, it's free)

`FALCON_VAULT_KEY` is the single most important secret. It decrypts every user's
broker credentials **and** their per-account egress-proxy URLs. **If it is ever
lost or set wrong, accounts silently fall back to direct/rejected egress — a
quiet real-money failure, not a loud crash.**

1. On the laptop, open `config/.env` and copy the **exact** value of
   `FALCON_VAULT_KEY`.
2. Store it in **two** durable places that are NOT this cloud account and NOT
   git — e.g. a password manager entry AND an offline note. This is your
   break-glass copy.

**Success looks like:** you can read `FALCON_VAULT_KEY` back from two places that
do not depend on AWS or the laptop being alive.

> **If you can't find `FALCON_VAULT_KEY` in `config/.env`:** STOP and paste back
> the list of keys you DO see (names only, never the values). Do not invent one —
> a fresh key would orphan every already-encrypted credential.

---

## Step 2 — Create the image repository in ECR (free)

The app runs from a container image. It lives in ECR (Elastic Container
Registry). Terraform does **not** create the repo (by design), so make it once:

```bash
aws ecr create-repository --repository-name kanida-backend --region ap-south-1
```

**Success looks like:** JSON that includes a `"repositoryUri"` ending in
`.../kanida-backend`. **Copy that URI** — you need it in Steps 3 and 4.

> **If it says the repository already exists:** that's fine — get the URI with
> `aws ecr describe-repositories --repository-names kanida-backend --region ap-south-1`.
> **Any other error:** paste it back with "Step 2".

---

## Step 3 — Build the app image and push it to ECR

> **⚠️ ASSUMPTION I could NOT verify (flag):** *"AWS CloudShell has Docker."* As
> of late-2023 AWS CloudShell **does** ship Docker — BUT CloudShell has a **small
> disk** (~1 GB persistent home + limited scratch), and this image bundles
> **Chromium** (for the Zerodha headless login), so the build may run out of disk
> with `no space left on device`. **The repo's own older Phase-0 note said
> "CloudShell has no Docker" — that is now out of date, but the disk limit is
> real.** Try CloudShell (Option A); if it runs out of space, use CodeBuild
> (Option B). I have **not** been able to test either.

### Option A — build in CloudShell (try first)

Get the `deploy/` folder into CloudShell (same zip-upload trick as Phase 1, but
you need the whole `deploy/` folder this time, not just `terraform/` — the build
needs the `Dockerfile`, `requirements.txt`, `backend/`, and `scripts/`).

```bash
# from inside the folder that contains deploy/Dockerfile:
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=ap-south-1
REPO="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/kanida-backend"

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
docker build -f deploy/Dockerfile -t kanida-backend:phase2 .
docker tag kanida-backend:phase2 "$REPO:phase2"
docker push "$REPO:phase2"
```

**Success looks like:** the `docker push` ends with a `sha256:...` digest and a
size, and `aws ecr list-images --repository-name kanida-backend --region ap-south-1`
shows the `phase2` tag. **Write down `"$REPO:phase2"`** — that's your
`container_image` for Step 4.

> **If `docker build` fails with `no space left on device`:** stop and use
> Option B (CodeBuild). Paste back "Step 3 out of disk".
> **If `docker` is "command not found":** CloudShell's Docker isn't available in
> your session — use Option B. Paste back "Step 3 no docker".
> **Any other build error:** paste the last ~30 lines with "Step 3 build".

### Option B — build with CodeBuild (fallback, handles big images)

If CloudShell can't build it, CodeBuild (a managed build service) can. This needs
a small one-time setup (a CodeBuild project + a `buildspec.yml`). **Tell Claude
"I need the CodeBuild fallback for Step 3"** and paste back your account id and
the ECR repo URI — Claude will hand you the exact project definition and
`buildspec.yml` to paste. (Not pre-written here to keep this runbook to the happy
path.)

> **Do NOT** try to build this on the laptop unless it has Docker Desktop — the
> authoring note says Docker is not installed there.

---

## Step 4 — Point Terraform at the image, with tasks = 0 (free)

Edit `terraform.tfvars` (create it from `terraform.tfvars.example` if you haven't):

```hcl
container_image   = "PASTE_THE_REPO:phase2_URI_FROM_STEP_3"
app_desired_count = 0     # <-- IMPORTANT: 0 for now, so the app does NOT try to boot yet
```

Leave everything else at the example defaults (Mumbai, single NAT, the EFS block,
`egress_users = []`).

**Success looks like:** `terraform.tfvars` has your real image URI and
`app_desired_count = 0`.

---

## Step 5 — Review the plan (free, creates nothing)

```bash
terraform plan
```

**Success looks like:** near the bottom, `Plan: NN to add, 0 to change,
0 to destroy` with **no `Error:`**. Compared to your clean Phase-1 plan you should
now ALSO see these **new** resources (the EFS work):

- `aws_efs_file_system.this`
- `aws_security_group.efs`
- `aws_efs_mount_target.this[0]` and `[1]` (one per AZ)
- `aws_efs_access_point.db`

and **one FEWER** secret (`FALCON_JWT_SECRET` was removed — nothing reads it).

> **Sanity-scan the plan for:** the ECS task definition showing a `volume` named
> `db` with an `efs_volume_configuration`, and the container `mountPoints`
> pointing at `/data/db`. If those are missing, paste back "Step 5 no efs volume".
> **If `plan` errors:** copy the whole `Error:` block(s) and paste with "Step 5".

---

## Step 6 — >>> `terraform apply` <<< (THE MONEY STEP)

> **🛑 STOP AND READ.** This is the command that **creates real, billable AWS
> resources**: the EFS filesystem, an ALB, an RDS Multi-AZ database, an
> ElastiCache Redis, a NAT gateway, the ECS cluster, CloudWatch logs, KMS key,
> and the (empty) Secrets Manager entries. Rough order-of-magnitude cost is **tens
> of dollars per month and up**, dominated by RDS Multi-AZ + NAT + the running
> task. Once you run this you are paying for infrastructure until you
> `terraform destroy` it.

First, review one more time and type the confirmation **yourself** (do not use
`-auto-approve`):

```bash
terraform apply
```

Terraform prints the same plan and then asks:
`Do you want to perform these actions?` — read the summary line, and if it says
`NN to add, 0 to change, 0 to destroy`, type **`yes`** and Enter.

**Success looks like:** it churns for several minutes (RDS is the slow one, ~10
min) and ends with `Apply complete! Resources: NN added, 0 changed, 0 destroyed.`
followed by the **Outputs:** block. **Copy the whole Outputs block** — you need
`efs_file_system_id`, `efs_access_point_id`, `artifacts_bucket`,
`ecs_cluster_name`, `app_task_definition_family`, `private_subnet_ids`, and
`app_security_group_id` for the next steps.

> **If `apply` fails partway:** this is the most likely place for a real-AWS
> issue (an RDS engine version no longer offered, a service quota, an EIP limit).
> Copy the whole `Error:` block and paste with "Step 6 apply". Terraform is safe
> to re-run after a fix — it continues from where it stopped. Because
> `app_desired_count = 0`, no app task is running yet, so a partial apply is calm.

---

## Step 7 — Set the secret VALUES (vault key first)

Terraform created **empty** secret placeholders. They have **no value yet**.

> **⚠️ CRITICAL GOTCHA (this WILL bite if skipped):** ECS refuses to start a task
> if ANY secret the task injects has no value. The next steps launch tasks, so
> **every** secret in the list below must get a value **now** — even ones you
> don't have a real value for yet (give those a temporary placeholder like
> `unset` or `false`). A single empty secret = `ResourceInitializationError:
> unable to retrieve secret` and the task never starts.

Set the **vault key first** (it must be the exact value you backed up in Step 1):

```bash
aws secretsmanager put-secret-value \
  --secret-id kanida-prod/env/FALCON_VAULT_KEY \
  --secret-string 'PASTE_THE_EXACT_VAULT_KEY' \
  --region ap-south-1
```

Then set the rest. The full list of secret names is in `deploy/SECRETS_MAP.md`
(the "Migration order" section is the order to do them in). For the paper-parity
boot you need real values for the auth + broker + API keys and can use safe
placeholders for the daily-expiring token and the live gate:

```bash
# real values (copy from the laptop's config/.env):
#   POWER_JWT_SECRET, POWER_ADMIN_SECRET, ADMIN_SECRET, FALCON_OPERATOR_TOKEN,
#   KITE_API_KEY, KITE_API_SECRET, ZERODHA_USERNAME, ZERODHA_PASSWORD,
#   ZERODHA_TOTP_SECRET, ANTHROPIC_API_KEY, POLYGON_API_KEY,
#   VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_CONTACT,
#   BROKER_PROXY_MAP, BROKER_EGRESS_POOL
# safe placeholders for the paper boot:
aws secretsmanager put-secret-value --secret-id kanida-prod/env/KITE_ACCESS_TOKEN     --secret-string 'refresh-me-daily' --region ap-south-1
aws secretsmanager put-secret-value --secret-id kanida-prod/env/RUPEEZY_LIVE_CERTIFIED --secret-string 'false'            --region ap-south-1
```

Repeat the `put-secret-value` command (swap `--secret-id` and `--secret-string`)
for **every** name in the managed list. To see which ones still have no value:

```bash
for k in $(aws secretsmanager list-secrets --region ap-south-1 \
  --query "SecretList[?starts_with(Name,'kanida-prod/env/')].Name" --output text); do
  v=$(aws secretsmanager get-secret-value --secret-id "$k" --region ap-south-1 --query SecretString --output text 2>/dev/null)
  [ -z "$v" ] && echo "STILL EMPTY: $k"
done
```

**Success looks like:** the loop above prints **nothing** (no "STILL EMPTY"
lines). Every managed secret has a value.

> **If any secret stays empty:** paste back the list of "STILL EMPTY" names
> (names only). A task launched while one is empty fails at start with
> `unable to retrieve secret`.

---

## Step 8 — Seed the production DB onto the (empty) EFS

The EFS volume exists but is empty, so the app can't boot against it yet. We copy
the prod DB onto it **once**, using the safest path: laptop → S3 → a one-off ECS
task that mounts the same EFS and writes the file. **The laptop cannot reach EFS
directly** (it lives inside the private VPC), which is why we stage through S3.

### 8a — Make a clean copy of the DB on the laptop (safe while the app runs)

SQLite's `.backup` takes a **consistent** copy even while the live app is using
the DB (do NOT just copy the `.db` file — that misses the `-wal`):

```powershell
# on the laptop, adjust the path to the live repo:
sqlite3 "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_universe.db" ".backup seed_kanida_universe.db"
```

**Success looks like:** a `seed_kanida_universe.db` file (~683 MB) appears with no
error.

> **If `sqlite3` isn't on the laptop:** paste back "Step 8a no sqlite3" — Claude
> gives you a one-line Python alternative that does the same safe backup.

### 8b — Upload it to the artifacts S3 bucket

Easiest: the **S3 console** (no CLI needed on the laptop).

1. AWS console → S3 → open the bucket named in your `artifacts_bucket` output.
2. Create a folder `seed/`, open it, **Upload** → add `seed_kanida_universe.db`
   → Upload. (683 MB is well within console limits; leave encryption at the
   bucket default.)

**Success looks like:** the object shows as `seed/kanida_universe.db` (rename it
on upload if needed so the key is exactly that).

### 8c — Make a temporary download link (in CloudShell)

```bash
BUCKET=$(terraform output -raw artifacts_bucket)
aws s3 presign "s3://$BUCKET/seed/kanida_universe.db" --expires-in 3600 --region ap-south-1
```

**Success looks like:** a long `https://...` URL is printed. It works for 1 hour.
**Copy it** for the next sub-step.

> **If the seeder later logs a `403`:** the link likely expired (re-run 8c) or is
> a KMS-permission issue — paste back "Step 8 presign 403".

### 8d — Run the one-off seeder task

This reuses the app's task definition but **overrides the command** to just
download the DB (it never starts the web app, so A2 does not run). It writes as
uid 10001 into the EFS-mounted `/data/db`, matching the access point.

Create the overrides file (paste your presigned URL where shown):

```bash
cat > seed-overrides.json <<'JSON'
{
  "containerOverrides": [
    {
      "name": "app",
      "command": [
        "python", "-c",
        "import urllib.request,os; urllib.request.urlretrieve('PASTE_PRESIGNED_URL_HERE','/data/db/kanida_universe.db'); print('SEEDED_BYTES', os.path.getsize('/data/db/kanida_universe.db'))"
      ]
    }
  ]
}
JSON
```

Then launch it:

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)
TASKDEF=$(terraform output -raw app_task_definition_family)
SG=$(terraform output -raw app_security_group_id)
SUBNET=$(terraform output -json private_subnet_ids | python3 -c 'import sys,json;print(json.load(sys.stdin)[0])')

aws ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$TASKDEF" \
  --launch-type FARGATE \
  --region ap-south-1 \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --overrides file://seed-overrides.json
```

**Success looks like:** JSON with a `taskArn`. Now wait for it to finish and check
it succeeded:

```bash
TASK_ARN=$(aws ecs list-tasks --cluster "$CLUSTER" --desired-status STOPPED --region ap-south-1 --query 'taskArns[0]' --output text)
aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" --region ap-south-1 \
  --query 'tasks[0].containers[0].exitCode'
```

A result of `0` means the copy succeeded. Confirm the byte count in the logs:
CloudWatch → log group `/ecs/kanida-prod-app` → newest stream → look for
`SEEDED_BYTES 683...` (a number close to the file size).

> **If the exit code is not 0**, or you see no `SEEDED_BYTES` line: open the log
> stream, copy the last ~20 lines, and paste with "Step 8d seeder failed". Common
> causes: expired presigned URL (redo 8c), an empty secret from Step 7 (the task
> won't even start), or the DB key name in S3 not being exactly
> `seed/kanida_universe.db`.

> **Optional integrity check** (nice-to-have): re-run 8d with the command
> `["python","-c","import sqlite3;print(sqlite3.connect('/data/db/kanida_universe.db').execute('PRAGMA integrity_check').fetchone())"]`
> — success prints `('ok',)`.

---

## Step 9 — Turn the app on (tasks = 1) and apply again

Now the volume has the DB, so the app can pass A2. Edit `terraform.tfvars`:

```hcl
app_desired_count = 1
```

Then:

```bash
terraform apply
```

Review that the ONLY change is the ECS service desired count going `0 → 1`
(`Plan: 0 to add, 1 to change, 0 to destroy`), type `yes`.

**Success looks like:** `Apply complete!` and the service now wants 1 task.

> **If the plan wants to add/destroy more than just the service change:** STOP,
> do not type yes, and paste the plan with "Step 9 unexpected changes".

---

# PHASE 3 — Prove it, keep it safe

## Step 10 — Confirm the task booted, passed A2, and answers health checks

Give it 2–3 minutes, then:

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)
aws ecs describe-services --cluster "$CLUSTER" --services kanida-prod-svc --region ap-south-1 \
  --query 'services[0].{running:runningCount,desired:desiredCount,events:events[0].message}'
```

**Success looks like:** `running` = `1`, `desired` = `1`. Then open the app log
(CloudWatch → `/ecs/kanida-prod-app` → newest stream) and confirm you see the A2
gate pass — the message **`Production DB preflight OK`** (or the app's equivalent
"database preflight passed" banner), followed by uvicorn's
`Application startup complete` / `Uvicorn running on ...`.

Finally, confirm the health endpoint answers. From CloudShell:

```bash
ALB=$(terraform output -raw alb_dns_name)
curl -s -o /dev/null -w "%{http_code}\n" "http://$ALB/"
```

**Success looks like:** `200`. (CloudShell is inside AWS but the ALB security
group only allows 443 from the front-door ranges, so this internal `:80` check
may time out from CloudShell — if so, rely on the CloudWatch log showing
`Application startup complete` + the target group showing the task **healthy**:
`aws elbv2 describe-target-health` on the target group.)

> **If the task keeps restarting (running flips 1→0→1):** it is almost certainly
> A2 failing. Open the log stream and look for the FATAL "production database not
> found" banner. Causes: the seed didn't land at `/data/db/kanida_universe.db`
> (re-check Step 8d byte count), or a secret is empty (re-run the Step 7 checker).
> Paste the last ~30 log lines with "Step 10 crash-loop".

## Step 11 — Keep ALL live-trading gates OFF; check paper-parity

The cloud task must run in **paper / gates-OFF** mode until it is proven to match
the laptop.

- **`FALCON_AUTOTRADE_ENABLED` must be unset** (it is not in the managed secret
  list and not in the task env, so it defaults OFF — good; do **not** add it).
- **`RUPEEZY_LIVE_CERTIFIED` = `false`** (you set this in Step 7).
- Leave `FALCON_AUTOTRADE_EXECUTION_MODE` at paper.

Now compare the cloud app to the laptop for a day or two **without** letting it
place any real order:

1. Check the cloud app produces the **same Falcon signals / same Top-N** as the
   laptop for the same trading day (compare the portal/API output).
2. Check the EOD pipeline and any scheduled work behave (remember: only **one**
   task runs — do not raise `app_desired_count`; the in-process scheduler assumes
   a single writer, per `docs/design/SCHEDULER_EXTERNALIZATION_DESIGN.md`).
3. Watch the CloudWatch logs for errors around auth refresh (Zerodha headless
   login) and DB writes.

**Success looks like:** for ≥1 full trading day the cloud app's signals/behaviour
match the laptop, with no live orders placed and no DB-write errors in the logs.

> **If cloud output differs from the laptop:** paste back what differs with
> "Step 11 parity mismatch" — do NOT proceed toward live until parity holds.

---

## 🚧 GUARDRAIL — we are NOT cutting over yet

- **Do NOT change DNS.** `www.kanida.ai` / `api.kanida.ai` keep pointing at the
  existing laptop path (Vercel → Cloudflare tunnel → laptop:8001). The cloud ALB
  is reachable only for your own testing.
- **The laptop stays the primary, live trading system** through Phases 2 and 3.
  The cloud server is a **shadow** you are validating.
- **Do NOT turn on any live-trading gate** (`FALCON_AUTOTRADE_ENABLED`,
  `RUPEEZY_LIVE_CERTIFIED`, live execution mode) on the cloud until: (a) Step 11
  paper-parity holds for multiple days, AND (b) a separate, explicit cut-over plan
  is written and approved (egress IPs re-registered per broker, vault key
  confirmed, DNS switch rehearsed, rollback path defined).
- **Never run two schedulers at once.** If you ever point live traffic at the
  cloud, the laptop's scheduler/auto-trade must be turned OFF first, or both will
  fire — do that only under the cut-over plan, not here.

---

## Appendix — tearing it down (if you need to stop the meter)

Phase 2 created billable resources. To remove them (this DESTROYS the cloud DB
copy and all infra — the laptop is untouched):

```bash
terraform destroy
```

Type `yes` only if you are sure. EFS data and the RDS instance are deleted. The
S3 seed copy and your off-cloud vault-key backup survive. **Never** run this
against anything the live laptop depends on — it only touches the AWS stack.
