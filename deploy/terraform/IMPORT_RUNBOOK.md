# Terraform State-Recovery Runbook — re-adopt the existing prod stack

**Situation:** the Phase-2 local Terraform state was lost. **All 68 resources still
exist in AWS** (RDS, EFS with the seeded 685 MB SQLite DB, 19 loaded Secrets
Manager secrets, VPC, ALB, ECS, …). We rebuild state by **importing** every
resource into a fresh S3-backed state. **We never re-create or destroy anything.**

- **Account:** `389642461326`  **Region:** `ap-south-1`
- **Naming:** resources are `kanida-prod-*`, tagged `Project=kanida-ai`, `Env=prod`, `ManagedBy=terraform`.
- **Terraform:** 1.9.8 (config-driven imports supported; this recovery uses CLI `terraform import`).

Run everything below **in AWS CloudShell** (has AWS CLI + creds + `jq`, region ap-south-1).

---

## Step 0 — Get the terraform code into CloudShell

The **module source is authoritative** for resource addresses. Get the current
`deploy/terraform/` tree (root + all 10 modules + `backend.tf` +
`discover_and_import.sh`) into CloudShell:

```bash
# From your machine: zip the terraform dir and upload via CloudShell "Actions > Upload file",
# OR pull from git if the branch is reachable from CloudShell.
mkdir -p ~/kanida-tf && cd ~/kanida-tf
unzip ~/terraform.zip           # -> you should see main.tf, backend.tf, modules/, discover_and_import.sh
```

> Do **not** create `terraform.tfvars` with `egress_users` or `acm_certificate_arn`
> set during recovery — the defaults (`egress_users=[]`, `acm_certificate_arn=null`)
> match what is actually deployed. Setting them would make `plan` propose NEW
> resources that are not part of this recovery. You *may* set
> `container_image` to the **real ECR image URI** to avoid a benign task-def diff
> (see Step 5).

---

## Step 1 — Create the S3 state bucket + DynamoDB lock table (one-time)

Already documented in `backend.tf`. Run once (idempotent-ish; skip if they exist):

```bash
aws s3api create-bucket --bucket kanida-tfstate-389642461326 \
    --region ap-south-1 --create-bucket-configuration LocationConstraint=ap-south-1
aws s3api put-bucket-versioning --bucket kanida-tfstate-389642461326 \
    --versioning-configuration Status=Enabled
aws dynamodb create-table --table-name kanida-tflock \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST --region ap-south-1
```

---

## Step 2 — `terraform init` (adopts the S3 backend; state starts EMPTY)

```bash
cd ~/kanida-tf
terraform init
terraform state list        # expect: EMPTY (0 resources) — this is the recovery starting point
```

---

## Step 3 — Run the import script

```bash
bash discover_and_import.sh
```

What it does:
- Verifies you're in account `389642461326`, region `ap-south-1`, and that `terraform init` ran.
- **Discovers** each resource's real AWS ID (by tag `Name`, by `group-name`, by
  friendly name, or uses the known IDs from the last apply).
- Runs `terraform import '<address>' '<id>'` for all **68** state entries
  (67 AWS resources + `random_password.master`), in a safe grouping.
- Is **idempotent**: every address already in `terraform state list` is **skipped**,
  so you can re-run it safely (e.g. after fixing one failed import).
- **Never** runs `apply`/`destroy`.
- Prints a final **imported / skipped / failed** summary + any **manual TODOs**.

Expected end state: **`terraform state list` shows 68 resources.**

If some imports **failed** (e.g. a transient AWS throttle, or an ID not
discovered), fix the cause and just **re-run the script** — completed imports are
skipped, only the missing ones are attempted.

---

## Step 4 — `terraform plan` — the acceptance gate

```bash
terraform plan
```

**PASS criteria:**
- **`0 to destroy`.** This is the hard gate.
- A **near-zero** change set. The only diffs that are acceptable here are
  **create-only or in-place**, specifically:
  - `module.compute.aws_ecs_task_definition.app` → **new revision (create)**, if
    any container field (esp. `container_image`) differs from the deployed
    revision. Set `container_image` to the real ECR URI in `terraform.tfvars`
    to make this go away. A new task-def revision is **create-only, never a destroy**.
  - `module.compute.aws_ecs_service.app` → **in-place update** to reference that
    new revision.
  - tag-only / description-only **in-place** updates (default_tags reconciliation).

**STOP and report** (do **not** apply) if the plan shows:
- **any resource being destroyed or replaced** — that means an import was
  **missed or mis-keyed**. Note the address(es) and cross-check the enumeration
  in this runbook / the script. Common misses: a `route_table_association`
  (subnet/rtb order), a mount target (subnet alignment), or a secret whose value
  is empty (see below).
- **`random_password.master` "must be replaced"** — see the section below; do
  not let it regenerate (it would churn the RDS master password).

---

## The tricky imports (reference)

| Resource | Import ID format | Source |
|---|---|---|
| `aws_iam_role_policy_attachment` | `role-name/policy-arn` | e.g. `kanida-prod-ecs-execution/arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy` |
| `aws_iam_role_policy` | `role-name:policy-name` | e.g. `kanida-prod-ecs-execution:kanida-prod-execution-secrets` |
| `aws_ecs_service` | `cluster/service` | `kanida-prod-cluster/kanida-prod-svc` |
| `aws_ecs_task_definition` | task-def **ARN** (with revision) | `aws ecs describe-task-definition` |
| `aws_lb_listener` | listener **ARN** | `aws elbv2 describe-listeners` (port 80) |
| `aws_lb` / `aws_lb_target_group` | **ARN** | known ALB ARN / `describe-target-groups --names kanida-prod-tg` |
| `aws_route_table_association` | `subnet-id/rtb-id` | discovered subnet + route-table IDs |
| `aws_efs_mount_target` | `fsmt-id` | `aws efs describe-mount-targets` (matched to the subnet so `[0]/[1]` align) |
| `aws_efs_file_system` / `aws_efs_access_point` | `fs-...` / `fsap-...` | known IDs |
| `aws_secretsmanager_secret` | **ARN** or friendly name | `aws secretsmanager describe-secret` |
| `aws_secretsmanager_secret_version` | `secret-arn\|version-id` | `get-secret-value --query VersionId` |
| `aws_kms_key` / `aws_kms_alias` | key-id / `alias/kanida-prod` | `aws kms list-aliases` |
| `aws_s3_bucket*` (bucket + PAB + versioning + SSE) | **bucket name** | `kanida-prod-artifacts-389642461326` |
| `aws_db_instance` | DB **identifier** | `kanida-prod-pg` |
| `aws_elasticache_replication_group` | replication-group-id | `kanida-prod-redis` |

**Security-group rules:** all ingress/egress are **inline blocks** — there are
**no** standalone `aws_security_group_rule` / `aws_vpc_security_group_*_rule`
resources to import. Only the 6 `aws_security_group` resources themselves.

### `random_password.master` (the one to watch)
- It is a **random-provider** resource (not AWS). Its `.result` feeds **both** the
  RDS master password and the `aws_secretsmanager_secret_version.master` value.
- If it is **absent** from state, `terraform plan` will **generate a new random
  value** and try to update the secret version (and the RDS password). To avoid
  that, the script imports it using the **real stored value** read from
  `kanida-prod/rds/master_password`.
- **Caveat:** importing `random_password` can still show a **replace** diff if the
  provider cannot reconcile `length`/`special`/`override_special` from the value
  alone. **If `plan` shows `random_password.master` must be replaced, STOP.** Do
  not apply. Options: (a) confirm the RDS password in Secrets Manager is the
  source of truth and add `lifecycle { ignore_changes = all }` to
  `random_password.master` (or replace it with a `data`/variable) before applying;
  (b) escalate. Never let it regenerate against the live RDS instance.

### Conditional resources NOT imported (by design)
- `module.compute.aws_lb_listener.https[0]` — `count = 0` (no ACM cert wired). The
  script detects if a **:443** listener exists in AWS and prints the exact import
  line to run **only if** you set `var.acm_certificate_arn`.
- `module.egress.aws_instance.proxy[*]`, `module.egress.aws_eip.proxy[*]` —
  `for_each` over `egress_users = []` (none provisioned in Phase 0).

---

## Enumerated resources (68 total = 67 AWS + 1 state-only)

- **module.vpc (14):** `aws_vpc.this`, `aws_internet_gateway.this`,
  `aws_subnet.public[0..1]`, `aws_subnet.private[0..1]`, `aws_eip.nat`,
  `aws_nat_gateway.this`, `aws_route_table.public`, `aws_route_table.private`,
  `aws_route_table_association.public[0..1]`, `aws_route_table_association.private[0..1]`
- **module.security (5):** `aws_security_group.{alb,app,rds,redis,egress_proxy}`
- **module.secrets (21):** `aws_kms_key.this`, `aws_kms_alias.this`,
  `aws_secretsmanager_secret.this["<KEY>"]` × 19
- **module.s3 (4):** `aws_s3_bucket.this`, `aws_s3_bucket_public_access_block.this`,
  `aws_s3_bucket_versioning.this`, `aws_s3_bucket_server_side_encryption_configuration.this`
- **module.iam (5):** `aws_iam_role.execution`, `aws_iam_role.task`,
  `aws_iam_role_policy.execution_secrets`, `aws_iam_role_policy.task`,
  `aws_iam_role_policy_attachment.execution_managed`
- **module.rds (4 AWS + 1 state-only):** `aws_db_subnet_group.this`,
  `aws_secretsmanager_secret.master`, `aws_secretsmanager_secret_version.master`,
  `aws_db_instance.this`, **+ `random_password.master` (state-only)**
- **module.redis (2):** `aws_elasticache_subnet_group.this`,
  `aws_elasticache_replication_group.this`
- **module.efs (5):** `aws_efs_file_system.this`, `aws_security_group.efs`,
  `aws_efs_mount_target.this[0..1]`, `aws_efs_access_point.db`
- **module.compute (7):** `aws_ecs_cluster.this`, `aws_cloudwatch_log_group.app`,
  `aws_ecs_task_definition.app`, `aws_lb.this`, `aws_lb_target_group.app`,
  `aws_lb_listener.http`, `aws_ecs_service.app`

**Total: 67 AWS + `random_password.master` = 68.**

---

## Step 5 — After a clean plan

Once `plan` shows **0 to destroy** and only the benign create/in-place diffs above:
- Optionally set `container_image` (real ECR URI) in `terraform.tfvars` and re-plan
  to shrink the diff to zero.
- The stack is re-adopted. Future changes go through normal `plan` / `apply` with
  state safely in S3 (versioned, locked). The lost-state incident cannot recur.
