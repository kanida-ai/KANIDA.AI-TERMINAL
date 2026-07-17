# deploy/terraform — Kanida.AI AWS IaC skeleton (Phase 0)

**Status: AUTHORED, UNVERIFIED.** Terraform, Docker, and the AWS CLI are ALL
absent from the authoring machine. Nothing here has been `init`/`validate`/
`plan`/`apply`'d. Every resource is a proposal to validate on a machine with
Terraform + AWS credentials. Region is **ap-south-1 (Mumbai)** for NSE/SEBI
proximity and order-path latency.

## Layout

```
terraform/
  versions.tf              provider + terraform version pins; remote-state stub
  main.tf                  provider + module wiring
  variables.tf             all root inputs (safe defaults)
  outputs.tf               ALB DNS, RDS endpoint, egress IPs, secret ARNs, ...
  terraform.tfvars.example copy -> terraform.tfvars (git-ignored)
  modules/
    vpc/        VPC, public/private subnets (2 AZ), IGW, NAT
    security/   per-tier security groups (ALB/app/RDS/Redis/egress)
    rds/        PostgreSQL Multi-AZ + PITR, KMS-encrypted, private
    redis/      ElastiCache Redis (empty, parallel)
    secrets/    KMS key + empty Secrets Manager placeholders
    s3/         private/versioned/encrypted artifacts bucket
    iam/        Fargate execution + task roles (least privilege)
    compute/    ECS Fargate service + ALB
    egress/     ONE Elastic IP + tinyproxy PER USER (for_each) — SEBI one-IP/account
```

## Key decisions baked in

- **ECS Fargate** for compute (justification in `modules/compute/README.md`);
  kept swappable behind a single module boundary.
- **EIP-per-user egress** (`modules/egress`): the IP-at-scale answer. Adding a
  user is one entry in `egress_users`; `for_each` provisions their EIP + proxy.
  This is the infra half of the existing app-side `BROKER_EGRESS_POOL` /
  `backend/autotrade/broker/egress.py` design.
- **RDS + Redis stand up EMPTY** in Phase 0 — the app is NOT wired to them yet
  (mirrors `deploy/docker-compose.yml`). No `DATABASE_URL` is set, so the app
  stays SQLite-shaped (`backend/db.py::IS_POSTGRES` stays false).
- **Secrets**: KMS + empty Secrets Manager placeholders; VALUES set out-of-band.
  No secret value ever transits tfvars or state. See `deploy/SECRETS_MAP.md`.

## How to validate (operator, on a Terraform-equipped machine)

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars    # edit sizing/topology (no secrets)
terraform init
terraform validate
terraform plan -out tf.plan                      # REVIEW — do not apply blindly
# terraform apply tf.plan                         # only when the plan is understood
```

## Known Phase-0 gaps (honest)

1. `compute` task has **no DB volume** → its A2 preflight will fail until EFS-
   mounted SQLite or an RDS cutover is added (LATER phase). The A2 boot test
   passes in **docker-compose**, not on Fargate, in Phase 0.
2. **`app_desired_count` must be 1** until the in-process 16:05 IST scheduler +
   boot resume threads are externalized (double-scheduler = double-fire).
3. The **Zerodha auth refresh** (Windows Scheduled Task today) has no cloud
   equivalent modeled here.
4. No remote state backend wired (stub in `versions.tf`); no ACM cert, Route53,
   WAF, or CloudWatch alarms yet.
5. AMI/engine versions (AL2023 arm64, PG 15.7, Redis 7.1) should be re-checked
   for availability in ap-south-1 at apply time.
