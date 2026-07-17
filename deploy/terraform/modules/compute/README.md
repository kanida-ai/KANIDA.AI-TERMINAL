# module: compute (ECS Fargate)

**Purpose.** Run the backend container (built from `deploy/Dockerfile`) as an ECS
Fargate service behind an Application Load Balancer, with Secrets Manager
injection and CloudWatch logging.

## Why Fargate, not EC2 (the decision)

| Factor | Fargate (chosen) | EC2/ASG |
|--------|------------------|---------|
| Host OS patching | none (AWS-managed) | operator's burden |
| Identity | per-task IAM role | shared instance profile |
| Secrets injection | native `secrets=[valueFrom]` | manual (SSM agent / userdata) |
| Rollback | new task def revision | AMI / userdata churn |
| Idle cost | pay per task | pay per instance |
| Playwright/Chromium | works in the container image | works, but you manage the host |

Fargate wins on operational simplicity for a small team. The trade-off is no
persistent local disk — which is exactly why the DB is RDS (or EFS-mounted),
never task-local.

**Swappability.** Everything else (vpc/rds/redis/secrets/egress/iam) is
compute-agnostic. Swapping to EC2 = replace this one module; its inputs
(`container_image`, subnets, SGs, role ARNs, secret map) and outputs
(`alb_dns_name`) are the contract.

## Phase-0 honesty / open items (flagged)

1. **No DB volume → A2 will fail here by design.** The task definition mounts no
   database. `main.py`'s A2 preflight refuses to boot without a readable prod DB.
   Cloud DB wiring — EFS-mounted SQLite **or** RDS Postgres after migration — is a
   LATER phase. The Phase-0 A2 boot test passes in **docker-compose**, not here.
2. **`desired_count` must stay 1** until the in-process 16:05 IST scheduler + the
   boot-time `resume_active_sessions` / ladder-resume threads (main.py lifespan)
   are externalized. Two tasks = two schedulers = double-fires. This is the
   single most important correctness constraint on horizontal scaling.
3. **HTTPS listener** needs an ACM cert ARN (`acm_certificate_arn`); null skips
   the listener so `plan` still succeeds. Add an HTTP→HTTPS redirect listener in
   prod.
4. **The Zerodha auth refresh** currently runs as a Windows Scheduled Task on the
   laptop (main.py comments, `KanidaZerodhaAuth`). In cloud it must become a
   scheduled ECS task / EventBridge rule — not modeled here.

**Phase-0 status.** Authored, unverified — never planned/applied.
