# CI/CD — arm the one-push cloud deploy

**Status:** the pipeline (`.github/workflows/deploy-cloud.yml`) is **committed but
DORMANT.** It has no push trigger and no AWS role, so it cannot deploy anything
until the 3 arming steps below. **Do these only AFTER the cutover 1-share proof
is green** (auto-deploy into a live-trading cloud is not something to wire during
the handoff).

## What it gives you
`git push` (to `main`) → the change is live in the cloud in ~10-12 min, with **no
manual zip → CodeBuild → redeploy dance.** Until armed, that manual loop stays the
way to ship.

## How it works (plain)
GitHub Actions zips your source (never `.git`, DBs, or secrets), hands it to your
**existing CodeBuild** project (which has the disk + buildspec to build the big
image), CodeBuild pushes `kanida-backend:phase2` to ECR, then Actions tells ECS to
pull the new image and waits until it's healthy. Auth is **GitHub OIDC → a scoped
AWS role** — no AWS keys are ever stored in GitHub.

---

## Arming — 3 steps (~15 min, do together post-cutover)

### Step A — create the AWS deploy role (OIDC, no stored keys)
This is the only new AWS resource. I'll generate the exact Terraform (a GitHub
OIDC provider + a role trusted *only* by `kanida-ai/KANIDA.AI-TERMINAL` on `main`,
allowed *only* to: put the source zip to the CI S3 prefix, start/read that one
CodeBuild project, and `update-service`/`describe` on `kanida-prod-svc`). We
`terraform apply` it and copy the printed **role ARN**.

> Why together: an IAM trust policy is easy to get subtly wrong (wrong `sub`
> claim = either it won't assume, or it's too broad). We verify the assume works
> with a dry manual run before flipping to auto.

### Step B — set 4 GitHub repo settings
In GitHub → repo **Settings → Secrets and variables → Actions**, add these
**secrets**:

| name | value |
|------|-------|
| `AWS_DEPLOY_ROLE_ARN` | the role ARN from Step A |
| `CODEBUILD_PROJECT`   | your CodeBuild project name (`aws codebuild list-projects --region ap-south-1`) |
| `CODEBUILD_SRC_BUCKET`| the S3 bucket the pipeline uploads source to (the artifacts bucket, `kanida-prod-artifacts-389642461326`) |

*(One-time: confirm the CodeBuild project's source type accepts an S3 override —
if it's currently `NO_SOURCE`, we set it to `S3`. I'll check this in Step A.)*

### Step C — prove it, then flip to auto
1. **Manual run first:** GitHub → **Actions → deploy-cloud → Run workflow**, type
   `DEPLOY`. Watch it build on CodeBuild and roll ECS. This proves the whole chain
   with zero risk (you chose to run it).
2. Once a manual run deploys cleanly, **uncomment the `push:` trigger** at the top
   of `.github/workflows/deploy-cloud.yml` and push. From then on, every push to
   `main` auto-deploys. (`paths-ignore` skips doc-only pushes.)

---

## Deploy safety: single-writer DB
The service runs **stop-then-start** (`deployment_maximum_percent=100`,
`deployment_minimum_healthy_percent=0` — in `modules/compute`). This is
**required**: the app writes SQLite on shared EFS, and a rolling deploy would run
two writers for ~1-2 min → `database disk image is malformed` (hit live
2026-07-21). So the pipeline's `force-new-deployment` is corruption-safe, at the
cost of ~1-2 min downtime per deploy. Revert to rolling only after the DB moves to
RDS Postgres. **Never** raise `maximum_percent` above 100 while on EFS-SQLite.

## Guardrails baked in
- **Dormant by default** — manual trigger only; the confirm box must read `DEPLOY`.
- **`concurrency`** — never two deploys at once.
- **Least privilege** — the role can't touch trades, secrets, RDS, or any service
  other than `kanida-prod-svc`.
- **Never ships secrets or DBs** — the zip step hard-excludes `.git`, `config/.env*`,
  and every `*.db`.
- **Rollback** = re-run the workflow on the previous commit (or `git revert` + push
  once auto is on).

## Not covered (deliberately, for later)
- Staging environment / blue-green (single prod service today; `desired_count` must
  stay 1 until the scheduler is externalized — see task #2).
- Auto-migrations / Terraform in the pipeline (infra changes stay a manual
  `terraform apply` — app code and infra ship on different cadences).
