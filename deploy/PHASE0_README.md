# Cloud-Migration PHASE 0 — Containerization + IaC + Local-Parity Scaffolding

**Authored:** 2026-07-17 IST · **Branch:** `feat/cloud-migration-phase0` · **Base:**
prod `a0dbe19` · **Worktree:** `C:/Users/SPS/Desktop/_kanida_phase0` (isolated;
the live prod tree was never touched).

> ## ⚠️ NOTHING HERE HAS BEEN RUN OR VERIFIED
> Docker, Terraform, and the AWS CLI are **all absent** from this machine
> (confirmed). So the image was **never built**, `terraform plan` was **never
> run**, and no cloud resource was provisioned. Every artifact in `deploy/` is
> **AUTHORED, UNVERIFIED** — validation is deferred to when Docker/Terraform are
> installed, or to the AWS side. Treat all of it as a reviewable proposal, not a
> tested deliverable.
>
> Phase 0 is **behavior-preserving scaffolding only**. The live ₹-trading
> backend on this laptop (:8001) is unchanged. Nothing cuts over.

## What was authored (all NEW files under `deploy/`)

```
deploy/
  Dockerfile                 production backend image (multi-stage; NO COPY . .; NO baked DB/secrets)
  .dockerignore              shrinks build context from GB to MB (excludes .git/data/DBs/docs/...)
  .gitignore                 keeps phase0.env + tfstate/tfvars out of git
  docker-compose.yml         LOCAL PARITY: app(SQLite mount) + EMPTY Postgres + EMPTY Redis
  phase0.env.example         non-secret env template for compose
  SECRETS_MAP.md             every config/.env key -> Secrets Manager/KMS target + migration order
  PHASE0_README.md           this file
  terraform/                 AWS ap-south-1 IaC skeleton (plan-ready, NOT applied)
    versions.tf main.tf variables.tf outputs.tf terraform.tfvars.example README.md
    modules/ vpc security rds redis compute egress s3 secrets iam   (each with its own README)
```

Existing `deploy/` files (`Procfile`, `README.md`, `CLOUDFLARE_TUNNEL_SETUP.md`,
`cloudflared`, `verify-deploy.sh`) were **left untouched** — this is additive.

## Grounding (read before trusting any of the above)

- `backend/main.py` — the FastAPI lifespan runs the **A2 preflight**
  (`verify_falcon_db` + `verify_power_db`); it raises `ProductionDBMissingError`
  and refuses to boot if the prod DB is missing. The image therefore **mounts**
  the DB and never bakes it — a mis-mount fails LOUD, as intended. Env is loaded
  from `config/.env` if present, else straight from `os.environ` (so injected
  container env Just Works, no `.env` shipped).
- `backend/falcon/config.py` / `backend/power_user/config.py` — DB resolution via
  `FALCON_DB_PATH` / `POWER_DB_PATH`; the silent R&D fallback is removed, so the
  container must point these at the mounted prod DB.
- `backend/db.py` — `IS_POSTGRES` flips on `DATABASE_URL`. Phase 0 keeps the app
  on SQLite by **not** setting it; Postgres/Redis stand up empty and unused.
- `backend/autotrade/broker/egress.py` — the existing broker-agnostic
  per-account egress-proxy pool the Terraform `egress` module provisions IPs for.
- `scripts/start_backend.bat` — launch contract: `uvicorn main:app --port 8001`,
  `PLAYWRIGHT_BROWSERS_PATH`, parallel warm-cache. Mirrored in the Dockerfile.

## DB reality (why the excludes matter)

| File | Size | In image? |
|---|---|---|
| `data/db/kanida_universe.db` (PROD) | 652 MB | **NO** — mounted volume |
| `data/db/kanida_quant.db` (legacy) | 83 MB | **NO** — mounted volume |
| `universe_engine/data/db/kanida_universe.db` (R&D) | 36 GB | **NEVER** |
| `.git` | 3.4 GB | **NEVER** (680 GiB bloat incident) |

## Phase-0 status vs the AutoTrade scalable-architecture plan

`docs/design/AUTOTRADE_SCALABLE_ARCHITECTURE.md` §8 tracks L1–L5. This Phase-0
scaffolding maps to it as:

- **DONE already in prod (not this work):** L1 egress hook
  (`BROKER_PROXY_URL`/pool), L2 broker abstraction, L3 vault + `broker_accounts`,
  L4/L5 sessions. The A2 fail-loud DB preflight (2026-07-16).
- **Authored here (unverified):** the container image, the local-parity compose,
  and the AWS IaC skeleton including the **EIP-per-user egress** infra that fills
  the existing `BROKER_EGRESS_POOL`. This is the "L10 Cloud VM move + KMS" row —
  scaffolded, not executed.
- **Still TODO (LATER phases, explicitly out of Phase-0 scope):** cloud DB wiring
  (EFS-mount SQLite **or** RDS Postgres migration), externalizing the in-process
  16:05 IST scheduler so `desired_count > 1` is safe, a cloud Zerodha auth-refresh
  job, remote tfstate, ACM/Route53/WAF, CloudWatch alarms.

## Operator validation checklist (run when tools are installed)

### A. Container + local parity (needs Docker Desktop)

1. Activate the ignore file at the build-context root:
   ```bash
   cd "C:/Users/SPS/Desktop/Kanida.ai Terminal Quant Intelligence Engine"
   cp deploy/.dockerignore .dockerignore
   ```
2. Build (context = repo root):
   ```bash
   DOCKER_BUILDKIT=1 docker build -f deploy/Dockerfile -t kanida-backend:phase0 .
   ```
   - CONFIRM the build context is small (MB). If Docker reports a multi-GB
     context, the `.dockerignore` did not take effect — fix step 1.
3. Prepare compose env: `cp deploy/phase0.env.example deploy/phase0.env` (leave
   the live gates OFF; no real secrets needed for the boot test).
4. Boot the parity stack:
   ```bash
   cd deploy && docker compose up
   ```
5. **PASS test (A2 preflight):** the app log shows
   `Production DB preflight OK (falcon + power_user).` and
   `curl http://localhost:8001/` returns 200 JSON.
6. **FAIL-LOUD test (the important one):** stop, remove/rename the mounted DB (or
   edit the compose volume to a bad path), `docker compose up` again → the app
   must exit with the `FATAL: Kanida production database not found` banner, NOT
   boot on the wrong DB. This is the whole point of mounting-not-baking.
7. Confirm Postgres + Redis are up and **empty/unused**: `docker compose exec
   postgres psql -U kanida -c '\dt'` (no app tables) and the app never connects.

> Use a COPY of the prod DB for this test, not the live file the running backend
> owns. Do NOT run this compose stack against the live :8001 DB while the real
> backend is trading.

### B. Terraform (needs Terraform + AWS creds — AWS-side)

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # sizing/topology only, NO secrets
terraform init
terraform validate
terraform plan -out tf.plan                     # REVIEW every resource; do not apply blindly
```
Expect the skeleton to need small fixes on first real `validate`/`plan` (it has
never been run). Do NOT `apply` until the plan is understood and the AWS-only
prerequisites below are done.

### C. AWS-account actions only the operator can do

1. Create the **remote state** S3 bucket + DynamoDB lock table; uncomment the
   `backend "s3"` block in `versions.tf`.
2. Create/import an **ACM certificate** for the app domain (feeds the ALB HTTPS
   listener) and Route53 records.
3. After apply, set **secret VALUES** out-of-band (see `SECRETS_MAP.md`) —
   `FALCON_VAULT_KEY` FIRST, and back it up off-cloud.
4. Build + push the image to **ECR**; set `container_image` to its URI.
5. Register each user's **egress EIP** (from `terraform output
   egress_ips_by_user`) on their broker app + SEBI profile; assemble the
   credentialed proxy URLs into `BROKER_EGRESS_POOL`.
6. Keep `FALCON_AUTOTRADE_ENABLED` + `RUPEEZY_LIVE_CERTIFIED` **OFF** until the
   cloud path is proven end-to-end.

## Assumptions flagged

- Model A egress posture (each user trades their own broker app) — the EIP-per-
  user module suits both Model A and per-account isolation; the regulatory choice
  is the operator's (`AUTOTRADE_SCALABLE_ARCHITECTURE.md` §8).
- `FALCON_JWT_SECRET` is in the migration brief but **not** in the current
  `config/.env`; placeholdered as SECRET pending a code check.
- Fargate task ships **without** a DB volume in Phase 0 → it will not pass A2 in
  cloud until DB wiring lands. This is intentional and documented, not an
  oversight.
- Instance/engine/AMI versions are best-effort; re-check ap-south-1 availability
  at apply time.
