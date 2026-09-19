# Deploy the Agent Builder to your AWS (S3 + ECS Fargate, behind your ALB)

This stack has its **own** Terraform state and **reads your existing estate** (VPC, private subnets,
ECS cluster, EFS) via `terraform_remote_state`. It creates only: an S3 data bucket, an ECR repo, IAM,
a Fargate service, and a `/api/builder/*` rule on your existing HTTPS listener. **It does not modify
your main stack.**

> Authored but **not** `terraform validate`d here (Terraform isn't on this machine — same caveat as your
> main stack's `versions.tf`). Run `terraform init && terraform validate && terraform plan` first and
> read the plan before `apply`.

## Automated deploy (GitHub Actions) — recommended after the first manual apply
`deploy/deploy-agent-builder.yml` → copy to **`.github/workflows/deploy-agent-builder.yml`** at your repo
root. On merge to `main` (paths `agent_builder_service/**`) it: OIDC-auths → ensures ECR → builds+pushes
the image (tagged with the commit SHA) → `terraform apply -var image_tag=<sha>` → waits for the service to
stabilize → health-checks `/api/builder/health`. Reuses your `secrets.AWS_DEPLOY_ROLE_ARN`; add two new
secrets: `TF_VAR_ALB_LISTENER_ARN`, `TF_VAR_ALB_SG_ID`. (Data sync in step 4 stays a separate/manual op —
it isn't in the deploy pipeline.) The manual steps below are the same actions, for the first run / debugging.

## 0. Prereqs
AWS CLI (logged into account `389642461326`, region `ap-south-1`), Docker, Terraform ≥ 1.6. The state
bucket `kanida-tfstate-389642461326` and lock table `kanida-tflock` already exist.

## 1. Fill the two ALB values Terraform can't read from outputs
```bash
ALB_ARN=$(aws elbv2 describe-load-balancers --query "LoadBalancers[?contains(LoadBalancerName,'kanida')].LoadBalancerArn" --output text)
aws elbv2 describe-listeners --load-balancer-arn "$ALB_ARN" --query "Listeners[?Port==\`443\`].ListenerArn" --output text
aws elbv2 describe-load-balancers --load-balancer-arns "$ALB_ARN" --query "LoadBalancers[0].SecurityGroups" --output text
```
Put both into `terraform.tfvars` (copy from `terraform.tfvars.example`).

## 2. Create the infra (bucket + ECR + IAM + service + ALB rule)
```bash
cd agent_builder_service/deploy/terraform
terraform init
terraform apply          # service will be UNHEALTHY until the image + data exist (steps 3-4) — expected
```
Note the outputs: `ecr_repo_url`, `data_bucket`, `data_uri`, `api_base`.

## 3. Build & push the image to ECR
```bash
cd ../..                 # agent_builder_service/  (has the Dockerfile)
ECR=$(terraform -chdir=deploy/terraform output -raw ecr_repo_url)
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin "${ECR%/*}"
docker build -t "$ECR:latest" .
docker push "$ECR:latest"
```

## 4. Convert market data to Parquet and upload to S3
```bash
python convert_to_parquet.py --db ../db/kanida.db --out ./parquet/daily --table ohlc_daily
BUCKET=$(terraform -chdir=deploy/terraform output -raw data_bucket)
aws s3 sync ./parquet/daily "s3://$BUCKET/kanida/daily/"
# (optional, heavy) 1-minute tier:
# python convert_to_parquet.py --db ../db/kanida.db --out ./parquet/1min --table ohlc_1min
# aws s3 sync ./parquet/1min "s3://$BUCKET/kanida/1min/"
```
`AGENT_DATA_URI` is already wired to `s3://$BUCKET/kanida/daily/` in the task definition.

## 5. Roll the service onto the new image + data
```bash
CLUSTER=$(aws ecs list-clusters --query "clusterArns[?contains(@,'kanida')]" --output text | head -1)
SVC=$(terraform -chdir=deploy/terraform output -raw ecs_service_name)
aws ecs update-service --cluster "$CLUSTER" --service "$SVC" --force-new-deployment
```

## 6. Verify
```bash
API=$(terraform -chdir=deploy/terraform output -raw api_base)
curl "$API/health"        # -> {"ok":true,"stocks":...,"bars":...}
```

## 7. Point the frontend at it
In your Next.js app set `NEXT_PUBLIC_API_URL` to your ALB's public domain (the same one that serves the
main app — the `/api/builder/*` rule routes to this service). Open `/power/builder`.

## EFS wallet mount — one manual SG grant (required)
The wallet SQLite lives on your existing EFS. Fargate can only mount it if your **EFS mount-target
security group allows NFS (2049) from this service's SG**. After `terraform apply`:
```bash
SVC_SG=$(terraform -chdir=deploy/terraform output -raw service_security_group_id)
# find your EFS mount-target SG (from the main stack), then:
aws ec2 authorize-security-group-ingress --group-id <EFS_MOUNT_TARGET_SG> \
    --protocol tcp --port 2049 --source-group "$SVC_SG"
```
If you'd rather not use EFS, skip the wallet volume and migrate the wallet to `power_user` RDS (below);
without a persistent store the token balance resets on task restart.

## Notes / prod hardening
- **Wallet**: this stack stores the token wallet as SQLite on **EFS** (`/data/wallet.db`, persistent).
  For real accounts, migrate it into `power_user_users.token_balance` and fund via your Razorpay router
  (see `../INTEGRATION.md`).
- **Auth**: swap `router.get_user_id()` for your power-auth JWT dependency so charges tie to real users.
- **Scale**: raise `desired_count` / `task_cpu` for heavier (esp. 1-minute) backtests.
- **Alternative**: skip this separate service and fold `agent_builder/` into your main backend image
  (one line in `main.py`) — then you only need the S3 bucket + an IAM read grant + the env vars.
