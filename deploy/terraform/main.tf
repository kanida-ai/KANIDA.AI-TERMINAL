# ============================================================================
# main.tf — root module: provider + module wiring for the Kanida.AI AWS stack
# ----------------------------------------------------------------------------
# Cloud-Migration PHASE 0 — AUTHORED, UNVERIFIED (Terraform absent on this
# machine; nothing has been init/plan/applied). Region is ap-south-1 (Mumbai)
# because the brokers (Zerodha/NSE) are Indian and SEBI static-IP registration
# expects an Indian egress IP; Mumbai also minimizes order-path latency.
#
# COMPUTE CHOICE: ECS Fargate (see modules/compute/README.md for the full
# justification). Kept swappable — swap the compute module for an EC2/ASG module
# without touching VPC/RDS/Redis/egress/secrets.
# ============================================================================

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "kanida-ai"
      Env       = var.environment
      ManagedBy = "terraform"
      Phase     = "phase0-scaffold"
    }
  }
}

# ── Networking: VPC with public + private subnets across 2 AZs ──────────────
# WHY: RDS Multi-AZ + a resilient compute service need ≥2 AZs. Private subnets
# isolate the database/cache/compute from the internet; the NAT gives them
# outbound (image pulls, broker APIs when NOT using a per-user egress proxy).
module "vpc" {
  source = "./modules/vpc"

  name               = "${var.name_prefix}-${var.environment}"
  cidr_block         = var.vpc_cidr
  availability_zones = var.availability_zones
  public_subnets     = var.public_subnet_cidrs
  private_subnets    = var.private_subnet_cidrs
}

# ── Security groups: least-privilege wiring between tiers ────────────────────
module "security" {
  source = "./modules/security"

  name       = "${var.name_prefix}-${var.environment}"
  vpc_id     = module.vpc.vpc_id
  vpc_cidr   = var.vpc_cidr
  alb_ingress_cidrs = var.alb_ingress_cidrs
}

# ── RDS PostgreSQL (Multi-AZ, PITR) in the private subnets ──────────────────
# WHY: the eventual datastore for the app (today SQLite). Multi-AZ removes the
# single-DB SPOF that the laptop currently is; PITR + automated snapshots remove
# the "one .db file, one machine" data-loss SPOF. Phase 0 stands it up EMPTY —
# the app is not yet pointed at it (mirrors docker-compose's parallel Postgres).
module "rds" {
  source = "./modules/rds"

  name                 = "${var.name_prefix}-${var.environment}"
  vpc_id               = module.vpc.vpc_id
  private_subnet_ids   = module.vpc.private_subnet_ids
  db_security_group_id = module.security.rds_sg_id

  instance_class       = var.rds_instance_class
  allocated_storage    = var.rds_allocated_storage
  multi_az             = var.rds_multi_az
  backup_retention_days = var.rds_backup_retention_days
  db_name              = var.rds_db_name
  master_username      = var.rds_master_username
  kms_key_arn          = module.secrets.kms_key_arn
}

# ── ElastiCache Redis in the private subnets ────────────────────────────────
# WHY: shared cache / session store / rate-limit backend for a multi-instance
# app (replaces per-process in-memory caches that don't survive horizontal
# scale). Phase 0 stands it up EMPTY (mirrors docker-compose's parallel Redis).
module "redis" {
  source = "./modules/redis"

  name                = "${var.name_prefix}-${var.environment}"
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  security_group_id   = module.security.redis_sg_id
  node_type           = var.redis_node_type
}

# ── Secrets Manager + KMS: the env-secret vault ─────────────────────────────
# WHY: config/.env on the laptop becomes managed secrets. KMS gives an audited,
# rotatable key; Secrets Manager injects values into the Fargate task at runtime
# (never baked into the image). See deploy/SECRETS_MAP.md for the key inventory.
module "secrets" {
  source = "./modules/secrets"

  name        = "${var.name_prefix}-${var.environment}"
  secret_keys = var.managed_secret_keys
}

# ── S3: reports / artifacts bucket ──────────────────────────────────────────
# WHY: the docs/ops xlsx/docx reports + data/artifacts currently live on the
# laptop disk (another SPOF). Versioned, encrypted, private bucket.
module "s3" {
  source = "./modules/s3"

  name        = "${var.name_prefix}-${var.environment}"
  kms_key_arn = module.secrets.kms_key_arn
}

# ── IAM: task execution + task roles for Fargate ────────────────────────────
module "iam" {
  source = "./modules/iam"

  name              = "${var.name_prefix}-${var.environment}"
  secret_arns       = module.secrets.secret_arns
  kms_key_arn       = module.secrets.kms_key_arn
  artifacts_bucket_arn = module.s3.bucket_arn
}

# ── Compute: ECS Fargate service behind an ALB ──────────────────────────────
# WHY Fargate: no EC2 hosts to patch, per-task IAM, native Secrets Manager
# injection, trivial rollback. Kept swappable — see modules/compute/README.md.
module "compute" {
  source = "./modules/compute"

  name                = "${var.name_prefix}-${var.environment}"
  vpc_id              = module.vpc.vpc_id
  public_subnet_ids   = module.vpc.public_subnet_ids
  private_subnet_ids  = module.vpc.private_subnet_ids
  alb_sg_id           = module.security.alb_sg_id
  app_sg_id           = module.security.app_sg_id

  container_image     = var.container_image        # ECR image URI (built from deploy/Dockerfile)
  container_port      = 8001                        # matches the Dockerfile/launch contract
  desired_count       = var.app_desired_count
  task_cpu            = var.app_task_cpu
  task_memory         = var.app_task_memory

  execution_role_arn  = module.iam.execution_role_arn
  task_role_arn       = module.iam.task_role_arn
  secret_arns_map     = module.secrets.secret_arns  # name -> ARN, injected as env
}

# ── Per-user static egress IPs (the SEBI one-IP-per-account solution) ───────
# WHY: SEBI/Zerodha bind ONE static IP to ONE broker account. The app already
# routes each account's orders through a per-account HTTP proxy
# (backend/autotrade/broker/egress.py + BROKER_EGRESS_POOL). This module is the
# infra side of that: one Elastic IP + one tiny tinyproxy box PER USER. Adding a
# user = ONE entry in var.egress_users (for_each). The resulting proxy URLs feed
# the app's BROKER_EGRESS_POOL. See modules/egress/README.md.
module "egress" {
  source = "./modules/egress"

  name              = "${var.name_prefix}-${var.environment}"
  vpc_id            = module.vpc.vpc_id
  vpc_cidr          = var.vpc_cidr
  public_subnet_ids = module.vpc.public_subnet_ids
  proxy_sg_id       = module.security.egress_proxy_sg_id
  egress_users      = var.egress_users
  proxy_instance_type = var.egress_proxy_instance_type
  ssh_key_name      = var.egress_ssh_key_name
}
