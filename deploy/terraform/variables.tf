# ============================================================================
# variables.tf — root input variables
# Phase 0 scaffold. Defaults are safe/small; override in terraform.tfvars.
# ============================================================================

variable "aws_region" {
  description = "AWS region. Mumbai (ap-south-1) for NSE/SEBI proximity + latency."
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Deployment environment (e.g. prod, staging)."
  type        = string
  default     = "prod"
}

variable "name_prefix" {
  description = "Prefix for all resource names."
  type        = string
  default     = "kanida"
}

# ── Networking ───────────────────────────────────────────────────────────────
variable "vpc_cidr" {
  description = "CIDR for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "AZs to spread subnets across (>=2 for RDS Multi-AZ)."
  type        = list(string)
  default     = ["ap-south-1a", "ap-south-1b"]
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDRs (ALB + NAT + egress proxies live here)."
  type        = list(string)
  default     = ["10.20.0.0/24", "10.20.1.0/24"]
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDRs (Fargate tasks, RDS, Redis live here)."
  type        = list(string)
  default     = ["10.20.10.0/24", "10.20.11.0/24"]
}

variable "alb_ingress_cidrs" {
  description = "CIDRs allowed to hit the ALB on 443. Default open; restrict to the Cloudflare/Vercel front-door ranges in prod."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ── RDS ──────────────────────────────────────────────────────────────────────
variable "rds_instance_class" {
  # DOWNSIZED 2026-07-18: RDS is UNUSED (app runs on EFS-SQLite; the Postgres
  # cutover is future). db.t4g.micro single-AZ ≈ $19/mo vs db.t4g.medium Multi-AZ
  # ≈ $110/mo. Restore db.t4g.medium (+ rds_multi_az=true) at the Postgres cutover.
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.micro"
}

variable "rds_allocated_storage" {
  description = "RDS storage (GB). Prod SQLite is ~652 MB today; start small, autoscale."
  type        = number
  default     = 50
}

variable "rds_multi_az" {
  description = "Multi-AZ standby (removes the single-DB SPOF). Keep true in prod."
  type        = bool
  default     = false # DOWNSIZED 2026-07-18 — unused RDS; re-enable at Postgres cutover.
}

variable "rds_backup_retention_days" {
  description = "Automated backup / PITR retention window (days)."
  type        = number
  default     = 14
}

variable "rds_db_name" {
  description = "Initial database name."
  type        = string
  default     = "kanida"
}

variable "rds_master_username" {
  description = "RDS master username (password is generated + stored in Secrets Manager)."
  type        = string
  default     = "kanida_admin"
}

# ── Redis ────────────────────────────────────────────────────────────────────
variable "redis_node_type" {
  description = "ElastiCache Redis node type."
  type        = string
  default     = "cache.t4g.micro"
}

# ── Compute (Fargate) ────────────────────────────────────────────────────────
variable "container_image" {
  description = "ECR image URI built from deploy/Dockerfile (e.g. <acct>.dkr.ecr.ap-south-1.amazonaws.com/kanida-backend:phase0). Placeholder until the image is pushed."
  type        = string
  default     = "PLACEHOLDER_ECR_IMAGE_URI"
}

variable "app_desired_count" {
  description = "Number of Fargate tasks. IMPORTANT: the in-process 16:05 IST scheduler + boot resume threads assume a SINGLE writer today; keep 1 until the scheduler is externalized (see compute README)."
  type        = number
  default     = 1
}

variable "app_task_cpu" {
  description = "Fargate task CPU units (1024 = 1 vCPU)."
  type        = number
  default     = 1024
}

variable "app_task_memory" {
  description = "Fargate task memory (MiB). Chromium (Playwright) + pandas are memory-hungry."
  type        = number
  default     = 4096
}

# ── Secrets ──────────────────────────────────────────────────────────────────
variable "managed_secret_keys" {
  description = "Secret KEY NAMES to create empty placeholders for in Secrets Manager (values set out-of-band by the operator). See deploy/SECRETS_MAP.md."
  type        = list(string)
  # NOTE: FALCON_JWT_SECRET was REMOVED here (was drift). The auth code reads
  # POWER_JWT_SECRET (power_user/config.py) — verified against deploy/SECRETS_MAP.md.
  # Nothing reads FALCON_JWT_SECRET, so creating a placeholder for it would give
  # the operator a secret to set that no code path consumes. Do not re-add it.
  default = [
    "FALCON_VAULT_KEY",
    "POWER_JWT_SECRET",
    "POWER_ADMIN_SECRET",
    "ADMIN_SECRET",
    "FALCON_OPERATOR_TOKEN",
    "ANTHROPIC_API_KEY",
    "KITE_API_KEY",
    "KITE_API_SECRET",
    "KITE_ACCESS_TOKEN",
    "ZERODHA_USERNAME",
    "ZERODHA_PASSWORD",
    "ZERODHA_TOTP_SECRET",
    "POLYGON_API_KEY",
    "VAPID_PUBLIC_KEY",
    "VAPID_PRIVATE_KEY",
    "VAPID_CONTACT",
    "BROKER_PROXY_MAP",
    "BROKER_EGRESS_POOL",
    "RUPEEZY_LIVE_CERTIFIED",
    # WS2 data-freshness: HMAC gate for the laptop→cloud publish endpoint
    # (/api/falcon/publish/intelligence, checked in falcon/routers/publish_router.py
    # as X-Publish-Secret == FALCON_PUBLISH_SECRET). Same value set on the laptop.
    "FALCON_PUBLISH_SECRET",
    # SQLite->Postgres migration (Stage 1). Full libpq URL for RDS
    # (kanida-prod-pg, private subnet, reachable only from the app SG). Injecting
    # it makes backend/pgdb.py PROBE reachability at boot; it does NOT route any
    # traffic to PG — routing is a separate flag (KANIDA_PG_ENABLED) plus the
    # per-module cutover. Created out-of-band, so `terraform import` it before
    # the first apply (see deploy/CICD_SETUP.md / the migration notes).
    "DATABASE_URL",
  ]
}

# ── EFS (persistent SQLite volume for the single Fargate task) ───────────────
variable "efs_posix_uid" {
  description = "POSIX uid the EFS access point squashes to. MUST equal the deploy/Dockerfile appuser uid (10001)."
  type        = number
  default     = 10001
}

variable "efs_posix_gid" {
  description = "POSIX gid the EFS access point squashes to. Matches the deploy/Dockerfile appuser gid (10001)."
  type        = number
  default     = 10001
}

variable "efs_throughput_mode" {
  description = "EFS throughput mode ('bursting' = zero idle cost, fine for ~683 MB SQLite w/ one writer; 'elastic' if it ever throttles)."
  type        = string
  default     = "bursting"
}

# ── Egress (per-user static IPs) ─────────────────────────────────────────────
variable "egress_users" {
  description = "One entry per user's broker account needing a dedicated static egress IP (SEBI one-IP-per-account). Adding a user = one entry here. The key is a stable handle (e.g. the broker_account_id or user slug)."
  type        = list(string)
  default     = []   # Phase 0: none provisioned yet
}

variable "egress_proxy_instance_type" {
  description = "Instance type for each per-user tinyproxy egress box."
  type        = string
  default     = "t4g.nano"
}

variable "egress_ssh_key_name" {
  description = "EC2 key pair name for the egress proxy boxes (for break-glass SSH). Optional."
  type        = string
  default     = null
}

# ── AutoTrade live-execution gate (THE cutover switch) ──────────────────────
# Default paper-safe. At the ownership handoff (laptop gate OFF first):
#   terraform apply -var autotrade_enabled=true -var autotrade_execution_mode=marketable_limit
# Rollback = re-apply with autotrade_enabled=false.
variable "autotrade_enabled" {
  type        = bool
  default     = false
}
variable "autotrade_execution_mode" {
  type        = string
  default     = "paper"
}

# ── Health layer (sysagents) — arm AFTER the 1-share proof ───────────────────
#   terraform apply -var sysagents_enabled=true            (paging stays 'off' to settle)
#   terraform apply -var sysagents_enabled=true -var sysagents_paging=on   (after a clean session)
variable "sysagents_enabled" {
  type        = bool
  default     = false
}
variable "sysagents_paging" {
  type        = string
  default     = "off"
}
