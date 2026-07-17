# ============================================================================
# outputs.tf — root outputs (what the operator needs after apply)
# ============================================================================

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "alb_dns_name" {
  description = "Point Cloudflare/Vercel at this once the app is live."
  value       = module.compute.alb_dns_name
}

output "rds_endpoint" {
  description = "RDS connection endpoint. Feeds DATABASE_URL in a LATER phase (NOT wired in Phase 0)."
  value       = module.rds.endpoint
}

output "rds_master_password_secret_arn" {
  description = "Secrets Manager ARN holding the generated RDS master password."
  value       = module.rds.master_password_secret_arn
}

output "redis_primary_endpoint" {
  value = module.redis.primary_endpoint
}

output "artifacts_bucket" {
  value = module.s3.bucket_name
}

# ── EFS (persistent DB volume) ───────────────────────────────────────────────
output "efs_file_system_id" {
  description = "EFS filesystem id (fs-...) backing /data/db. Seed the prod DB onto it once before first boot (see PHASE2_3_RUNBOOK.md)."
  value       = module.efs.file_system_id
}

output "efs_access_point_id" {
  description = "EFS access point id (fsap-...) the Fargate task mounts (POSIX-squashed to uid/gid 10001)."
  value       = module.efs.access_point_id
}

# ── Seeder helpers (copy-paste inputs for the one-time DB seed run-task) ──────
# The DB-seed step (PHASE2_3_RUNBOOK.md) launches a one-off ECS task in the
# private subnets with the app SG. Exposing these as outputs saves the operator
# from hunting subnet/SG IDs in the console.
output "private_subnet_ids" {
  description = "Private subnet IDs — pass to the one-time DB-seed `aws ecs run-task` networkConfiguration."
  value       = module.vpc.private_subnet_ids
}

output "app_security_group_id" {
  description = "App SG id — pass to the one-time DB-seed run-task networkConfiguration (it can reach EFS on 2049)."
  value       = module.security.app_sg_id
}

output "ecs_cluster_name" {
  description = "ECS cluster name — target for the one-time DB-seed run-task."
  value       = module.compute.cluster_name
}

output "app_task_definition_family" {
  description = "App task-def family — reused (with a command override) by the one-time DB-seed run-task."
  value       = module.compute.task_definition_family
}

output "secret_arns" {
  description = "Map of secret KEY NAME -> Secrets Manager ARN (values set out-of-band)."
  value       = module.secrets.secret_arns
}

# ── The per-user egress IPs (SEBI registration list) ─────────────────────────
output "egress_ips_by_user" {
  description = "user handle -> dedicated static egress IP. Each user registers THEIR IP on their broker app + SEBI profile. Feeds the app's BROKER_EGRESS_POOL."
  value       = module.egress.egress_ips_by_user
}
