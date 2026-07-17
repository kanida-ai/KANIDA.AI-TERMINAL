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

output "secret_arns" {
  description = "Map of secret KEY NAME -> Secrets Manager ARN (values set out-of-band)."
  value       = module.secrets.secret_arns
}

# ── The per-user egress IPs (SEBI registration list) ─────────────────────────
output "egress_ips_by_user" {
  description = "user handle -> dedicated static egress IP. Each user registers THEIR IP on their broker app + SEBI profile. Feeds the app's BROKER_EGRESS_POOL."
  value       = module.egress.egress_ips_by_user
}
