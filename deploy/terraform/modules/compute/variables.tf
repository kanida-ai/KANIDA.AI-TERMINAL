variable "name" { type = string }
variable "vpc_id" { type = string }
variable "public_subnet_ids" { type = list(string) }
variable "private_subnet_ids" { type = list(string) }
variable "alb_sg_id" { type = string }
variable "app_sg_id" { type = string }

variable "container_image" { type = string }
variable "container_port" {
  type    = number
  default = 8001
}
variable "desired_count" { type = number }
variable "task_cpu" { type = number }
variable "task_memory" { type = number }

variable "execution_role_arn" { type = string }
variable "task_role_arn" { type = string }

variable "secret_arns_map" {
  description = "Map of env-var name -> Secrets Manager ARN to inject."
  type        = map(string)
}

# ── Persistent DB volume (from modules/efs) ──────────────────────────────────
variable "efs_file_system_id" {
  description = "EFS filesystem id backing the /data/db mount (module.efs.file_system_id)."
  type        = string
}

variable "efs_access_point_id" {
  description = "EFS access point id the volume mounts (POSIX-squashed to the container uid/gid; module.efs.access_point_id)."
  type        = string
}

variable "acm_certificate_arn" {
  description = "ACM cert ARN for the HTTPS listener. Null = no listener created (plan still succeeds)."
  type        = string
  default     = null
}
