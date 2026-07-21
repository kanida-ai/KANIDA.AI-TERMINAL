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

# ── AutoTrade live-execution gate (THE cutover switch) ──────────────────────
# Default paper-safe: the cloud can NEVER place a real order until these flip.
# Flip to true / "marketable_limit" ONLY at the ownership handoff (after the
# laptop gate is OFF), then apply. Rollback = flip back + apply.
variable "autotrade_enabled" {
  description = "FALCON_AUTOTRADE_ENABLED. false = paper-safe. Set true only at cutover handoff (laptop must be false first — single trade owner)."
  type        = bool
  default     = false
}
variable "autotrade_execution_mode" {
  description = "FALCON_AUTOTRADE_EXECUTION_MODE. 'paper' until cutover; 'marketable_limit' at handoff (matches the laptop's live mode)."
  type        = string
  default     = "paper"
}

# ── System Engineering Agent Hierarchy (sysagents) — observe-only health layer ─
# Default OFF. Arm AFTER the 1-share proof: sysagents_enabled=true with paging
# 'off' during settling, then paging 'on' after one clean session. Never touches
# trades (read-only health monitors); SYSAGENTS_KILL_SWITCH hard-stops the layer.
variable "sysagents_enabled" {
  description = "SYSAGENTS_ENABLED. Default off. Arm after the 1-share proof (observe-only; never touches trades)."
  type        = bool
  default     = false
}
variable "sysagents_paging" {
  description = "SYSAGENTS_PAGING. 'off' during the cutover-settling window; 'on' after one clean live session."
  type        = string
  default     = "off"
}

# ── WS4 on-demand per-user egress-IP provisioning (KANIDA_EGRESS_* task env) ──
variable "egress_proxy_sg_id" {
  description = "Security group id for on-demand per-user egress proxy boxes (module.security.egress_proxy_sg_id). Injected as KANIDA_EGRESS_SG_ID."
  type        = string
  default     = ""
}

variable "egress_ami_id" {
  description = "AMI id for on-demand egress proxy instances (AL2023, arch matching egress_instance_type). Injected as KANIDA_EGRESS_AMI_ID."
  type        = string
  default     = ""
}

variable "egress_instance_type" {
  description = "Instance type for on-demand egress proxy boxes (default Graviton t4g.nano, matches an arm64 AMI). Injected as KANIDA_EGRESS_INSTANCE_TYPE."
  type        = string
  default     = "t4g.nano"
}
