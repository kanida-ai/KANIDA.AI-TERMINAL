variable "name" {
  description = "Name prefix for EFS resources (e.g. kanida-prod)."
  type        = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs — one mount target is created per subnet/AZ."
  type        = list(string)
}

variable "app_sg_id" {
  description = "The Fargate app security group; the ONLY source allowed to reach EFS on 2049."
  type        = string
}

variable "kms_key_arn" {
  description = "Stack KMS key ARN used to encrypt the filesystem at rest (reused from modules/secrets)."
  type        = string
}

# ── POSIX identity — MUST match the container's runtime user ─────────────────
# deploy/Dockerfile: `useradd --system --uid 10001 ... appuser` + `USER appuser`,
# so the app process runs as uid 10001. The access point squashes all I/O to
# these values, so uid MUST equal the container uid for A2's R_OK read to pass.
variable "posix_uid" {
  description = "POSIX uid the access point squashes to. MUST equal the deploy/Dockerfile appuser uid (10001)."
  type        = number
  default     = 10001
}

variable "posix_gid" {
  description = "POSIX gid the access point squashes to. Matches the deploy/Dockerfile appuser gid (10001)."
  type        = number
  default     = 10001
}

variable "root_directory_path" {
  description = "Sub-directory ON the EFS filesystem that the access point roots at (mounted at /data/db in the task). Auto-created owned by posix_uid/gid on first mount."
  type        = string
  default     = "/kanida-db"
}

variable "throughput_mode" {
  description = "EFS throughput mode. 'bursting' = zero idle cost (fine for a ~683 MB SQLite DB with one writer). Switch to 'elastic' if the DB workload ever throttles on burst credits."
  type        = string
  default     = "bursting"
}
