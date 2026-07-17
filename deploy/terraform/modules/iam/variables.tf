variable "name" { type = string }

variable "secret_arns" {
  description = "Map of secret KEY NAME -> ARN the task/execution roles may read."
  type        = map(string)
}

variable "kms_key_arn" { type = string }

variable "artifacts_bucket_arn" { type = string }
