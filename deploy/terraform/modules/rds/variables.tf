variable "name" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "db_security_group_id" { type = string }
variable "kms_key_arn" { type = string }

variable "engine_version" {
  type    = string
  default = "15.7"
}
variable "instance_class" { type = string }
variable "allocated_storage" { type = number }
variable "multi_az" { type = bool }
variable "backup_retention_days" { type = number }
variable "db_name" { type = string }
variable "master_username" { type = string }
