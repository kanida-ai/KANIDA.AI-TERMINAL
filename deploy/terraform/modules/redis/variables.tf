variable "name" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "security_group_id" { type = string }
variable "node_type" { type = string }

variable "engine_version" {
  type    = string
  default = "7.1"
}
