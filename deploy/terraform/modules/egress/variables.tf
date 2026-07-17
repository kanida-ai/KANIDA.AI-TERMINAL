variable "name" { type = string }
variable "vpc_id" { type = string }

variable "vpc_cidr" {
  description = "VPC CIDR — tinyproxy Allow rule so only the app tier can use the proxy."
  type        = string
  default     = "10.20.0.0/16"
}

variable "public_subnet_ids" {
  description = "Public subnets the proxy boxes launch into (need routable EIPs)."
  type        = list(string)
}

variable "proxy_sg_id" { type = string }

variable "egress_users" {
  description = "One stable handle per user's broker account needing a static IP. Adding a user = one entry."
  type        = list(string)
}

variable "proxy_instance_type" {
  type    = string
  default = "t4g.nano"
}

variable "ssh_key_name" {
  description = "Optional EC2 key pair for break-glass SSH."
  type        = string
  default     = null
}
