variable "name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  description = "VPC CIDR — source range for VPC-internal proxy access."
  type        = string
}

variable "alb_ingress_cidrs" {
  description = "CIDRs allowed to hit the ALB on 443."
  type        = list(string)
}
