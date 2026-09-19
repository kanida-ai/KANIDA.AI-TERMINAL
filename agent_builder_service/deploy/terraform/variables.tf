variable "aws_region" {
  type    = string
  default = "ap-south-1"
}
variable "name_prefix" {
  type    = string
  default = "kanida"
}
variable "environment" {
  type    = string
  default = "prod"
}

# Container image — build+push to the ECR repo this stack creates, then set the tag.
variable "image_tag" {
  type    = string
  default = "latest"
}

# Fargate sizing — backtests are CPU-bound; give it room. 1 vCPU / 2 GB default.
variable "task_cpu" {
  type    = string
  default = "1024"
}
variable "task_memory" {
  type    = string
  default = "2048"
}
variable "desired_count" {
  type    = number
  default = 1
}
variable "container_port" {
  type    = number
  default = 8010
}

# ALB wiring — NOT exposed by the main stack's outputs, so the operator supplies these:
#   aws elbv2 describe-listeners --load-balancer-arn <alb-arn> --query "Listeners[?Port==\`443\`].ListenerArn"
#   ALB security-group id: aws elbv2 describe-load-balancers ... --query "...SecurityGroups"
variable "alb_https_listener_arn" {
  type = string
}
variable "alb_security_group_id" {
  type = string
}
variable "listener_rule_priority" {
  type    = number
  default = 500
}
variable "path_pattern" {
  type    = string
  default = "/api/builder/*"
}

# Data — after convert_to_parquet.py + aws s3 sync, the daily prefix in this stack's bucket.
variable "data_prefix" {
  type    = string
  default = "kanida/daily/"
}
variable "nifty_symbol" {
  type    = string
  default = "NIFTY 50"
}
