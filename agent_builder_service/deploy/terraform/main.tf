# ============================================================================
# Agent Builder — deploy stack. SEPARATE Terraform state; references the existing
# kanida estate via terraform_remote_state (read-only). Adds: S3 data bucket, ECR
# repo, IAM, a Fargate service (behind your ALB) running the agent_builder API.
# Does NOT modify your existing stack.
# ============================================================================
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.40" }
  }
  # Own state key, same state bucket + lock table as your main stack.
  backend "s3" {
    bucket         = "kanida-tfstate-389642461326"
    key            = "kanida/agent-builder/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "kanida-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}

# --- read the existing kanida estate (VPC, subnets, cluster, EFS) --------------
data "terraform_remote_state" "main" {
  backend = "s3"
  config = {
    bucket         = "kanida-tfstate-389642461326"
    key            = "kanida/prod/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "kanida-tflock"
  }
}

data "aws_caller_identity" "me" {}

locals {
  acct        = data.aws_caller_identity.me.account_id
  name        = "${var.name_prefix}-agent-builder"
  vpc_id      = data.terraform_remote_state.main.outputs.vpc_id
  subnets     = data.terraform_remote_state.main.outputs.private_subnet_ids
  cluster     = data.terraform_remote_state.main.outputs.ecs_cluster_name
  efs_id      = data.terraform_remote_state.main.outputs.efs_file_system_id
  efs_ap_id   = data.terraform_remote_state.main.outputs.efs_access_point_id
  data_bucket = "${var.name_prefix}-market-data-${local.acct}"
  tags = { Project = "kanida", Component = "agent-builder", Environment = var.environment }
}
