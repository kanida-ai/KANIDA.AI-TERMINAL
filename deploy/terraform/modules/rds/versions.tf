# Provider requirements for this module. rds uses BOTH aws (db instance, subnet
# group, secrets) AND random (random_password for the generated master password).
# Declared explicitly so `terraform validate` is warning-clean.
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
