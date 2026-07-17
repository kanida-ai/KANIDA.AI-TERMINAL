# Provider requirements for this module. Declared explicitly (not just inherited
# from the root) so `terraform validate` is warning-clean and the module is
# self-describing if ever used standalone. The provider CONFIG (region, tags)
# still comes from the root module.
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }
}
