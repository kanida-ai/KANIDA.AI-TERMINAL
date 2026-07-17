# ============================================================================
# versions.tf — Terraform + provider version pins
# ----------------------------------------------------------------------------
# Cloud-Migration PHASE 0 — AUTHORED, UNVERIFIED. Terraform is NOT installed on
# the authoring machine. `terraform init`/`validate`/`plan` have NEVER been run.
# Pins are conservative and known-good as of authoring; bump deliberately.
# ============================================================================

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

  # ── Remote state (REQUIRED before anyone runs apply) ──────────────────────
  # State holds secret ARNs, resource IDs, and (avoid, but possible) sensitive
  # values. It MUST NOT live in git. Uncomment + fill once the S3 state bucket +
  # DynamoDB lock table exist (create them once, by hand or a bootstrap module).
  #
  # backend "s3" {
  #   bucket         = "kanida-tfstate-apsouth1"
  #   key            = "phase0/terraform.tfstate"
  #   region         = "ap-south-1"
  #   dynamodb_table = "kanida-tflock"
  #   encrypt        = true
  # }
}
