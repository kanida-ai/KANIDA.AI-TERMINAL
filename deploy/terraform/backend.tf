# backend.tf — remote Terraform state (S3 + DynamoDB lock)
# ----------------------------------------------------------------------------
# WHY: the Phase-2 local state was lost when the working dir was rm -rf'd
# (2026-07-18). Remote state in S3 (versioned, encrypted, locked) makes that
# impossible to recur — state survives the working dir, every apply is
# versioned, and the DynamoDB lock prevents concurrent-apply corruption.
#
# One-time bootstrap (operator runs in CloudShell BEFORE `terraform init`):
#   aws s3api create-bucket --bucket kanida-tfstate-389642461326 \
#       --region ap-south-1 --create-bucket-configuration LocationConstraint=ap-south-1
#   aws s3api put-bucket-versioning --bucket kanida-tfstate-389642461326 \
#       --versioning-configuration Status=Enabled
#   aws dynamodb create-table --table-name kanida-tflock \
#       --attribute-definitions AttributeName=LockID,AttributeType=S \
#       --key-schema AttributeName=LockID,KeyType=HASH \
#       --billing-mode PAY_PER_REQUEST --region ap-south-1
#
# Then `terraform init` adopts this backend (empty state), and the 68 existing
# resources are re-adopted via `terraform import` (import.tf, generated next).
terraform {
  backend "s3" {
    bucket         = "kanida-tfstate-389642461326"
    key            = "kanida/prod/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "kanida-tflock"
    encrypt        = true
  }
}
