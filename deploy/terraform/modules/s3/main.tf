# ============================================================================
# modules/s3 — private, versioned, KMS-encrypted reports/artifacts bucket
# WHY: docs/ops/*.xlsx|docx reports + data/artifacts currently live only on the
# laptop disk (a data-loss SPOF). This bucket is the durable home. Fully private;
# access via the task role only.
# ============================================================================

# S3 bucket names are GLOBALLY unique across ALL AWS accounts. A bare
# "kanida-prod-artifacts" risks a BucketAlreadyExists failure at apply if anyone
# (anywhere) already took it — a confusing wall-of-text for the operator. Suffix
# with this account's ID: account IDs are unique, so the name is effectively
# collision-proof, yet still deterministic (no perpetual diff, unlike random_id).
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "this" {
  bucket = "${var.name}-artifacts-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.name}-artifacts" }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}
