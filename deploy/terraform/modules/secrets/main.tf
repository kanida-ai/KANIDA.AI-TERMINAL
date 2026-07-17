# ============================================================================
# modules/secrets — KMS key + Secrets Manager placeholders for every env secret
# WHY: config/.env on the laptop becomes managed, audited, rotatable secrets.
# The KMS key encrypts the secrets (and is reused by RDS/S3). Fargate injects
# these into the task at runtime via `secrets = [...]` — values NEVER bake into
# the image and NEVER hit git. See deploy/SECRETS_MAP.md for the inventory +
# which are load-bearing (FALCON_VAULT_KEY gates egress + broker-cred decrypt).
#
# This module creates EMPTY placeholder secrets (one per key name). The operator
# sets the actual VALUES out-of-band (console/CLI) — Terraform never sees them.
# ============================================================================

resource "aws_kms_key" "this" {
  description             = "${var.name} envelope key for Secrets Manager / RDS / S3."
  deletion_window_in_days = 30
  enable_key_rotation     = true
  tags                    = { Name = "${var.name}-kms" }
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}

# One empty secret per key name. No secret_version here on purpose — creating a
# version would require a VALUE. The operator populates values after apply.
resource "aws_secretsmanager_secret" "this" {
  for_each = toset(var.secret_keys)

  name        = "${var.name}/env/${each.value}"
  description = "Kanida env secret ${each.value} (value set out-of-band; see SECRETS_MAP.md)."
  kms_key_id  = aws_kms_key.this.arn
}
