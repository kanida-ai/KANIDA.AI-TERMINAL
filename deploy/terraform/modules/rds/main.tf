# ============================================================================
# modules/rds — PostgreSQL, Multi-AZ, PITR, private-subnet, KMS-encrypted
# WHY: the eventual datastore (today a single SQLite file on one laptop — the
# largest data-loss + availability SPOF in the system). Multi-AZ = automatic
# standby failover; backup_retention_period > 0 = point-in-time recovery.
# PHASE 0: stood up EMPTY. The app is NOT pointed at it yet (no DATABASE_URL
# wired). This mirrors docker-compose's parallel-but-unused Postgres.
# ============================================================================

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db-subnets"
  subnet_ids = var.private_subnet_ids
  tags       = { Name = "${var.name}-db-subnets" }
}

# Generated master password → Secrets Manager (never in tfvars/state-plaintext).
resource "random_password" "master" {
  length  = 32
  special = true
  # RDS disallows a few chars in the master password.
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "master" {
  name        = "${var.name}/rds/master_password"
  description = "RDS master password for ${var.name}."
  kms_key_id  = var.kms_key_arn
}

resource "aws_secretsmanager_secret_version" "master" {
  secret_id     = aws_secretsmanager_secret.master.id
  secret_string = random_password.master.result
}

resource "aws_db_instance" "this" {
  identifier     = "${var.name}-pg"
  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  allocated_storage     = var.allocated_storage
  max_allocated_storage = var.allocated_storage * 4   # storage autoscaling headroom
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = var.kms_key_arn

  db_name  = var.db_name
  username = var.master_username
  password = random_password.master.result

  multi_az               = var.multi_az                     # standby failover — removes the single-DB SPOF
  backup_retention_period = var.backup_retention_days       # >0 enables PITR
  copy_tags_to_snapshot   = true
  deletion_protection     = true                            # guard against accidental destroy
  skip_final_snapshot     = false
  final_snapshot_identifier = "${var.name}-pg-final"

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.db_security_group_id]
  publicly_accessible    = false                            # private subnet only

  performance_insights_enabled = true
  auto_minor_version_upgrade   = true

  tags = { Name = "${var.name}-pg" }
}
