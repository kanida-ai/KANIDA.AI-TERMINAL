# ============================================================================
# modules/efs — encrypted EFS filesystem + per-AZ mount targets + access point
# ----------------------------------------------------------------------------
# Cloud-Migration PHASE 2 — AUTHORED, UNVALIDATED (Terraform absent on the
# authoring machine; NOT init/validate/plan/apply'd here). Added on top of the
# Phase-1-clean stack (prod 27709ec) to close the KNOWN GAP documented in
# modules/compute/README.md item 1: "the Fargate task ships WITHOUT a DB volume,
# so A2 fail-loud crash-loops until EFS-mounted SQLite is added."
#
# WHAT THIS GIVES THE APP:
#   The backend's A2 preflight (main.py lifespan → verify_falcon_db /
#   verify_power_db) REFUSES to boot unless FALCON_DB_PATH / POWER_DB_PATH
#   resolve to a real, readable file. The Dockerfile pins those to
#   /data/db/kanida_universe.db. This module provides the PERSISTENT storage that
#   gets mounted at /data/db so the SQLite DB survives task restarts (Fargate has
#   NO persistent local disk) and A2 passes.
#
# WHY EFS (not task-local, not baked-in):
#   * Fargate tasks are ephemeral — a restart wipes the container filesystem, so
#     an unmounted SQLite file would vanish (and A2 would then crash-loop).
#   * Baking the DB into the image is explicitly rejected (bloats the image,
#     defeats the A2 fail-loud gate, and re-introduces the 652 MB-in-git problem
#     the deploy/Dockerfile was written to avoid).
#   * EFS is a network filesystem shared across AZs, so the single Fargate task
#     (desired_count = 1) always sees the same DB no matter which AZ it lands in.
#
# HONEST CAVEAT (SQLite-on-EFS): SQLite over a network filesystem works ONLY with
# a single writer and correct NFSv4 locking (EFS supports NFSv4 byte-range
# locks). desired_count is pinned to 1 precisely so there is exactly ONE writer —
# see modules/compute + docs/design/SCHEDULER_EXTERNALIZATION_DESIGN.md. The real
# end-state is RDS Postgres (already scaffolded in modules/rds, stood up empty);
# EFS-SQLite is the "one cloud server, off the laptop" bridge, not the finish
# line. Do NOT raise desired_count above 1 while the app is on EFS-SQLite.
# ============================================================================

# ── The filesystem ──────────────────────────────────────────────────────────
# encrypted at rest with the SAME stack KMS key used by RDS/S3/Secrets (single
# audited, rotatable key). generalPurpose performance mode = lowest latency
# (maxIO trades latency for parallelism we do not need with one writer).
resource "aws_efs_file_system" "this" {
  creation_token = "${var.name}-db"
  encrypted      = true
  kms_key_id     = var.kms_key_arn

  performance_mode = "generalPurpose"
  throughput_mode  = var.throughput_mode

  tags = { Name = "${var.name}-db-efs" }
}

# ── Security group: NFS 2049 from the app tier ONLY ─────────────────────────
# The data plane must be unreachable except from the Fargate task. Ingress is
# sourced from the app SG (not a CIDR), mirroring the least-privilege pattern the
# rest of modules/security uses (rds/redis are 5432/6379 from the app SG only).
resource "aws_security_group" "efs" {
  name        = "${var.name}-efs-sg"
  description = "EFS NFS 2049 from the app tier only."
  vpc_id      = var.vpc_id

  ingress {
    description     = "NFS from the Fargate app tier."
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [var.app_sg_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-efs-sg" }
}

# ── Mount targets: one per private subnet / AZ ──────────────────────────────
# A Fargate task can only mount EFS through a mount target IN ITS OWN AZ, so we
# create one per private subnet. `count` (not for_each) is deliberate: the subnet
# IDs are unknown at plan time on a first apply, and for_each over unknown values
# fails `plan`. The LIST LENGTH is known (from var.private_subnet_cidrs), so
# count keeps `plan` clean.
resource "aws_efs_mount_target" "this" {
  count           = var.subnet_count
  file_system_id  = aws_efs_file_system.this.id
  subnet_id       = var.private_subnet_ids[count.index]
  security_groups = [aws_security_group.efs.id]
}

# ── Access point: rooted at the DB dir, POSIX-squashed to the container user ─
# The access point is what the ECS task mounts. Two jobs:
#   1. root_directory.path scopes the mount to a sub-directory of the filesystem
#      (/kanida-db), and creation_info makes EFS auto-create that directory owned
#      by the container's UID/GID on first mount.
#   2. posix_user SQUASHES every NFS operation through this access point to
#      uid/gid = var.posix_uid/var.posix_gid REGARDLESS of the client's real
#      uid/gid. The deploy/Dockerfile runs the app as uid 10001 (USER appuser),
#      so with posix_uid/gid = 10001 the app's process uid MATCHES the file
#      owner → owner rwx (0755) applies → A2's R_OK read AND SQLite's -wal/-shm
#      writes both succeed. This is the belt-and-suspenders that makes the mount
#      correct even if the image's supplementary GID were ever to drift.
resource "aws_efs_access_point" "db" {
  file_system_id = aws_efs_file_system.this.id

  posix_user {
    uid = var.posix_uid
    gid = var.posix_gid
  }

  root_directory {
    path = var.root_directory_path
    creation_info {
      owner_uid   = var.posix_uid
      owner_gid   = var.posix_gid
      permissions = "0755"
    }
  }

  tags = { Name = "${var.name}-db-ap" }
}
