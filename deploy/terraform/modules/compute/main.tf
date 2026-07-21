# ============================================================================
# modules/compute — ECS Fargate service behind an ALB
# WHY Fargate over EC2 (justification; see README): no host OS to patch, per-task
# IAM identity, native Secrets Manager injection, trivial rollback, scale via
# desired_count. Kept swappable — replace THIS module with an EC2/ASG module and
# nothing in vpc/rds/redis/secrets/egress changes.
#
# PHASE-2 UPDATE (EFS wired): this task definition now mounts a PERSISTENT EFS
# volume at /data/db (matching FALCON_DB_PATH/POWER_DB_PATH in the Dockerfile),
# so the app's A2 preflight (main.py) can find the prod SQLite DB and boot. The
# volume is an EFS access point POSIX-squashed to uid/gid 10001 (the Dockerfile
# appuser) — see modules/efs/README.md. The EFS must be SEEDED with the prod DB
# ONCE before first boot (deploy/PHASE2_3_RUNBOOK.md), else A2 still (correctly)
# refuses to start against an empty volume. RDS Postgres (modules/rds) remains
# the eventual end-state; EFS-SQLite is the single-writer bridge, so desired_count
# MUST stay 1.
# ============================================================================

resource "aws_ecs_cluster" "this" {
  name = "${var.name}-cluster"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${var.name}-app"
  retention_in_days = 30
}

# ── Task definition ─────────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "app" {
  family                   = "${var.name}-app"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([
    {
      name      = "app"
      image     = var.container_image
      essential = true
      portMappings = [
        { containerPort = var.container_port, protocol = "tcp" }
      ]
      # Mount the persistent EFS volume where the app expects the SQLite DB.
      # containerPath MUST equal the FALCON_DB_PATH/POWER_DB_PATH directory
      # (/data/db) from the Dockerfile so the A2 preflight finds the DB file.
      mountPoints = [
        { sourceVolume = "db", containerPath = "/data/db", readOnly = false }
      ]
      # Non-secret env. DB paths shown for when a DB volume is wired (EFS mount
      # or, post-migration, replaced by DATABASE_URL from secrets). DATABASE_URL
      # is intentionally NOT set here in Phase 0 (app stays SQLite-shaped).
      environment = [
        { name = "PORT", value = tostring(var.container_port) },
        # Pin the container clock to IST so naive date/time (e.g. kite token_date
        # via date.today(), any naive datetime.now()) matches the laptop and the
        # app's IST-centric logic. The scheduler already uses datetime.now(IST)
        # explicitly, so this only removes latent UTC-vs-IST date edges.
        { name = "TZ", value = "Asia/Kolkata" },
        { name = "FALCON_DB_PATH", value = "/data/db/kanida_universe.db" },
        { name = "POWER_DB_PATH", value = "/data/db/kanida_universe.db" },
        { name = "KANIDA_DB_PATH", value = "/data/db/kanida_quant.db" },
        # R&D-DB split: serve-time reads use the published artifacts on EFS so
        # the container never reaches for the 38GB universe_engine R&D DB (absent
        # in cloud). Seeded alongside the serving DB at /data/db (Step 8).
        { name = "FALCON_OUTCOMES_ARTIFACT", value = "/data/db/falcon_serve_evidence.db" },
        { name = "FALCON_SIM_PATTERNS_ARTIFACT", value = "/data/db/falcon_sim_patterns.db" },
        # Paper-safe by construction. Live trading requires FALCON_AUTOTRADE_ENABLED
        # == "true"; set explicitly to "false" so the cloud box can NEVER place a
        # real order during bring-up/parity, independent of any default drift.
        { name = "FALCON_AUTOTRADE_ENABLED", value = "false" },
        { name = "FALCON_AUTOTRADE_EXECUTION_MODE", value = "paper" },
        # Rupeezy/Vortex instrument master (symbol→numeric token; needed to PLACE a
        # Rupeezy order). Shipped to EFS alongside the DB seed at /data/db so it
        # refreshes without an image rebuild. Absent until shipped → Rupeezy orders
        # fail loud ("instrument master not configured"); harmless while gates are OFF.
        { name = "RUPEEZY_INSTRUMENT_MASTER", value = "/data/db/rupeezy_instruments.json" },
        # WS4 on-demand per-user egress-IP provisioning (egress_provisioner.py via
        # boto3). Region + the public subnet / egress-proxy SG / AMI / instance type
        # a provisioned proxy box uses. Empty AMI/SG = provisioning endpoints return
        # a clean config error (they never auto-run), so this is safe to ship even
        # before the values are finalized.
        { name = "AWS_DEFAULT_REGION", value = data.aws_region.current.name },
        { name = "KANIDA_EGRESS_SUBNET_ID", value = var.public_subnet_ids[0] },
        { name = "KANIDA_EGRESS_SG_ID", value = var.egress_proxy_sg_id },
        { name = "KANIDA_EGRESS_AMI_ID", value = var.egress_ami_id },
        { name = "KANIDA_EGRESS_INSTANCE_TYPE", value = var.egress_instance_type },
      ]
      # Secrets injected from Secrets Manager (name -> valueFrom ARN). Values
      # never appear in the task def, image, or Terraform state.
      secrets = [
        for k, arn in var.secret_arns_map : { name = k, valueFrom = arn }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.app.name
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = "app"
        }
      }
    }
  ])

  # ── Persistent DB volume (EFS) ─────────────────────────────────────────────
  # Mounted at /data/db by the container mountPoint above. transit_encryption is
  # ENABLED (Fargate platform 1.4.0+ tunnels NFS over TLS on 2049 — the EFS SG
  # still gates 2049 to the app tier). authorization_config pins the POSIX-
  # squashed access point; iam = "DISABLED" because access is already scoped by
  # the access point + the 2049-from-app-SG-only rule, so NO extra task-role IAM
  # is required (keeps modules/iam untouched).
  volume {
    name = "db"
    efs_volume_configuration {
      file_system_id     = var.efs_file_system_id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = var.efs_access_point_id
        iam             = "DISABLED"
      }
    }
  }
}

data "aws_region" "current" {}

# ── ALB ─────────────────────────────────────────────────────────────────────
resource "aws_lb" "this" {
  name               = "${var.name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [var.alb_sg_id]
  subnets            = var.public_subnet_ids
}

resource "aws_lb_target_group" "app" {
  name        = "${var.name}-tg"
  port        = var.container_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"   # required for awsvpc/Fargate

  health_check {
    path                = "/"          # main.py root() returns 200 JSON once A2 passes
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    matcher             = "200"
  }
}

# HTTP listener (port 80) — ALWAYS created.
# WHY (load-bearing, not cosmetic): an aws_ecs_service with a load_balancer block
# fails at APPLY ("The target group ... does not have an associated load
# balancer") unless the referenced target group is already attached to the ALB
# via at least one listener. Before this listener existed, a plan with no ACM
# cert (the default) produced a stack that VALIDATED and PLANNED fine but could
# NOT be applied. This listener guarantees the target group is always associated.
# NOTE: the ALB security group only allows 443 inbound (see modules/security), so
# :80 is NOT reachable from the internet — this listener exists purely to satisfy
# the ECS association requirement and to give the local/boot path a target. In
# real prod, once ACM is wired, switch this to a 301 redirect to 443.
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app.arn
  }
}

# HTTPS listener. cert ARN required — supply an ACM cert for the app domain.
# Conditional: no cert => no 443 listener (the HTTP listener above still keeps
# the target group associated so the service applies).
resource "aws_lb_listener" "https" {
  count             = var.acm_certificate_arn == null ? 0 : 1
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app.arn
  }
}

# ── Service ─────────────────────────────────────────────────────────────────
resource "aws_ecs_service" "app" {
  name            = "${var.name}-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.app.arn
  desired_count   = var.desired_count   # keep 1 until the in-process scheduler is externalized
  launch_type     = "FARGATE"

  # ── STOP-THEN-START deploy (single-writer SQLite on shared EFS) ────────────
  # The app writes SQLite files on the shared EFS volume at /data/db, and SQLite
  # is single-writer. The DEFAULT rolling deploy (max 200% / min 100%) starts the
  # NEW task BEFORE draining the old one, so for ~1-2 min TWO tasks write the same
  # DB over NFS → "database disk image is malformed" (hit live 2026-07-21 during a
  # force-new-deployment; required a stop + re-seed to recover). Forcing
  # max=100% / min=0% makes ECS STOP the old task before starting the new one, so
  # there is NEVER more than one writer. Cost: ~1-2 min of downtime per deploy —
  # acceptable and correct until the DB moves to RDS Postgres (multi-writer safe),
  # at which point revert to a rolling deploy. This also makes the CI/CD pipeline's
  # force-new-deployment corruption-safe. See deploy/CICD_SETUP.md.
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.app.arn
    container_name   = "app"
    container_port   = var.container_port
  }

  # Only attach to the LB once a listener exists. aws_lb_listener.http is always
  # created, so the target group is always LB-associated (the https listener is
  # conditional on an ACM cert and may be absent).
  depends_on = [aws_lb_listener.http, aws_lb_listener.https]
}
