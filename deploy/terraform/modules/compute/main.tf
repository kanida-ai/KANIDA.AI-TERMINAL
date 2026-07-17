# ============================================================================
# modules/compute — ECS Fargate service behind an ALB
# WHY Fargate over EC2 (justification; see README): no host OS to patch, per-task
# IAM identity, native Secrets Manager injection, trivial rollback, scale via
# desired_count. Kept swappable — replace THIS module with an EC2/ASG module and
# nothing in vpc/rds/redis/secrets/egress changes.
#
# PHASE-0 HONESTY: this task definition has NO database volume. The app's A2
# preflight (main.py) REFUSES to boot without a readable prod DB. In the cloud
# that DB is either (a) an EFS-mounted SQLite file, or (b) RDS Postgres after the
# data migration — BOTH are LATER phases. So a Fargate task launched from this
# module as-is will crash-loop on the A2 gate BY DESIGN until DB wiring is added.
# The local docker-compose is where the boot/A2 test actually passes in Phase 0.
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
      # Non-secret env. DB paths shown for when a DB volume is wired (EFS mount
      # or, post-migration, replaced by DATABASE_URL from secrets). DATABASE_URL
      # is intentionally NOT set here in Phase 0 (app stays SQLite-shaped).
      environment = [
        { name = "PORT", value = tostring(var.container_port) },
        { name = "FALCON_DB_PATH", value = "/data/db/kanida_universe.db" },
        { name = "POWER_DB_PATH", value = "/data/db/kanida_universe.db" },
        { name = "KANIDA_DB_PATH", value = "/data/db/kanida_quant.db" },
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
