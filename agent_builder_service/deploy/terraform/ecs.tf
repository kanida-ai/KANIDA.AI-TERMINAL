resource "aws_cloudwatch_log_group" "svc" {
  name              = "/ecs/${local.name}"
  retention_in_days = 30
  tags              = local.tags
}

# Service SG — accept ALB traffic on the container port; egress anywhere (S3/EFS).
resource "aws_security_group" "svc" {
  name        = "${local.name}-svc"
  description = "agent-builder fargate service"
  vpc_id      = local.vpc_id
  ingress {
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [var.alb_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

resource "aws_ecs_task_definition" "svc" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.task.arn

  volume {
    name = "wallet"
    efs_volume_configuration {
      file_system_id     = local.efs_id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = local.efs_ap_id
        iam             = "ENABLED"
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "agent-builder"
    image     = "${aws_ecr_repository.builder.repository_url}:${var.image_tag}"
    essential = true
    portMappings = [{ containerPort = var.container_port, protocol = "tcp" }]
    mountPoints  = [{ sourceVolume = "wallet", containerPath = "/data", readOnly = false }]
    environment = [
      { name = "AGENT_DATA_URI", value = "s3://${local.data_bucket}/${var.data_prefix}" },
      { name = "AGENT_NIFTY_SYMBOL", value = var.nifty_symbol },
      { name = "AWS_REGION", value = var.aws_region },
      { name = "WALLET_DB", value = "/data/wallet.db" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.svc.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "svc"
      }
    }
    # Liveness is covered by the ALB target-group health check (/api/builder/health).
  }])
  tags = local.tags
}

resource "aws_ecs_service" "svc" {
  name            = local.name
  cluster         = local.cluster
  task_definition = aws_ecs_task_definition.svc.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.subnets
    security_groups  = [aws_security_group.svc.id]
    assign_public_ip = false
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.svc.arn
    container_name   = "agent-builder"
    container_port   = var.container_port
  }
  depends_on = [aws_lb_listener_rule.svc]
  tags       = local.tags
}
