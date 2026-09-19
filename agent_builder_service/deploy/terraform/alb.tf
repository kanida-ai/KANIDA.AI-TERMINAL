# Target group for the Fargate service (ip targets), health = /api/builder/health.
resource "aws_lb_target_group" "svc" {
  name        = "${var.name_prefix}-agent-builder"
  port        = var.container_port
  protocol    = "HTTP"
  vpc_id      = local.vpc_id
  target_type = "ip"
  health_check {
    path                = "/api/builder/health"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

# Path rule on your existing HTTPS listener: /api/builder/* -> this service.
resource "aws_lb_listener_rule" "svc" {
  listener_arn = var.alb_https_listener_arn
  priority     = var.listener_rule_priority
  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.svc.arn
  }
  condition {
    path_pattern { values = [var.path_pattern] }
  }
  tags = local.tags
}
