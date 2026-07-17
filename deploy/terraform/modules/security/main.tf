# ============================================================================
# modules/security — least-privilege security groups per tier
# WHY: the laptop today has no per-tier network isolation. Each SG below opens
# EXACTLY the one port the next tier needs, sourced from the previous tier's SG
# (not a CIDR), so the data plane is unreachable except from the app.
# ============================================================================

# ── ALB: public 443 in, anything out ────────────────────────────────────────
resource "aws_security_group" "alb" {
  name        = "${var.name}-alb-sg"
  description = "ALB ingress (443)."
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTPS from the front door (restrict to CF/Vercel ranges in prod)."
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.alb_ingress_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-alb-sg" }
}

# ── App (Fargate): 8001 from the ALB only ───────────────────────────────────
resource "aws_security_group" "app" {
  name        = "${var.name}-app-sg"
  description = "Fargate task ingress on 8001 from the ALB only."
  vpc_id      = var.vpc_id

  ingress {
    description     = "App port from ALB."
    from_port       = 8001
    to_port         = 8001
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  egress {
    description = "Outbound to RDS/Redis/egress-proxies/broker APIs/ECR."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-app-sg" }
}

# ── RDS: 5432 from the app SG only ──────────────────────────────────────────
resource "aws_security_group" "rds" {
  name        = "${var.name}-rds-sg"
  description = "Postgres 5432 from the app tier only."
  vpc_id      = var.vpc_id

  ingress {
    description     = "Postgres from app."
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-rds-sg" }
}

# ── Redis: 6379 from the app SG only ────────────────────────────────────────
resource "aws_security_group" "redis" {
  name        = "${var.name}-redis-sg"
  description = "Redis 6379 from the app tier only."
  vpc_id      = var.vpc_id

  ingress {
    description     = "Redis from app."
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-redis-sg" }
}

# ── Egress proxies: 8888 (tinyproxy) from the app tier; open egress out ─────
# The app forwards a user's broker orders to that user's proxy on 8888; the
# proxy egresses to the broker from its own Elastic IP (the SEBI-registered IP).
resource "aws_security_group" "egress_proxy" {
  name        = "${var.name}-egress-proxy-sg"
  description = "Per-user tinyproxy: 8888 from the app, egress to broker APIs."
  vpc_id      = var.vpc_id

  ingress {
    description = "tinyproxy from the app tier (VPC-internal)."
    from_port   = 8888
    to_port     = 8888
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    description = "Proxy out to broker APIs over HTTPS."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.name}-egress-proxy-sg" }
}
