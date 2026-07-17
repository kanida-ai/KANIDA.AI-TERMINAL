# ============================================================================
# modules/redis — ElastiCache Redis (replication group), private subnets
# WHY: a shared cache / session store / rate-limit backend for a horizontally
# scaled app. Today the app uses per-process in-memory caches (the ~150s persona
# warm cache, replay warmer, rate limiters) that don't survive multiple
# instances. PHASE 0: stood up EMPTY, UNUSED by the app — mirrors compose.
# ============================================================================

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis-subnets"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = "${var.name}-redis"
  description          = "Kanida shared Redis (empty in Phase 0)."
  engine               = "redis"
  engine_version       = var.engine_version
  node_type            = var.node_type
  port                 = 6379

  # Single node in Phase 0 (cost). Bump num_cache_clusters >=2 +
  # automatic_failover_enabled=true for HA when the app actually depends on it.
  num_cache_clusters         = 1
  automatic_failover_enabled = false

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [var.security_group_id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  tags = { Name = "${var.name}-redis" }
}
