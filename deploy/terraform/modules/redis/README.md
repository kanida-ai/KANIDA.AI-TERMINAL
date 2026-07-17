# module: redis

**Purpose.** ElastiCache Redis replication group in private subnets, encrypted
at rest + in transit.

**SPOF / requirement addressed.** Gives multi-instance app a shared cache /
session / rate-limit store (today those are per-process in-memory and don't
survive horizontal scale).

**Phase-0 status.** Authored, unverified. Stood up **EMPTY, UNUSED** — mirrors
docker-compose's parallel Redis. Single node for cost; set
`num_cache_clusters >= 2` + `automatic_failover_enabled = true` for HA when the
app depends on it.
