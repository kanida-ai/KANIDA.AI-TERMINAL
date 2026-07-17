# module: security

**Purpose.** One security group per tier, wired source-by-SG (not by CIDR) so
each tier can reach only the next.

| SG | Ingress | From |
|----|---------|------|
| `alb` | 443 | `alb_ingress_cidrs` (lock to CF/Vercel in prod) |
| `app` | 8001 | ALB SG only |
| `rds` | 5432 | app SG only |
| `redis` | 6379 | app SG only |
| `egress_proxy` | 8888 | VPC CIDR (app tier), tinyproxy |

**SPOF / requirement addressed.** No network isolation on the laptop today. This
enforces that RDS/Redis are unreachable except from the app, and the per-user
egress proxies accept traffic only from inside the VPC.

**Phase-0 status.** Authored, unverified.

**Flag.** `alb_ingress_cidrs` defaults to `0.0.0.0/0` for first bring-up;
restrict to the real front-door egress ranges before serving traffic.
