# module: rds

**Purpose.** Managed PostgreSQL: Multi-AZ, PITR, private-subnet-only, KMS-encrypted
at rest. Master password is generated and stored in Secrets Manager.

**SPOF / requirement addressed.** Replaces the single SQLite file on one laptop —
the biggest data-loss + availability SPOF. Multi-AZ = auto standby failover;
`backup_retention_days` > 0 = point-in-time recovery; `deletion_protection` +
`final_snapshot` guard against accidental loss.

**Phase-0 status.** Authored, unverified. Stood up **EMPTY**; the app is NOT
wired to it (no `DATABASE_URL`). Pointing the app at RDS — and the SQLite→PG
data migration — is a LATER phase. `backend/db.py` already supports Postgres
(`IS_POSTGRES` on `DATABASE_URL`), so the app-side switch is a one-env-var change
once the data is migrated.

**Flags / trade-offs.**
- `deletion_protection = true` and `skip_final_snapshot = false` mean `terraform
  destroy` will refuse / snapshot first — intentional for a prod DB.
- `engine_version` pinned to 15.7; confirm the exact minor is still offered in
  ap-south-1 at apply time.
