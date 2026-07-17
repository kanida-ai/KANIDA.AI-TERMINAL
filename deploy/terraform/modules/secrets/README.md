# module: secrets

**Purpose.** A customer-managed KMS key (rotation on) + one **empty** Secrets
Manager placeholder per env-secret key name. Fargate injects these into the task
at runtime; values never bake into the image or git.

**SPOF / requirement addressed.** Moves `config/.env` off the laptop disk into an
audited, rotatable, access-controlled store. The KMS key is reused by RDS + S3
so one key governs all encryption at rest.

**Load-bearing keys (see deploy/SECRETS_MAP.md for the full map).**
- `FALCON_VAULT_KEY` — gates per-user egress-proxy decryption + broker-credential
  decryption. Lose it and accounts silently drop to direct/rejected. Back it up
  before anything else.
- `POWER_JWT_SECRET` — HS256 portal-auth signing key; change ⇒ all users logged
  out. (`FALCON_JWT_SECRET` was removed — no code path reads it; see SECRETS_MAP.md.)

**Phase-0 status.** Authored, unverified. Placeholders are created EMPTY; the
operator sets values out-of-band (`aws secretsmanager put-secret-value ...`).
Terraform deliberately creates NO secret versions so no value ever transits
Terraform state.
