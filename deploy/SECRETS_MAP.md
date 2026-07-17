# deploy/SECRETS_MAP.md — config/.env → AWS Secrets Manager / KMS

**Cloud-Migration PHASE 0 — DOCUMENT ONLY.** This maps every key currently in
`config/.env` to its cloud target. It lists **KEY NAMES ONLY** — no secret
VALUES are printed, moved, or committed anywhere. Nothing here has been applied.

**How env reaches the app today (verified).** `backend/main.py` (lines ~20-36)
loads `config/.env` at import via `load_dotenv` + a manual parser, then reads
`os.environ`. In the container we DO NOT ship `config/.env`; the same variables
arrive from the container environment — locally via `docker-compose` `env_file`,
in cloud via ECS **Secrets Manager injection** (`modules/secrets` + the task
def's `secrets=[valueFrom]`). No app code change is required for this.

## Classification

- **SECRET** → AWS Secrets Manager, KMS-encrypted, injected into the task at
  runtime. Never in the image, tfvars, or state.
- **CONFIG** (non-sensitive) → plain task-def environment (or SSM Parameter
  Store). Fine to keep in `terraform.tfvars` / task env.

| Key (config/.env) | Class | Cloud target | Load-bearing? / notes |
|---|---|---|---|
| `FALCON_VAULT_KEY` | SECRET | Secrets Manager + KMS | **CRITICAL.** Fernet key that decrypts broker creds (`api_secret_enc`, `access_token_enc`) AND per-user egress proxy URLs (`egress_proxy_url_enc`). **Lose/rotate it wrong and every account silently fails-open to DIRECT/rejected egress** (see `backend/autotrade/broker/egress.py`, `autotrade/vault.py`). **Back up FIRST; migrate FIRST.** Rotate via Fernet MultiFernet (old+new) grace window. |
| `POWER_JWT_SECRET` | SECRET | Secrets Manager | HS256 signing key for portal auth (`power_user/config.py`). Change ⇒ all users logged out. |
| ~~`FALCON_JWT_SECRET`~~ | REMOVED | — | **UNUSED — do not create.** Was listed in the migration brief as a gate but **no code path reads it** (auth reads `POWER_JWT_SECRET`). Removed from `terraform/variables.tf managed_secret_keys` so the operator isn't handed a placeholder secret nothing consumes. |
| `POWER_ADMIN_SECRET` | SECRET | Secrets Manager | Admin login code (with `POWER_ADMIN_EMAIL`). Backend needs it at startup for admin auth. |
| `ADMIN_SECRET` | SECRET | Secrets Manager | Legacy operator admin gate; `POWER_ADMIN_SECRET` falls back to it (`power_user/config.py`). |
| `FALCON_OPERATOR_TOKEN` | SECRET | Secrets Manager | Gates `/api/autotrade/*` operator routes. |
| `KITE_API_KEY` | SECRET | Secrets Manager | Zerodha app key. |
| `KITE_API_SECRET` | SECRET | Secrets Manager | Zerodha app secret. |
| `KITE_ACCESS_TOKEN` | SECRET (ephemeral) | Secrets Manager or DB | Daily-expiring token. In cloud, prefer the per-account token store; a static Secrets entry goes stale daily. Flag for the auth-refresh redesign. |
| `ZERODHA_USERNAME` | SECRET | Secrets Manager | Headless auth (Playwright). |
| `ZERODHA_PASSWORD` | SECRET | Secrets Manager | Headless auth. |
| `ZERODHA_TOTP_SECRET` | SECRET | Secrets Manager | TOTP seed (pyotp). Highly sensitive — 2FA seed. |
| `ANTHROPIC_API_KEY` | SECRET | Secrets Manager | Claude API (Ask-Falcon). |
| `POLYGON_API_KEY` | SECRET | Secrets Manager | US market data. |
| `VAPID_PRIVATE_KEY` | SECRET | Secrets Manager | Web Push signing (`power_user/services/web_push.py`). |
| `VAPID_PUBLIC_KEY` | CONFIG | task env | Public half; safe as plain env. |
| `VAPID_CONTACT` | CONFIG | task env | Contact mailto. |
| `BROKER_PROXY_MAP` | SECRET | Secrets Manager | Legacy hand-written account→proxy map; the URLs embed proxy credentials. |
| `BROKER_EGRESS_POOL` | SECRET | Secrets Manager | Pool of credentialed proxy URLs; assembled from `modules/egress` output. URLs carry passwords. |
| `LIGHTSAIL_PROXY_USER2` | SECRET | Secrets Manager | Existing per-user proxy URL (credentialed). Superseded by the EIP-per-user egress module in cloud. |
| `RUPEEZY_LIVE_CERTIFIED` | CONFIG (gate) | task env / SSM | Boolean live-cert gate. Not a secret, but a **real-money gate** — treat changes as privileged. |
| `FALCON_AUTOTRADE_ENABLED` | CONFIG (gate) | task env / SSM | Master live-trading gate. Privileged. |
| `FALCON_AUTOTRADE_EXECUTION_MODE` | CONFIG | task env | paper/live mode. |
| `FALCON_AUTOTRADE_AUTO_TRIAGE` | CONFIG | task env | Feature flag. |
| `FALCON_AUTOTRADE_BROKER_RECONCILE` | CONFIG | task env | Feature flag. |
| `AUTOTRADE_PORTAL_STRICT` | CONFIG | task env | Tenant-isolation strictness (on). |
| `COMPLIANCE_MODE` | CONFIG | task env | Flag. |
| `POWER_ADMIN_EMAIL` | CONFIG | task env | Admin email (public knowledge; auth needs the secret too). |
| `POWER_INVITE_LOGIN_LIMIT_PER_HOUR` | CONFIG | task env | Rate limit. |
| `DATA_QUALITY_MAX_MOVE_PCT` | CONFIG | task env | Data-quality guard. |
| `DATA_QUALITY_MIN_VOLUME_RATIO` | CONFIG | task env | Data-quality guard. |
| `NSE_MARKET_OPEN_IST` / `NSE_MARKET_CLOSE_IST` | CONFIG | task env | Market hours. |
| `US_MARKET_OPEN_ET` / `US_MARKET_CLOSE_ET` | CONFIG | task env | Market hours. |
| `KANIDA_DB_PATH` | CONFIG (path) | task env | Legacy DB path → set to the mounted/volume path (see Dockerfile). NOT a secret. |
| `BACKEND_HOST` / `BACKEND_PORT` | CONFIG | task env | Bind config; container uses PORT=8001. |
| `FRONTEND_URL` | CONFIG | task env | CORS/redirect origin. |

> If a key exists in `config/.env` but is missing above, treat it as **SECRET
> until proven CONFIG** and add it — fail safe.

## Migration order (do NOT reorder blindly)

1. **`FALCON_VAULT_KEY` first.** Back it up out-of-band, then create its Secrets
   Manager entry. Everything broker-credential- and egress-related depends on it;
   a wrong/missing vault key silently drops accounts to DIRECT/rejected — a
   real-money failure mode, not a loud one.
2. **Auth/JWT/admin secrets** (`POWER_JWT_SECRET`, `POWER_ADMIN_SECRET`,
   `ADMIN_SECRET`, `FALCON_OPERATOR_TOKEN`) — needed for the app to authenticate
   anyone at boot.
3. **Broker + market-data secrets** (`KITE_*`, `ZERODHA_*`, `POLYGON_API_KEY`,
   `ANTHROPIC_API_KEY`).
4. **Egress secrets** (`BROKER_EGRESS_POOL`, `BROKER_PROXY_MAP`) — assemble from
   the `modules/egress` EIP outputs AFTER those boxes exist.
5. **CONFIG values + gates** — set task env; keep `FALCON_AUTOTRADE_ENABLED` and
   `RUPEEZY_LIVE_CERTIFIED` OFF until the cloud path is proven end-to-end.

## Rules

- Terraform creates the KMS key + **empty** Secrets Manager placeholders only.
  Operator sets VALUES out-of-band: `aws secretsmanager put-secret-value
  --secret-id kanida-prod/env/<KEY> --secret-string <value>`.
- No secret value in git, tfvars, task defs, image layers, or Terraform state.
- KEEP a durable off-cloud backup of `FALCON_VAULT_KEY` (memory: "vault ON —
  back up FALCON_VAULT_KEY!").
