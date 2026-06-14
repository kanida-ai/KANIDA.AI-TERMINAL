# Kanida.AI Launch — Environment Variables

**Secret?** = never commit; set in host dashboard (Railway/Supabase) only.
**Set by you (operator)** vs **set by code/default**.

## App DB + Auth

| Var | Secret | Set by | Purpose |
|---|---|---|---|
| `DATABASE_URL` | ✅ | You | Supabase Postgres connection string. If set, code uses Postgres; else SQLite (dev). |
| `POWER_JWT_SECRET` | ✅ | **You (CRITICAL)** | HS256 JWT signing key. Currently random per boot → users logged out on restart. **MUST be set in prod.** |
| `POWER_JWT_TTL_HR` | ❌ | default `24` | JWT validity hours |
| `POWER_DB_PATH` | ❌ | code | Local SQLite path (dev only; ignored when `DATABASE_URL` set) |
| `POWER_RND_DB_PATH` | ❌ | You (machine) | Path to the 14G R&D warehouse — stays on your machine, used by weekly mining + (until M2 precompute) historical-evidence |
| `POWER_CHECKOUT_URL` | ❌ | default `/power/billing` | Where the 402 paywall sends unpaid users (frontend billing page) |

## Billing — Razorpay

| Var | Secret | Set by | Purpose |
|---|---|---|---|
| `RAZORPAY_KEY_ID` | ✅ | You | Razorpay API key id |
| `RAZORPAY_KEY_SECRET` | ✅ | You | Razorpay API secret |
| `RAZORPAY_WEBHOOK_SECRET` | ✅ | You | HMAC secret to verify webhook authenticity |
| `RAZORPAY_PLAN_MONTHLY` | ❌ | You | Plan id for the ₹999/mo plan (created in Razorpay dashboard) |

## Email

| Var | Secret | Set by | Purpose |
|---|---|---|---|
| `EMAIL_PROVIDER` | ❌ | You | `resend` or `ses` |
| `RESEND_API_KEY` | ✅ | You | if provider=resend |
| `EMAIL_FROM` | ❌ | You | sender address (e.g. `team@kanida.ai`) |

## Operator surface + broker (unchanged, already in use)

| Var | Secret | Purpose |
|---|---|---|
| `SITE_USER` / `SITE_PASS` | ✅ | HTTP Basic Auth gating `/falcon/*` operator routes |
| `KITE_API_KEY` / `KITE_API_SECRET` | ✅ | Zerodha Kite Connect |
| `ZERODHA_USERNAME` / `ZERODHA_PASSWORD` / `ZERODHA_PIN` | ✅ | Playwright auto-auth |

## Operator checklist before first deploy

1. Create Supabase project → copy `DATABASE_URL`.
2. Generate `POWER_JWT_SECRET` (`openssl rand -hex 32`).
3. Create Razorpay ₹999/mo plan → copy plan id + keys + webhook secret.
4. (Email) create Resend account → API key.
5. Set all of the above in the host (Railway) env. **Nothing secret goes in git.**
