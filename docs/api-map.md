# API Map

Every endpoint, grouped by product. Mount points are in `backend/main.py`.

## Power User — `/api/power/*` (8 routers, invite-gated)

| Router file | Mount | Purpose |
|-------------|-------|---------|
| `power_user/routers/auth_router.py` | `/api/power/auth` | Google + invite sign-in, `/me`, logout |
| `power_user/routers/invites_router.py` | `/api/power/invites` | Public redeem + waitlist |
| `power_user/routers/admin_router.py` | `/api/power/admin` | Invite issue/revoke, user list/deactivate, metrics, Zerodha auth status, jobs control, push subscribe, replay-warm |
| `power_user/routers/picks_router.py` | `/api/power/picks` | Live-tier intraday picks |
| `power_user/routers/auth_refresh_router.py` | `/api/power/auth-refresh` | Magic-link token-refresh flow |
| `power_user/routers/portfolios_router.py` | `/api/power/portfolios` | Co-Trader personas — listings, positions, equity, trades |
| `power_user/routers/persona_backtest_router.py` | `/api/power/personas` | Persona simulator output (yearly/monthly/trades/reconciliation) |
| `power_user/routers/falcon_top20_router.py` | `/api/power/today` | Falcon Top-10 + 3-bucket explainability (the `/power/today` data) |

## Falcon Auto-Trade — `/api/falcon/*` (5 routers, operator-only)

| Mount | Purpose |
|-------|---------|
| `/api/falcon/signals` | EOD signal management |
| `/api/falcon/portfolio` | Active Kite positions |
| `/api/falcon/patterns` | Pattern taxonomy + audit |
| `/api/falcon/admin` | Engine playbook config, pipeline trigger, freshness |
| `/api/falcon/trade` | Premarket staging, deploy queue, position monitor |

## Legacy — `/api/*` (11 routers, status mixed)

Mounted in `main.py` but mostly pre-Falcon. **Audit consumers before removing.**

| Mount | Likely status |
|-------|---------------|
| `/api/jobs` | **Still used** (pipeline trigger) |
| `/api/universe` | **Still used** (stock lookups) |
| `/api/ai` | Used by `/terminal/chat` |
| `/api/swing` | May power `/analysis` |
| `/api/admin` | OG admin — used by legacy `/admin`, `/analysis` |
| `/api/quant`, `/api/backtest`, `/api/live`, `/api/execution`, `/api/orders`, `/api/strategy` | Likely deprecated — verify no live consumer before unmounting |

## Frontend routes

**Active:** `/power/*` (Power User), `/falcon/*` (operator).
**Legacy (candidates for archive after usage audit):** `/admin`, `/analysis`,
`/analysis-v2`, `/analysis-v3`, `/dashboard`, `/engine`, `/terminal*`, `/welcome`,
`/login`.

Next.js rewrites (`next.config.ts`) proxy `/api/power/*` and friends to
`BACKEND_ORIGIN` (api.kanida.ai). `middleware.ts` applies HTTP Basic Auth to
everything except `/power/*`.
