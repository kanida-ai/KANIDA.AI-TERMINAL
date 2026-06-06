# Architecture

One repo, one backend process, one frontend deploy, two logical products.

## System diagram

```
                    ┌──────────────────────────────────────────────┐
                    │  Vercel (www.kanida.ai) — Next.js 16 frontend │
                    └─────────────────────┬────────────────────────┘
                                          │ HTTPS
                                          ▼
                    ┌──────────────────────────────────────────────┐
                    │ Cloudflared tunnel → api.kanida.ai (443)     │
                    │ uuid 7c7ef96e-240d-488a-9690-bb76bc13c34e    │
                    └─────────────────────┬────────────────────────┘
                                          │ HTTP
                                          ▼
   ╔══════════════════════════════════════════════════════════════════╗
   ║        backend/main.py (FastAPI, uvicorn :8001, on laptop)        ║
   ║                                                                   ║
   ║   Legacy routers          Falcon routers        Power User routers║
   ║   /api/quant,/admin,...    /api/falcon/*          /api/power/*     ║
   ║   (11, mostly stale)       (5)                    (8, invite-only) ║
   ║                                                                   ║
   ║   Background threads (started in lifespan):                       ║
   ║    • _schedule_daily_pipeline  (V7 EOD chain, 16:05 IST)          ║
   ║    • replay_warmer             (every 6h)                         ║
   ║    • position_monitor          (60s, market hours)                ║
   ║    • KiteTicker WebSocket      (live ticks, auto-trade)           ║
   ║   NOTE: Zerodha auth is NO LONGER a backend thread — it runs as   ║
   ║   a standalone Scheduled Task (see setup-guide.md, Fix 1).        ║
   ╚════════════════════╤════════════════════════════════╤════════════╝
                        ▼                                ▼
        ┌───────────────────────────┐     ┌─────────────────────────────┐
        │ data/db/                  │     │ universe_engine/data/db/    │
        │  kanida_universe.db PROD  │     │  kanida_universe.db  (RND)  │
        │   546 MB                  │     │   13+ GB                    │
        │   falcon_signals_live     │     │   falcon_outcomes (827k)    │
        │   falcon_features         │◄────│   ohlc_1min (87.8M rows)    │
        │   falcon_pattern_taxonomy │ R/O │   pattern mining            │
        │   falcon_top10_audit      │     └─────────────────────────────┘
        │   power_user_*            │
        │                           │     External:
        │  kanida_quant.db  LEGACY  │       Kite Connect API (token-gated)
        │   83 MB                   │       Zerodha Web (Playwright auth)
        │   kite_tokens             │       Vercel / GitHub (deploy)
        │   falcon_auth_log         │
        └───────────────────────────┘
```

## The two products

### Power User (`/power/*`, `backend/power_user/`)
- **Auth:** invite code → `power_jwt` HTTP-only cookie (JWT). See `power_user/auth`.
- **Surface:** daily Top-10 picks with 3-bucket explainability, Co-Trader
  persona portfolios, live intraday overlay, replays, position sizing.
- **Reads:** `kanida_universe.db` PROD (signals/features/patterns) + the R&D DB
  (read-only, for historical outcomes).
- **Owns no schedulers** — consumes the V7 pipeline's output.

### Falcon Auto-Trade (`/falcon/*`, `backend/falcon/`)
- **Auth:** HTTP Basic Auth via `frontend/middleware.ts` (`SITE_USER`/`SITE_PASS`).
- **Surface:** operator-only — premarket staging, deploy queue, live positions,
  manual trade panel, engine playbook config.
- **Owns:** the V7 pipeline + live execution on Zerodha (KiteTicker, order placement).

### What's shared
| Component | Why shared |
|-----------|-----------|
| `backend/services/kite_auth.py` | One Kite token, read by both (Power User shows auth status; Falcon places orders). |
| `backend/services/zerodha_auto_auth.py` | The Playwright auth bot that refreshes that token. |
| `config/.env` | All secrets (KITE_*, ZERODHA_*, POWER_*). |
| `kanida_universe.db` PROD | V7 signals feed both the auto-trader and the Power User cards. |

## The V7 daily pipeline (16:05 IST)

`backend/falcon/jobs/_pipeline.py` runs the chain:
1. `daily_data_refresh` — fetch latest Kite bars
2. `daily_features` — compute features for gap dates
3. `daily_signals` — emit Top-N picks (postflight invariant: refuses to record
   a stale signal_date as success — see CHANGELOG 2026-05-27)
4. `top10_audit` — populate the audit trail + close-job exit walk
5. `portfolio_engine.run_eod_for_date` — Co-Trader portfolio EOD update

The kick-off gate (`_signals_fresh_for_now`) compares
`falcon_signals_live.MAX(signal_date)` against the date expected for the current
IST window — NOT a run-log timestamp. This is the source of truth for "is today
done?" and is shared by the boot catch-up and the retry loop.

## Databases — which is which

| DB | Size | Role | Key tables |
|----|------|------|-----------|
| `data/db/kanida_universe.db` | 546 MB | **PROD** | `falcon_signals_live`, `falcon_features`, `falcon_pattern_taxonomy`, `falcon_top10_audit`, `power_user_users`, `power_user_invite_codes`, `power_user_waitlist`, `portfolio_*` |
| `data/db/kanida_quant.db` | 83 MB | **LEGACY** | `kite_tokens`, `falcon_auth_log`, legacy backtest tables |
| `universe_engine/data/db/kanida_universe.db` | 13+ GB | **R&D** | `falcon_outcomes`, `ohlc_1min`, mining outputs |

> ⚠️ `KANIDA_DB_PATH` in `config/.env` points at the **legacy** `kanida_quant.db`
> (used by `data_freshness.py` and `kite_auth.py` for tokens/auth-log). The PROD
> data lives in `kanida_universe.db`. Don't conflate them.

## Auth layers (two independent gates)

1. **Power User JWT** — `power_jwt` cookie, gates `/power/*` app data. A non-admin
   invitee gets a JWT; admin gets a JWT with `role=admin`.
2. **HTTP Basic Auth** (`middleware.ts`) — gates operator surfaces (`/`, `/falcon/*`,
   `/admin`, `/analysis*`). Fails **closed** (503) if `SITE_USER`/`SITE_PASS`
   are unset. A Power User invitee can never reach `/falcon/*` even by typing the
   URL — Basic Auth blocks them independently of the JWT.

The "Full Kanida.AI mode" admin toggle (UserMenu) only controls nav-link
*visibility* — access is still enforced by these two gates.
