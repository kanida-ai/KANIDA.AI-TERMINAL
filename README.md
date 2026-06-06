# KANIDA.AI

Engineered edge for Indian equities. One repo, one backend, one frontend deploy,
serving **two logical products** that share infrastructure:

| Product | Audience | Frontend | Backend |
|---------|----------|----------|---------|
| **Power User** | Invite-only retail beta | `/power/*` | `backend/power_user/` |
| **Falcon Auto-Trade** | Operator only (auto-execution on Zerodha) | `/falcon/*` | `backend/falcon/` |

Both are powered by the **Falcon Top 10** engine: 1,900+ historically-validated
patterns mined from a 9-year walk-forward pipeline, emitting daily ranked picks
with plain-English explanations.

## Quick orientation

- **New here? Start with [`docs/architecture.md`](docs/architecture.md)** — the
  full system diagram and how the pieces fit.
- **Running the system?** → [`docs/setup-guide.md`](docs/setup-guide.md) — the
  operational runbook (start backend, deploy, env vars, recovery procedures).
- **Looking for an endpoint?** → [`docs/api-map.md`](docs/api-map.md)
- **Where does code live?** → [`docs/folder-structure.md`](docs/folder-structure.md)
- **What changed and why?** → [`docs/CHANGELOG.md`](docs/CHANGELOG.md)

## The 30-second mental model

```
Vercel (Next.js)  →  Cloudflared tunnel (api.kanida.ai)  →  FastAPI backend (laptop :8001)
                                                                  │
                          ┌───────────────────────────────────────┼──────────────────────┐
                          ▼                                        ▼                      ▼
              data/db/kanida_universe.db          data/db/kanida_quant.db   universe_engine/.../kanida_universe.db
              (PROD: signals, features,           (LEGACY: kite_tokens,     (R&D: mining, 1-min bars,
               patterns, power_user_*)             auth_log)                 historical outcomes — 13+ GB)
```

- The backend runs as a **single FastAPI/uvicorn process on a laptop**, port 8001.
- A daily **V7 pipeline** (16:05 IST) refreshes data → features → signals →
  Top-10 audit → Co-Trader portfolios.
- A **standalone Windows Scheduled Task** (`KanidaZerodhaAuth`) refreshes the
  Zerodha token every 30 min in a fresh process (see setup-guide for *why*
  fresh-process + machine-wide Playwright path matter).

## Repo layout (top level)

| Path | What |
|------|------|
| `backend/` | FastAPI app. `power_user/` + `falcon/` are the two products; `services/` is shared (Kite auth); `routers/` is legacy operator API. |
| `frontend/` | Next.js 16 app. `app/power/*` + `app/falcon/*` are active; other top-level routes are legacy. |
| `universe_engine/` | R&D — pattern mining + walk-forward validation. Holds the 13+ GB research DB. |
| `data/db/` | Production + legacy SQLite DBs. |
| `scripts/` | Ops + data + research scripts (`run_auth_worker.bat`, `start_backend.bat`, `repair_playwright.bat`, …). |
| `config/` | `.env` (gitignored secrets), templates, auth-bot setup docs. |
| `deploy/` | Cloudflared tunnel config. |
| `docs/` | All documentation (this directory). |
| `archive/` | Local-only stale artifacts (gitignored). |

## Status

Beta. The two things that matter operationally:
1. The pipeline keeps emitting daily signals.
2. Users don't get kicked out of the site.

See [`docs/CHANGELOG.md`](docs/CHANGELOG.md) for the incident/fix history.
