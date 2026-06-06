# Power User backend

The invite-only retail beta. Serves `/api/power/*` and feeds the `/power/*`
frontend. Auth is a `power_jwt` HTTP-only cookie (JWT) gated by invite codes.

## Layout

| Path | What |
|------|------|
| `routers/` | 8 routers — see `../../docs/api-map.md` |
| `services/` | Business logic (below) |
| `tests/` | Unit tests (`pytest`) — the parity suite lives here |
| `SPEC/` | Requirements / Design / Tasks (spec-driven dev) |
| `config.py` | Reads `config/.env`; exposes `POWER_DB_PATH` |
| `db_init.py`, `db_schema.sql` | Schema bootstrap |

## Key services

| Service | Role |
|---------|------|
| `falcon_top20_explainer.py` | Builds the `/power/today` cards — re-evaluates all patterns per stock, 3-bucket explainability. |
| `pattern_narrator.py` | Turns pattern rules + the stock's **actual** feature values into plain-English narrative. (Grounding in actual values — not thresholds — is load-bearing; see CHANGELOG 2026-06-02.) |
| `persona_simulator.py` | Backtest engine for the 6 Co-Trader personas. `PERSONA_CONFIGS` is the single source of truth. |
| `portfolio_engine.py` | LIVE Co-Trader EOD engine (`run_eod_for_date`) — runs as V7 pipeline step 5. |
| `auth_status.py` | Single source of truth for "is Zerodha auth healthy" — read by the admin panel + the live-tier degradation banner. Exposes `browser_health`. |
| `web_push.py` | VAPID Web Push (Layer 2 auth fallback). |
| `replay_warmer.py` | Keeps the landing-page replay cache hot. |

## Data sources

- **PROD** `data/db/kanida_universe.db` — signals, features, patterns, `power_user_*`.
- **R&D** `universe_engine/data/db/kanida_universe.db` — read-only, historical outcomes.
- Token/auth state via `services/kite_auth.py` (legacy `kanida_quant.db`).

## Tests

```cmd
cd backend
C:\Users\SPS\anaconda3\python.exe -m pytest power_user/tests -q
```
The parity suite guards the `falcon-top-10` persona engine against drift.
