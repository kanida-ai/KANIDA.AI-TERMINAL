# Agent Builder — drop into your existing app (zero overwrites)

This module was built to **repurpose your existing `Kanida.ai Terminal Quant Intelligence Engine`** —
its FastAPI backend, power-user auth/billing, Next.js frontend, and AWS Terraform/Docker deploy. Nothing
in that app is modified; you copy this module in and add **one line**.

## 1. Backend — copy the module + one line
1. Copy `agent_builder/` → `backend/agent_builder/`.
2. In `backend/main.py`, next to the other `include_router` calls, add:
   ```python
   from agent_builder.router import router as agent_builder_router
   app.include_router(agent_builder_router, prefix="/api", tags=["Builder"])
   ```
3. Add to `backend/requirements.txt` (if missing): `duckdb>=0.10`, `pyarrow>=14` (pandas/numpy already present).

That's the only change to existing files (one import + one line). Endpoints appear under `/api/builder/*`.

## 2. Auth — reuse power-user (don't build new auth)
`router.get_user_id()` is a placeholder that reads `X-User-Id`. In prod, replace it with your existing
power-auth JWT dependency so the wallet is tied to the real `power_user_users.id`. One function swap.

## 3. Wallet — reuse power_user DB (one migration)
Standalone, the wallet uses its own SQLite (`WALLET_DB`). To co-locate with power-user:
```sql
ALTER TABLE power_user_users ADD COLUMN token_balance INTEGER NOT NULL DEFAULT 0;
```
then repoint `wallet.py` at the power_user DB (or re-implement its 3 functions against `power_user_users`).
Top-ups can flow through your existing `power_billing_router` (Razorpay) → credit `token_balance`.

## 4. Data — move 1-min/daily to S3 as Parquet
```
python convert_to_parquet.py --db ../db/kanida.db --out ./parquet/daily --table ohlc_daily
python convert_to_parquet.py --db ../db/kanida.db --out ./parquet/1min  --table ohlc_1min   # heavy/optional
aws s3 sync ./parquet/daily s3://YOUR-BUCKET/kanida/daily/
```
Then set env on the service:
```
AGENT_DATA_URI=s3://YOUR-BUCKET/kanida/daily/
AGENT_NIFTY_SYMBOL=NIFTY 50
AWS_REGION=ap-south-1
```
Before the S3 move it runs on the local DB via `AGENT_SQLITE_FALLBACK=../db/kanida.db`.

## 5. Frontend — reuse the Next.js app
Add `frontend/lib/builder-api.ts` (mirror `backtest-api.ts`: `POST /api/builder/quote`, `/backtest`,
`GET /api/builder/indicators`, `/wallet`) and a page `frontend/app/power/builder/page.tsx` using the
existing `power-auth` + `terminal-ui`. The Next.js app CAN call the API (unlike the claude.ai artifact).

## 6. Deploy — reuse your infra
Fold `agent_builder/` into the existing backend image, or run this module's `Dockerfile` as its own
ECS/Fargate service. Deploy via your existing `deploy/terraform/`.

## API quick reference
| Method | Path | Purpose |
|---|---|---|
| GET  | `/api/builder/indicators` | catalog for the builder form (13 indicators, ops, exits) |
| POST | `/api/builder/quote` | token cost for a strategy (price before running) |
| POST | `/api/builder/backtest` | charge wallet → run backtest → evidence card + Market Worlds |
| GET  | `/api/builder/wallet` | balance |
| POST | `/api/builder/wallet/topup` | add tokens (wire to Razorpay) |

## Local test
```
pip install -r requirements.txt
set AGENT_SQLITE_FALLBACK=..\db\kanida.db
uvicorn app:app --port 8010      # open http://localhost:8010/docs
```
