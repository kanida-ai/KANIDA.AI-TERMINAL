# Rupeezy (Vortex API) — Adapter Build Reference

Real spec extracted from the official Vortex docs, for building `RupeezyBroker(BrokerClient)` + `RupeezyAuth(BrokerAuthProvider)`. Items marked **CONFIRM** need verification against the live docs/API during the build (a few pages are access-gated).

**Sources:** [Vortex auth](https://vortex.rupeezy.in/docs/1.0/authentication/), [regular orders](https://vortex.rupeezy.in/docs/2.1/regular-order/), [docs home](https://vortex.rupeezy.in/docs/2.1/)

---

## Authentication (partner-app / OAuth flow — what we use)

- **App credentials (operator/per-user):** `application_id` + `x-api-key` (secret) from the Vortex developer portal.
- **Step 1 — login redirect:** send the user to `https://flow.rupeezy.in?applicationId=<application_id>`.
- **Step 2 — callback:** capture the `auth` query param on our registered redirect URL.
- **Step 3 — exchange:** `POST {base_url}/user/session`
  ```json
  { "checksum": "<SHA-256>", "applicationId": "<application_id>", "token": "<auth>" }
  ```
  **checksum = SHA-256(application_id + auth + x-api-key)**
- **Response:** `data.access_token` (RS512 JWT), `user_name`, `user_id`, `tradingActive`.
- **Subsequent calls:** header `Authorization: Bearer <access_token>`.
- **Expiry/refresh:** not documented → **CONFIRM**. Lifecycle layer detects expiry via a `validate()` health ping (cheap authenticated GET) and prompts reconnect. Treat as session-scoped; no assumed refresh token.

**Maps to `BrokerAuthProvider`:**
- `capabilities = BrokerCapabilities(auth_kind="oauth2_flow", has_refresh_token=False /*CONFIRM*/, token_lifetime="session", supports_gtt=True, supports_mtf=True, fno=True)`
- `login_url(creds, redirect_uri, state)` → `https://flow.rupeezy.in?applicationId={api_key_app_id}` (state via our redirect)
- `exchange(creds, request_token=auth)` → compute checksum, `POST /user/session` → `TokenSet(access_token=...)`
- `refresh` → `RefreshNotSupported` (until CONFIRM says otherwise)
- `validate(creds)` → authenticated funds/positions GET → `TokenHealth`
- `expiry` → None (session) → health-ping drives EXPIRED detection

> Note: `x-api-key` is the app secret → store in vault `api_secret`; `application_id` → vault `api_key`.

---

## Orders (execution — `BrokerClient`)

- **Place:** `POST {base_url}/trading/orders/regular`
  | field | values |
  |---|---|
  | `exchange` | NSE_EQ, NSE_FO, NSE_CUR, MCX_FO |
  | `token` | **numeric instrument token** (needs symbol→token map) |
  | `transaction_type` | BUY, SELL |
  | `product` | INTRADAY, DELIVERY, BTST, **MTF** |
  | `variety` | RL (limit), **RL-MKT (market)**, SL (SL-limit), SL-MKT (SL-market) |
  | `quantity` | integer (lot-based for NSE_FO) |
  | `price` | tick-multiple |
  | `trigger_price` | for SL orders |
  | `disclosed_quantity` | ≤ quantity |
  | `validity` | DAY, IOC |
  | `is_amo` | true/false |
  | `gtt` (optional) | object with SL/profit triggers → our **GTT-OCO backup** |
  - Response: `data.order_id`.
- **Modify:** `PUT {base_url}/trading/orders/regular/{order_id}` (variety, quantity, price, trigger_price, disclosed_quantity, validity)
- **Cancel:** `DELETE {base_url}/trading/orders/regular/{order_id}`; bulk `POST .../multi_delete` `{order_ids:[…≤10]}`
- **Order book:** `GET {base_url}/trading/orders` · **history:** `GET {base_url}/trading/orders/{order_id}`

**Adapter mapping (our Order → Vortex):**
- MARKET → `variety=RL-MKT`; LIMIT → `variety=RL` + price.
- `place_market_exit` → SELL RL-MKT.
- Our product: EQ/CNC→DELIVERY, MTF→MTF, MIS→INTRADAY.
- GTT-OCO → the inline `gtt` object on entry (stop + target) — **CONFIRM exact shape**.
- SL/SL-M → variety SL / SL-MKT + trigger_price.

---

## Still to CONFIRM during build (from access-gated pages)

1. **Base URL / host** for production (e.g. `https://vortex-api.rupeezy.in` — CONFIRM).
2. **Positions:** `GET {base_url}/trading/portfolio/positions` — CONFIRM path.
3. **Holdings:** `GET {base_url}/trading/portfolio/holdings` — CONFIRM.
4. **Funds/margin/balance:** `GET {base_url}/user/funds` (+ order-margin calc) — CONFIRM.
5. **Instrument master** (symbol → numeric token) download URL + format — needed for order routing + LTP. CONFIRM.
6. **Quotes/LTP** endpoint (+ whether a market-data websocket exists for live ticks).
7. **Postbacks/webhooks:** event types (order/trade fill) + how the webhook URL is registered → use for real-time fill tracking instead of polling.
8. **MTF/GTT exact semantics** + any per-segment limits.

The auth + order-placement core is fully specified above — enough to build and paper-test the adapter. The CONFIRM items are read-only/enrichment endpoints resolved during the build against the live docs (and validated with your real Rupeezy account in certification).
