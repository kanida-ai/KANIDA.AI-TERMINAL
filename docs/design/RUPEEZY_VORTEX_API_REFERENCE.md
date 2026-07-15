# Rupeezy (Vortex API) — Adapter Build Reference

Real spec extracted from the official Vortex docs, for building `RupeezyBroker(BrokerClient)` + `RupeezyAuth(BrokerAuthProvider)`. Items marked **CONFIRM** need verification against the live docs/API during the build (a few pages are access-gated).

**Sources:** [Vortex auth](https://vortex.rupeezy.in/docs/1.0/authentication/), [regular orders](https://vortex.rupeezy.in/docs/2.1/regular-order/), [docs home](https://vortex.rupeezy.in/docs/2.1/)

**Base URL (verified):** `https://vortex-api.rupeezy.in/v2` · header `Authorization: Bearer <access_token>` · **order WRITES must egress the allowlisted STATIC IPv4** (a dual-stack host egresses IPv6 → `403 IP_NOT_ALLOWED`; reads do not check, masking it — the adapter forces IPv4 on every call).

---

## LIVE CERTIFICATION STATUS (2026-07-15)

Read/status paths **live-verified** by read-only probe against the operator's ACTIVE account (NO orders placed), plus the earlier live entry that produced real fills. Write/exit paths are **certified-by-shape** and await one operator-fired controlled round trip.

| Path | Endpoint | Status |
|---|---|---|
| Quotes / LTP | `GET /data/quotes?q=<exch>-<token>&mode=ltp` | **LIVE-VERIFIED** (`get_ltp("RELIANCE")=1304.3`) |
| Funds / available_margin | `GET /user/funds` → top-level `nse/mcx/exchange_combined`, `net_available` | **LIVE-VERIFIED** (₹1,099,995.75) |
| Order-margin (MIS/MTF sizing) | `POST /trading/margins/order` (qty=1) → `required_margin` | LIVE-VERIFIED path (committed f3419f3) |
| Order status (fill tracking) | `GET /trading/orders` (order book), match by `order_id`; `traded_quantity`/`traded_price`/`total_quantity`/`pending_quantity`/`status` | **LIVE-VERIFIED** on a real `EXECUTED` order `NZXAH00001?7` → `COMPLETE`, filled=1 @ 1174.70 |
| Pending orders | `GET /trading/orders` → `orders[]` (NOT `data`), rows named `symbol` | Parsing LIVE-VERIFIED (0 resting at probe time; alive-filter unexercised live) |
| Positions | `GET /trading/portfolio/positions` → `data.net[]` (`symbol,quantity,product,average_price,token,buy_quantity,sell_quantity,...`) | **LIVE-VERIFIED** (3-row net book) |
| Holdings | `GET /trading/portfolio/holdings` → `data[]` (nested `nse:{token,symbol}`) or `data:null` empty | **LIVE-VERIFIED** (3 holdings) |
| Place entry | `POST /trading/orders/regular` → `data.order_id` (contains `?`) | Real fills earlier today; body shape (RL-MKT + required `price`=LTP) certified-by-shape |
| Place exit (SELL / buy-to-cover) | `POST /trading/orders/regular` variety=RL-MKT + `price`=LTP | **certified-by-shape — needs 1 live exit** |
| Cancel | `DELETE /trading/orders/regular/{urlencoded_order_id}` | certified-by-shape (URL-encode of `?`) — needs 1 live cancel |
| Partial fill | status `PARTIALLY_EXECUTED` → surfaced as `PARTIAL` (not COMPLETE) with real `traded_quantity` | certified-by-shape (no live partial occurred) |

**Live-verified order-book row fields:** `order_id, status, error_reason, transaction_type, product, variety, total_quantity, pending_quantity, traded_quantity, order_price, trigger_price, traded_price, validity, symbol, order_identifier, tags_ids, order_number, …`. Order ids **contain a `?`** (e.g. `NZXAH00001?7`) → URL-encode everywhere they sit in a path.

> **Attribution note:** the *place* payload has no client-tag field (attribute by `order_id`), but the *order book* surfaces `order_identifier` + `tags_ids` — a possible future client-tag hook to certify (not wired; would need a live place test to confirm the request field name).

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

## CONFIRM items — resolutions (2026-07-15)

1. **Base URL / host** — RESOLVED: `https://vortex-api.rupeezy.in/v2` (verified).
2. **Positions** — RESOLVED: `GET /trading/portfolio/positions` → `data.net[]` (fields above). Verified.
3. **Holdings** — RESOLVED: `GET /trading/portfolio/holdings` → `data[]` (nested `nse:{token,symbol}`) / `data:null` empty. Verified.
4. **Funds/margin** — RESOLVED: `GET /user/funds` (top-level per-segment `net_available`) + order-margin `POST /trading/margins/order`. Verified.
5. **Instrument master** (symbol → numeric token) — STILL OPEN: the live master DOWNLOAD URL + format is not yet wired. The adapter reads a pluggable local cache (`RUPEEZY_INSTRUMENT_MASTER` / `data/config/rupeezy_instruments.json`) and NEVER fabricates a token. Orders/LTP fail loudly without it.
6. **Quotes/LTP** — RESOLVED: `GET /data/quotes?q=<exch>-<token>&mode=ltp` → `data["<exch>-<token>"].last_trade_price`. Verified. (Market-data websocket: not yet used.)
7. **Postbacks/webhooks** — STILL OPEN: fill tracking is by polling the order book (works, verified). No Vortex postback wired yet.
8. **MTF/GTT exact semantics** — STILL OPEN: MTF product places (certified-by-shape); the inline `gtt` OCO object shape is UNVERIFIED — the adapter places NO short GTT and relies on the software stop; a long GTT is best-effort (any error → None, protection never blocked).

## REMAINING for FULL live certification (human-fired, one controlled order)

The read + fill-status lifecycle is live-verified. To flip `RUPEEZY_LIVE_CERTIFIED` on, a human must fire ONE controlled round trip (`FALCON_AUTOTRADE_ENABLED=true`, tiny qty) and confirm:
- **entry** places (`data.order_id` with `?`), reconciles to the real fill via the order book;
- **exit** SELL (and a short buy-to-cover) places RL-MKT with `price`=LTP and the book flips to `EXECUTED`;
- **cancel** of a resting order succeeds with the URL-encoded `?`;
- a **partial** fill (if it occurs) reads as `PARTIAL` with the real traded qty;
- the position/holdings books reflect the round trip.

Until then the adapter stays gated: `RUPEEZY_LIVE_CERTIFIED` defaults False (blocks real orders) — do NOT change that default.
