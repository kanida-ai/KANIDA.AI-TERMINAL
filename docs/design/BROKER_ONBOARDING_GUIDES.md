# Broker Onboarding Guides (User-Facing)

Per-broker connection guides surfaced in the AutoTrade "Connect your broker" flow. Each is a 4–5 step guide the UI renders after the user picks a broker from the dropdown.

> **Accuracy note:** auth flows below are grounded in each broker's official developer docs (linked). Items marked **(verify at certification)** must be confirmed against the live API during Stage 2 paper-testing before we advertise them — brokers change limits/fees. Token lifetimes and MTF/GTT/basket support especially must be certified per account, not assumed.

---

## 1. Zerodha (Kite Connect) — LIVE

- **API access required?** Yes. Kite Connect is a **paid** developer add-on (subscription per app). Create the app at [developers.kite.trade](https://developers.kite.trade).
- **Get credentials:** Create a Kite Connect app → note the **API Key** and **API Secret**. Set the app's redirect URL to our callback.
- **Connect:** User clicks **Connect Zerodha** → we open the Kite login URL → user logs in + authorizes → Kite redirects back with a `request_token` → we exchange it (api_key + api_secret) for an **access token**.
- **Daily login / refresh?** **Daily login required.** Kite access tokens **expire every morning (~06:00 IST)** and there is **no refresh token** — the user must reconnect each trading day (one click). The account card shows **EXPIRED → Reconnect** each morning.
- **Confirm active:** Green **ACTIVE** badge on the account card (backed by a live `kite.profile()` ping).
- **Limitations:** MTF ✅, GTT-OCO ✅ (broker-held stop/target backup), basket = multiple orders, SL/SL-M ✅. Live orders also require the SEBI-registered static egress IP on the app's allowed-IPs. AutoTrade fully supported.

**Sources:** [developers.kite.trade](https://developers.kite.trade), [kite.trade/docs/connect](https://kite.trade/docs/connect/v3/)

---

## 2. FivePaisa (5paisa Xstream / Developer API)

- **API access required?** Yes — 5paisa **Developer/Xstream API**. Enable it in your 5paisa account and create an API app to get your keys.
- **Get credentials:** From the 5paisa developer portal, obtain **App/User Key + Encryption Key + (Client/User ID)**. **TOTP two-factor must be enabled** (Security Settings → Enable TOTP → scan the QR in Google Authenticator/Authy).
- **Connect:** OAuth login flow → user authenticates (with TOTP) → a **request token** is returned → we exchange it (AppKey in header; UserID + encryptionKey in body) for an **access token**.
- **Daily login / refresh?** **Daily** — the access token is **valid throughout the day** (expires daily), so a daily reconnect is required (similar to Kite). No long-lived refresh token **(verify at certification)**.
- **Confirm active:** ACTIVE badge, backed by a lightweight authenticated call (e.g. margin/holdings).
- **Limitations:** Orders reference **ScripCode** (numeric), not symbol — the adapter maps our symbols → 5paisa ScripCodes via the scrip master. MTF / GTT / basket / SL support **(verify at certification)**. Official Python SDK: `py5paisa`.

**Sources:** [5paisa Xstream dev docs](https://xstream.5paisa.com/dev-docs/user-authentication-system/access-token), [py5paisa SDK](https://github.com/5paisa/py5paisa), [5paisa developer API](https://5paisa.com/developerapi/order-request-place-order)

---

## 3. Rupeezy (Vortex API — formerly AsthaTrade)

- **API access required?** Yes — **Vortex API** (free trading API). Register an application on the Vortex developer portal.
- **Get credentials:** Create your app → get **application_id** and **x-api-key** (keep the x-api-key secret). Configure the redirect URL and (optionally) a **postback/webhook URL** for order/trade updates.
- **Connect:** User is sent to **[flow.rupeezy.in](https://flow.rupeezy.in)** with your `application_id` → logs in → we receive the auth artifact → exchange for an **access token** used in the `Authorization` header for all subsequent calls.
- **Daily login / refresh?** Session-based token in the `Authorization` header; exact lifetime/refresh **(verify at certification)** — the lifecycle layer detects expiry via a health ping and prompts reconnect if needed.
- **Confirm active:** ACTIVE badge, backed by a Portfolio/positions ping. Rupeezy also pushes **postbacks (webhooks)** for order/trade fills — we can use these for near-real-time execution tracking.
- **Limitations:** Order Management supports all exchanges + order types incl. **stop-loss**. MTF / GTT / basket support **(verify at certification)**. Python SDK: `pyvortex`.

**Sources:** [Vortex API docs](https://vortex.rupeezy.in/docs/2.1/), [Vortex authentication](https://vortex.rupeezy.in/docs/1.0/authentication/), [Register for Vortex API](https://support.rupeezy.in/support/solutions/articles/21000001388-how-to-register-for-trading-api-through-vortex-)

---

## Capability matrix (drives the engine; certify per broker)

| Broker | auth_kind | Refresh token | Token lifetime | 2FA | Order key | GTT | MTF | Postbacks |
|---|---|---|---|---|---|---|---|---|
| Zerodha | request_token | No | daily ~06:00 IST | at login | symbol | ✅ | ✅ | no (poll) |
| FivePaisa | oauth_request_token | (verify) | daily | **TOTP** | **ScripCode** | (verify) | (verify) | (verify) |
| Rupeezy | oauth2_flow | (verify) | session (verify) | at login | token/instrument | (verify) | (verify) | ✅ webhooks |

The AutoTrade engine branches only on these **capability flags** — never on the broker name. A broker with no GTT falls back to the software stop only; a broker with postbacks uses them for fills, others poll `get_order_status`.

---

## The universal onboarding UX (Tradetron-style)

1. **Pick broker** from a dropdown (only registered brokers appear; "live" vs "coming soon" badge).
2. UI renders **this broker's step guide** (the sections above) + the exact credential fields that broker needs.
3. User completes **login / API setup / token authorization** on the broker's own site (we never see the password; secrets are entered once and encrypted into the vault).
4. Account is **connected to their profile** → shows PENDING → ACTIVE.
5. User can now **run eligible AutoTrade strategies** through that account. Daily-expiry brokers show a one-click **Reconnect** each morning; the account's health badge is always visible.
