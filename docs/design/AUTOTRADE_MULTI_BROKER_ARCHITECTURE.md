# AutoTrade — Broker-Agnostic Multi-User / Multi-Broker Architecture

**Status:** Design (for review before build). Grounded in the current codebase.
**Principle:** The **strategy engine is broker-independent**; **authentication, token refresh, account access, and order routing happen at the (user → broker-account) level.**

---

## 1. Design goal (restated)

One user can connect and manage multiple broker accounts (Kite, Upstox, Angel One, Dhan, Fyers, …) inside their profile. Each account carries its own authentication, token lifecycle, health, and order routing. The strategy logic (Falcon picks → sizing → entry → kill-switch/trail → exit) never knows which broker it is running on.

Two orthogonal axes — keep them separate:

```
        STRATEGY ENGINE  (broker-independent — one code path for all brokers)
                 │  talks only to interfaces ↓↓
   ┌─────────────┴───────────────┐
   │                             │
BrokerClient (EXECUTION)     BrokerAuthProvider (AUTH/TOKEN)      ← two interfaces
   │  place/exit/quotes/         │  login-url / exchange / refresh /
   │  margins/GTT/positions      │  expiry / re-auth / health
   └─────────────┬───────────────┘
                 │ decrypted creds (in memory)
             VAULT  (per user, per broker, per account — encrypted at rest)
```

---

## 2. What exists today (reuse, don't rebuild)

- **`BrokerClient` ABC** (`broker/base.py`) — execution interface. The kill switch, capital engine, monitor, and executor already talk ONLY to this. **Keep as-is.**
- **`BrokerRouter`** (`broker/router.py`) — factory `broker_name → adapter instance`. **Extend into a proper registry.**
- **Adapters:** `zerodha.py` (LIVE) + `upstox/angel/dhan/fyers.py` (stubs). **Fill in the live adapters.**
- **Vault** (`vault.py`) — per (user, broker, account) encrypted `api_secret` + `access_token`, status state machine, Fernet + KeyProvider (KMS-ready). **Extend the schema for refresh tokens + health.**
- **Session binding** — `autotrade_sessions.broker_account_id` + `TradingSession._resolve_account_creds` already inject vaulted creds into a session leg. **Keep; generalise creds→client construction.**

**The gap:** there is no auth abstraction. `kite_auth.py` / `zerodha_auth.py` are Kite-specific (daily request-token login, no refresh token). We introduce `BrokerAuthProvider` and move Kite's flow behind it.

---

## 3. The two interfaces

### 3.1 `BrokerClient` (EXECUTION) — already exists, unchanged contract

Responsible for everything the strategy needs at run time, per account:
`get_ltp` / `get_ltps_batch`, `get_margin_per_share` / `get_margins_batch`, `get_lot_size` / `get_active_futures` / `get_option_chain`, `place_order`, `place_market_exit`, `cancel_order(_sync)`, `get_order_status`, `get_pending_orders`, `place_gtt_oco` / `cancel_gtt` / `get_gtt`, **+ add** `get_positions()` / `get_holdings()` for position tracking.

Each adapter is constructed with a resolved, authenticated session (creds handed in — the adapter does NOT do auth itself).

### 3.2 `BrokerAuthProvider` (AUTH / TOKEN) — **new**

A tiny per-broker interface owning the credential lifecycle. Proposed contract:

```python
class BrokerAuthProvider(ABC):
    broker_name: str
    capabilities: BrokerCapabilities          # see §4

    # 1. CONNECT — where to send the user to authenticate
    def login_url(self, creds: DecryptedCreds, *, redirect_uri: str,
                  state: str) -> str: ...

    # 2. EXCHANGE — turn the broker's post-login artifact into tokens
    #    (request_token / auth_code / etc. → access_token [+ refresh_token])
    def exchange(self, creds: DecryptedCreds, *,
                 request_token: str) -> TokenSet: ...

    # 3. REFRESH — silently renew using a refresh_token, if the broker supports it
    def refresh(self, creds: DecryptedCreds) -> TokenSet: ...      # raises NotSupported if none

    # 4. VALIDATE / HEALTH — is the current token live? (cheap profile ping)
    def validate(self, creds: DecryptedCreds) -> TokenHealth: ...

    # 5. EXPIRY — when does the current token die? (absolute IST, or "daily 06:00")
    def expiry(self, token_set: TokenSet) -> Optional[datetime]: ...
```

Supporting value types:

```python
@dataclass
class TokenSet:
    access_token: str
    refresh_token: Optional[str] = None
    issued_at: datetime = ...
    expires_at: Optional[datetime] = None      # None → broker-defined daily expiry

@dataclass
class TokenHealth:
    ok: bool
    status: str          # ACTIVE | EXPIRED | REVOKED | ERROR
    detail: str = ""
```

An adapter package therefore ships **two** classes: `XxxBroker(BrokerClient)` (execution) and `XxxAuth(BrokerAuthProvider)` (auth), registered together.

---

## 4. Per-broker capability matrix (the abstraction, not hardcoded facts)

Brokers differ in exactly the ways the interface must absorb. Each adapter declares its capabilities; the generic engine branches on the capability flags, never on `broker_name`.

```python
@dataclass
class BrokerCapabilities:
    auth_kind: str            # "request_token" (Kite) | "oauth2_code" (Upstox/Angel/Fyers) | ...
    has_refresh_token: bool   # can renew silently without a fresh interactive login?
    token_lifetime: str       # "daily_0600_ist" | "seconds:<n>" | "until_revoked"
    supports_gtt: bool        # broker-held OCO backup available?
    supports_mtf: bool
    fno: bool
```

> ⚠️ **Verify per broker's CURRENT API docs at build time — do not hardcode from memory.** The abstraction is what matters: some brokers expire daily with no refresh (interactive re-auth required each morning); others issue a refresh token that renews the access token silently for a longer window. The engine handles both via `has_refresh_token` + `token_lifetime`, so adding/adjusting a broker never touches strategy code.

| Broker | auth_kind | has_refresh_token | token_lifetime (CONFIRM) | Live today |
|---|---|---|---|---|
| Zerodha (Kite) | request_token | No | daily ~06:00 IST | ✅ adapter live |
| Upstox | oauth2_code | Yes (confirm) | confirm | stub |
| Angel One (SmartAPI) | oauth2_code | Yes (confirm) | confirm | stub |
| Dhan | api-key/token | confirm | confirm | stub |
| Fyers | oauth2_code | confirm | confirm | stub |

---

## 5. The generic connection & token lifecycle (one flow for all brokers)

```
CONNECT (user, in their profile)
  1. User picks broker + labels the account, enters api_key/api_secret.
     → vault.put_account(...)  → status PENDING
  2. UI requests login_url  → AuthProvider.login_url(creds, redirect_uri, state)
  3. User authenticates on the broker's site (popup/redirect).
  4. Broker returns request_token / auth_code to our redirect_uri.
  5. AuthProvider.exchange(...) → TokenSet(access[, refresh])
     → vault.store_tokens(...)  → status ACTIVE, token_date=today, expiry set

USE (per session, per account)
  6. Session build resolves DecryptedCreds from the vault (scoped to user_id),
     constructs the broker's BrokerClient with them.  Orders + positions route
     through THAT account's client.

STAY ALIVE (background health loop, per ACTIVE account)
  7. On a schedule / before each trading action:
     health = AuthProvider.validate(creds)
       ok            → continue
       EXPIRED + has_refresh_token   → AuthProvider.refresh() → vault.store_tokens()  (silent)
       EXPIRED + no refresh          → mark EXPIRED, emit "reconnect needed" (re-auth)
       REVOKED/ERROR                 → mark, emit re-auth
  8. Expiry detection = vault status derivation (token_date/expires_at vs now IST)
     + a cheap validate() ping. Kite → EXPIRED every morning → user re-auth prompt.

RE-AUTH (when a token dies and can't refresh)
  9. Account card shows EXPIRED with a one-click "Reconnect" → back to step 2.
     No session is placed against an EXPIRED account (preflight blocks it).
```

Key rule: **a session/order NEVER fires against a non-ACTIVE account.** The preflight (already exists for the operator path) is extended to check the bound account's health.

---

## 6. Order placement & position tracking per account

- Every session leg is bound to exactly one `broker_account_id` (or NULL = operator/global path, unchanged).
- `place_order` / `place_market_exit` / GTT all go through that leg's `BrokerClient`.
- **Add `get_positions()` / `get_holdings()`** to `BrokerClient` so the monitor reconciles DB ↔ that specific broker account (today's Zerodha reconcile becomes per-account).
- Slippage, journal, kill-switch, and trail are unchanged — they operate on `autotrade_positions` rows scoped by `session_id`, which already carries the account binding.

---

## 7. Data model (extend `broker_accounts`, additive)

Existing columns: `broker_account_id, user_id, broker, account_label, api_key, api_secret_enc, access_token_enc, token_date, token_expiry, status, created_at, updated_at, last_login_at`.

**Add:**
- `refresh_token_enc` (nullable) — encrypted refresh token where supported.
- `token_expires_at` (ISO IST, nullable) — absolute expiry for non-daily brokers.
- `last_health_at`, `last_health_status`, `last_error` — connection health.
- `redirect_state` (nullable) — CSRF `state` for the OAuth round-trip.

`vault.store_access_token` generalises to `vault.store_tokens(access, refresh?, expires_at?)`.

---

## 8. Security & isolation (carry forward today's fixes)

- **Per-user scoping everywhere:** every vault read/write is `WHERE user_id = ?` (already true). API endpoints derive `user_id` from the authenticated JWT (the isolation fix shipped 2026-07-01), never from a client param.
- **Secrets never leave the server:** masked previews only; `api_secret`/tokens are write-only from the client's perspective.
- **Vault key = crown jewel:** `FALCON_VAULT_KEY` must be backed up (losing it bricks every stored cred) and rotated via `FALCON_VAULT_KEY_PREV`. In the cloud end-state, swap `EnvKeyProvider` → `KmsKeyProvider` (no caller change).
- **Live-execution gate stays:** real orders still require `FALCON_AUTOTRADE_ENABLED`; paper-default holds.

---

## 9. Staged build plan (SDD — review at each gate)

**Stage 0 — Foundations (no user-facing change)**
- Add `BrokerAuthProvider` + `TokenSet`/`TokenHealth`/`BrokerCapabilities` interfaces.
- Extend `broker_accounts` schema (refresh token, expiry, health) — additive migration.
- Refactor Kite's existing auth behind `ZerodhaAuth(BrokerAuthProvider)` (behaviour identical). Turn on the vault (`FALCON_VAULT_KEY`) in a controlled way.

**Stage 1 — Generic account lifecycle + health**
- Registry: `broker_name → (BrokerClient, BrokerAuthProvider)`.
- Background health/refresh loop; preflight blocks non-ACTIVE accounts.
- Expose the REAL connect UI (`BrokerAccountsPanel`) to power users, per-user scoped (replaces the "launching soon" placeholder), still paper-only.

**Stage 2 — Second live broker (proves the abstraction)**
- Implement ONE non-Kite adapter end-to-end (both interfaces) — likely one with a refresh token, to exercise silent renewal. Validate the whole flow on that broker in paper.

**Stage 3 — Remaining brokers + live rollout**
- Fill in the other adapters. Per-broker paper→live certification. Flip `FALCON_AUTOTRADE_ENABLED` per cohort only after end-to-end verification.

Each stage is independently shippable and paper-safe. No strategy-engine code changes in any stage — that's the proof the architecture is correct.

---

## 10. Acceptance criteria

1. Adding a new broker = adding one adapter package (2 classes + capabilities) + registry line. **Zero** changes to kill-switch/trail/sizing/monitor.
2. A user connects broker X, its token refreshes silently (if supported) or prompts re-auth on expiry, and a session places + tracks orders through that specific account.
3. Two users, two brokers, multiple accounts each, concurrent sessions — fully isolated (creds, tokens, sessions, positions).
4. Losing/rotating the vault key is a controlled, documented operation.
```
