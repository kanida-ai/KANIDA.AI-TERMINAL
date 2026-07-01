# Kanida.AI AutoTrade — Scalable End-State Architecture (Design Doc)

Status: DESIGN ONLY (no code in this doc). Author: architecture pass, 2026-06-28 IST.
Scope: define the end-state so the backend supports **multiple users × multiple brokers × multiple broker-accounts-per-user**, concurrent broker sessions, real-time order placement, and future AutoTrade workflows — **without redesigning the core** when we later move to a static-IP / cloud-egress deployment.

Grounded in (verified, not invented):
- `_kanida_autotrade/backend/autotrade/` — `config.py` (`TradingSessionConfig`, `BrokerProfile`), `session.py` (`TradingSession`, `_build_brokers`), `broker/base.py` (`BrokerClient`/`OrderResult`/`Pick`), `broker/router.py` (`BrokerRouter`, `build_client`), `broker/zerodha.py` (wraps the Kite stack), `db_migrations.py` (isolated `autotrade_*` tables).
- `backend/services/kite_auth.py` — `_kite_proxies()` / `_new_kite()` egress hook (verified), `kite_tokens` table, `get_kite_client`.
- `docs/ops/ORACLE_STATIC_IP_SETUP.md` — the static-IP runbook.
- Memories: `autotrade-system-map`, `autotrade-portfolio-build`.

Anything not directly confirmed in code is marked **(to confirm)**.

---

## 0. Design principles (non-negotiable, carried from the live build)

1. **Additive only.** Never touch `falcon_position_state` for session state (a symbol-PK collision corrupted real positions once — `autotrade_portfolio_build` incident 2026-06-25). All session state stays in the isolated `autotrade_*` tables.
2. **Paper-default, env-gated live.** Live orders require `dry_run` off **and** `FALCON_AUTOTRADE_ENABLED=true` (`zerodha.ZerodhaBroker._live_allowed`).
3. **Fractions, not percents.** All pct knobs are fractions in `(0, 0.5]`, validated at the door (`config.validate`).
4. **Secrets never reach the browser; never serialized to the session DB.** `BrokerProfile` already strips secrets in `to_public_dict`; `config_json` in `autotrade_sessions` is sans-secrets. The vault (L3) must preserve this.
5. **The static IP is infrastructure, not application logic.** L1 (egress) is orthogonal to L2–L5 — turning it on is a config/deploy change.

---

## 1. Layered architecture (L1–L5): what exists vs. what to build

```
                         ┌──────────────────────────────────────────────┐
  L5  Order routing +    │  TradingSession · BrokerRouter · CapitalAlloc │  EXISTS (single-tenant)
      monitoring         │  KillSwitch · GTT · tick_driver · ws_driver   │  ADD: user_id/account scoping
                         └───────────────────────┬──────────────────────┘
                                                 │
  L4  Session manager    │  TradingSession lifecycle (create/start/tick/ │  EXISTS
      (binding)          │  kill/recovery) · entry/squareoff schedulers  │  ADD: bind (user_id, broker_account_id)
                         └───────────────────────┬──────────────────────┘
                                                 │
  L3  Credential vault   │  api_key / ENCRYPTED secret / access_token /  │  PARTIAL (env+kite_tokens, single)
                         │  token_expiry — per (user, broker, account)   │  ADD: broker_accounts table + Fernet
                         └───────────────────────┬──────────────────────┘
                                                 │
  L2  Broker adapters    │  BrokerClient ABC → ZerodhaBroker (live),     │  EXISTS (Zerodha live; rest stubs)
                         │  Upstox/Angel/Dhan/Fyers (stubs)              │  ADD: implement adapters
                         └───────────────────────┬──────────────────────┘
                                                 │
  L1  Egress / IP        │  ONE static egress IP (Oracle proxy or cloud  │  HOOK WIRED (BROKER_PROXY_URL, default-off)
                         │  VM) via BROKER_PROXY_URL / _new_kite()       │  ADD: stand up proxy / VM
                         └──────────────────────────────────────────────┘
```

| Layer | What exists today (verified) | What to build (Phase 2) |
|---|---|---|
| **L1 Egress / IP** | `_kite_proxies()` reads `BROKER_PROXY_URL`; `_new_kite(api_key)` passes `proxies=` only when set (byte-identical when unset). All 3 KiteConnect construction sites use it. `GET /api/falcon/egress-ip` returns `{ip, proxy_ip}`. | Stand up the Oracle Always-Free proxy (runbook exists) **or** move backend to a Mumbai VM. No code change beyond setting one env line. |
| **L2 Broker adapters** | `BrokerClient` ABC with async order lifecycle + GTT-OCO defaults. `ZerodhaBroker` wraps the existing Kite stack (kite_auth, kite_ticker, order_executor, margin_calc). `build_client()` dispatch. Upstox/Angel/Dhan/Fyers are `NotImplementedError` stubs. | Implement real adapters by wrapping each SDK the way `zerodha.py` wraps Kite. Per-broker margin/GTT/order semantics. |
| **L3 Credential vault** | `BrokerProfile` holds `api_key/api_secret/access_token` **in memory only** (never persisted). Real creds come from **env** `KITE_API_KEY`/`KITE_API_SECRET` + `kite_tokens` table (single-tenant, keyed by `token_date` only). access_token stored **plaintext**. | New `broker_accounts` table keyed by `(user_id, broker, account_label)` with **encrypted** secret + per-account token/expiry. Session resolves creds from it. |
| **L4 Session manager** | `TradingSession.create/start/tick/kill/status`; `recovery.resume_active_sessions()` on boot; entry/squareoff/tick/ws drivers per session. State in `autotrade_sessions`/`autotrade_positions` (keyed by `session_id`). | Add `user_id` + `broker_account_id` columns; bind each session to a specific account; per-user ownership + quotas. |
| **L5 Routing + monitoring** | `BrokerRouter.route_picks` → per-profile picks; `CapitalAllocator` (batched prefetch); `_fire_entries` parallel `asyncio.gather` under a `Semaphore`; `KillSwitchExecutor`; `gtt_manager`; event-driven `ws_driver` + `tick_driver`. | Reuse as-is; thread `user_id`/`broker_account_id` through the same isolation keys. Shared-vs-per-session WS for scale. |

**Key structural truth:** the layering already exists in the build. L2 is fully abstracted (`BrokerClient`), L4/L5 are session-isolated by `session_id`, and L1 is a clean env hook. The **only** parts not yet multi-tenant are **L3 (creds are single-account env/kite_tokens)** and the **`user_id` scoping on L4/L5 tables**. That is the Phase-2 delta — not a rewrite.

---

## 2. User-level broker-account separation — `broker_accounts` table

### 2.1 Problem with today's model
Today there is exactly one set of creds: `KITE_API_KEY`/`KITE_API_SECRET` in env + one `access_token` row per day in `kite_tokens` (keyed by `token_date`, **no user_id**). `ZerodhaBroker.kite` calls the global `get_kite_client()`. So:
- One broker, one account, one user. No isolation. (Verified in `kite_auth.py`.)
- `BrokerProfile.profile_id` is a *routing* label inside a single session, **not** a user/account identity.

### 2.2 Proposed schema (additive, new table)

```sql
CREATE TABLE IF NOT EXISTS broker_accounts (
    broker_account_id TEXT PRIMARY KEY,            -- uuid4 hex (stable handle)
    user_id           TEXT NOT NULL,               -- portal user (FK to users)
    broker            TEXT NOT NULL,               -- zerodha | upstox | angel | dhan | fyers
    account_label     TEXT NOT NULL,               -- user-chosen, e.g. "Main Kite", "F&O Kite", "Upstox"
    api_key           TEXT NOT NULL,               -- broker app api_key (not secret on its own)
    api_secret_enc    BLOB NOT NULL,               -- Fernet-ENCRYPTED api_secret (never plaintext)
    access_token_enc  BLOB,                         -- Fernet-ENCRYPTED daily access_token (NULL until login)
    token_date        TEXT,                         -- IST date the token was minted (Kite expires daily)
    token_expiry      TEXT,                         -- ISO IST expiry (broker-specific; Kite ≈ next 06:00 IST)
    status            TEXT NOT NULL DEFAULT 'PENDING',
        -- PENDING (no token) | ACTIVE (valid token) | EXPIRED | REVOKED | ERROR
    created_at        TEXT NOT NULL,               -- ISO IST
    updated_at        TEXT,
    last_login_at     TEXT,
    UNIQUE (user_id, broker, account_label)         -- a user CAN hold many accounts
);
CREATE INDEX IF NOT EXISTS idx_broker_accounts_user ON broker_accounts(user_id, status);
```

Notes:
- **`api_secret_enc` / `access_token_enc` are encrypted at rest** (L3, §4). The encryption key lives outside the DB.
- **A user holds MULTIPLE accounts** via distinct `account_label`s under the same `user_id`: e.g. `(u123, zerodha, "Main Kite")`, `(u123, zerodha, "F&O Kite")` (a 2nd Kite app), `(u123, upstox, "Upstox")`. The `UNIQUE(user_id, broker, account_label)` constraint enforces no dup labels but unlimited accounts.
- **Per-user isolation:** every read is `WHERE user_id = ?`. No cross-user query path. The portal session (already auth'd) supplies `user_id`; the operator token never substitutes for it on user-scoped routes.
- This **replaces the role** of the single-tenant `kite_tokens` table for the new system. `kite_tokens` stays for the **legacy `/falcon` path** untouched (to confirm: legacy keeps using env + `kite_tokens`; new system reads `broker_accounts` only).

### 2.3 How `session.py` resolves creds per session (end state)

Today (verified) `_build_brokers` builds a default `zerodha_default` `BrokerProfile` with empty secrets and `ZerodhaBroker` lazily calls the **global** `get_kite_client()`. End state:

1. A session is created with a `broker_account_id` (chosen in the UI from the user's `broker_accounts`).
2. `_build_brokers` resolves that account: `acct = broker_accounts.get(broker_account_id)` scoped to the session's `user_id` (defense-in-depth: reject if `acct.user_id != session.user_id`).
3. Decrypt `api_secret_enc` / `access_token_enc` **in memory only** into the `BrokerProfile` (`api_key`, `api_secret`, `access_token`) — exactly the in-memory-only contract `BrokerProfile` already has.
4. `build_client(profile, dry_run)` constructs the adapter; the adapter sets the access token on its **own** KiteConnect instance (`kite.set_access_token(acct_token)`) instead of relying on the process-global one.
   - **(to confirm / required change):** `ZerodhaBroker` must accept a per-account token via the profile rather than always calling the global `get_kite_client()`. Today it calls the global helper; for multi-account it needs a "build a KiteConnect for *this* api_key + token + proxies" path. This is the single most important L2/L3 wiring change. `_new_kite(api_key)` already builds a proxy-aware client per api_key — extend it (or add a sibling) to also `set_access_token`.
5. One session ⇒ one (or more, via multiple `BrokerProfile`s) account-bound clients, each isolated.

---

## 3. Broker-session handling

### 3.1 Binding model
Each `TradingSession` binds to **one `user_id`** and, per `BrokerProfile`, to **one `broker_account_id`**:

```
TradingSession(session_id)
  ├─ user_id                       (owner; from portal auth)
  ├─ broker_profiles[]             (routing + capital split — EXISTS)
  │     └─ each profile → broker_account_id   (NEW: which real account this leg trades)
  └─ brokers{profile_id → BrokerClient}        (account-bound adapter instances — EXISTS shape)
```

A user can run **a Kite session and an Upstox session and a 2nd-Kite session concurrently** — they are independent `TradingSession` rows, each with its own `broker_account_id`, its own positions (`autotrade_positions` keyed by `session_id`), its own drivers.

### 3.2 Concurrent sessions across users/brokers
- Already isolated by `session_id` (positions, snapshots, kill log, schedulers, fire-guard lock). Adding `user_id`/`broker_account_id` columns doesn't change the isolation key — it adds ownership + cred binding.
- `recovery.resume_active_sessions()` re-arms all RUNNING/SCHEDULED sessions on boot regardless of owner — extend to skip sessions whose `broker_account_id` token is EXPIRED (don't fire with a dead token; surface for re-login).

### 3.3 Per-account daily token refresh (Kite tokens expire daily)
Kite access tokens die at ~06:00 IST next day (see `zerodha_auth_standalone_task` memory; `kite_tokens` keyed by `token_date`). End-state per-account flow:

```
Per (user_id, broker_account_id):
  1. User clicks "Connect <account_label>" → backend builds the broker login URL
     for THAT account's api_key (Kite: kite.login_url()).
  2. User logs in at the broker, is redirected back with a request_token.
  3. Backend exchange_and_save(request_token) for THAT api_key/secret →
     access_token → ENCRYPT → store in broker_accounts(access_token_enc, token_date,
     token_expiry, status=ACTIVE, last_login_at).
  4. Daily refresh: each account needs its own daily login (broker OAuth can't be
     fully headless for retail Kite). Options:
       (a) User-driven: portal nudges "re-connect <account_label>" each morning.
       (b) Operator's OWN account: keep the existing KanidaZerodhaAuth scheduled
           task (auto re-auth, 30-min cadence, weekday-gated) — applies only to the
           operator's master account, NOT user accounts.
  5. status=EXPIRED when token_date != today (IST); sessions on that account refuse
     to fire live and prompt re-login.
```

**(to confirm):** whether any broker offers a refresh-token / long-lived token (Upstox has a longer-lived flow than Kite). Design the table to hold `token_expiry` so each adapter encodes its own refresh cadence — don't hardcode Kite's daily expiry into the schema.

### 3.4 Adding Upstox/Angel/Dhan adapters
The abstraction already exists: `BrokerClient` ABC + `build_client()` dispatch + `NotImplementedError` stubs. To add a broker:
1. Implement `broker/<name>.py` wrapping that SDK (market data, order lifecycle, margin, GTT/OCO-equivalent).
2. `build_client()` already routes by `broker_name`.
3. Per-broker quirks live entirely inside the adapter (e.g. Upstox instrument keys, Angel SmartAPI session model, Dhan order schema). **L4/L5 never learn a broker's name** beyond the profile string — that's the whole point of L2.

---

## 4. Secure credential storage (L3) — encryption, gating, threat model

### 4.1 Today (verified)
- `KITE_API_KEY` / `KITE_API_SECRET`: **plaintext in env**.
- `access_token`: **plaintext** in `kite_tokens` (single row/day, no user_id).
- `BrokerProfile` secrets: in-memory only, never serialized (good — keep).
- Browser never sees secrets: frontend calls via `/api/falcon-proxy/[...path]` which injects the operator token server-side. `to_public_dict` exposes only `creds_configured: bool`.

### 4.2 End-state encryption at rest
- **Symmetric encryption (Fernet, AES-128-CBC + HMAC)** for `api_secret_enc` and `access_token_enc`. One library, no KMS dependency to start. **(to confirm: `cryptography` package availability — likely already present transitively; verify before building.)**
- **Where the key lives:** a single `KANIDA_VAULT_KEY` (32-byte urlsafe-base64 Fernet key) in **env / `config/.env`**, NOT in the DB and NOT in git. Threat model accepts: anyone with both the DB file **and** the env key can decrypt — same trust boundary as the operator already has on the laptop server. The DB-at-rest (backups, accidental commit) is protected; the running process is not (it must decrypt to trade).
- **Cloud end-state upgrade path:** when the backend moves to the Mumbai VM, swap the env key for **a managed KMS** (Oracle Vault / a cloud KMS) — decrypt-on-demand, key never on disk. Because L3 is an interface (`encrypt()/decrypt()`), this is a provider swap, **not** a schema or session change. Keep the encryption behind a tiny `vault` module so the backend is KMS-ready without redesign.

### 4.3 Gating & exposure rules
- **User secrets are user-scoped, not operator-scoped.** A user's `broker_accounts` rows are readable only with that user's auth context. The `FALCON_OPERATOR_TOKEN` gates the *system* (`/api/autotrade/*`) but must **not** be a backdoor to read another user's plaintext secret.
- **Never return decrypted secrets over any API.** Endpoints return `creds_configured`, `status`, `account_label`, `token_expiry` — never `api_secret`/`access_token`.
- **Decrypt only at session build / order time**, in memory, into the `BrokerProfile`; never log it (`BrokerProfile` already uses `repr=False` on the secret fields — keep).

### 4.4 Secret rotation
- **api_secret rotation:** user re-enters in the portal → re-encrypt → overwrite `api_secret_enc`, bump `updated_at`. Running sessions keep their in-memory copy until restart.
- **access_token rotation:** daily by design (§3.3).
- **Vault key rotation:** decrypt-all-with-old → re-encrypt-all-with-new in a one-shot migration; keep `KANIDA_VAULT_KEY` + `KANIDA_VAULT_KEY_PREV` for a grace window. (Standard Fernet `MultiFernet` pattern.)

### 4.5 Threat model summary
| Threat | Mitigation |
|---|---|
| DB file leaked / committed to git | secrets are Fernet-encrypted; key not in DB/git (and `.gitignore` guard exists per `codex_git_bloat_incident`). |
| Browser / network sniff | secrets never sent to browser; proxy injects operator token server-side; HTTPS to broker. |
| One user reads another's creds | per-user `WHERE user_id=?`; no API returns plaintext; operator token ≠ user identity. |
| Stolen env key + DB | full compromise (accepted on single laptop; mitigated by KMS in cloud end state). |
| Proxy credential leak (BROKER_PROXY_URL) | only grants egress from the static IP; still needs api_key+token to trade (runbook §Notes). Rotate if leaked. |

---

## 5. Order routing flow (end-to-end)

```
 USER (portal, authenticated)                    [L4/L5 — already isolated by session_id]
   │  POST /api/autotrade/session {config, broker_account_id}
   ▼
 RESOLVE USER  ──────────► user_id from portal auth (not operator token)
   │
   ▼
 RESOLVE BROKER_ACCOUNT  ─► broker_accounts WHERE broker_account_id=? AND user_id=?   [L3]
   │                         status must be ACTIVE (token fresh) for live
   ▼
 DECRYPT CREDS (in memory) ► api_key, api_secret, access_token → BrokerProfile        [L3]
   │                         (never persisted, never logged, never to browser)
   ▼
 BUILD ADAPTER  ──────────► build_client(profile, dry_run) → ZerodhaBroker/Upstox/... [L2]
   │                         adapter.set_access_token(this account's token)
   ▼
 ROUTE + SIZE  ───────────► BrokerRouter.route_picks → CapitalAllocator.prefetch       [L5]
   │                         (ONE batched LTP + ONE batched MTF-margin)
   ▼
 PLACE (parallel)  ───────► _fire_entries: asyncio.gather over _place_one              [L5]
   │                         under Semaphore(FALCON_AUTOTRADE_ENTRY_CONCURRENCY=8)
   ▼
 EGRESS  ─────────────────► KiteConnect REST → proxies={BROKER_PROXY_URL} → ONE        [L1]
   │                         STATIC IP → broker. (WS market data stays DIRECT.)
   ▼
 FILLS  ──────────────────► OrderResult{status, broker_order_id, filled_qty, avg_price}
   │                         freeze invested_basis = Σ(qty*avg_price)
   ▼
 ISOLATED POSITIONS  ─────► autotrade_positions (session_id, symbol[, broker_profile]) [L5]
   │                         + GTT-OCO per position (broker-held floor)
   ▼
 SUB-SECOND MONITOR  ─────► ws_driver (event-driven tick listener, 0.1s backstop)      [L5]
   │                         + tick_driver (5s) → kill_switch on INVESTED basis G
   ▼
 EXIT  ───────────────────► kill / trail / square-off → cancel GTTs (parallel) THEN
                            parallel market exits → session CLOSED
```

**Reuse the existing isolation:** add `user_id` and `broker_account_id` columns to `autotrade_sessions` (and carry `broker_account_id` on `autotrade_positions` via the existing `broker_profile` slot, or a new column). The isolation key stays `session_id`; ownership/cred binding are additive columns. No change to the positions store's collision-safe design.

---

## 6. Concurrent execution & capacity

### 6.1 What the current design supports
- **Per-session drivers:** each RUNNING session owns a `tick_driver` (5s) + a `ws_driver` (event-driven via the shared KiteTicker tick-listener hook, 0.1s backstop) + (when armed) entry/squareoff schedulers + a `fire_guard` lock. (Verified in system map / build log.)
- **Entry concurrency:** legs fan out via `asyncio.gather` under a `Semaphore` (default 8, cap 1..10 for Kite's ~10 orders/s limit).
- **Shared WS:** the `ws_driver` registers a coalescing listener on the **single** legacy `kite_ticker` WS and caches the open-symbol set, re-subscribing on change. So many sessions share **one** market-data socket (per broker), not N sockets.

### 6.2 The laptop-as-server ceiling (honest)
- **Single broker WS token / Kite connection** is shared today — fine for the operator's one account. **(to confirm)** whether KiteTicker is per-api_key; if so, each distinct **account** needs its own WS subscription, which multiplies sockets with users — the real scaling pressure point.
- **Order rate limit** is per broker app (Kite ~10/s) — so multi-user through *distinct* broker apps is naturally parallel, but a single shared app would serialize. Model A (each user = own broker app) sidesteps this: rate limits are per-user-app, not global.
- **CPU/threads:** N sessions × (2 driver threads + schedulers) on a laptop caps in the low hundreds of sessions before thread/GIL pressure. The event-driven design keeps idle cost low (wakes on tick, not busy-poll). **(to confirm via load test)**.
- **Uptime:** laptop sleep kills the backend (`laptop_sleep_watchdog` memory — pending fix). This is the hard operational ceiling, resolved only by the cloud move.

### 6.3 What changes for many users
- **WS scaling (L5):** move from "one shared legacy ticker" to **a per-broker-account WS pool** (one subscription per distinct connected account; coalesce session listeners onto it). This is a monitor-internal change; sessions/kill-switch logic unchanged.
- **Process model:** for real multi-tenant scale, run the backend on the **cloud VM** (also fixes uptime) and consider a worker pool / async event loop sizing. **(to confirm)** whether to shard sessions across processes — likely unnecessary until well past beta.
- **DB:** SQLite (`falcon_conn`) is fine for current write rates but is the next bottleneck at many concurrent writers — Postgres migration is the eventual end state (flagged in roadmap; not Phase-2-blocking).

---

## 7. IP handling + Oracle/cloud migration WITHOUT redesign

### 7.1 The orthogonality claim (the core point)
**L1 (egress IP) is infrastructure, decoupled from L2–L5 by exactly one env var.** `_new_kite(api_key)` passes `proxies=` to KiteConnect **only when `BROKER_PROXY_URL` is set**; unset ⇒ byte-identical to today (verified). KiteConnect 5.2.0 applies `self.proxies` on every REST call → all order/GTT/quote APIs egress through the proxy automatically. The session/router/kill-switch code **does not know or care** whether egress is direct or proxied.

### 7.2 Model A (multi-user, one IP)
ONE static IP serves **all users × all brokers × all accounts**. Each user registers **that same IP** on **their own** broker app's Allowed-IPs + SEBI profile registration (runbook §8–9). Shared egress = one IP to allowlist everywhere. No per-user IP. (system map "Model A".)

### 7.3 Migration steps (no core redesign)
**Phase A — proxy in front of the laptop (zero backend code change):**
1. Stand up the Oracle Always-Free Mumbai VM + tinyproxy (runbook STEP 1–6).
2. Set `BROKER_PROXY_URL=http://user:pass@<ORACLE_IP>:8888` in `config/.env`, restart :8001.
3. `GET /api/falcon/egress-ip` → confirm `proxy_ip == <ORACLE_IP>`.
4. Register `<ORACLE_IP>` on each user's broker app + SEBI profile; `/api/falcon/preflight` → GREEN.
   → Backend code **unchanged**; this is config + ops only.

**Phase B — move the whole backend to the cloud VM (deploy change, not redesign):**
1. Deploy the same `backend/` tree to the Mumbai VM.
2. **Unset** `BROKER_PROXY_URL` (the VM *is* the static IP now — no proxy hop, lower latency).
3. Re-point Cloudflare tunnel / Vercel proxy at the VM backend (`power_user_portal_ops` request path).
4. Move `KANIDA_VAULT_KEY` to the VM's KMS (L3 §4.2) — interface swap, no schema change.
   → L2–L5 code **unchanged**; only deploy target + 2 env vars move. WS stays direct (now from the VM's static IP).

**Why no redesign:** every L1 change is `BROKER_PROXY_URL` set/unset + where the process runs. The cred vault, broker adapters, session manager, and routing are all egress-agnostic by construction.

---

## 8. Phase plan / build sequence

| # | Item | Layer | Status | Notes |
|---|---|---|---|---|
| 0 | Single-account live AutoTrade (Sessions) | L4/L5 | **DONE** | kill switch, GTT-OCO, instant/scheduled entry, MTF sizing, trail engine, speed pass — all live (paper-default). |
| 1 | Position isolation (`autotrade_positions`) | L5 | **DONE** | the falcon_position_state collision fix; proven live. |
| 2 | Broker abstraction (`BrokerClient`/router) | L2 | **DONE** | Zerodha live; Upstox/Angel/Dhan/Fyers stubs. |
| 3 | Egress hook (`BROKER_PROXY_URL`) | L1 | **DONE** | wired, default-off, byte-identical when unset; runbook exists. |
| 4 | **Credential vault** (`broker_accounts` + Fernet) | L3 | **TODO** | the central Phase-2 unlock; replaces single-tenant env/kite_tokens for the new system. |
| 5 | **User scoping on sessions** (`user_id`, `broker_account_id` cols) | L4/L5 | **TODO** | additive columns; isolation key stays `session_id`. |
| 6 | **Per-account daily token refresh** (per-account OAuth) | L3/L4 | **TODO** | per-account login URL + exchange + expiry status; recovery skips expired-token sessions. |
| 7 | **Additional adapters** (Upstox first) | L2 | **TODO** | wrap each SDK like zerodha.py; per-broker margin/GTT semantics. |
| 8 | **Adapter cred wiring** (per-account token, not global) | L2/L3 | **TODO** | `ZerodhaBroker` must use the session's account token, not the process-global `get_kite_client()`. |
| 9 | **WS scaling** (per-account WS pool) | L5 | **TODO** | move off the single shared legacy ticker for many users. |
| 10 | **Cloud VM move + KMS** | L1/L3 | **TODO** | deploy + 2 env vars; fixes uptime + latency; KMS swap behind the vault interface. |
| 11 | Risk + reporting (per-user audit, realized P&L ledger, quotas) | L5 | **TODO** | per-user caps, kill-switch analytics. |
| 12 | F&O / more order types | L2/L4 | partial | option/future selection stubbed in zerodha; bracket/cover variants. |

### Model A regulatory posture (FLAG — operator decision, do not decide here)
- **Model A (each user trades their OWN broker app)** is the assumed posture: every user holds their own broker account + api_key, registers the shared static IP themselves. The platform routes their orders through their own creds. This is the lowest-friction, lowest-regulatory-risk model.
- **Multi-user-through-ONE-app** (the platform's single broker app placing orders for many end-users) is a fundamentally different regulatory animal — it implies the platform is an intermediary and would need **broker/SEBI approval** (likely a different license class). **This doc does not choose** — it is built for Model A, and flags that any move toward shared-app multiplexing is a business/legal decision, not a code change.

---

## 9. Open design questions for the operator

1. **Vault key custody:** start with a single `KANIDA_VAULT_KEY` in `config/.env` (laptop trust boundary), or jump straight to a cloud KMS? (Affects whether we ship the laptop interim at all.)
2. **Token refresh UX:** for *user* accounts, is daily re-login acceptable (portal nudge each morning), or do we restrict launch to brokers with longer-lived tokens (Upstox) to avoid daily friction? Kite is daily-expiry by design.
3. **Operator master account vs. user accounts:** keep the existing `KanidaZerodhaAuth` scheduled auto-auth for the operator's own account only, and require manual per-user login for everyone else? (Confirms §3.3 split.)
4. **Legacy `/falcon` path:** does the legacy single-tenant path stay on env + `kite_tokens` permanently, or eventually migrate onto `broker_accounts` too? (Default assumption: leave legacy untouched.)
5. **WS per-account:** confirm whether KiteTicker is per-api_key (drives the WS-pool design in §6.3) — needs a quick check against kiteconnect docs / a live test.
6. **Cloud move timing:** Phase A (proxy only) first, or go straight to Phase B (full VM) to also fix the laptop-sleep uptime ceiling? Latency and uptime both favor B; B is more ops work up front.
7. **DB:** when do we commit to Postgres? (Not Phase-2-blocking, but `broker_accounts` is the natural first table to design Postgres-compatible from day one.)
8. **Quotas/limits:** per-user session count, capital cap, daily order cap — what are the launch defaults?

---

### Appendix — files this design touches (for the eventual build)
- New: `backend/autotrade/vault.py` (encrypt/decrypt interface), `broker_accounts` migration in `db_migrations.py`.
- Modify (additive): `config.py` (`BrokerProfile` gains `broker_account_id`), `session.py` (`_build_brokers` resolves account via vault; session gains `user_id`/`broker_account_id`), `broker/zerodha.py` (per-account token instead of global client), `broker/upstox.py` (implement), `recovery.py` (skip expired-token sessions), `api/autotrade_routes.py` (user-scoped account CRUD + login flow), `db_migrations.py` (`user_id`/`broker_account_id` columns on `autotrade_sessions`).
- Unchanged by L1 moves: everything in L2–L5 except the cred-resolution seam in §2.3/§8.8.
```
