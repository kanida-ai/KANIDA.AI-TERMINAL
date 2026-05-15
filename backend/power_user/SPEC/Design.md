# KANIDA.AI Power User Portal — Design

**Version:** 1.0 (draft)
**Date:** 2026-05-14 IST
**Status:** Awaiting operator go/no-go (SDD gate 2)

---

## 1. System diagram

```
                    ┌────────────────────────────────────────────────────────┐
                    │                  app.kanida.ai (Vercel)                │
                    │  ┌─────────────────────┐    ┌──────────────────────┐  │
                    │  │ /(public)/...       │    │ /(power)/... [JWT]   │  │
                    │  │  - landing          │    │  - today (top 100)   │  │
                    │  │  - replay/[date]    │    │  - replay custom     │  │
                    │  │  - today (top 5)    │    │  - live              │  │
                    │  │  - login            │    │  - watchlist (1B)    │  │
                    │  │  - redeem           │    └──────────────────────┘  │
                    │  └─────────────────────┘                              │
                    └────────────────┬───────────────────────────────────────┘
                                     │ NextAuth Google OAuth + fetch
                                     ▼
                    ┌────────────────────────────────────────────────────────┐
                    │             api.kanida.ai (Railway, FastAPI :8001)    │
                    │  ┌──────────────────────────────────────────────────┐ │
                    │  │  /api/power/picks/today          (public + auth) │ │
                    │  │  /api/power/picks/replay?date=   (public + auth) │ │
                    │  │  /api/power/picks/live           (auth)          │ │
                    │  │  /api/power/auth/google          (OAuth callback)│ │
                    │  │  /api/power/invites/redeem       (auth)          │ │
                    │  │  /api/power/admin/invites/issue  (operator)      │ │
                    │  └──────────────────────────────────────────────────┘ │
                    │           │                          │                │
                    │  services/explainer.py        services/live_tier.py   │
                    │  services/replay_cache.py     services/scheduler.py   │
                    │  services/auth.py             services/invites.py     │
                    └────────────┬─────────────────────────┬─────────────────┘
                                 │                         │
                                 ▼                         ▼
                    ┌──────────────────────────┐  ┌─────────────────────────┐
                    │ kanida_universe.db (RO)  │  │  power_user tables (RW) │
                    │ - falcon_signals_live    │  │ - power_user_users      │
                    │ - falcon_features        │  │ - power_user_invite_*   │
                    │ - falcon_pattern_*       │  │ - power_user_watchlists │
                    │ - falcon_promoted_*      │  │ - power_user_waitlist   │
                    │ - falcon_sectors         │  │ - falcon_live_decisions │
                    │ - ohlc_daily             │  │ - falcon_replay_cache   │
                    └──────────────────────────┘  └─────────────────────────┘
                                 │
                                 ▼
                    kite_tokens (kanida_quant.db, scheduler reads admin token only)
```

## 2. File layout

```
backend/
├── power_user/                              # NEW — entire portal
│   ├── __init__.py
│   ├── SPEC/
│   │   ├── Requirements.md                  # ✓ done
│   │   ├── Design.md                        # this file
│   │   └── Tasks.md                         # next
│   ├── db_schema.sql                        # 6 new tables
│   ├── db_init.py                           # idempotent migrations
│   ├── services/
│   │   ├── explainer.py                     # ← port of explain_picks_v3.py + polish
│   │   ├── feature_cols.py                  # ← FEATURE_COLS, in_drawdown_bounce (from sim_sweep)
│   │   ├── live_tier.py                     # ← port of live_tier_tool_v2.py
│   │   ├── replay_cache.py                  # featured pre-compute + on-the-fly + cache
│   │   ├── scheduler.py                     # live_tier cron (09:30:30 / 09:45 / 10:00 IST)
│   │   ├── auth.py                          # Google ID-token verify + JWT issue
│   │   ├── invites.py                       # code generate / redeem / mark-used
│   │   └── observability.py                 # per-request log
│   ├── routers/
│   │   ├── picks_router.py                  # /picks/today /picks/replay /picks/live
│   │   ├── auth_router.py                   # /auth/google /auth/me /auth/logout
│   │   ├── invites_router.py                # /invites/redeem
│   │   ├── admin_router.py                  # /admin/invites/issue /admin/metrics
│   │   └── public_router.py                 # /public/* unauthenticated routes
│   └── tests/
│       ├── test_explainer.py                # polish + sector dict + capitalization
│       ├── test_replay_cache.py             # featured = correct hit rates
│       ├── test_auth.py                     # JWT issue/verify, invite redeem
│       └── test_perf.py                     # budget compliance smoke

frontend/
├── app/
│   ├── (public)/                            # NEW — unauthenticated routes
│   │   ├── page.tsx                         # landing (3 featured + Random + Today top 5)
│   │   ├── replay/[date]/page.tsx
│   │   ├── today/page.tsx                   # top-5 preview, "Sign in for top-100" CTA
│   │   ├── login/page.tsx                   # Google OAuth + invite redeem
│   │   └── waitlist/page.tsx                # capture email if no invite
│   ├── (power)/                             # NEW — JWT-gated routes
│   │   ├── today/page.tsx                   # top-100 + filters
│   │   ├── replay/[date]/page.tsx           # custom date deeper view
│   │   ├── live/page.tsx                    # 09:30/09:45/10:00 decisions
│   │   └── watchlist/page.tsx               # Phase 1B
│   ├── (admin)/                             # NEW — operator-only invite/metrics
│   │   ├── invites/page.tsx
│   │   └── metrics/page.tsx
│   └── falcon/                              # UNCHANGED — operator auto-trade
├── components/
│   ├── PickCard.tsx                         # collapsible card per pick
│   ├── StoryBlock.tsx                       # "THE STORY" section
│   ├── PatternList.tsx                      # "WHAT THE ENGINE NOTICED"
│   ├── ExpectedOutcomeTable.tsx
│   ├── ActualOutcomeRow.tsx                 # historical only
│   ├── RiskControl.tsx
│   ├── FeaturedReplayCard.tsx               # landing page tile
│   ├── RandomReplayButton.tsx               # 🎲 button
│   └── PreflightBanner.tsx                  # already exists (operator side); not used here
├── lib/
│   ├── power-api.ts                         # NEW — typed fetch wrappers
│   └── auth.ts                              # NextAuth config
```

## 3. New DB schemas (additive only — no changes to engine tables)

```sql
-- power_user_users: anyone who's redeemed an invite + signed in via Google
CREATE TABLE IF NOT EXISTS power_user_users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    google_sub      TEXT NOT NULL UNIQUE,          -- Google account ID
    display_name    TEXT,
    picture_url     TEXT,
    invite_code     TEXT,                          -- code they used
    role            TEXT NOT NULL DEFAULT 'user',  -- 'user' | 'partner' | 'admin'
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,                 -- IST ISO
    last_seen_at    TEXT
);

-- power_user_invite_codes: admin-issued, one-time-use
CREATE TABLE IF NOT EXISTS power_user_invite_codes (
    code            TEXT PRIMARY KEY,              -- e.g. "kn-2026-a7f3-b2"
    issued_by       TEXT NOT NULL,                 -- admin email or 'system'
    issued_at       TEXT NOT NULL,
    expires_at      TEXT,                          -- NULL = no expiry
    used_by_user_id INTEGER,                       -- FK → power_user_users.id
    used_at         TEXT,
    note            TEXT                           -- "Influencer: @username"
);

-- power_user_waitlist: caught at /waitlist when email has no invite
CREATE TABLE IF NOT EXISTS power_user_waitlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    joined_at       TEXT NOT NULL,
    source          TEXT,                          -- 'login_no_invite' | 'landing_cta'
    invite_issued   INTEGER DEFAULT 0
);

-- power_user_watchlists: Phase 1B feature, schema created now for forward-compat
CREATE TABLE IF NOT EXISTS power_user_watchlists (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    added_at        TEXT NOT NULL,
    note            TEXT,
    UNIQUE(user_id, symbol),
    FOREIGN KEY (user_id) REFERENCES power_user_users(id)
);

-- falcon_live_decisions: scheduler writes 09:30:30 / 09:45 / 10:00 IST
CREATE TABLE IF NOT EXISTS falcon_live_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date     TEXT NOT NULL,
    entry_date      TEXT NOT NULL,
    cycle           TEXT NOT NULL,                 -- '0930' | '0945' | '1000'
    rank            INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    sector          TEXT,
    score           REAL,
    tier            TEXT,                          -- ELITE/HIGH/MID/LOWER/TAIL/DEEP-TAIL
    action          TEXT,                          -- ENTER/WAIT/SKIP
    reason          TEXT,
    ret_15          REAL,                          -- first 15-min ret %
    vol_pct         REAL,                          -- 15-min vol / yesterday total %
    close_loc       REAL,                          -- 15-min close location in range
    computed_at     TEXT NOT NULL,
    UNIQUE(entry_date, cycle, rank)
);
CREATE INDEX IF NOT EXISTS ix_live_dec_date_cycle ON falcon_live_decisions(entry_date, cycle);

-- falcon_replay_cache: featured-date payloads + arbitrary-date 24h cache
CREATE TABLE IF NOT EXISTS falcon_replay_cache (
    replay_date     TEXT PRIMARY KEY,
    payload_json    TEXT NOT NULL,                 -- full picks + actual outcomes
    is_featured     INTEGER NOT NULL DEFAULT 0,
    computed_at     TEXT NOT NULL,
    expires_at      TEXT                           -- NULL for featured = never expires
);

-- Request observability (already useful in Phase 1)
CREATE TABLE IF NOT EXISTS power_user_request_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER,                       -- NULL for anonymous
    route           TEXT NOT NULL,
    status_code     INTEGER NOT NULL,
    latency_ms      INTEGER,
    created_at      TEXT NOT NULL,
    ip_hash         TEXT                           -- for rate-limit, not stored raw
);
CREATE INDEX IF NOT EXISTS ix_pulog_created ON power_user_request_log(created_at);
```

## 4. API contracts

### 4.1 Public (no auth)

#### `GET /api/power/picks/today/preview`
Top 5 picks for the latest signal_date.

```json
{
  "signal_date": "2026-05-14",
  "entry_date": "2026-05-15",
  "total_available": 100,
  "picks_shown": 5,
  "picks": [ {pick}, ... ]
}
```

#### `GET /api/power/replay/featured`
List the 3 featured replay dates.

```json
{
  "featured": [
    {"date":"2026-04-15","title":"The Blowout","wr_d5":92,"wr_d15":92,"avg_d15":12.0,"big_wins_d15":74},
    {"date":"2024-11-04","title":"Year-Ago Proof","wr_d5":96,"wr_d15":82,"avg_d15":6.2,"big_wins_d15":62},
    {"date":"2025-12-15","title":"Steady Day","wr_d5":78,"wr_d15":82,"avg_d15":5.7,"big_wins_d15":44}
  ]
}
```

#### `GET /api/power/picks/replay/{date}` (public if featured OR random)
Replay for a featured date OR a one-shot random date. Backend enforces:
- If `date` is one of 3 featured → serve from `falcon_replay_cache` (sub-200ms)
- If `date` is random AND no auth → check anonymous rate limit (max 3 random calls per IP-hash per hour)
- Else → 401

```json
{
  "signal_date": "2026-04-15",
  "entry_date": "2026-04-16",
  "is_featured": true,
  "title": "The Blowout",
  "aggregate": {
    "n_picks": 50,
    "horizons": {
      "D+1":  {"wr": 80, "hit_5pct": 25, "avg_ret": 3.49},
      "D+3":  {"wr": 100, "hit_5pct": 60, "avg_ret": 6.20},
      "D+5":  {"wr": 95, "hit_5pct": 70, "avg_ret": 7.72},
      "D+10": {"wr": 100, "hit_5pct": 80, "avg_ret": 9.81},
      "D+15": {"wr": 90, "hit_5pct": 80, "avg_ret": 13.67}
    }
  },
  "picks": [ {pick_with_actual_outcomes}, ... ]
}
```

#### `POST /api/power/picks/replay/random`
Picks a uniformly-random trading day from the last 2yr, computes the replay, caches in `falcon_replay_cache` with 24h TTL.

```json
{ "date": "2025-08-19", "is_featured": false, ...same shape as above }
```

### 4.2 Auth-gated (JWT required)

#### `GET /api/power/picks/today`
Top 100 with stories.

```json
{
  "signal_date": "2026-05-14",
  "entry_date": "2026-05-15",
  "total": 100,
  "picks": [ {pick}, ... ]
}
```

#### `GET /api/power/picks/replay/{date}`
Any date (no rate limit for authed users).

#### `GET /api/power/picks/live?cycle=latest`
Live tier decisions. `cycle ∈ {0930, 0945, 1000, latest}`.

```json
{
  "entry_date": "2026-05-15",
  "cycle": "0945",
  "computed_at": "2026-05-15T09:45:12+05:30",
  "summary": {"enter": 12, "wait": 6, "skip": 82},
  "decisions": [
    {"rank":1,"symbol":"HFCL","tier":"ELITE","action":"ENTER","ret_15":0.84,"vol_pct":18.2,"close_loc":0.87,"reason":"ret=+0.84% > +0.2%"},
    ...
  ]
}
```

### 4.3 Auth flow

#### `POST /api/power/auth/google`
Body: `{id_token: "<google id token>"}`
Validates id_token via Google's tokeninfo endpoint. Checks email in `power_user_users`:
- found + is_active → mint JWT (24h), return user
- not found → return 403 `{code: "NEEDS_INVITE", email}`

#### `POST /api/power/invites/redeem`
Body: `{id_token, code}`
On valid code: insert into `power_user_users`, mark code used, return JWT.

#### `GET /api/power/auth/me`
Returns current user from JWT.

### 4.4 Admin (behind ADMIN_SECRET)

#### `POST /api/power/admin/invites/issue`
Body: `{secret, n, expires_in_days?, note?}`
Issues `n` codes, returns the list.

#### `GET /api/power/admin/metrics`
Returns DAU, retention curve, top symbols viewed, etc.

## 5. Pick payload (the single shape used everywhere)

```typescript
type Pick = {
  rank: number;
  symbol: string;
  sector: string;            // natural-language form ("Telecom", "Capital Goods", etc.)
  score: number;             // sum of fired pattern lifts
  n_fires: number;           // total patterns firing for this stock today
  tier: 'ELITE' | 'HIGH' | 'MID' | 'LOWER' | 'TAIL';
  tier_icon: string;         // ⭐ 🟢 🟡 🟠 ⚪
  tier_desc: string;         // "Top-tier conviction. Only ~5% of trading days produce this."

  story: string;             // 2-3 sentence trader-voice narrative (polished)

  top_patterns: Array<{
    rank: number;            // 1, 2, 3
    trader_phrase: string;   // "above-average volatility + closed the WEEK powerfully + ..."
    hit_phrase: string;      // "6 of 10 hit +15% within 20 days (baseline 2 of 10 → 2.9× edge)"
    target: string;          // "hit_15pc_20d"
    oos_lift: number;        // 41.64
    mined_year: number;      // 2025
    pattern_id: number;
  }>;

  expected: {
    d5:  [number, number];   // [60, 70] = 60-70% chance of being green
    d10: [number, number];   // [60, 70] = 60-70% chance of being +5%
    d15_avg: string;         // "+8-12%"
  };

  actual?: {                 // historical mode only
    "D+1":  number;
    "D+3":  number;
    "D+5":  number;
    "D+10": number;
    "D+15": number;
  };

  risk: {
    stop_loss_pct:   -7;
    trail_trigger:   12;
    time_exit_days:  7;
  };
};
```

## 6. Live tier scheduler (mirrors operator's premarket_deployer pattern)

```python
# backend/power_user/services/scheduler.py
class LiveTierScheduler:
    """Daemon thread; wakes at 09:30:30, 09:45, 10:00 IST weekdays.
    Reuses admin Kite token (kanida_quant.db.kite_tokens). Writes to falcon_live_decisions."""

    CYCLES = [("0930", (9, 30, 30)), ("0945", (9, 45, 0)), ("1000", (10, 0, 0))]

    def run_cycle(self, cycle_name: str):
        # 1. preflight kite token (reuse falcon.preflight.run with include_kite=True)
        # 2. compute top-100 from falcon_features (today's date)
        # 3. fetch first-15min OHLCV from kite.historical_data per symbol
        # 4. apply_tier_rule(rank, ret_15, vol_pct, close_loc) → action
        # 5. UPSERT into falcon_live_decisions
```

Failure mode: if Kite call fails for a symbol, write `action='WAIT'` with `reason='kite_unavailable'` (do not poison the table).

## 7. Caching strategy

| Layer | TTL | Invalidation | Store |
|---|---|---|---|
| Featured replays | infinite | never (historical data immutable) | `falcon_replay_cache` rows with `is_featured=1` |
| Random replay results | 24h | natural expiry | `falcon_replay_cache` rows with `is_featured=0` and `expires_at` |
| Today's top-100 | until next 16:35 IST emit | replaced by next pipeline run | in-memory + 5min TTL |
| Live decisions | until next cycle | overwritten by scheduler | DB read each request, no app cache |
| Patterns library | until weekly_remine | invalidate-on-publish hook | in-memory module global, refresh once at boot |

Anonymous rate limit (random replay endpoint): SHA256(ip + UA) → counter in `power_user_request_log`, sliding 1h window, max 3 requests.

## 8. Frontend route structure (Next.js App Router)

```
app/
├── (public)/                          # no JWT required
│   ├── layout.tsx                     # public chrome (logo, nav, "Sign in" CTA)
│   ├── page.tsx                       # /  → 3 featured cards + Random + Today top 5
│   ├── replay/[date]/page.tsx         # /replay/2026-04-15
│   ├── today/page.tsx                 # /today  (top 5 + locked "see all 100")
│   ├── login/page.tsx                 # /login  (Google + redeem)
│   ├── waitlist/page.tsx              # /waitlist
│   └── credibility/page.tsx           # /credibility (₹30L → ₹1.05Cr proof)
├── (power)/                           # JWT required (middleware redirects)
│   ├── layout.tsx                     # gated chrome (user menu, logout)
│   ├── today/page.tsx                 # /me/today  (full top 100)
│   ├── replay/[date]/page.tsx         # /me/replay/[date]
│   ├── live/page.tsx                  # /me/live
│   └── watchlist/page.tsx             # Phase 1B
├── (admin)/                           # ADMIN_SECRET header required
│   ├── invites/page.tsx
│   └── metrics/page.tsx
├── api/auth/[...nextauth]/route.ts    # NextAuth Google provider
└── middleware.ts                       # route group → JWT check
```

State management: server components for static; React Query (TanStack) for client fetches; persists JWT in httpOnly cookie set by NextAuth.

## 9. Performance budget compliance — how each target is met

| Target | Plan |
|---|---|
| Landing < 500ms | SSR with featured payload baked into `getServerSideProps`. Single SQLite SELECT, no enrichment at request time. |
| Today's top-100 < 1s | `falcon_signals_live` already has top-100 ranked, sample_rules JSON. Pattern detail backfilled in-memory from a 865-row module-level dict. |
| Random replay cache miss < 2s | Worktree script clocks ~1.5s end-to-end. Move OHLC outcomes lookup to a single batched SQL (`WHERE symbol IN (...) AND trade_date BETWEEN ?`). |
| Random replay warm < 200ms | DB read of pre-computed payload_json. |
| Live decisions < 200ms | Pre-computed table; UPSERT-replace per cycle. |
| Detail expansion instant | Top-100 endpoint ships full per-pick payload; no second request. |

## 10. Deployment topology

- **Backend (Railway):** new service, separate from operator API. Reads `kanida_universe.db` via Litestream-mirrored S3 sync OR shares the operator's persistent volume. Phase 1: shared volume; Phase 2: read-replica on object store.
- **Frontend (Vercel):** standard Next.js project. Env vars: `NEXT_PUBLIC_API_URL`, `NEXTAUTH_URL`, `NEXTAUTH_SECRET`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`.
- **Database:** SQLite (same file as operator). Power-user tables and operator tables coexist; cross-table queries are READ-ONLY from power-user code.
- **Secrets:** Google OAuth client (web app), JWT signing key (256-bit random, rotated quarterly), ADMIN_SECRET (already exists).

## 11. Risks + mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Anonymous /random spam | Med | SHA256(ip+UA) rate limit; 3/hr cap; CAPTCHA on Phase 1B if abused |
| Admin Kite token expiry breaks live tier | High | Reuse falcon.preflight.run; surface in /admin/metrics; ops gets paged |
| Featured replay payload becomes stale (price corrections) | Low | Re-cache featured at boot; manual re-run via /admin |
| First-time user spam during invite gate | Med | Waitlist captures email; admin batch-issues per cohort |
| Cross-pollination of bugs between operator + power-user code | Low (separated dirs) | Tests assert no power_user module imports from `backend/falcon/trade/*` |
| OHLC has no row for a symbol on a date | Med | Outcomes returns `null` per horizon; UI shows "—" instead of crashing |

## 12. What we don't have to build (reuse)

- **Preflight pattern** — borrow shape from `backend/falcon/preflight.py` (named invariants, RED/YELLOW/GREEN, gate function)
- **PreflightBanner UI** — visual pattern transferable; build a `LiveTierBanner` for power-user side using the same shape
- **IST helpers** — same `datetime.now(IST)` discipline; copy from `backend/falcon/preflight.py`
- **Kite client factory** — `services.kite_auth.get_kite_client` already exists; live_tier scheduler imports it
- **DB connection** — `backend/falcon/db.py` `falcon_conn()` already works for SQLite; reuse
- **Sector mapping** — `falcon_sectors` table already populated

## 13. Open architectural questions for operator (go/no-go)

1. **Cache infrastructure choice:** SQLite tables (no Redis). OK?
2. **DB sharing on Railway:** Operator and power-user backends share the SQLite file via Railway persistent volume. OK?  Alternative: separate volume + Litestream sync (more complex).
3. **NextAuth on frontend (Google provider) vs custom OAuth handler:** NextAuth is faster but adds a dep. OK?
4. **Random replay anonymous rate limit:** 3/hr per IP-hash. Higher (e.g. 10) or lower?
5. **Featured replay re-cache cadence:** boot-time recompute, or just trust the data is immutable? (I lean trust; mark immutable.)
6. **Operator's Falcon Admin URL:** stays at `/falcon/*` on the operator-only host (not on app.kanida.ai). Correct?

Answer these 6 and I write `Tasks.md` + start porting `explain_picks_v3.py` → `services/explainer.py`.
