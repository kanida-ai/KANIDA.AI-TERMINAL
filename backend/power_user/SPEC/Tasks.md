# KANIDA.AI Power User Portal — Tasks

**Version:** 1.0
**Date:** 2026-05-14 IST
**Architecture decisions:** all 6 locked (SQLite cache, shared Railway DB, NextAuth.js, 3/hr anon rate limit, immutable featured, operator host stays separate)

Each task: ETA, dependencies, exit criteria. ETAs assume 1 working operator + me, parallel where deps allow.

---

## SPRINT 1 — Backend foundation (Days 1-2)

### T01 — Port sim_sweep deps → `services/feature_cols.py`
**ETA:** 30 min · **Dep:** none · **Status:** in progress now
- Extract `FEATURE_COLS`, `FEATURE_IDX`, `in_drawdown_bounce` from `universe_engine/.../sim_sweep.py`
- Verify column list matches `falcon_features` schema (40 cols)
- Add docstring explaining "why these features, where they come from"
- **Exit:** `from power_user.services.feature_cols import FEATURE_COLS, in_drawdown_bounce` succeeds; FEATURE_COLS has same content as worktree

### T02 — Port `explain_picks_v3.py` → `services/explainer.py` with 3 polish fixes
**ETA:** 1.5 hr · **Dep:** T01
- Copy translation library (`phrase_*` + `feature_to_phrase` + `rule_to_trader_phrase`) verbatim
- Copy `BASELINE_RATES`, `OUTCOME_LABEL`, `hit_rate_phrase` verbatim
- Copy `generate_story` with the **3 polish fixes**:
  - Sector naturalization via new `SECTOR_NATURAL` dict (22 sectors → natural forms)
  - Sentence capitalization helper `_capitalize_sentences`
  - Story joining cleanup (no double periods, capitalize each sentence)
- Refactor `compute_top_n` / `get_outcomes` as importable functions (no `if __name__ == "__main__"`)
- Build the canonical `Pick` payload (per Design.md §5) — a single `build_pick_payload(rank, pick, outcomes=None) → dict`
- Add `EXPECTED_BY_TIER` + `tier_info` + `OUTCOME_LABEL` as module constants
- Add module-level `load_patterns_cached()` with 5-min in-process cache
- **Exit:** `build_pick_payload(...)` returns dict matching Design.md §5 `Pick` shape; story has no `"telecommunication"` literal, no double periods, every sentence capitalized

### T03 — Tests for explainer polish + payload shape
**ETA:** 45 min · **Dep:** T02
- `test_sector_naturalization` — every sector enum value maps to natural form
- `test_no_double_periods_in_story` — generates 50 random stories, asserts `".." not in s`
- `test_first_letter_capitalized_after_period` — regex check
- `test_pick_payload_shape` — assert keys + types match Design.md §5
- `test_baseline_rates_present` — every used target in OUTCOME_LABEL is in BASELINE_RATES
- `test_in_drawdown_bounce_filter` — ensure the filter still drops the expected rule shape
- **Exit:** 6/6 tests pass

### T04 — Write `db_schema.sql` + `db_init.py`
**ETA:** 45 min · **Dep:** none (parallel with T01-T03)
- 7 tables per Design.md §3 (`power_user_users`, `power_user_invite_codes`, `power_user_waitlist`, `power_user_watchlists`, `falcon_live_decisions`, `falcon_replay_cache`, `power_user_request_log`)
- Indices: `ix_live_dec_date_cycle`, `ix_pulog_created`
- `db_init.py` — idempotent CREATE IF NOT EXISTS migrations; runs on backend boot
- **Exit:** Hitting `/api/power/db/init` (admin) twice produces same state (idempotent); all 7 tables visible in sqlite

### T05 — `replay_cache.py` — featured pre-compute + on-the-fly arbitrary + cache
**ETA:** 1.5 hr · **Dep:** T02, T04
- `precompute_featured()` — runs explainer for 3 featured dates, stores payload in `falcon_replay_cache` with `is_featured=1`
- `get_or_compute(date)` — featured lookup → cache lookup → compute + cache (24h TTL)
- `random_date()` — uniform pick from `ohlc_daily` dates in last 2yr
- **Exit:** featured replays return in <200ms (cache hit); arbitrary date warms cache on first call, sub-200ms on second

### T06 — Port `live_tier_tool_v2.py` → `services/live_tier.py`
**ETA:** 1 hr · **Dep:** T01, T04
- Refactor `apply_tier_rule` + `tier_of` + `compute_top_n` as importable
- New entry point: `run_cycle(cycle_name) → dict` that UPSERTs into `falcon_live_decisions`
- `load_kite()` reuses `services.kite_auth.get_kite_client` (already in main repo)
- Failure mode: per-symbol Kite error → write `action='WAIT'` with `reason='kite_unavailable'`
- **Exit:** `run_cycle("0930")` writes 100 rows; second call replaces in-place (idempotent)

---

## SPRINT 2 — Auth + invite gate (Day 3)

### T07 — `services/auth.py` — Google ID-token verify + JWT issue
**ETA:** 1 hr · **Dep:** T04
- Verify Google ID token via `google-auth` lib
- Issue JWT (PyJWT) — 24h expiry, signed with `JWT_SECRET`
- `verify_jwt(token) → user_dict | None`
- `get_current_user(request) → User` FastAPI dep
- **Exit:** valid id_token → returns matching user_dict; expired/forged → None

### T08 — `services/invites.py` + admin router
**ETA:** 45 min · **Dep:** T07
- `generate_codes(n, expires_days, note) → List[code]`
- `redeem(id_token, code) → (user, jwt)` — atomic check + mark used
- `POST /api/power/admin/invites/issue` (ADMIN_SECRET required)
- **Exit:** unit test: issue 5 codes, redeem 1, redeem same again → second fails with `CODE_ALREADY_USED`

### T09 — `routers/auth_router.py` + `routers/invites_router.py`
**ETA:** 1 hr · **Dep:** T07, T08
- `POST /api/power/auth/google` — returns JWT or `{code: NEEDS_INVITE}`
- `POST /api/power/invites/redeem`
- `GET /api/power/auth/me`
- `POST /api/power/auth/logout` (client clears cookie)
- **Exit:** Postman collection — full sign-up flow completes for a new email + valid invite

---

## SPRINT 3 — Public routes (Day 3-4)

### T10 — `routers/public_router.py` — featured replays + today preview + random
**ETA:** 1.5 hr · **Dep:** T05
- `GET /api/power/replay/featured`
- `GET /api/power/picks/today/preview` (top 5 only)
- `GET /api/power/picks/replay/{date}` — public if featured, else rate-limited random
- `POST /api/power/picks/replay/random` — uniform-random pick
- IP-hash rate limiter (SHA256 ip+UA, 3/hr per hash) via `power_user_request_log`
- **Exit:** anonymous curl hits all 4 endpoints; 4th random call in an hour → HTTP 429

### T11 — `routers/picks_router.py` — JWT-gated full endpoints
**ETA:** 45 min · **Dep:** T05, T07
- `GET /api/power/picks/today` (top 100)
- `GET /api/power/picks/replay/{date}` (custom date)
- `GET /api/power/picks/live?cycle=latest|0930|0945|1000`
- All require valid JWT; 401 otherwise
- **Exit:** authed user gets full payload; anonymous → 401

---

## SPRINT 4 — Live tier scheduler (Day 4)

### T12 — `services/scheduler.py` daemon thread
**ETA:** 1 hr · **Dep:** T06
- Mirrors `backend/falcon/trade/services/premarket_deployer.py` pattern (idempotent, single global lock)
- Cycles: `(0930, 9:30:30 IST), (0945, 9:45:00 IST), (1000, 10:00:00 IST)` weekdays only
- On boot: if today is past a cycle time but no row exists → run that cycle (catch-up)
- Logs each cycle to `falcon_job_runs` (or equivalent)
- **Exit:** boot at 09:50 IST → runs missed 09:45 cycle within 30s; never double-fires

### T13 — Mount routers in `main.py` + observability middleware
**ETA:** 30 min · **Dep:** T09, T10, T11
- `from power_user.routers import auth_router, picks_router, public_router, admin_router, invites_router`
- `from power_user.services.scheduler import start as start_live_tier_scheduler`
- Power-user request log middleware (logs user_id, route, latency, status to `power_user_request_log`)
- **Exit:** backend boots cleanly; `curl /api/power/replay/featured` returns 200 with 3 featured

---

## SPRINT 5 — Frontend skeleton (Days 5-6)

### T14 — Next.js route groups + NextAuth wiring
**ETA:** 1.5 hr · **Dep:** T09
- Route groups: `app/(public)`, `app/(power)`, `app/(admin)`
- `app/api/auth/[...nextauth]/route.ts` — Google provider, returns JWT to backend, stores in httpOnly cookie
- `middleware.ts` — redirect `(power)` and `(admin)` to `/login` if no JWT
- **Exit:** sign in with Google → cookie set → /me/today loads (or 401 if no invite)

### T15 — `lib/power-api.ts` — typed fetch client
**ETA:** 30 min · **Dep:** T13
- Mirror shape of `frontend/lib/falcon-api.ts`
- Typed wrappers for all 11 endpoints
- Auth header injection from cookie
- **Exit:** import `PowerAPI` in any component → autocomplete works

### T16 — Landing page (`app/(public)/page.tsx`)
**ETA:** 2 hr · **Dep:** T15
- Hero + "Sign in" CTA
- 3 `FeaturedReplayCard` components (server-rendered from cached payload, < 500ms)
- `RandomReplayButton` (🎲) — calls `/api/power/picks/replay/random`
- "Today's top 5" section (calls preview endpoint)
- Footer with `/credibility` link
- **Exit:** Lighthouse score > 90; first paint < 500ms on cache hit

### T17 — `PickCard` + `StoryBlock` + `PatternList` + `ExpectedOutcomeTable` + `ActualOutcomeRow` + `RiskControl` components
**ETA:** 2 hr · **Dep:** T15
- Collapsible card per pick (header always visible, body toggle)
- Render the canonical `Pick` payload (Design.md §5)
- Tier badge + icon styling per `EXPECTED_BY_TIER`
- "Actual outcome" row only renders if `pick.actual` present
- **Exit:** rendering a sample pick payload shows the exact format from the worktree CLI output

### T18 — `app/(public)/replay/[date]/page.tsx`
**ETA:** 1 hr · **Dep:** T17
- Reuses `PickCard` for each pick
- Aggregate header (`{wr, hit_5pct, avg_ret}` per horizon)
- Title chip ("The Blowout", "Year-Ago Proof", "Steady Day") for featured, or just date for random
- **Exit:** /replay/2026-04-15 renders all 50 picks with actual outcomes

### T19 — `app/(public)/today/page.tsx` (top 5 preview)
**ETA:** 45 min · **Dep:** T17
- Top 5 with full stories
- "See all 100 →" CTA → `/login` if anonymous, else `/me/today`
- **Exit:** unauthed user sees top 5; "see all" routes to login

### T20 — `app/(public)/login/page.tsx` + invite-redemption flow
**ETA:** 1 hr · **Dep:** T14, T08
- Google sign-in button
- On `NEEDS_INVITE` response → show code-entry form
- On invalid code → "Join waitlist" CTA → `/waitlist`
- **Exit:** end-to-end: new email → sign in → enter valid code → land on /me/today

---

## SPRINT 6 — Power-user routes + admin + polish (Days 6-7)

### T21 — `app/(power)/today/page.tsx` (full top-100)
**ETA:** 1.5 hr · **Dep:** T17, T11
- Top-100 list (compact rows, expand on click)
- Filters: sector dropdown, score band, tier
- Save-to-watchlist button (no-op in MVP; Phase 1B activates)
- **Exit:** 100 rows render; filters reduce visible set

### T22 — `app/(power)/replay/[date]/page.tsx` (custom date)
**ETA:** 45 min · **Dep:** T18
- Same shape as public replay but with date picker for any date
- "Share" button (copies URL)
- **Exit:** picking 2025-08-19 returns replay with full 100 picks

### T23 — `app/(power)/live/page.tsx`
**ETA:** 1 hr · **Dep:** T12, T11
- Renders the 3 cycles (09:30 / 09:45 / 10:00) as tabs
- ENTER / WAIT / SKIP buckets
- Auto-refresh every 60s during market hours
- "Last computed at: ..." stamp
- **Exit:** during market hours, page shows current cycle's decisions; off-hours shows "Markets closed, last decision at HH:MM IST"

### T24 — Admin routes (`/admin/invites`, `/admin/metrics`)
**ETA:** 1 hr · **Dep:** T13
- ADMIN_SECRET header check
- Invite generation form (n, expires_days, note)
- Metrics table: DAU, signups, invite redemptions, top viewed symbols
- **Exit:** operator can issue 12 codes, see them appear in the table

### T25 — Credibility page (`/credibility`)
**ETA:** 1 hr · **Dep:** T16
- ₹30L → ₹1.05Cr equity curve (4-year chart)
- Year-by-year breakdown table (2023 +83%, 2024 +91%, 2025 +53%, 2026 +23%)
- Walk-forward methodology disclosure
- **Exit:** investor screenshot-worthy page

---

## SPRINT 7 — Deploy + smoke (Day 7)

### T26 — Railway backend service config
**ETA:** 1 hr · **Dep:** T13
- New Railway service, separate from operator API
- Shared persistent volume (or read-replica via Litestream — Phase 2)
- Env vars: `JWT_SECRET`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `KITE_API_KEY` etc
- **Exit:** `curl https://api-power.kanida.ai/api/power/replay/featured` returns 200

### T27 — Vercel frontend deploy
**ETA:** 30 min · **Dep:** T26
- Connect repo, set env vars (`NEXTAUTH_URL`, `NEXTAUTH_SECRET`, `GOOGLE_CLIENT_ID/SECRET`, `NEXT_PUBLIC_API_URL`)
- Custom domain `app.kanida.ai`
- **Exit:** https://app.kanida.ai resolves, landing page loads

### T28 — Production smoke
**ETA:** 30 min · **Dep:** T26, T27
- All 3 featured replays load in < 500ms
- 🎲 Random replay loads in < 2s (cold), < 200ms (warm)
- Sign-in with Google + invite redemption flow
- Live decisions page during market hours
- **Exit:** zero P0 issues; investor demo-able

---

## Total estimate: 7 working days for 1 engineer (parallelize T04, T16-T20 to compress)

## What I'm starting NOW (T01-T03)
Next 3 hours: port `feature_cols.py` + `explainer.py` with polish + write tests + run them. Then show you the explainer module shape + a sample story output for one go/no-go before continuing T04 onward.

## What you owe me before Sprint 5

- Google OAuth client credentials (web app, redirect URI: `http://localhost:3000/api/auth/callback/google` + `https://app.kanida.ai/api/auth/callback/google`)
- `app.kanida.ai` DNS configured (CNAME → Vercel)
- Railway project + persistent volume access
- Decision: which email goes on the first 12 invite codes (so I can pre-issue them for your distribution)

I'll flag these again when each sprint approaches.
