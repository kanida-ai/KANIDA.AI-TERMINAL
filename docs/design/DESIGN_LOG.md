# Falcon Front-End — Design Decisions & Feedback Log

The `falcon-ui` agent reads this every run and APPENDS a dated entry after each
change. Operator feedback goes here too. This is how design iteration compounds.
Spec lives in `docs/specs/FALCON_AI_FRONTEND_PLAN.md`.

## Locked decisions (as of 2026-06-19)
- **Theme:** dark + mint `#3FE3A4` ("green = profit"). No new accent colors.
- **Benchmark:** Claude.ai — left nav switches modes; one primary action/surface;
  progressive disclosure; conversational (guided) entry. Feel = AI product, not dashboard.
- **IA — 6 modes:** Ask Falcon (home) · Signals · Co-Trading · AutoTrade ·
  Performance · Plans. Account/Learn/Admin in footer. (Collapses the 17 legacy routes.)
- **Home layout:** prompt is the HERO; Today's Top 10 is a compact one-tap "peek"
  strip below it (not a dashboard). Greeting "Good {morning…}, {first_name}" +
  one rotating market-pulse line.
- **Prompts are GUIDED only:** intent dropdown -> entity (stock/sector) picker -> Ask
  -> focused result panel. No open free-text to an LLM.
- **Personas:** Falcon Top 10 Swing (+ Weekly) = LIVE. BTST / Intraday / Long-Term =
  Launch-Pending. Per-user AutoTrade = Launch-Pending (operator-only today).
- **Paywall:** re-wire (built+audited, currently dormant). Free = history/proof;
  Basic = Standard; Pro = +Gold; Enterprise = +Enterprise + rulebook + AutoTrade.
  Gating via a single `<PlanGate>`; tier badges/locks inside cards, not on home.
- **Honesty:** no "certified 80%", no "smartest on earth", no "500 agents/stock".
  Explainability-led copy. Tiers = qualitative quality bands.
- **Build discipline:** dev branch/worktree off main; never touch prod tree or the
  auto-trade execution path.
- **Path:** build directly in the mint theme (no static-design detour); hero-first
  as a clickable prototype, operator reacts, then continue.

## Phase order
1a. AppShell (6-mode nav) + Ask-Falcon home (greeting, pulse line, guided composer,
    chips, Top-10 peek) + LaunchPending component.   <-- START HERE
1b. Persona-aware Signals + 7/14/20/30d tracker + paywall re-wire.
2.  Ask-Falcon analysis endpoints + result panels.
3.  Performance + free-user proof.
4.  Co-Trading + AutoTrade readiness.
5.  Persona expansion (real BTST/Intraday/Long-Term engines).

## Feedback / change entries
<!-- newest first; falcon-ui appends here -->
- 2026-06-28 — **AutoTrade Phase-2 multi-tenant: connect-your-broker-account panel
  + per-account session selection + graceful vault-disabled state** (operator ask;
  backend worktree commit a770457 adds the operator-token-gated broker-account
  endpoints under the existing `/api/falcon-proxy/api/autotrade` root, deploys
  later). UI-ONLY; NO backend/execution code touched; all calls via the existing
  same-origin `/api/falcon-proxy`. **NOT pushed/deployed — operator deploys with
  the backend later.** Files: `lib/autotrade-api.ts`,
  `components/power/autotrade/BrokerAccountsPanel.tsx` (NEW),
  `components/power/autotrade/PortfolioAutoTrade.tsx`,
  `components/power/autotrade/AutoTradePanel.tsx`,
  `app/power/(app)/autotrade/page.tsx`. tsc clean (EXIT 0) + `npx next build`
  GREEN (`/power/autotrade` built, all routes intact).
  - **Types/API (`lib/autotrade-api.ts`).** New `BrokerAccountStatus`
    (PENDING|ACTIVE|EXPIRED), `BrokerName`, `BrokerAccount {broker_account_id,
    broker, account_label, status, api_key_masked?, has_secret?, has_token?}`,
    `BrokerAccountsResponse {accounts?, vault_enabled?}`,
    `CreateBrokerAccountRequest` (api_secret documented WRITE-ONLY),
    `LoginUrlResponse`, `RefreshTokenResponse`, `SessionScope {user_id?,
    broker_account_id?}`. New methods: `brokerAccounts(userId)` →
    GET `/broker-accounts?user_id=`; `createBrokerAccount(req)` → POST
    `/broker-account`; `deleteBrokerAccount(id, userId)` → DELETE
    `/broker-account/{id}?user_id=`; `brokerLoginUrl(id, userId)` → GET
    `/broker-account/{id}/login-url?user_id=`; `refreshBrokerToken(id, token,
    userId?)` → POST `/broker-account/{id}/refresh-token`. `listSessions` /
    `createSession` / `preview` gained an optional `scope` (user_id via query on
    list, user_id+broker_account_id in the body on create/preview) — default
    (no scope) = the global/operator session, unchanged. Added `q`/`userQuery`/
    `scopeBody` query helpers.
  - **BROKER ACCOUNTS panel (NEW `BrokerAccountsPanel`).** New "Broker accounts"
    tab in `AutoTradePanel`. (1) Connect form: broker dropdown (Zerodha tagged
    **verified**; Upstox/Angel/Dhan/Fyers tagged **beta — unverified**),
    account_label, api_key, **api_secret (type=password, WRITE-ONLY)** → POST
    broker-account. The secret is snapshotted to a local then **cleared from
    state in the same tick** before the call resolves, and is NEVER read back or
    displayed. (2) Accounts list: account_label · broker · status badge (PENDING
    amber / ACTIVE green / EXPIRED red) · api_key_masked · secret-on-file ·
    token-active — NO secrets ever rendered. (3) Per row: **Connect / Re-connect**
    → GET login-url → `window.open` popup → user logs in at broker → pastes the
    `request_token` into an inline input → POST refresh-token → list reloads (row
    flips ACTIVE). **Delete** with `window.confirm`. (4) **vault_enabled=false →
    a calm amber "Broker-account vault isn't enabled yet — operator must set the
    vault key" empty state** (NOT an error; says sessions keep running on the
    global operator account).
  - **PER-ACCOUNT SESSION SELECTION (`PortfolioAutoTrade`).** The component now
    takes a `userId` prop (the logged-in operator's `user.id` from the existing
    power-auth session — no new auth invented). When `userId` exists it loads the
    user's accounts and the create form shows an optional **"Broker account"**
    selector — default **"Global account (operator default)"** (unchanged
    behaviour) or a connected account. When chosen, `broker_account_id`+`user_id`
    are sent on create + preview; the sessions list is scoped with `?user_id=`.
    **A LIVE Start on an EXPIRED account is BLOCKED** with a "re-connect first"
    prompt (paper is allowed; the global session is unaffected). An EXPIRED
    selection shows an inline red re-connect warning.
  - **Honesty.** api_secret never read back/displayed/persisted in the browser;
    every backend field degrades to "—"; vault-disabled is a friendly empty
    state, not an error; beta brokers flagged unverified; ALL existing AutoTrade
    UI (strategy dropdown, trail panel, dual returns, Kite P&L, kill_preview,
    speed strip, egress card, entry_date/trading-day rule, the three non-placed
    statuses, list/resume/delete, SCHEDULED flow) intact 1:1. Mint/F2 theme +
    viewport-lock preserved (reuses cotrade-kit `C`/`ICON`).
  - **Backend needs:** the operator-token-gated broker-account endpoints under
    `/api/autotrade` (commit a770457, deploys later): POST `/broker-account`
    `{user_id, broker, account_label, api_key, api_secret}` → masked dict; GET
    `/broker-accounts?user_id=` → `{accounts:[{broker_account_id, broker,
    account_label, status, api_key_masked, has_secret, has_token}],
    vault_enabled}`; DELETE `/broker-account/{id}?user_id=`; GET
    `/broker-account/{id}/login-url?user_id=` → `{login_url}`; POST
    `/broker-account/{id}/refresh-token {user_id?, request_token}`; and
    create/`/preview`/GET `/sessions` accepting optional `user_id` +
    `broker_account_id`. The UI degrades gracefully (empty selector, "—",
    silent fallback to the global session) if any endpoint isn't reporting yet.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ Compiled —
    `/power/autotrade` built, all other routes intact. **NOT pushed.**
- 2026-06-28 — **AutoTrade: wired the new execution-date / trading-day rule into
  the console (entry-date picker + on-missed-window + advanced grace; the 400
  "not a trading day" one-click apply; the resolved-fire line; the three new
  non-placed statuses)** (operator ask; backend now supports the rule). UI-ONLY;
  no backend/execution code touched; all calls still via the existing same-origin
  `/api/falcon-proxy`. Two files: `lib/autotrade-api.ts` +
  `components/power/autotrade/PortfolioAutoTrade.tsx`. Strategy dropdown, trail
  panel, dual returns, Kite P&L, kill_preview, speed strip, egress card, list/
  resume/delete, SCHEDULED flow all untouched. tsc + `npx next build` GREEN
  (`/power/autotrade` compiled).
  - **Types (`lib/autotrade-api.ts`).** New `OnMissedWindow = 'expire' |
    'carry_next_trading_day'`. `SessionConfig` gained `entry_date?` ("YYYY-MM-DD",
    optional — empty = backend resolves the next valid session),
    `on_missed_window?` (default 'expire'), `entry_grace_seconds?` (default 120).
    `SessionStatusName` widened with the three non-placed statuses
    `REJECTED_NON_TRADING_DAY` | `EXPIRED_MISSED_WINDOW` | `DEFERRED_MARKET_CLOSED`.
    `StatusResponse` gained `resolved_fire_datetime?`, `resolved_fire_date?`,
    `is_trading_day?`, `market_open_now?`, `entry_date?`, `on_missed_window?`,
    `deferred_reason?`.
  - **Create/Preview form.** A DATE PICKER ("Entry date (IST)") sits next to the
    entry-time field with a Clear button + an "auto" chip when empty (empty =
    next valid trading session). A segmented "If the fire moment is missed"
    control = Expire | Carry to next trading day, with the exact helper line
    ("if the fire moment is missed or lands on a non-trading day: drop it, or
    roll to the next trading day"). An "Advanced" `<details>` tucks the
    "Entry grace (seconds)" number input (default 120). `toWireConfig` sends
    `entry_date` only when non-empty (empty string would be an invalid date),
    always sends `on_missed_window` + a numeric `entry_grace_seconds`; pct→fraction
    conversion + the per-strategy boundary are unchanged.
  - **400 "not a trading day" → friendly one-click apply.** `onCreate` no longer
    dumps the raw 400. `parseSuggestedTradingDay` pulls the suggested
    `YYYY-MM-DD` out of the detail (matches "...Next trading day: 2026-06-29");
    `isNonTradingDayError` recognises the case. The form then shows an amber
    inline message ("That date isn't a trading day. The next trading day is
    {date}.") with a mint **"Use {date}"** button that sets `entry_date` + lets
    them retry, plus "Clear date (use next session)" and "Dismiss". Cleared on
    new-session / back-to-list / editing the date.
  - **Status / live view.** New `ResolvedFireLine` renders
    "Fires {resolved_fire_datetime} IST · trading day ✓/✗ · market open/closed"
    (trading-day ✓=mint / ✗=amber; market open=mint / closed=muted; every absent
    field → "—"; renders nothing when no relevant field is present). It shows
    PROMINENTLY in the SCHEDULED card (the resolved fire datetime now leads the
    "Fires at" stat, not just the bare time) and also inside the normal RUNNING
    Live-status block.
  - **Three new statuses, distinct + muted/amber.** New `NonPlacedCard` handles
    `REJECTED_NON_TRADING_DAY` / `EXPIRED_MISSED_WINDOW` / `DEFERRED_MARKET_CLOSED`
    with an amber/muted treatment (a "did not place" pill — never the green
    RUNNING look). It surfaces `deferred_reason` verbatim when present (else a
    per-status fallback sentence), the resolved-fire line, and the echoed
    entry_date / on_missed_window. A `nonPlacedNow` gate HIDES the RUNNING views +
    KILL block for these; only `DEFERRED_MARKET_CLOSED` (still alive) keeps a
    Cancel action.
  - **Honesty held 1:1.** Empty entry_date is shown as "auto / next valid
    session"; every new backend field degrades to "—"; non-placed statuses are
    visually unmistakable as not-placed; ships-disabled banner + paper-default/
    typed-LIVE + KILL all intact. Mint/F2 theme preserved (reuses cotrade-kit
    `ICON.clock`/`info`/`check`/`chevronR`/`C`).
  - **Backend needs:** none — the ask states the rule is LIVE
    (`POST /session/create` accepts `entry_date` / `on_missed_window` /
    `entry_grace_seconds` and 400s with the suggested-date detail; `status()`
    returns `resolved_fire_datetime` / `resolved_fire_date` / `is_trading_day` /
    `market_open_now` / `entry_date` / `on_missed_window` / `deferred_reason` and
    the three new status values). The UI degrades gracefully if any field/status
    isn't reporting yet.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ Compiled
    successfully — `/power/autotrade` built, all other routes intact. NOT pushed
    (operator deploys).
- 2026-06-25 — **AutoTrade: added a compact SPEED / latency readout to the Live
  status area (works for BOTH strategies — kill switch + intraday_basket)**
  (operator ask; backend `status()` now ALWAYS returns three latency ints, may be
  null). UI-ONLY; no backend/execution code touched; all calls still via the
  existing same-origin `/api/falcon-proxy`. Two files: `lib/autotrade-api.ts` +
  `components/power/autotrade/PortfolioAutoTrade.tsx`. Trail panel / dual returns /
  Kite P&L table / kill_preview all untouched. tsc + `npm run build` GREEN
  (`/power/autotrade`).
  - **Type.** `StatusResponse` gained `entry_latency_ms` (fire → all legs settled,
    deploy speed), `exit_latency_ms` (flatten trigger → all flat, exit speed),
    `last_tick_age_ms` (now − newest tick, data freshness) — all `number | null`,
    documented as ALWAYS-present ints in ms.
  - **Speed strip.** New `<SpeedStrip>` renders inside Live status, ABOVE the
    strategy-specific panels (so identical for both strategies), below the bases
    line. Shows "Entry {…}" (neutral), "Exit {…}" (only once a flatten has been
    measured, i.e. `exit_latency_ms != null`, so it never implies an exit that
    hasn't happened), and "Data {…}" (last tick age) with a dot.
  - **Formatting.** `fmtMs`: <1000 → "368 ms"; ≥1000 → "2.7 s" (1 dp, trailing .0
    trimmed); null/non-finite → "—" (not measured yet).
  - **Liveness colour.** `tickLiveness` tones the tick-age + dot: green +pulsing
    dot + label "monitoring sub-second" when <1500ms; amber "feed lagging"
    1500–5000ms; red "stale" when older; faint "no data" when null. Entry/Exit
    stay neutral. This is the heartbeat proving sub-second monitoring.
- 2026-06-25 — **AutoTrade: wired the new Falcon Intraday Basket strategy into the
  console (strategy dropdown + preset prefill, branched form, live trail status
  panel, intraday strategy-summary)** (operator ask; backend now supports two exit
  strategies). UI-ONLY; no backend/execution code touched; all calls via the
  existing same-origin `/api/falcon-proxy`. Two files: `lib/autotrade-api.ts` +
  `components/power/autotrade/PortfolioAutoTrade.tsx`. Co-Trading / other tabs /
  the kill-switch UX all untouched. tsc + `npm run build` GREEN (`/power/autotrade`).
  - **Strategy dropdown (create form).** New `strategy` field on `SessionConfig`:
    `portfolio_kill_switch` (default, "flat ±% basket exit", the EXISTING behaviour)
    | `intraday_basket` ("Falcon Intraday Basket — arm & trail, square-off").
    Selecting intraday seeds the VALIDATED PRESET (`INTRADAY_PRESET`): top_n=5,
    entry 09:15:00, MTF, arm 1.0%, floor 1.0%, trail giveback 0.75%, stop 1.5%,
    square-off 15:29:00 — all editable. Switching back restores kill-switch defaults.
  - **Branched form.** kill-switch → unchanged single kill_switch_pct + direction.
    intraday → hides the kill number; shows four labelled %-inputs (Arm/profit,
    Lock floor, Trail giveback, Stop loss) each with helper text, plus a
    square-off-time field (HH:MM, sent as HH:MM:SS). UNITS: state in PERCENTS,
    `toWireConfig` converts ÷100 per-strategy ONLY at the send boundary (one
    conversion site); intraday forces kill_switch_enabled=false on the wire.
  - **Strategy summary (config).** For intraday, instead of the kill-switch
    "Potential outcome" card, a single legible line: entry → arm +X% → trail Y%
    giveback (floor +Z%) → stop −W% → square-off HH:MM, on ₹{invested} notional
    ~{leverage}× from `/preview` (invested_basis+leverage). Honest —/loading/error.
  - **Trail status panel (Live status).** For intraday sessions, reads
    `status.trail{…}` (flat mirrors as fallback). Shows strategy pill + ARMED
    state, Current notional return / Peak / live Exit-trigger (×100) + a mm:ss
    square-off countdown, the plain-English line "Armed at +1% · peak +2.3% ·
    exits if it gives back to +1.55%", and the four configured numbers. On CLOSE:
    exit_reason + notional_return×100 + own_funds_return×100. Kill-switch's dual
    return + Kite ₹ P&L + kill_preview displays are unchanged and shown only for
    the kill-switch strategy. All fields degrade to "—" when absent.
- 2026-06-24 — **AutoTrade: DUAL return display (invested vs fund, kill basis
  made explicit) + P&L "Potential outcome" preview card** (operator ask; backend
  moved the kill switch + gross return to an INVESTED-capital basis and now
  returns the new fields + a `POST /api/autotrade/preview`). UI-ONLY; no backend/
  execution code touched; all calls via the existing same-origin `/api/falcon-proxy`.
  Two files: `lib/autotrade-api.ts` + `components/power/autotrade/PortfolioAutoTrade.tsx`.
  Co-Trading / other AutoTrade tabs / all other pages untouched. tsc +
  `npm run build` GREEN (`/power/autotrade` built).
  - **(1) DUAL RETURN (Live status).** The single "Gross return" stat is replaced
    by TWO clearly-labelled stats: **Return (invested)** = `gross_return×100`
    (sub-label **"kill basis"**) and **Return (on fund)** = `gross_return_fund×100`
    (sub-label **"÷ your fund"**). Below them a basis line shows **Invested basis**
    = `fmtINR(invested_basis)` ("the kill basis") + **Fund** =
    `fmtCapital(total_allocated_capital)` so invested-vs-fund is unambiguous (MTF =
    leveraged invested value, CNC = cash). The kill-switch readout now states it
    **"triggers at ±X% … on the invested return"** (was a bare "±X%"). The existing
    Kite-style per-row P&L (₹) + Chg columns and the Invested/Current/Total-P&L
    `PortfolioSummary` footer are unchanged. `Stat` gained an optional `sub` label.
    Honest "—" when `gross_return_fund`/`invested_basis` aren't sent.
  - **(2) P&L PREVIEW (the operator's ask, CONFIG form).** New `PotentialOutcome`
    card renders ONLY when the kill switch is enabled, sourced from a new
    `AutoTradeAPI.preview(config)` → `POST /api/autotrade/preview` (creates no
    session, places nothing). It shows, per the configured direction:
    **At +target% → +₹{basis_value_rs} ( +{fund_pct×100}% on your fund )** and/or
    **At −stop% → −₹{basis_value_rs} ( −{fund_pct×100}% on your fund )**, plus the
    basis line **"on ₹{invested_basis} invested · ~{leverage}× MTF · fund {…}"**.
    The call is **debounced 450ms** on the config fields that move the bases/
    outcome (capital, top_n, sizing, order_product, kill_switch_pct, direction,
    max_pct_per_position). Honest **"Estimating…" / error ("preview endpoint may
    not be reporting yet" → "—") / "—"** states; never fabricated. The same
    two-sided renderer (`KillPreviewCard`/`OutcomeSide`) ALSO surfaces the LIVE,
    exact `status().kill_preview` in the running view (sub-tagged **"· live"**)
    when the backend supplies it.
  - **UNITS held 1:1.** Everything from the backend is a FRACTION → **×100** for
    %; ₹ via `fmtINR`/`signedINR`. State keeps `kill_switch_pct` as a PERCENT
    (input reads "1"); the preview call sends **/100** at the boundary — the SAME
    convention as `createSession`, so there is no double-conversion. `basis_value_rs`
    (a magnitude) is signed by side (target +, stop −) so ₹ + % read consistently.
  - **API (`lib/autotrade-api.ts`):** added `KillPreviewSide { pct; basis_value_rs;
    fund_pct }`, `KillPreview { target?; stop? }`, `PreviewResponse { invested_basis;
    total_allocated_capital; leverage; kill_preview }`; extended `StatusResponse`
    with `gross_return_fund?`, `invested_basis?`, `kill_preview?` (gross_return
    re-documented as the INVESTED/kill basis); added `AutoTradeAPI.preview(config)`
    → `POST /preview`.
  - **Honesty.** Preview/live cards render only the sides + figures the backend
    returns; missing fields → "—"; estimate is labelled "places nothing"; ships-
    disabled banner, paper-default/typed-LIVE, KILL, scheduled flow, list/resume/
    delete, egress-IP all untouched. Mint/F2 theme + viewport-lock intact; reuses
    cotrade-kit `fmtINR`/`signedINR`/`fmtPct`/`fmtCapital`/`pctTone`/`ICON`/`C`.
  - **Backend needs:** none (the ask states `status()` already returns
    `gross_return`/`gross_return_fund`/`invested_basis`/`total_allocated_capital`/
    `kill_preview` and `POST /api/autotrade/preview {config}` →
    `{ invested_basis, total_allocated_capital, leverage, kill_preview }` are LIVE).
    The UI degrades to "—"/honest-error if any field/endpoint isn't reporting yet.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ compiled —
    `/power/autotrade` built, all other routes intact.
- 2026-06-24 — **AutoTrade Sessions: critical % UNITS fix + Kite-style P&L +
  egress-IP self-service** (operator ask; 3 fixes). UI-ONLY; no backend/execution
  code touched; all calls via the existing same-origin `/api/falcon-proxy`. Two
  files: `lib/autotrade-api.ts` + `components/power/autotrade/PortfolioAutoTrade.tsx`.
  Co-Trading / other AutoTrade tabs / all other pages untouched. tsc +
  `npm run build` GREEN (`/power/autotrade` built).
  - **(1) UNITS — the backend speaks FRACTIONS, the UI was sending/showing raw.**
    The backend uses fractions for percentages (`kill_switch_pct=0.01`=1%,
    `gross_return=-0.0136`=-1.36%). Previously the UI sent the user's "1" as `1.0`
    (=100% → kill switch never fired) and displayed fractions without ×100
    (showed "-0.0%" for -1.36%). FIXED both directions:
    • **SEND ÷100** — `onCreate` now converts `config.kill_switch_pct` (kept in
      state as a PERCENT so the input reads "1") to a FRACTION at the create
      boundary only (`/100`); state is untouched, so there is NO double-conversion.
      The input stays labelled "Trigger at (%)".
    • **DISPLAY ×100** — every backend fraction now ×100 + "%": Live status
      **GROSS RETURN** (`fmtPct(status.gross_return*100)`), the kill-switch
      threshold line (**"±X%"** from `status.kill_switch_pct*100`, trailing zeros
      trimmed), and the **session-list gross** (`s.gross_return*100`). Per-position
      Chg % was already computed as a true percent from ltp/avg — left as-is.
    • Net: a "1%" kill switch now actually arms at 1%; GROSS RETURN reads -1.36%
      not -0.0%. (Per-position-stop/target fields aren't in this form yet — only
      `kill_switch_pct` exists to convert; noted for when they're added.)
  - **(2) KITE-STYLE P&L.** The Live-status positions table gained an absolute
    **P&L (₹)** column per row = the backend's `unrealised_pnl` (signed ₹ via
    `signedINR`, green/red via `pctTone`); the existing per-position return % is
    now the **Chg** column; "LTP" relabelled from "Last". Added a Kite-footer
    **portfolio summary** (`PortfolioSummary`): **Invested** = Σ(qty×avg_price),
    **Current value** = Σ(qty×ltp), **Total P&L** = Σ(unrealised_pnl) shown as ₹
    AND % (Total P&L ÷ Invested ×100), color-coded. Each cell is HONEST — a total
    is summed only when EVERY contributing position supplies the field, else "—"
    (never a partial/fabricated total). ₹ via cotrade-kit `fmtINR`/`signedINR`.
  - **(3) EGRESS-IP SELF-SERVICE.** Added `AutoTradeAPI.egressIp()` → GET
    `/api/falcon-proxy/api/falcon/egress-ip` (the `call` helper gained an optional
    `base` so this one call targets the `/api/falcon` proxy root instead of
    `/api/autotrade`; proxy injects the operator token + forwards verbatim). New
    `EgressIpCard` on the panel (below KILL/sessions, above the saved-presets
    panel): shows **"Broker allowlist IP: <ip>"** with a **Copy** button (+ "as of"
    when given) and the line *"Add this to your broker's Allowed IPs
    (developers.kite.trade) so live orders don't error."* Graceful: a 404 / not-yet-
    live endpoint shows **"—"** + a Retry + an honest "endpoint isn't reporting yet"
    note — never a fabricated IP.
  - **API (`lib/autotrade-api.ts`):** added `EgressIpResponse { ip; as_of? }` +
    `egressIp()`; `call<T>` now accepts `{ base? }` to reach the sibling
    `/api/falcon` proxy root. `OpenPosition.unrealised_pnl` was already typed — now
    consumed by the P&L column + summary.
  - **Honesty held 1:1.** No conversion is applied twice (state stays in percent,
    convert only at send; display only at render). P&L cells/totals use the
    backend's own fields and show "—" on any missing field. Egress IP never
    fabricated. Ships-disabled banner, paper-default/typed-LIVE, KILL, scheduled
    flow, list/resume/delete all untouched. Mint/F2 theme + viewport-lock intact.
  - **Backend needs:** `GET /api/falcon/egress-ip` → `{ ip: string, as_of?: string }`
    (the server's outbound IP for the broker allowlist) — being added; the card
    degrades to "—" until live. Better-if-present: `unrealised_pnl` on each
    `open_position` of the session status (the P&L column + Total P&L read it; rows
    show "—" where absent). No other new endpoints.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ compiled —
    `/power/autotrade` built, all other routes intact.
- 2026-06-24 — **AutoTrade Sessions list: position count + live indicator,
  immediate-refresh-on-resume, multi-select bulk delete** (operator ask: a
  RUNNING session showed only "Running" with no position count, so it read as
  "no orders were placed" even though the backend holds 5 positions; also needed
  bulk delete of paper/test sessions; also resuming briefly showed a blank "No
  open positions" until the first poll). UI-ONLY; no backend/execution code
  touched; all calls go through the existing same-origin `/api/falcon-proxy`. Two
  files: `lib/autotrade-api.ts` + `components/power/autotrade/PortfolioAutoTrade.tsx`.
  Co-Trading / other AutoTrade tabs / all other pages untouched. tsc +
  `npm run build` GREEN (`/power/autotrade` built).
  - **List rows no longer look empty for RUNNING sessions.** A RUNNING row now
    renders a mint **"Running" pill with a pulsing live dot** (new `RunningPill`
    + scoped `at-live-pulse` keyframe, `prefers-reduced-motion` honored) instead
    of a flat "Running" text label. Each non-scheduled row gained a **Positions**
    cell: shows `n_open_positions` when the list endpoint provides it, else an
    honest **"open"** (RUNNING, with the live dot) or **"—"** — never a fabricated
    count. **Gross return** + **mode pill** were already shown; kept. Net effect:
    a running session visibly indicates it holds positions even before you open
    it. SCHEDULED rows keep their existing distinct treatment (no Positions cell).
  - **RESUME fix — status fetched IMMEDIATELY.** `onResume` now awaits a direct
    `AutoTradeAPI.sessionStatus(s.session_id)` (using the row's id, not relying
    on closure timing) right after switching to the running view, seeding
    positions/LTP/gross/countdown at once. Previously the first status only
    arrived on the poll interval (up to 12s), leaving a blank "No open positions".
    Errors here are non-fatal (the poll retries); `busy='status'` keeps the
    loading state honest. The poll effect is unchanged and still takes over.
  - **MULTI-SELECT BULK DELETE (paper/test housekeeping).** Each row gained a
    selection checkbox (stop-propagation so it doesn't trigger resume) + a header
    **"Select all"** + a **"Delete selected (N)"** red button that appears only
    when ≥1 is checked. Selected rows get a red-tinted border/fill. On delete:
    `window.confirm` (states it removes the session RECORD, paper/test
    housekeeping, cannot be undone) → `AutoTradeAPI.deleteSessions(ids)` → clears
    the selection + reloads the list. Honest loading ("Deleting…") + an inline
    red error toast with retry/dismiss. A small caption under "Select all" says
    the checkboxes select records for deletion and don't open the session.
  - **API (`lib/autotrade-api.ts`):** added `deleteSessions(ids: string[])` →
    `POST /api/falcon-proxy/api/autotrade/sessions/delete` body `{ session_ids }`,
    typed `DeleteSessionsResponse { deleted: number; ids: string[] }`. The
    `SessionSummary.n_open_positions?` field was already in the type — now
    consumed by the list row.
  - **Honesty held 1:1.** No fabricated counts (uses `n_open_positions` if given,
    else "open"/"—"); delete is framed as paper/test record cleanup, confirmed,
    irreversible-warned; ships-disabled banner + paper-default + all other honesty
    untouched. Mint/F2 theme + viewport-lock + everything else (instant/scheduled
    start, SCHEDULED countdown, KILL) intact.
  - **Backend needs:** `POST /api/autotrade/sessions/delete` body
    `{ session_ids: string[] }` → `{ deleted: number, ids: string[] }` (being
    added per the ask). Optional-but-better: include `n_open_positions` on each
    row of `GET /api/autotrade/sessions` so the list shows the exact count
    without opening the session (the UI already reads it if present, else shows
    "open"/"—").
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled —
    `/power/autotrade` built, all other routes intact.
- 2026-06-24 — **AutoTrade: instant-vs-scheduled start wired into the portal**
  (operator ask; backend already LIVE). UI-ONLY; no backend/execution code
  touched; everything goes through the existing same-origin `/api/falcon-proxy`
  (which forwards the JSON body verbatim, so `{ when }` reaches
  `/api/autotrade/session/{id}/start`). Two files:
  `lib/autotrade-api.ts` + `components/power/autotrade/PortfolioAutoTrade.tsx`.
  Co-Trading / the other AutoTrade tabs / all other pages untouched. tsc +
  `npm run build` GREEN (`/power/autotrade` built).
  - **API (`lib/autotrade-api.ts`):** added `StartWhen = 'now' | 'scheduled'`;
    `startSession(id, when='now')` now POSTs `{ when }` (was a bare POST).
    `StatusResponse` + `StartResponse` + `SessionSummary` gained the SCHEDULED
    fields `fires_at?` (ISO IST), `seconds_remaining?` (int), `scheduler_armed?`
    (bool), and `status` is typed `SessionStatusName` (incl `'SCHEDULED'`).
    `entry_time` was already on `SessionConfig` — kept.
  - **Create form:** the **Entry time (IST)** `<input type="time">` is wired to
    `config.entry_time`; default changed `09:20 → 09:15` (matches the backend
    default + the spec). Used only when you choose to schedule.
  - **TWO clear start actions (replaces the single "Start session"):** after
    Create the CREATED card now shows side-by-side **"Start now"** (mint/paper
    or red/live → `startSession(id,'now')`, places immediately → RUNNING) and
    **"Schedule for {entry_time}"** (mint outline → `startSession(id,'scheduled')`,
    arms it → SCHEDULED, places nothing). Each button carries a one-line honest
    subtitle; paper stays the safe default, live framing/typed-LIVE-confirm
    unchanged. The header line now also shows the entry time.
  - **SCHEDULED waiting state (running/session view):** when status is SCHEDULED
    the normal RUNNING view (Live status + positions + KILL block) is HIDDEN and
    a mint **"Scheduled — waiting to fire"** card shows instead: **Fires at
    {fires_at}** + a big **live countdown** (seeded from `seconds_remaining`,
    ticked locally each 1s, re-synced on every poll — backend stays source of
    truth; shows "firing…" at 0), an honest "nothing placed yet" line, and a
    red **"Cancel schedule"** button (calls `kill` — places nothing). If
    `scheduler_armed === false` (post-restart) it instead shows an amber **"Not
    armed — re-schedule to arm the timer again"** note with a **Re-schedule**
    action (`startSession(id,'scheduled')`). Polling SPEEDS UP to 6s while
    SCHEDULED (12s otherwise) so the auto-flip to RUNNING + its placement shows
    promptly; on flip the countdown clears and the normal running view returns.
  - **Sessions list row:** a SCHEDULED session renders distinctly — a mint
    clock icon + **"Scheduled"** pill (new `SchedPill`), a mint "fires {fires_at}
    · in {countdown}" subline (instead of id/created_at), a mint status cell
    "Scheduled" (instead of Return), and a mint-tinted border — so it reads apart
    from RUNNING/CLOSED rows. Resuming a SCHEDULED row seeds the countdown from
    the row immediately.
  - **Honesty held 1:1.** Ships-disabled banner, paper-default + typed-LIVE,
    KILL/cancel framing, "nothing placed until entry time", not-armed honesty,
    all numbers from the backend, no fabrication. Mint/F2 theme + viewport-lock
    preserved (reuses cotrade-kit `ICON.clock`/`C`/`fmtPct`/`pctTone`).
  - **Backend needs:** none — `start {when}`, the SCHEDULED status fields, and
    `kill`-cancels-a-scheduled-session are all LIVE. (The sessions LIST showing
    `fires_at`/`seconds_remaining` per-row is best-effort: if the list endpoint
    omits them the row still shows the Scheduled pill from `status` and resume
    re-fetches the live fields via `status`.)
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled —
    `/power/autotrade` built, all other routes intact.
- 2026-06-24 — **UNIFIED AutoTrade panel — ONE place for everything**
  (operator's explicit ask). Replaced the two-tab `OperatorAutoTrade`
  (PortfolioAutoTrade + `AutoTradeConsoleHub` launcher) with a SINGLE sub-tabbed
  `components/power/autotrade/AutoTradePanel.tsx` rendered by the operator branch
  of `app/power/(app)/autotrade/page.tsx` (role==='admin'). The
  `AutoTradeConsoleHub` LAUNCHER is GONE — its 6 links are now real tabs.
  UI-ONLY; no backend/execution code touched; `/falcon/*` routes left fully
  working as a fallback. tsc + `npm run build` GREEN.
  - **5 tabs, everything in one panel:**
    1. **Sessions** (HOME/default) — the new multi-broker `/api/autotrade/*`
       system (`PortfolioAutoTrade`).
    2. **Pre-Market** — REUSES the legacy `app/falcon/premarket/page.tsx`
       verbatim (default export imported + rendered in the tab).
    3. **Positions** — REUSES `app/falcon/positions/page.tsx` verbatim.
    4. **Config** — REUSES `app/falcon/config/page.tsx` verbatim (trail config +
       engine playbook).
    5. **Engine** — REUSES `app/falcon/admin/page.tsx` (preflight / jobs / Kite
       token / inbox) + an embedded `<details>` of `app/falcon/trade/page.tsx`
       (manual preview→smoke→place).
    Reuse mechanism: the legacy pages are plain default-export client components
    that call `FalconAPI`, which ALREADY routes through the same-origin
    `/api/falcon-proxy` (operator token injected server-side) — so they work
    unchanged inside `/power`. No logic/behaviour changed; imported via the
    `@/app/falcon/*/page` alias and mounted in a `LegacyMount` padded container.
  - **DISAPPEARING-SESSION BUG FIXED (list + resume).** Added
    `listSessions()` to `lib/autotrade-api.ts` (`GET` →
    `/api/falcon-proxy/api/autotrade/sessions`, typed `SessionsListResponse` /
    `SessionSummary`, read-only). `PortfolioAutoTrade` gained a new `'list'` HOME
    phase: on mount it fetches the sessions and shows **"Your sessions"**
    (newest-first, sorted by `created_at` desc) — each row shows status · mode
    pill · gross return · capital · id · created_at. **Clicking a row RESUMES**
    it (jumps to the live `running` view + polls status) instead of resetting to
    a blank form. **"New session"** is now an explicit action that opens the
    create form; after Create the list refreshes so the new session is visible,
    and the running view has a **"← Your sessions"** back button + a **"New
    session"** button. So a reload no longer loses your session — it restores
    the list and you resume.
  - **Honesty kept 1:1.** Top-of-panel framing: "Sessions run in PAPER by
    default; live needs `FALCON_AUTOTRADE_ENABLED`; Pre-Market/Positions/Config/
    Engine are your live Falcon operator controls." Ships-disabled banner,
    paper-default/green + live-behind-typed-"LIVE"-confirm, KILL-typed-confirm,
    honest loading/empty/error on the sessions list (never fabricated). All
    numbers from the backend. Mint/F2 theme + viewport-lock preserved.
  - **Dead code:** `OperatorAutoTrade.tsx` + `AutoTradeConsoleHub.tsx` are no
    longer imported anywhere (kept in tree, tree-shaken; safe to delete later).
  - **Backend needs:** `GET /api/autotrade/sessions` → `{ sessions: [{
    session_id, status, mode, total_allocated_capital, gross_return, created_at,
    top_n_stocks?, n_open_positions? }] }` (any order; the UI sorts by
    created_at). If this endpoint isn't live yet, the list shows an honest error
    + Retry (no fabrication) — the rest of the panel is unaffected.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled —
    `/power/autotrade` + all `/falcon/*` routes built.
- 2026-06-24 — **Portfolio AutoTrade operator UI** (LIVE multi-broker
  `/api/autotrade/*`, operator-token gated). UI-ONLY; no backend/execution code
  touched. New files: `lib/autotrade-api.ts` (transport-only client → existing
  same-origin `/api/falcon-proxy/api/autotrade/...`; proxy injects the operator
  token server-side, secret never hits the browser),
  `components/power/autotrade/PortfolioAutoTrade.tsx` (the new console) and
  `components/power/autotrade/OperatorAutoTrade.tsx` (two-tab shell). Wired into
  `app/power/(app)/autotrade/page.tsx` — the `isOperator` branch now renders
  `OperatorAutoTrade` (tab "Portfolio Sessions" [new] + tab "Operator Console" =
  the EXISTING `AutoTradeConsoleHub`, kept, not removed). Flow: config form
  (capital + presets / top_n 3·5·7·10 / sizing equal·pct_cap·manual /
  max_pct_per_position when pct_cap / order_product CNC·MIS·MTF·NRML / kill switch
  {toggle, pct, direction} / entry_time) → Create → Start (per-symbol
  placed/skipped table) → live Status card (gross return %, kill-switch state,
  open-positions table, 12s auto-refresh + manual refresh) → red KILL with a
  typed "KILL" confirm. **HONESTY:** standing amber "Ships disabled" banner
  (paper default · kill off · live needs server `FALCON_AUTOTRADE_ENABLED`);
  Mode selector presents PAPER as green/safe DEFAULT and LIVE as red behind a
  typed "LIVE" confirm + a standing red warning that it still does nothing until
  the server flag is set. All numbers from backend; honest loading/empty/error
  states, never fabricated. Read-only `config/list` + `broker/list` in a
  load-on-demand "Saved presets & brokers" panel. Reuses cotrade-kit palette +
  icons; viewport-lock (shrink-0 header + flex-1 min-h-0 scroll body) preserved.
  Backend needs: none (all 9 endpoints already live). tsc + `npm run build`
  GREEN.
- 2026-06-23 — **MIGRATION: legacy portal functionality mounted inside the new
  AI-native shell** (route group `app/power/(app)/*`) by REUSING the working
  legacy components — no rebuild, no backend/auth/admin/execution touched. tsc +
  `npm run build` GREEN. All legacy routes kept as fallback (deprecate later).
  - **P1 — SIGNALS** (`signals/page.tsx` rewritten + new
    `components/power/signals/SignalsExperience.tsx`): replaced the launch-pending
    shell with REAL content as a 2-tab surface inside the shell's viewport-lock
    (shrink-0 header + flex-1 min-h-0 overflow-y-auto). Tab "Today's Top 10"
    REUSES `Top20Card` + `Top20Filters` (data: `PowerAPI.falconTop20(universe,
    sector?, signal_date?)`) — 3-bucket explainability + universe/sector/EOD-date
    filters + low-signal-day banner. Tab "Live decisions" REUSES `CyclePicker` +
    the ENTER/WAIT/SKIP bucket layout (data: `PowerAPI.liveDecisions(jwt, cycle)`;
    skipped honestly when no JWT, e.g. preview-no-auth). A mint CTA links to
    Performance → replay outcomes for the viewed signal date
    (`/power/performance?date=…`). Server page fetches both via
    `Promise.allSettled` (one failure never blanks the other). nav `signals`
    stays `live:true`.
  - **P2 — PERFORMANCE** (`performance/page.tsx` rewritten + new
    `components/power/performance/PerformanceExperience.tsx`): replaced
    launch-pending with a 2-tab surface. Tab "The proof" = the ₹30L→₹1.05Cr
    3.3-yr walk-forward credibility content (reused 1:1 from `/power/credibility`
    — static, operator-locked, year-by-year + methodology + "what we don't hide").
    Tab "Replay outcomes" REUSES `ExpandablePickRow` (the `/power/replay/[date]`
    surface; data: `PowerAPI.replayForDate(date, jwt)`) with an IN-SHELL date
    navigator routing to `/power/performance?date=…` (so the user never leaves the
    shell) — aggregate D+1/3/5/10/15 band + per-pick outcomes. Deep-links from
    Signals open straight on the Replay tab. nav `performance` flipped
    `false → true`.
  - **P3 — CO-TRADING** (additive; the existing 2-stage simulate flow UNCHANGED):
    • Persona LISTING — `SetupStage` now renders a "Browse live AI co-traders"
      section REUSING `PortfolioCard` (data: `PowerAPI.portfolios()` +
      `portfolioEquity(slug)` sparklines, fetched server-side in the page,
      `Promise.allSettled` degrade). Section omitted entirely if the fetch fails
      (never faked). `CoTradingExperience` + `SetupStage` gained optional
      `personas`/`sparklines` props; everything else (mechanism strip, steps,
      sim wiring, honesty) untouched 1:1.
    • Persona DETAIL — new sub-route `co-trading/[slug]/page.tsx` REUSES
      `PortfolioDashboardClient` verbatim (the `/power/portfolios/[slug]` surface:
      Zerodha-style P&L, open positions, recent trades, equity curve, year-by-
      year), data via `portfolioDetail/positions/trades/equity`. Wrapped in the
      shell viewport-lock with a "← Back to Co-Trading" link.
    • `PortfolioCard` gained a backward-compatible `hrefBase` prop (default
      `/power/portfolios`) so the shell cards link to `/power/co-trading/[slug]`
      without changing the legacy listing.
  - **AppShell** `fullBleed` set extended to `/power/signals`, `/power/performance`,
    and `/power/co-trading/*` so each owns its internal viewport-lock scroll
    region (no shell page-scroll).
  - **LEFT as honest "Soon" (not flipped):** Plans, Account, Learn.
  - **Honesty:** only real payloads rendered; live decisions skipped (not faked)
    without a JWT; persona listing omitted (not faked) on fetch failure; replay
    surfaces backend errors plainly. Only Falcon Top 10 Swing is live; other
    co-trading styles stay "soon".
  - **Backend needs:** none — all reused endpoints are already LIVE.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ Compiled
    successfully — `/power/signals`, `/power/performance`, `/power/co-trading`,
    `/power/co-trading/[slug]` all built; legacy `/power/today`, `/power/live`,
    `/power/replay/[date]`, `/power/credibility`, `/power/portfolios/[slug]`
    intact.
- 2026-06-22 — **Co-Trading RESULT wired to the now-LIVE POST /api/power/cotrade/
  simulate** (power-api.ts + CoTradingExperience.tsx only; setup stage / switcher /
  mechanism strip / mint-F2 theme / viewport-lock all UNCHANGED — DATA WIRING ONLY).
  The result is now the user's REAL virtual portfolio (a SIMULATION on EOD data —
  modelled entries/exits, not real fills), replacing the prior "pending" tiles and
  the persona year-grain-scaled replay numbers.
  - **power-api.ts** — added the typed contract `CotradeSimulateRequest` +
    `CotradeSimulateResponse` (summary / positions / equity / actions / honesty,
    matching the verified shape) and `PowerAPI.cotradeSimulate({style,capital,
    start_date,end_date?})` (POST via the existing apiFetch).
  - **handleStart** now calls `cotradeSimulate('falcon-top-10', capital, start_date)`
    for BOTH paths — live (start_date = IST today, no end_date) and replay
    (start_date = chosen date, end_date = today) — then drives the RESULT stage from
    the response. New `sim`/`simLoading`/`simErr` state.
  - **SummaryStrip** now reads the REAL `summary`: Starting / Current value / Total
    P&L / Return % / Open / Cash / Max Drawdown. The old "—/pending" dashes for
    these are REPLACED with real numbers; while the sim runs they read
    "…/simulating…" (calm, never fabricated).
  - **SimResult** (new) is the hero: big Starting→Current value + Return%/P&L, the
    EQUITY CURVE built from the REAL `equity` series (reuses `EquityChart`), and a
    5-cell stat row (max DD / win rate / trades / open / closed) — all from
    `summary`. Surfaces the endpoint's `honesty` string verbatim as a caption; a
    day-one LIVE follow (pnl≈0, n_closed=0) is flagged honestly.
  - **SimPositions / SimPositionCard** (new) render the REAL `positions` (entry,
    qty, capital, SL for open / exit for closed, status pill Open/Closed, per-
    position P&L ₹ + %). Tier badge: `positions[].tier` first, else JOINED to the
    live signal_tier surface (seeded Top20) by symbol, else NO badge (never
    fabricated). BAND-derived colours (never signal_tier_color).
  - **Falcon ACTIONS feed** (`SimActionsFeed`, new) renders `actions`
    (entry/exit/trail/skip + reason), folded INSIDE the "Inspect deeper" expand so
    the result stays calm. Colour-toned by action type.
  - **Honesty / states.** Loading → calm `SimLoadingCard`; failure → honest
    `SimErrorCard` ("couldn't run the simulation … POST /api/power/cotrade/simulate")
    — never a fabricated portfolio. Replay returning an empty sim → the existing
    `ReplayPendingCard`. The client allocation `LivePortfolio` preview is kept ONLY
    as a fallback when the sim is unavailable.
  - **Kept AS-IS:** the persona "Proven track record" year-by-year table in
    "Inspect deeper" still reads `GET /api/power/personas/falcon-top-10` (separate
    historical confidence, NOT the user's sim). Removed the now-dead `ReplayHero`
    (the sim replaces the year-grain-scaled replay); `Stat` retained (SimResult uses it).
  - **Backend needs (remaining):** per-user persistence of the simulated portfolio
    (so a live follow accumulates across sessions) + live intra-day ticks beyond
    EOD (the sim is EOD-only by design).
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npx next build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-22 — **AutoTrade page built (launch-pending UX) + shared PlanSwitcher +
  Co-Trading /quote entry-reference wiring** (3 tasks; worktree feat/falcon-ai-shell).
  DRY refactor: extracted the shared Co-Trading primitives into a new
  `components/power/shared/cotrade-kit.tsx` (the F2 `C` palette, full `ICON` set,
  `Gear`/`MechanismStrip` + `MECHANISM_CSS`, tier-band helpers `TIER_STYLE`/
  `BAND_COLORKEY`/`tierBand`, formatters `fmtINR`/`signedINR`/`fmtNum`/`fmtCapital`/
  `pctTone`/`fmtPct`/`istTodayISO`); CoTradingExperience now imports them instead of
  its local duplicates (NO behaviour change to the clean 2-stage flow — all copy/
  math/honesty 1:1). MechanismStrip gained a `variant` ('cotrade'|'autotrade'),
  `onBridge`, and a `launching` badge prop. Backend untouched.
  - **TASK 1 — AutoTrade** (`AutoTradeExperience.tsx` + rewired `autotrade/page.tsx`):
    MIRRORS the Co-Trading 2-stage flow + the gear mechanism strip so it reads as the
    same product family, but STRICT honesty — it places/simulates NOTHING.
    • Mechanism strip variant "Falcon trades for you, automatically, with your real
      broker" + a persistent amber **"Launching soon for your account"** badge.
    • STAGE 1 SETUP mirrors Co-Trading: choose style (Swing live; others "soon") →
      set capital (presets, REAL ₹50k@₹5L per-pick sizing line) → **Connect your
      broker (Zerodha)** shown as the STRUCTURE, **disabled/"soon"** (per-user connect
      not built, operator-only execution stated) → a 4-item **readiness checklist**
      (broker connected=soon · funds=pending · risk config=pending · market hours=
      pending) shown honestly as structure.
    • Primary button is **NOT execute** — it's **"Join the AutoTrade waitlist"** → the
      preview; copy states that once per-user automation is live Falcon auto-enters
      the Top 10 at 9:15 IST, manages SL/trailing/exit and reports.
    • STAGE 2 PREVIEW = **"What Falcon would trade today"** from the REAL
      `PowerAPI.falconTop20('all500')` (seeded server-side), labelled **"preview —
      not executing"**, per-card status pill **"Would auto-enter"**; REAL allocation
      math (qty=floor(perTrade/entry), SL from action.stop_loss_pct else −7%, BAND-
      derived tier colours). A **"Notify me when it's live"** waitlist card (local
      state; persistence flagged as a Backend need) + a bridge to Co-Trading.
    • Made the page **full-bleed** (added `/power/autotrade` to AppShell's fullBleed
      check) for parity with Co-Trading; **nav kept `live:false`** ("Soon") because
      the FEATURE is still launch-pending even though the page is built (honest).
  - **TASK 2 — shared PlanSwitcher** (`components/power/shared/PlanSwitcher.tsx`):
    compact, forward-looking, used at the top of BOTH Co-Trading (mint accent) and
    AutoTrade (amber accent). A slim row of plan chips ("Swing · ₹5L ●") + an
    **"All plans"** aggregate chip + **"+ Add a style"** popover (only Swing live;
    others disabled "soon", already-added flagged). `AllPlansAggregate` shows combined
    capital (REAL) + combined P&L/positions/return (honest "—/pending" until the
    live-tracking backend supplies real per-plan P&L). Structural today (one live
    plan) and SAYS so; designed to scale to N plans. Per-style chips only render the
    extra row when >1 plan so the clean pages aren't re-cluttered; the switcher always
    appears on the result/preview header.
  - **TASK 3 — Co-Trading /quote entry reference** (CoTradingExperience.tsx): where a
    pick's signal payload has no entry price, the LIVE result now fetches
    `PowerAPI.quote()` (already live) for ONLY the missing symbols and uses
    `last_close` as an entry **REFERENCE**, labelled **"ref: last close · {as_of}"**
    (amber, Entry→"Entry (ref)") — explicitly NOT a live tick, NOT a fill. qty/capital
    sized honestly off that reference. `AllocRow` gained `entrySource`
    ('signal'|'quote'|null) + `quoteAsOf`; an honest banner explains the ref sizing;
    the old "entry missing" banner now only fires when neither signal NOR quote has a
    price. No fabrication; quote-fetch failure leaves entry "—".
  - **Honesty held 1:1.** AutoTrade places/simulates nothing; quote ref is EOD not a
    fill; multi-style others = "soon"; preview uses real picks only; all P&L stays
    "—/pending" where unserved.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ Compiled
    successfully (`/power/autotrade` + `/power/co-trading` both built).
- 2026-06-22 — **Ask-Falcon home WIRED to the 3 now-LIVE backend endpoints**
  (AskFalconHome.tsx + power-api.ts only; layout / mechanism strip / composer /
  3-column structure UNCHANGED — pure data-wiring). Replaced every "coming
  soon"/"pending"/fallback stub with real data, honest states preserved.
  - **power-api.ts — 3 typed fetchers added** under `PowerAPI`, using the existing
    `apiFetch`/`apiBase` pattern: `universeSymbols()` → `UniverseSymbolsResponse
    { as_of, count, symbols:[{symbol,name,sector}] }`; `quote(symbols[])` →
    `QuoteResponse = Record<string, { last_close, prev_close, as_of }>` (filters
    falsy + caps the list at 60 client-side); `analyzeStock(symbol)` →
    `AnalyzeStockResponse` (all 11 prose/label fields + `risk_warnings[]`; a 404
    surfaces as `PowerAPIError(404,'NOT_COVERED')` via the existing error mapper).
  - **Universe search** — `StockSearch.ensureLoaded()` now calls
    `PowerAPI.universeSymbols()` instead of a raw `fetch`; the full ~477-name list
    loads once on first focus and filters client-side as before. REMOVED the
    "full-universe search coming soon" note; the only remaining note is an honest
    "couldn't load the full universe just now — searching today's Top 10 instead"
    that shows ONLY on a real fetch failure (graceful fallback to Top-10 symbols).
  - **Price row (right detail card)** — new `quotes: Record<string, Quote>` state
    on the home, fetched in ONE batched `PowerAPI.quote(top10Symbols)` call
    (≤60) keyed off `data.signal_date/universe/sector`. `PriceBreakdown` now takes
    a `quote` prop: **Current LTP = `last_close`** (₹ formatted) labeled
    **"last close · {as_of}"** (+ derived day-change `last_close/prev_close-1`,
    mint/red) — explicitly NOT a live tick; **Prev-day LTP = `prev_close`**
    ("prior EOD close"). On a missing quote the cells show "—" / "quote
    unavailable" (no crash, nothing fabricated). Signal-day % stays REAL; **Entry
    day kept as "market open pending"** (genuinely pending). Added `fmtRs()`.
  - **Single-stock analysis** — `StockAnalysis` rewritten from the "analysis
    coming soon" scaffold to call `PowerAPI.analyzeStock(symbol)` and render each
    REAL field as its own section (explanation lead → current_trend / price /
    volume / signal-day / sector / falcon_pattern_observations / entry_context →
    `risk_warnings[]` bullets → "as of {as_of} · not financial advice"), plus a
    tier chip (`tier` + `tier_reason` tooltip) coloured via the canonical
    `BAND_COLORKEY`. **404 → honest "{SYM} isn't covered by Falcon yet"**; other
    errors → honest "couldn't load … try again". Top-10 names still short-circuit
    to the REAL DetailCard (never hit this path). New helpers `AnalysisSection`
    (renders only when prose present), `AnalysisTierBadge`. Removed the
    `ANALYSIS_SECTIONS` "soon"-tagged placeholder grid.
  - **Honesty held 1:1.** EOD close never mislabeled as live; no fabricated
    prices/analysis; graceful empty/"unavailable" on every failure path.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/ask` built).
- 2026-06-21 — **Co-Trading SETUP polish round — fill the horizontal space, fit one
  viewport, fix the truncated mechanism step** (CoTradingExperience.tsx only;
  page.tsx / AppShell / globals.css untouched; 2-stage flow + mechanism strip +
  "How Falcon manages your money" rules link + all real wiring/honesty intact 1:1).
  Operator: setup was a thin centered column with big black side-margins, scrolled
  vertically, and the mechanism strip's last loop-back step "Repeats every trading
  day — automatically" was truncated/cramped; general shrink/alignment issues.
  - **FIX 1 — MECHANISM STRIP gear-train no longer truncates.** The 4 step cells
    were a `flex … overflow-x-auto` row of fixed `min-w-[118/132px]` cells with the
    loop-back chip pinned inline at `max-w-[120px]` (clipped "automatically"). Now
    the steps are a responsive CSS grid (`grid-cols-2 sm:grid-cols-[1fr_auto_1fr_
    auto_1fr_auto_1fr]`) — equal `1fr` cells that SIZE TO FIT the full strip width
    with the `→` arrows in their own `auto` columns between cells (arrows hidden
    `<sm`, where it falls to a 2-up grid). The `↻` loop-back is pulled OUT of the
    row onto its own full-width centered pill line below (border + faint mint fill),
    so its label can never clip/wrap-cramp. Removed the `truncate` on step titles
    (was shrinking "Kanida.AI"). Strip stays a slim banner.
  - **FIX 2 — use the width + fit the viewport.** Setup container widened
    `max-w-[760px]` → **`max-w-[1120px]`** (px-6→px-8) so it fills the space instead
    of leaving dead side-margins. Vertical rhythm tightened (`py-7/9 gap-6` →
    `py-5/6 gap-4 md:gap-5`; heading logo 30→28, h1 25→24, gap-2→1.5) to fit
    1366×768 / 1440×900 without page scroll. Composition rebalanced: heading +
    mechanism strip + **Step 1 (style cards) full-width** (cards now `lg:grid-cols-5`
    — all five styles on ONE row at desktop, was `md:grid-cols-3` = 2 rows), then
    **Step 2 (capital) + Step 3 (start) SIDE-BY-SIDE** in a `lg:grid-cols-2` row
    (they're short — kills the tall vertical gap), then the full-width **"Start
    Co-Trading"** button below. Stacks cleanly on narrow widths.
  - **FIX 3 — alignment/shrink cleanup.** Removed the `max-w-[320px]` clamp on the
    capital input (now fills its column); step grid items `items-start` so the two
    short columns top-align; subtitle max-w 480→560. No density added — same cards,
    same copy, same controls, just balanced whitespace + even rhythm.
  - **Honesty/scope held 1:1.** No copy/data/wiring change — real picks, FIXED
    ₹50k@₹5L allocation math, persona backtest, honest pending states, the single
    rules slide-over, viewport-lock (`md:overflow-hidden` + internal scroll),
    mint/F2 theme, gear animation + `prefers-reduced-motion` all unchanged. Added
    one import (`Fragment`) for the grid step/arrow interleave.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading: added a "How Co-Trading works" MECHANISM strip at the
  top** (CoTradingExperience.tsx only; page.tsx / AppShell / globals.css untouched;
  the existing 2-stage SetupStage/ResultStage flow, all real wiring + honesty
  preserved 1:1). Operator goal: ONE compact visual that makes the automation
  instantly graspable — "set it once, Kanida.AI then runs continuously like a
  machine" — and acts as the hook toward AutoTrade.
  - **New `MechanismStrip` component** (+ a small inline-SVG `Gear` cog, the
    `MECHANISM_STEPS` config, scoped `MECHANISM_CSS`, and two icons `user`/`loop`).
    A slim mint/F2 gear-train banner: a cluster of GENTLY-ROTATING inline-SVG cogs
    (8-tooth, `@keyframes ct-gear-spin` 11s linear infinite, meshing pairs spin in
    ALTERNATE directions via `animation-direction: reverse`) reads as a running
    mechanism. Headline "Set it once. Falcon runs the machine." + the subline. A
    left→right 4-step flow with arrows: 1 YOU (mint, marked "input", the only human
    action) → 2/3/4 ⚙ Kanida.AI (picks Top 10 · decides entry/SL/exit · reports
    performance), each non-human step carrying its own little spinning cog, then a
    ↻ loop-back chip "Repeats every trading day — automatically". A compact
    secondary bridge button "AutoTrade does exactly this with your real broker →"
    routes to /power/autotrade.
  - **Placement.** Prominent full version at the TOP of STAGE 1 (SETUP), directly
    under the heading and above Step 1 — does NOT push the steps below the fold
    (banner is slim; setup region keeps its `md:overflow-y-auto`, viewport-lock
    intact). STAGE 2 (RESULT) shows the SLIM 1-line variant in the fixed header
    (one running cog + "Set once · Falcon picks, enters, manages & exits — every
    trading day, automatically"), hidden < sm to keep the result uncluttered.
  - **Honesty + a11y.** 100% STATIC explanatory content — no numbers/P&L/prices.
    `prefers-reduced-motion: reduce` disables the spin (scoped `<style>` media
    query; globals.css NOT touched, per the locked "no new bare :root after @theme"
    rule — keyframes are component-scoped with a unique `ct-gear-*` namespace).
    Mint accent only, no new color. SetupStage gained an `onAutoTrade` prop
    (router.push('/power/autotrade')); ResultStage reused its existing one.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading MAJOR REDESIGN → simple, visual, decision-oriented
  2-STAGE flow** (CoTradingExperience.tsx only; page.tsx / AppShell / globals.css
  untouched). Operator: prior 3-column page was too detailed/textbook; must feel
  like "Choose style → Add capital → Falcon manages the plan → See performance" —
  understood in a few seconds. Replaced the dense always-on 3-column workspace with
  ONLY TWO user actions then Falcon does everything; layout is now a centered
  STAGE state machine (`stage: 'setup' | 'result'`), still viewport-locked
  (`md:h-full md:overflow-hidden`, internal-scroll regions, no page scroll).
  - **STAGE 1 — SETUP** (`SetupStage`, centered `max-w-[760px]`, calm): Step 1
    Choose trading style = visual cards (Swing live; BTST/Intraday/Weekly/Long-Term
    disabled "soon"). Step 2 virtual capital input + ₹1L/₹5L/₹10L chips. Step 3
    Choose Start = a clear two-card toggle "Start Today" (live, mint) vs "Replay a
    past date" (amber, reveals a date picker). ONE big primary **"Start
    Co-Trading"**. NO risk profile, NO allocation config, NO rules tables — a single
    secondary link **"How Falcon manages your money"** opens the existing
    `RulesSlideOver` (renamed header; still the ONLY place rules/capital-model/cycle
    live). Falcon decides everything (fixed ₹50k@₹5L sizing, entry/SL/trail/exit).
  - **STAGE 2 — RESULT** (`ResultStage`, the hero): top = compact `SummaryStrip`
    (Starting · Current/Ending · Total P&L · Return% · Open · Cash · Max Drawdown,
    honest "—/pending" where live data isn't served). Then branches:
    • **LIVE ("Start Today")** → `LivePortfolio` "Falcon selected your Top 10": a
      clean VISUAL card grid (symbol · BAND tier badge · capital · entry+qty · stop)
      with an HONEST status pill ("Queued · 9:15" when an entry price exists, else
      "Waiting") + a one-line Falcon caption (enter 9:15 · −7% stop · trail +12% ·
      exit by day 7). Allocation = REAL (fixed ₹50k@₹5L scaled, qty=floor(perTrade/
      entry), SL from action.stop_loss_pct else −7%). Live Hold/Trailing/Exit + P&L
      = Backend need, NOT fabricated.
    • **REPLAY (historical date)** → `ReplayHero`: big "Starting → Ending value"
      with Return% / P&L, a real **EQUITY CURVE** (reuses `EquityChart` from
      EquitySparkline.tsx, built from the REAL persona monthly `end_equity` series
      for the start-date's calendar year, linearly scaled from the ₹5 L book to the
      user's capital — flagged honestly), plus Max DD / Win rate / Completed trades /
      Open-at-end. Pulled from the REAL persona endpoint
      (`PowerAPI.persona('falcon-top-10')` → yearly/monthly). Year-grain +
      not-walk-forward + linear-scale all flagged in-card as Backend needs.
  - **"Inspect deeper"** = ONE collapsed `<details>` (replaces the old default-visible
    track-record card): rulebook link → slide-over, the REAL year-by-year table
    (`YearByYearTable`, click a year → real months), and the risk disclosure folded
    inside. Nothing dense up front. **"← Change plan"** (header back button + chip)
    returns to Stage 1; AutoTrade bridge CTA at the bottom of the result.
  - **Removed:** the always-on MIDDLE setup column + RIGHT workspace, the
    `AllocationCard`/`AllocRowView` table + per-row Reduce/Remove/scale controls and
    `removed`/`scale` state, the `MobileSetup` bottom bar, `committedReplayDate`,
    `SetupField`/`MetricsStrip`/`Metric`/`Disclaimer`/`HeadlineStat`/`TrackRecordCard`.
    Added `SummaryStrip`/`Cell`, `LivePortfolio`/`PositionCard`/`Mini`, `ReplayHero`/
    `Stat`, `InspectDeeper`, `Step`, `signedINR`, `perTradeFor`. Kept `RulesSlideOver`
    + all its STATIC sections, `YearByYearTable`, tier-band colouring, the verbatim
    risk disclosure, all formatters + icons.
  - **Honesty held 1:1.** Real picks only; allocation math unchanged; no fabricated
    P&L/price/status; equity curve from the real monthly series; replay window
    coverage gap + per-date walk-forward + month-DD all surfaced as Backend needs;
    "virtual capital · not financial advice" kept (compact on setup, header chip-less
    but stated; status pills honest). Replay still attempts a point-in-time
    `falconTop20(startDate)` and shows the honest `ReplayPendingCard` on no data.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading SIMPLIFY / DECLUTTER round** (CoTradingExperience.tsx
  only; page.tsx / AppShell / globals.css untouched). Operator: page too dense,
  rules too long, analysis over-informative, flow weak. GOAL FLOW now = setup
  (middle) → your plan + the proof (right) → start. The 3-column viewport-lock
  (`md:h-screen` chain, independently-scrolling columns) and ALL real wiring +
  honesty preserved 1:1.
  - **CHANGE 1 — rules became ONE button + a slide-over.** Removed the inline
    7-row "Kanida.ai Virtual Trading Rules" list from the MIDDLE column; replaced
    with a single compact **"Trading rules"** button. New `RulesSlideOver` (an
    in-flow faux-overlay: `absolute inset-0`, scrim + right panel, NOT
    position:fixed — so the viewport-lock/independent-scroll is untouched)
    consolidates ALL rules content in ONE place: the rule list (VIRTUAL_RULES) +
    Capital model (fixed allocation) + What this trader does + the 7-step trading
    cycle. MIDDLE column is now just: trading style · virtual capital (+chips) ·
    start date (live/replay) · [Trading rules] button · Update/Build button.
  - **CHANGE 2 — RIGHT panel collapsed from SIX sections to THREE.** Removed the
    standalone `BacktestSummaryCard`, `SelectedPeriodCard`, and the five
    `Collapsible`s (Year-by-year / Locked Falcon Rules / Capital Model / What this
    trader does / Trading cycle), plus the `ActionsFeed`, `ControlsCard`, the
    generic `Collapsible`, `LockedRules`, `PerfCell`, and `buildSeedActions`/`paused`
    state. Right panel top→bottom is now ONLY: (a) the **metrics strip** (kept —
    Starting · Current · P&L · Return% · Open · Cash · Drawdown, honest dashed
    pending where live data isn't served); (b) the **Allocation plan** (the REAL
    positions table from Top20Response — the core "what Falcon does with my
    money"); (c) ONE clean **"Proven track record"** card — three big scannable
    headline numbers from the REAL persona endpoint (`PowerAPI.persona('falcon-top-10')`:
    avg yearly return, positive years X-of-Y, win rate), a SINGLE native
    `<details>` **Year-by-year** expand (real `yearly` → click a year → real
    `monthly`), and the operator RISK DISCLOSURE folded into one secondary
    `<details>` line. The selected-period numbers (year return + equiv P&L on the
    user's capital, REAL) now fold into a slim strip INSIDE the track-record card
    on a replay date — no longer its own dense section. Then the **AutoTrade
    bridge** CTA at the very bottom.
  - **Honesty held.** Metrics strip still shows only computable values as REAL
    (Starting / Open+%deployed / Cash) and dashes live P&L/return/drawdown as
    pending; allocation math unchanged (fixed ₹50k@₹5L scaled, qty=floor(alloc/
    entry), SL from action.stop_loss_pct else locked −7%, BAND-derived tier
    colours); track-record numbers fetched live from the persona endpoint with
    loading/error/no-data → "no numbers" not fabrications; the replay strip still
    flags whole-window-not-walk-forward as a Backend need; the verbatim risk
    disclosure kept. Disclaimer kept (chip lg+, full card mobile). Added `book` +
    `close` icons.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading content round: fixed-allocation model + REAL backtest
  panel** (CoTradingExperience.tsx only; page.tsx/AppShell untouched). The
  3-column viewport-first layout (md:h-screen chain, independently-scrolling
  columns, no page scroll) and ALL existing real allocation wiring are PRESERVED.
  HONESTY SPLIT enforced: strategy RULES = STATIC documented content; all
  PERFORMANCE NUMBERS = the REAL persona endpoint, never hardcoded.
  - **MIDDLE — removed the Risk profile selector** (Conservative/Balanced/Aggressive
    + `RiskId`/`Risk`/`RISKS` + tilt-weight math all deleted). Replaced with a
    compact STATIC **"Kanida.ai Virtual Trading Rules"** card (Entry 9:15 IST /
    Trade size ₹50k@₹5L / Holding 7d / SL −7% gap-down / Smart trailing +12% →
    higher of entry or 10-day low / Capital cash-only integer shares / Position
    skip-held). Allocation now uses a single **FIXED model**: perTrade =
    (capital/₹5L)×₹50k across the full Top 10; qty = floor(perTrade·scale/entry).
    Same REAL entry/qty/SL math + per-row Reduce/Remove controls kept.
  - **RIGHT (default-visible):** Allocation preview (REAL) + NEW **Backtested
    Performance Summary** (REAL `GET /api/power/personas/falcon-top-10`): avg
    yearly return, equiv. yearly P&L (= avg_return × user capital, COMPUTED),
    worst year, win rate (avg_win_rate_pct), avg trades/year (total_trades ÷
    total_years, COMPUTED), positive years (positive_years of total_years) + two
    STATIC logic lines ("avg_lift × 10-pattern gate" / confidence) + the
    operator's RISK DISCLOSURE verbatim (2021–2026 only, not crash-tested
    2008/2015/2020, 2021=15-trade sample, −15.59% 2025). Plus a **Selected-period
    snapshot** (REAL, scoped to the chosen date's calendar year: period P&L on the
    base, YTD return, latest-month MTD, open-at-period-end, held-overnight).
  - **RIGHT (collapsible):** Year-by-year table (REAL `yearly`; click a year →
    REAL months from the already-loaded `monthly` array, winning months DERIVED
    count(return_pct>0)); Locked Falcon Rules (STATIC full rulebook); Capital
    Model: Fixed Allocation (STATIC); What this trader does (STATIC); Trading
    Cycle 7 steps (STATIC). Generic `Collapsible` (mint chevron).
  - **Honesty held:** persona fetched on mount; loading/error/no-data states show
    "no numbers" not fabrications; summary typed locally off `Record<string,
    unknown>`; replay note clarifies aggregates are whole-window not per-date
    walk-forward (Backend need); sub-year point-in-time P&L + month-level max DD
    flagged as Backend needs.
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓ compiled
    (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading REFACTORED to the 3-column viewport-first layout**
  (CoTradingExperience.tsx only; page.tsx + AppShell untouched — AppShell already
  full-bleeds /power/co-trading with `md:h-[100dvh] md:overflow-hidden`, same as
  /power/ask). Goal: identical no-page-scroll, independently-scrolling-columns feel
  as the Ask-Falcon home. **COPIED AskFalconHome's `md:h-screen` mechanism exactly.**
  - **Before:** a two-STAGE single-column flow (SETUP screen → "Build my allocation"
    → PREVIEW screen) inside `md:h-[calc(100dvh-7rem)]` with ONE internal scroll
    region; full-screen stage switching; the right-side cards were a `lg:grid` only
    inside the preview stage.
  - **After:** ONE screen, two columns shown together (no stage switching, no page
    scroll). Root `flex flex-row min-h-screen md:min-h-0 md:h-full md:overflow-hidden`;
    EACH column pinned `md:h-screen md:min-h-0 md:overflow-hidden` (the h-screen chain,
    NOT h-full — matching the Ask fix note).
    • **MIDDLE = SETUP CONTROLS** (`w-[360px]`, flex-col): shrink-0 heading; a
      `flex-1 min-h-0 overflow-y-auto` region with the four compact controls (style
      selector dropdown — only Swing live, others "soon"; virtual capital input +
      ₹1L/₹5L/₹10L chips; start date; risk profile); and a shrink-0 pinned primary
      button at the BOTTOM (like Ask's composer). Button is "Update plan" for live
      (the right column already updates live on every input change) and
      "Build replay / Update replay" for a past date (that needs the point-in-time
      fetch).
    • **RIGHT = PORTFOLIO WORKSPACE** (`flex-1`, flex-col): shrink-0 header = title +
      a NEW compact horizontal **metrics strip** (Ask price-strip treatment: 7 cells
      — Starting · Current value · Total P&L · Return% · Open · Cash · Drawdown —
      with a deploy bar) + a compact disclaimer chip; then a `flex-1 min-h-0
      overflow-y-auto` scroll region with the allocation table → Falcon actions feed
      → Controls → AutoTrade bridge.
  - **Honesty PRESERVED 1:1.** Same `entryPriceOf` (action.entry_price_rs only, else
    "—" + the amber "live quote feed" banner), same REAL allocation math (weight =
    1+tilt·((N−1−i)/(N−1)), qty=floor(alloc/entry), SL = action.stop_loss_pct else
    locked −7%), same BAND-derived tier colors (never signal_tier_color). The metrics
    strip shows ONLY computable values as REAL (Starting, Open count + % deployed,
    Cash); Current value / Total P&L / Return% / Drawdown are dashed "—/pending".
    Actions feed still SEEDED from the real allocation decisions + the live-feed
    Backend-need note. Replay still attempts a point-in-time `falconTop20(startDate)`
    and shows the honest "replay · backend pending" card on no data. Disclaimer
    ("virtual capital · not financial advice") kept — as a chip in the right header
    (lg+) and as the full card above the workspace on smaller widths.
  - **Mobile:** middle column hidden < md (same as Ask); a compact fixed-bottom
    setup bar (style/risk/capital/date + a replay Build button) keeps it usable.
  - **Real source per panel (unchanged):** Setup = local state · Allocation =
    Top20Response.picks (REAL) + client math · Metrics strip = starting capital +
    computed allocated/cash (REAL) / live = Backend · Actions = derived from
    allocation (REAL) / live feed = Backend · Controls = local state / persistence =
    Backend · Bridge = /power/autotrade. Only Falcon Top 10 Swing live.
  - **Verify:** `cd frontend && npx tsc --noEmit` clean (EXIT 0); `npm run build` ✓
    compiled (`/power/co-trading` built).
- 2026-06-21 — **Co-Trading PHASE 1 built** (COTRADING.md Phase 1 §§1-7; spec
  "Phase 1 (this build)"). Additive, SCOPE-FENCED: only NEW files —
  `app/power/(app)/co-trading/page.tsx` (was a LaunchPending stub → now a server
  component seeding `PowerAPI.falconTop20('all500')` + the real session first name)
  and the NEW `components/power/cotrading/CoTradingExperience.tsx` client component.
  Did NOT touch AppShell / AskFalconHome / globals.css / any /power/ask file.
  - **Two stages, one screen.** SETUP (guided, four numbered blocks: trading style,
    virtual capital, start date, risk profile) → "Build my allocation" → PREVIEW
    (allocation table + actions feed on the left; portfolio summary + controls +
    AutoTrade bridge on the right). Viewport-first: root is
    `md:h-[calc(100dvh-7rem)]` (the AppShell renders /co-trading in the PADDED,
    NON-full-bleed `max-w-5xl pt-8 pb-16` main, so I subtract that chrome) with a
    single internal `overflow-y-auto` scroll region; the actions feed has its own
    `max-h-[260px]` inner scroll. F2 mint theme; tier bands derived from the BAND
    NAME via BAND_COLORKEY (GOLD=amber/PREMIUM=teal/ENTERPRISE=green/STANDARD=slate/
    AVOID=red), NEVER signal_tier_color.
  - **ALLOCATION RULE (documented, REAL math):** take the top-N picks where N =
    risk.maxPositions (Conservative 6, Balanced 8, Aggressive 10). Weight_i =
    1 + tilt·((N−1−i)/(N−1)) with tilt 0 / 0.5 / 1.0 (Conservative = EVEN spread;
    Balanced/Aggressive tilt toward better ranks), then normalise across the
    NON-removed names after applying each pick's user scale so freed capital
    redistributes. targetAlloc = weight·capital; qty = floor(targetAlloc/entry);
    capital = qty·entry. SL = the pick's `action.stop_loss_pct` if present, else the
    locked −7% standard (labelled). All of this is REAL (real picks + a real entry
    price + arithmetic).
  - **HONESTY held.** Entry price = `pick.action.entry_price_rs` ONLY (no other real
    close on the Top20Pick contract); when absent the row shows "—", qty 0, and an
    amber banner points at the live-quote Backend need — NEVER a fabricated price.
    Portfolio summary shows ONLY computable values (starting capital, positions,
    capital allocated + % deployed bar, cash available) as REAL; current value /
    total P&L / return % / max drawdown are dashed "—/pending" tiles with
    "tracking starts when you begin · market open pending". NO fabricated P&L. The
    actions feed is SEEDED from the real allocation decisions ("Allocate ₹X to SYM —
    N sh, GOLD tier, stop −7% (₹…), hold ~7d") and explicitly notes the live
    entries/exits/trailing/skip/rotation feed is a Backend need. Replay (past start
    date < IST today) attempts a point-in-time `falconTop20(signal_date)` re-fetch;
    if the engine has no picks for that date it shows an honest "replay · backend
    pending" card describing the no-look-ahead walk-forward sim that's needed —
    never invented.
  - **Controls (client state).** Per-row Why panel exposes Reduce 25% (clamped to
    25%), Remove/Restore (redistributes weight), and "Deeper analysis" →
    `/power/ask?symbol=SYM`. A "Pause new entries" toggle + "Change capital/risk"
    (back to setup) sit in the Controls card; copy states persistence is a Backend
    need. AutoTrade bridge CTA → `/power/autotrade` (labelled Launch-Pending).
  - **Real source per panel:** Setup = local state · Allocation = Top20Response.picks
    (REAL) + client math · Portfolio = starting capital + computed allocated/cash
    (REAL) / live = Backend · Actions = derived from allocation (REAL) / live feed =
    Backend · Controls = local state / persistence = Backend · Bridge =
    /power/autotrade route. Only Falcon Top 10 Swing live; other styles disabled.
  - **Verify:** `cd frontend && npx tsc --noEmit` clean. `npm run build` skipped per
    directive (concurrent agent editing the home; full build runs separately).
- 2026-06-21 — **Ask-Falcon home: VIEWPORT-FIRST fixed-height layout overhaul**
  (AppShell.tsx + AskFalconHome.tsx only; co-trading + all other files untouched —
  another agent owns those). Operator goal: at 100% zoom on 1366×768 / 1440×900,
  ZERO browser page scroll — the shell fills the viewport and each panel scrolls
  INTERNALLY only; the search bar + all 10 signals always visible. This was a real
  fixed-height structure pass, NOT a "shrink the fonts" pass.
  1. **Shell = viewport-locked.** AppShell fullBleed `<main>` `md:h-screen` →
     `md:h-[100dvh]` (overflow-hidden kept); removed-by-intent any extra main
     padding so the three columns share the exact same usable height with no white
     gap. Comment updated to say each region's top/padding is owned by AskFalconHome.
  2. **MIDDLE column = true fixed-height flex column** (`md:h-full md:min-h-0
     md:overflow-hidden`), three regions:
     • TOP (shrink-0): style selector `pt-2.5 pb-2`→`pt-2 pb-1.5`; list-header +
       one-line filters `pt-2.5 pb-2`→`pt-2 pb-1.5`; gap between filters and the
       engine/entry context line `mt-1.5`→`mt-1`.
     • MIDDLE (flex-1 **min-h-0**, the fix that makes internal-only scroll work):
       list container `py-1`→`py-0.5`; `ListRow` `py-1.5`→`py-1`, sector label
       `10.5px`→`10px` + `opacity .8` (smaller/lighter). ~25px/row → all 10 ≈ 250px,
       fits between filters and composer at 768px with headroom; inner scroll
       engages only on a smaller viewport.
     • BOTTOM (shrink-0, sticky): the F2 composer (Intent ▾ · Stock · Ask) now
       `shrink-0` so it is ALWAYS pinned at the bottom of the column, never pushed
       below the fold; `pt-2.5 pb-3`→`pt-2 pb-2.5`. Visual unchanged.
  3. **RIGHT column = fixed-height flex column** aligned to the middle's top
     (`md:h-full md:min-h-0 md:overflow-hidden`): FIXED header (shrink-0) =
     greeting + compact [Co-Trading | AutoTrade] CTA row (`pt-5/6 pb-3`→`pt-2/3
     pb-2`); SCROLLABLE region (flex-1 min-h-0 overflow-y-auto) = the analysis card.
     First no-scroll view now shows stock name+rank+tier, the metrics row, "Why
     we're picking this", and the start of "Historical track record".
  4. **PRICE CARDS → one compact horizontal metrics ROW.** Replaced the four
     bordered `py-2` cards with a single divided strip (Current LTP · Prev-day LTP ·
     AI Signal Day · Entry Day) inside one rounded border; cells `py-1.5`, labels
     `9px`, values `14px` mono, notes `8.5px`; the signal-day cell carries a faint
     mint wash (the only REAL value). On `<sm` it falls back to a clean 2×2 grid
     (theme-colored left/top cell borders, no bright default `divide`). Same data +
     honest "—" / "live quote pending" / "market open pending"; nothing fabricated.
  5. **CTA cards compact.** `px-3 py-2.5`→`px-2.5 py-2`, icon 7→6, gap/mt trimmed —
     action buttons, not tall blocks.
  6. **Greeting trimmed** to fit the fixed header: title `24px`/logo 26 →
     `20px`/logo 22, subtitle `14px mt-1.5 leading-relaxed`→`12.5px mt-1
     leading-snug`. DetailCard `p-5 rounded-[18px]`→`p-4 rounded-[16px]`; `Section`
     `mt-3.5`/`13.5px`/`mb-1.5`→`mt-3`/`13px`/`mb-1`; `Rule` `my-3`→`my-2.5`.
     Everything else preserved (logo, tiers/bands, REAL falconTop20 wiring, composer
     intents, greeting copy, all honest placeholders).
  - **Verify:** `npx tsc --noEmit` clean (EXIT 0). `npm run build` deliberately NOT
    run — another agent is editing concurrently; a full build is run separately.
- 2026-06-21 — **Ask-Falcon home: STRICT MINIMAL 2-fix round** (AskFalconHome.tsx +
  AppShell.tsx only; everything else preserved — middle column / all 10 rows,
  one-line filters, F2 composer visual, right-column CTAs + analysis card, logo,
  tiers, greeting, REAL data wiring). Scope was exactly these two:
  1. **No more whole-page scroll — viewport-locked shell, columns scroll
     independently.** Before: AppShell fullBleed `<main>` was `flex-1 min-h-0`
     (no height cap) and the right column was one `overflow-y-auto` div wrapping
     greeting + CTA row + analysis card → the WHOLE page scrolled to read the
     analysis. After: (a) AppShell fullBleed main = `flex-1 min-h-0 pt-14 md:pt-0
     md:h-screen md:overflow-hidden` (md+ viewport-locked, mobile stays scrollable);
     (b) AskFalconHome root `h-full min-h-screen md:min-h-0 md:h-screen` →
     `min-h-screen md:min-h-0 md:h-full md:overflow-hidden` (fills the locked main
     exactly); (c) RIGHT column rebuilt as a flex-col: a FIXED header block
     (greeting + [Co-Trading|AutoTrade] CTA row, `shrink-0`) then a SCROLLABLE
     analysis region (`flex-1 min-h-0 md:overflow-y-auto`, thin scrollbar) holding
     the DetailCard / StockAnalysis / mobile fallback. Left nav + middle Top-10
     now stay fixed; only the analysis pane scrolls. (d) Tightened the detail card
     modestly so header + tier badges + price grid + "Why we're picking this" are
     visible without scrolling: card `p-6 mt-6`→`p-5 mt-1`, badges row `mt-3`→`mt-2.5`,
     `Section` `mt-5`/`mb-2`/`14px`→`mt-3.5`/`mb-1.5`/`13.5px`, `Rule` `my-[18px]`→`my-3`,
     `PriceCell` `px-3 py-2.5`→`px-2.5 py-2`. No data/typography redesign beyond this.
  2. **Restored the full guided-intent dropdown in the F2 composer.** Before: the
     restored F2 composer hard-coded Intent to a single fixed "Analyze a stock"
     span. After: re-added the original 9-intent closed list (Analyze a stock ·
     Analyze a sector · Explain today's Top 10 · Find setups beyond Top 10 ·
     Compare two stocks · Why is X Gold/Enterprise · Review my portfolio · Market
     & sector strength · Check AutoTrade readiness) as an `INTENTS` config + a
     dropdown styled exactly like the F2 Intent cell (label + chevron, mint-tinted
     selected, "soon" tag on non-live items, disabled). Composer VISUAL unchanged
     (Intent ▾ · Stock · Ask →). The Stock field is now shown only for
     stock-needing intents and still searches the full Nifty 500 (unchanged
     `/api/power/universe/symbols` + graceful 404 fallback). LIVE intents:
     "Analyze a stock" (→ DetailCard for Top-10 / StockAnalysis "soon" otherwise)
     and "Explain today's Top 10" (→ selects #1, shows the pick workspace) run
     against the already-loaded REAL Top20Response; all others are disabled "soon".
     Ask is disabled unless the intent is live and (if it needs a stock) one is
     chosen → invalid/empty requests stay impossible.
  - **Verify:** `npx tsc --noEmit` clean (full build skipped to stay fast, per
    directive).
- 2026-06-21 — **Ask-Falcon home: STRICT MINIMAL "all 10 must fit" round**
  (AskFalconHome.tsx only; everything not listed preserved — logo, "Choose Your
  Trading Style" rename, F2 composer, right-column analysis/price card, tier
  colors, greeting, one-line filters; data wiring stays REAL `falconTop20`).
  PRIMARY GOAL = all 10 Top-10 signals visible at once, no inner scroll, on a
  ~720px content-height laptop (was only ~5 rows).
  1. **Rows collapsed to a SINGLE tight line** — `ListRow` was a 2-line cell
     (symbol over sector) at `px-2.5 py-2.5` ≈ 56–58px/row → 10 rows = ~580px
     (off-screen). Now: rank · symbol with **sector as a dim inline suffix** ·
     tier badge · signal-day %, all on one baseline-aligned line at `px-2 py-1.5`
     ≈ 32–34px/row → 10 rows = ~330px. Grid cols `18px→16px`, gaps `2.5→2`.
     Same REAL fields (rank, symbol, sector, signal_tier band, flags.day_return_pct).
  2. **Removed the middle-column "Start Co-Trading" CTA entirely** (freed ~64px).
  3. **Tightened everything above the list** so the list dominates: style selector
     `pt-4 pb-3`→`pt-2.5 pb-2`, button `py-2.5`→`py-1.5` + icon 28→24px (~28px
     saved); list-header block `pt-3 pb-2.5`→`pt-2.5 pb-2`; context line
     `mt-2 text-[11px]`→`mt-1.5 text-[10.5px]`; list container `pt-2`→`py-1`;
     composer field padding `py-1.5`→`py-1`, top pad `pt-3`→`pt-2.5`.
     Net middle budget (~720px): selector ~64 + header/filters/context ~110 +
     10 rows ~330 + composer ~74 ≈ 580px → all 10 fit with headroom, no scroll.
  4. **RIGHT column: two compact CTA cards added** — `[Start Co-Trading | Create
     AutoTrade]` side-by-side `grid-cols-2`, placed directly under the greeting
     subtitle and ABOVE the analysis/detail card (compact `px-3 py-2.5`, ~46px
     tall, doesn't push analysis far down). Route to `/power/co-trading` and
     `/power/autotrade` (both routes verified to exist). New `bot` icon added to
     the ICON map. Mint theme, no new accent.
  - **Column contract now exact:** MIDDLE = style selector + one-line filters +
    Falcon Top 10 (all 10) + stock-search composer, nothing else. RIGHT =
    greeting + [Co-Trading | AutoTrade] CTA row + analysis/price card + explanation.
  - **Verify:** `npx tsc --noEmit` clean; `npm run build` ✓ compiled
    (`/power/ask`, `/power/autotrade`, `/power/co-trading` all built).
- 2026-06-21 — **Ask-Falcon home: STRICT 3-change minimal round** (AskFalconHome.tsx
  only; no redesign/restyle/respacing — smallest diffs, data wiring unchanged).
  Operator was frustrated by over-building; scope was exactly these three:
  1. **Filters collapsed to ONE compact line** — the 5 universe pills (which wrapped
     to 2 lines) + a second EOD/Sector row were pushing the Top-10 list below the
     fold (only ranks 8–10 visible). `FilterBar` now renders universe as a single
     compact `<select>` alongside the EOD-date and Sector `<select>`s on one
     `flex-nowrap` row. Vertical footprint cut from ~3 rows to 1; the Top-10 list is
     now the dominant element. Still drives the REAL
     `PowerAPI.falconTop20(universe, sector, signal_date)` refetch — functionality
     identical, only layout compacted. "Engine emitted at … qualified for entry on …"
     context line kept as the single small line under it.
  2. **Co-Trading CTA relocated to the TOP** — moved the compact "Start Co-Trading"
     card out of the bottom of the middle column to directly UNDER the "Choose Your
     Trading Style" selector (above the Falcon Top 10 heading/filters). Same card,
     same `/power/co-trading` route; only `m-3.5 mt-2` → `mx-3.5 mt-3` for the new
     position.
  3. **Reverted the plain search box to the F2 guided composer** — the prior round
     replaced the F2 "INTENT [▾] STOCK [field] [Ask →]" bar with a plain "Type a
     name or symbol" box; restored the F2 composer visual (.f2-gp/.f2-gsel/.f2-ask
     look: bordered shell, Intent + Stock fields, mint Ask button). ONLY the STOCK
     field's FUNCTIONALITY differs from the static F2: it searches the full Nifty 500
     (type name/symbol → live suggestions, selectable) via
     `GET /api/power/universe/symbols` with the SAME graceful 404 fallback (loaded
     Top-10 + honest "coming soon"). Intent is fixed "Analyze a stock" (guided,
     closed list → invalid request impossible). Ask renders the chosen stock in the
     RIGHT column (Top-10 → real DetailCard; non-Top-10 → StockAnalysis "soon").
  - **Untouched (per directive):** CompassLogo, the "Choose Your Trading Style"
     rename, the right-column analysis workspace, the price-breakdown card, tier
     colors, the greeting — all left exactly as they were.
  - **Verify:** `npx tsc --noEmit` clean; `npm run build` ✓ Compiled successfully.
- 2026-06-21 — **Ask-Falcon home: 6-item operator feedback round** (AskFalconHome.tsx,
  data wiring stays REAL — no mock). Acting on the 2026-06-21 worktree directive.
  1. **Logo fix** — replaced the Claude-style "✴" spark in the right-column greeting
     with the real Kanida brand mark (`CompassLogo` — the same compass/dial used in
     the left nav). Consistent branding across the screen.
  2. **Rename** — middle-column label "Trader persona" → **"Choose Your Trading Style"**.
  3. **Filters in the middle column** — brought the Power-User-Portal filters in under
     the "Falcon Top 10" heading (reusing Top20Filters' semantics, adapted to the F2
     panel + client refetch): universe toggles (Nifty 500 / Nifty 50 / 100 / 200 / F&O),
     an EOD date dropdown, a Sector dropdown. They drive a REAL client re-fetch of
     `PowerAPI.falconTop20(universe, sector, signal_date)` (endpoint already supports all
     three params). Page seeds the first response server-side; filter changes refetch in
     the browser. Context line kept/improved: "Engine emitted at <signal_date> EOD ·
     qualified for entry on <entry_date> · <universe> [· <sector>]" — all REAL Top20Response
     fields. Component switched from server-prop-only to a client filter container.
  4. **Signal-row price data** — the MIDDLE list stays clean (rank · symbol · sector ·
     tier · signal-day %). Full price breakdown moved to the RIGHT detail card as a
     4-cell grid: Current LTP · Previous-day LTP · **AI signal day (<signal_date>)** ·
     Entry day. Only signal-day %=`pick.flags.day_return_pct` is REAL (mint-ringed cell);
     Current LTP / Prev-day LTP = honest "—", Entry day = entry_date or "market open
     pending". NONE fabricated → all added to Backend needs.
  5. **Co-Trading CTA** (operator option 1) — one compact "Start Co-Trading" card low in
     the middle column routing to `/power/co-trading` (the full capital/date/allocation
     workflow stays on that page, still LaunchPending). No flow added to the middle column.
  6. **Search = full Nifty 500 + output in RIGHT column** — added a searchable stock
     picker (type name OR symbol → live-filtered suggestions) calling
     `GET /api/power/universe/symbols`. That endpoint does NOT exist yet → graceful 404
     handling: falls back to the loaded Top-10 symbols + an honest "full-universe search
     coming soon" note. Picking a stock renders in the RIGHT column (analysis workspace),
     NOT the middle. Top-10 picks render the REAL DetailCard from loaded data; a non-Top-10
     stock renders a StockAnalysis card that calls `GET /api/power/ask/analyze-stock?symbol=`
     (also not live → honest "analysis coming soon" with the full section scaffold: current
     trend, recent price/volume, signal-day, sector, Falcon patterns, tier eligibility,
     entry context, risk warnings, plain-English explanation). No fabricated numbers.
  - **Layout invariant held:** Left=nav · Middle=style + filters + Top 10 + search +
     Co-Trading CTA (focused) · Right=analysis workspace. Mint theme + F2 tokens preserved;
     no new accent. The old GuidedPrompt intent/Ask box was retired — the searchable picker
     IS the (still guided, closed-list) entry now, so invalid requests stay impossible.
  - **Verify:** `npx tsc --noEmit` clean; `npm run build` ✓ Compiled successfully; all
     legacy `/power/*` routes intact (additive). Removed the page→component "guided prompt"
     plumbing; page.tsx still seeds the first falconTop20 server-side.
- 2026-06-20 — **Ask-Falcon home rebuilt as F2 master-detail, 100% REAL data**
  (handoff_ask_falcon_home + the 2026-06-20 F2 directive). Replaced the Phase-1a
  centered hero with the 3-column F2 layout. ONE shell: the AppShell left rail IS
  the F2 nav; AppShell `<main>` now renders full-bleed ONLY on `/power/ask` (other
  modes keep the centered padded column).
  - **AskFalconHome.tsx** (new, client): middle = persona selector → Falcon Top 10
    list → guided prompt; right = IST greeting + explanation card. Wired to ONE real
    call (`PowerAPI.falconTop20('all500')` → Top20Response, fetched server-side in
    page.tsx). Mappings: list row = `rank·symbol·sector·signal_tier(+color)·flags.day_return_pct`;
    card Why = `bucket1.synthesis` + `bucket1.total_fires_today`; track record =
    `per_stock_backtest` (honest empty when n_trades==0); sector = `bucket3.narrative`;
    setup-quality badge derived from `signal_tier`/`rating` (NOT a buy call).
  - **Honest swaps applied:** real engine tiers GOLD/PREMIUM/ENTERPRISE/STANDARD/AVOID
    (compound labels like PREMIUM-Pullback collapsed to band; invented Gold+/Ent.Cand
    dropped); "STRONG BUY" → tier-derived quality language; greeting market-pulse line
    is a NEUTRAL honest sentence (no invented commentary) pending /market-pulse.
  - **NO mock data anywhere:** deleted the orphaned Phase-1a subtree (AskHomeClient,
    GreetingHero, FalconComposer, ActionChips, **Top10Peek w/ its STUB_ROWS**,
    EntityPicker, AskResultPanel, composer-config). Backend-down → honest loading/empty
    states, not stubs.
  - **Personas:** only Swing live (real falconTop20). BTST/Intraday/Weekly/Long-Term
    selectable but render a Launch-Pending state in BOTH the middle list and the right
    panel — no faked re-ranking.
  - **Guided prompt:** "Explain today's Top 10" + "Analyze this Top-10 stock" are LIVE
    (answer from loaded Top20Response, no LLM/free-text). analyze-any/sector/compare/
    why-tier/portfolio/autotrade disabled with a "soon" tag (Backend needs). Stock field
    = the selected real pick only (no free-text → invalid request impossible).
  - **Tokens:** added F2 palette (--f2-canvas/panel/card/mint/ink/…/amber/teal/red) to
    globals.css :root, locked to the handoff table. No new accent — mint stays the only one.
  - **Verify:** `npx tsc --noEmit` clean; `npm run build` compiled successfully; all other
    `/power/*` routes intact (additive). Resolved the prior TierColor/PREMIUM(teal) note
    by mapping signal_tier_color directly in the F2 badge (teal=PREMIUM) — no change to
    the v1 TierColor union needed.
- 2026-06-20 — **Phase 1a built** (spec §§1,2,3,8). Shipped the AppShell + Ask-Falcon
  home + LaunchPending, additively under route group `app/power/(app)/*` (URLs
  `/power/ask|signals|co-trading|autotrade|performance|plans|account|learn`). Legacy
  `/power/today|live|replay|admin|…` untouched.
  - **AppShell** (`components/power/shell/`): persistent left rail, 6 modes + footer
    (Account/Learn/Admin, Admin operator-only), mint active pill + accent bar, mobile
    drawer. Launch-Pending modes carry a subtle "Soon" tag in the nav. Inline SVG icons
    (no icon lib in package.json). Nav is a single config (`nav-config.tsx`).
  - **Chrome de-dupe decision:** parent `app/power/layout.tsx` now passes shell routes
    through raw (detected via new `x-pathname` middleware header) so the public TopBar/
    Footer don't double-wrap the AppShell. Middleware change is read-only/additive — no
    auth behavior change.
  - **Ask-Falcon home** (`components/power/ask/`): IST-aware greeting (client clock,
    first_name from display_name→email→"trader"); ONE rotating market-pulse line (STUB,
    marked in-UI); **guided composer** = intent dropdown → contextual entity picker
    (searchable, closed-list only → invalid request impossible) → Ask → focused STUBBED
    result panel (no page-hop). 9 intents per §3, each carrying its backend-source marker.
    Action chips seed the composer intent or route to a mode. **Top-10 peek** = real
    `PowerAPI.falconTop20` data (4 compact rows, signal-tier chip, day return), falls
    back to a clearly-marked stub on fetch failure.
  - **Honesty:** AutoTrade intent + 4 modes (Co-Trading/AutoTrade/Performance/Plans) and
    Account/Learn are LaunchPending with explicit "why not yet"; Signals is a thin REAL
    shell pointing at the live `/power/today`. Only Falcon Top 10 Swing (+Weekly) framed
    as live. No "80%"/"smartest"/"500 agents". Tiers = qualitative bands.
  - **Verify:** worktree has no node_modules — couldn't run next/tsc against project types.
    Syntax-checked all 21 new/edited TS(X) files with standalone tsc (`--noResolve`); zero
    syntax errors (only expected unresolved-module/React-namespace noise, which matches the
    codebase's existing `React.ReactNode`-without-import pattern in UserMenu).
  - **Open for 1b:** persona-aware Signals + 7/14/20/30d tracker; paywall re-wire; wire
    composer intents + market-pulse + Top-10 peek tier color to real endpoints (handoff
    list in the run output's Backend-needs block). TierChip in the peek maps a `teal`
    color (PREMIUM) not yet present in the v1 `TierColor` union — confirm backend
    signal_tier_color values for GOLD/ENTERPRISE/PREMIUM/STANDARD/AVOID.
- 2026-06-19 — Log + falcon-ui agent created. No screens built yet. Next: Phase 1a.

- 2026-06-20 — **F2 design adopted as the Ask-Falcon home LAYOUT REFERENCE ONLY**
  (docs/design/handoff_ask_falcon_home/). HARD RULE from operator: rebuild it with
  100% REAL data/APIs/routes — NO mock/static content anywhere. Mapping: the home
  (middle Top-10 list + right explanation card) wires to the EXISTING
  `PowerAPI.falconTop20('all500')` -> Top20Response (tier=signal_tier, day%=flags.day_return_pct,
  why=bucket1, track record=per_stock_backtest, sector=bucket3, signals=n_fires). Greeting
  name from /auth/me. Honest swaps: use REAL engine tiers (GOLD/PREMIUM/ENTERPRISE/STANDARD;
  drop invented Gold+/Ent.Candidate); REPLACE "STRONG BUY/BUY/ACCUMULATE" with tier-derived
  setup-quality language (NOT a buy call — not financial advice). Personas: only Swing live
  (real falconTop20); BTST/Intraday/Weekly/Long-Term = Launch-Pending (do NOT fake re-ranking).
  Any field not in the real API (e.g. company name, 0-100 conviction, 30d return) -> omit or
  honest empty state + add to Backend needs; never fabricate.

- 2026-06-20 — **F2 VISUAL feedback round on AskFalconHome.tsx** (data wiring untouched,
  no mock reintroduced). Fixed the "flat/grayscale" rendering by restoring F2's vivid
  color + depth. (1) TIER BADGES now use the FULL F2 token hexes (cite README "Design
  Tokens" + .f2-badge): GOLD=`--f2-amber #E6B450`, PREMIUM=`--f2-teal #4BCBE0`,
  ENTERPRISE=`--f2-tier-green #3FE3A4`, STANDARD=`--f2-slate #8595a0`, AVOID=`--f2-red`
  — each as matching-color text on a faint same-hue fill (~0.14α) with an inset same-hue
  ring (`box-shadow: inset 0 0 0 1px <ring>`), 6px radius (`rounded-md`), uppercase
  0.04em. Previous code desaturated these (amber rendered as `#f0d089`) which read gray;
  removed. (2) MINT accents confirmed/kept: list day-% mint mono+tabular (red only on a
  down-day for honesty), selected row = `--f2-mint-dim` bg with mint rank, greeting ✴
  mint, glowing "today" dot. (3) RIGHT CARD depth already correct (gradient card→card-2,
  line-2 border, r18, `0 24px 70px -34px` shadow, mint mono `#rank`, .f2-badge
  setup-quality chip with inset mint ring). (4) Added `tabular-nums` to all mono numerics
  (rank, day-%). Honesty held: most picks legitimately STANDARD/slate, a couple GOLD/amber
  — colors only vivified, none invented. `tsc --noEmit` + `next build` both green.

- 2026-06-20 — **BUGFIX: tier badges rendered gray (looked grayscale despite F2 pass).**
  Root cause: TierBadgeF2/DetailCard looked up TIER_STYLE by `pick.signal_tier_color`,
  but the backend's signal_tier_color semantics differ (GOLD->"yellow", PREMIUM->"amber"),
  and TIER_STYLE had no "yellow" key -> GOLD fell back to gray (and PREMIUM would've gone
  amber not teal). Fix: added BAND_COLORKEY {GOLD:amber, PREMIUM:teal, ENTERPRISE:green,
  STANDARD:gray, AVOID:red} and key the style off the tier BAND (tierBand(signal_tier)),
  not signal_tier_color. Verified: GOLD now amber, mint accents present, tsc clean, dev
  hot-reloaded. LESSON for future runs: NEVER style tiers off signal_tier_color (it uses
  yellow/amber differently) — always derive from the canonical band.

- 2026-06-20 — **ROOT-CAUSE FIX: F2 colors never rendered (looked black & white).**
  The --f2-* tokens were added as a SEPARATE bare `:root {}` block AFTER Tailwind v4's
  `@theme` in globals.css — and the bundler (Turbopack/Lightning) DROPPED that block, so
  NONE of the --f2-* custom properties existed in the compiled CSS. Every var(--f2-*) text
  color fell back to white (mint %, red negatives, slate, etc.); only literal rgba()/hex
  (e.g. badge backgrounds) showed, which is why GOLD "looked amber" (bg only) but the % were
  white. FIX: moved ALL --f2-* defs into the FIRST compiling `:root` (the one with
  --background, proven by dark-mode working). Verified: compiled CSS chunk now contains
  --f2-mint/#3fe3a4 etc. LESSON: in this Tailwind v4 setup, define CSS custom properties in
  the existing top `:root` (or `@theme`), NEVER as a new bare `:root` after `@theme` — it
  won't compile. (Orphan dead block still in globals.css ~L60; harmless, remove on next edit.)

- 2026-06-23 — **AutoTrade operator-console LAUNCHER (role-branched /power/autotrade).**
  Spec section: AutoTrade mode. AutoTrade is now LIVE *for operators* and remains an honest
  launch-pending preview for everyone else. The real, live Falcon operator console (app/falcon/*:
  Trade / Pre-Market / Positions / Config / Admin + the /falcon overview) is NOT moved — it stays
  behind the site-wide HTTP Basic Auth, OUTSIDE the /power invite-auth zone, by design. We only
  added a launcher that LINKS to it. Changes: (1) NEW
  components/power/autotrade/AutoTradeConsoleHub.tsx — operator-only mint/F2 card grid (6 cards:
  /falcon overview, /falcon/trade, /falcon/premarket, /falcon/positions, /falcon/config,
  /falcon/admin), each title + one-line purpose + "Open console →". Plain <a href> (full
  navigation out of the (app) group into the Basic-Auth zone), NOT Next <Link>. Reuses cotrade-kit
  C palette + ICON set + Gear/MECHANISM_CSS so it reads as one product family. Honest header note
  ("Your live AutoTrade engine. These open the operator console — separately signed in") + footnote
  (links place no orders / read no live data here). (2) app/power/(app)/autotrade/page.tsx now
  branches on isOperator = user.role === 'admin' (same check the shell uses for isAdmin): operator →
  AutoTradeConsoleHub (NO /api/falcon fetch, links only); non-operator → unchanged
  AutoTradeExperience (falconTop20('all500') preview only — the launch-pending waitlist UX).
  (3) nav-config.tsx: autotrade live false→true (it now does something real for operators; the nav
  drops its "Soon" tag; non-operators still land on the honest launch-pending page). SAFETY held:
  zero /api/falcon/* calls from the /power zone, execution path / app/falcon/* pages untouched —
  pure navigation UI. Verify: `tsc --noEmit` clean; `npm run build` green (/power/autotrade compiles
  as a dynamic route).

- 2026-07-04 — **Auto-Ladder ("Monthly Campaign") — Falcon Positional Auto-Ladder UI.**
  Spec: AutoTrade mode (new "set once, run for a month" campaign; backend already DEPLOYED,
  fixed API contract). Built a NEW top-level tab **Auto-Ladder** in
  `components/power/autotrade/AutoTradePanel.tsx`, sibling to Sessions (icon: ICON.loop). A
  running campaign's child baskets remain ordinary sessions (carry ladder_id) and stay in the
  Sessions tab — this tab is the higher-level view. Changes: (1) `lib/autotrade-api.ts` — added
  the ladder types (LadderProduct CNC|MTF, LadderEndMode, LadderKillMode, LadderStatus/-Summary/
  -Session/-Alert, all OPTIONAL-SAFE) + 7 methods on AutoTradeAPI: ladderCreate/ladderStart/
  ladderPause/ladderResume/ladderKill(mode)/ladderStatus/ladders(userId), all via the existing
  `call()` helper + BASE `/api/falcon-proxy/api/autotrade` (operator token + power_jwt injected by
  the proxy). (2) NEW `components/power/autotrade/AutoLadderPanel.tsx` — list (your campaigns,
  re-open a running one) → setup form → live panel. Setup: total-capital input with live "Each
  basket ≈ ₹{total/3}" beneath it; Product = CNC|MTF segmented ONLY (NO MIS — backend 400s it);
  Duration radio (This month auto / Until I stop) + optional explicit date; Mode paper(default)/
  live behind the same typed-LIVE confirm + ships-disabled warnings as the rest of the panel;
  Start → ladderCreate then ladderStart → live panel. Live panel polls ladderStatus every ~5s:
  Capital deployed/free/total (+per-basket), Active baskets · Open positions, Daily/Realized/
  Unrealized P&L (theme +/- coloured), status pill (RUNNING/PAUSED/ENDED/COMPLETED) + "runs to
  {end_date}", a calm amber DOWNTURN note rendering alert.message VERBATIM (informational, NOT an
  error) with optional trailing_5d_avg_return, and the child-baskets list from sessions[] (each an
  `<a href="?attab=sessions&session=…">` full-nav deep-link that lands on the Sessions tab via a new
  `?attab=` initial-tab reader). Controls: Pause/Resume + KILL modal that REQUIRES choosing a mode
  ("Flatten everything now" vs "Stop opening new — let open baskets finish"; no silent default,
  since the API needs it). HONESTY held: all numbers from the live status, missing fields render
  "—", calm retry on status failure (mirrors Sessions), no fabricated P&L. TRADER TERMS ONLY —
  the word "sleeve"/internal terms never appear (capital deployed / active baskets / open
  positions / daily P&L). SAFETY: UI/transport only; no execution/backend logic touched. Verify:
  `npx tsc --noEmit` clean; ESLint clean on the three touched files.

- 2026-07-04 — **Auto-Ladder MERGED INTO Sessions — separate tab REMOVED (supersedes the
  entry above).** Operator feedback: no separate UI surface — Auto-Ladder must be a STRATEGY
  option inside the existing Sessions create form (reuse the config form the trader already
  knows; new UI has caused bugs). Changes: (1) `components/power/autotrade/AutoTradePanel.tsx`
  — deleted the "Auto-Ladder" tab + its `AutoLadderPanel` import/render; Sessions is the single
  home. `AutoLadderPanel.tsx` DELETED (its card/controls rendering re-created inside Sessions).
  (2) `components/power/autotrade/PortfolioAutoTrade.tsx` — added a 3rd dropdown option **"Falcon
  Positional — Auto-Ladder (monthly campaign)"** as a UI CONSTRUCT (UiStrategy `'auto_ladder'`;
  NOT a backend strategy). Selecting it maps internally to a POSITIONAL `intraday_basket`
  (POSITIONAL_TRAIL preset arm3/floor1/give4/stop6, square_off_enabled:false, max_hold 3, EQ,
  CNC/MTF) and REUSES the existing config form with only the specified deltas: capital relabelled
  "Total campaign capital" + live "Each basket ≈ ₹{total÷3}" helper; product restricted to
  CNC|MTF (no MIS); the SAME arm/floor/giveback/stop inputs seeded to the positional preset;
  NO Hold toggle / NO square-off / NO max-hold surfaced; Instrument/Direction/Entry-time/Entry-
  date/Missed-window hidden; NEW **Duration** control (This month → month_end / Until I stop →
  manual); SizingBreakdown/preview computed on the PER-BASKET slice (total÷3) so the trader sees
  one day's basket. Primary button becomes **"Start campaign"** → `ladderCreate({total_capital,
  order_product, mode, end_date_mode, kill_mode:'flatten_now'})` then `ladderStart(id)` (NOT
  session/create); on success returns to the list. (3) Running campaigns render as **summary
  cards ATOP the same Sessions list** (one per running/paused ladder from `ladders(userId)`,
  each polling `ladderStatus` ~5s): deployed/free/total (+per-basket), active baskets, open
  positions, daily/realized/unrealized P&L (theme +/- coloured), status pill, "runs to
  {end_date}", the VERBATIM amber downturn banner when `alert.active`, and Pause/Resume + Stop
  (kill modal that REQUIRES flatten_now vs stop_new_let_finish — no silent default). (4) The
  campaign's child baskets appear as NORMAL rows in the Sessions list, each tagged with a small
  mint "Campaign" chip (read from the new `ladder_id?` field added to `SessionSummary` in
  `lib/autotrade-api.ts`). HONESTY held: all numbers from live status, "—" on missing, calm
  retry on error (mirrors the sessions list), no fabricated P&L. TRADER TERMS ONLY — "sleeve"/
  internal terms never appear. Fixed + Dynamic-Trailing flows unchanged. SAFETY: UI/types only;
  no execution/backend logic touched. Verify: `npx tsc --noEmit` clean; `next build` — "Compiled
  successfully", no errors/warnings.

- 2026-07-04 · **Auto-Ladder campaign: split into create → Start-now / Schedule two-step**
  (mirrors the single-session created-phase). Acting on the backend contract where
  `POST /ladder/create` now yields a **CREATED** draft (spawns nothing) and
  `POST /ladder/{id}/start` accepts an OPTIONAL `{ start_date }` (omit → RUNNING;
  future trading day → SCHEDULED; weekend/holiday → HTTP 400 with an OBJECT detail
  `{ message, suggested_date, code:'NON_TRADING_START_DATE' }`). Changes: (1)
  `lib/autotrade-api.ts` — `ladderStart(id, startDate?)` POSTs `{ start_date }` only
  when a date is passed (no-body call unchanged, backward-compatible); the shared
  `call<>()` error path now handles an OBJECT `detail` — throws `Error(detail.message)`
  and attaches `(err as any).suggested_date`/`.code`/`.detail` (STRING details behave
  exactly as before); added CREATED/SCHEDULED to `LadderStatusName`. (2)
  `PortfolioAutoTrade.tsx` — the Auto-Ladder primary button is now **"Create campaign"**
  → `onCreateCampaign` (ladderCreate ONLY → CREATED draft held in new `createdLadder`
  state → `phase==='created'` campaign card, guarded `!session` so it never collides with
  the session created-phase). New **campaign CREATED phase**: "Campaign created" card +
  **Start now** (`ladderStart(id)` → RUNNING) and a **Schedule** column with an inline
  `<input type="date" min={tomorrowIST()}>` + **"Schedule for {date}"** (`ladderStart(id,
  date)` → SCHEDULED); a non-trading-day 400 shows an amber "That's not a trading day —
  use {suggested_date}?" with one-click apply (mirrors `createSuggest`). On success sets
  `ladderNotice` (RUNNING → "Campaign started…"; SCHEDULED → "Campaign scheduled — first
  basket 09:15 on {date}"), `backToList()`, `loadLadders()`, clears `createdLadder`;
  Discard/Back resets the draft. (3) `liveLadders` filter widened to include **SCHEDULED**
  (CREATED drafts deliberately excluded — a draft lives only in the transient created-phase
  card). (4) `LadderCampaignCard` renders SCHEDULED clearly: amber "Scheduled" pill (via
  `LadderStatusPill`) + "starts {start_date}" header line; Pause/Resume hidden for SCHEDULED,
  **Cancel campaign** (kill) still offered; footer note explains it activates on its start
  date. TRADER TERMS ONLY; all numbers from live status; calm retry on error. Fixed +
  Dynamic-Trailing + the campaign config form unchanged. SAFETY: front-end/types only — no
  execution/backend logic touched. Verify: `npx tsc --noEmit` clean; `next build` — compiled
  successfully, no errors.

- **2026-07-05 — Broker onboarding rebuilt into a guided, data-driven 3-screen flow
  (AlgoTest-style; replaces the single dropdown+form).** Acting on the operator brief to
  make broker-connect "as easy as eating a donut" and to consume the extended
  `GET /brokers/supported` CONTRACT. (1) `lib/autotrade-api.ts` — added `FieldDef`
  (`name/label/type/secret/required/placeholder/maps_to`), `BrokerBrand`, `BrokerSetup`
  (`docs_url/callback_url/steps/token_note`), and extended `SupportedBroker` with
  `display_name/brand/exchanges/fields/setup`; exported `BrokerMeta` alias. Every field
  OPTIONAL-safe. NO API client method or lifecycle changed. (2)
  `components/power/autotrade/BrokerAccountsPanel.tsx` — full rewrite. SCREEN 1 GALLERY:
  searchable grid of broker cards (CSS brand chip from `brand.color`+`initial`,
  display_name, exchange chips, "Connected: N" from `brokerAccounts`, Set up/Add account
  for live, muted "Coming soon" for `live:false`) + a "N connections" count. SCREEN 2
  GUIDED SETUP: two-pane — LEFT numbered `setup.steps`, copyable `callback_url`, "View
  docs", `token_note`, and the egress/allowlist IP helper (self-contained
  `EgressIpHelper`, since the exported one lives inside PortfolioAutoTrade); RIGHT
  Connection name + DYNAMIC `fields` from the schema (secret fields get a Show/Hide toggle),
  one "Test & Connect" running the UNCHANGED create→login-popup→paste request_token→activate
  lifecycle with step-by-step inline status (Saving… → Opening broker login… → paste-token →
  Activating… → back to gallery). Coming-soon brokers render steps but disable connect.
  SCREEN 3 YOUR CONNECTIONS: per-account cards (brand chip + label + status pill ACTIVE mint/
  EXPIRED·ERROR·REVOKED red/PENDING amber, "Last verified …") with Generate token/Reconnect
  (EXPIRED → prominent mint CTA + "Token expired — one click to renew."), Health-check,
  Delete. ADMIN: new `isAdmin` prop (threaded from `AutoTradePanel`; power users default
  false → own accounts only) adds an "All users" toggle → compact table (broker · user ·
  label · status · last verified) with per-row Health-check + Delete, using the unscoped
  admin list (empty `user_id` → `q()` drops it). Defensive: partial/absent contract falls
  back to a static enrich registry + the legacy API-key + API-secret schema; `fields` never
  empty for live brokers. api_secret stays WRITE-ONLY — snapshotted, sent once, cleared from
  state on submit, never rendered. Vault-disabled empty state preserved. (3)
  `AutoTradePanel.tsx` — passes `isAdmin` to the panel. Mint/F2 theme; no new accent colours.
  SAFETY: front-end + types only — no execution/backend logic touched. Verify: `npx tsc
  --noEmit` clean; `next build` — compiled successfully, no errors.

## 2026-07-05 — Broker OAuth auto-capture landing (no more copy-paste)
- **What:** Replaced the manual `request_token` copy-paste in the broker connect flow
  with an auto-capture redirect landing, keeping the manual paste as a visible FALLBACK
  (never regressed the working flow).
- **New route `/power/autotrade/connect`** (`app/power/autotrade/connect/page.tsx`, client).
  DELIBERATELY placed OUTSIDE the `app/power/(app)/*` route group so it does NOT inherit
  the AppShell left-rail or the hard login redirect — an OAuth popup landing must be a
  calm, self-contained page that runs → reports to its opener → closes. Parent
  `app/power/layout.tsx` already passes `/power/autotrade/*` through without TopBar/Footer.
  On mount: reads token (`request_token`|`auth`|`code`|`token`) + `status` from the URL and
  the pending `{broker_account_id,user_id,broker}` from `localStorage['kanida.brokerConnect']`,
  calls `AutoTradeAPI.refreshBrokerToken`. SUCCESS → `postMessage` opener (explicit origin,
  never `'*'`), "Connected ✓", clear LS, `window.close()` after ~1.2s (or a "Back to
  AutoTrade" link if opened directly with no opener). ERROR → clear message + read-only token
  box with Copy + "paste this back in AutoTrade" so a failed auto-activate still recovers
  manually. Missing token/id → friendly guidance, no crash. Never renders/logs secrets beyond
  the one-time token.
- **Panel wiring** (`components/power/autotrade/BrokerAccountsPanel.tsx`): in `onConnect`,
  right before `window.open(login_url…)`, writes the pending handshake to `localStorage`
  (ids + broker name, no secret). Added a mount/unmount `message` listener that trusts the
  event ONLY when `event.origin === window.location.origin` and
  `data.type === 'kanida-broker-connected'`: `ok:true` → clear LS + run existing success path
  (`onConnected()`+`onBack()`); `ok:false` → reveal the manual paste box + surface the error.
  Refs keep the once-registered listener pointing at the latest handlers. Reworded the paste
  box as "Didn't connect automatically? Paste the request_token here" — auto is the happy
  path, manual is the safety net.
- **Security:** opener listener rejects cross-origin; landing posts with explicit target origin.
- Mint/F2 theme, reused `C`/`ICON` from `cotrade-kit`; no new accent colours. Front-end +
  types only — no execution/backend logic touched. Verify: `npx tsc --noEmit` clean;
  `next build` compiled successfully, `/power/autotrade/connect` present in route manifest,
  no collision with `/power/autotrade`.
