/**
 * autotrade-api.ts — thin client for the LIVE multi-broker Portfolio AutoTrade
 * backend (operator-token gated), called via the EXISTING same-origin Falcon
 * proxy so the operator token never reaches the browser:
 *
 *   browser → /api/falcon-proxy/api/autotrade/<path>
 *           → falcon-proxy route injects X-Operator-Token (server-only)
 *           → ${BACKEND}/api/autotrade/<path>
 *
 * SAFETY / SCOPE: this file is TRANSPORT ONLY. It places no order itself, adds
 * no trading logic, and changes nothing server-side beyond what the operator
 * explicitly POSTs. The backend ships DISABLED — sessions default to PAPER mode
 * (no real orders), the kill switch defaults OFF, and real LIVE orders ALSO
 * require the server env flag FALCON_AUTOTRADE_ENABLED. This client never
 * implies real trading is on; it just relays request/response verbatim.
 */

const BASE = '/api/falcon-proxy/api/autotrade'

// ── Request / response shapes (mirror the 9 backend endpoints) ───────────────
export type Mode = 'paper' | 'live'
// When to start: 'now' fires immediately (→ RUNNING); 'scheduled' arms the
// session to auto-fire at its configured entry_time (→ SCHEDULED until then).
export type StartWhen = 'now' | 'scheduled'
export type SizingMode = 'equal' | 'pct_cap' | 'manual'
export type OrderProduct = 'CNC' | 'MIS' | 'MTF' | 'NRML'
export type KillDirection = 'profit' | 'loss' | 'both'

// Instrument the session trades. 'EQ' (default) = the existing cash-equity path
// (order_product CNC/MIS/MTF). 'FUT' = current-month futures — the backend sets
// the product to NRML server-side and the user does NOT pick CNC/MIS/MTF. (CE/PE
// are NOT supported yet.)
export type InstrumentType = 'EQ' | 'FUT'
// Trade side. 'long' (default) = buy. 'short' = sell — the backend REJECTS
// direction='short' with HTTP 400 "short is currently supported only for FUT"
// unless instrument_type=='FUT'.
export type TradeDirection = 'long' | 'short'

// Execution-date / trading-day rule. What to do if the fire moment is missed or
// lands on a non-trading day: drop it ('expire', the default) or roll it forward
// to the next valid trading session ('carry_next_trading_day').
export type OnMissedWindow = 'expire' | 'carry_next_trading_day'

// The backend supports two exit strategies:
//   • 'portfolio_kill_switch' (default, the EXISTING behaviour) — a single flat
//     ±% basket exit on the invested return (the kill switch).
//   • 'intraday_basket' — Falcon Intraday Basket: arm a trailing exit once the
//     basket hits +arm_pct, lock a floor, trail by a giveback %, hard stop at
//     −stop_pct, and square off at square_off_time. The four trail %s + the
//     square-off time replace the single kill number.
export type Strategy = 'portfolio_kill_switch' | 'intraday_basket'

export type SessionConfig = {
  strategy: Strategy
  total_allocated_capital: number
  top_n_stocks: number
  // universe_filter: which index/universe the top-N picks are drawn from.
  // Default 'all500' (= current behaviour; backend ignores absent/null).
  universe_filter?: string
  // symbol_whitelist: when set, ONLY these symbols are traded (ignoring rank
  // beyond the list). Absent = default top-N by rank (current behaviour).
  symbol_whitelist?: string[]
  sizing_mode: SizingMode
  max_pct_per_position?: number
  manual_amounts?: Record<string, number>
  order_product: OrderProduct
  // ── Instrument / direction ── (optional; both default server-side to EQ/long,
  // i.e. the existing equity behaviour when omitted). For FUT the backend uses
  // the current-month expiry (expiry_preference="near") and sets product=NRML;
  // the user does not pick CNC/MIS/MTF. direction='short' is FUT-only (backend
  // returns HTTP 400 "short is currently supported only for FUT" otherwise).
  instrument_type?: InstrumentType
  direction?: TradeDirection
  // ── portfolio_kill_switch strategy ──
  kill_switch_enabled: boolean
  kill_switch_pct: number
  kill_switch_direction: KillDirection
  // ── ASYMMETRIC kill switch ── (optional; both FRACTIONS on the wire, send ÷100,
  // display ×100). When set they OVERRIDE the single kill_switch_pct for that side:
  //   • kill_switch_target_pct → the PROFIT-side exit threshold.
  //   • kill_switch_stop_pct   → the LOSS-side exit threshold.
  // When a side's field is unset the backend falls back to kill_switch_pct
  // (symmetric — today's behaviour). kill_switch_direction still gates which
  // side(s) are armed (loss / profit / both).
  kill_switch_target_pct?: number
  kill_switch_stop_pct?: number
  entry_time: string
  // ── A · MIS defensive square-off ── (optional, "HH:MM:SS" IST; backend default
  // "15:12:00", must be < square_off_time). For MIS sessions the backend force-
  // squares intraday at this time, ahead of the broker's own square-off window.
  mis_square_off_time?: string
  // ── C · leftover-capital redistribution ── (optional, default true). When true
  // the backend redistributes capital freed by skipped picks (a pick whose 1 unit
  // costs more than its per-slice budget) across the remaining picks.
  redistribute_unused_capital?: boolean
  // ── Execution-date / trading-day rule ── (applies to BOTH strategies)
  // entry_date: optional "YYYY-MM-DD" the session should fire on; empty/omitted
  //   → the backend resolves to the next valid trading session.
  // on_missed_window: if the fire moment is missed or lands on a non-trading day,
  //   'expire' (default) drops it; 'carry_next_trading_day' rolls it forward.
  // entry_grace_seconds: how late after the fire moment the session may still
  //   fire before it's treated as missed (advanced; backend default 120).
  entry_date?: string
  on_missed_window?: OnMissedWindow
  entry_grace_seconds?: number
  // ── intraday_basket strategy ── (all percent fields are FRACTIONS on the wire,
  // i.e. send ÷100; the UI captures + displays them as percents). square_off_time
  // is an "HH:MM:SS" IST string.
  arm_pct?: number
  floor_pct?: number
  trail_giveback_pct?: number
  stop_pct?: number
  square_off_time?: string
  // ── D · dynamic Hold mode ── (optional, default true; only meaningful for
  // 'intraday_basket'). true = INTRADAY — force a square-off at square_off_time.
  // false = POSITIONAL — the trailing floor + hard stop persist ACROSS days with
  // NO forced square-off. The backend REJECTS positional (square_off_enabled=false)
  // when order_product is MIS (HTTP 400 "positional (no square-off) is not allowed
  // for MIS…"), since MIS must square off intraday.
  square_off_enabled?: boolean
  // ── Positional MAX-HOLD cap ── (only meaningful for intraday_basket +
  // square_off_enabled=false). Square off the whole basket at square_off_time on
  // the Nth trading session (entry day = session 1); weekends/NSE holidays are
  // skipped. 0 / absent = NO cap (today's behaviour). e.g. 3 = the Falcon
  // Positional strategy's 3-session hold.
  max_hold_sessions?: number
}

// A pick the backend SKIPPED because one unit of it costs more than its per-slice
// budget (e.g. a high-priced name like PAGEIND vs a small slice). Returned on
// create/preview/status so the UI can name what was dropped. Any field may be
// absent — render honestly.
export type SkippedPick = {
  symbol?: string
  broker_profile?: string
  reason?: string
  [k: string]: unknown
}

export type CreateResponse = {
  session_id: string
  status: string
  mode: Mode
  // Picks dropped at sizing time (1 unit > their per-slice budget). Present when
  // the config would skip one or more names; absent/[] otherwise.
  skipped_picks?: SkippedPick[]
}

export type PlacedOrder = {
  symbol: string
  status: string
  reason?: string
}

export type StartResponse = {
  // 'now' → RUNNING with placed orders; 'scheduled' → SCHEDULED (nothing placed
  // yet) carrying the scheduling fields.
  status: SessionStatusName
  mode: Mode
  n_placed: number
  orders: PlacedOrder[]
  fires_at?: string
  seconds_remaining?: number
  scheduler_armed?: boolean
}

export type OpenPosition = {
  symbol?: string
  qty?: number
  avg_price?: number
  ltp?: number              // live last price from backend (was mis-read as last_price)
  last_price?: number       // legacy/unused — backend sends `ltp`
  unrealised_pnl?: number
  pnl?: number
  return_pct?: number
  [k: string]: unknown
}

// One side (target/stop) of the kill-switch preview. The backend returns the
// pcts as FRACTIONS (×100 to display); basis_value_rs is already in ₹ on the
// INVESTED basis (the kill basis); fund_pct is the same outcome expressed as a
// FRACTION of YOUR fund (×100 to display).
export type KillPreviewSide = {
  pct: number              // FRACTION (0.05 = 5%) — the configured threshold
  basis_value_rs: number   // ₹ P&L at that threshold, on the INVESTED basis
  fund_pct: number         // FRACTION — same ₹ ÷ your fund
}
// Kill-switch outcome preview — either side may be absent depending on the
// configured direction (loss / profit / both).
export type KillPreview = {
  target?: KillPreviewSide
  stop?: KillPreviewSide
}

// Live trail state for an 'intraday_basket' session (read from status()). All
// pct fields are FRACTIONS on the wire (×100 to display); square_off_time is an
// "HH:MM:SS" IST string; seconds_to_square_off counts down to it. Any field may
// be absent → the UI degrades to "—".
export type TrailState = {
  armed?: boolean                 // has the basket armed the trail (hit +arm_pct)?
  peak?: number                   // FRACTION — best notional return seen
  current_gross_return?: number   // FRACTION — current notional return
  trigger?: number                // FRACTION — the live exit-trigger level
  arm_pct?: number                // FRACTION — configured arm/profit threshold
  floor_pct?: number              // FRACTION — configured lock floor
  trail_giveback_pct?: number     // FRACTION — configured trail giveback
  stop_pct?: number               // FRACTION — configured hard stop
  square_off_time?: string        // "HH:MM:SS" IST
  seconds_to_square_off?: number  // seconds until square-off
  square_off_armed?: boolean      // is the square-off timer armed?
  // ── Positional max-hold cap (present when max_hold_sessions>0) ──
  max_hold_sessions?: number             // configured N-session cap (0 = none)
  max_hold_cap_datetime?: string | null  // ISO IST — when the force-close fires
}

// Session status. `status` is permissive (backend may report PAPER/RUNNING/
// CLOSED/SCHEDULED/etc.). A SCHEDULED session has NOT placed yet — it is armed
// to fire at `fires_at` and the scheduling fields below are present.
// REJECTED_NON_TRADING_DAY — the chosen entry_date is not a trading day and the
//   session was rejected outright. EXPIRED_MISSED_WINDOW — the fire moment was
//   missed (past the grace) and on_missed_window='expire', so nothing was placed.
//   DEFERRED_MARKET_CLOSED — the fire moment landed while the market was closed
//   and the session is deferred (see deferred_reason). None of these placed orders.
export type SessionStatusName =
  | 'SCHEDULED' | 'RUNNING' | 'CLOSED'
  | 'REJECTED_NON_TRADING_DAY' | 'EXPIRED_MISSED_WINDOW' | 'DEFERRED_MARKET_CLOSED'
  | string
// Exit reason on a CLOSED intraday_basket session.
export type ExitReason = 'SQUARE_OFF' | 'STOP' | 'TRAIL_EXIT' | 'FLOOR_EXIT' | string
export type StatusResponse = {
  status: SessionStatusName
  mode: Mode
  // Which exit strategy this session runs. Absent → treat as 'portfolio_kill_switch'.
  strategy?: Strategy
  // gross_return is the KILL BASIS = return on INVESTED capital (product-aware:
  // MTF = leveraged invested value, CNC = cash), a FRACTION (-0.0136 = -1.36%).
  gross_return: number
  // gross_return_fund = the same P&L ÷ your fund, a FRACTION. Shown alongside
  // gross_return so the operator sees both the kill basis and the fund-level view.
  gross_return_fund?: number
  // The ₹ bases behind the two returns: invested_basis (the kill basis, ₹) and
  // the fund (total_allocated_capital, ₹).
  invested_basis?: number
  total_allocated_capital: number
  // Exact, LIVE kill-switch outcome preview for the running session (mirrors the
  // POST /preview shape). Present when the kill switch is configured.
  kill_preview?: KillPreview
  kill_switch_enabled: boolean
  kill_switch_pct: number
  kill_switch_direction: KillDirection
  n_open_positions: number
  open_positions: OpenPosition[]
  // Present (and meaningful) while status === 'SCHEDULED'. fires_at is ISO IST;
  // seconds_remaining counts down to the entry; scheduler_armed=false means a
  // backend restart dropped the in-memory timer → it must be re-started.
  fires_at?: string
  seconds_remaining?: number
  scheduler_armed?: boolean
  // ── Execution-date / trading-day rule (read-back) ──
  // resolved_fire_datetime is the exact ISO IST moment the session will/did fire
  // (the backend's resolution of entry_date + entry_time → next valid session);
  // resolved_fire_date is its date part. is_trading_day / market_open_now describe
  // that resolved moment. entry_date / on_missed_window echo the config back.
  // deferred_reason carries the human reason for a DEFERRED_MARKET_CLOSED (and may
  // accompany the other non-placed statuses) so the UI can surface it verbatim.
  resolved_fire_datetime?: string
  resolved_fire_date?: string
  is_trading_day?: boolean
  market_open_now?: boolean
  entry_date?: string
  on_missed_window?: OnMissedWindow
  deferred_reason?: string
  // ── intraday_basket live trail state ── (present when strategy === 'intraday_basket').
  // Prefer the nested `trail{...}`; the flat mirror fields are a fallback.
  trail?: TrailState
  trail_armed?: boolean
  trail_peak?: number
  trail_trigger?: number
  square_off_time?: string
  seconds_to_square_off?: number
  // Picks skipped for this session (1 unit > per-slice budget). Present when the
  // running/created session dropped one or more names.
  skipped_picks?: SkippedPick[]
  // ── intraday_basket close summary ── (present on a CLOSED session).
  exit_reason?: ExitReason
  notional_return?: number    // FRACTION — final return on the invested/notional basis
  own_funds_return?: number   // FRACTION — final return on your own funds
  // ── SPEED / latency readout ── (ALWAYS present; ints in ms, may be null).
  // Works for BOTH strategies. entry/exit are deploy/exit speeds; last_tick_age_ms
  // is the data-freshness heartbeat that proves sub-second monitoring.
  entry_latency_ms?: number | null  // fire start → all legs settled (deploy speed)
  exit_latency_ms?: number | null   // flatten trigger → all flat (exit speed; once flattened)
  last_tick_age_ms?: number | null  // now − newest tick used (data freshness / liveness)
}

// POST /api/autotrade/preview — an ESTIMATE before Start. Creates no session and
// places nothing; it just reports the bases + the kill-switch outcome for the
// given config so the operator can see "+₹X / −₹Y" before committing.
// One sized (or skipped) position row in the preview. For F&O the lots/lot_size/
// margin_per_lot/notional fields are present; for cash/MTF `margin` is the deployed
// margin and the F&O fields are absent. A skipped row has status==='SKIPPED'+reason.
export type PreviewPosition = {
  symbol: string
  qty?: number
  ref_price?: number
  invested_value?: number
  order_product?: string
  instrument_type?: string        // 'EQ' | 'FUT' | 'CE' | 'PE'
  margin?: number                 // ₹ capital/margin deployed for THIS position
  lots?: number                   // F&O: number of lots placed
  lot_size?: number               // F&O: contract lot size (shares per lot)
  margin_per_lot?: number         // F&O: ₹ margin per lot
  notional?: number               // F&O: qty × price = contract exposure
  status?: string                 // 'SKIPPED' when dropped
  reason?: string                 // skip reason (e.g. 1 lot margin > slice)
  [k: string]: unknown
}

export type PreviewResponse = {
  invested_basis: number          // ₹ — the kill basis (product-aware; notional for F&O)
  total_allocated_capital: number // ₹ — your fund
  total_margin?: number           // ₹ — total margin/capital deployed (F&O / MTF margin)
  leverage: number                // ~×N (MTF/F&O leverage; ~1 for CNC)
  n_positions?: number
  positions?: PreviewPosition[]    // per-name sized rows (+ skipped rows) with lots/margin
  kill_preview: KillPreview
  // Picks that WOULD be skipped for this config (1 unit > per-slice budget), so
  // the operator sees them before committing. Absent/[] when nothing is skipped.
  skipped_picks?: SkippedPick[]
}

export type KillResponse = {
  status: string
  trigger_reason?: string
  [k: string]: unknown
}

// Bulk-delete (paper/test) sessions. Read-only on positions — removes the
// session rows; the backend returns how many it actually removed + their ids.
export type DeleteSessionsResponse = {
  deleted: number
  ids: string[]
  [k: string]: unknown
}

export type PositionsResponse = { positions: OpenPosition[] }

// Egress IP self-service. The broker (Zerodha / developers.kite.trade) requires
// the SERVER's outbound IP on its Allowed-IPs list or live orders error. This
// endpoint reports the IP the backend places orders FROM, plus when it was read.
// Backend being added; a 404 is handled gracefully (the UI shows "—").
export type EgressIpResponse = {
  ip: string
  as_of?: string
  [k: string]: unknown
}

// A session as returned by GET /autotrade/sessions (newest first). The backend
// shape is permissive — we read the fields we know and keep the rest indexable
// so a missing/renamed field never crashes the list.
export type SessionSummary = {
  session_id: string
  status?: string
  mode?: Mode
  total_allocated_capital?: number
  gross_return?: number
  created_at?: string
  top_n_stocks?: number
  n_open_positions?: number
  // A SCHEDULED session in the list shows its fire time distinctly from
  // RUNNING/CLOSED. These mirror StatusResponse and may be absent for others.
  fires_at?: string
  seconds_remaining?: number
  scheduler_armed?: boolean
  // Set when this session is a CHILD BASKET of a running Auto-Ladder campaign.
  // The Sessions list tags such rows with a small "Campaign" chip; the row is
  // otherwise an ordinary session. Absent for standalone sessions.
  ladder_id?: string | null
  [k: string]: unknown
}
export type SessionsListResponse = {
  sessions?: SessionSummary[]
  [k: string]: unknown
}

export type SavedConfig = {
  id?: string | number
  name?: string
  config?: Partial<SessionConfig>
  [k: string]: unknown
}
export type ConfigListResponse = { configs?: SavedConfig[]; [k: string]: unknown }

export type Broker = {
  id?: string | number
  broker?: string
  name?: string
  label?: string
  status?: string
  [k: string]: unknown
}
export type BrokerListResponse = { brokers?: Broker[]; [k: string]: unknown }

// ── Broker accounts (Phase-2 multi-tenant: connect-your-broker) ──────────────
// A connected broker account, operator-token-gated, scoped by user_id. The
// api_secret is WRITE-ONLY — it is NEVER returned by any endpoint and never
// stored in the browser; only `has_secret` reports whether one is on file.
// status: PENDING (no daily token yet — needs a Connect login) | ACTIVE (has a
// live token, ready to trade) | EXPIRED (token lapsed — needs Re-connect).
export type BrokerAccountStatus = 'PENDING' | 'ACTIVE' | 'EXPIRED' | string
export type BrokerName = 'zerodha' | 'upstox' | 'angel' | 'dhan' | 'fyers' | string

export type BrokerAccount = {
  broker_account_id: string
  broker: BrokerName
  account_label: string
  status: BrokerAccountStatus
  api_key_masked?: string
  has_secret?: boolean
  has_token?: boolean
  [k: string]: unknown
}

// GET /autotrade/broker-accounts?user_id= →
//   { accounts:[...], vault_enabled }
// vault_enabled=false means the operator hasn't set the vault key yet, so the
// feature is dormant — the UI shows a friendly empty state, never an error.
export type BrokerAccountsResponse = {
  accounts?: BrokerAccount[]
  vault_enabled?: boolean
  [k: string]: unknown
}

// POST /autotrade/broker-account → the public masked dict of the new account
// (the secret is consumed server-side and never echoed).
export type CreateBrokerAccountRequest = {
  user_id: number | string
  broker: BrokerName
  account_label: string
  api_key: string
  api_secret: string  // WRITE-ONLY — sent once, never read back; cleared from state after submit
}

// GET /autotrade/broker-account/{id}/login-url?user_id= → { login_url }
export type LoginUrlResponse = { login_url: string; [k: string]: unknown }

// POST /autotrade/broker-account/{id}/refresh-token { user_id?, request_token }
// → the updated (now ACTIVE) masked account.
export type RefreshTokenResponse = BrokerAccount

// ── Supported-broker registry (GET /brokers/supported) ───────────────────────
// The backend advertises which brokers exist and, per broker, whether the
// integration is LIVE (selectable) and its capability flags (the AutoTrade
// engine branches on THESE, never on the broker name). Non-live brokers are
// shown disabled with a "coming soon" tag. All capability fields are optional
// so a partial/renamed backend shape never crashes the UI.
export type BrokerCapabilities = {
  // How the daily auth is obtained (e.g. 'request_token', 'oauth2_flow').
  auth_kind?: string
  // Does the broker issue a long-lived refresh token (false → daily reconnect)?
  has_refresh_token?: boolean
  // Human token lifetime, e.g. 'daily ~06:00 IST' | 'session'.
  token_lifetime?: string
  supports_gtt?: boolean
  supports_mtf?: boolean
  // F&O supported?
  fno?: boolean
  [k: string]: unknown
}
export type SupportedBroker = {
  broker: BrokerName
  live: boolean
  capabilities?: BrokerCapabilities
  [k: string]: unknown
}
export type SupportedBrokersResponse = {
  brokers?: SupportedBroker[]
  [k: string]: unknown
}

// ── Per-account health (GET /broker-account/{id}/health) ─────────────────────
// A live liveness ping for a connected account (e.g. kite.profile()). `ok` is
// the boolean verdict; `status` mirrors the account status (ACTIVE/EXPIRED/…);
// `detail` is a human string to surface verbatim. Any field may be absent.
export type BrokerAccountHealth = {
  ok?: boolean
  status?: BrokerAccountStatus
  detail?: string
  [k: string]: unknown
}

// ── Trade Journal types ───────────────────────────────────────────────────────

export type JournalPosition = {
  symbol: string
  exchange: string
  qty: number
  avg_price: number
  invested_rs: number
  sl_level: number
  target_price: number
  sl_pct: number
  target_pct: number
  gtt_stop: number | null
  gtt_target: number | null
  ltp: number | null
  status: 'OPEN' | 'CLOSED'
  exit_price: number | null
  realised_pnl: number | null
  unrealised_pnl: number | null
  pnl_rs: number
  pnl_pct: number
  close_reason: string | null
  opened_at: string
  closed_at: string | null
  hold_minutes: number | null
  review_flag: 'DEEP_LOSS' | 'LARGE_WIN' | 'STOP_RECOVERED' | 'CONTINUED_DECLINE_OPEN' | 'WINNER_OPEN' | null
  review_note: string | null
}

export type JournalReviewItem = {
  symbol: string
  flag: string
  note: string
}

export type JournalSessionSummary = {
  total_allocated_capital: number
  invested_basis: number
  leverage: number
  total_realised_pnl: number
  total_unrealised_pnl: number
  total_pnl: number
  total_pnl_pct_invested: number   // already ×100 = percent
  total_pnl_pct_fund: number
  n_positions: number
  n_closed: number
  n_open: number
  n_winners: number
  n_losers: number
  n_stop_hits: number
  n_target_hits: number
  n_trail_exits: number
  n_square_off: number
  n_kill_switch: number
  best_trade: { symbol: string; pnl_rs: number; pnl_pct: number } | null
  worst_trade: { symbol: string; pnl_rs: number; pnl_pct: number } | null
  avg_hold_minutes: number | null
  session_status: string
  entry_latency_ms: number | null
  exit_latency_ms: number | null
}

export type TradeJournal = {
  session_id: string
  trading_date: string      // "YYYY-MM-DD"
  strategy: string
  mode: 'paper' | 'live'
  session_summary: JournalSessionSummary
  positions: JournalPosition[]
  review_items: JournalReviewItem[]
}

// ── Universe filter + manual stock picker (session creation) ──────────────────

// The five universe filter options — identical to the Top20Filters / signals page.
export type UniverseFilter = 'all500' | 'nifty50' | 'nifty100' | 'nifty200' | 'fno'

// A single ranked pick in the session-picks preview list.
export type PickItem = {
  rank: number
  symbol: string
  sector: string
  score: number
  n_fires: number
  avg_lift: number
  close_at_signal: number
}

// Response from GET /api/autotrade/session/picks?universe=&top_n=
export type PicksResponse = {
  signal_date: string
  universe_filter: string
  top_n: number
  picks: PickItem[]
}

// ── Falcon Positional Auto-Ladder ("Monthly Campaign") ───────────────────────
// A "set once, run for a month" campaign. The trader sets total capital +
// product + duration ONCE and Starts; the backend then autonomously opens,
// manages and rolls positional baskets every trading day until the end date.
// The trader NEVER manages individual baskets — a running campaign's child
// baskets are ordinary sessions (they carry ladder_id) and remain visible in the
// Sessions tab; this API is the higher-level campaign view.
//
// TRADER TERMS ONLY on the wire we can't control, but the UI must never surface
// internal words like "sleeve" — capital deployed / active baskets / open
// positions / daily P&L. All response fields are OPTIONAL-SAFE: the backend
// shape may omit any of them and the UI degrades to "—" rather than crash.

// Product for a campaign — CNC (cash-and-carry) or MTF (margin). NO MIS: the
// backend 400s an intraday product for a multi-day campaign, so the UI must not
// offer it.
export type LadderProduct = 'CNC' | 'MTF'

// When the campaign ends: 'month_end' auto-stops at the current month's end;
// 'manual' runs until the trader stops it. An explicit end_date may accompany
// either (backend resolves).
export type LadderEndMode = 'month_end' | 'manual'

// How a KILL winds the campaign down. 'flatten_now' exits every open basket
// immediately; 'stop_new_let_finish' stops opening NEW baskets but lets the
// already-open ones finish their normal exits. The backend REQUIRES one — there
// is no silent default, so the UI forces the trader to choose.
export type LadderKillMode = 'flatten_now' | 'stop_new_let_finish'

// CREATED = a draft campaign (created but not started; spawns nothing).
// SCHEDULED = armed for a future start_date (auto-activates on that date).
export type LadderStatusName =
  | 'CREATED' | 'SCHEDULED' | 'RUNNING' | 'PAUSED' | 'ENDED' | 'COMPLETED' | string

// POST /ladder/create body. total_capital is ₹; the backend derives the per-
// basket budget (~total/3). mode defaults paper (no real orders). end_date is
// optional and only meaningful with an explicit calendar stop.
export type LadderCreateBody = {
  total_capital: number
  order_product: LadderProduct
  mode: Mode
  end_date_mode: LadderEndMode
  end_date?: string
  kill_mode?: LadderKillMode
  // Ownership scope — MUST be sent (same as createSession) so the campaign is
  // created with the caller's user_id. Otherwise the ?user_id-scoped /ladders
  // list filters it out (WHERE user_id = ?) and a SCHEDULED campaign never
  // appears in the Sessions list. This is the Positional-parity fix.
  user_id?: number | string
  broker_account_id?: string
}

export type LadderCreateResponse = {
  ladder_id: string
  status?: LadderStatusName
  [k: string]: unknown
}

// A child basket of a running campaign (an ordinary session carrying ladder_id).
// Every field is optional-safe — render "—" for anything absent, never crash.
export type LadderSession = {
  session_id: string
  status?: string
  total_allocated_capital?: number
  started_at?: string
  closed_at?: string
  [k: string]: unknown
}

// The informational downturn note. When active, message is plain-language and
// must be rendered VERBATIM in a calm amber note — it is NOT an error. The
// optional trailing_5d_avg_return is a FRACTION (×100 to display).
export type LadderAlert = {
  active: boolean
  message: string
  trailing_5d_avg_return?: number
  n_days_in_window?: number
  [k: string]: unknown
}

// GET /ladder/{id}/status — the live campaign view (poll ~5s). Every field is
// optional-safe: the backend shape may omit any of them and the UI degrades to
// "—". realized/unrealized/today P&L are ₹; per_basket_capital is ₹.
export type LadderStatus = {
  ladder_id?: string
  status?: LadderStatusName
  total_capital?: number
  capital_deployed?: number
  capital_free?: number
  per_basket_capital?: number
  n_active_baskets?: number
  n_open_positions?: number
  realized_pnl?: number
  unrealized_pnl?: number
  today_pnl?: number
  order_product?: LadderProduct
  start_date?: string
  end_date?: string
  mode_kill?: LadderKillMode
  alert?: LadderAlert
  sessions?: LadderSession[]
  [k: string]: unknown
}

// A campaign as returned by GET /ladders?user_id= (newest first). Permissive
// shape — we read what we know and keep the rest indexable.
export type LadderSummary = {
  ladder_id: string
  status?: LadderStatusName
  mode?: Mode
  total_capital?: number
  order_product?: LadderProduct
  start_date?: string
  end_date?: string
  created_at?: string
  n_active_baskets?: number
  today_pnl?: number
  [k: string]: unknown
}
export type LaddersListResponse = {
  ladders?: LadderSummary[]
  [k: string]: unknown
}

// ── Transport helper — honest errors, never fabricates a success ─────────────
// `base` lets a call target a sibling proxy root (e.g. /api/falcon for the
// egress-IP endpoint) without changing the default /api/autotrade transport.
async function call<T>(path: string, init?: RequestInit & { base?: string }): Promise<T> {
  const { base = BASE, ...rest } = init ?? {}
  const r = await fetch(`${base}${path}`, {
    cache: 'no-store',
    ...rest,
    headers: { 'content-type': 'application/json', ...(rest.headers ?? {}) },
  })
  const text = await r.text()
  let body: unknown = null
  try {
    body = text ? JSON.parse(text) : null
  } catch {
    body = { error: text || 'invalid_response' }
  }
  if (!r.ok) {
    const detail =
      (body && typeof body === 'object' && 'detail' in body && (body as { detail?: unknown }).detail) ||
      (body && typeof body === 'object' && 'error' in body && (body as { error?: unknown }).error) ||
      `HTTP ${r.status}`
    // Structured `detail` OBJECTS (e.g. the non-trading-day rejection:
    // { message, suggested_date, code }) carry a human message plus fields the
    // caller can act on. Surface `.message` and attach the extras onto the Error
    // so a caller can read e.g. `(err as any).suggested_date`. String details
    // keep their existing behaviour verbatim.
    if (detail && typeof detail === 'object') {
      const d = detail as { message?: unknown; suggested_date?: unknown; code?: unknown }
      const msg = typeof d.message === 'string' ? d.message : `HTTP ${r.status}`
      const err = new Error(msg) as Error & { suggested_date?: string; code?: string; detail?: unknown }
      if (typeof d.suggested_date === 'string') err.suggested_date = d.suggested_date
      if (typeof d.code === 'string') err.code = d.code
      err.detail = detail
      throw err
    }
    throw new Error(typeof detail === 'string' ? detail : `HTTP ${r.status}`)
  }
  return body as T
}

// ── Query-string / scope helpers ─────────────────────────────────────────────
// Build a "?a=b&c=d" string from defined params (skips null/undefined/empty).
function q(params: Record<string, string | number | undefined | null>): string {
  const usp = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === '') continue
    usp.set(k, String(v))
  }
  const s = usp.toString()
  return s ? `?${s}` : ''
}

// Optional per-user / per-account scoping for session create / preview / list.
// When a user context exists (and an ACTIVE broker account is chosen) these are
// sent so the session runs on that account. Default (both absent) = the
// operator/global session, exactly as before.
export type SessionScope = { user_id?: number | string; broker_account_id?: string }

// Scope as a query string (for GET /sessions) — only the user_id is used.
function userQuery(scope?: SessionScope): string {
  return scope?.user_id != null ? q({ user_id: scope.user_id }) : ''
}

// Scope as request-body fields (for POST create/preview) — only defined fields.
function scopeBody(scope?: SessionScope): Record<string, string | number> {
  const out: Record<string, string | number> = {}
  if (scope?.user_id != null) out.user_id = scope.user_id
  if (scope?.broker_account_id) out.broker_account_id = scope.broker_account_id
  return out
}

export const AutoTradeAPI = {
  // List existing sessions (newest first) so the operator can RESUME a session
  // instead of starting from a blank form — fixes the "session disappears on
  // reload" gap. Read-only; places nothing. Scoped by user_id when present.
  listSessions: (scope?: SessionScope) =>
    call<SessionsListResponse>(`/sessions${userQuery(scope)}`),

  createSession: (mode: Mode, config: SessionConfig, scope?: SessionScope) =>
    call<CreateResponse>('/session/create', {
      method: 'POST',
      body: JSON.stringify({ mode, config, ...scopeBody(scope) }),
    }),

  // Estimate the invested basis + kill-switch outcome for a config BEFORE Start.
  // Creates no session and places nothing — pure read-through estimate. The
  // caller must send `config.kill_switch_pct` as a FRACTION (the same /100
  // convention as createSession), since the backend speaks fractions.
  preview: (config: SessionConfig, scope?: SessionScope) =>
    call<PreviewResponse>('/preview', {
      method: 'POST',
      body: JSON.stringify({ config, ...scopeBody(scope) }),
    }),

  // ── Supported-broker registry ───────────────────────────────────────────────
  // GET /brokers/supported → { brokers:[{ broker, live, capabilities }] }.
  // Sources the broker dropdown: live brokers are selectable; non-live are shown
  // disabled with "coming soon". Read-only; a 404/error surfaces normally so the
  // caller can fall back to a static list without fabricating capabilities.
  supportedBrokers: () => call<SupportedBrokersResponse>('/brokers/supported'),

  // GET /broker-account/{id}/health → { ok, status, detail } — a live liveness
  // ping for one account. Scoped by user_id so the backend authorises the read.
  brokerAccountHealth: (id: string, userId: number | string) =>
    call<BrokerAccountHealth>(`/broker-account/${encodeURIComponent(id)}/health${q({ user_id: userId })}`),

  // ── Broker accounts (connect-your-broker; operator-token-gated) ─────────────
  // List a user's connected broker accounts + whether the server vault is on.
  brokerAccounts: (userId: number | string) =>
    call<BrokerAccountsResponse>(`/broker-accounts${q({ user_id: userId })}`),

  // Connect a new broker account. api_secret is WRITE-ONLY — consumed server-side
  // and never echoed back. The caller MUST clear it from state after this resolves.
  createBrokerAccount: (req: CreateBrokerAccountRequest) =>
    call<BrokerAccount>('/broker-account', {
      method: 'POST',
      body: JSON.stringify(req),
    }),

  // Remove a connected account (with the user scope so the backend authorises it).
  deleteBrokerAccount: (id: string, userId: number | string) =>
    call<{ ok?: boolean; [k: string]: unknown }>(
      `/broker-account/${encodeURIComponent(id)}${q({ user_id: userId })}`,
      { method: 'DELETE' },
    ),

  // Get the broker login URL (Zerodha) so the user can authenticate in a popup
  // and obtain a request_token to paste back.
  brokerLoginUrl: (id: string, userId: number | string) =>
    call<LoginUrlResponse>(`/broker-account/${encodeURIComponent(id)}/login-url${q({ user_id: userId })}`),

  // Exchange the pasted request_token for a daily access token → account ACTIVE.
  refreshBrokerToken: (id: string, requestToken: string, userId?: number | string) =>
    call<RefreshTokenResponse>(`/broker-account/${encodeURIComponent(id)}/refresh-token`, {
      method: 'POST',
      body: JSON.stringify({ request_token: requestToken, ...(userId != null ? { user_id: userId } : {}) }),
    }),

  // when='now' (default) places immediately → RUNNING; when='scheduled' arms the
  // session to auto-fire at its entry_time → SCHEDULED (places nothing yet).
  startSession: (id: string, when: StartWhen = 'now') =>
    call<StartResponse>(`/session/${encodeURIComponent(id)}/start`, {
      method: 'POST',
      body: JSON.stringify({ when }),
    }),

  sessionStatus: (id: string) =>
    call<StatusResponse>(`/session/${encodeURIComponent(id)}/status`),

  killSession: (id: string) =>
    call<KillResponse>(`/session/${encodeURIComponent(id)}/kill`, { method: 'POST' }),

  // Bulk-delete sessions (paper/test housekeeping). POSTs the id list; the
  // backend removes the rows and reports { deleted, ids }. Does NOT place or
  // exit any order — it's session-record cleanup only.
  deleteSessions: (ids: string[]) =>
    call<DeleteSessionsResponse>('/sessions/delete', {
      method: 'POST',
      body: JSON.stringify({ session_ids: ids }),
    }),

  positions: (id: string) =>
    call<PositionsResponse>(`/session/${encodeURIComponent(id)}/positions`),

  configList: () => call<ConfigListResponse>('/config/list'),
  brokerList: () => call<BrokerListResponse>('/broker/list'),

  // Fetch the trade journal for a completed (or live) session.
  // Route: GET /api/falcon-proxy/api/autotrade/session/{id}/journal
  // Returns the full session summary, per-position detail, and review flags.
  journal: (sessionId: string) =>
    call<TradeJournal>(`/session/${encodeURIComponent(sessionId)}/journal`),

  // The backend's outbound IP (for the broker's Allowed-IPs allowlist). This is
  // under /api/falcon (not /api/autotrade), so it bypasses BASE and hits the
  // Falcon proxy root directly. Read-only; a 404 surfaces as a normal error so
  // the caller can fall back to "—" without fabricating an IP.
  egressIp: () =>
    call<EgressIpResponse>('/egress-ip', { base: '/api/falcon-proxy/api/falcon' }),

  // Ranked picks list for the session creation manual picker.
  // GET /api/autotrade/session/picks?universe=&top_n=
  // A 404 or error means the backend doesn't have this endpoint yet — callers
  // must catch and degrade gracefully (show "Universe preview unavailable").
  sessionPicks: (universe: string, topN: number): Promise<PicksResponse> => {
    const qs = new URLSearchParams({ universe, top_n: String(topN) })
    return call<PicksResponse>(`/session/picks?${qs.toString()}`)
  },

  // ── Falcon Positional Auto-Ladder ("Monthly Campaign") ─────────────────────
  // A "set once, run for a month" campaign: create → start → the backend then
  // opens/manages/rolls positional baskets every trading day until end_date. The
  // operator token + power_jwt are injected by the proxy, exactly like every
  // other method here — no scope/body wrangling needed beyond the create body.

  // Create a campaign (places nothing — it just registers the plan). Returns the
  // ladder_id you then Start.
  ladderCreate: (body: LadderCreateBody) =>
    call<LadderCreateResponse>('/ladder/create', {
      method: 'POST',
      body: JSON.stringify(body),
    }),

  // Start a created (CREATED/draft) campaign. Backward-compatible:
  //   • ladderStart(id)            → no body → RUNNING now (first basket next
  //                                   trading morning). Existing callers unchanged.
  //   • ladderStart(id, startDate) → POSTs { start_date }. A FUTURE trading day →
  //                                   SCHEDULED (auto-activates on that date). A
  //                                   weekend/holiday → HTTP 400 whose detail is an
  //                                   OBJECT { message, suggested_date, code:
  //                                   'NON_TRADING_START_DATE' }; the shared call<>()
  //                                   helper surfaces .message and attaches
  //                                   (err as any).suggested_date for the caller.
  ladderStart: (id: string, startDate?: string) =>
    call<LadderCreateResponse>(`/ladder/${encodeURIComponent(id)}/start`, {
      method: 'POST',
      ...(startDate ? { body: JSON.stringify({ start_date: startDate }) } : {}),
    }),

  // Pause the campaign — stops opening new baskets; open baskets keep running.
  ladderPause: (id: string) =>
    call<LadderCreateResponse>(`/ladder/${encodeURIComponent(id)}/pause`, { method: 'POST' }),

  // Resume a paused campaign → RUNNING.
  ladderResume: (id: string) =>
    call<LadderCreateResponse>(`/ladder/${encodeURIComponent(id)}/resume`, { method: 'POST' }),

  // Kill the campaign. The backend REQUIRES a mode — no silent default:
  //   'flatten_now'          → exit every open basket immediately.
  //   'stop_new_let_finish'  → stop opening new; let open baskets finish.
  ladderKill: (id: string, mode: LadderKillMode) =>
    call<LadderCreateResponse>(`/ladder/${encodeURIComponent(id)}/kill`, {
      method: 'POST',
      body: JSON.stringify({ mode }),
    }),

  // Live campaign view (poll ~5s). Optional-safe shape — never crash on a missing
  // field; degrade to "—".
  ladderStatus: (id: string) =>
    call<LadderStatus>(`/ladder/${encodeURIComponent(id)}/status`),

  // List a user's campaigns (newest first) so they can re-open a running one.
  ladders: (userId: number | string) =>
    call<LaddersListResponse>(`/ladders${q({ user_id: userId })}`),
}
