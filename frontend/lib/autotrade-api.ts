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
  sizing_mode: SizingMode
  max_pct_per_position?: number
  manual_amounts?: Record<string, number>
  order_product: OrderProduct
  // ── portfolio_kill_switch strategy ──
  kill_switch_enabled: boolean
  kill_switch_pct: number
  kill_switch_direction: KillDirection
  entry_time: string
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
}

export type CreateResponse = {
  session_id: string
  status: string
  mode: Mode
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
export type PreviewResponse = {
  invested_basis: number          // ₹ — the kill basis (product-aware)
  total_allocated_capital: number // ₹ — your fund
  leverage: number                // ~×N (MTF leverage; ~1 for CNC)
  kill_preview: KillPreview
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
    throw new Error(typeof detail === 'string' ? detail : `HTTP ${r.status}`)
  }
  return body as T
}

export const AutoTradeAPI = {
  // List existing sessions (newest first) so the operator can RESUME a session
  // instead of starting from a blank form — fixes the "session disappears on
  // reload" gap. Read-only; places nothing.
  listSessions: () => call<SessionsListResponse>('/sessions'),

  createSession: (mode: Mode, config: SessionConfig) =>
    call<CreateResponse>('/session/create', {
      method: 'POST',
      body: JSON.stringify({ mode, config }),
    }),

  // Estimate the invested basis + kill-switch outcome for a config BEFORE Start.
  // Creates no session and places nothing — pure read-through estimate. The
  // caller must send `config.kill_switch_pct` as a FRACTION (the same /100
  // convention as createSession), since the backend speaks fractions.
  preview: (config: SessionConfig) =>
    call<PreviewResponse>('/preview', {
      method: 'POST',
      body: JSON.stringify({ config }),
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

  // The backend's outbound IP (for the broker's Allowed-IPs allowlist). This is
  // under /api/falcon (not /api/autotrade), so it bypasses BASE and hits the
  // Falcon proxy root directly. Read-only; a 404 surfaces as a normal error so
  // the caller can fall back to "—" without fabricating an IP.
  egressIp: () =>
    call<EgressIpResponse>('/egress-ip', { base: '/api/falcon-proxy/api/falcon' }),
}
