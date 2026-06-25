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

export type SessionConfig = {
  total_allocated_capital: number
  top_n_stocks: number
  sizing_mode: SizingMode
  max_pct_per_position?: number
  manual_amounts?: Record<string, number>
  order_product: OrderProduct
  kill_switch_enabled: boolean
  kill_switch_pct: number
  kill_switch_direction: KillDirection
  entry_time: string
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

// Session status. `status` is permissive (backend may report PAPER/RUNNING/
// CLOSED/SCHEDULED/etc.). A SCHEDULED session has NOT placed yet — it is armed
// to fire at `fires_at` and the scheduling fields below are present.
export type SessionStatusName = 'SCHEDULED' | 'RUNNING' | 'CLOSED' | string
export type StatusResponse = {
  status: SessionStatusName
  mode: Mode
  gross_return: number
  total_allocated_capital: number
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
async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    cache: 'no-store',
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
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
}
