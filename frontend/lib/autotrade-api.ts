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
  status: string
  mode: Mode
  n_placed: number
  orders: PlacedOrder[]
}

export type OpenPosition = {
  symbol?: string
  qty?: number
  avg_price?: number
  last_price?: number
  pnl?: number
  return_pct?: number
  [k: string]: unknown
}

export type StatusResponse = {
  status: string
  mode: Mode
  gross_return: number
  total_allocated_capital: number
  kill_switch_enabled: boolean
  kill_switch_pct: number
  kill_switch_direction: KillDirection
  n_open_positions: number
  open_positions: OpenPosition[]
}

export type KillResponse = {
  status: string
  trigger_reason?: string
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

  startSession: (id: string) =>
    call<StartResponse>(`/session/${encodeURIComponent(id)}/start`, { method: 'POST' }),

  sessionStatus: (id: string) =>
    call<StatusResponse>(`/session/${encodeURIComponent(id)}/status`),

  killSession: (id: string) =>
    call<KillResponse>(`/session/${encodeURIComponent(id)}/kill`, { method: 'POST' }),

  positions: (id: string) =>
    call<PositionsResponse>(`/session/${encodeURIComponent(id)}/positions`),

  configList: () => call<ConfigListResponse>('/config/list'),
  brokerList: () => call<BrokerListResponse>('/broker/list'),
}
