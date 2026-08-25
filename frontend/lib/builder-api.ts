// Agent Builder API client — mirrors lib/backtest-api.ts. Drop into frontend/lib/.
// Talks to the agent_builder backend (/api/builder/*). Authed calls send the power-auth JWT.
const API = process.env.NEXT_PUBLIC_API_URL || ''

export type IndicatorMeta = { name: string; defaults: Record<string, number>; label: string }
export type Catalog = {
  indicators: IndicatorMeta[]
  ops: string[]
  exits: string[]
  directions: string[]
  universe: { stocks: number; bars: number }
}

export type Condition = { indicator: string; params?: Record<string, number>; op: '>' | '<' | '>=' | '<='; value: number }
export type ExitRule = { type: 'horizon' | 'target_stop' | 'trail'; days?: number; target?: number; stop?: number; pct?: number; max_days?: number }
export type Strategy = {
  name: string
  direction: 'long' | 'short'
  entry: { logic: 'AND' | 'OR'; conditions: Condition[] }
  exit: ExitRule
  cost_bps?: number
  granularity?: 'daily' | '1min'
}

export type TokenCost = { input: number; market_worlds: number; output: number; total: number; stocks: number; bars: number; ops_per_cell: number; granularity: string }
export type Card = { n: number; win: number; expct: number; med: number; pf: number | null; edge?: number | null } | null
export type BacktestResult = {
  strategy: Strategy
  overall: Card
  market_worlds: Record<string, Card>
  tokens: TokenCost
  tokens_charged: number
  wallet_balance: number
}

function authHeaders(user?: string | null): Record<string, string> {
  // Sends both: Authorization (power-auth JWT in prod) AND X-User-Id (standalone backend). Backend reads either.
  const h: Record<string, string> = { 'Content-Type': 'application/json' }
  if (user) { h['Authorization'] = `Bearer ${user}`; h['X-User-Id'] = user }
  return h
}

export async function fetchCatalog(): Promise<Catalog> {
  const r = await fetch(`${API}/api/builder/indicators`, { cache: 'no-store' })
  if (!r.ok) throw new Error('catalog failed')
  return r.json()
}

export async function quote(strat: Strategy): Promise<{ tokens: TokenCost }> {
  const r = await fetch(`${API}/api/builder/quote`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(strat) })
  if (!r.ok) throw new Error('quote failed')
  return r.json()
}

export async function getWallet(jwt: string): Promise<{ user_id: string; balance: number }> {
  const r = await fetch(`${API}/api/builder/wallet`, { headers: authHeaders(jwt) })
  if (!r.ok) throw new Error('wallet failed')
  return r.json()
}

export async function topup(jwt: string, tokens: number): Promise<{ user_id: string; balance: number }> {
  const r = await fetch(`${API}/api/builder/wallet/topup?tokens=${tokens}`, { method: 'POST', headers: authHeaders(jwt) })
  if (!r.ok) throw new Error('topup failed')
  return r.json()
}

export async function runBacktest(strat: Strategy, jwt: string): Promise<BacktestResult> {
  const r = await fetch(`${API}/api/builder/backtest`, { method: 'POST', headers: authHeaders(jwt), body: JSON.stringify(strat) })
  if (r.status === 402) throw new Error((await r.json()).detail || 'insufficient tokens')
  if (!r.ok) throw new Error('backtest failed')
  return r.json()
}
