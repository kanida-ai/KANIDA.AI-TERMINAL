// KANIDA.AI — Admin API client
// All admin operations go through this module.

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

// ── Types ─────────────────────────────────────────────────────────────────────

export type UniverseStock = {
  symbol:        string
  exchange:      string
  asset_class:   string
  company_name:  string | null
  sector:        string | null
  industry:      string | null
  universe_sets: string[]
  is_active:     boolean
  added_date:    string
  added_by:      string
  notes:         string | null
}

export type UniverseStats = {
  total:       number
  active:      number
  inactive:    number
  by_sector:   { sector: string; count: number }[]
  by_exchange: { exchange: string; count: number }[]
}

export type DataAudit = {
  total_ohlcv_rows:      number
  yfinance_rows:         number
  contamination_pct:     number
  is_clean:              boolean
  sources:               { source: string; rows: number; tickers: number; latest_date: string }[]
  stale_tickers:         { ticker: string; latest: string; rows: number }[]
  universe_missing_data: string[]
  warnings:              { level: 'critical' | 'warning' | 'info'; message: string; action: string }[]
}

export type PipelineStatus = {
  running:     boolean
  last_run:    string | null
  last_result: string | null
}

export type DataFreshness = {
  last_trading_day: string
  overall_healthy:  boolean
  ohlcv: {
    latest_date:       Record<string, string>
    nse_total_tickers: number
    nse_fresh_tickers: number
    nse_stale_tickers: number
    is_fresh:          boolean
  }
  signals: {
    latest_snapshot_date: string | null
    signal_count:         number
    live_opportunities:   number
    is_fresh:             boolean
  }
  pipeline_logs: Record<string, { last_run: string | null; last_line: string | null }>
}

export type KiteStatus = {
  valid:     boolean
  user?:     string
  expires?:  string
  reason?:   string
}

// ── Universe endpoints ─────────────────────────────────────────────────────────

export async function fetchUniverse(params?: {
  sector?: string
  active?: boolean
  search?: string
  limit?: number
}): Promise<{ total: number; results: UniverseStock[] }> {
  const q = new URLSearchParams()
  if (params?.sector) q.set('sector', params.sector)
  if (params?.active !== undefined) q.set('active', String(params.active))
  if (params?.search) q.set('search', params.search)
  if (params?.limit)  q.set('limit', String(params.limit))
  const r = await fetch(`${API}/api/universe?${q}`)
  if (!r.ok) throw new Error(`Universe fetch failed: ${r.status}`)
  return r.json()
}

export async function fetchUniverseStats(): Promise<UniverseStats> {
  const r = await fetch(`${API}/api/universe/stats`)
  if (!r.ok) throw new Error(`Stats fetch failed: ${r.status}`)
  return r.json()
}

export async function fetchDataAudit(): Promise<DataAudit> {
  const r = await fetch(`${API}/api/universe/data-audit`)
  if (!r.ok) throw new Error(`Audit fetch failed: ${r.status}`)
  return r.json()
}

export async function addStock(body: {
  symbol: string
  sector?: string
  exchange?: string
  company_name?: string
  universe_sets?: string[]
  notes?: string
}): Promise<{ status: string; symbol: string }> {
  const r = await fetch(`${API}/api/universe`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ ...body, added_by: 'admin' }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Add stock failed')
  }
  return r.json()
}

export async function updateStock(
  symbol: string,
  body: Partial<{ sector: string; is_active: boolean; universe_sets: string[]; notes: string; company_name: string }>
): Promise<{ status: string }> {
  const r = await fetch(`${API}/api/universe/${encodeURIComponent(symbol)}`, {
    method:  'PUT',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Update failed')
  }
  return r.json()
}

export async function deactivateStock(symbol: string): Promise<{ status: string }> {
  const r = await fetch(`${API}/api/universe/${encodeURIComponent(symbol)}`, {
    method: 'DELETE',
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Deactivate failed')
  }
  return r.json()
}

export async function seedUniverse(secret: string): Promise<{
  status: string; inserted: number; skipped: number; message: string
}> {
  const r = await fetch(`${API}/api/universe/seed`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ secret }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Seed failed')
  }
  return r.json()
}

export async function bulkImport(body: {
  csv_text?: string
  data?: object[]
  exchange?: string
  universe_sets?: string[]
}): Promise<{ status: string; processed: number; inserted: number; errors: number }> {
  const r = await fetch(`${API}/api/universe/bulk-import`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ ...body, added_by: 'admin' }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Import failed')
  }
  return r.json()
}

// ── Pipeline endpoints ─────────────────────────────────────────────────────────

export async function fetchPipelineStatus(): Promise<PipelineStatus> {
  const r = await fetch(`${API}/api/jobs/pipeline`)
  if (!r.ok) throw new Error('Pipeline status unavailable')
  return r.json()
}

export async function triggerPipeline(secret: string): Promise<{ status: string; message: string }> {
  const r = await fetch(`${API}/api/jobs/run`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ secret }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || 'Pipeline trigger failed')
  }
  return r.json()
}

export async function fetchDataFreshness(): Promise<DataFreshness> {
  const r = await fetch(`${API}/api/jobs/status`)
  if (!r.ok) throw new Error('Freshness unavailable')
  return r.json()
}

// ── Auth endpoints ─────────────────────────────────────────────────────────────

export async function fetchKiteStatus(): Promise<KiteStatus> {
  const r = await fetch(`${API}/api/admin/kite/status`)
  if (!r.ok) throw new Error('Kite status unavailable')
  return r.json()
}

export async function refreshKiteToken(
  requestToken: string,
  secret: string
): Promise<{ status: string; token_preview: string; railway_updated: boolean; message: string }> {
  const r = await fetch(`${API}/api/admin/kite/refresh-token`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ request_token: requestToken, secret }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(err.detail || err.message || 'Token refresh failed')
  }
  return r.json()
}
