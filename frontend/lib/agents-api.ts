// Agent Platform API client — the read-only Chart Agent surface (/api/agents/chart/*).
// Same-origin base (NEXT_PUBLIC_API_URL || ''); the next.config rewrite proxies
// /api/agents/* to the backend. In production NEXT_PUBLIC_API_URL is the absolute API host.
//
// HONESTY: every value rendered comes straight from these endpoints. Nothing is fabricated.
// The pattern LIBRARY and its readiness come from the LIVE manifest (fetchManifest), NOT a
// hardcoded constant. At current sample sizes the per-stock decision is usually WATCH — the
// client never invents a TRADE.
const API = process.env.NEXT_PUBLIC_API_URL || ''

// --------------------------------------------------------------------------- manifest
export type PatternManifest = {
  pattern_id: string
  name: string
  status: 'built' | 'spec' | string   // built = active detector; spec = advertised, not yet live
}
export type ManifestResp = {
  agent_id: string
  name: string
  agent_class?: string
  universe?: string
  timeframe?: string
  schedule?: string
  outputs?: string[]
  tracking?: number[]
  execution?: Record<string, unknown>
  permissions?: Record<string, unknown>
  patterns: PatternManifest[]
  version?: string
}

// --------------------------------------------------------------------------- scanner
export type Stage = 'BREAKOUT' | 'RETEST' | 'APPROACHING' | 'FAILED' | string
export type Served = 'precompute' | 'pending' | 'live' | string

export type ScanRow = {
  stock: string
  pattern: string
  stage: Stage
  level: number | null
  distance_pct: number | null
  volume_x: number | null
  direction: string          // long | short
  touches: number
  as_of_date: string | null
}
export type ScanResp = {
  ok: boolean
  date: string | null
  served?: Served
  count: number
  occurrences: ScanRow[]
  // coverage accounting — surfaced so the universe-vs-classified gap is labelled, never hidden
  universe_size?: number
  scanned?: number
  skipped_min_bars?: number
  skipped_stale?: number
  skipped_no_window?: number
  trading_day?: boolean
  note?: string
  screen_note?: string | null
  error?: string
}

// --------------------------------------------------------------------------- decision / storyline
export type Gate = { gate: string; pass: boolean | null; reason: string; skipped?: boolean }
export type StrategyHead = {
  version?: string
  etv?: number | null
  win?: number | null
  payoff?: number | null
  n?: number | null
  avg_win?: number | null
  avg_loss?: number | null
  mae?: number | null
  ci_low?: number | null
  exits?: Record<string, number>
  avg_holding?: number | null
} | null
export type ForwardRow = { win: number | null; etv: number | null; mfe: number | null; mae: number | null }
export type PatternForward = { n?: number | null; horizons: Record<string, ForwardRow> } | null
export type Occurrence = {
  pattern?: string; stock?: string; stage?: string; level?: number | null
  signal_idx?: number; entry_idx?: number; touches?: number[]; direction?: string
  context?: { distance_to_level_pct?: number; volume_x?: number; as_of_date?: string | null }
} | null
export type Policy = Record<string, unknown> | null

export type DecisionResp = {
  ok: boolean
  symbol: string
  date: string | null
  decision: 'TRADE' | 'WATCH' | 'NO_TRADE' | null
  reason?: string
  basis?: string | null
  spec_note?: string | null
  strategy: StrategyHead
  pattern_forward: PatternForward
  gates: Gate[]
  policy: Policy
  occurrence: Occurrence
  note?: string
  error?: string
}

export type StoryEvent = {
  kind: 'level' | 'breakout' | 'retest' | 'approaching' | 'stage' | 'evidence' | 'decision' | 'watch' | string
  title: string
  detail: string
  basis?: string | null
  spec_note?: string | null
}
export type StorylineResp = {
  ok: boolean
  symbol: string
  date: string | null
  decision?: string | null
  reason?: string
  stage?: string | null
  level?: number | null
  events: StoryEvent[]
  note?: string
  error?: string
}

// --------------------------------------------------------------------------- fetchers
async function getJSON<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: 'no-store' })
  // The backend is guarded (returns honest ok:false JSON, not a 500), so we parse either way.
  return r.json() as Promise<T>
}

export const CHART_AGENT_ID = 'chart-v1'

export function fetchManifest(): Promise<ManifestResp> {
  return getJSON<ManifestResp>(`/api/agents/${CHART_AGENT_ID}`)
}

export function fetchScan(date: string, opts?: { full?: boolean; limit?: number }): Promise<ScanResp> {
  const q = new URLSearchParams()
  if (date) q.set('date', date)
  if (opts?.full) q.set('full', '1')
  else q.set('limit', String(opts?.limit ?? 500))
  return getJSON<ScanResp>(`/api/agents/chart/scan?${q.toString()}`)
}

export function fetchDecision(symbol: string, date: string): Promise<DecisionResp> {
  const q = new URLSearchParams({ symbol })
  if (date) q.set('date', date)
  return getJSON<DecisionResp>(`/api/agents/chart/decision?${q.toString()}`)
}

export function fetchStoryline(symbol: string, date: string): Promise<StorylineResp> {
  const q = new URLSearchParams({ symbol })
  if (date) q.set('date', date)
  return getJSON<StorylineResp>(`/api/agents/chart/storyline?${q.toString()}`)
}

// Last-known-good populated screen — the FINAL fallback if none of the probed recent dates is
// precomputed (e.g. a brand-new environment). Normally the mount probe lands on the freshest date.
// (A backend "latest available screen date" endpoint would remove all client probing — see backend-needs.)
export const KNOWN_POPULATED_DATE = '2026-07-31'

const isoDaysAgo = (n: number) => {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - n)
  return d.toISOString().slice(0, 10)
}

/**
 * Find the FRESHEST precomputed screen without a server-side "latest" endpoint (interim, no-outage).
 * Probes the last `days` calendar days from today backward IN PARALLEL (covers weekends/holidays),
 * and resolves to the most-recent date whose scan returned served="precompute" (with setups).
 * Returns { date, scan } for that date, or null if none of the probed days is populated.
 */
export async function findFreshestScan(days = 7): Promise<{ date: string; scan: ScanResp } | null> {
  const dates = Array.from({ length: days }, (_, i) => isoDaysAgo(i)) // today, yesterday, …
  const results = await Promise.all(
    dates.map(async (d) => {
      try {
        const scan = await fetchScan(d, { full: true })
        return { date: d, scan }
      } catch {
        return null
      }
    }),
  )
  // dates[] is already newest→oldest, so the first qualifying hit is the freshest.
  for (const r of results) {
    if (r && r.scan.ok && r.scan.served === 'precompute' && r.scan.count > 0) return r
  }
  return null
}

// --------------------------------------------------------------------------- display metadata
// Canonical direction per pattern — the detectors carry this as a class attribute but the manifest
// does not (yet) surface it, so this is a static DISPLAY label (never market data). Unknown/newly
// added patterns fall through to 'either', which is honest (direction resolves from the break).
export const PATTERN_DIRECTION: Record<string, 'long' | 'short' | 'either'> = {
  horizontal_trendline: 'long',
  ascending_triangle: 'long',
  descending_triangle: 'short',
  symmetrical_triangle: 'either',
  rising_wedge: 'short',
  falling_wedge: 'long',
  rectangle: 'either',
  channel: 'either',
  cup_and_handle: 'long',
}

// A short human label for each pattern id (falls back to the manifest name, trimmed of its
// parenthetical, if a pattern is not in this map — so new backend patterns still render sanely).
export const PATTERN_SHORT: Record<string, string> = {
  horizontal_trendline: 'Horizontal Trendline',
  ascending_triangle: 'Ascending Triangle',
  descending_triangle: 'Descending Triangle',
  symmetrical_triangle: 'Symmetrical Triangle',
  rising_wedge: 'Rising Wedge',
  falling_wedge: 'Falling Wedge',
  rectangle: 'Rectangle',
  channel: 'Channel',
  cup_and_handle: 'Cup & Handle',
}

export function patternShort(id: string, fallbackName?: string): string {
  if (PATTERN_SHORT[id]) return PATTERN_SHORT[id]
  if (fallbackName) return fallbackName.split(/[·(]/)[0].trim()
  return id
}
