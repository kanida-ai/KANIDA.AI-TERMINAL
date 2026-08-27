// Agent Platform API client — mirrors lib/builder-api.ts. Drop into frontend/lib/.
// Talks to the read-only Chart Agent surface (/api/agents/chart/*). Same-origin base
// (NEXT_PUBLIC_API_URL || ''); the next.config rewrite proxies /api/agents/* to the backend.
//
// HONESTY: these render the REAL agent output. At current sample sizes the decision is
// almost always WATCH — the client never invents a TRADE. Triangle/Channel + G4
// nested-coherence are SPEC and are labelled as such in the UI, not faked here.
const API = process.env.NEXT_PUBLIC_API_URL || ''

export type ScanRow = {
  stock: string
  pattern: string
  stage: 'BREAKOUT' | 'RETEST' | 'APPROACHING' | 'FAILED' | string
  level: number | null
  distance_pct: number | null
  volume_x: number | null
  direction: string
  touches: number
  as_of_date: string | null
}
export type ScanResp = {
  ok: boolean
  date: string | null
  universe_size?: number
  count: number
  occurrences: ScanRow[]
  note?: string
  error?: string
}

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

async function getJSON<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: 'no-store' })
  // The backend is guarded (returns honest ok:false JSON, not a 500), so we parse either way.
  return r.json() as Promise<T>
}

export function fetchScan(date: string, limit = 40): Promise<ScanResp> {
  const q = new URLSearchParams()
  if (date) q.set('date', date)
  q.set('limit', String(limit))
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

// The default scan universe (mirrors backend agents/chart/data.DEFAULT_UNIVERSE) — used to
// seed the stock picker so the page is useful even before a scan returns.
export const DEFAULT_UNIVERSE = [
  'RELIANCE', 'INFY', 'SBIN', 'TCS', 'HDFCBANK', 'ICICIBANK', 'AXISBANK', 'KOTAKBANK',
  'LT', 'ITC', 'TITAN', 'MARUTI', 'SUNPHARMA', 'TATAMOTORS', 'TATASTEEL', 'HINDALCO',
]

// The Chart Agent pattern library — LIVE vs SPEC, mirrored from the backend manifest so the
// left rail can advertise readiness honestly without a round-trip.
export const CHART_PATTERNS = [
  { id: 'horizontal_trendline', name: 'Horizontal Trendline', status: 'live' as const,
    note: 'Breakout · Retest + Volume' },
  { id: 'triangle', name: 'Triangle', status: 'soon' as const, note: 'detector is SPEC' },
  { id: 'channel', name: 'Channel', status: 'soon' as const, note: 'detector is SPEC' },
]
