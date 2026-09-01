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

// tier = the agent's qualitative confidence band for a single setup.
//   qualified 🔥 · strong ⚡ · watch 👀 · weak ⚠.  When the backend does not (yet)
//   emit a tier the client falls back to 'watch' (neutral) — it never promotes a
//   setup to a higher band than the data supports.
export type Tier = 'qualified' | 'strong' | 'watch' | 'weak' | string
export type EvidenceSummary = { n?: number | null; win_t5?: number | null; etv_t5?: number | null } | null

export type ScanRow = {
  stock: string
  pattern: string
  stage: Stage
  level: number | null
  distance_pct: number | null
  volume_x: number | null
  direction: string          // long | short
  touches: number
  as_of_date?: string | null
  // NEW contract fields (present once the ranked-scan backend lands; guarded everywhere)
  tier?: Tier | null
  quality_score?: number | null       // 0..100
  evidence_summary?: EvidenceSummary
  hook?: string | null                // the pre-baked storyline one-liner
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
  statistically_meaningful?: number   // NEW: how many setups clear the min-sample bar
  qualified?: number                  // NEW: how many pass every gate
  by_pattern?: Record<string, number> // NEW: per-pattern occurrence counts
  skipped_min_bars?: number
  skipped_stale?: number
  skipped_no_window?: number
  trading_day?: boolean
  note?: string
  screen_note?: string | null
  error?: string
}

// tier display metadata (glyph + tone). Kept here so the left/middle/right columns agree.
export const TIER_META: Record<string, { glyph: string; label: string; tone: 'green' | 'amber' | 'neutral' | 'red' }> = {
  qualified: { glyph: '🔥', label: 'Qualified', tone: 'green' },
  strong:    { glyph: '⚡', label: 'Strong',    tone: 'amber' },
  watch:     { glyph: '👀', label: 'Watch',     tone: 'neutral' },
  weak:      { glyph: '⚠',  label: 'Weak',      tone: 'red' },
}
export function tierMeta(t?: Tier | null) {
  return (t && TIER_META[t]) || TIER_META.watch
}
export const TIER_RANK: Record<string, number> = { qualified: 0, strong: 1, watch: 2, weak: 3 }

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

// Coarse pattern FAMILY (for the middle-column pattern filter: Triangle/Wedge/Channel/Horizontal/Cup).
export type PatternFamily = 'Triangle' | 'Wedge' | 'Channel' | 'Horizontal' | 'Cup' | 'Other'
export function patternFamily(id: string): PatternFamily {
  if (/triangle/.test(id)) return 'Triangle'
  if (/wedge/.test(id)) return 'Wedge'
  if (/channel/.test(id)) return 'Channel'
  if (/cup/.test(id)) return 'Cup'
  if (/horizontal|rectangle|trendline/.test(id)) return 'Horizontal'
  return 'Other'
}

// --------------------------------------------------------------------------- setup (deep dive)
// GET /api/agents/chart/setup?symbol=&pattern=&date=  — everything the RIGHT column renders.
export type GeomPoint = { date: string; price: number }
export type GeomLine = { a: GeomPoint; b: GeomPoint } | null
export type LevelLine = { from?: GeomPoint; to?: GeomPoint; a?: GeomPoint; b?: GeomPoint } | null
export type SetupGeometry = {
  upper?: GeomLine
  lower?: GeomLine
  touches?: GeomPoint[]
  breakout?: GeomPoint | null
  level_line?: LevelLine
  apex?: GeomPoint | null
} | null
export type QualitySub = Record<string, number | null>
export type SetupQuality = { score?: number | null; subscores?: QualitySub; weights?: QualitySub; note?: string } | null
// A horizon row carries the LIVE fields (win / mean-return / median / mfe / mae).
// `etv` is kept for back-compat; the live API returns the average return under `mean`.
export type SetupHorizon = {
  win?: number | null
  etv?: number | null
  mean?: number | null
  median?: number | null
  p25?: number | null
  p75?: number | null
  mfe?: number | null
  mae?: number | null
}
export type EvidenceSummaryBlock = {
  n?: number | null
  ref_h?: number | null
  pct_up?: number | null
  pct_down?: number | null
  avg_up?: number | null
  avg_down?: number | null
} | null
export type SetupEvidence = {
  n?: number | null
  win_rate?: number | null
  etv?: number | null
  payoff?: number | null
  mae?: number | null
  mfe?: number | null
  ci_low?: number | null
  ref_h?: number | null
  summary?: EvidenceSummaryBlock
  horizons?: Record<string, SetupHorizon>   // "1".."10"
} | null
export type SetupPaths = {
  winners?: number[] | null
  losers?: number[] | null
  n_win?: number | null
  n_loss?: number | null
  n_total?: number | null
  small_n?: boolean
  note?: string
} | null
// Live gate rows use {gate, pass, reason}; the earlier draft used {name, passed, detail}.
// Both variants are accepted and normalised in the UI.
export type SetupGate = {
  name?: string
  passed?: boolean | null
  detail?: string
  gate?: string
  pass?: boolean | null
  reason?: string
  skipped?: boolean
}
export type SetupStrategy = {
  version?: string
  n?: number | null
  etv?: number | null
  win?: number | null
  payoff?: number | null
  avg_win?: number | null
  avg_loss?: number | null
  mae?: number | null
  ci_low?: number | null
  avg_holding?: number | null
} | null
export type SetupDecision = {
  verdict?: string | null
  decision?: string | null      // live field name for the verdict
  reason?: string
  gates?: SetupGate[]
  basis?: string | null
  etv?: number | null
  edge?: number | null
  strategy?: SetupStrategy
} | null
export type WatchPlan = {
  confirmation?: number | null
  warning?: number | null
  invalidation?: number | null
  direction?: string
  note?: string
} | null
export type SetupResp = {
  ok?: boolean
  symbol?: string
  pattern?: string
  date?: string | null
  as_of_date?: string | null
  stage?: string
  direction?: string
  level?: number | null
  sector?: string | null
  context?: { distance_to_level_pct?: number | null; volume_x?: number | null; as_of_date?: string | null }
  geometry?: SetupGeometry
  quality?: SetupQuality
  evidence?: SetupEvidence
  paths?: SetupPaths
  decision?: SetupDecision
  watch_plan?: WatchPlan
  bars?: Bar[]                   // embedded point-in-time candles (precomputed → instant)
  note?: string
  error?: string
}

// The live verdict lives under decision.decision (interim) OR decision.verdict.
export function setupVerdict(d?: SetupDecision): string | null {
  if (!d) return null
  return (d.decision ?? d.verdict ?? null)
}

// --------------------------------------------------------------------------- bars (candles)
export type Bar = { date: string; o: number; h: number; l: number; c: number; v: number }
export type BarsResp = { ok?: boolean; symbol?: string; date?: string | null; bars?: Bar[]; note?: string; error?: string }

// Guarded fetch: returns a shaped error object instead of throwing, so the page
// renders honest empty/coming states even while /setup and /bars are 404 during
// the parallel backend build.
async function getJSONSafe<T>(path: string, onErr: () => T): Promise<T> {
  try {
    const r = await fetch(`${API}${path}`, { cache: 'no-store' })
    if (!r.ok) return onErr()
    return (await r.json()) as T
  } catch {
    return onErr()
  }
}

export function fetchSetup(symbol: string, pattern: string, date: string): Promise<SetupResp> {
  const q = new URLSearchParams({ symbol, pattern })
  if (date) q.set('date', date)
  return getJSONSafe<SetupResp>(`/api/agents/chart/setup?${q.toString()}`,
    () => ({ ok: false, symbol, pattern, error: 'setup-endpoint-unavailable' }))
}

export function fetchBars(symbol: string, date: string, lookback = 90): Promise<BarsResp> {
  const q = new URLSearchParams({ symbol, lookback: String(lookback) })
  if (date) q.set('date', date)
  return getJSONSafe<BarsResp>(`/api/agents/chart/bars?${q.toString()}`,
    () => ({ ok: false, symbol, bars: [], error: 'bars-endpoint-unavailable' }))
}
