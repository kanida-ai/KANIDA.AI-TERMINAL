// KANIDA.AI Dashboard — API client for the signals endpoints.
// Backend lives on the same FastAPI app as the Terminal; these routes are
// mounted under /signals and read kanida_signals.db directly.

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

export type MarketSnapshot = {
  latest_signal_date:         string | null
  latest_bullish_signal_date: string | null
  latest_bearish_signal_date: string | null
  firing_bullish:             number
  firing_bearish:             number
  active_roster:              number
  core_signals:               number
  rare_edges:                 number
}

export type DashboardHealth = {
  markets:     Record<string, MarketSnapshot>
  db_path:     string
  server_time: string
}

export type TopRow = {
  rank:              number
  ticker:            string
  market:            string
  timeframe:         string
  strategy_name:     string
  bias:              string
  tier:              string
  tier_label:        string
  quality_grade:     string | null
  frequency_grade:   string | null
  fitness_score:     number
  size_multiplier:   number
  trend_gate:        string | null
  total_appearances: number
  recent_appearances: number
  win_rate_15d:      number | null
  avg_ret_15d:       number | null
  wilson_lower_15d:  number | null
  avg_mfe_pct:       number | null
  avg_mae_pct:       number | null
  last_signal_date:  string | null
  firing_today:      number
  company_name:      string | null
  sector:            string | null
  latest_signal_date: string | null
  live_score:        number
}

export type TopResponse = {
  market:             string
  bias:               string
  count:              number
  total_candidates:   number
  rows:               TopRow[]
}

export type FiringEvent = {
  timeframe:     string
  strategy_name: string
  bias:          string
  entry_price:   number
  trend_state:   string | null
}

export type HistoryRow = {
  timeframe:       string
  strategy_name:   string
  bias:            string
  entry_date:      string
  entry_price:     number
  stop_price:      number | null
  target_1:        number | null
  exit_date:       string | null
  exit_price:      number | null
  outcome_pct:     number | null
  win:             number | null
  status:          string
  days_held:       number | null
  roster_tier:     string | null
  tier_label?:     string
  size_multiplier?: number
}

export type PaperIdea = {
  timeframe:      string
  strategy_name:  string
  bias:           string
  entry_date:     string
  entry_price:    number
  stop_price:     number | null
  target_1:       number | null
  target_2:       number | null
  roster_tier:    string | null
  tier_label?:    string
  size_multiplier: number | null
  status:         string
}

export type StockDetail = {
  ticker:              string
  market:              string
  company_name:        string | null
  sector:              string | null
  predictability:      number
  roster_active:       number
  roster_top:          number
  latest_signal_date:  string | null
  firing_today:        FiringEvent[]
  ladder:              TopRow[]
  paper_idea:          PaperIdea | null
  history:             HistoryRow[]
}

async function req<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { cache: 'no-store' })
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`)
  return res.json()
}

export const getDashboardHealth = () => req<DashboardHealth>('/signals/health')

export type TopQuery = {
  market?:      'NSE' | 'US' | 'BOTH'
  bias:         'bullish' | 'bearish'
  limit?:       number
  timeframe?:   '1D' | '1W' | ''
  tier?:        string
  firingOnly?:  boolean
  q?:           string
}

export const getTop = (p: TopQuery) => {
  const qs = new URLSearchParams()
  qs.set('market', p.market ?? 'NSE')
  qs.set('bias', p.bias)
  qs.set('limit', String(p.limit ?? 20))
  if (p.timeframe)   qs.set('timeframe', p.timeframe)
  if (p.tier)        qs.set('tier', p.tier)
  if (p.firingOnly)  qs.set('firing_only', 'true')
  if (p.q)           qs.set('q', p.q)
  return req<TopResponse>(`/signals/top?${qs.toString()}`)
}

export const getStockDetail = (ticker: string, market = 'NSE') =>
  req<StockDetail>(`/signals/stock/${encodeURIComponent(ticker)}?market=${market}`)

// ── Insights (homepage headline cards) ───────────────────────────────────────
export type InsightTicker = {
  ticker:           string
  company_name:     string | null
  one_line_reason:  string
  drill_prompt_id:  string
  timeframe?:       string
  tier?:            string
  tier_label?:      string
}

export type InsightBucket = {
  id:        string
  title:     string
  one_liner: string
  accent:    'emerald' | 'violet' | 'rose' | 'amber' | 'sky' | 'zinc'
  count:     number
  tickers:   InsightTicker[]
}

export type MarketInsights = {
  market:                     string
  headline:                   string
  latest_bullish_signal_date: string | null
  latest_bearish_signal_date: string | null
  insights:                   InsightBucket[]
}

export type InsightsResponse =
  | MarketInsights
  | { markets: Record<string, MarketInsights> }

export const getInsights = (market: 'NSE' | 'US' | 'BOTH' = 'NSE') =>
  req<InsightsResponse>(`/signals/insights?market=${market}`)

// ── Smart Prompts (stock detail conversation layer) ──────────────────────────
export type PromptMenuItem = {
  id:       string
  group:    string
  question: string
}

export type PromptList = {
  ticker:              string
  market:              string
  company_name:        string | null
  sector:              string | null
  latest_signal_date:  string | null
  prompts:             PromptMenuItem[]
}

export type PromptEvidence = {
  label:  string
  detail: string
  when?:  string | null
}

export type PromptAnswer = {
  ticker:       string
  market:       string
  prompt_id:    string
  question:     string
  answer:       string
  evidence:     PromptEvidence[]
  next_prompts: { id: string; question: string }[]
}

export const getPromptList = (ticker: string, market = 'NSE') =>
  req<PromptList>(`/signals/prompts/${encodeURIComponent(ticker)}?market=${market}`)

export const getPromptAnswer = (ticker: string, promptId: string, market = 'NSE') =>
  req<PromptAnswer>(
    `/signals/prompts/${encodeURIComponent(ticker)}/answer?prompt_id=${encodeURIComponent(promptId)}&market=${market}`
  )

// ── UI helpers ───────────────────────────────────────────────────────────────

export const TIER_COLORS: Record<string, { bg: string; fg: string; border: string }> = {
  core_active:     { bg: 'bg-emerald-500/15', fg: 'text-emerald-300', border: 'border-emerald-500/40' },
  high_conviction: { bg: 'bg-violet-500/15',  fg: 'text-violet-300',  border: 'border-violet-500/40' },
  steady:          { bg: 'bg-sky-500/15',     fg: 'text-sky-300',     border: 'border-sky-500/40' },
  emerging:        { bg: 'bg-amber-500/15',   fg: 'text-amber-300',   border: 'border-amber-500/40' },
  experimental:    { bg: 'bg-zinc-500/15',    fg: 'text-zinc-300',    border: 'border-zinc-500/40' },
  retired:         { bg: 'bg-red-500/15',     fg: 'text-red-300',     border: 'border-red-500/40' },
}

export const TIER_LABEL: Record<string, string> = {
  core_active:     'Core Signal',
  high_conviction: 'Rare Edge',
  steady:          'Proven',
  emerging:        'Building Track Record',
  experimental:    'Observing',
  retired:         'Stopped Working',
}

export function pct(v: number | null | undefined, digits = 0): string {
  if (v === null || v === undefined) return '–'
  return `${(v * 100).toFixed(digits)}%`
}

export function signedPct(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined) return '–'
  const s = v >= 0 ? '+' : ''
  return `${s}${v.toFixed(digits)}%`
}

export function isMarketOpen(market: string): boolean {
  // Rough check — NSE: Mon–Fri 09:15–15:30 IST; US: Mon–Fri 09:30–16:00 ET.
  const now = new Date()
  const day = now.getUTCDay() // 0=Sun..6=Sat
  if (day === 0 || day === 6) return false
  const utcHH = now.getUTCHours() + now.getUTCMinutes() / 60
  if (market === 'NSE') {
    // IST = UTC+5:30 → 09:15 IST = 03:45 UTC, 15:30 IST = 10:00 UTC
    return utcHH >= 3.75 && utcHH <= 10.0
  }
  if (market === 'US') {
    // ET ≈ UTC−4 (DST) → 09:30 ET = 13:30 UTC, 16:00 ET = 20:00 UTC
    return utcHH >= 13.5 && utcHH <= 20.0
  }
  return false
}
