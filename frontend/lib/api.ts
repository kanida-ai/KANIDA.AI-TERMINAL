// KANIDA.AI Terminal — API client
// All calls go to the FastAPI backend.
// Change NEXT_PUBLIC_API_URL in .env.local for production.

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

export type FeedCard = {
  ticker:           string
  market:           string
  signalType:       string
  headline:         string
  subline:          string
  triggerReason:    string
  suggestedPrompts: string[]
  conviction:       'HIGH' | 'MEDIUM' | 'LOW'
  edgeScore:        number
  price:            number | null
  regime:           string
  regimeScore:      number
  avgWinRate:       number
  avgOutcome:       number
  primaryBias:      string
  totalSignals:     number
  snapshotDate:     string
  // Counter-trend intelligence — populated by the feed endpoint
  isCounterTrend:  boolean
  dominantBias:    string
  dominantWinRate: number   // decimal 0–1
  dominantSignals: number
}

export type FeedGroup = {
  signalType: string
  label:      string
  sub:        string
  bull:       boolean
  count:      number
  tickers:    string[]
  cards:      FeedCard[]
}

export type ScreenerRow = {
  rank:       number
  ticker:     string
  conviction: string
  win_rate:   number
  signals:    number
  avg_gain:   number
  regime:     string
  price:      number | null
}

export type Pattern = {
  category:    string
  win_rate:    number
  avg_gain:    number
  occurrences: number
}

export type TickerData = {
  ticker:       string
  market:       string
  primary_bias: string
  conviction:   string
  regime: {
    regime:       string
    regime_score: number
    regime_label: string
    snapshot_date: string
  } | null
  bullish: {
    conviction:    string
    avg_win_rate:  number
    best_win_rate: number
    avg_gain:      number
    best_gain:     number
    worst_loss:    number
    total_signals: number
    data_depth:    string
    patterns:      Pattern[]
  } | null
  bearish: {
    conviction:    string
    avg_win_rate:  number
    avg_gain:      number
    total_signals: number
  } | null
  levels: {
    price:     number | null
    target_1:  number | null
    target_2:  number | null
    stop_loss: number | null
    t1_pct:    number | null
    t2_pct:    number | null
    sl_pct:    number | null
    levels_bias: string
    rr:        number | null
  } | null
  // What Bot 2 detected in the most recent snapshot for this ticker
  active_signal: {
    bias:            string
    score_pct:       number
    score_label:     string
    firing_count:    number
    qualified_total: number
    top_strategy:    string
    top_win_rate:    number
    snapshot_time:   string
    snapshot_date:   string
    // Wired up from agent_signal_snapshots — what timeframe + regime the signal fired on
    timeframe:       string
    regime:          string | null
    regime_score:    number | null
    // From paper_ledger — real avg gain on winners / real avg loss on losers
    avg_win:         number | null
    avg_loss:        number | null
    // Consecutive trading days this bias has been active (Option A freshness)
    signal_age_days: number | null
  } | null
  // Dominant long-term edge + counter-trend status
  historical_context: {
    dominant_bias:          string
    dominant_win_rate:      number   // percentage, e.g. 90.5
    dominant_total_signals: number
    is_counter_trend:       boolean
  } | null
}

export type StrategyRow = {
  ticker:      string
  pattern:     string
  timeframe:   string
  bias:        string
  win_rate:    number
  avg_gain:    number
  conviction:  string
  occurrences: number
}

export type ChatResponse = {
  type:      string
  ticker?:   string
  market?:   string
  response:  string
  data?:     Record<string, unknown>
  rows?:     unknown[]
  count?:    number
}

export type Health = {
  status:               string
  fingerprints:         number
  paper_trades:         number
  tickers:              number
  snapshots:            number
  snapshot_date:        string | null
  snapshot_age_minutes: number | null
  snapshot_stale:       boolean
}

export type SnapshotStatusRow = {
  market:      string
  bias:        string
  latest_date: string | null
  latest_time: string | null
  stocks:      number
  age_minutes: number | null
  stale:       boolean
}

export type SnapshotStatus = {
  today:              string
  snapshots:          SnapshotStatusRow[]
  build_running:      boolean
  last_build_started: string | null
  last_build_result:  Record<string, unknown> | null
}

async function req<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${API}${path}`, opts)
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`)
  return res.json()
}

export const getHealth         = () => req<Health>('/api/health')
export const getSnapshotStatus = () => req<SnapshotStatus>('/api/snapshot/status')
export const triggerSnapshotBuild = (market = 'NSE', bias = 'ALL') =>
  req<{ status: string; message: string }>('/api/snapshot/build', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market, bias }),
  })
export const getFeed      = (market = 'NSE', limit = 20) =>
  req<{ cards: FeedCard[] }>(`/api/feed?market=${market}&limit=${limit}`)
    .then(d => d.cards ?? [])
export const getFeedGrouped = (market = 'NSE') =>
  req<{ groups: FeedGroup[]; total: number }>(`/api/feed/grouped?market=${market}`)
    .then(d => d.groups ?? [])
export const getScreener    = (market = 'NSE', bias = 'bullish') =>
  req<{ rows: ScreenerRow[] }>(`/api/screener?market=${market}&bias=${bias}`)
    .then(d => d.rows ?? [])
export const getStrategies  = (market = 'NSE', bias = 'bullish') =>
  req<{ rows: StrategyRow[] }>(`/api/strategies?market=${market}&bias=${bias}`)
    .then(d => d.rows ?? [])
export const getTicker    = (ticker: string, market = 'NSE') =>
  req<TickerData>(`/api/ticker/${ticker}?market=${market}`)
export const sendChat     = (
  message: string,
  history: { role: string; content: string }[] = [],
  ticker?: string,
  market?: string,
) => req<ChatResponse>('/api/chat', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ message, history, ticker, market }),
})
