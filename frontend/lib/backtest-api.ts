const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export type StockOverview = {
  ticker:                    string
  total_trades:              number
  wins:                      number
  losses:                    number
  win_rate:                  number
  avg_pnl:                   number
  avg_win:                   number
  avg_loss:                  number
  turbo:                     number
  super:                     number
  standard:                  number
  trap:                      number
  best_year:                 string
  avg_competing_patterns:    number
  by_year: { year: string; total: number; wins: number; win_rate: number }[]
}

export type OverviewResponse = {
  stocks:           StockOverview[]
  total_trades:     number
  overall_win_rate: number
  overall_avg_pnl:  number
  data_note:        string
  timeframe_note:   string
}

export type Trade = {
  trade_id:              number
  signal_id:             string
  ticker:                string
  market:                string
  year:                  string
  timeframe:             string
  signal_type:           string
  pattern:               string
  direction:             string
  signal_date:           string
  signal_datetime:       string
  entry_date:            string
  entry_datetime:        string
  signal_to_entry_mins:  number | null
  delay_label:           string
  entry_price:           number
  stop_price:            number
  target_price:          number
  rr:                    number
  exit_date:             string
  exit_price:            number
  exit_reason:           string
  days_held:             number
  pnl_pct:               number
  bucket:                string
  mfe_pct:               number | null
  mae_pct:               number | null
  mpi_pct:               number | null
  post_5d_pct:           number | null
  reason_code:           string
  multi_pattern_count:   number
  opportunity_score:     number | null
  tier:                  string
  credibility:           string
}

export type Combination = {
  signal_type:  string
  pattern:      string
  full_pattern: string
  win_rate:     number
  avg_return:   number
  total:        number
  wins:         number
  category:     string
  tickers:      string[]
}

export type MpiTrade = {
  trade_id:        number
  signal_id:       string
  ticker:          string
  signal_datetime: string
  entry_datetime:  string
  exit_date:       string
  direction:       string
  entry_price:     number
  exit_price:      number
  booked_pct:      number
  continued_pct:   number
  total_available: number
  missed_pct:      number
  pattern:         string
  signal_type:     string
  timeframe:       string
  reason_code:     string
}

export type MpiRecommendation = {
  ticker:              string
  signal_type:         string
  total_tp_trades:     number
  avg_booked_pct:      number
  avg_mpi_pct:         number
  max_mpi_pct:         number
  high_mpi_rate:       number
  action:              string
  action_code:         string
  rationale:           string
  simulated_avg_pnl:   number
  extra_gain_pct:      number
}

export type BucketStat = {
  label:            string
  description:      string
  total:            number
  win_rate:         number
  avg_return:       number
  avg_days_to_exit: number
  avg_mfe:          number
  avg_continuation: number | null
  by_ticker:        Record<string, number>
}

async function req<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { cache: 'no-store' })
  if (!res.ok) throw new Error(`${path} → ${res.status}`)
  return res.json()
}

export const getOverview        = ()                                =>
  req<OverviewResponse>('/api/backtest/overview')

export const getTrades          = (ticker?: string, year?: string, bucket?: string) => {
  const p = new URLSearchParams()
  if (ticker) p.set('ticker', ticker)
  if (year)   p.set('year', year)
  if (bucket) p.set('bucket', bucket)
  const qs = p.toString()
  return req<{ count: number; trades: Trade[] }>(`/api/backtest/trades${qs ? '?' + qs : ''}`)
}

export const getCombinations    = (ticker?: string)                =>
  req<{ count: number; combinations: Combination[] }>(`/api/backtest/combinations${ticker ? '?ticker=' + ticker : ''}`)

export const getMissedProfit    = (ticker?: string)                =>
  req<{ count: number; avg_missed_pct: number; trades: MpiTrade[] }>(`/api/backtest/missed-profit${ticker ? '?ticker=' + ticker : ''}`)

export const getMpiRecommendations = (ticker?: string)             =>
  req<{ count: number; recommendations: MpiRecommendation[] }>(`/api/backtest/mpi-recommendations${ticker ? '?ticker=' + ticker : ''}`)

export const getBuckets         = (ticker?: string)                =>
  req<{ buckets: Record<string, BucketStat> }>(`/api/backtest/buckets${ticker ? '?ticker=' + ticker : ''}`)

// ── Live Positions ────────────────────────────────────────────────────────────

export type LivePosition = {
  trade_id:            number
  signal_id:           string
  ticker:              string
  signal_type:         string
  pattern:             string
  direction:           string
  bucket:              string
  timeframe:           string
  signal_date:         string
  signal_datetime:     string
  entry_date:          string
  entry_datetime:      string
  delay_label:         string
  entry_price:         number
  stop_price:          number
  target_price:        number
  rr:                  number
  pct_to_tp:           number
  pct_to_sl:           number
  current_price:       number | null
  live_pnl_pct:        number
  live_pnl_rs:         number
  days_open:           number
  days_left:           number
  max_hold_days:       number
  status_code:         string    // pending_entry | open | near_target | near_stop | target_hit | stop_hit | expired
  status_label:        string
  opportunity_score:   number | null
  reason_code:         string
  multi_pattern_count: number
  tier:                string
  credibility:         string
}

export type MarketInfo = {
  status:             string
  label:              string
  ist_time:           string
  ist_date:           string
  ist_datetime:       string
  is_open:            boolean
  mins_to_open:       number | null
  mins_to_close:      number | null
  refresh_interval_s: number
}

export type LivePositionsResponse = {
  market:          MarketInfo
  current_prices:  Record<string, number | null>
  summary:         Record<string, number>
  positions:       LivePosition[]   // actionable: pending_entry, open, near_target, near_stop
  closed:          LivePosition[]   // resolved:   target_hit, stop_hit, expired
  alerts:          LivePosition[]
  total_active:    number
  total_closed:    number
  as_of:           string
}

export type LiveHistoryItem = {
  trade_id:      number
  signal_id:     string
  ticker:        string
  signal_type:   string
  pattern:       string
  direction:     string
  bucket:        string
  entry_date:    string
  exit_date:     string
  entry_price:   number
  exit_price:    number
  pnl_pct:       number
  exit_reason:   string
  days_held:     number
  status_code:   string
  status_label:  string
  mfe_pct:       number | null
  mae_pct:       number | null
  cumulative_pnl: number
}

export type LiveHistoryResponse = {
  total:    number
  wins:     number
  losses:   number
  win_rate: number
  history:  LiveHistoryItem[]
}

export const getLivePositions = (ticker?: string, bucket?: string) => {
  const p = new URLSearchParams()
  if (ticker) p.set('ticker', ticker)
  if (bucket) p.set('bucket', bucket)
  const qs = p.toString()
  return req<LivePositionsResponse>(`/api/live/positions${qs ? '?' + qs : ''}`)
}

export const getLiveHistory   = (ticker?: string) =>
  req<LiveHistoryResponse>(`/api/live/history${ticker ? '?ticker=' + ticker : ''}`)

// ── Execution Intelligence ────────────────────────────────────────────────────

export type ExecTickerStat = {
  ticker:         string
  total:          number
  taken:          number
  taken_pct:      number
  blind_avg_pnl:  number
  smart_avg_pnl:  number
  blind_win_rate: number
  smart_win_rate: number
}

export type ExecDistItem = {
  exec_code: string
  count:     number
  pct:       number
}

export type ExecGapItem = {
  category: string
  count:    number
}

export type ExecSummaryResponse = {
  total:               number
  taken:               number
  skipped:             number
  taken_pct:           number
  blind_avg_pnl:       number
  blind_win_rate:      number
  smart_avg_pnl:       number
  smart_win_rate:      number
  avg_pnl_improvement: number
  avg_gap_pct:         number
  nifty_weak_days:     number
  exec_distribution:   ExecDistItem[]
  per_ticker:          ExecTickerStat[]
  gap_distribution:    ExecGapItem[]
}

export type ExecTrade = {
  id:                number
  trade_log_id:      number
  ticker:            string
  direction:         string
  signal_date:       string
  entry_date:        string
  exec_code:         string
  trade_taken:       boolean
  entry_window:      string | null
  exec_notes:        string
  gap_pct:           number
  gap_category:      string
  day_move_pct:      number
  day_range_pct:     number
  nifty_day_move:    number | null
  nifty_is_weak:     boolean
  prev_close:        number
  entry_open:        number
  entry_high:        number
  entry_low:         number
  entry_close:       number
  blind_entry_price: number
  smart_entry_price: number | null
  exit_price:        number
  blind_pnl_pct:     number
  smart_pnl_pct:     number | null
  pnl_improvement:   number | null
  pattern:           string
  signal_type:       string
  bucket:            string
  tier:              string
}

export type ExecComparisonRow = {
  exec_code?:    string
  gap_category?: string
  direction?:    string
  total:         number
  taken?:        number
  blind_avg:     number
  smart_avg:     number | null
  blind_wr:      number
  smart_wr:      number | null
}

export type ExecMonthlyItem = {
  month:      string
  total:      number
  blind_avg:  number
  smart_avg:  number | null
}

export type ExecComparisonResponse = {
  by_exec_code:    ExecComparisonRow[]
  by_gap_category: ExecComparisonRow[]
  by_direction:    ExecComparisonRow[]
  monthly_trend:   ExecMonthlyItem[]
}

export const getExecSummary    = (ticker?: string) =>
  req<ExecSummaryResponse>(`/api/execution/summary${ticker ? '?ticker=' + ticker : ''}`)

export const getExecTrades     = (ticker?: string, execCode?: string, taken?: number) => {
  const p = new URLSearchParams()
  if (ticker)   p.set('ticker', ticker)
  if (execCode) p.set('exec_code', execCode)
  if (taken !== undefined) p.set('trade_taken', String(taken))
  const qs = p.toString()
  return req<{ count: number; trades: ExecTrade[] }>(`/api/execution/trades${qs ? '?' + qs : ''}`)
}

export const getExecComparison = (ticker?: string) =>
  req<ExecComparisonResponse>(`/api/execution/comparison${ticker ? '?ticker=' + ticker : ''}`)

// ── Swing Trading Terminal ────────────────────────────────────────────────────

export type SwingTopStock = {
  rank:         number
  ticker:       string
  total:        number
  win_rate:     number
  avg_pnl:      number
  total_pnl:    number
  avg_days:     number
  last_trade:   string
  trades_90d:   number
  avg_pnl_90d:  number | null
  active:       boolean
}

export type SwingEngine = {
  bucket:            string
  label:             string
  icon:              string
  description:       string
  total_trades:      number
  win_rate:          number
  smart_win_rate:    number
  avg_pnl:           number
  smart_avg_pnl:     number
  avg_days:          number
  total_pnl_all:     number
  pnl_90d_avg:       number | null
  pnl_90d_trades:    number
  pnl_180d_avg:      number | null
  pnl_180d_trades:   number
  active_signals:    number
  active_tickers:    string[]
  top_stocks:        SwingTopStock[]
}

export type ActiveSignal = {
  ticker:            string
  bucket:            string
  tier:              string
  opportunity_score: number
  credibility:       string
  latest_date:       string
  setup_summary:     string
}

export type SwingOverviewResponse = {
  as_of:           string
  summary: {
    total_long_trades: number
    smart_win_rate:    number
    smart_avg_pnl:     number
    avg_days_held:     number
    active_signals:    number
    first_trade:       string
    last_trade:        string
    hc_trades:         number
    hc_win_rate:       number
    hc_avg_pnl:        number
    hc_total_pnl:      number
  }
  engines:         SwingEngine[]
  active_signals:  ActiveSignal[]
}

export type SwingTrade = {
  id:                     number
  trade_id:               number
  signal_id:              string
  ticker:                 string
  direction:              string
  timeframe:              string
  signal_type:            string
  pattern:                string
  signal_date:            string
  signal_datetime:        string
  entry_date:             string
  entry_datetime:         string
  delay_label:            string
  entry_price:            number
  effective_entry_price:  number
  stop_price:             number
  target_price:           number
  rr:                     number
  exit_date:              string
  exit_price:             number
  exit_reason:            string
  days_held:              number
  pnl_pct:                number
  effective_pnl:          number
  bucket:                 string
  year:                   string
  tier:                   string
  credibility:            string
  opportunity_score:      number | null
  reason_code:            string
  multi_pattern_count:    number
  mfe_pct:                number | null
  mae_pct:                number | null
  mpi_pct:                number | null
  exec_code:              string | null
  trade_taken:            boolean | null
  entry_window:           string | null
  smart_entry_price:      number | null
  smart_pnl_pct:          number | null
  gap_category:           string | null
  gap_pct:                number | null
  day_move_pct:           number | null
  rs_vs_nifty:            number | null
}

export type LeaderboardEntry = {
  rank:         number
  ticker:       string
  bucket:       string
  total_trades: number
  win_rate:     number
  avg_pnl:      number
  total_pnl:    number
  avg_days:     number
  last_trade:   string
  active:       boolean
}

export const getSwingOverview = (
  year?: string,
  ticker?: string,
  index?: string,
) => {
  const qs = new URLSearchParams()
  if (year)   qs.set('year', year)
  if (ticker) qs.set('ticker', ticker)
  if (index)  qs.set('index', index)
  const tail = qs.toString()
  return req<SwingOverviewResponse>(`/api/swing/overview${tail ? '?' + tail : ''}`)
}

export type ActiveSignalRow = {
  ticker:            string
  engine:            'turbo' | 'super' | 'standard'
  tier:              string | null
  opportunity_score: number
  credibility:       string | null
  latest_date:       string | null
  current_close:     number | null
  sector:            string | null
  setup_summary:     string
}
export type ActiveSignalsResponse = { count: number; signals: ActiveSignalRow[] }

export const getActiveSignals = (params: {
  engine?: string; index?: string; sector?: string;
  credibility?: string; search?: string; ticker?: string;
} = {}) => {
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v) qs.set(k, v)
  const tail = qs.toString()
  return req<ActiveSignalsResponse>(`/api/swing/active-signals${tail ? '?' + tail : ''}`)
}

export type IndexInfo = { index_name: string; members: number; last_updated: string }
export const getIndices = () =>
  req<{ indices: IndexInfo[] }>(`/api/universe/indices`)

export const getSwingTickers    = () =>
  req<{ tickers: string[] }>('/api/swing/tickers')

export const getSwingTrades     = (ticker?: string, bucket?: string, year?: string) => {
  const p = new URLSearchParams()
  if (ticker) p.set('ticker', ticker)
  if (bucket) p.set('bucket', bucket)
  if (year)   p.set('year', year)
  const qs = p.toString()
  return req<{ count: number; trades: SwingTrade[] }>(`/api/swing/trades${qs ? '?' + qs : ''}`)
}

export const getSwingLeaderboard = (bucket?: string, period = 'all', sortBy = 'avg_pnl') => {
  const p = new URLSearchParams({ period, sort_by: sortBy })
  if (bucket) p.set('bucket', bucket)
  return req<{ period: string; count: number; leaderboard: LeaderboardEntry[] }>(
    `/api/swing/leaderboard?${p.toString()}`
  )
}

export const getSwingStockProfile = (ticker: string) =>
  req<{ ticker: string; active_signal: ActiveSignal | null; buckets: any[] }>(
    `/api/swing/stock-profile?ticker=${ticker}`
  )
