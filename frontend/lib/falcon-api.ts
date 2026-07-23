// KANIDA.AI Terminal — Falcon API client
// All Falcon endpoints live under /api/falcon/* on the FastAPI backend.

// Route the operator console through the same-origin Falcon proxy
// (app/api/falcon-proxy) so the operator token is injected server-side and
// never reaches the browser. The proxy forwards to NEXT_PUBLIC_API_URL with the
// X-Operator-Token header. The /falcon console is interactive (client-side), so
// a relative same-origin base is correct.
const API = '/api/falcon-proxy'

// ── Types ─────────────────────────────────────────────────────────────────────

export type FalconLiveSignal = {
  signal_date:       string
  entry_date:        string | null
  rank:              number
  symbol:            string
  sector:            string | null
  n_fires:           number
  score:             number
  close_at_signal:   number | null
  avg_value_60d:     number | null
  fired_pattern_ids: number[]
  sample_rules:      Array<{
    pattern_id: number
    target:     string
    oos_lift:   number
    rule:       string
  }>
  engine_version:    string
  emitted_at:        string
}

export type FalconSignalsToday = {
  signal_date:    string
  entry_date:     string | null
  n_picks:        number
  engine_version: string
  picks:          FalconLiveSignal[]
}

export type FalconSignalDay = {
  signal_date: string
  n_picks:     number
  emitted_at:  string
}

export type FalconPattern = {
  pattern_id:               number
  classification:           'universal' | 'sector_specific' | 'regime_dependent'
  mined_year:               string
  scope:                    string
  outcome_target:           string
  n_obs:                    number
  precision_pct:            number
  base_rate_pct:            number
  is_lift_pp:               number
  avg_oos_year_lift_pp:     number
  n_years_passed:           number
  avg_cross_sector_lift_pp: number
  rule_text:                string
}

export type FalconPortfolioSummary = {
  engine_version:    string
  starting_capital:  number
  ending_equity:     number
  total_pnl:         number
  return_pct:        number
  trades_taken:      number
  trades_skipped:    number
  win_rate:          number
  win_loss_ratio:    number | null
  avg_win:           number
  avg_loss:          number
  best_trade:        number
  worst_trade:       number
  max_drawdown_pct:  number
  max_concurrent:    number
  avg_utilization:   number
  yearly_pnl:        Record<string, number>
  monthly_pnl:       Record<string, number>
}

export type FalconPortfolioTrade = {
  symbol:      string
  signal_date: string
  entry_date:  string
  exit_date:   string
  exit_reason: string
  n_fires:     number
  score:       number
  net_pnl:     number
  ret_pct:     number
}

export type FalconStatus = {
  engine_version:      string
  db_path:             string
  db_size_mb:          number
  n_promoted_patterns: number
  n_signals_emitted:   number
  latest_signal_date:  string | null
  n_features_rows:     number
  n_outcomes_rows:     number
  // Phase 2 — pattern sync indicator (last B4 publish_patterns.py run)
  patterns_last_published:         string | null
  patterns_last_published_status:  'success' | 'failed' | null
  patterns_last_published_notes:   string | null
}

export type FalconJobRun = {
  id:            number
  job_name:      string
  started_at:    string
  finished_at:   string | null
  status:        'running' | 'success' | 'failed'
  rows_affected: number | null
  notes:         string | null
  error:         string | null
}

// ── Endpoints ────────────────────────────────────────────────────────────────

async function _extractErrorDetail(r: Response): Promise<string> {
  try {
    const text = await r.text()
    if (!text) return ''
    try {
      const j = JSON.parse(text)
      const d = j?.detail
      if (typeof d === 'string') return d
      if (d) return JSON.stringify(d)
      return text.slice(0, 300)
    } catch {
      return text.slice(0, 300)
    }
  } catch {
    return ''
  }
}

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: 'no-store' })
  if (!r.ok) {
    const detail = await _extractErrorDetail(r)
    throw new Error(detail ? `${path} → ${r.status}: ${detail}` : `${path} → ${r.status}`)
  }
  return r.json()
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const r = await fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!r.ok) {
    const detail = await _extractErrorDetail(r)
    throw new Error(detail ? `${path} → ${r.status}: ${detail}` : `${path} → ${r.status}`)
  }
  return r.json()
}

export const FalconAPI = {
  signalsToday: (topN = 100) =>
    get<FalconSignalsToday>(`/api/falcon/signals/today?top_n=${topN}`),

  signalsByDate: (date: string, topN = 25) =>
    get<FalconSignalsToday>(`/api/falcon/signals/by-date?date=${date}&top_n=${topN}`),

  signalDates: (limit = 60) =>
    get<FalconSignalDay[]>(`/api/falcon/signals/dates?limit=${limit}`),

  signalsForStock: (symbol: string, limit = 60) =>
    get<FalconLiveSignal[]>(`/api/falcon/signals/stock/${symbol}?limit=${limit}`),

  patterns: (params: {
    limit?: number
    classification?: string
    target?: string
    minOosLift?: number
    sort?: 'oos_lift' | 'is_lift' | 'n_obs'
  } = {}) => {
    const q = new URLSearchParams()
    if (params.limit) q.set('limit', String(params.limit))
    if (params.classification) q.set('classification', params.classification)
    if (params.target) q.set('target', params.target)
    if (params.minOosLift !== undefined) q.set('min_oos_lift', String(params.minOosLift))
    if (params.sort) q.set('sort', params.sort)
    return get<FalconPattern[]>(`/api/falcon/patterns?${q.toString()}`)
  },

  patternStats: () =>
    get<{
      total_promoted: number
      by_classification: Array<{ classification: string; n: number }>
      by_target:         Array<{ outcome_target: string; n: number }>
      by_mined_year:     Array<{ mined_year: string; n: number }>
    }>(`/api/falcon/patterns/stats`),

  portfolioSummary: () =>
    get<FalconPortfolioSummary>(`/api/falcon/portfolio/summary`),

  portfolioTrades: (limit = 200) =>
    get<FalconPortfolioTrade[]>(`/api/falcon/portfolio/trades?limit=${limit}`),

  adminStatus: () =>
    get<FalconStatus>(`/api/falcon/admin/status`),

  adminRuns: (limit = 30) =>
    get<FalconJobRun[]>(`/api/falcon/admin/runs?limit=${limit}`),

  adminRerun: (jobName: string) =>
    post<{ status: string; job_name: string; tip: string }>(`/api/falcon/admin/rerun/${jobName}`),

  inbox: (limit = 20, unreadOnly = false) =>
    get<{
      unread: number
      items: Array<{
        id: number
        subject: string
        body_md: string | null
        payload: {
          kind?: string
          signal_date?: string
          entry_date?: string
          n_picks?: number
          top10?: Array<{ rank: number; symbol: string; sector: string | null; score: number }>
        }
        status: 'pending' | 'sent' | 'read'
        created_at: string
      }>
    }>(`/api/falcon/admin/inbox?limit=${limit}&unread_only=${unreadOnly}`),

  inboxMarkRead: (ids: number[]) =>
    post<{ updated: number }>(`/api/falcon/admin/inbox/mark-read`, ids),

  // ── Trade endpoints ────────────────────────────────────────────────────────

  tradePreview: (req: TradePreviewRequest) =>
    post<TradePreviewResponse>(`/api/falcon/trade/preview`, req),

  tradeSmokeTest: (previewId: string) =>
    post<TradeSmokeResponse>(`/api/falcon/trade/smoke-test`, { preview_id: previewId }),

  tradePlace: (previewId: string, confirmText: string) =>
    post<TradePlaceResponse>(`/api/falcon/trade/place`, {
      preview_id: previewId,
      confirm_text: confirmText,
    }),

  tradeCancelAll: (batchId: string) =>
    post<TradeCancelResponse>(`/api/falcon/trade/cancel-all`, { batch_id: batchId }),

  tradeStatus: (batchId: string) =>
    get<TradePlaceResponse>(`/api/falcon/trade/status?batch_id=${encodeURIComponent(batchId)}`),

  tradePositions: () =>
    get<TradePositionsResponse>(`/api/falcon/trade/positions`),

  tradePositionsExit: (symbol: string) =>
    post<TradeExitResponse>(`/api/falcon/trade/positions/exit`, { symbol, confirm: true }),

  tradeMtfCheck: (symbols: string[]) =>
    post<Record<string, boolean>>(`/api/falcon/trade/mtf-check`, { symbols }),

  tradeAdoptPreview: (symbol: string) =>
    post<TradeAdoptPreview>(`/api/falcon/trade/positions/adopt`,
      { symbol, confirm: false }),

  tradeAdoptConfirm: (symbol: string, sl_price?: number) =>
    post<TradeAdoptResult>(`/api/falcon/trade/positions/adopt`,
      { symbol, sl_price, confirm: true }),

  tradeBulkAdoptPreview: (items: BulkAdoptItem[]) =>
    post<TradeBulkAdoptResult>(`/api/falcon/trade/positions/bulk-adopt`,
      { items, confirm: false }),

  tradeBulkAdoptConfirm: (items: BulkAdoptItem[]) =>
    post<TradeBulkAdoptResult>(`/api/falcon/trade/positions/bulk-adopt`,
      { items, confirm: true }),

  // Phase 2 monitor endpoints
  tradeEvents: (limit = 50, since?: string) => {
    const q = new URLSearchParams({ limit: String(limit) })
    if (since) q.set('since', since)
    return get<{ events: TradeEvent[] }>(`/api/falcon/trade/events?${q.toString()}`)
  },

  tradeMonitor: () =>
    get<TradeMonitorStatus>(`/api/falcon/trade/monitor`),

  tradeTickerStatus: () =>
    get<TickerStatus>(`/api/falcon/trade/ticker`),

  // ── Phase 2.3: pre-market staging + deployer ─────────────────────────────
  premarketList: (target_date?: string) => {
    const q = target_date ? `?target_date=${target_date}` : ''
    return get<PremarketListResponse>(`/api/falcon/trade/premarket${q}`)
  },
  premarketTokenStatus: () =>
    get<KiteTokenStatus>(`/api/falcon/trade/premarket/token-status`),
  premarketStageNow: (target_date?: string) =>
    post<PremarketStageSummary>(`/api/falcon/trade/premarket/stage`, { target_date }),
  premarketConfirm: (item_ids: number[] = []) =>
    post<{ status: string; n_confirmed: number; target_date?: string }>(
      `/api/falcon/trade/premarket/confirm`, { item_ids }),
  premarketCancel: (item_ids: number[]) =>
    post<{ status: string; n_cancelled: number }>(
      `/api/falcon/trade/premarket/cancel`, { item_ids }),
  premarketPatchItem: (item_id: number, patch: { sl_price?: number; qty?: number }) =>
    fetch(`${API}/api/falcon/trade/premarket/items/${item_id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(patch),
    }).then(async r => {
      if (!r.ok) throw new Error(`PATCH ${r.status}: ${await r.text().catch(() => r.statusText)}`)
      return r.json() as Promise<{ status: string; item: PremarketItem }>
    }),
  premarketDeployNow: () =>
    post<{ status: string; result: Record<string, unknown> }>(
      `/api/falcon/trade/premarket/deploy-now`, {}),
  premarketDeployerStatus: () =>
    get<PremarketDeployerStatus>(`/api/falcon/trade/premarket/deployer-status`),

  // Stage from /trade — convert preview → NEW_ENTRY items in pre-market
  premarketStageEntries: (preview_id: string) =>
    post<{
      target_date: string
      n_requested: number
      n_staged:    number
      n_skipped:   number
      staged:      Array<{ symbol: string; id: number; status: string }>
      skipped:     Array<{ symbol: string; reason: string }>
      preview_id:  string
    }>(`/api/falcon/trade/premarket/stage-entries`, { preview_id }),

  // Stage from /positions — list of (symbol, sl_price) → BULK_ADOPT items
  premarketStageAdopts: (items: Array<{ symbol: string; sl_price?: number }>) =>
    post<{
      target_date: string
      n_requested: number
      n_staged:    number
      n_skipped:   number
      staged:      Array<{ symbol: string; id: number; status: string; sl_price: number; qty: number; product: string }>
      skipped:     Array<{ symbol: string; reason: string }>
    }>(`/api/falcon/trade/premarket/stage-adopts`, { items }),

  tradeTradebookUpload: async (file: File) => {
    const fd = new FormData()
    fd.append('file', file)
    const r = await fetch(`${API}/api/falcon/trade/positions/tradebook`, {
      method: 'POST',
      body:   fd,
    })
    if (!r.ok) {
      const txt = await r.text().catch(() => '')
      throw new Error(`tradebook upload ${r.status}: ${txt || r.statusText}`)
    }
    return r.json() as Promise<{
      status:         string
      trades_parsed:  number
      symbols_dated:  number
      inserted:       number
      updated:        number
      symbols:        string[]
    }>
  },

  tradeTickerStart: () =>
    post<{ started: boolean; status: TickerStatus }>(`/api/falcon/trade/ticker/start`, {}),

  tradeEodLast: () =>
    get<{ last: { run_date: string; ran_at: string; summary: Record<string, unknown> } | null }>(`/api/falcon/trade/eod`),

  tradeEodRunNow: () =>
    post<{ status: 'ran' | 'already_ran'; summary?: Record<string, unknown>; last?: unknown }>(`/api/falcon/trade/eod/run`, {}),

  tradeGetConfig: () =>
    get<{ configs: TrailConfig[] }>(`/api/falcon/trade/config`),

  tradeSaveConfig: (cfg: TrailConfigInput) =>
    post<{ status: string; config: TrailConfig }>(`/api/falcon/trade/config`, cfg),

  // Engine Playbook (operator-level rules for staging NEW trades)
  tradeGetEngineConfig: () =>
    get<{ config: EngineConfig }>(`/api/falcon/trade/engine-config`),

  tradeSaveEngineConfig: (cfg: EngineConfigInput) =>
    post<{ status: string; config: EngineConfig }>(`/api/falcon/trade/engine-config`, cfg),
}

// ── Phase 2 types ────────────────────────────────────────────────────────────

export type TradeEvent = {
  id:                number
  detected_at:       string
  symbol:            string
  kind:              'NEAR_SL' | 'NEAR_TARGET' | 'HW_REACHED' | 'BREACHED_SL'
                   | 'TARGET_HIT' | 'TIME_STOP_DUE' | 'SL_REPLACED' | 'EXIT_PLACED'
  severity:          'info' | 'warn' | 'critical'
  detail:            string | null
  auto_action_taken: number   // 0|1
  related_kite_id:   string | null
}

export type TradePositionState = {
  symbol:            string
  managed_by:        'falcon' | 'external'
  qty:               number
  avg_entry:         number
  initial_sl_price:  number
  current_sl_price:  number
  target_price:      number
  high_water_price:  number
  hw_reached:        number   // 0|1
  trail_active:      number   // 0|1
  entry_date:        string | null
  hold_days_max:     number
  last_seen_price:   number | null
  last_polled_at:    string | null
  last_event_kind:   string | null
  last_event_at:     string | null
}

export type TradeMonitorStatus = {
  interval_sec:       number
  auto_exit_enabled:  boolean
  n_tracked:          number
  states:             TradePositionState[]
}

export type TrailMethod = 'percentage' | '10d_low'

export type TickerStatus = {
  connected:        boolean
  started_at:       string | null
  last_tick_at:     string | null
  tick_count:       number
  subscribed_count: number
  cached_ltp_count: number
  last_error:       string | null
}

export type TrailConfig = {
  product:            'MTF' | 'CNC'
  activate_pct:       number
  lock_pct:           number
  trail_sl_pct:       number
  trail_profit_pct:   number
  auto_exit_enabled:  number   // 0|1 in DB; coerced from bool in API
  initial_sl_pct:     number
  hold_days_max:      number
  trail_method:       TrailMethod
  trail_lookback:     number
  updated_at:         string
}

export type TrailConfigInput = {
  product:            'MTF' | 'CNC'
  activate_pct:       number
  lock_pct:           number
  trail_sl_pct:       number
  trail_profit_pct:   number
  initial_sl_pct:     number
  hold_days_max:      number
  auto_exit_enabled:  boolean
  trail_method:       TrailMethod
  trail_lookback:     number
}

// Engine Playbook — operator-level rules for staging NEW trades
// (sizing %, top-N cap, skip-held). Distinct from per-product trail config.
export type EngineConfig = {
  id:                   1
  per_trade_pct:        number    // e.g. 6.0 → per-trade cash = total × 6%
  daily_picks_max:      number    // e.g. 14
  skip_already_held:    number    // 0|1 in DB
  mining_window_years:  number    // B4 publish cutoff: mined_year >= (current_year - this)
  updated_at:           string
}

export type EngineConfigInput = {
  per_trade_pct:        number
  daily_picks_max:      number
  skip_already_held:    boolean
  mining_window_years:  number
}

export type TradeAdoptPreview = {
  preview:                   true
  symbol:                    string
  qty:                       number
  avg_entry:                 number
  current_price:             number
  product:                   string
  computed_sl_price:         number
  computed_sl_limit:         number
  distance_to_sl_pct:        number
  will_trigger_immediately:  boolean
  warning:                   string | null
}

export type TradeAdoptResult = {
  preview:        false
  status:         'ADOPTED'
  symbol:         string
  qty:            number
  sl_price:       number
  sl_limit:       number
  kite_order_id:  string
  batch_id:       string
  managed_by:     'falcon'
  message:        string
}

export type BulkAdoptItem = {
  symbol:    string
  sl_price?: number
}

export type BulkAdoptItemResult = {
  symbol:                    string
  // Most fields are present only on items that went through the full pre-check
  // path (NOT_HELD / preview / fresh-place); the dedup-skip paths (ALREADY_ADOPTED,
  // ADOPTED_EXISTING_KITE_SL) carry only symbol/qty/kite_order_id/status.
  product?:                  string
  qty?:                      number
  avg_entry?:                number
  current_price?:            number
  sl_price?:                 number
  sl_limit?:                 number
  distance_to_sl_pct?:       number
  will_trigger_immediately?: boolean
  will_trail_activate?:      boolean
  kite_order_id?:            string
  status?:                   'ADOPTED' | 'ALREADY_ADOPTED' | 'ADOPTED_EXISTING_KITE_SL'
  error?:                    string
}

export type TradeBulkAdoptResult = {
  preview:      boolean
  batch_id:     string | null
  n_requested:  number
  previews:     BulkAdoptItemResult[]
  placed:       BulkAdoptItemResult[]
  failed:       BulkAdoptItemResult[]
}

// ── Trade types ──────────────────────────────────────────────────────────────

export type TradePreviewRequest = {
  signal_date?:          string | null
  selected_symbols:      string[]
  total_capital:         number
  per_trade:             number
  use_leverage:          boolean
  hold_days:             number
  sl_pct:                number
  trail_trigger_pct:     number
  trail_lookback_days:   number
  hold_actions:          Record<string, 'skip' | 'average'>
}

export type TradeOrderSpec = {
  rank:           number
  symbol:         string
  sector:         string | null
  close:          number
  qty:            number
  notional:       number
  sl_price:       number
  target_price:   number
  sl_order_type:  'SL-L' | 'SL-M'
  sl_limit_price: number | null
  is_averaging:   boolean
  existing_qty:   number
  // Phase 2.2 — margin-aware sizing.
  product:                'MTF' | 'CNC'
  margin_per_share:       number | null   // ₹ per share (MTF only)
  margin_required:        number          // ₹ — qty × margin_per_share for MTF, qty × close for CNC
  margin_pct:             number | null   // margin_per_share / close × 100
  effective_leverage:     number | null   // notional / margin_required
  margin_lookup_failed:   boolean         // true → cash-sized fallback (amber)
}

export type TradeExistingPosition = {
  symbol:         string
  qty:            number
  avg_entry:      number
  current_price:  number
  product:        'MTF' | 'CNC' | string
  entry_date:     string | null
  days_held:      number | null
}

export type TradePreviewResponse = {
  preview_id:           string
  signal_date:          string
  entry_date:           string
  n_signals:            number
  n_selected:           number
  n_eligible:           number
  n_skipped_mtf:        number
  n_overlap:            number
  n_skip_action:        number
  n_average_action:     number
  skipped:              Array<{ symbol: string; reason: string }>
  existing_positions:   TradeExistingPosition[]
  per_trade:            number
  total_deployed:       number
  headroom:             number
  utilization_pct:      number
  plan_total_notional:  number
  // Phase 2.2 — margin-aware totals.
  plan_total_margin?:   number
  plan_eff_leverage?:   number | null
  n_margin_failed?:     number
  orders:               TradeOrderSpec[]
  warnings:             string[]
  holdings_fetch_error: string | null
}

export type TradeSmokeResponse = {
  status:          'PLACED' | 'REJECTED' | 'FAILED'
  smoke_symbol:    string
  smoke_qty:       number
  smoke_price:     number
  kite_order_id:   string | null
  placed_at:       string | null
  error:           string | null
  smoke_batch_id:  string
}

export type TradePlaceOrderRow = {
  id:              number
  batch_id:        string
  symbol:          string
  side:            'BUY' | 'SELL'
  role:            'ENTRY' | 'STOP' | 'SMOKE' | 'EXIT'
  is_averaging:    number   // 0 | 1
  kite_order_id:   string | null
  qty:             number
  price:           number | null
  trigger_price:   number | null
  product:         string
  order_type:      string
  status:          'PENDING' | 'PLACING' | 'PLACED' | 'REJECTED' | 'CANCELLED' | 'FAILED'
  placed_at:       string | null
  filled_at:       string | null
  error:           string | null
  idempotency_key: string
}

export type TradePlaceResponse = {
  batch_id:     string
  status:       'COMPLETED' | 'ABORTED' | 'PLACING'
  n_attempted:  number
  n_filled:     number
  n_failed:     number
  started_at:   string
  finished_at:  string | null
  abort_reason: string | null
  orders:       TradePlaceOrderRow[]
}

export type TradeCancelResponse = {
  cancelled: number
  skipped:   number
  errors:    Array<{ order_id: number; error: string }>
}

export type TradePosition = {
  symbol:                string
  managed_by:            'falcon' | 'external'
  qty:                   number
  avg_entry:             number
  current_price:         number
  product:               string
  entry_date:            string | null
  days_held:             number | null      // null when unknown (external w/o trade log)
  hold_days_max:         number
  sl_price:              number
  sl_distance_pct:       number
  sl_kite_order_id:      string | null
  sl_type:               string         // SL-L | SL-M | ADVISORY
  target_price:          number
  target_distance_pct:   number
  trail_active:          boolean
  trail_low_10d:         number | null
  pnl_pct:               number
}

export type TradePositionsResponse = {
  as_of:             string
  n_positions:       number
  total_notional:    number
  total_entry_value: number
  unrealized_pnl:    number
  pnl_pct:           number
  positions:         TradePosition[]
  trigger_watch: {
    near_sl:        Array<{ symbol: string; distance_pct: number }>
    near_target:    Array<{ symbol: string; distance_pct: number }>
    near_time_stop: Array<{ symbol: string; days_held:    number }>
  }
}

export type TradeExitResponse = {
  status:             'EXITED'
  symbol:             string
  cancelled_sl:       string
  exit_kite_order_id: string
  exit_qty:           number
  exit_price:         number
}


// ── Phase 2.3: pre-market staging + deployer ────────────────────────────────

export type PremarketKind = "NEW_ENTRY" | "BULK_ADOPT"
export type PremarketStatus = "STAGED" | "QUEUED" | "DEPLOYED" | "FAILED" | "CANCELLED"

export type PremarketItem = {
  id:             number
  staged_at:      string
  target_date:    string
  kind:           PremarketKind
  symbol:         string
  status:         PremarketStatus
  confirmed_at:   string | null
  deployed_at:    string | null
  deploy_error:   string | null
  deploy_result:  Record<string, unknown> | null
  payload:        Record<string, unknown>
}

export type PremarketSummary = Record<PremarketKind, Partial<Record<PremarketStatus, number>>>

export type KiteTokenStatus = {
  valid:        boolean
  reason?:      string
  expires_at?:  string
  age_hours?:   number
  [k: string]:  unknown
}

export type PremarketListResponse = {
  target_date:   string
  summary:       PremarketSummary
  items:         PremarketItem[]
  token_status:  KiteTokenStatus
}

export type PremarketStageSummary = {
  target_date:    string
  ran_at:         string
  total_staged:   number
  new_entries:    {
    target_date:    string
    n_signals:      number
    n_staged:       number
    n_skipped:      number
    staged_symbols?: string[]
    skipped_symbols?: Array<{ symbol: string; reason: string }>
  }
  bulk_adopts:    {
    target_date:    string
    n_held?:        number
    n_external:     number
    n_managed?:     number
    n_staged:       number
    skipped:        Array<{ symbol: string; reason: string }>
    staged_symbols?: Array<{ symbol: string; qty: number; sl_price: number; product: string }>
  }
}

export type PremarketDeployerStatus = {
  started:        boolean
  started_at:     string | null
  last_cycle_at:  string | null
  last_deploy_at: string | null
  n_deployed:     number
  n_failed:       number
  last_error:     string | null
}

