/**
 * KANIDA.AI Power User Portal — typed API client.
 *
 * Single source of HTTP for the entire power-user surface. Every page/component
 * imports from here — never from raw `fetch`. Lets us add retries, interceptors,
 * error toasts, telemetry in one place.
 *
 * Pick contract is locked at v1 — see backend/power_user/SPEC/Design.md §5.
 * If `_version` ever flips to 2, every consumer of `Pick` must be updated.
 */

/**
 * API base URL — resolved per-call so the browser path picks up `window`
 * at runtime (a module-level IIFE gets statically evaluated at build time
 * by Turbopack/Webpack and locks the value before hydration runs).
 *
 * Rules:
 *   - In the BROWSER, default to '' (same-origin). The Next.js rewrites in
 *     next.config.ts forward /api/power/* to the backend, so this works
 *     whether the page is served from localhost:3000 or a Cloudflared tunnel.
 *   - On the SERVER (SSR), default to 127.0.0.1:8001 so server components
 *     can talk to the backend directly (rewrites only apply to client fetches).
 *   - NEXT_PUBLIC_API_URL overrides both — set on Vercel to the Railway URL.
 */
function apiBase(): string {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL
  if (typeof window !== 'undefined')   return ''
  return process.env.BACKEND_ORIGIN || 'http://127.0.0.1:8001'
}

// ──────────────────────────────────────────────────────────────────────────
// Pick v1 contract — mirrors backend/power_user/services/explainer.py
// ──────────────────────────────────────────────────────────────────────────

export type Tier      = 'ELITE' | 'HIGH' | 'MID' | 'LOWER' | 'TAIL'
export type TierColor = 'amber' | 'green' | 'yellow' | 'orange' | 'gray' | 'red'

// v2: signal-time tier (distinct axis from the rank `tier`). Derived from
// signal-day price + volume (see backend signal_tier.py).
export type SignalTier =
  | 'PREMIUM-Pullback' | 'PREMIUM-Compression' | 'ENTERPRISE-Dryup'
  | 'GOLD' | 'GOLD-baseline' | 'STANDARD' | 'STANDARD-weak' | 'AVOID'
export type LiveAction = 'ENTER' | 'WAIT' | 'SKIP'
export type LiveCycle  = '0930' | '0945' | '1000'

export type PickPattern = {
  position:        number       // 1, 2, 3 — order within top_patterns
  pattern_id:      string       // 'p_9370'
  trader_phrase:   string       // 'above-average volatility + closed the WEEK powerfully + ...'
  hit_phrase:      string       // '6 of 10 hit +15% within 20 days (baseline 2 of 10 → 2.9× edge)'
  hit_rate_pct:    number       // 63.6
  baseline_pct:    number       // 22
  edge_multiplier: number       // 2.89
  target:          string       // 'hit_15pc_20d'
  oos_lift_pp:     number       // 41.64
  mined_year:      number       // 2025
}

export type PickExpected = {
  d5_range:        [number, number]   // [60, 70] — pct chance green
  d10_range:       [number, number]   // [60, 70] — pct chance +5%
  d15_avg_range:   [number, number]   // [8, 12]  — pct avg return
}

export type PickRisk = {
  stop_loss_pct:     number    // -7
  trail_trigger_pct: number    //  12
  time_exit_days:    number    //   7
}

export type PickActual = {
  d1?:  number    // % return — only for historical replay picks
  d3?:  number
  d5?:  number
  d10?: number
  d15?: number
}

/** v1 Pick payload — the locked contract. Bumping requires updating every consumer. */
export type Pick = {
  _version:          1 | 2
  rank:              number
  symbol:            string
  sector:            string | null
  signal_date:       string | null
  entry_date:        string | null
  score:             number
  n_fires:           number
  tier:              Tier
  tier_icon:         string
  tier_color:        TierColor
  tier_desc:         string
  tier_action_hint:  string
  story:             string
  top_patterns:      PickPattern[]
  expected:          PickExpected
  risk:              PickRisk
  actual?:           PickActual
  // v2 signal-time tiering (optional; present when the backend enriched the pick)
  signal_tier?:        SignalTier | null
  signal_tier_reason?: string | null
  signal_tier_color?:  TierColor | null
  signal_day_ret_pct?: number | null
  two_day_ret_pct?:    number | null
}

// ──────────────────────────────────────────────────────────────────────────
// Response envelopes
// ──────────────────────────────────────────────────────────────────────────

export type TodayResponse = {
  _schema_version: 1
  signal_date:     string | null
  entry_date:      string | null
  picks_shown:     number
  total_available: number
  picks:           Pick[]
}

export type FeaturedReplaySummary = {
  replay_date: string
  title:       string | null
  hook:        string | null
  n_picks:     number
  wr_d5:       number | null
  wr_d15:      number | null
  avg_d15:     number | null
  hit_5_d15:   number | null
}

export type FeaturedReplayListResponse = {
  featured: FeaturedReplaySummary[]
}

export type ReplayAggregateHorizon = {
  wr:       number
  hit_5pct: number
  avg_ret:  number
}

export type ReplayPayload = {
  replay_date:  string
  entry_date:   string | null
  is_featured:  boolean
  title:        string | null
  hook:         string | null
  aggregate: {
    n_picks: number
    horizons: Partial<Record<'d1'|'d3'|'d5'|'d10'|'d15', ReplayAggregateHorizon>>
  }
  picks:        Pick[]
  computed_at?: string
  error?:       string
}

export type LiveDecision = {
  rank:             number
  symbol:           string
  sector:           string | null
  score:            number
  tier:             Tier
  action:           LiveAction
  reason:           string
  decided_at_cycle: LiveCycle | null
  ret_15:           number | null
  vol_pct:          number | null
  close_loc:        number | null
  computed_at:      string
  signal_date:      string
  // v2 signal-time tiering (best-effort on the live panel; no PREMIUM without n_fires)
  signal_tier?:        SignalTier | null
  signal_tier_reason?: string | null
  signal_tier_color?:  TierColor | null
  signal_day_ret_pct?: number | null
  two_day_ret_pct?:    number | null
}

export type LiveDecisionsResponse = {
  entry_date:  string
  signal_date: string | null
  cycle:       LiveCycle | null
  computed_at: string | null
  summary:     { enter: number; wait: number; skip: number; locked: number }
  decisions:   LiveDecision[]
  // Layer 3 graceful degradation flag (Sprint 5c-1). True when Zerodha auto-auth
  // has failed past 09:30 IST on a weekday — intraday ENTER/WAIT/SKIP overlay
  // is unavailable, but EOD picks are still valid.
  degraded?:   boolean
}

export type GoogleSignInOK = {
  status: 'ok'
  jwt: string
  user: { id: number; email: string; display_name: string | null; picture_url: string | null; role: string }
}

/** Phase 1b: email + invite-code sign-in response (same shape as GoogleSignInOK). */
export type InviteLoginOK = GoogleSignInOK

/** Phase 1b: request payload for the new email-only login flow. */
export type InviteLoginRequest = {
  email: string
  /** Empty string allowed only for existing-user re-login; new users must supply a valid code. */
  code:  string
}
export type GoogleSignInNeedsInvite = {
  status: 'needs_invite'
  email: string
  display_name: string | null
  google_sub: string
  picture_url: string | null
}
export type GoogleSignInResponse = GoogleSignInOK | GoogleSignInNeedsInvite

// ──────────────────────────────────────────────────────────────────────────
// Error types — typed for the UI to render specific messages
// ──────────────────────────────────────────────────────────────────────────

export class PowerAPIError extends Error {
  constructor(
    public readonly status: number,
    public readonly code:   string,
    message: string,
    public readonly detail: Record<string, unknown> = {},
  ) {
    super(message)
    this.name = 'PowerAPIError'
  }
  isRateLimited(): boolean { return this.status === 429 }
  isUnauthorized(): boolean { return this.status === 401 }
  isForbidden(): boolean { return this.status === 403 }
}

// ──────────────────────────────────────────────────────────────────────────
// Internal fetch helper — never use raw fetch from a page
// ──────────────────────────────────────────────────────────────────────────

type FetchOpts = {
  method?: 'GET' | 'POST' | 'DELETE'
  body?:   unknown
  jwt?:    string | null
  signal?: AbortSignal
  /** Opt-in: cache this fetch via Next.js data cache for N seconds (SSR only).
   *  Default is `no-store` — power-user surfaces are mostly per-request dynamic.
   *  Pass a positive integer (e.g. 300) for endpoints whose result is stable
   *  for that window — typically operator-curated, low-churn data like the
   *  featured replays list. Ignored in the browser. */
  revalidate?: number
}

async function apiFetch<T>(path: string, opts: FetchOpts = {}): Promise<T> {
  const headers: Record<string, string> = {}
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json'
  if (opts.jwt)                headers['Authorization'] = `Bearer ${opts.jwt}`

  // Cache strategy: default no-store; opt-in to Next data cache via `revalidate`.
  // (Inline type — the local `Pick` trader type shadows the TS utility, so we
  // can't write `Pick<RequestInit, 'cache'>` in this file.)
  const cacheOpts: { cache?: RequestCache; next?: { revalidate: number } } =
    typeof opts.revalidate === 'number' && opts.revalidate > 0
      ? { next: { revalidate: opts.revalidate } }
      : { cache: 'no-store' }

  let r: Response
  try {
    r = await fetch(`${apiBase()}${path}`, {
      method:  opts.method ?? 'GET',
      headers,
      body:    opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal:  opts.signal,
      ...cacheOpts,
    })
  } catch (e: unknown) {
    throw new PowerAPIError(0, 'NETWORK_ERROR',
      e instanceof Error ? e.message : 'Network unavailable')
  }

  // Try JSON first; some endpoints return text-only on rare error paths
  let body: unknown
  try {
    body = await r.json()
  } catch {
    body = { detail: { code: 'PARSE_ERROR', message: await r.text().catch(() => '') } }
  }

  if (!r.ok) {
    const detail = (body as { detail?: Record<string, unknown> })?.detail ?? {}
    const code    = (detail.code   as string) ?? `HTTP_${r.status}`
    const message = (detail.message as string) ?? `Request failed: ${r.status}`
    throw new PowerAPIError(r.status, code, message, detail)
  }

  return body as T
}

// ──────────────────────────────────────────────────────────────────────────
// Endpoint wrappers
// ──────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────
// Co-Trader portfolio types (Sprint 5d)
// ──────────────────────────────────────────────────────────────────────────

export type PortfolioParametersJSON = {
  fixed_per_trade_rs:  number     // V3 audit: locked ₹ per trade at ₹5 L base
  top_n:               number
  entry_cadence:       'daily' | 'tuesday_only' | 'first_of_month'
  entry_time_label:    string
  hold_days_max:       number
  sl_pct:              number
  target_pct:          number | null
  trail_trigger_pct:   number | null
  trail_method:        'donchian_10d' | 'percentage' | null
  intraday_filter:     'wait_15min_volume_check' | null
  skip_if_held:        boolean
}

/** V3 audit-ready backtest metrics — operator-locked, NEVER computed live.
 *  Source: portfolio_defs.py backtest_metrics (post 2026-05-16 lock). */
export type PortfolioValidatedMetrics = {
  // Headline yearly numbers
  avg_yearly_return_pct?:    number
  median_yearly_return_pct?: number
  best_year_pct?:            number
  best_year_label?:          string
  worst_year_pct?:           number
  worst_year_label?:         string
  positive_years?:           string         // e.g. "5 of 6", "6 of 6 ✓"
  // Per-trade colour
  win_rate_pct?:             number
  avg_max_drawdown_pct?:     number
  trades_per_year?:          number
  total_pnl_rs_over_window?: number
  stop_loss_pct?:            number
  target_pct?:               number
  // Optional callout strings (operator-locked per persona)
  headline_callout?:         string         // e.g. P4's "never had a losing calendar year"
  backtest_caveat?:          string         // e.g. P2's "tested only 2024-2026"
  // Metadata sidecar (set by serialiser, not by hand)
  backtest_window?:          string
  risk_tier?:                string
  risk_tier_color?:          string
  persona_number?:           number
}

/** V3 narrative blocks rendered on the persona detail page. */
export type PortfolioNarrative = {
  what_it_does?:       string
  trading_cycle?:      string[]
  best_fit?:           string
  not_for?:            string
  what_to_expect?:     string
  disclosure?:         string | null
  execution_guidance?: string                    // Fix 6: "How to execute this strategy"
}

export type PortfolioReturnBucket = {
  total_pnl_rs:        number
  total_return_pct:    number
  today_pnl_rs:        number
  today_pnl_pct:       number
  ytd_return_pct:      number
  mtd_return_pct:      number
  worst_stretch_pct:   number
}

export type PortfolioLiveStats = {
  as_of_date:             string
  total_investment_rs:    number     // = capital_start (locked ₹50 L virtual)
  current_value_rs:       number     // mark-to-market equity (reinvest view)
  invested_right_now_rs:  number     // cost basis of currently-open positions
  n_open_positions:       number
  reinvest:               PortfolioReturnBucket   // profits stay in the book (default)
  cash_out:               PortfolioReturnBucket   // profits "to wallet" on each exit
  // Live-derived best/worst from this portfolio's actual history
  best_win:      { symbol: string; pnl_pct: number; date: string } | null
  worst_loss:    { symbol: string; pnl_pct: number; date: string } | null
  worst_stretch: { dd_pct: number; from_date: string; to_date: string; weeks: number; rs_lost: number } | null
  avg_winner_rs:  number | null
  avg_loser_rs:   number | null
  avg_winner_pct: number | null
  avg_loser_pct:  number | null
}

export type PortfolioSummary = {
  slug:                  string
  name:                  string
  tagline:               string
  display_order:         number
  virtual_capital_start: number
  start_date:            string
  parameters:            PortfolioParametersJSON
  validated:             PortfolioValidatedMetrics
  narrative:             PortfolioNarrative
  risk_tier:             string | null
  risk_tier_color:       string | null
  persona_number:        number | null
  backtest_window:       string | null
  live:                  PortfolioLiveStats | null
  recent_events?:        Array<{
    event_date:  string
    event_type:  string
    symbol:      string
    price:       number
    reason_text: string
  }>
}

export type PortfolioPosition = {
  id:                  number
  signal_date:         string
  entry_date:          string
  symbol:              string
  sector:              string | null
  rank_on_entry:       number | null
  story_on_entry:      string | null
  entry_price:         number
  qty:                 number
  capital_committed:   number
  sl_level:            number
  target_level:        number | null
  trail_active:        number
  trail_high_water:    number | null
  current_price:       number | null
  unrealized_pnl_rs:   number | null
  unrealized_pnl_pct:  number | null
}

export type PortfolioTrade = {
  signal_date:        string
  entry_date:         string
  exit_date:          string
  symbol:             string
  sector:             string | null
  entry_price:        number
  exit_price:         number
  qty:                number
  capital_committed:  number
  pnl_rs:             number
  pnl_pct:            number
  exit_reason:        'SL' | 'TARGET' | 'TIME' | 'TRAIL' | 'MANUAL'
  holding_days:       number
  story_on_entry:     string | null
}

export type PortfolioEquityPoint = {
  trade_date:                     string
  total_equity:                   number       // reinvest view (= live equity)
  cumulative_return_pct:          number
  daily_pnl_pct:                  number
  max_drawdown_pct:               number
  n_open_positions:               number
  // Cash Out counterparts — step function rising on each trade exit by the
  // realised pnl_rs. Flat between exits. Operator architecture decision:
  // ONE book, this is a DERIVED display curve.
  cash_out_total_equity:          number
  cash_out_cumulative_return_pct: number
}

export type ReturnMode = 'reinvest' | 'cash_out'

export type PortfolioYearlyRow = {
  year:              number
  return_pct:        number
  max_drawdown_pct:  number
  win_rate_pct:      number
  n_closed_trades:   number
  winning_months:    number | null
  is_partial:        number
  end_equity_rs:     number
  start_cap_rs:      number
}


// ──────────────────────────────────────────────────────────────────────────
// New persona-simulator response shape (2026-05-16 — replaces Excel pipeline).
// Frontend wraps PowerAPI.persona() through `adaptPersonaToPerformance()` so
// the year/month UI component stays the same.
// ──────────────────────────────────────────────────────────────────────────

export type PersonaYearlyRowAPI = {
  year:                  number
  start_capital:         number
  end_equity:            number
  pnl:                   number
  return_pct:            number
  max_dd_pct:            number          // note: API uses `max_dd_pct`, UI uses `max_drawdown_pct`
  win_rate_pct:          number
  n_closed:              number          // API uses `n_closed`, UI uses `n_closed_trades`
  n_open_at_year_end:    number
  closed_net_pnl_sum:    number
  open_mtm_net_pnl_sum:  number
}

export type PersonaMonthlyRowAPI = {
  year:        number
  month:       string                    // "YYYY-MM" (NOT 1..12)
  end_equity:  number
  return_pct:  number
}

export type PersonaBacktestResponse = {
  persona_id:        string
  name:              string
  tagline:           string
  risk_tier:         string
  warning:           string | null
  sim_start:         string
  sim_end:           string
  config:            Record<string, unknown>
  yearly:            PersonaYearlyRowAPI[]
  monthly:           PersonaMonthlyRowAPI[]
  trades:            Array<Record<string, unknown>>
  summary:           Record<string, unknown>
  reconciliation:    Array<Record<string, unknown>>
  open_positions:    unknown[]
  last_updated:      string
  source_sim_script: string
}


/** Adapt the new persona-simulator response to the existing
 *  `PortfolioPerformanceResponse` shape so the dashboard's year+month table
 *  doesn't have to be rewritten. `winning_months` is computed here from the
 *  monthly returns (count(return_pct > 0)). */
export function adaptPersonaToPerformance(
  p: PersonaBacktestResponse,
): PortfolioPerformanceResponse {
  // Group monthly rows by year (string "YYYY") and tally positive months.
  const monthly_by_year: Record<string, PortfolioMonthlyRow[]> = {}
  const winning_months_by_year: Record<number, number> = {}
  for (const m of p.monthly) {
    const yearStr = String(m.year)
    const monthInt = parseInt(m.month.slice(5, 7), 10)
    if (!monthly_by_year[yearStr]) monthly_by_year[yearStr] = []
    monthly_by_year[yearStr].push({
      year:          m.year,
      month:         monthInt,
      return_pct:    m.return_pct,
      end_equity_rs: m.end_equity,
      is_partial:    0,
    })
    if (m.return_pct > 0) {
      winning_months_by_year[m.year] = (winning_months_by_year[m.year] ?? 0) + 1
    }
  }
  // The last year in `sim_end` is partial if sim_end is mid-year.
  const lastYear = p.yearly.length > 0 ? p.yearly[p.yearly.length - 1].year : null
  const simEndYear = parseInt(p.sim_end.slice(0, 4), 10)
  const simEndIsMidYear = !p.sim_end.endsWith('-12-31')

  const yearly: PortfolioYearlyRow[] = p.yearly.map(y => ({
    year:              y.year,
    return_pct:        y.return_pct,
    max_drawdown_pct:  y.max_dd_pct,
    win_rate_pct:      y.win_rate_pct,
    n_closed_trades:   y.n_closed,
    winning_months:    winning_months_by_year[y.year] ?? null,
    is_partial:        (y.year === simEndYear && simEndIsMidYear) ? 1 : 0,
    end_equity_rs:     y.end_equity,
    start_cap_rs:      y.start_capital,
  }))
  return {
    slug:    p.persona_id,
    yearly,
    monthly: monthly_by_year,
  }
}

export type PortfolioMonthlyRow = {
  year:          number
  month:         number     // 1..12
  return_pct:    number
  end_equity_rs: number
  is_partial:    number
}

export type PortfolioPerformanceResponse = {
  slug:    string
  yearly:  PortfolioYearlyRow[]
  monthly: Record<string, PortfolioMonthlyRow[]>     // keyed by year as string
}


export type SizingResult = {
  user_capital_rs:       number
  user_capital_rs_input: number
  clamped:               boolean
  min_capital_rs:        number
  max_capital_rs:        number
  portfolios: Array<{
    slug:                     string
    name:                     string
    tagline:                  string
    persona_number:           number
    risk_tier:                string
    risk_tier_color:          string
    per_trade_size_rs:        number
    expected_n_positions:     number
    invested_right_now_rs:    number
    invested_right_now_pct:   number
    avg_yearly_return_pct:    number
    avg_yearly_return_rs:     number
    best_year_pct:            number
    best_year_rs:             number
    best_year_label:          string | null
    worst_year_pct:           number
    worst_year_rs:            number       // negative for losing years
    worst_year_label:         string | null
    positive_years:           string | null
    win_rate_pct:             number | null
    avg_max_drawdown_pct:     number | null
    avg_trades_per_year:      number
    avg_trades_per_month:     number
    backtest_window:          string
    has_disclosure:           boolean
  }>
}


export const PowerAPI = {
  // ── Public (no JWT) ───────────────────────────────────────────────────
  todayPreview: (signal?: AbortSignal) =>
    apiFetch<TodayResponse>('/api/power/picks/today/preview', { signal }),

  // Co-Trader portfolios (all public)
  portfolios: (signal?: AbortSignal) =>
    apiFetch<{ portfolios: PortfolioSummary[]; n: number }>('/api/power/portfolios', { signal }),

  portfolioDetail: (slug: string, signal?: AbortSignal) =>
    apiFetch<PortfolioSummary>(`/api/power/portfolios/${encodeURIComponent(slug)}`, { signal }),

  portfolioPositions: (slug: string, signal?: AbortSignal) =>
    apiFetch<{ slug: string; n: number; positions: PortfolioPosition[] }>(
      `/api/power/portfolios/${encodeURIComponent(slug)}/positions`, { signal }),

  portfolioTrades: (slug: string, limit = 50, offset = 0, signal?: AbortSignal) =>
    apiFetch<{ slug: string; n: number; total: number; trades: PortfolioTrade[]; limit: number; offset: number }>(
      `/api/power/portfolios/${encodeURIComponent(slug)}/trades?limit=${limit}&offset=${offset}`, { signal }),

  portfolioPerformance: (slug: string, signal?: AbortSignal) =>
    apiFetch<PortfolioPerformanceResponse>(
      `/api/power/portfolios/${encodeURIComponent(slug)}/performance`, { signal }),

  // New persona-simulator backed API (replaces the Excel-import pipeline).
  // Single-shot call returning yearly + monthly + summary in one response;
  // cached server-side for 1 hour. Use `adaptPersonaToPerformance()` to plug
  // into UI components that still expect the old PortfolioPerformanceResponse.
  persona: (slug: string, signal?: AbortSignal) =>
    apiFetch<PersonaBacktestResponse>(
      `/api/power/personas/${encodeURIComponent(slug)}`, { signal }),

  portfolioEquity: (slug: string, signal?: AbortSignal) =>
    apiFetch<{ slug: string; n: number; capital_start: number; points: PortfolioEquityPoint[] }>(
      `/api/power/portfolios/${encodeURIComponent(slug)}/equity`, { signal }),

  portfolioSizing: (capitalRs: number, signal?: AbortSignal) =>
    apiFetch<SizingResult>(`/api/power/portfolios/sizing?capital=${encodeURIComponent(capitalRs)}`, { signal }),

  // Featured replays are operator-curated and change at most a couple of times
  // a week. 5-min Next data cache keeps the landing-page Suspense boundary
  // near-instant for repeat visitors. Pass `signal` only when you genuinely
  // need to cancel (rare for this endpoint).
  featuredReplays: (signal?: AbortSignal) =>
    apiFetch<FeaturedReplayListResponse>('/api/power/replay/featured', { signal, revalidate: 300 }),

  replayForDate: (date: string, jwt?: string | null, signal?: AbortSignal) =>
    apiFetch<ReplayPayload>(`/api/power/picks/replay/${encodeURIComponent(date)}`,
                            { jwt: jwt ?? null, signal }),

  replayRandom: (jwt?: string | null) =>
    apiFetch<ReplayPayload>('/api/power/picks/replay/random',
                            { method: 'POST', jwt: jwt ?? null }),

  // ── Auth (JWT) ────────────────────────────────────────────────────────
  signInWithGoogle: (id_token: string) =>
    apiFetch<GoogleSignInResponse>('/api/power/auth/google',
                                    { method: 'POST', body: { id_token } }),

  /** Phase 1b: invite-code login. Accepts (email, code). Returns {jwt, user}. */
  signInWithInviteCode: (req: InviteLoginRequest) =>
    apiFetch<InviteLoginOK>('/api/power/auth/invite-login',
                             { method: 'POST', body: req }),

  me: (jwt: string) =>
    apiFetch<{ id: number; email: string; display_name: string | null;
                picture_url: string | null; role: string; created_at: string }>(
      '/api/power/auth/me', { jwt }),

  logout: () => apiFetch<{ status: string }>('/api/power/auth/logout', { method: 'POST' }),

  // ── Invites ───────────────────────────────────────────────────────────
  redeemInvite: (id_token: string, code: string) =>
    apiFetch<GoogleSignInOK>('/api/power/invites/redeem',
                              { method: 'POST', body: { id_token, code } }),

  validateInviteCode: (code: string) =>
    apiFetch<{ valid: boolean }>(
      `/api/power/invites/validate/${encodeURIComponent(code)}`),

  joinWaitlist: (email: string, source = 'landing_cta') =>
    apiFetch<{ ok: boolean; joined: boolean }>(
      '/api/power/invites/waitlist', { method: 'POST', body: { email, source } }),

  // ── Authed picks (JWT) ────────────────────────────────────────────────
  todayFull: (jwt: string, signal?: AbortSignal) =>
    apiFetch<TodayResponse>('/api/power/picks/today', { jwt, signal }),

  // ── Falcon Top 20 (3-bucket explainability — public endpoint) ─────────
  // Backend gates audience at the UI layer (Phase 1b invite-only login).
  // The endpoint itself is open: read-only data, the moat is the rendering.
  falconTop20: (
    universe: import('./falcon-top20-types').Top20Universe = 'all500',
    sector?:   string | null,
    signal_date?: string | null,
    signal?: AbortSignal,
  ) => {
    const qs = new URLSearchParams({ universe })
    if (sector)      qs.set('sector', sector)
    if (signal_date) qs.set('signal_date', signal_date)
    return apiFetch<import('./falcon-top20-types').Top20Response>(
      `/api/power/today/falcon-top-20?${qs.toString()}`, { signal })
  },

  liveDecisions: (jwt: string, cycle: LiveCycle | 'latest' = 'latest', entry_date?: string) => {
    const qs = new URLSearchParams({ cycle })
    if (entry_date) qs.set('entry_date', entry_date)
    return apiFetch<LiveDecisionsResponse>(
      `/api/power/picks/live?${qs.toString()}`, { jwt })
  },
}

// ──────────────────────────────────────────────────────────────────────────
// Helpers — Pick payload version assertion (run on every fetched Pick)
// ──────────────────────────────────────────────────────────────────────────

export const PICK_SCHEMA_VERSION = 1 as const

export function assertPickVersion(p: { _version: number }): void {
  if (p._version !== PICK_SCHEMA_VERSION) {
    throw new Error(`Pick payload schema drift: got v${p._version}, ` +
                    `expected v${PICK_SCHEMA_VERSION}. ` +
                    `Frontend + backend versions are misaligned.`)
  }
}
