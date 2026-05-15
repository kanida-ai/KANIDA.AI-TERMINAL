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

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

// ──────────────────────────────────────────────────────────────────────────
// Pick v1 contract — mirrors backend/power_user/services/explainer.py
// ──────────────────────────────────────────────────────────────────────────

export type Tier      = 'ELITE' | 'HIGH' | 'MID' | 'LOWER' | 'TAIL'
export type TierColor = 'amber' | 'green' | 'yellow' | 'orange' | 'gray'
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
  _version:          1
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
}

async function apiFetch<T>(path: string, opts: FetchOpts = {}): Promise<T> {
  const headers: Record<string, string> = {}
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json'
  if (opts.jwt)                headers['Authorization'] = `Bearer ${opts.jwt}`

  let r: Response
  try {
    r = await fetch(`${API}${path}`, {
      method:  opts.method ?? 'GET',
      headers,
      body:    opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal:  opts.signal,
      cache:   'no-store',  // power-user surfaces are dynamic; no Next caching
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

export const PowerAPI = {
  // ── Public (no JWT) ───────────────────────────────────────────────────
  todayPreview: (signal?: AbortSignal) =>
    apiFetch<TodayResponse>('/api/power/picks/today/preview', { signal }),

  featuredReplays: (signal?: AbortSignal) =>
    apiFetch<FeaturedReplayListResponse>('/api/power/replay/featured', { signal }),

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
