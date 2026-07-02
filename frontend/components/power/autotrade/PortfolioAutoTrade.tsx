'use client'

/**
 * PortfolioAutoTrade — OPERATOR-ONLY console for the LIVE multi-broker Portfolio
 * AutoTrade backend (/api/autotrade/*, operator-token gated, reached via the
 * same-origin Falcon proxy through lib/autotrade-api.ts).
 *
 * HARD HONESTY (real money is involved):
 *   • The backend SHIPS DISABLED. Sessions default to PAPER mode (no real
 *     orders); the kill switch defaults OFF; and real LIVE orders ADDITIONALLY
 *     require the server env flag FALCON_AUTOTRADE_ENABLED — so even a LIVE
 *     session stays inert until that flag is on, server-side.
 *   • This UI NEVER implies trading is on. PAPER is presented as the green/safe
 *     default; LIVE is red, gated behind a typed confirm + a standing warning.
 *   • All numbers come from the backend. We render "—" / honest empty + error
 *     states; we fabricate no fills, no P&L, no positions.
 *
 * Flow: Config form → Create → Start (per-symbol placed/skipped) → live Status
 * card (gross return, kill-switch state, open positions) with poll → KILL
 * (red, typed-confirm). Plus read-only saved-configs + brokers lists.
 *
 * Reuses the cotrade-kit F2 palette + icon set so it reads as one product family
 * with Co-Trading / the existing AutoTrade console.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import {
  C, ICON, Gear, MECHANISM_CSS, fmtCapital, fmtPct, pctTone, fmtINR, signedINR,
} from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type Mode, type StartWhen, type SizingMode, type OrderProduct, type KillDirection,
  type InstrumentType, type TradeDirection,
  type OnMissedWindow,
  type Strategy, type SessionConfig, type CreateResponse, type StartResponse,
  type StatusResponse, type SavedConfig, type Broker, type SessionSummary,
  type OpenPosition, type PreviewResponse, type KillPreview,
  type BrokerAccount, type SessionScope,
  type UniverseFilter, type PickItem, type PicksResponse,
} from '@/lib/autotrade-api'

// ── Safe defaults — paper + kill switch OFF, per the ships-disabled contract ──
const DEFAULT_CONFIG: SessionConfig = {
  strategy: 'portfolio_kill_switch',
  total_allocated_capital: 500_000,
  top_n_stocks: 5,
  sizing_mode: 'equal',
  max_pct_per_position: 25,
  order_product: 'CNC',
  // Instrument / direction — default to the existing equity long behaviour.
  instrument_type: 'EQ',
  direction: 'long',
  kill_switch_enabled: false,
  kill_switch_pct: 5,
  kill_switch_direction: 'loss',
  entry_time: '09:15',
  // Execution-date / trading-day rule — empty entry_date = backend resolves to
  // the next valid trading session; expire = drop a missed/non-trading-day fire.
  entry_date: '',
  on_missed_window: 'expire',
  entry_grace_seconds: 120,
}

// ── Intraday-basket VALIDATED PRESET ──────────────────────────────────────────
// The precoded defaults for the Falcon Intraday Basket strategy. Operator can
// still edit every field; this just seeds the form when they pick the strategy.
// Percent fields are kept as PERCENTS in state (sent ÷100); times are "HH:MM:SS".
const INTRADAY_PRESET: Partial<SessionConfig> = {
  top_n_stocks: 5,
  entry_time: '09:15:00',
  order_product: 'MTF',
  arm_pct: 1.0,
  floor_pct: 1.0,
  trail_giveback_pct: 0.75,
  stop_pct: 1.5,
  square_off_time: '15:29:00',
}

const STRATEGY_OPTIONS: { id: Strategy; label: string }[] = [
  { id: 'portfolio_kill_switch', label: 'Portfolio Kill Switch — flat ±% basket exit' },
  { id: 'intraday_basket',       label: 'Falcon Intraday Basket — arm & trail, square-off' },
]

// A SCHEDULED session that lost its in-memory timer (backend restart) reports
// scheduler_armed === false. Everything else is "armed enough to wait".
const isScheduled = (s?: string | null) => (s ?? '').toUpperCase() === 'SCHEDULED'

// Live countdown helper — turns seconds into "2h 14m 03s" / "14m 03s" / "43s".
function fmtCountdown(totalSec: number): string {
  const s = Math.max(0, Math.floor(totalSec))
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  const pad = (n: number) => String(n).padStart(2, '0')
  if (h > 0) return `${h}h ${pad(m)}m ${pad(sec)}s`
  if (m > 0) return `${m}m ${pad(sec)}s`
  return `${sec}s`
}

// mm:ss countdown for the square-off timer (intraday_basket). Negative/absent → '—'.
function fmtMMSS(totalSec: number | null | undefined): string {
  if (totalSec == null || !Number.isFinite(totalSec)) return '—'
  const s = Math.max(0, Math.floor(totalSec))
  const m = Math.floor(s / 60)
  const sec = s % 60
  return `${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`
}

// A latency in ms → a compact, human string: <1000 stays "368 ms"; ≥1000 becomes
// "2.7 s" (one decimal, trailing .0 trimmed). null/non-finite → "—" (not measured).
function fmtMs(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '—'
  const v = Math.max(0, ms)
  if (v < 1000) return `${Math.round(v)} ms`
  return `${(v / 1000).toFixed(1).replace(/\.0$/, '')} s`
}

// Liveness tone for the last-tick age (the monitoring heartbeat): green if the
// feed is sub-second-ish (<1500ms), amber while it lags (1500–5000ms), red/stale
// when older or not measured. Returns the tone color + an honest label.
function tickLiveness(ms: number | null | undefined): { color: string; label: string; fresh: boolean } {
  if (ms == null || !Number.isFinite(ms)) return { color: C.faint, label: 'no data', fresh: false }
  if (ms < 1500) return { color: C.mint, label: 'monitoring sub-second', fresh: true }
  if (ms <= 5000) return { color: C.amber, label: 'feed lagging', fresh: false }
  return { color: C.red, label: 'stale', fresh: false }
}

// A backend FRACTION → a trimmed, signed percent string ("+2.3%", "-1.55%").
// Returns '—' for absent/non-finite values so nothing is fabricated.
function fracPct(frac: number | null | undefined, signed = true, dp = 2): string {
  if (frac == null || !Number.isFinite(frac)) return '—'
  const v = frac * 100
  const trimmed = v.toFixed(dp).replace(/\.?0+$/, '')
  const sign = signed && v >= 0 ? '+' : ''
  return `${sign}${trimmed}%`
}

// ── Universe filter options — identical to the signals page Top20Filters ─────
const UNIVERSE_OPTIONS: Array<{ key: UniverseFilter; label: string }> = [
  { key: 'all500',   label: 'All 500'   },
  { key: 'nifty50',  label: 'Nifty 50'  },
  { key: 'nifty100', label: 'Nifty 100' },
  { key: 'nifty200', label: 'Nifty 200' },
  { key: 'fno',      label: 'F&O'       },
]

const TOP_N_OPTIONS = [3, 5, 7, 10]
const SIZING_OPTIONS: { id: SizingMode; label: string; hint: string }[] = [
  { id: 'equal',   label: 'Equal',   hint: 'Split capital evenly across picks' },
  { id: 'pct_cap', label: '% cap',   hint: 'Cap each position at a max % of capital' },
  { id: 'manual',  label: 'Manual',  hint: 'Per-symbol amounts (advanced)' },
]
// NRML is an F&O / currency / commodity CARRY product — the broker REJECTS it on
// NSE cash equity, and the backend now rejects it at session creation. So it is
// NOT offered for equity sessions. (Futures sessions will send NRML themselves,
// server-side, via the FUT order builder — the user never picks it here.)
const PRODUCT_OPTIONS: OrderProduct[] = ['CNC', 'MIS', 'MTF']
// Instrument: Equity (the existing cash path) | Futures (current-month, NRML set
// server-side). CE/PE are NOT offered — futures-only this pass.
const INSTRUMENT_OPTIONS: { id: InstrumentType; label: string }[] = [
  { id: 'EQ',  label: 'Equity'   },
  { id: 'FUT', label: 'Futures'  },
]
// Direction for a FUT session: Buy (long) | Sell / Short. 'short' is FUT-only
// (backend rejects it on equity). Equity always runs long.
const DIRECTION_OPTIONS: { id: TradeDirection; label: string }[] = [
  { id: 'long',  label: 'Buy'          },
  { id: 'short', label: 'Sell (Short)' },
]
const KILL_DIR_OPTIONS: { id: KillDirection; label: string }[] = [
  { id: 'loss',   label: 'Loss only' },
  { id: 'profit', label: 'Profit only' },
  { id: 'both',   label: 'Both' },
]
const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000, 3_000_000]
const ON_MISSED_OPTIONS: { id: OnMissedWindow; label: string }[] = [
  { id: 'expire',                label: 'Expire' },
  { id: 'carry_next_trading_day', label: 'Carry to next trading day' },
]

// The three non-placed terminal/blocked statuses from the trading-day rule. A
// session in one of these did NOT place — render it muted/amber, never the green
// RUNNING look.
const NON_PLACED_STATUSES = ['REJECTED_NON_TRADING_DAY', 'EXPIRED_MISSED_WINDOW', 'DEFERRED_MARKET_CLOSED']
const isNonPlaced = (s?: string | null) => NON_PLACED_STATUSES.includes((s ?? '').toUpperCase())

// Parse a backend 400 detail that names the suggested next trading day, e.g.
// "2026-06-28 is not a trading day. Next trading day: 2026-06-29" → "2026-06-29".
// Returns null when no YYYY-MM-DD follows a "next trading day" cue.
function parseSuggestedTradingDay(detail: string | null | undefined): string | null {
  if (!detail) return null
  const m = detail.match(/next\s+trading\s+day[^0-9]*(\d{4}-\d{2}-\d{2})/i)
  return m ? m[1] : null
}

// True when an error detail looks like the "not a trading day" rejection (so we
// show the friendly one-click apply rather than a raw error toast).
function isNonTradingDayError(detail: string | null | undefined): boolean {
  if (!detail) return false
  return /not\s+a\s+trading\s+day/i.test(detail) || parseSuggestedTradingDay(detail) != null
}

// Friendly label for a non-placed status (the three trading-day-rule outcomes).
function nonPlacedLabel(status: string): string {
  switch (status.toUpperCase()) {
    case 'REJECTED_NON_TRADING_DAY': return 'Rejected — not a trading day'
    case 'EXPIRED_MISSED_WINDOW':    return 'Expired — entry window missed'
    case 'DEFERRED_MARKET_CLOSED':   return 'Deferred — market closed'
    default:                         return status
  }
}

// 'list' is the HOME phase: your saved sessions (newest first). 'config' is the
// explicit New-Session form. 'created'/'running' are the live session views,
// reached either by creating one OR by RESUMING an existing one from the list.
type Phase = 'list' | 'config' | 'created' | 'running'

// ── Small shared field primitives ────────────────────────────────────────────
function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
        {label}
      </label>
      {children}
      {hint && <span className="text-[10.5px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}

function Segmented<T extends string | number>({
  options, value, onChange,
}: {
  options: { id: T; label: string }[]; value: T; onChange: (v: T) => void
}) {
  return (
    <div className="inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      {options.map((o) => {
        const active = o.id === value
        return (
          <button
            key={String(o.id)}
            type="button"
            onClick={() => onChange(o.id)}
            className="px-3 py-1.5 rounded-lg text-[12px] font-medium transition-colors"
            style={{
              color: active ? '#06130c' : C.ink2,
              background: active ? C.mint : 'transparent',
            }}
          >
            {o.label}
          </button>
        )
      })}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

export function PortfolioAutoTrade({
  userId,
  onSessionChange,
}: {
  userId?: number | string
  // Called whenever the user focuses a session (resume or create).
  // Passes the session_id up so sibling tabs (Journal) can use it.
  onSessionChange?: (sessionId: string) => void
}) {
  const [config, setConfig] = useState<SessionConfig>(DEFAULT_CONFIG)
  const [mode, setMode] = useState<Mode>('paper')

  // ── Per-account session selection (Phase-2 multi-tenant) ──────────────────────
  // The user's connected broker accounts (for the optional "Broker account"
  // selector on the create form). '' = the global/operator session (default,
  // unchanged). Only ACTIVE accounts may run a LIVE session; an EXPIRED account
  // is offered but blocks live Start until re-connected. Loaded only when a user
  // context exists; on failure we silently fall back to the global session.
  const [accounts, setAccounts] = useState<BrokerAccount[]>([])
  const [brokerAccountId, setBrokerAccountId] = useState<string>('')

  const [phase, setPhase] = useState<Phase>('list')
  const [session, setSession] = useState<CreateResponse | null>(null)
  const [startResult, setStartResult] = useState<StartResponse | null>(null)
  const [status, setStatus] = useState<StatusResponse | null>(null)

  // Your Sessions list (newest first) — the HOME view. Resumes survive reload.
  const [sessions, setSessions] = useState<SessionSummary[] | null>(null)
  const [sessionsLoading, setSessionsLoading] = useState(false)
  const [sessionsErr, setSessionsErr] = useState<string | null>(null)

  // Multi-select delete (paper/test housekeeping). `selected` holds the chosen
  // session_ids; `deleting` gates the bulk action; `deleteErr` is honest.
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [deleting, setDeleting] = useState(false)
  const [deleteErr, setDeleteErr] = useState<string | null>(null)

  const [busy, setBusy] = useState<null | 'create' | 'start' | 'status' | 'kill'>(null)
  const [error, setError] = useState<string | null>(null)

  // When Create returns a "not a trading day" 400, we parse the suggested next
  // trading day from the detail and offer a one-click "Use {date}" apply instead
  // of a raw error. { detail } is the friendly message; { suggested } the date.
  const [createSuggest, setCreateSuggest] = useState<{ detail: string; suggested: string | null } | null>(null)

  // ── P&L preview (the "Potential outcome" card on the CONFIG form) ─────────────
  // An ESTIMATE from POST /api/autotrade/preview — creates no session, places
  // nothing. Debounced on the config fields that move the bases / kill outcome.
  const [preview, setPreview] = useState<PreviewResponse | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewErr, setPreviewErr] = useState<string | null>(null)

  // ── Universe filter + manual stock picker ─────────────────────────────────
  // universeFilter: which index the picks come from (default all500 = current
  // behaviour). Sent in SessionConfig as universe_filter; the backend falls
  // back silently when the field is absent (graceful degradation).
  const [universeFilter, setUniverseFilter] = useState<UniverseFilter>('all500')

  // picks: the ranked list for the manual picker (from GET /session/picks).
  // null = not yet loaded or unavailable; [] = loaded but empty.
  const [picks, setPicks] = useState<PickItem[] | null>(null)
  const [picksLoading, setPicksLoading] = useState(false)
  const [picksErr, setPicksErr] = useState<string | null>(null)

  // checkedSymbols: which symbols the user has checked. null = use default top-N
  // (the picker hasn't been interacted with or failed to load). We seed it from
  // the picks list once loaded (top_n items pre-checked, rest unchecked), but
  // the user can override freely.
  const [checkedSymbols, setCheckedSymbols] = useState<Set<string> | null>(null)

  // Live-mode typed confirmation + kill typed confirmation
  const [liveConfirm, setLiveConfirm] = useState('')
  const [killArmed, setKillArmed] = useState(false)
  const [killConfirm, setKillConfirm] = useState('')

  // Poll toggle
  const [poll, setPoll] = useState(true)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Live countdown for a SCHEDULED session. Seeded from status.seconds_remaining
  // on every poll, then ticked down locally each second so the display is smooth
  // between the (slower) status polls. Re-sync on each fresh status keeps it
  // honest — the backend remains the source of truth.
  const [countdown, setCountdown] = useState<number | null>(null)
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const set = <K extends keyof SessionConfig>(k: K, v: SessionConfig[K]) =>
    setConfig((c) => ({ ...c, [k]: v }))

  // Switch instrument. Selecting Equity forces direction back to 'long' (short is
  // FUT-only; the backend rejects a short equity order). Selecting Futures keeps
  // the current direction (defaults to 'long').
  const onInstrumentChange = (next: InstrumentType) =>
    setConfig((c) => ({
      ...c,
      instrument_type: next,
      direction: next === 'EQ' ? 'long' : (c.direction ?? 'long'),
    }))

  // The selected account object (if any) + the scope to send on create/preview/list.
  // scope is omitted entirely when no user context exists (default global session).
  const selectedAccount = accounts.find((a) => a.broker_account_id === brokerAccountId) ?? null
  const accountExpired = (selectedAccount?.status ?? '').toUpperCase() === 'EXPIRED'
  const scope: SessionScope | undefined =
    userId != null
      ? { user_id: userId, ...(brokerAccountId ? { broker_account_id: brokerAccountId } : {}) }
      : undefined

  // Load the user's broker accounts (for the selector). Best-effort: a missing
  // endpoint / disabled vault leaves the selector showing only "Global account".
  useEffect(() => {
    if (userId == null) return
    let cancelled = false
    AutoTradeAPI.brokerAccounts(userId)
      .then((res) => { if (!cancelled) setAccounts(res.accounts ?? []) })
      .catch(() => { if (!cancelled) setAccounts([]) })
    return () => { cancelled = true }
  }, [userId])

  // Switch strategy. For intraday_basket, SEED the validated preset (operator can
  // still edit). For portfolio_kill_switch, fall back to the safe defaults so the
  // existing kill-switch UX is byte-for-byte unchanged.
  const onStrategyChange = (next: Strategy) => {
    setConfig((c) => {
      if (next === 'intraday_basket') {
        return { ...c, ...INTRADAY_PRESET, strategy: next }
      }
      return {
        ...c,
        strategy: next,
        order_product: DEFAULT_CONFIG.order_product,
        entry_time: DEFAULT_CONFIG.entry_time,
        kill_switch_enabled: DEFAULT_CONFIG.kill_switch_enabled,
        kill_switch_pct: DEFAULT_CONFIG.kill_switch_pct,
        kill_switch_direction: DEFAULT_CONFIG.kill_switch_direction,
      }
    })
  }

  // Build the wire payload for a config: percents → fractions, exactly at the
  // send boundary (state stays in percents so the inputs read naturally). Both
  // create + preview use this so there is one conversion site per strategy.
  // Also carries universe_filter + symbol_whitelist through (passed as extra args
  // at the call site so the effect dependency arrays can declare them cleanly).
  const toWireConfig = useCallback((
    c: SessionConfig,
    opts?: { universeFilter?: string; symbolWhitelist?: string[] },
  ): SessionConfig => {
    // Execution-date / trading-day rule: an empty entry_date means "let the
    // backend resolve the next valid trading session" — send it as omitted (the
    // empty string would be an invalid date), and pass through the rest.
    const entry_date = c.entry_date && c.entry_date.trim() ? c.entry_date.trim() : undefined
    const exec = {
      entry_date,
      on_missed_window: c.on_missed_window ?? 'expire',
      entry_grace_seconds: Number.isFinite(Number(c.entry_grace_seconds))
        ? Number(c.entry_grace_seconds) : 120,
    }
    // universe_filter: only send when not the default (all500 = current behaviour).
    // symbol_whitelist: only send when the user has made a non-default selection.
    const universeExtra: Partial<SessionConfig> = {}
    if (opts?.universeFilter && opts.universeFilter !== 'all500') {
      universeExtra.universe_filter = opts.universeFilter
    }
    if (opts?.symbolWhitelist && opts.symbolWhitelist.length > 0) {
      universeExtra.symbol_whitelist = opts.symbolWhitelist
    }
    if (c.strategy === 'intraday_basket') {
      return {
        ...c,
        ...exec,
        ...universeExtra,
        arm_pct: (Number(c.arm_pct) || 0) / 100,
        floor_pct: (Number(c.floor_pct) || 0) / 100,
        trail_giveback_pct: (Number(c.trail_giveback_pct) || 0) / 100,
        stop_pct: (Number(c.stop_pct) || 0) / 100,
        // intraday_basket exits via the trail, not the flat kill switch
        kill_switch_enabled: false,
      }
    }
    return { ...c, ...exec, ...universeExtra, kill_switch_pct: (Number(c.kill_switch_pct) || 0) / 100 }
  }, [])

  // ── Load picks when the config form is open (universe or top_n changes) ──────
  // If the endpoint errors (backend not yet deployed), gracefully degrade: show
  // an inline note and leave checkedSymbols null so create still works normally.
  useEffect(() => {
    if (phase !== 'config') {
      setPicks(null); setPicksErr(null); setPicksLoading(false); setCheckedSymbols(null)
      return
    }
    let cancelled = false
    setPicksLoading(true)
    setPicksErr(null)
    const t = setTimeout(async () => {
      try {
        const res: PicksResponse = await AutoTradeAPI.sessionPicks(universeFilter, config.top_n_stocks)
        if (cancelled) return
        setPicks(res.picks ?? [])
        setPicksErr(null)
        // Seed the checked set: top_n items pre-checked by rank, rest unchecked.
        const defaultChecked = new Set(
          (res.picks ?? [])
            .filter((p) => p.rank <= config.top_n_stocks)
            .map((p) => p.symbol),
        )
        setCheckedSymbols(defaultChecked)
      } catch {
        if (cancelled) return
        setPicks(null)
        setPicksErr('Universe preview unavailable — top N picks will be used.')
        setCheckedSymbols(null)
      } finally {
        if (!cancelled) setPicksLoading(false)
      }
    }, 300)
    return () => { cancelled = true; clearTimeout(t) }
  }, [phase, universeFilter, config.top_n_stocks])

  // When the user clicks a different Top N, re-sync checked state to the new N
  // (top N pre-checked, rest unchecked). Called in addition to set('top_n_stocks').
  const onTopNChange = useCallback((n: number) => {
    set('top_n_stocks', n)
    if (picks) {
      const newChecked = new Set(
        picks.filter((p) => p.rank <= n).map((p) => p.symbol),
      )
      setCheckedSymbols(newChecked)
    }
  }, [picks])

  // Compute the symbol_whitelist to send: undefined if the selection equals the
  // default top-N (no override needed); string[] otherwise (the checked set).
  const symbolWhitelist: string[] | undefined = (() => {
    if (!checkedSymbols || !picks) return undefined
    const defaultSet = new Set(
      picks.filter((p) => p.rank <= config.top_n_stocks).map((p) => p.symbol),
    )
    const isDefault =
      checkedSymbols.size === defaultSet.size &&
      [...checkedSymbols].every((s) => defaultSet.has(s))
    if (isDefault) return undefined
    return [...checkedSymbols]
  })()

  const liveReady = mode === 'paper' || liveConfirm.trim().toUpperCase() === 'LIVE'

  // ── Debounced P&L preview (CONFIG form, kill switch enabled) ──────────────────
  // Re-estimate the invested basis + kill outcome whenever a config field that
  // moves them changes. Debounced 450ms so typing in the capital/threshold inputs
  // doesn't spam the backend. UNITS: state holds kill_switch_pct as a PERCENT
  // ("1" reads naturally); the backend speaks FRACTIONS, so we send /100 here —
  // the SAME convention as createSession (no double-conversion; state untouched).
  // intraday_basket ALWAYS previews (for the strategy-summary line: invested
  // basis + leverage); portfolio_kill_switch previews only when the kill switch
  // is on (the "Potential outcome" card). Both convert percents→fractions via
  // toWireConfig at the send boundary.
  const intraday = config.strategy === 'intraday_basket'
  useEffect(() => {
    if (phase !== 'config' || (!intraday && !config.kill_switch_enabled)) {
      setPreview(null); setPreviewErr(null); setPreviewLoading(false)
      return
    }
    let cancelled = false
    setPreviewLoading(true)
    const t = setTimeout(async () => {
      try {
        const res = await AutoTradeAPI.preview(
          toWireConfig(config, { universeFilter, symbolWhitelist }),
          scope,
        )
        if (!cancelled) { setPreview(res); setPreviewErr(null) }
      } catch (e) {
        if (!cancelled) { setPreview(null); setPreviewErr(e instanceof Error ? e.message : 'Could not estimate the outcome.') }
      } finally {
        if (!cancelled) setPreviewLoading(false)
      }
    }, 450)
    return () => { cancelled = true; clearTimeout(t) }
  }, [
    phase,
    intraday,
    config.strategy,
    config.kill_switch_enabled,
    config.total_allocated_capital,
    config.top_n_stocks,
    config.sizing_mode,
    config.order_product,
    config.instrument_type,
    config.direction,
    config.kill_switch_pct,
    config.kill_switch_direction,
    config.max_pct_per_position,
    config.arm_pct,
    config.floor_pct,
    config.trail_giveback_pct,
    config.stop_pct,
    config.square_off_time,
    universeFilter,
    symbolWhitelist,
    userId,
    brokerAccountId,
    toWireConfig,
  ])

  // ── Your Sessions (list + resume) ────────────────────────────────────────────
  const loadSessions = useCallback(async () => {
    setSessionsLoading(true); setSessionsErr(null)
    try {
      const res = await AutoTradeAPI.listSessions(userId != null ? { user_id: userId } : undefined)
      // Newest first — sort by created_at desc when present, else keep order.
      const list = (res.sessions ?? []).slice().sort((a, b) => {
        const ta = a.created_at ? Date.parse(a.created_at) : 0
        const tb = b.created_at ? Date.parse(b.created_at) : 0
        return tb - ta
      })
      setSessions(list)
    } catch (e) {
      setSessionsErr(e instanceof Error ? e.message : 'Could not load your sessions.')
    } finally {
      setSessionsLoading(false)
    }
  }, [userId])

  // Fetch the list once on mount so a reload RESTORES your sessions (the
  // "session disappears" fix) instead of dumping you on a blank form.
  useEffect(() => { loadSessions() }, [loadSessions])

  // ── Multi-select delete (paper/test housekeeping) ────────────────────────────
  const toggleSelect = useCallback((id: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }, [])

  const allIds = (sessions ?? []).map((s) => s.session_id).filter(Boolean) as string[]
  const allSelected = allIds.length > 0 && allIds.every((id) => selected.has(id))
  const toggleSelectAll = useCallback(() => {
    setSelected((prev) => (allIds.length > 0 && allIds.every((id) => prev.has(id)))
      ? new Set()
      : new Set(allIds))
  }, [allIds])

  const onDeleteSelected = useCallback(async () => {
    const ids = Array.from(selected)
    if (!ids.length) return
    if (!window.confirm(
      `Delete ${ids.length} session${ids.length > 1 ? 's' : ''}? This removes the ` +
      `session record${ids.length > 1 ? 's' : ''} (paper/test housekeeping) and cannot be undone.`,
    )) return
    setDeleting(true); setDeleteErr(null)
    try {
      await AutoTradeAPI.deleteSessions(ids)
      setSelected(new Set())
      await loadSessions()
    } catch (e) {
      setDeleteErr(e instanceof Error ? e.message : 'Could not delete the selected sessions.')
    } finally {
      setDeleting(false)
    }
  }, [selected, loadSessions])

  // Resume an existing session: jump straight to its live view + pull status.
  // RESUME FIX: fetch status IMMEDIATELY using the row's id (not relying on the
  // poll interval, which would leave the view on a blank "No open positions"
  // for up to 12s while positions/LTP/gross are unknown). The poll effect still
  // takes over afterwards; this just makes the first paint correct at once.
  const onResume = useCallback(async (s: SessionSummary) => {
    setError(null)
    setSession({ session_id: s.session_id, status: s.status ?? 'unknown', mode: s.mode ?? 'paper' })
    onSessionChange?.(s.session_id)
    setStartResult(null)
    setStatus(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm('')
    // Seed the countdown from the list row if this is a SCHEDULED session, so
    // the waiting view is correct the instant you resume (the poll re-syncs it).
    setCountdown(isScheduled(s.status) && typeof s.seconds_remaining === 'number' ? s.seconds_remaining : null)
    setPhase('running')
    setPoll(true)
    // Immediate, non-poll-dependent status pull so positions + LTP + gross show
    // right away. Errors here are non-fatal — the poll will retry.
    setBusy('status')
    try {
      const res = await AutoTradeAPI.sessionStatus(s.session_id)
      setStatus(res)
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      } else if (!isScheduled(res.status)) {
        setCountdown(null)
      }
    } catch {
      /* poll will retry; keep the loading state honest */
    } finally {
      setBusy(null)
    }
  }, [onSessionChange])

  // ── Actions ────────────────────────────────────────────────────────────────
  const onCreate = useCallback(async () => {
    setError(null); setCreateSuggest(null); setBusy('create')
    try {
      // UNITS: the backend uses FRACTIONS for percentages (0.01 = 1%). The form
      // captures every pct as a PERCENT so it reads naturally; toWireConfig
      // converts to fractions ONLY at the send boundary (per strategy). State
      // stays in percent (inputs keep showing "1"); no double-conversion.
      const res = await AutoTradeAPI.createSession(
        mode,
        toWireConfig(config, { universeFilter, symbolWhitelist }),
        scope,
      )
      setSession(res)
      setPhase('created')
      setStartResult(null)
      setStatus(null)
      // Notify sibling tabs (Journal) of the newly active session.
      onSessionChange?.(res.session_id)
      // Keep the list fresh so the new session shows the moment you return.
      loadSessions()
    } catch (e) {
      // Execution-date rule: a 400 whose detail says the chosen entry_date isn't
      // a trading day carries the suggested next trading day. Don't dump a raw
      // error — parse it, show a friendly inline message + a one-click apply.
      const detail = e instanceof Error ? e.message : 'Failed to create session'
      if (isNonTradingDayError(detail)) {
        setCreateSuggest({ detail, suggested: parseSuggestedTradingDay(detail) })
      } else {
        setError(detail)
      }
    } finally {
      setBusy(null)
    }
  }, [mode, config, loadSessions, toWireConfig, userId, brokerAccountId, onSessionChange, universeFilter, symbolWhitelist])

  const onStart = useCallback(async (when: StartWhen) => {
    if (!session) return
    // Live placement is blocked on an EXPIRED account — its daily token has
    // lapsed, so a real order would fail. Prompt to re-connect first. Paper is
    // fine (no real order); the global/operator session is unaffected.
    if (session.mode === 'live' && accountExpired) {
      setError(
        `This broker account is EXPIRED — re-connect it first (Broker accounts → ` +
        `Re-connect) before starting a LIVE session on it.`,
      )
      return
    }
    setError(null); setBusy('start')
    try {
      const res = await AutoTradeAPI.startSession(session.session_id, when)
      setStartResult(res)
      // A scheduled start places nothing yet — seed the countdown from the
      // backend's seconds_remaining so the waiting state is immediate.
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      }
      setPhase('running')
      setPoll(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start session')
    } finally {
      setBusy(null)
    }
  }, [session, accountExpired])

  const refreshStatus = useCallback(async (silent = false) => {
    if (!session) return
    if (!silent) { setError(null); setBusy('status') }
    try {
      const res = await AutoTradeAPI.sessionStatus(session.session_id)
      setStatus(res)
      // Re-sync the countdown from the backend on every poll while SCHEDULED;
      // clear it once the session has flipped to RUNNING/CLOSED.
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      } else if (!isScheduled(res.status)) {
        setCountdown(null)
      }
    } catch (e) {
      if (!silent) setError(e instanceof Error ? e.message : 'Failed to load status')
    } finally {
      if (!silent) setBusy(null)
    }
  }, [session])

  const onKill = useCallback(async () => {
    if (!session) return
    setError(null); setBusy('kill')
    try {
      const res = await AutoTradeAPI.killSession(session.session_id)
      setKillArmed(false); setKillConfirm('')
      await refreshStatus(true)
      if (res?.trigger_reason) setError(`Kill complete — ${res.trigger_reason}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to kill session')
    } finally {
      setBusy(null)
    }
  }, [session, refreshStatus])

  // ── Poll status while running ────────────────────────────────────────────────
  // While SCHEDULED, poll FASTER (6s) so the auto-flip to RUNNING (and the
  // placement that comes with it) shows promptly; otherwise 12s is plenty.
  const scheduledNow = isScheduled(status?.status)
  // A non-placed terminal/blocked status from the trading-day rule (rejected /
  // expired / deferred). When true the normal RUNNING + KILL views are hidden in
  // favour of a muted/amber explanation card — it never placed.
  const nonPlacedNow = isNonPlaced(status?.status)
  useEffect(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null }
    if (phase === 'running' && poll && session) {
      refreshStatus(true)
      pollRef.current = setInterval(() => refreshStatus(true), scheduledNow ? 6_000 : 12_000)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [phase, poll, session, refreshStatus, scheduledNow])

  // ── Local 1s ticker for the SCHEDULED countdown ──────────────────────────────
  // Runs only while a countdown is active; floors at 0 (the next poll then
  // confirms the flip to RUNNING). Independent of the auto-refresh toggle.
  useEffect(() => {
    if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null }
    if (phase === 'running' && scheduledNow && countdown !== null) {
      tickRef.current = setInterval(() => {
        setCountdown((c) => (c === null ? null : Math.max(0, c - 1)))
      }, 1_000)
    }
    return () => { if (tickRef.current) clearInterval(tickRef.current) }
  }, [phase, scheduledNow, countdown !== null])  // eslint-disable-line react-hooks/exhaustive-deps

  // Open the explicit New-Session form (resets the in-flight session).
  const openNewSession = () => {
    setPhase('config'); setSession(null); setStartResult(null); setStatus(null)
    setConfig(DEFAULT_CONFIG); setMode('paper'); setCountdown(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm(''); setError(null)
    setCreateSuggest(null)
    // Reset universe filter + picker
    setUniverseFilter('all500'); setPicks(null); setPicksErr(null); setCheckedSymbols(null)
  }

  // Return to the Your-Sessions list and refresh it (so a just-created/started
  // session is visible — the disappearing-session fix).
  const backToList = () => {
    setPhase('list'); setSession(null); setStartResult(null); setStatus(null)
    setCountdown(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm(''); setError(null)
    setCreateSuggest(null)
    loadSessions()
  }

  // ════════════════════════════════════════════════════════════════════════════
  return (
    <div className="flex flex-col gap-4">
      <style>{MECHANISM_CSS}</style>
      {/* Scoped live-indicator pulse (mint), namespaced to avoid token clashes. */}
      <style>{`@keyframes at-live-pulse{0%,100%{opacity:1}50%{opacity:.35}}.live-dot{animation:at-live-pulse 1.6s ease-in-out infinite}@media (prefers-reduced-motion: reduce){.live-dot{animation:none}}`}</style>

      {/* ── Ships-disabled honesty banner (always on) ────────────────────────── */}
      <div
        className="flex items-start gap-2.5 rounded-2xl border px-4 py-3"
        style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)' }}
      >
        <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.shield(16)}</span>
        <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
          <b style={{ color: C.amber }}>Ships disabled.</b>{' '}
          Sessions default to <b>PAPER</b> mode (no real orders). The kill switch is{' '}
          <b>OFF</b> unless you enable it. Real LIVE orders ADDITIONALLY require the
          server flag <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code> — until
          that is set on the backend, a LIVE session stays inert and places nothing.
        </div>
      </div>

      {/* ── LIST PHASE (HOME) — Your Sessions, newest first ──────────────────── */}
      {phase === 'list' && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            <span style={{ color: C.mint }}>{ICON.bolt(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Your sessions</span>
            <div className="ml-auto flex items-center gap-2">
              {/* Delete-selected — appears only when something is checked. These
                  are paper/test session records; deletion is record cleanup. */}
              {selected.size > 0 && (
                <button type="button" disabled={deleting} onClick={onDeleteSelected}
                  className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                  style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                  {ICON.close(12)} {deleting ? 'Deleting…' : `Delete selected (${selected.size})`}
                </button>
              )}
              <button type="button" disabled={sessionsLoading} onClick={loadSessions}
                className="text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors disabled:opacity-40"
                style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                {sessionsLoading ? 'Refreshing…' : 'Refresh'}
              </button>
              <button type="button" onClick={openNewSession}
                className="flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-1.5 rounded-lg transition-opacity"
                style={{ color: '#06130c', background: C.mint }}>
                {ICON.bolt(13)} New session
              </button>
            </div>
          </div>

          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            {sessionsLoading && sessions === null ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading your sessions…</p>
            ) : sessionsErr ? (
              <div className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
                <span>{sessionsErr} <button type="button" onClick={loadSessions} className="underline" style={{ color: C.mint }}>Retry</button></span>
              </div>
            ) : !sessions?.length ? (
              <div className="flex flex-col items-start gap-3">
                <p className="text-[12.5px]" style={{ color: C.muted }}>
                  No sessions yet. Create one to begin — it stays here after you create or
                  reload, so you can resume its live view anytime.
                </p>
                <button type="button" onClick={openNewSession}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold"
                  style={{ color: '#06130c', background: C.mint }}>
                  {ICON.bolt(14)} Create your first session
                </button>
              </div>
            ) : (
              <>
                {/* Honest delete error (record cleanup failed) */}
                {deleteErr && (
                  <div className="mb-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                    style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
                    <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
                    <span>{deleteErr}</span>
                    <button type="button" onClick={() => setDeleteErr(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
                      {ICON.close(12)}
                    </button>
                  </div>
                )}

                {/* Select-all + housekeeping note */}
                <div className="mb-2 flex items-center gap-2.5 px-1">
                  <label className="flex items-center gap-2 text-[11px] cursor-pointer select-none" style={{ color: C.muted }}>
                    <input type="checkbox" checked={allSelected} onChange={toggleSelectAll} />
                    Select all
                  </label>
                  <span className="text-[10.5px]" style={{ color: C.faint }}>
                    Checkboxes select paper/test session records for deletion — they don&apos;t open the session.
                  </span>
                </div>

                <ul className="flex flex-col gap-2">
                {sessions.map((s, i) => {
                  // Backend gross_return is a FRACTION (-0.0136 = -1.36%); ×100 to display.
                  const ret = typeof s.gross_return === 'number' ? s.gross_return * 100 : null
                  const sched = isScheduled(s.status)
                  const running = (s.status ?? '').toUpperCase() === 'RUNNING'
                  const nOpen = typeof s.n_open_positions === 'number' ? s.n_open_positions : null
                  const checked = selected.has(s.session_id)
                  return (
                    <li key={s.session_id ?? i}>
                      <div
                        className="w-full flex items-center gap-3 rounded-xl border px-3.5 py-3 transition-colors"
                        style={{
                          borderColor: checked ? 'rgba(232,115,107,0.45)' : (sched ? 'rgba(63,227,164,0.32)' : C.line2),
                          background: checked ? 'rgba(232,115,107,0.05)' : 'rgba(255,255,255,0.015)',
                        }}>
                        {/* Selection checkbox — not part of the resume click target */}
                        <label className="shrink-0 flex items-center cursor-pointer" onClick={(e) => e.stopPropagation()}>
                          <input type="checkbox" checked={checked}
                            onChange={() => s.session_id && toggleSelect(s.session_id)} />
                        </label>

                        {/* Resume target — the rest of the row opens the live view */}
                        <button type="button" onClick={() => onResume(s)}
                          className="min-w-0 flex-1 flex items-center gap-3 text-left">
                          <span className="shrink-0" style={{ color: C.mint }}>{sched ? ICON.clock(15) : ICON.bolt(15)}</span>
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2">
                              {sched ? <SchedPill /> : running ? <RunningPill /> : (
                                <span className="text-[12.5px] font-semibold truncate" style={{ color: C.ink }}>
                                  {s.status ?? 'session'}
                                </span>
                              )}
                              {s.mode && <ModePill mode={s.mode} />}
                            </div>
                            <div className="text-[10.5px] mt-0.5 font-mono truncate" style={{ color: C.faint }}>
                              {sched && s.fires_at
                                ? <span style={{ color: C.mint }}>fires {s.fires_at}{typeof s.seconds_remaining === 'number' ? ` · in ${fmtCountdown(s.seconds_remaining)}` : ''}</span>
                                : <>{s.session_id}{s.created_at ? ` · ${s.created_at}` : ''}</>}
                            </div>
                          </div>

                          {/* Positions — a RUNNING session holds positions; never
                              look empty. Show the count if the list gives one,
                              else an honest "open" / "—" with the live dot. */}
                          {!sched && (
                            <div className="shrink-0 text-right">
                              <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Positions</div>
                              <div className="text-[13px] font-semibold flex items-center justify-end gap-1.5" style={{ color: nOpen ? C.ink : C.ink2 }}>
                                {running && <span className="inline-block w-1.5 h-1.5 rounded-full live-dot" style={{ background: C.mint }} />}
                                {nOpen != null ? nOpen : (running ? 'open' : '—')}
                              </div>
                            </div>
                          )}

                          <div className="shrink-0 text-right">
                            <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{sched ? 'Status' : 'Return'}</div>
                            <div className="text-[13px] font-semibold" style={{ color: sched ? C.mint : (ret == null ? C.faint : pctTone(ret)) }}>
                              {sched ? 'Scheduled' : (ret == null ? '—' : fmtPct(ret))}
                            </div>
                          </div>
                          <div className="shrink-0 text-right hidden sm:block">
                            <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Capital</div>
                            <div className="text-[13px] font-semibold" style={{ color: C.ink2 }}>
                              {typeof s.total_allocated_capital === 'number' ? fmtCapital(s.total_allocated_capital) : '—'}
                            </div>
                          </div>
                          <span className="shrink-0" style={{ color: C.faint }}>{ICON.chevronR(14)}</span>
                        </button>
                      </div>
                    </li>
                  )
                })}
                </ul>
              </>
            )}
          </div>
        </div>
      )}

      {/* ── CONFIG PHASE — explicit New Session form ─────────────────────────── */}
      {phase === 'config' && (
        <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
          <button type="button" onClick={backToList}
            className="mb-4 inline-flex items-center gap-1.5 text-[12px] px-3 py-1.5 rounded-lg transition-colors"
            style={{ color: C.muted, border: `1px solid ${C.line}` }}>
            ← Your sessions
          </button>

          {/* Strategy selector — picks the exit engine. Switching to the
              intraday basket seeds its validated preset (operator can still
              edit); switching back restores the kill-switch defaults. */}
          <div className="mb-5">
            <Field
              label="Strategy"
              hint={config.strategy === 'intraday_basket'
                ? 'Arms a trailing exit once the basket profits, locks a floor, trails a giveback %, hard-stops, and squares off at a set time.'
                : 'A single flat ±% basket exit on the invested return (the kill switch).'}
            >
              <select
                value={config.strategy}
                onChange={(e) => onStrategyChange(e.target.value as Strategy)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              >
                {STRATEGY_OPTIONS.map((o) => (
                  <option key={o.id} value={o.id} style={{ background: '#0b1410', color: C.ink }}>
                    {o.label}
                  </option>
                ))}
              </select>
            </Field>
          </div>

          {/* Broker account selector — Phase-2 multi-tenant. Optional; only shown
              when a user context exists. '' = the global/operator session
              (default, unchanged). An ACTIVE account runs the session on that
              account; an EXPIRED account is selectable but blocks a LIVE start
              until re-connected (paper is fine). */}
          {userId != null && (
            <div className="mb-5">
              <Field
                label="Broker account"
                hint={selectedAccount
                  ? (accountExpired
                      ? 'This account is EXPIRED — re-connect it (Broker accounts tab) before a LIVE start. Paper is fine.'
                      : 'This session will run on the selected connected account.')
                  : 'Default — the global operator account. Pick a connected account to run this session on it.'}
              >
                <select
                  value={brokerAccountId}
                  onChange={(e) => setBrokerAccountId(e.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                >
                  <option value="" style={{ background: '#0b1410', color: C.ink }}>
                    Global account (operator default)
                  </option>
                  {accounts.map((a) => {
                    const st = (a.status ?? '').toUpperCase()
                    return (
                      <option key={a.broker_account_id} value={a.broker_account_id}
                        style={{ background: '#0b1410', color: C.ink }}>
                        {a.account_label || a.broker_account_id} · {a.broker}{st ? ` · ${st}` : ''}
                      </option>
                    )
                  })}
                </select>
              </Field>
              {selectedAccount && accountExpired && (
                <div className="mt-2 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                  style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
                  <span>This account&apos;s token is <b style={{ color: C.red }}>expired</b>. Re-connect it in the <b>Broker accounts</b> tab before starting a LIVE session on it.</span>
                </div>
              )}
            </div>
          )}

          {/* Mode selector */}
          <div className="flex flex-col sm:flex-row sm:items-center gap-3 mb-5">
            <span className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>Mode</span>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => { setMode('paper'); setLiveConfirm('') }}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{
                  color: mode === 'paper' ? '#06130c' : C.mint,
                  background: mode === 'paper' ? C.mint : 'rgba(63,227,164,0.10)',
                  boxShadow: mode === 'paper' ? 'none' : 'inset 0 0 0 1px rgba(63,227,164,0.4)',
                }}
              >
                {ICON.shield(14)} Paper — safe
              </button>
              <button
                type="button"
                onClick={() => setMode('live')}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{
                  color: mode === 'live' ? '#1a0908' : C.red,
                  background: mode === 'live' ? C.red : 'rgba(232,115,107,0.10)',
                  boxShadow: mode === 'live' ? 'none' : 'inset 0 0 0 1px rgba(232,115,107,0.4)',
                }}
              >
                {ICON.bolt(14)} Live — real orders
              </button>
            </div>
          </div>

          {/* Live warning + typed confirm */}
          {mode === 'live' && (
            <div className="mb-5 rounded-xl border px-3.5 py-3" style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)' }}>
              <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(15)}</span>
                <span>
                  <b style={{ color: C.red }}>LIVE places REAL orders on a connected broker.</b>{' '}
                  It still does nothing until the server flag{' '}
                  <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code> is set — but
                  type <b>LIVE</b> below to confirm you intend a live session.
                </span>
              </div>
              <input
                value={liveConfirm}
                onChange={(e) => setLiveConfirm(e.target.value)}
                placeholder='Type "LIVE" to confirm'
                className="mt-2.5 w-full rounded-lg px-3 py-2 text-[12.5px] outline-none"
                style={{ ...inputStyle, borderColor: 'rgba(232,115,107,0.4)' }}
              />
            </div>
          )}

          {/* Config grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <Field label="Allocated capital" hint="Total capital this session may deploy.">
              <input
                type="number" min={0} step={10000}
                value={config.total_allocated_capital}
                onChange={(e) => set('total_allocated_capital', Number(e.target.value) || 0)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              />
              <div className="flex flex-wrap gap-1.5 mt-1">
                {CAPITAL_PRESETS.map((p) => (
                  <button key={p} type="button" onClick={() => set('total_allocated_capital', p)}
                    className="px-2 py-1 rounded-md text-[10.5px] transition-colors"
                    style={{ color: C.muted, border: `1px solid ${C.line}`, background: 'rgba(255,255,255,0.02)' }}>
                    {fmtCapital(p)}
                  </button>
                ))}
              </div>
            </Field>

            <Field label="Top N stocks" hint="How many of today's picks to trade.">
              <Segmented
                options={TOP_N_OPTIONS.map((n) => ({ id: n, label: String(n) }))}
                value={config.top_n_stocks}
                onChange={(v) => onTopNChange(v)}
              />
            </Field>

            <Field label="Sizing mode" hint={SIZING_OPTIONS.find((s) => s.id === config.sizing_mode)?.hint}>
              <Segmented
                options={SIZING_OPTIONS.map((s) => ({ id: s.id, label: s.label }))}
                value={config.sizing_mode}
                onChange={(v) => set('sizing_mode', v)}
              />
            </Field>

            {/* Instrument — Equity (existing cash path) | Futures (current-month,
                NRML set server-side). Default Equity keeps the equity flow intact. */}
            <Field label="Instrument" hint="Equity trades cash; Futures trades the current-month contract (product set automatically).">
              <Segmented
                options={INSTRUMENT_OPTIONS.map((o) => ({ id: o.id, label: o.label }))}
                value={config.instrument_type ?? 'EQ'}
                onChange={(v) => onInstrumentChange(v)}
              />
            </Field>

            {config.sizing_mode === 'pct_cap' ? (
              <Field label="Max % per position" hint="Cap on any single position.">
                <input
                  type="number" min={1} max={100} step={1}
                  value={config.max_pct_per_position ?? 25}
                  onChange={(e) => set('max_pct_per_position', Number(e.target.value) || 0)}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                />
              </Field>
            ) : (config.instrument_type ?? 'EQ') !== 'FUT' ? (
              <Field label="Order product" hint="Broker product type for entries.">
                <Segmented
                  options={PRODUCT_OPTIONS.map((p) => ({ id: p, label: p }))}
                  value={config.order_product}
                  onChange={(v) => set('order_product', v)}
                />
              </Field>
            ) : null}

            {/* Futures: hide the equity product row; instead pick Buy / Sell (Short)
                and show the auto-product note. Product = NRML is set server-side. */}
            {(config.instrument_type ?? 'EQ') === 'FUT' && (
              <Field label="Direction" hint="Current-month futures · product set automatically (NRML).">
                <Segmented
                  options={DIRECTION_OPTIONS.map((d) => ({ id: d.id, label: d.label }))}
                  value={config.direction ?? 'long'}
                  onChange={(v) => set('direction', v)}
                />
                {(config.direction ?? 'long') === 'short' && (
                  <div
                    className="mt-2 flex items-start gap-2 rounded-lg px-3 py-2 text-[11px] leading-snug"
                    style={{ color: C.amber, border: `1px solid ${C.amber}`, background: 'rgba(230,180,80,0.10)' }}
                    role="note"
                  >
                    <span className="shrink-0 mt-0.5">{ICON.shield(13)}</span>
                    <span>
                      <b>Short futures</b> — margin + unbounded-loss risk; runs in paper until certified.
                    </span>
                  </div>
                )}
              </Field>
            )}

            {config.sizing_mode === 'pct_cap' && (config.instrument_type ?? 'EQ') !== 'FUT' && (
              <Field label="Order product" hint="Broker product type for entries.">
                <Segmented
                  options={PRODUCT_OPTIONS.map((p) => ({ id: p, label: p }))}
                  value={config.order_product}
                  onChange={(v) => set('order_product', v)}
                />
              </Field>
            )}

            <Field label="Entry time (IST)" hint="When the session places entries.">
              <input
                type="time"
                value={(config.entry_time ?? '').slice(0, 5)}
                onChange={(e) => set('entry_time', e.target.value)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              />
            </Field>

            {/* Execution date — optional. Empty = the backend resolves to the
                next valid trading session. Clearing it is one tap (the chip). */}
            <Field label="Entry date (IST)" hint="Optional. Leave empty to fire on the next valid trading session.">
              <div className="flex items-center gap-2">
                <input
                  type="date"
                  value={config.entry_date ?? ''}
                  onChange={(e) => { set('entry_date', e.target.value); setCreateSuggest(null) }}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                />
                {config.entry_date ? (
                  <button type="button" onClick={() => { set('entry_date', ''); setCreateSuggest(null) }}
                    className="shrink-0 text-[11px] px-2.5 py-2 rounded-lg transition-colors"
                    style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                    Clear
                  </button>
                ) : (
                  <span className="shrink-0 text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-1"
                    style={{ color: C.mint, background: 'rgba(63,227,164,0.10)' }}>auto</span>
                )}
              </div>
            </Field>

            {/* On missed window — drop or roll forward. */}
            <Field
              label="If the fire moment is missed"
              hint="If the fire moment is missed or lands on a non-trading day: drop it, or roll to the next trading day."
            >
              <Segmented
                options={ON_MISSED_OPTIONS}
                value={config.on_missed_window ?? 'expire'}
                onChange={(v) => set('on_missed_window', v)}
              />
            </Field>
          </div>

          {/* ── Universe filter + manual stock picker ───────────────────────────
              Placed OUTSIDE the 2-col grid so it spans the full width.
              Both controls are progressive-disclosure refinements above
              the strategy section — the defaults (all500 + top-N) are
              exactly current behaviour, so the form is backward-compatible. */}
          <div className="mt-5 rounded-xl border p-3.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.015)' }}>
            <div className="flex items-center gap-2 mb-3">
              {ICON.trend(15) /* reuse trend icon = filter context */}
              <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Universe</span>
              <span className="text-[10.5px]" style={{ color: C.faint }}>
                Which index to draw picks from · default All 500
              </span>
            </div>

            {/* Universe filter chips — same style as signals page Top20Filters */}
            <div className="flex items-center gap-2 flex-wrap">
              {UNIVERSE_OPTIONS.map((opt) => {
                const active = opt.key === universeFilter
                return (
                  <button
                    key={opt.key}
                    type="button"
                    onClick={() => setUniverseFilter(opt.key)}
                    className="px-3 py-1.5 rounded-full text-xs font-semibold uppercase tracking-wide border transition-colors"
                    style={active ? {
                      color: C.mint,
                      background: 'rgba(63,227,164,0.12)',
                      borderColor: 'rgba(63,227,164,0.4)',
                    } : {
                      color: 'rgba(255,255,255,0.7)',
                      background: 'rgba(255,255,255,0.04)',
                      borderColor: C.line2,
                    }}
                    aria-pressed={active}
                  >
                    {opt.label}
                  </button>
                )
              })}
            </div>

            {/* ── Manual stock picker ─────────────────────────────────────────── */}
            <div className="mt-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-[11.5px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
                  Pick stocks
                </span>
                {checkedSymbols && (
                  <span className="text-[11px]" style={{ color: C.mint }}>
                    {checkedSymbols.size} selected
                    {symbolWhitelist
                      ? <span style={{ color: C.amber }}> · custom override</span>
                      : <span style={{ color: C.faint }}> · default top {config.top_n_stocks}</span>}
                  </span>
                )}
              </div>

              {/* Loading skeleton */}
              {picksLoading && (
                <div className="flex flex-col gap-1.5">
                  {[...Array(Math.min(config.top_n_stocks, 5))].map((_, i) => (
                    <div key={i} className="h-8 rounded-lg animate-pulse" style={{ background: 'rgba(255,255,255,0.05)' }} />
                  ))}
                </div>
              )}

              {/* Graceful-degradation: endpoint not deployed yet */}
              {!picksLoading && picksErr && (
                <div className="flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                  style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(13)}</span>
                  <span>{picksErr}</span>
                </div>
              )}

              {/* Picks list */}
              {!picksLoading && !picksErr && picks && picks.length > 0 && checkedSymbols && (
                <div className="flex flex-col gap-1">
                  {picks.map((p) => {
                    const checked = checkedSymbols.has(p.symbol)
                    const isTopN = p.rank <= config.top_n_stocks
                    return (
                      <label
                        key={p.symbol}
                        className="flex items-center gap-3 rounded-lg px-3 py-2 cursor-pointer transition-colors"
                        style={{
                          background: checked ? 'rgba(63,227,164,0.07)' : 'rgba(255,255,255,0.02)',
                          border: `1px solid ${checked ? 'rgba(63,227,164,0.28)' : C.line2}`,
                        }}
                      >
                        {/* Rank badge */}
                        <span
                          className="shrink-0 w-5 h-5 rounded-md flex items-center justify-center text-[10px] font-bold tabular-nums"
                          style={{
                            background: isTopN ? 'rgba(63,227,164,0.18)' : 'rgba(255,255,255,0.06)',
                            color: isTopN ? C.mint : C.faint,
                          }}
                        >
                          {p.rank}
                        </span>

                        {/* Symbol + sector */}
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>
                              {p.symbol}
                            </span>
                            {p.sector && (
                              <span
                                className="text-[9.5px] uppercase tracking-[0.06em] rounded-full px-1.5 py-0.5"
                                style={{ color: C.muted, background: 'rgba(255,255,255,0.06)', border: `1px solid ${C.line2}` }}
                              >
                                {p.sector}
                              </span>
                            )}
                          </div>
                          {/* Score in small type — secondary info */}
                          <div className="text-[10px] tabular-nums mt-0.5" style={{ color: C.faint }}>
                            score {p.score.toFixed ? p.score.toFixed(0) : p.score}
                            {p.avg_lift != null && p.avg_lift !== 0
                              ? ` · avg lift ${p.avg_lift > 0 ? '+' : ''}${typeof p.avg_lift === 'number' ? p.avg_lift.toFixed(1) : p.avg_lift}%`
                              : ''}
                          </div>
                        </div>

                        {/* Checkbox */}
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => {
                            setCheckedSymbols((prev) => {
                              const next = new Set(prev ?? [])
                              if (next.has(p.symbol)) next.delete(p.symbol)
                              else next.add(p.symbol)
                              return next
                            })
                          }}
                          className="shrink-0 w-4 h-4 accent-mint cursor-pointer"
                        />
                      </label>
                    )
                  })}
                </div>
              )}

              {/* Empty picks list (universe + date with no signals yet) */}
              {!picksLoading && !picksErr && picks && picks.length === 0 && (
                <p className="text-[11.5px]" style={{ color: C.muted }}>
                  No picks available for the selected universe and date.
                  Top N picks will be used by the engine as usual.
                </p>
              )}

              {/* Summary line when a custom override is active */}
              {symbolWhitelist && (
                <div className="mt-2 flex items-center gap-2 text-[11px]" style={{ color: C.ink2 }}>
                  <span style={{ color: C.amber }}>{ICON.info(12)}</span>
                  <span>
                    Custom selection: <b style={{ color: C.ink }}>{symbolWhitelist.join(', ')}</b>{' '}
                    will be sent as <code style={{ color: C.mint }}>symbol_whitelist</code>.
                    <button
                      type="button"
                      onClick={() => {
                        if (picks) {
                          setCheckedSymbols(new Set(
                            picks.filter((p) => p.rank <= config.top_n_stocks).map((p) => p.symbol),
                          ))
                        } else {
                          setCheckedSymbols(null)
                        }
                      }}
                      className="ml-2 underline"
                      style={{ color: C.mint }}
                    >
                      Reset to top {config.top_n_stocks}
                    </button>
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Advanced — entry grace window (tucked away; backend default 120s). */}
          <details className="mt-3 group">
            <summary className="inline-flex items-center gap-1.5 text-[11.5px] cursor-pointer select-none list-none"
              style={{ color: C.muted }}>
              <span className="transition-transform group-open:rotate-90" style={{ color: C.faint }}>{ICON.chevronR(11)}</span>
              Advanced
            </summary>
            <div className="mt-3 max-w-[280px]">
              <Field label="Entry grace (seconds)" hint="How late after the fire moment the session may still fire before it's treated as missed.">
                <input
                  type="number" min={0} step={10}
                  value={config.entry_grace_seconds ?? 120}
                  onChange={(e) => set('entry_grace_seconds', Number(e.target.value) || 0)}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                />
              </Field>
            </div>
          </details>

          {config.sizing_mode === 'manual' && (
            <div className="mt-3 rounded-xl border px-3.5 py-2.5 text-[11px] leading-snug"
              style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.muted }}>
              Manual per-symbol amounts are sent only when configured by the backend
              preset (config/list). This UI creates the session with the chosen mode;
              per-symbol manual amounts are not edited here yet.
            </div>
          )}

          {/* ── intraday_basket: four trail %s + square-off + strategy summary ── */}
          {config.strategy === 'intraday_basket' && (
            <div className="mt-5 rounded-xl border p-3.5" style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.04)' }}>
              <div className="flex items-center gap-2 mb-3">
                <span style={{ color: C.mint }}>{ICON.trend(15)}</span>
                <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Trailing exit (intraday basket)</span>
                <span className="ml-auto text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                  style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
                  Preset loaded
                </span>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Arm / profit (%)" hint="Arms trailing once the basket hits this on notional.">
                  <input type="number" min={0} step={0.05}
                    value={config.arm_pct ?? ''}
                    onChange={(e) => set('arm_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                </Field>
                <Field label="Lock floor (%)" hint="Locks in at least this profit once armed.">
                  <input type="number" min={0} step={0.05}
                    value={config.floor_pct ?? ''}
                    onChange={(e) => set('floor_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                </Field>
                <Field label="Trail giveback (%)" hint="Exits if the basket gives back this much from its peak.">
                  <input type="number" min={0} step={0.05}
                    value={config.trail_giveback_pct ?? ''}
                    onChange={(e) => set('trail_giveback_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                </Field>
                <Field label="Stop loss (%)" hint="Hard exit if the basket drops this much on notional.">
                  <input type="number" min={0} step={0.05}
                    value={config.stop_pct ?? ''}
                    onChange={(e) => set('stop_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                </Field>
                <Field label="Square-off time (IST)" hint="Forces a flat basket at this time (HH:MM).">
                  <input type="time"
                    value={(config.square_off_time ?? '15:29:00').slice(0, 5)}
                    onChange={(e) => set('square_off_time', `${e.target.value}:00`)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                </Field>
              </div>

              <IntradayStrategySummary
                config={config}
                preview={preview}
                loading={previewLoading}
                err={previewErr}
              />
            </div>
          )}

          {/* Kill switch block — portfolio_kill_switch strategy only */}
          {config.strategy === 'portfolio_kill_switch' && (
          <div className="mt-5 rounded-xl border p-3.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.015)' }}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span style={{ color: config.kill_switch_enabled ? C.red : C.faint }}>{ICON.shield(15)}</span>
                <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Kill switch</span>
                <span className="text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                  style={config.kill_switch_enabled
                    ? { color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }
                    : { color: C.faint, background: 'rgba(255,255,255,0.04)' }}>
                  {config.kill_switch_enabled ? 'ON' : 'OFF (default)'}
                </span>
              </div>
              <button
                type="button"
                onClick={() => set('kill_switch_enabled', !config.kill_switch_enabled)}
                className="relative w-11 h-6 rounded-full transition-colors"
                style={{ background: config.kill_switch_enabled ? C.red : 'rgba(255,255,255,0.12)' }}
                aria-pressed={config.kill_switch_enabled}
              >
                <span className="absolute top-0.5 w-5 h-5 rounded-full bg-white transition-all"
                  style={{ left: config.kill_switch_enabled ? '22px' : '2px' }} />
              </button>
            </div>

            {config.kill_switch_enabled && (
              <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Trigger at (%)" hint="Auto-exits when this threshold is hit (on INVESTED return).">
                  <input
                    type="number" min={0} step={0.5}
                    value={config.kill_switch_pct}
                    onChange={(e) => set('kill_switch_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                    style={inputStyle}
                  />
                </Field>
                <Field label="Direction">
                  <Segmented
                    options={KILL_DIR_OPTIONS}
                    value={config.kill_switch_direction}
                    onChange={(v) => set('kill_switch_direction', v)}
                  />
                </Field>
              </div>
            )}

            {/* ── Potential outcome (P&L preview) — only when the kill switch is on ── */}
            {config.kill_switch_enabled && (
              <PotentialOutcome
                preview={preview}
                loading={previewLoading}
                err={previewErr}
                direction={config.kill_switch_direction}
              />
            )}
          </div>
          )}

          {/* ── Non-trading-day suggestion (from a Create 400) ──────────────────
              Friendly inline message + a one-click "Use {date}" that sets
              entry_date and lets the operator retry — instead of a raw error. */}
          {createSuggest && (
            <div className="mt-5 rounded-xl border px-3.5 py-3"
              style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
              <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                <span>
                  <b style={{ color: C.amber }}>That date isn&apos;t a trading day.</b>{' '}
                  {createSuggest.suggested
                    ? <>The next trading day is <b style={{ color: C.ink }}>{createSuggest.suggested}</b>.</>
                    : <>Pick a valid trading day, or clear the date to use the next valid session.</>}
                </span>
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-2.5">
                {createSuggest.suggested && (
                  <button
                    type="button"
                    onClick={() => {
                      set('entry_date', createSuggest.suggested as string)
                      setCreateSuggest(null)
                    }}
                    className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-[12px] font-semibold transition-opacity"
                    style={{ color: '#06130c', background: C.mint }}
                  >
                    {ICON.check(13)} Use {createSuggest.suggested}
                  </button>
                )}
                <button type="button"
                  onClick={() => { set('entry_date', ''); setCreateSuggest(null) }}
                  className="text-[11.5px] px-3 py-1.5 rounded-lg transition-colors"
                  style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Clear date (use next session)
                </button>
                <button type="button" onClick={() => setCreateSuggest(null)}
                  className="text-[11.5px]" style={{ color: C.faint }}>
                  Dismiss
                </button>
              </div>
            </div>
          )}

          {/* Create CTA */}
          <div className="mt-5 flex items-center gap-3">
            <button
              type="button"
              disabled={busy === 'create' || !liveReady}
              onClick={onCreate}
              className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40"
              style={{ color: '#06130c', background: C.mint }}
            >
              {busy === 'create' ? 'Creating…' : <>Create {mode} session {ICON.arrow(13)}</>}
            </button>
            <span className="text-[11px]" style={{ color: C.muted }}>
              {mode === 'live' && !liveReady ? 'Type LIVE above to enable.' : `Creates a ${mode} session — no orders are placed yet.`}
            </span>
          </div>
        </div>
      )}

      {/* ── CREATED PHASE — confirm then Start ───────────────────────────────── */}
      {phase === 'created' && session && (
        <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
          <div className="flex items-center gap-2 mb-1">
            <span style={{ color: C.mint }}>{ICON.check(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Session created</span>
            <ModePill mode={session.mode} />
          </div>
          <div className="text-[11.5px] mb-4" style={{ color: C.muted }}>
            ID <code style={{ color: C.ink2 }}>{session.session_id}</code> · status{' '}
            <span style={{ color: C.ink2 }}>{session.status}</span> · entry{' '}
            <span style={{ color: C.ink2 }}>
              {config.entry_date ? `${config.entry_date} ` : ''}{(config.entry_time ?? '').slice(0, 5)} IST
            </span>
            {!config.entry_date && <span style={{ color: C.faint }}> · next valid session</span>}
          </div>

          {/* TWO clear ways to begin — fire now, or arm for the entry time. */}
          <div className="flex flex-col sm:flex-row sm:items-stretch gap-3">
            {/* Start now → places immediately (RUNNING). */}
            <button
              type="button"
              disabled={busy === 'start'}
              onClick={() => onStart('now')}
              className="flex-1 flex flex-col items-start gap-1 px-4 py-3 rounded-xl text-left transition-opacity disabled:opacity-40"
              style={session.mode === 'live'
                ? { color: '#1a0908', background: C.red }
                : { color: '#06130c', background: C.mint }}
            >
              <span className="flex items-center gap-2 text-[13px] font-semibold">
                {ICON.bolt(14)} {busy === 'start' ? 'Starting…' : 'Start now'}
              </span>
              <span className="text-[10.5px] leading-snug opacity-80">
                Places {session.mode === 'paper' ? 'simulated' : 'real'} entries immediately.
              </span>
            </button>

            {/* Schedule → arms the session to auto-fire at entry_time (SCHEDULED). */}
            <button
              type="button"
              disabled={busy === 'start'}
              onClick={() => onStart('scheduled')}
              className="flex-1 flex flex-col items-start gap-1 px-4 py-3 rounded-xl text-left transition-colors disabled:opacity-40"
              style={{ color: C.mint, background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}
            >
              <span className="flex items-center gap-2 text-[13px] font-semibold">
                {ICON.clock(14)} {busy === 'start' ? 'Scheduling…' : `Schedule for ${config.entry_time}`}
              </span>
              <span className="text-[10.5px] leading-snug" style={{ color: C.muted }}>
                Arms it to auto-fire at {config.entry_time} IST — nothing is placed until then.
              </span>
            </button>
          </div>

          <div className="mt-3 flex items-center gap-3">
            <button type="button" onClick={backToList}
              className="text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              Discard
            </button>
            <p className="text-[11px] leading-snug" style={{ color: C.faint }}>
              {session.mode === 'paper'
                ? 'Paper simulates placement — no broker orders are sent.'
                : 'Live attempts real broker orders, but only if the server flag FALCON_AUTOTRADE_ENABLED is set; otherwise it reports skipped.'}
            </p>
          </div>
        </div>
      )}

      {/* ── RUNNING PHASE — start result + live status + kill ─────────────────── */}
      {phase === 'running' && session && (
        <div className="flex flex-col gap-4">
          {/* SCHEDULED — armed, waiting to fire. Places nothing until entry time. */}
          {scheduledNow && (
            <div className="rounded-2xl border p-4 sm:p-5"
              style={{ borderColor: 'rgba(63,227,164,0.32)', background: 'rgba(63,227,164,0.05)' }}>
              <div className="flex items-center gap-2 mb-3">
                <span style={{ color: C.mint }}>{ICON.clock(17)}</span>
                <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Scheduled — waiting to fire</span>
                {status && <ModePill mode={status.mode} />}
              </div>

              {/* Armed: show fire time + live countdown. Not armed: honest note. */}
              {status?.scheduler_armed === false ? (
                <div className="rounded-xl border px-3.5 py-3 mb-4"
                  style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
                  <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                    <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                    <span>
                      <b style={{ color: C.amber }}>Not armed.</b>{' '}
                      The backend restarted and lost this session&apos;s in-memory timer, so it
                      will NOT auto-fire. Re-schedule it to arm the timer again.
                    </span>
                  </div>
                  <button
                    type="button"
                    disabled={busy === 'start'}
                    onClick={() => onStart('scheduled')}
                    className="mt-3 inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: '#06130c', background: C.mint }}
                  >
                    {ICON.clock(13)} {busy === 'start' ? 'Re-scheduling…' : 'Re-schedule'}
                  </button>
                </div>
              ) : (
                <>
                <div className="flex flex-wrap items-end gap-x-8 gap-y-3 mb-3">
                  <div>
                    <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Fires at (IST)</div>
                    {/* Prefer the backend's RESOLVED fire datetime (the exact moment
                        it will fire after resolving entry_date+time → next valid
                        session), not just the bare time. */}
                    <div className="text-[15px] font-semibold mt-0.5" style={{ color: C.ink }}>
                      {status?.resolved_fire_datetime ?? status?.fires_at ?? `${config.entry_time}`}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>In</div>
                    <div className="text-[20px] font-semibold mt-0.5 tabular-nums" style={{ color: C.mint }}>
                      {countdown !== null
                        ? (countdown <= 0 ? 'firing…' : fmtCountdown(countdown))
                        : (typeof status?.seconds_remaining === 'number' ? fmtCountdown(status.seconds_remaining) : '—')}
                    </div>
                  </div>
                </div>
                {/* "Fires {…} · trading day ✓/✗ · market open/closed" */}
                {status && <ResolvedFireLine status={status} />}
                </>
              )}

              <p className="text-[11px] leading-snug mb-3" style={{ color: C.faint }}>
                Nothing is placed yet. At the entry time the session auto-flips to RUNNING
                and places its entries — this view updates automatically.
              </p>

              {/* Cancel a SCHEDULED session — kill cancels it (places nothing). */}
              <button
                type="button"
                disabled={busy === 'kill'}
                onClick={onKill}
                className="inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}
              >
                {ICON.close(13)} {busy === 'kill' ? 'Cancelling…' : 'Cancel schedule'}
              </button>
            </div>
          )}

          {/* NON-PLACED — rejected / expired / deferred by the trading-day rule.
              Muted/amber so it's obvious the session did NOT place. */}
          {nonPlacedNow && status && <NonPlacedCard status={status} onKill={onKill} busyKill={busy === 'kill'} />}

          {/* Placement result */}
          {!scheduledNow && !nonPlacedNow && startResult && (
            <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
              <div className="flex items-center gap-2 mb-3">
                <span style={{ color: C.mint }}>{ICON.bolt(15)}</span>
                <span className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Placement result</span>
                <ModePill mode={startResult.mode} />
                <span className="ml-auto text-[11.5px]" style={{ color: C.muted }}>
                  {startResult.n_placed} placed · {startResult.orders?.length ?? 0} total
                </span>
              </div>
              {startResult.orders?.length ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-[12px]">
                    <thead>
                      <tr style={{ color: C.faint }}>
                        <th className="text-left font-medium pb-2">Symbol</th>
                        <th className="text-left font-medium pb-2">Status</th>
                        <th className="text-left font-medium pb-2">Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {startResult.orders.map((o, i) => {
                        const placed = /placed|complete|success|open/i.test(o.status)
                        return (
                          <tr key={`${o.symbol}-${i}`} style={{ borderTop: `1px solid ${C.line}` }}>
                            <td className="py-1.5 font-medium" style={{ color: C.ink }}>{o.symbol}</td>
                            <td className="py-1.5">
                              <span style={{ color: placed ? C.mint : C.amber }}>{o.status}</span>
                            </td>
                            <td className="py-1.5" style={{ color: C.muted }}>{o.reason ?? '—'}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-[12px]" style={{ color: C.muted }}>No orders reported.</p>
              )}
            </div>
          )}

          {/* Live status — the normal RUNNING view (hidden while SCHEDULED or
              when a trading-day-rule status means it never placed). */}
          {!scheduledNow && !nonPlacedNow && (
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 mb-4">
              <Gear size={18} dir={1} />
              <span className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Live status</span>
              {status && <ModePill mode={status.mode} />}
              {status?.strategy && <StrategyPill strategy={status.strategy} />}
              <div className="ml-auto flex items-center gap-2">
                <label className="flex items-center gap-1.5 text-[11px] cursor-pointer" style={{ color: C.muted }}>
                  <input type="checkbox" checked={poll} onChange={(e) => setPoll(e.target.checked)} />
                  Auto-refresh
                </label>
                <button type="button" disabled={busy === 'status'} onClick={() => refreshStatus(false)}
                  className="text-[11.5px] px-2.5 py-1 rounded-lg transition-colors disabled:opacity-40"
                  style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.3)` }}>
                  {busy === 'status' ? 'Refreshing…' : 'Refresh'}
                </button>
              </div>
            </div>

            {!status ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading status…</p>
            ) : (
              <>
                {/* DUAL RETURN — both returns, clearly labelled. All backend
                    figures are FRACTIONS → ×100. The kill switch triggers on the
                    INVESTED return (gross_return), so that one is the kill basis. */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-3">
                  <Stat label="Status" value={status.status} />
                  <Stat
                    label="Return (invested)"
                    value={fmtPct(status.gross_return * 100)}
                    valueColor={pctTone(status.gross_return)}
                    sub="kill basis"
                  />
                  <Stat
                    label="Return (on fund)"
                    value={typeof status.gross_return_fund === 'number' ? fmtPct(status.gross_return_fund * 100) : '—'}
                    valueColor={typeof status.gross_return_fund === 'number' ? pctTone(status.gross_return_fund) : undefined}
                    sub="÷ your fund"
                  />
                  <Stat label="Open positions" value={String(status.n_open_positions)} />
                </div>

                {/* The two ₹ bases behind the returns, so "invested" vs "fund" is
                    unambiguous (MTF = leveraged invested value, CNC = cash). */}
                <div className="flex flex-wrap items-center gap-x-5 gap-y-1 mb-3 text-[11px]" style={{ color: C.faint }}>
                  <span>
                    Invested basis{' '}
                    <b style={{ color: C.ink2 }}>{typeof status.invested_basis === 'number' ? fmtINR(status.invested_basis) : '—'}</b>
                    {' '}· the kill basis
                  </span>
                  <span>
                    Fund{' '}
                    <b style={{ color: C.ink2 }}>{fmtCapital(status.total_allocated_capital)}</b>
                  </span>
                </div>

                {/* Resolved fire moment + trading-day / market state. Shown when
                    the backend reports it (degrades to nothing when absent). */}
                <ResolvedFireLine status={status} className="mb-3" />

                {/* SPEED strip — deploy/exit latency + the live data-freshness
                    heartbeat. Works for BOTH strategies; sits above the
                    strategy-specific panels and doesn't disturb them. */}
                <SpeedStrip status={status} />

                {/* intraday_basket → the live TRAIL STATUS PANEL. */}
                {status.strategy === 'intraday_basket' && (
                  <div className="mb-4">
                    <TrailStatusPanel status={status} />
                  </div>
                )}

                {/* portfolio_kill_switch → the kill-switch readout + live preview
                    (unchanged). Hidden for intraday_basket, which trails instead. */}
                {status.strategy !== 'intraday_basket' && (
                  <>
                {/* Kill-switch state readout — threshold is a backend FRACTION (0.01 = 1%). */}
                <div className="flex items-center gap-2 mb-3 text-[11.5px]" style={{ color: C.muted }}>
                  <span style={{ color: status.kill_switch_enabled ? C.red : C.faint }}>{ICON.shield(14)}</span>
                  Kill switch{' '}
                  <b style={{ color: status.kill_switch_enabled ? C.red : C.faint }}>
                    {status.kill_switch_enabled ? 'ARMED' : 'OFF'}
                  </b>
                  {status.kill_switch_enabled && (
                    <span>· triggers at ±{(status.kill_switch_pct * 100).toFixed(2).replace(/\.?0+$/, '')}% {status.kill_switch_direction} on the <b style={{ color: C.ink2 }}>invested</b> return</span>
                  )}
                </div>

                {/* LIVE, exact kill-switch outcome for the running session (same
                    shape as the config preview) — surfaced when the backend sends it. */}
                {status.kill_switch_enabled && status.kill_preview && (status.kill_preview.target || status.kill_preview.stop) && (
                  <div className="mb-4">
                    <KillPreviewCard kill={status.kill_preview} direction={status.kill_switch_direction} live />
                  </div>
                )}
                  </>
                )}

                {/* Open positions table — Kite-style: absolute P&L (₹) + Chg % per row */}
                {status.open_positions?.length ? (
                  <>
                  <div className="overflow-x-auto">
                    <table className="w-full text-[12px]">
                      <thead>
                        <tr style={{ color: C.faint }}>
                          <th className="text-left font-medium pb-2">Symbol</th>
                          <th className="text-right font-medium pb-2">Qty</th>
                          <th className="text-right font-medium pb-2">Avg</th>
                          <th className="text-right font-medium pb-2">LTP</th>
                          <th className="text-right font-medium pb-2">P&amp;L (₹)</th>
                          <th className="text-right font-medium pb-2">Chg</th>
                        </tr>
                      </thead>
                      <tbody>
                        {status.open_positions.map((p, i) => {
                          // Backend sends `ltp` + `avg_price`; per-position Chg %
                          // is derived from them (no return_pct field exists). The
                          // absolute P&L (₹) is the backend's own `unrealised_pnl`.
                          const ret = (typeof p.ltp === 'number' && typeof p.avg_price === 'number' && p.avg_price > 0)
                            ? ((p.ltp - p.avg_price) / p.avg_price) * 100
                            : null
                          const pnl = typeof p.unrealised_pnl === 'number' ? p.unrealised_pnl : null
                          return (
                            <tr key={`${p.symbol ?? i}`} style={{ borderTop: `1px solid ${C.line}` }}>
                              <td className="py-1.5 font-medium" style={{ color: C.ink }}>{p.symbol ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.qty ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.avg_price ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.ltp ?? '—'}</td>
                              <td className="py-1.5 text-right font-medium tabular-nums" style={{ color: pnl == null ? C.faint : pctTone(pnl) }}>
                                {pnl == null ? '—' : signedINR(pnl)}
                              </td>
                              <td className="py-1.5 text-right" style={{ color: ret == null ? C.faint : pctTone(ret) }}>
                                {ret == null ? '—' : fmtPct(ret)}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                  <PortfolioSummary positions={status.open_positions} />
                  </>
                ) : (
                  <p className="text-[12px]" style={{ color: C.muted }}>No open positions.</p>
                )}
              </>
            )}
          </div>
          )}

          {/* KILL block — hidden while SCHEDULED (use "Cancel schedule" above) and
              when a trading-day-rule status means there's nothing to kill. */}
          {!scheduledNow && !nonPlacedNow && (
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: 'rgba(232,115,107,0.32)', background: 'rgba(232,115,107,0.04)' }}>
            <div className="flex items-center gap-2 mb-2">
              <span style={{ color: C.red }}>{ICON.bolt(15)}</span>
              <span className="text-[13.5px] font-semibold" style={{ color: C.red }}>Kill session</span>
            </div>
            <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.ink2 }}>
              Immediately exits all open positions and stops this session.
              {' '}This is irreversible for the session.
            </p>
            {!killArmed ? (
              <button type="button" onClick={() => setKillArmed(true)}
                className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                Kill session…
              </button>
            ) : (
              <div className="flex flex-col sm:flex-row sm:items-center gap-2.5">
                <input
                  value={killConfirm}
                  onChange={(e) => setKillConfirm(e.target.value)}
                  placeholder='Type "KILL" to confirm'
                  className="rounded-lg px-3 py-2 text-[12.5px] outline-none sm:w-56"
                  style={{ ...inputStyle, borderColor: 'rgba(232,115,107,0.4)' }}
                />
                <button
                  type="button"
                  disabled={busy === 'kill' || killConfirm.trim().toUpperCase() !== 'KILL'}
                  onClick={onKill}
                  className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                  style={{ color: '#1a0908', background: C.red }}
                >
                  {busy === 'kill' ? 'Killing…' : 'Confirm kill'}
                </button>
                <button type="button" onClick={() => { setKillArmed(false); setKillConfirm('') }}
                  className="text-[12px] px-3 py-2 rounded-lg" style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Cancel
                </button>
              </div>
            )}
          </div>
          )}

          <div className="flex items-center gap-2">
            <button type="button" onClick={backToList}
              className="self-start text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              ← Your sessions
            </button>
            <button type="button" onClick={openNewSession}
              className="self-start text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.3)` }}>
              New session
            </button>
          </div>
        </div>
      )}

      {/* ── Error toast ──────────────────────────────────────────────────────── */}
      {error && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
            {ICON.close(13)}
          </button>
        </div>
      )}

      {/* ── Broker allowlist (egress IP self-service) ────────────────────────── */}
      <EgressIpCard />

      {/* ── Read-only saved configs + brokers ────────────────────────────────── */}
      <ReferenceLists />
    </div>
  )
}

// ── Mode pill ────────────────────────────────────────────────────────────────
function ModePill({ mode }: { mode: Mode }) {
  const live = mode === 'live'
  return (
    <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={live
        ? { color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }
        : { color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {mode}
    </span>
  )
}

// Strategy pill — names the exit engine in the live header. Mint for both; the
// label disambiguates (kill-switch vs intraday trailing basket).
function StrategyPill({ strategy }: { strategy: Strategy }) {
  const label = strategy === 'intraday_basket' ? 'Intraday Basket' : 'Kill Switch'
  return (
    <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {label}
    </span>
  )
}

// ── Resolved fire line (execution-date / trading-day rule) ───────────────────
// "Fires {resolved_fire_datetime IST} · trading day ✓/✗ · market open/closed".
// Renders only what the backend actually returned — every absent field becomes
// an honest "—"; if nothing relevant is present it renders nothing at all.
function ResolvedFireLine({ status, className = '' }: { status: StatusResponse; className?: string }) {
  const fire = status.resolved_fire_datetime ?? status.resolved_fire_date
  const hasTradingDay = typeof status.is_trading_day === 'boolean'
  const hasMarket = typeof status.market_open_now === 'boolean'
  // Nothing to show → render nothing (don't fabricate a line).
  if (!fire && !hasTradingDay && !hasMarket) return null

  const tradingColor = hasTradingDay ? (status.is_trading_day ? C.mint : C.amber) : C.faint
  const marketColor = hasMarket ? (status.market_open_now ? C.mint : C.muted) : C.faint

  return (
    <div className={`flex flex-wrap items-center gap-x-2 gap-y-1 text-[11.5px] ${className}`} style={{ color: C.muted }}>
      <span style={{ color: C.faint }}>{ICON.clock(13)}</span>
      <span>
        Fires{' '}
        <b style={{ color: C.ink }}>{fire ?? '—'}</b>
        {fire && <span style={{ color: C.faint }}> IST</span>}
      </span>
      <span style={{ color: C.faint }}>·</span>
      <span>
        trading day{' '}
        <b style={{ color: tradingColor }}>
          {hasTradingDay ? (status.is_trading_day ? '✓' : '✗') : '—'}
        </b>
      </span>
      <span style={{ color: C.faint }}>·</span>
      <span>
        market{' '}
        <b style={{ color: marketColor }}>
          {hasMarket ? (status.market_open_now ? 'open' : 'closed') : '—'}
        </b>
      </span>
    </div>
  )
}

// ── Non-placed card (trading-day rule) ───────────────────────────────────────
// Surfaces REJECTED_NON_TRADING_DAY / EXPIRED_MISSED_WINDOW / DEFERRED_MARKET_CLOSED
// distinctly: a MUTED/AMBER treatment (never the green RUNNING look) so it's
// obvious the session did NOT place. Shows the friendly status label, the
// backend's deferred_reason (verbatim when present), the resolved-fire line, and
// the echoed entry_date / on_missed_window. DEFERRED keeps a Cancel action.
function NonPlacedCard({ status, onKill, busyKill }: { status: StatusResponse; onKill: () => void; busyKill: boolean }) {
  const raw = (status.status ?? '').toUpperCase()
  const deferred = raw === 'DEFERRED_MARKET_CLOSED'
  // Default reasons when the backend doesn't supply deferred_reason.
  const fallbackReason =
    raw === 'REJECTED_NON_TRADING_DAY' ? 'The chosen entry date is not a trading day, so the session was rejected.'
    : raw === 'EXPIRED_MISSED_WINDOW'  ? 'The entry window was missed (past the grace period) and on-missed was set to expire — nothing was placed.'
    : raw === 'DEFERRED_MARKET_CLOSED' ? 'The fire moment landed while the market was closed — the session is deferred and placed nothing.'
    : 'This session did not place.'
  const reason = status.deferred_reason ?? fallbackReason

  return (
    <div className="rounded-2xl border p-4 sm:p-5"
      style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
      <div className="flex items-center gap-2 mb-2">
        <span style={{ color: C.amber }}>{ICON.info(17)}</span>
        <span className="text-[14px] font-semibold" style={{ color: C.amber }}>{nonPlacedLabel(raw)}</span>
        {status.mode && <ModePill mode={status.mode} />}
        <span className="ml-auto text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
          style={{ color: C.amber, background: 'rgba(230,180,80,0.14)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
          did not place
        </span>
      </div>

      <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.ink2 }}>{reason}</p>

      {/* Resolved fire moment + trading-day / market state (degrades to "—"). */}
      <ResolvedFireLine status={status} className="mb-3" />

      {/* The echoed config that drove the outcome. */}
      <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-[11px] mb-1" style={{ color: C.faint }}>
        <span>entry date <b style={{ color: C.ink2 }}>{status.entry_date || '—'}</b></span>
        <span>on missed <b style={{ color: C.ink2 }}>{status.on_missed_window ?? '—'}</b></span>
        <span>status <b style={{ color: C.amber }}>{raw}</b></span>
      </div>

      {/* A DEFERRED session is still alive (awaiting the next open) — let the
          operator cancel it. Rejected/expired are terminal — nothing to cancel. */}
      {deferred && (
        <button
          type="button"
          disabled={busyKill}
          onClick={onKill}
          className="mt-3 inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
          style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}
        >
          {ICON.close(13)} {busyKill ? 'Cancelling…' : 'Cancel session'}
        </button>
      )}
    </div>
  )
}

// ── Speed / latency strip (BOTH strategies) ──────────────────────────────────
// A compact readout of the three backend latency ints (ms, may be null):
//   • Entry — fire start → all legs settled (deploy speed); neutral.
//   • Exit  — flatten trigger → all flat (exit speed); shown ONLY once a flatten
//             has happened (exit_latency_ms present), so it doesn't imply a
//             flatten that hasn't occurred. Neutral.
//   • Data  — age of the newest tick used; the monitoring heartbeat. Coloured as
//             a liveness signal (green sub-second, amber lagging, red/stale/—).
// Every value degrades to "—" when not measured; nothing is fabricated.
function SpeedStrip({ status }: { status: StatusResponse }) {
  const entry = status.entry_latency_ms
  const exit = status.exit_latency_ms
  const tickAge = status.last_tick_age_ms
  const live = tickLiveness(tickAge)
  const showExit = exit != null && Number.isFinite(exit)

  return (
    <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1.5 rounded-xl border px-3.5 py-2.5"
      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)' }}>
      <span className="text-[10px] font-semibold uppercase tracking-[0.06em]" style={{ color: C.faint }}>Speed</span>

      {/* Entry deploy speed — neutral */}
      <span className="text-[11.5px]" style={{ color: C.muted }}>
        Entry <b className="tabular-nums" style={{ color: C.ink2 }}>{fmtMs(entry)}</b>
      </span>

      {/* Exit speed — only once a flatten has actually been measured */}
      {showExit && (
        <span className="text-[11.5px]" style={{ color: C.muted }}>
          Exit <b className="tabular-nums" style={{ color: C.ink2 }}>{fmtMs(exit)}</b>
        </span>
      )}

      {/* Data freshness — the live heartbeat, coloured by liveness */}
      <span className="inline-flex items-center gap-1.5 text-[11.5px]" style={{ color: C.muted }}>
        <span
          className={`inline-block w-1.5 h-1.5 rounded-full${live.fresh ? ' live-dot' : ''}`}
          style={{ background: live.color }}
        />
        Data <b className="tabular-nums" style={{ color: live.color }}>{fmtMs(tickAge)}</b>
      </span>

      {/* Honest liveness label (last tick age) */}
      <span className="text-[10px] font-mono uppercase tracking-[0.05em]" style={{ color: live.color }}>
        {live.label}
      </span>
    </div>
  )
}

// ── Live trail status (intraday_basket) ──────────────────────────────────────
// Reads the nested `trail{...}` (falling back to the flat mirror fields), all
// FRACTIONS (×100 to display). Shows armed state, peak, current notional return,
// the live exit-trigger level, the four configured numbers, and a square-off
// countdown. On a CLOSED session it shows the exit reason + the dual final
// returns. Every field degrades to "—" when absent — nothing is fabricated.
function TrailStatusPanel({ status }: { status: StatusResponse }) {
  const t = status.trail ?? {}
  // Prefer nested trail fields; fall back to the flat status mirrors.
  const armed = t.armed ?? status.trail_armed
  const peak = t.peak ?? status.trail_peak
  const current = t.current_gross_return ?? status.gross_return
  const trigger = t.trigger ?? status.trail_trigger
  const armPct = t.arm_pct
  const floorPct = t.floor_pct
  const trailPct = t.trail_giveback_pct
  const stopPct = t.stop_pct
  const sqTime = t.square_off_time ?? status.square_off_time ?? '—'
  const sqSecs = t.seconds_to_square_off ?? status.seconds_to_square_off
  const closed = (status.status ?? '').toUpperCase() === 'CLOSED'

  // Plain-English trail line, built only from fields the backend actually sent.
  const armedLine = armed
    ? `Armed at ${fracPct(armPct)} · peak ${fracPct(peak)} · exits if it gives back to ${fracPct(trigger)}`
    : `Not armed yet — arms once the basket reaches ${fracPct(armPct)} on notional.`

  return (
    <div className="rounded-xl border p-3.5" style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.05)' }}>
      <div className="flex items-center gap-2 mb-3">
        <span style={{ color: C.mint }}>{ICON.trend(15)}</span>
        <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Trailing exit</span>
        <span className="text-[9px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
          style={armed
            ? { color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }
            : { color: C.faint, background: 'rgba(255,255,255,0.04)' }}>
          {armed == null ? 'TRAIL —' : armed ? 'ARMED' : 'NOT ARMED'}
        </span>
        <span className="ml-auto text-[11px]" style={{ color: C.faint }}>
          square-off <b style={{ color: C.ink2 }}>{sqTime !== '—' ? sqTime.slice(0, 5) : '—'}</b>
          {' '}· in <b style={{ color: C.mint }}>{fmtMMSS(sqSecs)}</b>
        </span>
      </div>

      {/* Closed → exit reason + dual final returns. Else the live trail metrics. */}
      {closed ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mb-3">
          <Stat label="Exit reason" value={status.exit_reason ?? '—'} valueColor={C.ink} />
          <Stat
            label="Final (notional)"
            value={fracPct(status.notional_return)}
            valueColor={pctTone(status.notional_return)}
          />
          <Stat
            label="Final (own funds)"
            value={fracPct(status.own_funds_return)}
            valueColor={pctTone(status.own_funds_return)}
          />
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-3">
            <Stat label="Current (notional)" value={fracPct(current)} valueColor={pctTone(current)} sub="trail basis" />
            <Stat label="Peak" value={fracPct(peak)} valueColor={pctTone(peak)} />
            <Stat label="Exit trigger" value={fracPct(trigger)} valueColor={C.ink} sub="exits here" />
            <Stat label="Square-off in" value={fmtMMSS(sqSecs)} valueColor={C.mint} />
          </div>
          <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.muted }}>{armedLine}</p>
        </>
      )}

      {/* The four configured numbers (×100), always shown for legibility. */}
      <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-[11px]" style={{ color: C.faint }}>
        <span>arm <b style={{ color: C.mint }}>{fracPct(armPct, false)}</b></span>
        <span>floor <b style={{ color: C.mint }}>{fracPct(floorPct, false)}</b></span>
        <span>trail <b style={{ color: C.ink2 }}>{fracPct(trailPct, false)}</b></span>
        <span>stop <b style={{ color: C.red }}>{fracPct(stopPct, false)}</b></span>
      </div>
    </div>
  )
}

// RUNNING status pill — mint with a pulsing live dot so a running session that
// holds positions never reads as empty/"no orders" in the list.
function RunningPill() {
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      <span className="inline-block w-1.5 h-1.5 rounded-full live-dot" style={{ background: C.mint }} />
      Running
    </span>
  )
}

// SCHEDULED status pill — mint, distinct from RUNNING/CLOSED in the list.
function SchedPill() {
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {ICON.clock(10)} Scheduled
    </span>
  )
}

// ── Kite-style portfolio footer ──────────────────────────────────────────────
// Invested = Σ(qty×avg_price); Current value = Σ(qty×ltp); Total P&L =
// Σ(unrealised_pnl) shown as ₹ AND % (Total P&L ÷ Invested ×100). Each cell is
// HONEST: a field is summed only when every contributing position supplies it;
// if any required field is missing the cell shows "—" (never a partial/fabricated
// total). Total P&L prefers the backend's own unrealised_pnl sum.
function PortfolioSummary({ positions }: { positions: OpenPosition[] }) {
  const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null)

  let invested = 0; let investedOk = positions.length > 0
  let current = 0; let currentOk = positions.length > 0
  let pnl = 0; let pnlOk = positions.length > 0

  for (const p of positions) {
    const qty = num(p.qty); const avg = num(p.avg_price); const ltp = num(p.ltp); const up = num(p.unrealised_pnl)
    if (qty != null && avg != null) invested += qty * avg; else investedOk = false
    if (qty != null && ltp != null) current += qty * ltp; else currentOk = false
    if (up != null) pnl += up; else pnlOk = false
  }

  const investedVal = investedOk ? invested : null
  const currentVal = currentOk ? current : null
  const pnlVal = pnlOk ? pnl : null
  const pnlPct = pnlVal != null && investedVal != null && investedVal > 0 ? (pnlVal / investedVal) * 100 : null

  return (
    <div className="mt-3 grid grid-cols-2 sm:grid-cols-3 gap-3 rounded-xl border px-3.5 py-3"
      style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <SumCell label="Invested" value={investedVal == null ? '—' : fmtINR(investedVal)} />
      <SumCell label="Current value" value={currentVal == null ? '—' : fmtINR(currentVal)} />
      <SumCell
        label="Total P&L"
        value={pnlVal == null ? '—' : signedINR(pnlVal)}
        sub={pnlPct == null ? undefined : fmtPct(pnlPct, 2)}
        valueColor={pnlVal == null ? C.faint : pctTone(pnlVal)}
      />
    </div>
  )
}

function SumCell({ label, value, sub, valueColor }: { label: string; value: string; sub?: string; valueColor?: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[15px] font-semibold mt-0.5 tabular-nums" style={{ color: valueColor ?? C.ink }}>
        {value}{sub && <span className="text-[12px] font-medium ml-1.5" style={{ color: valueColor ?? C.muted }}>({sub})</span>}
      </div>
    </div>
  )
}

function Stat({ label, value, valueColor, sub }: { label: string; value: string; valueColor?: string; sub?: string }) {
  return (
    <div className="rounded-xl border px-3 py-2.5" style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)' }}>
      <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[15px] font-semibold mt-0.5" style={{ color: valueColor ?? C.ink }}>{value}</div>
      {sub && <div className="text-[9.5px] mt-0.5" style={{ color: C.faint }}>{sub}</div>}
    </div>
  )
}

// ── P&L preview card (CONFIG form) ───────────────────────────────────────────
// Renders the "Potential outcome" estimate from POST /api/autotrade/preview.
// Honest "—" / loading / error states; nothing is fabricated. UNITS: every pct
// from the backend is a FRACTION (×100 to display); ₹ via fmtINR/signedINR.
function PotentialOutcome({
  preview, loading, err, direction,
}: { preview: PreviewResponse | null; loading: boolean; err: string | null; direction: KillDirection }) {
  return (
    <div className="mt-4 rounded-xl border p-3.5" style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.04)' }}>
      <div className="flex items-center gap-2 mb-2">
        <span style={{ color: C.mint }}>{ICON.info(14)}</span>
        <span className="text-[12px] font-semibold" style={{ color: C.ink }}>Potential outcome</span>
        <span className="ml-auto text-[10px]" style={{ color: C.faint }}>estimate — places nothing</span>
      </div>

      {loading && !preview ? (
        <p className="text-[11.5px]" style={{ color: C.muted }}>Estimating…</p>
      ) : err ? (
        <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
          <span style={{ color: C.amber }}>Couldn&apos;t estimate the outcome</span>{' '}
          <span style={{ color: C.faint }}>({err}) — the preview endpoint may not be reporting yet. Showing &ldquo;—&rdquo;.</span>
        </p>
      ) : preview ? (
        <>
          <KillPreviewCard kill={preview.kill_preview} direction={direction} />
          <p className="text-[10.5px] leading-snug mt-2.5" style={{ color: C.faint }}>
            on <b style={{ color: C.ink2 }}>{fmtINR(preview.invested_basis)}</b> invested
            {' '}· ~{(preview.leverage ?? 1).toFixed(preview.leverage % 1 === 0 ? 0 : 2)}× MTF
            {' '}· fund <b style={{ color: C.ink2 }}>{fmtCapital(preview.total_allocated_capital)}</b>
          </p>
        </>
      ) : (
        <p className="text-[11.5px]" style={{ color: C.faint }}>—</p>
      )}
    </div>
  )
}

// ── Intraday-basket strategy summary (CONFIG form) ───────────────────────────
// A single legible line describing the configured trail, on the ESTIMATED bases
// from POST /api/autotrade/preview (invested_basis + leverage). The pct fields
// come from the config in PERCENTS (display as-is). Honest "—"/loading/error.
function IntradayStrategySummary({
  config, preview, loading, err,
}: { config: SessionConfig; preview: PreviewResponse | null; loading: boolean; err: string | null }) {
  const num = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : null)
  const arm = num(config.arm_pct); const floor = num(config.floor_pct)
  const trail = num(config.trail_giveback_pct); const stop = num(config.stop_pct)
  const sq = (config.square_off_time ?? '').slice(0, 5) || '—'
  const entry = (config.entry_time ?? '').slice(0, 5) || '—'
  const trim = (v: number | null) => (v == null ? '—' : String(v))

  return (
    <div className="mt-4 rounded-xl border p-3.5" style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.05)' }}>
      <div className="flex items-center gap-2 mb-2">
        <span style={{ color: C.mint }}>{ICON.info(14)}</span>
        <span className="text-[12px] font-semibold" style={{ color: C.ink }}>Strategy summary</span>
        <span className="ml-auto text-[10px]" style={{ color: C.faint }}>estimate — places nothing</span>
      </div>
      <p className="text-[12px] leading-relaxed" style={{ color: C.ink2 }}>
        Enter <b style={{ color: C.ink }}>{entry}</b>
        {' → '}arm <b style={{ color: C.mint }}>+{trim(arm)}%</b>
        {' → '}trail <b style={{ color: C.ink }}>{trim(trail)}%</b> giveback
        {' '}(floor <b style={{ color: C.mint }}>+{trim(floor)}%</b>)
        {' → '}stop <b style={{ color: C.red }}>−{trim(stop)}%</b>
        {' → '}square-off <b style={{ color: C.ink }}>{sq}</b>
      </p>
      <p className="text-[10.5px] leading-snug mt-2" style={{ color: C.faint }}>
        {loading && !preview ? (
          'Estimating notional…'
        ) : err ? (
          <><span style={{ color: C.amber }}>Couldn&apos;t estimate the notional</span>{' '}({err}) — showing &ldquo;—&rdquo;.</>
        ) : preview ? (
          <>on <b style={{ color: C.ink2 }}>{fmtINR(preview.invested_basis)}</b> notional
            {' '}· ~{(preview.leverage ?? 1).toFixed(preview.leverage % 1 === 0 ? 0 : 2)}× ({config.order_product})
            {' '}· fund <b style={{ color: C.ink2 }}>{fmtCapital(preview.total_allocated_capital)}</b></>
        ) : '—'}
      </p>
    </div>
  )
}

// The two-sided kill outcome (target / stop). Shared by the CONFIG preview and
// the LIVE running status. Each side: ₹ on the INVESTED basis + (% on your fund).
// UNITS: pct/fund_pct are FRACTIONS (×100); basis_value_rs is ₹.
function KillPreviewCard({ kill, direction, live }: { kill: KillPreview; direction: KillDirection; live?: boolean }) {
  const showTarget = (direction === 'profit' || direction === 'both') && kill.target
  const showStop = (direction === 'loss' || direction === 'both') && kill.stop
  if (!showTarget && !showStop) {
    return <p className="text-[11.5px]" style={{ color: C.faint }}>—</p>
  }
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
      {kill.target && (direction === 'profit' || direction === 'both') && (
        <OutcomeSide
          label={`At +${(kill.target.pct * 100).toFixed(2).replace(/\.?0+$/, '')}% target`}
          rs={kill.target.basis_value_rs}
          fundPct={kill.target.fund_pct}
          positive
          live={live}
        />
      )}
      {kill.stop && (direction === 'loss' || direction === 'both') && (
        <OutcomeSide
          label={`At −${(kill.stop.pct * 100).toFixed(2).replace(/\.?0+$/, '')}% stop`}
          rs={kill.stop.basis_value_rs}
          fundPct={kill.stop.fund_pct}
          positive={false}
          live={live}
        />
      )}
    </div>
  )
}

function OutcomeSide({
  label, rs, fundPct, positive, live,
}: { label: string; rs: number; fundPct: number; positive: boolean; live?: boolean }) {
  // basis_value_rs is reported as a magnitude on the invested basis; sign it by
  // side (target = +, stop = −) so the ₹ + % read consistently. fund_pct is a
  // FRACTION (×100). Never fabricate — caller only renders sides the backend gave.
  const signedRs = positive ? Math.abs(rs) : -Math.abs(rs)
  const signedFund = positive ? Math.abs(fundPct) : -Math.abs(fundPct)
  const tone = positive ? C.mint : C.red
  return (
    <div className="rounded-lg border px-3 py-2.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>
        {label}{live && <span className="ml-1.5" style={{ color: C.mint }}>· live</span>}
      </div>
      <div className="text-[15px] font-semibold mt-0.5 tabular-nums" style={{ color: tone }}>
        {Number.isFinite(rs) ? signedINR(signedRs) : '—'}
      </div>
      <div className="text-[10.5px] mt-0.5" style={{ color: tone }}>
        {Number.isFinite(fundPct) ? `${fmtPct(signedFund * 100, 2)} on your fund` : '—'}
      </div>
    </div>
  )
}

// ── Broker allowlist IP self-service ─────────────────────────────────────────
// Shows the backend's outbound IP so the operator can add it to the broker's
// Allowed-IPs list (developers.kite.trade) — otherwise live orders error. Loads
// on mount; graceful if the endpoint isn't live yet (shows "—", no fabrication).
function EgressIpCard() {
  const [ip, setIp] = useState<string | null>(null)
  const [asOf, setAsOf] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const load = useCallback(async () => {
    setLoading(true); setErr(null)
    try {
      const res = await AutoTradeAPI.egressIp()
      setIp(typeof res.ip === 'string' && res.ip ? res.ip : null)
      setAsOf(typeof res.as_of === 'string' ? res.as_of : null)
    } catch (e) {
      setIp(null)
      setErr(e instanceof Error ? e.message : 'unavailable')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const onCopy = useCallback(async () => {
    if (!ip) return
    try {
      await navigator.clipboard.writeText(ip)
      setCopied(true)
      setTimeout(() => setCopied(false), 1600)
    } catch { /* clipboard blocked — the IP is shown for manual copy */ }
  }, [ip])

  return (
    <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
      <div className="flex items-center gap-2 mb-2">
        <span style={{ color: C.mint }}>{ICON.shield(15)}</span>
        <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Broker allowlist IP</span>
        {asOf && <span className="ml-auto text-[10px]" style={{ color: C.faint }}>as of {asOf}</span>}
      </div>

      <div className="flex flex-wrap items-center gap-2.5 mb-2">
        <code className="text-[14px] font-mono rounded-lg px-3 py-1.5"
          style={{ color: ip ? C.ink : C.faint, background: 'rgba(255,255,255,0.03)', border: `1px solid ${C.line2}` }}>
          {loading ? 'Loading…' : (ip ?? '—')}
        </code>
        {ip && (
          <button type="button" onClick={onCopy}
            className="inline-flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-colors"
            style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.3)` }}>
            {copied ? ICON.check(13) : ICON.book(13)} {copied ? 'Copied' : 'Copy'}
          </button>
        )}
        {!loading && !ip && (
          <button type="button" onClick={load}
            className="text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors"
            style={{ color: C.muted, border: `1px solid ${C.line}` }}>
            Retry
          </button>
        )}
      </div>

      <p className="text-[11px] leading-snug" style={{ color: C.muted }}>
        Add this to your broker&apos;s Allowed IPs (developers.kite.trade) so live orders don&apos;t error.
        {!loading && !ip && <span style={{ color: C.faint }}>{' '}The egress-IP endpoint isn&apos;t reporting yet{err ? ` (${err})` : ''} — showing &ldquo;—&rdquo;.</span>}
      </p>
    </div>
  )
}

// ── Read-only saved configs + brokers (load on demand) ───────────────────────
function ReferenceLists() {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [configs, setConfigs] = useState<SavedConfig[] | null>(null)
  const [brokers, setBrokers] = useState<Broker[] | null>(null)

  const load = useCallback(async () => {
    setLoading(true); setErr(null)
    const [c, b] = await Promise.allSettled([AutoTradeAPI.configList(), AutoTradeAPI.brokerList()])
    if (c.status === 'fulfilled') setConfigs(c.value.configs ?? [])
    if (b.status === 'fulfilled') setBrokers(b.value.brokers ?? [])
    if (c.status === 'rejected' && b.status === 'rejected') {
      setErr('Could not load presets or brokers.')
    }
    setLoading(false)
  }, [])

  const toggle = () => {
    const next = !open
    setOpen(next)
    if (next && configs === null && brokers === null && !loading) load()
  }

  return (
    <div className="rounded-2xl border" style={{ borderColor: C.line2, background: C.card2 }}>
      <button type="button" onClick={toggle}
        className="w-full flex items-center gap-2 px-4 py-3 text-left">
        <span style={{ color: C.muted }}>{ICON.book(15)}</span>
        <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Saved presets &amp; brokers</span>
        <span className="ml-auto" style={{ color: C.faint }}>{open ? ICON.chevron(15) : ICON.chevronR(13)}</span>
      </button>

      {open && (
        <div className="px-4 pb-4 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <div className="text-[10px] uppercase tracking-[0.05em] mb-2" style={{ color: C.faint }}>Presets (config/list)</div>
            {loading ? <Empty text="Loading…" />
              : err ? <Empty text={err} />
              : !configs?.length ? <Empty text="No saved presets." />
              : <ul className="flex flex-col gap-1.5">
                  {configs.map((c, i) => (
                    <li key={c.id ?? i} className="rounded-lg border px-3 py-2 text-[12px]"
                      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.ink2 }}>
                      {c.name ?? `Preset ${i + 1}`}
                    </li>
                  ))}
                </ul>}
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-[0.05em] mb-2" style={{ color: C.faint }}>Brokers (broker/list)</div>
            {loading ? <Empty text="Loading…" />
              : !brokers?.length ? <Empty text="No brokers connected." />
              : <ul className="flex flex-col gap-1.5">
                  {brokers.map((b, i) => (
                    <li key={b.id ?? i} className="rounded-lg border px-3 py-2 text-[12px] flex items-center gap-2"
                      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.ink2 }}>
                      <span style={{ color: C.mint }}>{ICON.link(13)}</span>
                      {b.label ?? b.name ?? b.broker ?? `Broker ${i + 1}`}
                      {b.status && <span className="ml-auto text-[10px]" style={{ color: C.faint }}>{b.status}</span>}
                    </li>
                  ))}
                </ul>}
          </div>
        </div>
      )}
    </div>
  )
}

function Empty({ text }: { text: string }) {
  return <p className="text-[11.5px]" style={{ color: C.faint }}>{text}</p>
}
