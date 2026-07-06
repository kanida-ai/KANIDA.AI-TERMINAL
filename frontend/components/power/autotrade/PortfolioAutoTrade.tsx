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
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
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
  type OpenPosition, type PreviewResponse, type PreviewPosition, type KillPreview, type SkippedPick,
  type BrokerAccount, type SessionScope,
  type UniverseFilter, type PickItem, type PicksResponse,
  type LadderProduct, type LadderEndMode, type LadderKillMode, type LadderStatusName,
  type LadderStatus, type LadderSummary,
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
  // Asymmetric kill switch — blank (undefined) = fall back to kill_switch_pct
  // (symmetric, today's behaviour). Captured as PERCENTS; sent ÷100.
  kill_switch_target_pct: undefined,
  kill_switch_stop_pct: undefined,
  entry_time: '09:15',
  // MIS defensive square-off — backend default; only sent for MIS sessions.
  mis_square_off_time: '15:12:00',
  // Leftover-capital redistribution — on by default (backend default true).
  redistribute_unused_capital: true,
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
  order_product: 'MIS',
  // Validated basket-only config (2026-07-04, 530-day backtest + MAE/MFE + param
  // grid): arm 2.5% / floor 1% / giveback 1.5% / stop 3%. Wide giveback rides the
  // +2-3% runner days; -3% basket hard stop; per-stock stop is OFF server-side.
  arm_pct: 2.5,
  floor_pct: 1.0,
  trail_giveback_pct: 1.5,
  stop_pct: 3.0,
  square_off_time: '15:29:00',
  // Hold mode — default INTRADAY (force square-off). Positional = false.
  square_off_enabled: true,
  max_hold_sessions: 0,   // intraday has no multi-session cap
}

// ── Hold-mode TRAIL presets (params only — product/entry/capital untouched) ────
// Switching the Hold toggle re-seeds the validated trail params for that mode:
//   INTRADAY  — arm 2.5 / floor 1 / giveback 1.5 / stop 3, square-off ON, no cap.
//   POSITIONAL — the Falcon Positional strategy (2026-07-04 doc, 519-day NET sim):
//     arm 3 / floor 1 / giveback 4 (wide, for multi-day swings) / stop 6 (wide,
//     for overnight gaps), carry overnight (square-off OFF), 3-session max-hold.
const INTRADAY_TRAIL: Partial<SessionConfig> = {
  arm_pct: 2.5, floor_pct: 1.0, trail_giveback_pct: 1.5, stop_pct: 3.0,
  square_off_enabled: true, max_hold_sessions: 0,
}
const POSITIONAL_TRAIL: Partial<SessionConfig> = {
  arm_pct: 3.0, floor_pct: 1.0, trail_giveback_pct: 4.0, stop_pct: 6.0,
  square_off_enabled: false, max_hold_sessions: 3,
}

// UI-level strategy choices for the create form's dropdown. The first two map
// 1:1 to a backend Strategy enum. 'auto_ladder' is a UI CONSTRUCT ONLY — it is
// NOT a backend strategy: selecting it builds a Falcon Positional Auto-Ladder
// (a monthly campaign) whose children are positional intraday_basket sessions,
// created via the LADDER API (ladderCreate → ladderStart), never session/create.
type UiStrategy = Strategy | 'auto_ladder'

// Protection mode = the exit engine. 'Fixed' is the flat ±% kill switch;
// 'Dynamic (Trailing)' is the arm-and-trail intraday basket; 'Auto-Ladder' is
// the set-once monthly campaign (positional baskets rolled daily by the backend).
const STRATEGY_OPTIONS: { id: UiStrategy; label: string }[] = [
  { id: 'portfolio_kill_switch', label: 'Fixed — flat ±% basket exit (kill switch)' },
  { id: 'intraday_basket',       label: 'Dynamic (Trailing) — arm, lock a floor & trail, square-off' },
  { id: 'auto_ladder',           label: 'Falcon Positional — Auto-Ladder (monthly campaign)' },
]

// Campaign Duration options (Auto-Ladder only). Maps to LadderEndMode on the wire.
const LADDER_DURATION_OPTIONS: { id: LadderEndMode; label: string; hint: string }[] = [
  { id: 'month_end', label: 'This month (auto)', hint: 'Runs to the end of this month, then stops' },
  { id: 'manual',    label: 'Until I stop',      hint: 'Runs every trading day until you stop it' },
]

// Kill modes for a running campaign (the confirm modal REQUIRES one — no default).
const LADDER_KILL_OPTIONS: { id: LadderKillMode; label: string; line: string }[] = [
  { id: 'flatten_now',        label: 'Flatten everything now',                    line: 'Exit every open basket immediately and stop the campaign.' },
  { id: 'stop_new_let_finish', label: 'Stop opening new — let open baskets finish', line: 'Open no new baskets; let the already-open ones finish their normal exits.' },
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
// D · Hold mode for the Dynamic (Trailing) basket. Intraday forces a square-off
// at square_off_time; Positional carries the floor + hard stop across days.
const HOLD_OPTIONS: { id: 'intraday' | 'positional'; label: string }[] = [
  { id: 'intraday',   label: 'Intraday'   },
  { id: 'positional', label: 'Positional' },
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

// Tomorrow's date (IST) as "YYYY-MM-DD" — the min for the campaign Schedule date
// picker (a campaign can only be scheduled for a FUTURE trading day; the backend
// validates the actual trading-day rule and 400s a weekend/holiday).
// Earliest date a campaign may be scheduled for: TODAY if the 09:15 IST open
// hasn't passed yet (so a pre-market "schedule for today" is allowed), else
// tomorrow. The backend still validates the trading-day + before-open rule.
function earliestScheduleDateIST(): string {
  const istMs = Date.now() + (5 * 60 + 30) * 60_000  // UTC epoch → IST wall-clock
  const ist = new Date(istMs)
  const beforeOpen =
    ist.getUTCHours() < 9 || (ist.getUTCHours() === 9 && ist.getUTCMinutes() < 15)
  const base = beforeOpen ? istMs : istMs + 24 * 60 * 60_000
  return new Date(base).toISOString().slice(0, 10)
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
  isAdmin = true,
  view,
  onStarted,
  onNewCampaign,
  onNeedBroker,
}: {
  userId?: number | string
  // Called whenever the user focuses a session (resume or create).
  // Passes the session_id up so sibling tabs (Journal) can use it.
  onSessionChange?: (sessionId: string) => void
  // isAdmin (DEFAULT true — the operator mount is unchanged). When false (a power
  // user), the broker-account selector NEVER offers the global/operator account
  // (the backend isolation guard refuses it), defaults to their single connected
  // account when they have exactly one, and a LIVE start REQUIRES a selected
  // ACTIVE account (paper may proceed without one). Everything else is identical.
  isAdmin?: boolean
  // view (DEFAULT undefined — the operator/admin mount is 100% UNCHANGED, combined
  // list + inline create). When the 4-tab power-user shell splits this component in
  // two:
  //   'create'   — mount straight into the config form (phase='config'); the
  //                sessions/campaigns list is NEVER shown. A successful start/schedule
  //                calls onStarted() so the shell jumps to the Live tab. Picking LIVE
  //                without a connected broker routes to onNeedBroker().
  //   'dashboard' — mount into the list only (phase='list'); the inline config form
  //                is NEVER shown. The "New session" button calls onNewCampaign()
  //                (shell → Start tab) instead of opening the inline form.
  view?: 'create' | 'dashboard'
  // Called after a session/campaign is successfully created AND started/scheduled
  // (view='create' only). The shell uses it to jump to the Live tab.
  onStarted?: () => void
  // Called by the list's "New session" button (view='dashboard' only). The shell
  // uses it to jump to the Start tab.
  onNewCampaign?: () => void
  // Called when a power user picks LIVE with no connected/active broker account
  // (view='create' only). The shell uses it to jump to the Broker tab.
  onNeedBroker?: () => void
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

  // view='create' mounts straight into the config form; every other mount (admin
  // combined view, or view='dashboard') starts on the sessions list. Admin
  // (view=undefined) is unchanged: 'list'.
  const [phase, setPhase] = useState<Phase>(view === 'create' ? 'config' : 'list')
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

  // UI-level strategy for the create form (see UiStrategy). 'auto_ladder' drives
  // the campaign create path; the other two map 1:1 to config.strategy.
  const [uiStrategy, setUiStrategy] = useState<UiStrategy>('portfolio_kill_switch')
  const isLadder = uiStrategy === 'auto_ladder'

  // Campaign Duration (Auto-Ladder only) → LadderEndMode on the wire.
  const [ladderEndMode, setLadderEndMode] = useState<LadderEndMode>('month_end')

  // ── Running campaigns (Auto-Ladder) — shown ATOP the sessions list ───────────
  // ladders = the user's campaigns; ladderStatuses = the live per-campaign poll
  // keyed by ladder_id (~5s). Both degrade to "—" on missing fields, never crash.
  const [ladders, setLadders] = useState<LadderSummary[] | null>(null)
  const [ladderStatuses, setLadderStatuses] = useState<Record<string, LadderStatus>>({})
  const [ladderBusy, setLadderBusy] = useState<Record<string, 'pause' | 'resume' | 'kill'>>({})
  const [ladderErr, setLadderErr] = useState<string | null>(null)
  // Confirmation banner shown after a campaign is created + started (parity with
  // the session "scheduled" confirmation — otherwise the user gets no feedback).
  const [ladderNotice, setLadderNotice] = useState<string | null>(null)
  // Kill modal — mode is REQUIRED (starts unset so the trader must choose).
  const [killLadderId, setKillLadderId] = useState<string | null>(null)
  const [ladderKillMode, setLadderKillMode] = useState<LadderKillMode | null>(null)
  const ladderPollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── Auto-Ladder create→start two-step (mirrors the session created-phase) ────
  // createdLadder holds the just-created CREATED (draft) campaign, awaiting a
  // Start-now / Schedule choice. It is rendered by the campaign CREATED phase
  // (phase==='created' && createdLadder && !session), parallel to the session
  // created-phase (phase==='created' && session).
  const [createdLadder, setCreatedLadder] = useState<{ ladder_id: string; mode: Mode; product: LadderProduct } | null>(null)
  // campaignDate — the picked future start day for Schedule (YYYY-MM-DD; '' = none).
  const [campaignDate, setCampaignDate] = useState<string>('')
  // busy flag for the campaign Start/Schedule action (parallel to `busy`).
  const [campaignBusy, setCampaignBusy] = useState<null | 'now' | 'scheduled'>(null)
  const [campaignErr, setCampaignErr] = useState<string | null>(null)
  // A non-trading-day 400 on Schedule → the backend's suggested_date, offered as a
  // one-click apply (mirrors the session createSuggest pattern).
  const [campaignSuggest, setCampaignSuggest] = useState<string | null>(null)
  // Duplicate-campaign soft guard: holds the product:capital:mode signature the
  // user was just warned about, so a second "Create campaign" click proceeds.
  const [dupConfirm, setDupConfirm] = useState<string | null>(null)

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

  // Power-user (isAdmin === false): the global/operator account is NOT an option
  // (the backend isolation guard refuses it). Default-select their connected
  // account when they have exactly one so the selector is never on the empty
  // global default. Only runs while no account is chosen yet.
  useEffect(() => {
    if (isAdmin) return
    if (brokerAccountId) return
    if (accounts.length === 1) setBrokerAccountId(accounts[0].broker_account_id)
  }, [isAdmin, accounts, brokerAccountId])

  // Switch strategy (UI level). For intraday_basket, SEED the validated preset.
  // For auto_ladder (UI construct), map to a POSITIONAL intraday_basket under the
  // hood — CNC product, the positional trail preset (arm3/floor1/give4/stop6),
  // carry-overnight (square_off_enabled:false) + 3-session hold — so the reused
  // preview/wire path produces a correct one-basket estimate. For
  // portfolio_kill_switch, restore the safe kill-switch defaults (byte-for-byte).
  const onStrategyChange = (next: UiStrategy) => {
    setUiStrategy(next)
    if (next === 'auto_ladder') {
      setConfig((c) => ({
        ...c,
        strategy: 'intraday_basket',
        ...POSITIONAL_TRAIL,                 // arm3/floor1/give4/stop6, off, hold 3
        order_product: c.order_product === 'MTF' ? 'MTF' : 'CNC', // CNC|MTF only
        instrument_type: 'EQ',
        direction: 'long',
        entry_time: INTRADAY_PRESET.entry_time ?? '09:15:00',
        kill_switch_enabled: false,
      }))
      return
    }
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
        kill_switch_target_pct: DEFAULT_CONFIG.kill_switch_target_pct,
        kill_switch_stop_pct: DEFAULT_CONFIG.kill_switch_stop_pct,
      }
    })
  }

  // Switch Hold mode (Intraday ↔ Positional). Re-seeds that mode's validated
  // trail preset (arm/floor/giveback/stop + square-off + max-hold) — product,
  // capital and entry are untouched. The operator can still edit every field.
  const onHoldChange = (hold: 'intraday' | 'positional') => {
    setConfig((c) => ({ ...c, ...(hold === 'positional' ? POSITIONAL_TRAIL : INTRADAY_TRAIL) }))
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
    // C · leftover-capital redistribution — always sent (default true). A boolean
    // is cheap and unambiguous; the backend defaults to true when absent anyway.
    const capitalExtra: Partial<SessionConfig> = {
      redistribute_unused_capital: c.redistribute_unused_capital !== false,
    }
    // A · MIS defensive square-off — only meaningful for MIS equity sessions; send
    // it only then so non-MIS sessions carry no spurious field.
    const misExtra: Partial<SessionConfig> = {}
    if (c.order_product === 'MIS' && c.mis_square_off_time) {
      misExtra.mis_square_off_time = c.mis_square_off_time
    }
    if (c.strategy === 'intraday_basket') {
      return {
        ...c,
        ...exec,
        ...universeExtra,
        ...capitalExtra,
        ...misExtra,
        arm_pct: (Number(c.arm_pct) || 0) / 100,
        floor_pct: (Number(c.floor_pct) || 0) / 100,
        trail_giveback_pct: (Number(c.trail_giveback_pct) || 0) / 100,
        stop_pct: (Number(c.stop_pct) || 0) / 100,
        // D · Hold mode — INTRADAY (true, default) forces a square-off; POSITIONAL
        // (false) carries the floor + hard stop across days. MIS cannot be
        // positional (backend rejects it), so force INTRADAY for MIS.
        square_off_enabled: c.order_product === 'MIS' ? true : (c.square_off_enabled !== false),
        // D · positional max-hold cap — sent only for a positional (carry-overnight)
        // session; MIS + intraday never carry a cap. Int, 0 = no cap.
        max_hold_sessions: (c.order_product !== 'MIS' && c.square_off_enabled === false)
          ? Math.max(0, Math.floor(Number(c.max_hold_sessions) || 0)) : 0,
        // intraday_basket exits via the trail, not the flat kill switch
        kill_switch_enabled: false,
      }
    }
    // B · asymmetric kill switch — send the per-side thresholds as FRACTIONS ONLY
    // when the operator set them (a positive number); leave them omitted otherwise
    // so the backend falls back to the single symmetric kill_switch_pct.
    const killExtra: Partial<SessionConfig> = {}
    const tgt = Number(c.kill_switch_target_pct)
    const stp = Number(c.kill_switch_stop_pct)
    if (c.kill_switch_target_pct != null && Number.isFinite(tgt) && tgt > 0) {
      killExtra.kill_switch_target_pct = tgt / 100
    }
    if (c.kill_switch_stop_pct != null && Number.isFinite(stp) && stp > 0) {
      killExtra.kill_switch_stop_pct = stp / 100
    }
    return {
      ...c,
      ...exec,
      ...universeExtra,
      ...capitalExtra,
      ...misExtra,
      ...killExtra,
      kill_switch_pct: (Number(c.kill_switch_pct) || 0) / 100,
    }
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

  // ── Power-user LIVE requires their OWN active broker account ───────────────────
  // A power user (isAdmin === false) may NEVER be silently pointed at the operator
  // global account, and a LIVE start needs a selected ACTIVE account (paper may
  // proceed without one). accountActive = a chosen, non-EXPIRED connected account.
  // liveAccountReady gates the LIVE Create/Start; for an operator (isAdmin) it is
  // always true (the global account is a valid live target), preserving behaviour.
  const accountActive = selectedAccount != null && !accountExpired
  const liveAccountReady = isAdmin || mode === 'paper' || accountActive
  // Combined LIVE readiness for the create/start buttons: the typed-LIVE confirm
  // AND (for a power user) a selected active account.
  const canGoLive = liveReady && liveAccountReady

  // Auto-Ladder splits the total campaign capital across ~3 baskets. The sizing
  // preview must reflect what ONE day's basket buys, so we preview on total ÷ 3.
  const perBasketCapital = isLadder
    ? Math.max(0, Math.floor((config.total_allocated_capital || 0) / 3))
    : config.total_allocated_capital

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
        // Auto-Ladder previews the PER-BASKET slice (total ÷ 3) so the trader
        // sees exactly what one day's basket buys; other strategies preview the
        // full capital, unchanged.
        const previewConfig = isLadder
          ? { ...config, total_allocated_capital: perBasketCapital }
          : config
        const res = await AutoTradeAPI.preview(
          toWireConfig(previewConfig, { universeFilter, symbolWhitelist }),
          scope,
        )
        if (!cancelled) { setPreview(res); setPreviewErr(null) }
      } catch (e) {
        // Keep the LAST-GOOD preview visible on a transient error (don't blank the
        // sizing table) — just surface the error. Only a strategy switch (above)
        // clears it. This fixes the "table sometimes disappears" flicker.
        if (!cancelled) setPreviewErr(e instanceof Error ? e.message : 'Could not estimate the outcome.')
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
    config.kill_switch_target_pct,
    config.kill_switch_stop_pct,
    config.max_pct_per_position,
    config.arm_pct,
    config.floor_pct,
    config.trail_giveback_pct,
    config.stop_pct,
    config.square_off_time,
    config.square_off_enabled,
    config.redistribute_unused_capital,
    config.mis_square_off_time,
    universeFilter,
    symbolWhitelist,
    userId,
    brokerAccountId,
    toWireConfig,
    isLadder,
    perBasketCapital,
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

  // ── Running campaigns (Auto-Ladder) — a summary card ATOP the sessions list ──
  // Load the user's campaigns; best-effort (a missing endpoint leaves the section
  // absent, never blocking the sessions list). Only fetched with a user context.
  const loadLadders = useCallback(async () => {
    if (userId == null) { setLadders([]); return }
    setLadderErr(null)
    try {
      const res = await AutoTradeAPI.ladders(userId)
      const list = (res.ladders ?? []).slice().sort((a, b) => {
        const ta = a.created_at ? Date.parse(a.created_at) : 0
        const tb = b.created_at ? Date.parse(b.created_at) : 0
        return tb - ta
      })
      setLadders(list)
    } catch (e) {
      // Calm degrade — surface a retry note; never crash the sessions list.
      setLadders([])
      setLadderErr(e instanceof Error ? e.message : 'Could not load your campaigns.')
    }
  }, [userId])

  useEffect(() => { loadLadders() }, [loadLadders])

  // The LIVE campaigns we render atop the list: RUNNING / PAUSED and now also
  // SCHEDULED (armed for a future start_date). CREATED drafts are deliberately
  // excluded — a draft lives only in the transient created-phase card until the
  // trader Starts or Schedules it.
  const liveLadders = (ladders ?? []).filter((l) => {
    const s = (l.status ?? '').toUpperCase()
    return s === 'RUNNING' || s === 'PAUSED' || s === 'SCHEDULED'
  })
  // Finished campaigns (COMPLETED / ENDED) — terminal history. Shown compactly so
  // the trader can permanently DELETE them (they no longer open baskets).
  const finishedLadders = (ladders ?? []).filter((l) => {
    const s = (l.status ?? '').toUpperCase()
    return s === 'COMPLETED' || s === 'ENDED'
  })
  const liveLadderIds = liveLadders.map((l) => l.ladder_id).join(',')

  // Poll each live campaign's status (~5s). Keeps last-good numbers on a transient
  // error (calm retry, mirroring the sessions list). Depends on the id set only.
  useEffect(() => {
    if (ladderPollRef.current) { clearInterval(ladderPollRef.current); ladderPollRef.current = null }
    const ids = liveLadderIds ? liveLadderIds.split(',').filter(Boolean) : []
    if (ids.length === 0) return
    let cancelled = false
    const tick = async () => {
      await Promise.all(ids.map(async (id) => {
        try {
          const res = await AutoTradeAPI.ladderStatus(id)
          if (!cancelled) setLadderStatuses((prev) => ({ ...prev, [id]: res }))
        } catch { /* keep last-good; next tick retries */ }
      }))
    }
    tick()
    ladderPollRef.current = setInterval(tick, 5_000)
    return () => { cancelled = true; if (ladderPollRef.current) clearInterval(ladderPollRef.current) }
  }, [liveLadderIds])

  // ── Campaign controls (pause / resume / kill) ────────────────────────────────
  const onLadderPause = useCallback(async (id: string) => {
    setLadderErr(null); setLadderBusy((b) => ({ ...b, [id]: 'pause' }))
    try {
      await AutoTradeAPI.ladderPause(id)
      const res = await AutoTradeAPI.ladderStatus(id)
      setLadderStatuses((prev) => ({ ...prev, [id]: res }))
      loadLadders()
    } catch (e) {
      setLadderErr(e instanceof Error ? e.message : 'Could not pause the campaign.')
    } finally {
      setLadderBusy((b) => { const n = { ...b }; delete n[id]; return n })
    }
  }, [loadLadders])

  const onLadderResume = useCallback(async (id: string) => {
    setLadderErr(null); setLadderBusy((b) => ({ ...b, [id]: 'resume' }))
    try {
      await AutoTradeAPI.ladderResume(id)
      const res = await AutoTradeAPI.ladderStatus(id)
      setLadderStatuses((prev) => ({ ...prev, [id]: res }))
      loadLadders()
    } catch (e) {
      setLadderErr(e instanceof Error ? e.message : 'Could not resume the campaign.')
    } finally {
      setLadderBusy((b) => { const n = { ...b }; delete n[id]; return n })
    }
  }, [loadLadders])

  const onLadderKill = useCallback(async (modeArg?: LadderKillMode) => {
    // modeArg lets a SCHEDULED-campaign cancel skip the wind-down question and
    // pass a mode directly (nothing is open, so flatten_now just marks it done).
    const mode = modeArg ?? ladderKillMode
    if (!killLadderId || !mode) return
    const id = killLadderId
    setLadderErr(null); setLadderBusy((b) => ({ ...b, [id]: 'kill' }))
    try {
      await AutoTradeAPI.ladderKill(id, mode)
      setKillLadderId(null); setLadderKillMode(null)
      const res = await AutoTradeAPI.ladderStatus(id).catch(() => null)
      if (res) setLadderStatuses((prev) => ({ ...prev, [id]: res }))
      loadLadders()
    } catch (e) {
      setLadderErr(e instanceof Error ? e.message : 'Could not stop the campaign.')
    } finally {
      setLadderBusy((b) => { const n = { ...b }; delete n[id]; return n })
    }
  }, [killLadderId, ladderKillMode, loadLadders])

  // Permanently delete a FINISHED campaign (COMPLETED/ENDED) — removes the row.
  const onLadderDelete = useCallback(async (id: string) => {
    setLadderErr(null); setLadderBusy((b) => ({ ...b, [id]: 'kill' }))
    try {
      await AutoTradeAPI.ladderDelete(id)
      loadLadders()
    } catch (e) {
      setLadderErr(e instanceof Error ? e.message : 'Could not delete the campaign.')
    } finally {
      setLadderBusy((b) => { const n = { ...b }; delete n[id]; return n })
    }
  }, [loadLadders])

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
  // Auto-Ladder — STEP 1: create the campaign only (ladderCreate → CREATED draft;
  // spawns nothing). This MIRRORS the session two-step: create here, then the
  // campaign CREATED phase offers Start-now / Schedule. The created draft is held
  // in `createdLadder` and rendered by phase==='created' && createdLadder.
  const onCreateCampaign = useCallback(async () => {
    setError(null); setCampaignErr(null); setCampaignSuggest(null)
    const product: LadderProduct = config.order_product === 'MTF' ? 'MTF' : 'CNC'
    const cap = config.total_allocated_capital || 0
    // DUPLICATE GUARD: don't silently create an identical active campaign. Warn
    // once (the button re-arms); a second click creates it anyway for the rare
    // case a duplicate is genuinely intended.
    const sig = `${product}:${cap}:${mode}`
    const isDup = (ladders ?? []).some((l) => {
      const s = (l.status ?? '').toUpperCase()
      return (s === 'RUNNING' || s === 'PAUSED' || s === 'SCHEDULED')
        && l.order_product === product
        && Number(l.total_capital) === Number(cap)
        && (l.mode ?? 'paper') === mode
    })
    if (isDup && dupConfirm !== sig) {
      setDupConfirm(sig)
      setError(`You already have an active ${product} ${fmtINR(cap)} campaign. Click “Create campaign” again to create another anyway.`)
      return
    }
    setDupConfirm(null); setBusy('create')
    try {
      const created = await AutoTradeAPI.ladderCreate({
        total_capital: cap,
        order_product: product,
        mode,
        end_date_mode: ladderEndMode,
        kill_mode: 'flatten_now',
        // PARITY FIX: scope the campaign to this user exactly like createSession,
        // so it lands with the same user_id and appears in the ?user_id-scoped
        // Sessions list (a SCHEDULED campaign was being dropped by WHERE user_id=?).
        ...(userId != null ? { user_id: userId } : {}),
        ...(brokerAccountId ? { broker_account_id: brokerAccountId } : {}),
      })
      // Hold the draft, clear the picked date, and move to the campaign CREATED
      // phase (Start-now / Schedule). Nothing has been placed or armed yet.
      setCreatedLadder({ ladder_id: created.ladder_id, mode, product })
      setCampaignDate('')
      setSession(null)   // ensure the SESSION created-phase doesn't also render
      setPhase('created')
      loadLadders()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not create the campaign.')
    } finally {
      setBusy(null)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, config.total_allocated_capital, config.order_product, ladderEndMode, loadLadders, ladders, dupConfirm, userId, brokerAccountId])

  // Auto-Ladder — STEP 2: start the held draft. `when==='now'` → ladderStart(id)
  // → RUNNING (first basket next trading morning). `when==='scheduled'` →
  // ladderStart(id, startDate) → SCHEDULED (auto-activates on that date). A
  // non-trading-day 400 surfaces the backend's suggested_date as a one-click apply.
  const onCampaignStart = useCallback(async (when: StartWhen) => {
    if (!createdLadder) return
    if (when === 'scheduled' && !campaignDate) {
      setCampaignErr('Pick a start date first.')
      return
    }
    setCampaignErr(null); setCampaignSuggest(null); setCampaignBusy(when)
    try {
      if (when === 'now') {
        await AutoTradeAPI.ladderStart(createdLadder.ladder_id)
        setLadderNotice(
          'Campaign started ✓ — it opens its first Falcon Top-5 basket at 09:15 on the '
          + 'next trading day, then automatically ladders a new basket every trading day. '
          + "It's shown below and runs on its own until it ends or you stop it.")
      } else {
        await AutoTradeAPI.ladderStart(createdLadder.ladder_id, campaignDate)
        setLadderNotice(
          `Campaign scheduled ✓ — it arms for ${campaignDate} and opens its first `
          + 'Falcon Top-5 basket at 09:15 that morning, then ladders a new basket every '
          + "trading day. It's shown below until it activates.")
      }
      setCreatedLadder(null)
      setCampaignDate('')
      backToList()
      loadLadders()
      // Shell (view='create'): campaign started/scheduled — hand off to the Live tab.
      onStarted?.()
    } catch (e) {
      // A weekend/holiday start → HTTP 400 whose detail is an object carrying a
      // suggested_date. The shared call<>() helper attached it to the Error.
      const suggested = (e as { suggested_date?: string })?.suggested_date
      if (suggested) setCampaignSuggest(suggested)
      setCampaignErr(e instanceof Error ? e.message : 'Could not start the campaign.')
    } finally {
      setCampaignBusy(null)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [createdLadder, campaignDate, loadLadders, onStarted])

  // Discard a draft campaign — PERMANENTLY delete it (it was never started), not
  // just navigate away leaving an orphan CREATED row in the backend.
  const onDiscardCampaign = useCallback(async () => {
    const id = createdLadder?.ladder_id
    if (id) await AutoTradeAPI.ladderDelete(id).catch(() => { /* best-effort */ })
    backToList()
    loadLadders()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [createdLadder, loadLadders])

  const onCreate = useCallback(async () => {
    // Power-user LIVE guard: a live session must run on THEIR OWN active account —
    // never the operator global default (the backend isolation guard refuses it).
    if (!isAdmin && mode === 'live' && !accountActive) {
      // Shell (view='create'): no connected/active account — send them to the
      // Broker tab to connect one, rather than dead-ending on an inline error.
      if (view === 'create' && onNeedBroker) {
        setError('Connect your broker first — live needs your own active account. Taking you there…')
        onNeedBroker()
        return
      }
      setError('Select your connected broker account — live needs your own active account. (Paper is fine without one.)')
      return
    }
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
  }, [mode, config, loadSessions, toWireConfig, userId, brokerAccountId, onSessionChange, universeFilter, symbolWhitelist, isAdmin, accountActive, view, onNeedBroker])

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
      // Shell (view='create'): the session is started/scheduled — hand off to the
      // Live tab. Passing the id keeps the shell's active-session focus in sync.
      onStarted?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start session')
    } finally {
      setBusy(null)
    }
  }, [session, accountExpired, onStarted])

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
    // Clear any in-flight campaign draft (Auto-Ladder two-step).
    setCreatedLadder(null); setCampaignDate(''); setCampaignErr(null); setCampaignSuggest(null); setDupConfirm(null)
    // Reset the strategy dropdown + campaign duration to defaults.
    setUiStrategy('portfolio_kill_switch'); setLadderEndMode('month_end')
    // Reset universe filter + picker
    setUniverseFilter('all500'); setPicks(null); setPicksErr(null); setCheckedSymbols(null)
  }

  // "New session" click router. In the split shell's DASHBOARD view the inline
  // config form is never shown here — hand off to the Start tab via onNewCampaign.
  // Admin (view=undefined) and the create view open the inline form as before.
  const onNewSessionClick = () => {
    if (view === 'dashboard' && onNewCampaign) { onNewCampaign(); return }
    openNewSession()
  }

  // Return to the Your-Sessions list and refresh it (so a just-created/started
  // session is visible — the disappearing-session fix).
  //
  // Shell (view='create'): this component never renders the list — going "back"
  // resets to a fresh config form (openNewSession) instead of a phase='list' that
  // would render nothing. Admin + dashboard views return to the list as before.
  const backToList = () => {
    if (view === 'create') { openNewSession(); return }
    setPhase('list'); setSession(null); setStartResult(null); setStatus(null)
    setCountdown(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm(''); setError(null)
    setCreateSuggest(null)
    // Clear any in-flight campaign draft (Auto-Ladder two-step).
    setCreatedLadder(null); setCampaignDate(''); setCampaignErr(null); setCampaignSuggest(null); setDupConfirm(null)
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
              <button type="button" onClick={onNewSessionClick}
                className="flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-1.5 rounded-lg transition-opacity"
                style={{ color: '#06130c', background: C.mint }}>
                {ICON.bolt(13)} New session
              </button>
            </div>
          </div>

          {/* Campaign-created confirmation (parity with the session scheduled note). */}
          {ladderNotice && (
            <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[12px] leading-snug"
              style={{ borderColor: 'rgba(63,227,164,0.4)', background: 'rgba(63,227,164,0.07)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.check(14)}</span>
              <span className="flex-1">{ladderNotice}</span>
              <button type="button" onClick={() => setLadderNotice(null)} className="shrink-0" style={{ color: C.faint }}>
                {ICON.close(13)}
              </button>
            </div>
          )}

          {/* ── Running Auto-Ladder campaigns — summary cards ATOP the list ─────
              One card per running/paused campaign, from the ~5s ladderStatus
              poll. The campaign's child baskets appear below as ordinary session
              rows tagged with a "Campaign" chip. */}
          {ladderErr && (ladders?.length ?? 0) === 0 && (
            <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
              style={{ borderColor: 'rgba(230,180,80,0.35)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
              <span>{ladderErr}{' '}
                <button type="button" onClick={loadLadders} className="underline" style={{ color: C.mint }}>Retry</button>
              </span>
            </div>
          )}
          {liveLadders.length > 0 && (
            <div className="flex items-baseline gap-2 px-1">
              <span className="text-[11px] font-semibold uppercase tracking-[0.06em]" style={{ color: C.muted }}>Monthly campaigns</span>
              <span className="text-[10.5px]" style={{ color: C.faint }}>set once · auto-ladders a basket every trading day</span>
            </div>
          )}
          {liveLadders.map((l) => (
            <LadderCampaignCard
              key={l.ladder_id}
              summary={l}
              status={ladderStatuses[l.ladder_id]}
              busy={ladderBusy[l.ladder_id]}
              onPause={() => onLadderPause(l.ladder_id)}
              onResume={() => onLadderResume(l.ladder_id)}
              onKill={() => { setLadderKillMode(null); setKillLadderId(l.ladder_id) }}
            />
          ))}

          {finishedLadders.length > 0 && (
            <div className="flex flex-col gap-1.5">
              <span className="text-[11px] font-semibold uppercase tracking-[0.06em] px-1" style={{ color: C.muted }}>Finished campaigns</span>
              {finishedLadders.map((l) => (
                <div key={l.ladder_id} className="flex items-center gap-2 rounded-xl border px-3 py-2"
                  style={{ borderColor: C.line, background: 'rgba(255,255,255,0.012)' }}>
                  <span style={{ color: C.faint }}>{ICON.loop(13)}</span>
                  <span className="text-[11.5px]" style={{ color: C.ink2 }}>Monthly campaign</span>
                  <span className="text-[9px] font-mono rounded-full px-1.5 py-0.5"
                    style={{ color: C.muted, background: 'rgba(255,255,255,0.05)' }}>#{String(l.ladder_id).slice(0, 6)}</span>
                  <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
                    style={{ color: C.faint, background: 'rgba(133,153,144,0.13)', boxShadow: 'inset 0 0 0 1px rgba(133,153,144,0.4)' }}>
                    {(l.status ?? '').toUpperCase()}
                  </span>
                  <button type="button" disabled={ladderBusy[l.ladder_id] != null}
                    onClick={() => onLadderDelete(l.ladder_id)}
                    className="ml-auto flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[11.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: C.red, background: 'rgba(232,115,107,0.10)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.35)' }}>
                    {ICON.close(12)} {ladderBusy[l.ladder_id] === 'kill' ? 'Deleting…' : 'Delete'}
                  </button>
                </div>
              ))}
            </div>
          )}

          {(liveLadders.length > 0 || finishedLadders.length > 0) && (
            <div className="flex items-baseline gap-2 px-1 pt-1">
              <span className="text-[11px] font-semibold uppercase tracking-[0.06em]" style={{ color: C.muted }}>Individual sessions</span>
              <span className="text-[10.5px]" style={{ color: C.faint }}>one-off sessions + each campaign’s daily baskets</span>
            </div>
          )}
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
                  // A child basket of an Auto-Ladder campaign carries ladder_id —
                  // tag it with a "Campaign" chip so it's clearly part of a ladder.
                  const isCampaignChild = s.ladder_id != null && s.ladder_id !== ''
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
                              {isCampaignChild && <CampaignChip />}
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
          {/* In the split shell's create view the tab bar is the nav — there is no
              in-component list to go back to, so this button is hidden there. */}
          {view !== 'create' && (
            <button type="button" onClick={backToList}
              className="mb-4 inline-flex items-center gap-1.5 text-[12px] px-3 py-1.5 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              ← Your sessions
            </button>
          )}

          {/* Strategy selector — picks the exit engine. Intraday basket seeds its
              validated preset; Auto-Ladder builds a monthly positional campaign
              (the same form, restricted to the campaign deltas); the kill switch
              is the default flat ±% exit. */}
          <div className="mb-5">
            <Field
              label="Strategy"
              hint={isLadder
                ? 'Set it once — Falcon opens, manages and rolls a positional basket every trading day for the whole campaign. You never manage a basket.'
                : config.strategy === 'intraday_basket'
                ? 'Arms a trailing exit once the basket profits, locks a floor, trails a giveback %, hard-stops, and squares off at a set time.'
                : 'A single flat ±% basket exit on the invested return (the kill switch).'}
            >
              <select
                value={uiStrategy}
                onChange={(e) => onStrategyChange(e.target.value as UiStrategy)}
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

            {/* Auto-Ladder intro strip — "set once, runs the month" (trader terms). */}
            {isLadder && (
              <div className="mt-3 flex items-start gap-2.5 rounded-xl border px-3.5 py-3"
                style={{ borderColor: 'rgba(63,227,164,0.22)', background: 'linear-gradient(180deg, rgba(63,227,164,0.06), rgba(255,255,255,0.015))' }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.loop(16)}</span>
                <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                  <b style={{ color: C.ink }}>Monthly campaign.</b>{' '}
                  Falcon splits your capital across ~3 baskets and opens a new one every trading day
                  at <b style={{ color: C.ink }}>09:15 IST</b> — each basket is held positional (a fixed
                  3-session hold), and you never manage one.
                </div>
              </div>
            )}
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
                      ? 'This account is EXPIRED — re-connect it (Broker accounts) before a LIVE start. Paper is fine.'
                      : 'This session will run on your selected connected account.')
                  : (isAdmin
                      ? 'Default — the global operator account. Pick a connected account to run this session on it.'
                      : (accounts.length === 0
                          ? 'Connect a broker account above to run a live session on your own account.'
                          : 'Select your connected account — live needs your own account.'))}
              >
                <select
                  value={brokerAccountId}
                  onChange={(e) => setBrokerAccountId(e.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                >
                  {/* Operator: the global/operator account is a valid default.
                      Power user: the global account is NEVER offered (backend
                      isolation refuses it) — only their own connected accounts.
                      A power user with no account sees a disabled placeholder. */}
                  {isAdmin ? (
                    <option value="" style={{ background: '#0b1410', color: C.ink }}>
                      Global account (operator default)
                    </option>
                  ) : (
                    <option value="" disabled={accounts.length > 0} style={{ background: '#0b1410', color: C.ink }}>
                      {accounts.length === 0 ? 'No connected account — connect one above' : 'Select your account…'}
                    </option>
                  )}
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
            <Field
              label={isLadder ? 'Total campaign capital' : 'Allocated capital'}
              hint={isLadder
                ? `Falcon splits this across ~3 baskets. Each basket ≈ ${fmtINR(perBasketCapital)}`
                : 'Total capital this session may deploy.'}
            >
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
                NRML set server-side). Hidden for Auto-Ladder (always Equity). */}
            {!isLadder && (
            <Field label="Instrument" hint="Equity trades cash; Futures trades the current-month contract (product set automatically).">
              <Segmented
                options={INSTRUMENT_OPTIONS.map((o) => ({ id: o.id, label: o.label }))}
                value={config.instrument_type ?? 'EQ'}
                onChange={(v) => onInstrumentChange(v)}
              />
            </Field>
            )}

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
              <Field label="Order product" hint={isLadder ? 'Campaign product — Cash (CNC) or Margin (MTF).' : 'Broker product type for entries.'}>
                <Segmented
                  options={(isLadder ? (['CNC', 'MTF'] as OrderProduct[]) : PRODUCT_OPTIONS).map((p) => ({ id: p, label: p }))}
                  value={config.order_product === 'MIS' && isLadder ? 'CNC' : config.order_product}
                  onChange={(v) => set('order_product', v)}
                />
              </Field>
            ) : null}

            {/* Futures: hide the equity product row; instead pick Buy / Sell (Short)
                and show the auto-product note. Product = NRML is set server-side.
                Never shown for Auto-Ladder (equity-only). */}
            {!isLadder && (config.instrument_type ?? 'EQ') === 'FUT' && (
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

            {/* Entry time / date / missed-window are managed by the campaign
                engine for Auto-Ladder — hidden there; a Duration control takes
                their place. Shown unchanged for the two session strategies. */}
            {!isLadder && (
            <Field label="Entry time (IST)" hint="When the session places entries.">
              <input
                type="time"
                value={(config.entry_time ?? '').slice(0, 5)}
                onChange={(e) => set('entry_time', e.target.value)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              />
            </Field>
            )}

            {/* Execution date — optional. Empty = the backend resolves to the
                next valid trading session. Clearing it is one tap (the chip). */}
            {!isLadder && (
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
            )}

            {/* On missed window — drop or roll forward. */}
            {!isLadder && (
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
            )}

            {/* Duration (Auto-Ladder only) — when the campaign ends. */}
            {isLadder && (
            <Field label="Duration" hint={LADDER_DURATION_OPTIONS.find((o) => o.id === ladderEndMode)?.hint}>
              <div className="inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
                {LADDER_DURATION_OPTIONS.map((o) => {
                  const active = o.id === ladderEndMode
                  return (
                    <button key={o.id} type="button" onClick={() => setLadderEndMode(o.id)}
                      className="px-3 py-1.5 rounded-lg text-[12px] font-medium transition-colors"
                      style={{ color: active ? '#06130c' : C.ink2, background: active ? C.mint : 'transparent' }}>
                      {o.label}
                    </button>
                  )
                })}
              </div>
            </Field>
            )}
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

            {/* ── C · Use leftover capital ── redistribute the capital freed by any
                skipped pick (1 unit > its slice) across the remaining picks. On by
                default; toggling off leaves that capital idle. */}
            <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border px-3 py-2.5"
              style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
              <div className="min-w-0">
                <div className="text-[12px] font-semibold" style={{ color: C.ink }}>Use leftover capital</div>
                <div className="text-[10.5px] leading-snug mt-0.5" style={{ color: C.faint }}>
                  Redistribute capital freed by skipped picks across the remaining names.
                </div>
              </div>
              <button
                type="button"
                onClick={() => set('redistribute_unused_capital', !(config.redistribute_unused_capital !== false))}
                className="relative shrink-0 w-11 h-6 rounded-full transition-colors"
                style={{ background: config.redistribute_unused_capital !== false ? C.mint : 'rgba(255,255,255,0.12)' }}
                aria-pressed={config.redistribute_unused_capital !== false}
              >
                <span className="absolute top-0.5 w-5 h-5 rounded-full bg-white transition-all"
                  style={{ left: config.redistribute_unused_capital !== false ? '22px' : '2px' }} />
              </button>
            </div>

            {/* Skipped-picks banner — names dropped because 1 unit > their slice
                budget (from the live preview estimate). Amber; honest. */}
            <SkippedPicksBanner picks={preview?.skipped_picks} redistribute={config.redistribute_unused_capital !== false} />

            {/* SIZING TRANSPARENCY — per-name qty/lots + margin/value for EVERY
                product & instrument (CNC cash · MTF/MIS margin · FUT lots+margin). */}
            <SizingBreakdown config={config} preview={preview} loading={previewLoading} err={previewErr} />
          </div>

          {/* A · MIS defensive square-off note — for MIS sessions, the backend
              force-squares at ~15:12 IST (before the broker's window). Read-only
              note; applies to both strategies. */}
          {config.order_product === 'MIS' && config.strategy !== 'intraday_basket' && (
            <div className="mt-3 flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
              style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
              <span>
                <b>MIS session</b> — positions auto-square at ~<b>15:12 IST</b> (defensive),
                ahead of the broker&apos;s own intraday square-off window.
              </span>
            </div>
          )}

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
                <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>
                  {isLadder ? 'Trailing exit (per basket)' : 'Trailing exit (intraday basket)'}
                </span>
                <span className="ml-auto text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                  style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
                  {isLadder ? 'Positional preset' : 'Preset loaded'}
                </span>
              </div>
              {isLadder && (
                <div className="mb-3 flex items-start gap-2 rounded-lg px-3 py-2 text-[11px] leading-snug"
                  style={{ border: `1px solid ${C.line2}`, background: 'rgba(255,255,255,0.02)', color: C.muted }}>
                  <span className="shrink-0 mt-px" style={{ color: C.mint }}>{ICON.info(12)}</span>
                  <span>Each basket runs the <b style={{ color: C.ink2 }}>validated positional config</b> below — held positional with a fixed 3-session hold; the floor + hard stop carry across days. These values are <b style={{ color: C.ink2 }}>fixed for the campaign</b> (shown for reference).</span>
                </div>
              )}

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Arm / profit (%)" hint={isLadder ? 'Validated positional value (fixed).' : 'Arms trailing once the basket hits this on notional.'}>
                  <input type="number" min={0} step={0.05} disabled={isLadder} readOnly={isLadder}
                    value={config.arm_pct ?? ''}
                    onChange={(e) => set('arm_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-60 disabled:cursor-not-allowed" style={inputStyle} />
                </Field>
                <Field label="Lock floor (%)" hint={isLadder ? 'Validated positional value (fixed).' : 'Locks in at least this profit once armed.'}>
                  <input type="number" min={0} step={0.05} disabled={isLadder} readOnly={isLadder}
                    value={config.floor_pct ?? ''}
                    onChange={(e) => set('floor_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-60 disabled:cursor-not-allowed" style={inputStyle} />
                </Field>
                <Field label="Trail giveback (%)" hint={isLadder ? 'Validated positional value (fixed).' : 'Exits if the basket gives back this much from its peak.'}>
                  <input type="number" min={0} step={0.05} disabled={isLadder} readOnly={isLadder}
                    value={config.trail_giveback_pct ?? ''}
                    onChange={(e) => set('trail_giveback_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-60 disabled:cursor-not-allowed" style={inputStyle} />
                </Field>
                <Field label="Stop loss (%)" hint={isLadder ? 'Validated positional value (fixed).' : 'Hard exit if the basket drops this much on notional.'}>
                  <input type="number" min={0} step={0.05} disabled={isLadder} readOnly={isLadder}
                    value={config.stop_pct ?? ''}
                    onChange={(e) => set('stop_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-60 disabled:cursor-not-allowed" style={inputStyle} />
                </Field>

                {/* ── D · Hold mode ── Intraday forces a square-off at the set time;
                    Positional carries the floor + hard stop across days (no forced
                    square-off). MIS MUST be intraday — the backend rejects positional
                    for MIS, so it's forced + Positional is disabled with a tooltip.
                    HIDDEN for Auto-Ladder: it is always positional with a fixed
                    3-session hold (sent under the hood), so no toggle is surfaced. */}
                {!isLadder && (() => {
                  const isMis = config.order_product === 'MIS'
                  // With MIS, always show Intraday selected (the wire also forces it).
                  const holdValue: 'intraday' | 'positional' =
                    isMis ? 'intraday' : (config.square_off_enabled === false ? 'positional' : 'intraday')
                  return (
                    <Field
                      label="Hold"
                      hint={holdValue === 'positional'
                        ? 'Positional — the trailing floor + hard stop carry across days. No forced square-off.'
                        : 'Intraday — the basket is force-squared-off at the square-off time.'}
                    >
                      <div className="inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
                        {HOLD_OPTIONS.map((o) => {
                          const active = o.id === holdValue
                          const disabled = isMis && o.id === 'positional'
                          return (
                            <button
                              key={o.id}
                              type="button"
                              disabled={disabled}
                              title={disabled ? 'MIS must square off intraday' : undefined}
                              onClick={() => !disabled && onHoldChange(o.id)}
                              className="px-3 py-1.5 rounded-lg text-[12px] font-medium transition-colors disabled:cursor-not-allowed"
                              style={{
                                color: active ? '#06130c' : (disabled ? C.faint : C.ink2),
                                background: active ? C.mint : 'transparent',
                                opacity: disabled ? 0.5 : 1,
                              }}
                            >
                              {o.label}
                            </button>
                          )
                        })}
                      </div>
                    </Field>
                  )
                })()}

                {/* Square-off time / max-hold — HIDDEN for Auto-Ladder (fixed
                    3-session positional hold under the hood). Shown for Intraday;
                    max-hold for Positional in the two session strategies. */}
                {isLadder ? null : (config.order_product === 'MIS' || config.square_off_enabled !== false) ? (
                  <Field label="Square-off time (IST)" hint="Forces a flat basket at this time (HH:MM).">
                    <input type="time"
                      value={(config.square_off_time ?? '15:29:00').slice(0, 5)}
                      onChange={(e) => set('square_off_time', `${e.target.value}:00`)}
                      className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                  </Field>
                ) : (
                  <Field label="Max hold (trading sessions)"
                    hint={`Force-close the basket at ${(config.square_off_time ?? '15:29:00').slice(0, 5)} on the Nth trading session (entry day = 1; weekends & NSE holidays skipped). 0 = no cap. Falcon Positional uses 3.`}>
                    <input type="number" min={0} step={1}
                      value={config.max_hold_sessions ?? 3}
                      onChange={(e) => set('max_hold_sessions', Math.max(0, Math.floor(Number(e.target.value) || 0)))}
                      className="w-full rounded-lg px-3 py-2 text-[13px] outline-none tabular-nums" style={inputStyle} />
                    <div className="mt-1.5 flex items-start gap-2 rounded-lg px-2.5 py-1.5 text-[11px] leading-snug"
                      style={{ border: `1px solid ${C.line2}`, background: 'rgba(255,255,255,0.02)', color: C.muted }}>
                      <span className="shrink-0 mt-px" style={{ color: C.mint }}>{ICON.info(12)}</span>
                      <span>Positional — the floor + hard stop carry <b style={{ color: C.ink2 }}>across days</b>; the basket force-closes on the max-hold session (or earlier on trail/stop).</span>
                    </div>
                  </Field>
                )}
              </div>

              {/* MIS forces intraday — surface the rule when MIS is the product. */}
              {config.order_product === 'MIS' && (
                <div className="mt-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11px] leading-snug"
                  style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(13)}</span>
                  <span>
                    <b>MIS must square off intraday</b> — Positional is disabled, and MIS auto-squares
                    at ~<b>15:12 IST</b> (defensive), ahead of the broker&apos;s own square-off window.
                  </span>
                </div>
              )}

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
              <>
              <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Direction" hint="Which side(s) trigger the flat basket exit.">
                  <Segmented
                    options={KILL_DIR_OPTIONS}
                    value={config.kill_switch_direction}
                    onChange={(v) => set('kill_switch_direction', v)}
                  />
                </Field>
                <Field label="Trigger at (%)" hint="Fallback threshold used for any side left blank below (on INVESTED return).">
                  <input
                    type="number" min={0} step={0.5}
                    value={config.kill_switch_pct}
                    onChange={(e) => set('kill_switch_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                    style={inputStyle}
                  />
                </Field>
              </div>

              {/* ── B · Asymmetric thresholds ── separate Target +% (profit side)
                  and Stop −% (loss side). Blank → the single Trigger above is used
                  for that side (symmetric, today's behaviour). Shown per direction. */}
              <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
                {(config.kill_switch_direction === 'profit' || config.kill_switch_direction === 'both') && (
                  <Field label="Target +% (profit exit)" hint="Optional. Overrides the fallback on the profit side. Blank = use Trigger at (%).">
                    <div className="relative">
                      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-[13px] font-semibold" style={{ color: C.mint }}>+</span>
                      <input
                        type="number" min={0} step={0.5}
                        value={config.kill_switch_target_pct ?? ''}
                        placeholder={`${config.kill_switch_pct}`}
                        onChange={(e) => set('kill_switch_target_pct', e.target.value === '' ? undefined : (Number(e.target.value) || 0))}
                        className="w-full rounded-lg pl-7 pr-3 py-2 text-[13px] outline-none"
                        style={inputStyle}
                      />
                    </div>
                  </Field>
                )}
                {(config.kill_switch_direction === 'loss' || config.kill_switch_direction === 'both') && (
                  <Field label="Stop −% (loss exit)" hint="Optional. Overrides the fallback on the loss side. Blank = use Trigger at (%).">
                    <div className="relative">
                      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-[13px] font-semibold" style={{ color: C.red }}>−</span>
                      <input
                        type="number" min={0} step={0.5}
                        value={config.kill_switch_stop_pct ?? ''}
                        placeholder={`${config.kill_switch_pct}`}
                        onChange={(e) => set('kill_switch_stop_pct', e.target.value === '' ? undefined : (Number(e.target.value) || 0))}
                        className="w-full rounded-lg pl-7 pr-3 py-2 text-[13px] outline-none"
                        style={inputStyle}
                      />
                    </div>
                  </Field>
                )}
              </div>
              </>
            )}

            {/* ── Potential outcome (P&L preview) — only when the kill switch is on ── */}
            {config.kill_switch_enabled && (
              <PotentialOutcome
                preview={preview}
                loading={previewLoading}
                err={previewErr}
                direction={config.kill_switch_direction}
                orderProduct={config.order_product}
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

          {/* Primary CTA — "Create campaign" for Auto-Ladder (ladderCreate → a
              CREATED draft; the campaign CREATED phase then offers Start-now /
              Schedule, mirroring the session flow). "Create session" otherwise. */}
          <div className="mt-5 flex items-center gap-3">
            <button
              type="button"
              disabled={busy === 'create' || !canGoLive || (isLadder && (config.total_allocated_capital || 0) <= 0)}
              onClick={isLadder ? onCreateCampaign : onCreate}
              className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40"
              style={{ color: '#06130c', background: C.mint }}
            >
              {isLadder
                ? (busy === 'create' ? 'Creating…' : <>{ICON.loop(14)} Create campaign</>)
                : (busy === 'create' ? 'Creating…' : <>Create {mode} session {ICON.arrow(13)}</>)}
            </button>
            <span className="text-[11px]" style={{ color: C.muted }}>
              {mode === 'live' && !liveReady
                ? 'Type LIVE above to enable.'
                : mode === 'live' && !liveAccountReady
                ? 'Select your connected broker account — live needs your own active account.'
                : isLadder
                ? `Creates the ${mode} campaign as a draft — next you choose Start now or Schedule for a future date. No baskets open yet.`
                : `Creates a ${mode} session — no orders are placed yet.`}
            </span>
          </div>
        </div>
      )}

      {/* ── CAMPAIGN CREATED PHASE — confirm then Start now / Schedule ────────────
          Mirrors the session created-phase, but for an Auto-Ladder draft: a
          "Campaign created" card + Start now (RUNNING) / Schedule for a future
          trading day (SCHEDULED, via an inline date picker). A non-trading-day
          400 offers the backend's suggested_date as a one-click apply. */}
      {phase === 'created' && createdLadder && !session && (
        <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
          <div className="flex items-center gap-2 mb-1">
            <span style={{ color: C.mint }}>{ICON.check(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Campaign created</span>
            <ModePill mode={createdLadder.mode} />
            <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
              style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
              {createdLadder.product}
            </span>
          </div>
          <div className="text-[11.5px] mb-4" style={{ color: C.muted }}>
            ID <code style={{ color: C.ink2 }}>{createdLadder.ladder_id}</code> · status{' '}
            <span style={{ color: C.ink2 }}>CREATED (draft)</span> · fires{' '}
            <span style={{ color: C.ink2 }}>09:15 IST</span>
            <span style={{ color: C.faint }}> · first basket next trading morning, then every trading day — nothing armed yet</span>
          </div>

          {/* Calm retry note (parity with the session error line). */}
          {campaignErr && !campaignSuggest && (
            <div className="mb-4 flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
              style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
              <span>{campaignErr}</span>
            </div>
          )}

          {/* Non-trading-day suggestion (Schedule 400) — one-click apply the
              backend's suggested_date, mirroring the session createSuggest UI. */}
          {campaignSuggest && (
            <div className="mb-4 rounded-xl border px-3.5 py-3"
              style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
              <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                <span>
                  <b style={{ color: C.amber }}>That&apos;s not a trading day.</b>{' '}
                  Use <b style={{ color: C.ink }}>{campaignSuggest}</b> instead?
                </span>
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-2.5">
                <button type="button"
                  onClick={() => { setCampaignDate(campaignSuggest); setCampaignSuggest(null); setCampaignErr(null) }}
                  className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-[12px] font-semibold transition-opacity"
                  style={{ color: '#06130c', background: C.mint }}>
                  {ICON.check(13)} Use {campaignSuggest}
                </button>
                <button type="button" onClick={() => { setCampaignSuggest(null); setCampaignErr(null) }}
                  className="text-[11.5px]" style={{ color: C.faint }}>
                  Dismiss
                </button>
              </div>
            </div>
          )}

          {/* TWO clear ways to begin — start now, or schedule for a future day. */}
          <div className="flex flex-col sm:flex-row sm:items-stretch gap-3">
            {/* Start now → RUNNING (first basket next trading morning). */}
            <button
              type="button"
              disabled={campaignBusy != null}
              onClick={() => onCampaignStart('now')}
              className="flex-1 flex flex-col items-start gap-1 px-4 py-3 rounded-xl text-left transition-opacity disabled:opacity-40"
              style={createdLadder.mode === 'live'
                ? { color: '#1a0908', background: C.red }
                : { color: '#06130c', background: C.mint }}
            >
              <span className="flex items-center gap-2 text-[13px] font-semibold">
                {ICON.bolt(14)} {campaignBusy === 'now' ? 'Starting…' : 'Start now'}
              </span>
              <span className="text-[10.5px] leading-snug opacity-80">
                First Falcon Top-5 basket opens at 09:15 next trading morning.
              </span>
            </button>

            {/* Schedule → SCHEDULED: pick a future trading day, then arm it. */}
            <div className="flex-1 flex flex-col gap-2 px-4 py-3 rounded-xl"
              style={{ background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
              <span className="flex items-center gap-2 text-[13px] font-semibold" style={{ color: C.mint }}>
                {ICON.clock(14)} Schedule for a date
              </span>
              <input
                type="date"
                min={earliestScheduleDateIST()}
                value={campaignDate}
                onChange={(e) => { setCampaignDate(e.target.value); setCampaignSuggest(null); setCampaignErr(null) }}
                className="w-full rounded-lg px-2.5 py-1.5 text-[12px] tabular-nums outline-none"
                style={{ color: C.ink, background: 'rgba(0,0,0,0.25)', border: `1px solid ${C.line2}`, colorScheme: 'dark' }}
              />
              <button
                type="button"
                disabled={campaignBusy != null || !campaignDate}
                onClick={() => onCampaignStart('scheduled')}
                className="inline-flex items-center justify-center gap-1.5 px-3.5 py-1.5 rounded-lg text-[12px] font-semibold transition-opacity disabled:opacity-40"
                style={{ color: '#06130c', background: C.mint }}
              >
                {ICON.clock(13)} {campaignBusy === 'scheduled'
                  ? 'Scheduling…'
                  : campaignDate ? `Schedule for ${campaignDate} · 09:15` : 'Pick a date'}
              </button>
              <span className="text-[10.5px] leading-snug" style={{ color: C.muted }}>
                Arms it to open its first basket at 09:15 IST that day — nothing is placed until then.
              </span>
            </div>
          </div>

          <div className="mt-3 flex items-center gap-3">
            <button type="button" onClick={onDiscardCampaign}
              className="text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              Discard
            </button>
            <p className="text-[11px] leading-snug" style={{ color: C.faint }}>
              {createdLadder.mode === 'paper'
                ? 'Paper simulates the daily baskets — no broker orders are sent.'
                : 'Live attempts real broker orders, but only if the server flag FALCON_AUTOTRADE_ENABLED is set; otherwise it reports skipped.'}
            </p>
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

          {/* Skipped-picks banner (create response) — names dropped at sizing. */}
          <div className="mb-4">
            <SkippedPicksBanner picks={session.skipped_picks} redistribute={config.redistribute_unused_capital !== false} />
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

                {/* Skipped-picks banner — names dropped at sizing (live). */}
                <div className="mb-3">
                  <SkippedPicksBanner picks={status.skipped_picks} redistribute />
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
            <button type="button" onClick={onNewSessionClick}
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

      {/* ── Campaign action error (pause/resume/kill) — calm, dismissible ─────── */}
      {ladderErr && (ladders?.length ?? 0) > 0 && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
          <span>{ladderErr}</span>
          <button type="button" onClick={() => setLadderErr(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
            {ICON.close(13)}
          </button>
        </div>
      )}

      {/* ── Campaign KILL modal — mode is REQUIRED (no silent default) ───────── */}
      {killLadderId && (() => {
        // SCHEDULED / CREATED campaigns have NOTHING deployed yet, so the
        // flatten-now vs let-open-baskets-finish choice is meaningless — show a
        // plain confirm instead. RUNNING / PAUSED keep the wind-down modes.
        const _kl = (ladders ?? []).find((l) => l.ladder_id === killLadderId)
        const _st = (_kl?.status ?? '').toUpperCase()
        const killScheduled = _st === 'SCHEDULED' || _st === 'CREATED'
        return (
        <div className="fixed inset-0 z-50 flex items-center justify-center px-4"
          style={{ background: 'rgba(0,0,0,0.6)' }}
          onClick={() => (ladderBusy[killLadderId] == null) && setKillLadderId(null)}>
          <div className="w-full max-w-md rounded-2xl border p-5"
            style={{ borderColor: 'rgba(232,115,107,0.4)', background: C.panel }} onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center gap-2 mb-2">
              <span style={{ color: C.red }}>{ICON.shield(16)}</span>
              <span className="text-[14px] font-semibold" style={{ color: C.ink }}>
                {killScheduled ? 'Cancel scheduled campaign' : 'Stop the campaign'}
              </span>
              <button type="button" onClick={() => setKillLadderId(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
                {ICON.close(14)}
              </button>
            </div>

            {killScheduled ? (
              <>
                <p className="text-[11.5px] leading-snug mb-4" style={{ color: C.muted }}>
                  This campaign hasn’t opened anything yet — cancelling just removes it.
                  There are no open baskets to exit.
                </p>
                <div className="flex items-center gap-2.5">
                  <button type="button"
                    disabled={ladderBusy[killLadderId] != null}
                    onClick={() => onLadderKill('flatten_now')}
                    className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: '#fff', background: C.red }}>
                    {ladderBusy[killLadderId] === 'kill' ? 'Cancelling…' : 'Cancel campaign'}
                  </button>
                  <button type="button" disabled={ladderBusy[killLadderId] != null} onClick={() => setKillLadderId(null)}
                    className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                    style={{ color: C.ink2, border: `1px solid ${C.line2}` }}>
                    Keep it
                  </button>
                </div>
              </>
            ) : (
              <>
                <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.muted }}>
                  Choose how to wind the campaign down — this is required.
                </p>
                <div className="flex flex-col gap-2">
                  {LADDER_KILL_OPTIONS.map((o) => {
                    const active = ladderKillMode === o.id
                    return (
                      <button key={o.id} type="button" onClick={() => setLadderKillMode(o.id)}
                        className="flex items-start gap-2.5 rounded-xl border px-3.5 py-3 text-left transition-colors"
                        style={{
                          borderColor: active ? 'rgba(232,115,107,0.55)' : C.line2,
                          background: active ? 'rgba(232,115,107,0.07)' : 'rgba(255,255,255,0.015)',
                        }}>
                        <span className="shrink-0 mt-0.5 grid place-items-center w-4 h-4 rounded-full"
                          style={{ boxShadow: `inset 0 0 0 1.5px ${active ? C.red : C.line2}` }}>
                          {active && <span className="w-2 h-2 rounded-full" style={{ background: C.red }} />}
                        </span>
                        <span className="min-w-0">
                          <span className="block text-[12.5px] font-semibold" style={{ color: active ? C.ink : C.ink2 }}>{o.label}</span>
                          <span className="block text-[10.5px] leading-snug mt-0.5" style={{ color: C.faint }}>{o.line}</span>
                        </span>
                      </button>
                    )
                  })}
                </div>
                <div className="flex items-center gap-2.5 mt-4">
                  <button type="button"
                    disabled={ladderBusy[killLadderId] != null || ladderKillMode == null}
                    onClick={() => onLadderKill()}
                    className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: '#fff', background: C.red }}>
                    {ladderBusy[killLadderId] === 'kill' ? 'Stopping…' : 'Confirm stop'}
                  </button>
                  <button type="button" disabled={ladderBusy[killLadderId] != null} onClick={() => setKillLadderId(null)}
                    className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                    style={{ color: C.ink2, border: `1px solid ${C.line2}` }}>
                    Cancel
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
        )
      })()}

      {/* ── Broker allowlist (egress IP self-service) ────────────────────────── */}
      <EgressIpCard />

      {/* ── Read-only saved configs + brokers ────────────────────────────────── */}
      <ReferenceLists />
    </div>
  )
}

// ── Campaign chip — tags a session row that is a child basket of a running
// Auto-Ladder campaign. Trader-term "Campaign"; never surfaces internal words.
function CampaignChip() {
  return (
    <span className="inline-flex items-center gap-1 text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {ICON.loop(9)} Campaign
    </span>
  )
}

// ── Ladder campaign summary card — reuses the AutoLadderPanel card/controls
// rendering, rendered ATOP the sessions list. Every number comes from the live
// ladderStatus poll (falls back to the list summary), missing → "—". Controls:
// Pause / Resume + Stop (opens the required-mode kill modal). Trader terms only.
function LadderCampaignCard({
  summary, status, busy, onPause, onResume, onKill,
}: {
  summary: LadderSummary
  status?: LadderStatus
  busy?: 'pause' | 'resume' | 'kill'
  onPause: () => void
  onResume: () => void
  onKill: () => void
}) {
  const st: LadderStatusName | undefined = status?.status ?? summary.status
  const upper = (s?: string | null) => (s ?? '').toUpperCase()
  const running = upper(st) === 'RUNNING'
  const paused = upper(st) === 'PAUSED'
  // SCHEDULED = armed for a future start_date; nothing deployed yet. Its card
  // shows "starts {start_date}" and offers Cancel (kill) but no Pause.
  const scheduled = upper(st) === 'SCHEDULED'
  // ₹ or "—"
  const rs = (v?: number) => (typeof v === 'number' && Number.isFinite(v) ? fmtINR(v) : '—')
  const signed = (v?: number) => (typeof v === 'number' && Number.isFinite(v) ? signedINR(v) : '—')
  const num = (v?: number) => (typeof v === 'number' && Number.isFinite(v) ? String(v) : '—')
  const product = status?.order_product ?? summary.order_product
  const totalCapital = status?.total_capital ?? summary.total_capital
  const perBasket = status?.per_basket_capital
  const endDate = status?.end_date ?? summary.end_date
  const startDate = status?.start_date ?? summary.start_date
  // Live countdown to the campaign's first-basket time (09:15 IST on start_date) —
  // parity with the single-session scheduled countdown. Ticks every 1s while
  // SCHEDULED; computed client-side from start_date (no backend seconds needed).
  const [nowMs, setNowMs] = useState(() => Date.now())
  useEffect(() => {
    if (!scheduled || !startDate) return
    const id = setInterval(() => setNowMs(Date.now()), 1000)
    return () => clearInterval(id)
  }, [scheduled, startDate])
  const fireMs = scheduled && startDate ? Date.parse(`${startDate}T09:15:00+05:30`) : NaN
  const remainMs = Number.isFinite(fireMs) ? Math.max(0, fireMs - nowMs) : null
  const fmtCountdown = (ms: number) => {
    const s = Math.floor(ms / 1000)
    const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600)
    const m = Math.floor((s % 3600) / 60), sec = s % 60
    return d > 0 ? `${d}d ${h}h ${m}m` : `${h}h ${m}m ${sec}s`
  }

  return (
    <div className="rounded-2xl border overflow-hidden"
      style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'linear-gradient(180deg, rgba(63,227,164,0.05), rgba(255,255,255,0.012))' }}>
      {/* Header */}
      <div className="flex items-center gap-2 flex-wrap px-4 pt-3.5 pb-2">
        <span style={{ color: C.mint }}>{ICON.loop(15)}</span>
        <span className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Monthly campaign</span>
        {/* Short id so multiple campaigns are distinguishable at a glance. */}
        <span className="text-[9px] font-mono rounded-full px-1.5 py-0.5"
          style={{ color: C.muted, background: 'rgba(255,255,255,0.05)' }}>
          #{String(summary.ladder_id).slice(0, 6)}
        </span>
        <LadderStatusPill status={st} />
        <ModePill mode={(summary.mode ?? 'paper') as Mode} />
        {product && (
          <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
            style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
            {product}
          </span>
        )}
        {(startDate || endDate) && (
          <span className="ml-auto text-[10.5px]" style={{ color: scheduled ? C.amber : C.muted }}>
            {scheduled
              ? (startDate ? `starts ${startDate}` : 'scheduled')
              : (startDate ? `started ${startDate}` : 'running')}
            {endDate ? ` · runs to ${endDate}` : ''}
          </span>
        )}
      </div>

      {/* Live countdown to the first-basket time — parity with the single-session
          scheduled view. Client-computed from start_date @ 09:15 IST. */}
      {scheduled && remainMs != null && (
        <div className="mx-4 mb-2 flex items-center justify-between gap-3 rounded-xl border px-3.5 py-2.5"
          style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
          <div className="flex items-center gap-2 text-[11.5px]" style={{ color: C.ink2 }}>
            <span style={{ color: C.amber }}>{ICON.clock(14)}</span>
            <span>Fires <b style={{ color: C.ink }}>{startDate} 09:15</b> IST — its first Falcon Top-5 basket</span>
          </div>
          <span className="text-[15px] font-semibold tabular-nums shrink-0" style={{ color: C.mint }}>
            {remainMs === 0 ? 'starting…' : fmtCountdown(remainMs)}
          </span>
        </div>
      )}

      {/* Verbatim downturn alert — calm amber, informational (NOT an error). */}
      {status?.alert?.active && (
        <div className="mx-4 mb-2 flex items-start gap-2.5 rounded-xl border px-3.5 py-2.5"
          style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
          <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
            <div>{status.alert.message}</div>
            {typeof status.alert.trailing_5d_avg_return === 'number' && (
              <div className="text-[10.5px] mt-1" style={{ color: C.faint }}>
                Trailing 5-day average return:{' '}
                <b style={{ color: pctTone(status.alert.trailing_5d_avg_return) }}>
                  {fracPct(status.alert.trailing_5d_avg_return)}
                </b>
                {typeof status.alert.n_days_in_window === 'number' ? ` · over ${status.alert.n_days_in_window} days` : ''}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Capital row */}
      <div className="grid grid-cols-3 gap-3 px-4 pb-2">
        <MiniStat label="Deployed" value={rs(status?.capital_deployed)} />
        <MiniStat label="Free" value={rs(status?.capital_free)} />
        <MiniStat label="Total" value={rs(totalCapital)} sub={typeof perBasket === 'number' ? `per basket ${fmtINR(perBasket)}` : undefined} />
      </div>

      {/* Activity + P&L row */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 px-4 pb-3">
        <MiniStat label="Active baskets" value={num(status?.n_active_baskets)} />
        <MiniStat label="Open positions" value={num(status?.n_open_positions)} />
        <MiniStat label="Daily P&L" value={signed(status?.today_pnl)} valueColor={typeof status?.today_pnl === 'number' ? pctTone(status.today_pnl) : C.faint} />
        <MiniStat label="Realized" value={signed(status?.realized_pnl)} valueColor={typeof status?.realized_pnl === 'number' ? pctTone(status.realized_pnl) : C.faint} />
        <MiniStat label="Unrealized" value={signed(status?.unrealized_pnl)} valueColor={typeof status?.unrealized_pnl === 'number' ? pctTone(status.unrealized_pnl) : C.faint} />
      </div>

      {/* Controls */}
      <div className="flex items-center gap-2.5 flex-wrap px-4 pb-3.5">
        {/* Pause/Resume only make sense once the campaign is live. A SCHEDULED
            campaign has nothing to pause — it only offers Cancel. */}
        {!scheduled && (paused ? (
          <button type="button" disabled={busy != null} onClick={onResume}
            className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
            style={{ color: '#06130c', background: C.mint }}>
            {ICON.bolt(14)} {busy === 'resume' ? 'Resuming…' : 'Resume'}
          </button>
        ) : (
          <button type="button" disabled={busy != null} onClick={onPause}
            className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
            style={{ color: C.ink2, border: `1px solid ${C.line2}` }}>
            {ICON.clock(14)} {busy === 'pause' ? 'Pausing…' : 'Pause'}
          </button>
        ))}
        {(running || paused || scheduled) && (
          <button type="button" disabled={busy != null} onClick={onKill}
            className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
            style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
            {ICON.close(13)} {scheduled ? 'Cancel campaign' : 'Stop campaign'}
          </button>
        )}
        <span className="ml-auto text-[10.5px]" style={{ color: C.faint }}>
          {scheduled
            ? 'It activates on its start date — its baskets will appear below then.'
            : (status?.n_active_baskets ?? summary.n_active_baskets ?? 0) === 0
              ? 'Waiting for the next trading day — it opens its first Falcon Top-5 basket at 09:15.'
              : 'Its baskets appear below, tagged “Campaign”.'}
        </span>
      </div>
    </div>
  )
}

// Small labelled stat used inside the campaign card.
function MiniStat({ label, value, valueColor, sub }: { label: string; value: string; valueColor?: string; sub?: string }) {
  return (
    <div className="rounded-xl border px-3 py-2" style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)' }}>
      <div className="text-[9.5px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[14px] font-semibold mt-0.5 tabular-nums" style={{ color: valueColor ?? C.ink }}>{value}</div>
      {sub && <div className="text-[9px] mt-0.5" style={{ color: C.faint }}>{sub}</div>}
    </div>
  )
}

// Campaign status pill (running pulses mint; paused amber; done/other muted).
function LadderStatusPill({ status }: { status?: LadderStatusName }) {
  const s = (status ?? '').toUpperCase()
  const running = s === 'RUNNING'
  const paused = s === 'PAUSED'
  // SCHEDULED (armed, not yet live) reads amber like Paused — a calm "waiting"
  // state, not the live-green look.
  const scheduled = s === 'SCHEDULED'
  const done = s === 'COMPLETED' || s === 'ENDED'
  const amberTone = paused || scheduled
  const tone = running ? C.mint : amberTone ? C.amber : done ? C.mint : C.muted
  const bg = running || done ? 'rgba(63,227,164,0.12)' : amberTone ? 'rgba(230,180,80,0.12)' : 'rgba(133,153,144,0.13)'
  const ring = running || done ? 'rgba(63,227,164,0.4)' : amberTone ? 'rgba(230,180,80,0.4)' : 'rgba(133,153,144,0.4)'
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: tone, background: bg, boxShadow: `inset 0 0 0 1px ${ring}` }}>
      {running && <span className="inline-block w-1.5 h-1.5 rounded-full live-dot" style={{ background: C.mint }} />}
      {s || 'campaign'}
    </span>
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

// ── Skipped-picks banner (C · leftover capital) ──────────────────────────────
// Renders an amber banner when the backend skipped one or more picks because a
// single unit of the name costs more than its per-slice budget (e.g. a high-
// priced stock like PAGEIND). Names the symbols; notes whether their freed
// capital is being redistributed. Renders nothing when there are no skips.
function SkippedPicksBanner({ picks, redistribute }: { picks?: SkippedPick[]; redistribute?: boolean }) {
  if (!picks || picks.length === 0) return null
  const syms = picks.map((p) => p.symbol).filter(Boolean) as string[]
  const n = picks.length
  return (
    <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
      style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
      <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
      <span>
        <b style={{ color: C.amber }}>{n} pick{n > 1 ? 's' : ''} skipped</b>
        {' '}— priced above their per-slice budget
        {syms.length ? <>: <b style={{ color: C.ink }}>{syms.join(', ')}</b></> : null}.
        {' '}
        {redistribute
          ? 'Their capital is redistributed across the remaining picks.'
          : 'Their capital is left idle (Use leftover capital is off).'}
      </span>
    </div>
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
  preview, loading, err, direction, orderProduct,
}: { preview: PreviewResponse | null; loading: boolean; err: string | null; direction: KillDirection; orderProduct: OrderProduct }) {
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
            {' '}· ~{(preview.leverage ?? 1).toFixed(preview.leverage % 1 === 0 ? 0 : 2)}× ({orderProduct})
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
// Sizing transparency — per-name breakdown for ANY product/instrument, showing the
// FUNDING split so it's clear how much is YOUR money vs the broker's:
//   Your margin  = capital you put up      (Σ = your money in)
//   Broker funds = leverage the broker adds (deployed − your margin; 0 for cash)
//   Deployed     = total position value    (your margin + broker funds)
// Reads preview.positions (qty/ref_price/margin/invested_value + lots/lot_size/
// margin_per_lot for FUT). ALWAYS renders a state — loading / error / empty /
// table — so the panel never silently disappears.
type SizeCol = { label: string; get: (p: PreviewPosition) => string | number; strong?: boolean; tone?: string; total?: 'margin' | 'broker' | 'deployed' }
function SizingBreakdown({ config, preview, loading, err }:
    { config: SessionConfig; preview: PreviewResponse | null; loading?: boolean; err?: string | null }) {
  const isFut = config.instrument_type === 'FUT'
  const product = String(config.order_product ?? 'CNC')
  const title = isFut ? 'Futures sizing — margin & buying power'
    : `${product} sizing — margin & buying power`

  // One shell so EVERY state (loading / error / empty / table) shows the header —
  // the panel is never a mysterious blank.
  const wrap = (inner: ReactNode) => (
    <div className="mt-3 rounded-xl border p-3.5" style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.05)' }}>
      <div className="flex items-center gap-2 mb-2.5">
        <span style={{ color: C.mint }}>{ICON.info(14)}</span>
        <span className="text-[12px] font-semibold" style={{ color: C.ink }}>{title}</span>
        <span className="ml-auto text-[10px]" style={{ color: C.faint }}>estimate — places nothing</span>
      </div>
      {inner}
    </div>
  )

  const rows = (preview?.positions ?? []).filter((p) => p.status !== 'SKIPPED' && (p.qty ?? 0) > 0)
  if (!preview || rows.length === 0) {
    if (loading) return wrap(<p className="text-[11.5px]" style={{ color: C.muted }}>Estimating sizing…</p>)
    if (err) return wrap(
      <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
        <span style={{ color: C.amber }}>Couldn&apos;t estimate sizing</span>{' '}
        <span style={{ color: C.faint }}>({err}). It will refresh on your next change.</span>
      </p>)
    if (preview) return wrap(<p className="text-[11.5px]" style={{ color: C.faint }}>No sizable positions for this capital / universe.</p>)
    return wrap(<p className="text-[11.5px]" style={{ color: C.muted }}>Estimating sizing…</p>)
  }

  const inr = (v: number | undefined) => fmtINR(v ?? 0)
  const lev = preview.leverage ?? 1
  const myFund = preview.total_allocated_capital
  const totalMargin = preview.total_margin ?? rows.reduce((s, p) => s + (p.margin ?? 0), 0)
  const totalDeployed = preview.invested_basis
  const totalBroker = Math.max(0, totalDeployed - totalMargin)
  const deployedOf = (p: PreviewPosition) => ((isFut ? (p.notional ?? p.invested_value) : p.invested_value) ?? 0)
  const brokerOf = (p: PreviewPosition) => Math.max(0, deployedOf(p) - (p.margin ?? 0))
  const totalFor = (t?: string) =>
    t === 'margin' ? fmtINR(totalMargin)
    : t === 'broker' ? (totalBroker > 1 ? fmtINR(totalBroker) : '—')
    : t === 'deployed' ? fmtINR(totalDeployed) : ''

  // Funding columns shown for EVERY product, so switching MIS↔MTF↔CNC makes the
  // difference obvious (Broker funds lights up only when the product leverages).
  const fundingCols: SizeCol[] = [
    { label: 'Your margin', get: (p) => inr(p.margin), strong: true, tone: C.mint, total: 'margin' },
    { label: 'Broker funds', get: (p) => { const b = brokerOf(p); return b > 1 ? inr(b) : '—' }, tone: C.amber, total: 'broker' },
    { label: 'Deployed', get: (p) => inr(deployedOf(p)), tone: C.ink2, total: 'deployed' },
  ]
  const cols: SizeCol[] = isFut
    ? [{ label: 'Lots', get: (p) => p.lots ?? 0, tone: C.mint }, { label: 'Lot size', get: (p) => p.lot_size ?? 0 },
       { label: 'Qty', get: (p) => p.qty ?? 0 }, { label: 'Margin/lot', get: (p) => inr(p.margin_per_lot) }, ...fundingCols]
    : [{ label: 'Qty', get: (p) => p.qty ?? 0 }, { label: 'Price', get: (p) => inr(p.ref_price) }, ...fundingCols]
  const cell = 'text-right tabular-nums py-1.5 px-2'

  return wrap(
    <>
      <div className="overflow-x-auto">
        <table className="w-full text-[11.5px]" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ color: C.faint }}>
              <th className="text-left font-medium py-1 px-2">Name</th>
              {cols.map((c) => <th key={c.label} className="text-right font-medium py-1 px-2">{c.label}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.symbol} style={{ borderTop: `1px solid ${C.line2}` }}>
                <td className="text-left py-1.5 px-2 font-semibold" style={{ color: C.ink }}>{p.symbol}</td>
                {cols.map((c) => (
                  <td key={c.label} className={cell + (c.strong ? ' font-semibold' : '')}
                      style={{ color: c.tone ?? C.ink2 }}>{c.get(p)}</td>
                ))}
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr style={{ borderTop: `1px solid ${C.line}` }}>
              <td className="text-left py-1.5 px-2 font-semibold" style={{ color: C.ink }}>Total</td>
              {cols.map((c) => (
                <td key={c.label} className={cell + (c.total ? ' font-semibold' : '')}
                    style={{ color: c.total === 'margin' ? C.mint : c.total === 'broker' ? C.amber : c.total === 'deployed' ? C.ink : 'transparent' }}>
                  {totalFor(c.total)}
                </td>
              ))}
            </tr>
          </tfoot>
        </table>
      </div>
      <p className="text-[10.5px] leading-snug mt-2" style={{ color: C.faint }}>
        {totalBroker > 1 ? (
          <>You put in <b style={{ color: C.mint }}>{fmtINR(totalMargin)}</b>, the broker funds{' '}
            <b style={{ color: C.amber }}>{fmtINR(totalBroker)}</b> → <b style={{ color: C.ink2 }}>{fmtINR(totalDeployed)}</b> deployed
            {' '}(~{lev.toFixed(lev % 1 === 0 ? 0 : 2)}× buying power). Your fund: <b style={{ color: C.ink2 }}>{fmtCapital(myFund)}</b>.</>
        ) : (
          <>You put in <b style={{ color: C.mint }}>{fmtINR(totalMargin)}</b> → <b style={{ color: C.ink2 }}>{fmtINR(totalDeployed)}</b> deployed
            {' '}(<b style={{ color: C.ink2 }}>no broker margin, 1×</b>). Your fund: <b style={{ color: C.ink2 }}>{fmtCapital(myFund)}</b>.</>
        )}
      </p>
    </>
  )
}

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
