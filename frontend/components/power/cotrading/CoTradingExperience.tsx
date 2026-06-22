'use client'

/**
 * CoTradingExperience — the Kanida.AI Co-Trading flow, redesigned (2026-06-21)
 * into a SIMPLE, VISUAL, decision-oriented 2-STAGE flow.
 *
 * The product must read in a few seconds: "Choose style → Add capital →
 * Falcon manages the plan → See performance." Only TWO user actions, then
 * Falcon does everything automatically.
 *
 *   STAGE 1 — SETUP (one calm centered screen):
 *     • Choose trading style (visual cards; only Falcon Top 10 Swing live).
 *     • Enter virtual capital (input + ₹1L/₹5L/₹10L chips).
 *     • Choose start: "Start Today" (live) OR a historical date (replay).
 *     • One big "Start Co-Trading" button.
 *     • ONE secondary link "How Falcon manages your money" → RulesSlideOver
 *       (the ONLY place rules live). No risk profile, no allocation config,
 *       no rules tables on this screen. Falcon decides everything.
 *
 *   STAGE 2 — RESULT (the hero; visual, minimal numbers):
 *     • A compact summary strip (Starting · Current/Ending · P&L · Return% ·
 *       Open · Cash · Max Drawdown — honest "pending" where live data isn't
 *       served).
 *     • LIVE ("Start Today"): "Falcon selected your Top 10" — a clean visual
 *       portfolio of cards (symbol · tier · capital · entry · stop · STATUS
 *       pill). Status is HONEST: pre-open shows "Queued for 9:15" / "Waiting";
 *       live Hold/Trailing/Exit + P&L are a Backend need (never fabricated).
 *       One-line Falcon caption — not a rules table.
 *     • REPLAY (historical date): a HERO performance result from the REAL
 *       persona endpoint (avg/positive years/win rate + an EQUITY CURVE built
 *       from the real monthly end_equity series) — honest about year grain.
 *     • "Inspect deeper ▾" — ONE expandable (collapsed) revealing the
 *       year-by-year table (real), month drill (real), and the rulebook link.
 *     • "← Change plan" returns to Stage 1; AutoTrade bridge CTA at the end.
 *
 * HARD HONESTY RULES (money is involved):
 *   • 100% REAL signals — PowerAPI.falconTop20('all500') (seeded server-side;
 *     re-fetched by signal_date for a replay attempt).
 *   • Allocation = REAL math: FIXED ₹50k/pick @ ₹5 L base scaled to capital,
 *     qty = floor(alloc / entry). Entry = the pick's real price else "—".
 *   • NO fabricated P&L, current value, return %, drawdown, or live status.
 *   • Replay performance/equity-curve = the REAL persona backtest endpoint
 *     (whole-window year grain; flagged where it isn't a per-date walk-forward).
 *   • Only "Falcon Top 10 Swing" is LIVE.
 */
import { Fragment, useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { PowerAPI } from '@/lib/power-api'
import type { PersonaBacktestResponse } from '@/lib/power-api'
import { CompassLogo } from '@/components/power/CompassLogo'
import { EquityChart } from '@/components/power/EquitySparkline'
import type { Top20Pick, Top20Response } from '@/lib/falcon-top20-types'

// ── Top-10 persona slug — backtest performance source of truth ───────────────
//   backend/power_user/services/persona_simulator.py PERSONA_CONFIGS
//   ("falcon-top-10") → PowerAPI.persona() → GET /api/power/personas/{slug}.
const TOP10_PERSONA_SLUG = 'falcon-top-10'

// ── REAL summary fields returned by simulate_persona() (typed locally —
//    PersonaBacktestResponse.summary is Record<string, unknown> on the client). ─
type PersonaSummary = {
  avg_yearly_return_pct?:    number
  median_yearly_return_pct?: number
  best_year_pct?:            number
  worst_year_pct?:           number
  positive_years?:           number
  total_years?:              number
  avg_max_drawdown_pct?:     number
  worst_drawdown_pct?:       number
  avg_win_rate_pct?:         number
  total_pnl_rs?:             number
  total_trades?:             number
}
type PersonaYearly = {
  year: number; return_pct: number; max_dd_pct: number; win_rate_pct: number
  n_closed: number; n_open_at_year_end: number
}
type PersonaMonthly = { year: number; month: string; end_equity: number; return_pct: number }

// ── F2 palette (tokens in globals.css :root — same helper as AskFalconHome) ──
const C = {
  canvas: 'var(--f2-canvas)', panel: 'var(--f2-panel)', card: 'var(--f2-card)', card2: 'var(--f2-card-2)',
  mint: 'var(--f2-mint)', mintHi: 'var(--f2-mint-hi)', mintDim: 'var(--f2-mint-dim)',
  ink: 'var(--f2-ink)', ink2: 'var(--f2-ink-2)', muted: 'var(--f2-muted)', faint: 'var(--f2-faint)',
  line: 'var(--f2-line)', line2: 'var(--f2-line-2)', red: 'var(--f2-red)', amber: 'var(--f2-amber)',
}

// ── Trading styles — only Swing is LIVE (rest Launch-Pending) ────────────────
type StyleId = 'swing' | 'btst' | 'intraday' | 'weekly' | 'longterm'
type Style = { id: StyleId; name: string; hold: string; holdDays: number; live: boolean; icon: keyof typeof ICON; blurb: string }
const STYLES: Style[] = [
  { id: 'swing',    name: 'Falcon Top 10 Swing', hold: '~7 trading days', holdDays: 7,  live: true,  icon: 'flame',  blurb: 'The live engine — 10 highest-conviction names, held about a week.' },
  { id: 'btst',     name: 'BTST',                hold: '1–2 days',        holdDays: 2,  live: false, icon: 'clock',  blurb: 'Buy today, sell tomorrow.' },
  { id: 'intraday', name: 'Intraday',           hold: 'Same day',        holdDays: 0,  live: false, icon: 'bolt',   blurb: 'Enter and exit within one session.' },
  { id: 'weekly',   name: 'Weekly Swing',       hold: '1–4 weeks',       holdDays: 20, live: false, icon: 'trend',  blurb: 'Ride multi-week trends.' },
  { id: 'longterm', name: 'Long-Term',          hold: '3+ months',       holdDays: 90, live: false, icon: 'shield', blurb: 'Position-build over months.' },
]

// ── FIXED allocation model. DOCUMENTED rule: ₹50,000 per pick at a ₹5,00,000
//   base across the full Top 10, scaled proportionally to the user's capital.
//   REAL math (₹ per trade × ratio, then qty = floor(perTrade / entry)). ───────
const BASE_CAPITAL = 500_000           // ₹5 L base the fixed sizing is defined at
const BASE_PER_TRADE = 50_000          // ₹50 k per pick at the ₹5 L base
const TOP_N = 10                       // full Top 10 deployment
const STANDARD_SL_PCT = -7             // locked Falcon Top 10 rule when a pick carries no SL

const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000]

// ── STATIC documented Kanida.ai virtual trading rules (facts about HOW Falcon
//    works — NOT performance numbers). Surfaced ONLY in the rules slide-over. ──
type RuleItem = { icon: keyof typeof ICON; title: string; body: string }
const VIRTUAL_RULES: RuleItem[] = [
  { icon: 'clock',  title: 'Entry',          body: '9:15 IST at the open — the Top 10 ranked by engine score.' },
  { icon: 'wallet', title: 'Trade size',     body: '₹50k per stock at a ₹5 L base; scales with your virtual capital.' },
  { icon: 'trend',  title: 'Holding',        body: 'Maximum 7 trading days per position.' },
  { icon: 'shield', title: 'Stop-loss',      body: '−7%; a gap-down exits at the actual open price.' },
  { icon: 'flame',  title: 'Smart trailing', body: 'After a +12% close, SL = higher of entry or the 10-day low — never moves down.' },
  { icon: 'bolt',   title: 'Capital',        body: 'Cash only — no leverage, integer shares, idle cash tracked.' },
  { icon: 'info',   title: 'Position rules', body: 'Skip names already held; no duplicate positions.' },
]

// ── STATIC: the 7-step trading cycle (documented flow). ──────────────────────
const TRADING_CYCLE: string[] = [
  'Engine ranks the universe at EOD; Top 10 by score are selected.',
  'Enter all 10 at 9:15 IST the next session — ₹50k each (at ₹5 L base).',
  'A −7% stop-loss is placed on every position from entry.',
  'Each position is held up to 7 trading days.',
  'On a +12% close, smart trailing arms: SL rises to the higher of entry or the 10-day low.',
  'Exit on stop, trailing stop, or the 7-day time limit (gap-downs exit at the actual price).',
  'Freed cash returns to the book; already-held names are skipped on the next cycle.',
]

// ── Tier band colouring — derive from the BAND name, NEVER signal_tier_color ─
const TIER_STYLE: Record<string, { color: string; bg: string; ring: string }> = {
  amber: { color: 'var(--f2-amber)',      bg: 'rgba(230,180,80,0.14)',  ring: 'rgba(230,180,80,0.45)' },
  green: { color: 'var(--f2-tier-green)', bg: 'rgba(63,227,164,0.14)',  ring: 'rgba(63,227,164,0.45)' },
  teal:  { color: 'var(--f2-teal)',       bg: 'rgba(75,203,224,0.14)',  ring: 'rgba(75,203,224,0.45)' },
  red:   { color: 'var(--f2-red)',        bg: 'rgba(232,115,107,0.14)', ring: 'rgba(232,115,107,0.45)' },
  gray:  { color: 'var(--f2-slate)',      bg: 'rgba(133,153,144,0.13)', ring: 'rgba(133,153,144,0.40)' },
}
const BAND_COLORKEY: Record<string, string> = {
  GOLD: 'amber', PREMIUM: 'teal', ENTERPRISE: 'green', STANDARD: 'gray', AVOID: 'red',
}
function tierBand(raw: string | null | undefined): string | null {
  if (!raw) return null
  return raw.split('-')[0].toUpperCase()
}

// ── Entry price resolution — only ever a REAL number off the pick; else null. ─
function entryPriceOf(p: Top20Pick): number | null {
  const fromAction = p.action?.entry_price_rs
  if (typeof fromAction === 'number' && fromAction > 0) return fromAction
  return null
}

// ── Allocation math (REAL): FIXED ₹/trade scaled, qty = floor(alloc/entry). ──
type AllocRow = {
  pick:    Top20Pick
  entry:   number | null
  qty:     number
  capital: number
  slPrice: number | null
  slPct:   number
}

type Props = {
  data:      Top20Response | null
  firstName: string
}

type Stage = 'setup' | 'result'

export function CoTradingExperience({ data: seed, firstName }: Props) {
  const router = useRouter()

  // ── Flow stage — the whole UX is two screens ───────────────────────────────
  const [stage, setStage] = useState<Stage>('setup')

  // ── Setup state ──────────────────────────────────────────────────────────
  const [styleId, setStyleId] = useState<StyleId>('swing')
  const [capital, setCapital] = useState<number>(500_000)
  const today = useMemo(() => istTodayISO(), [])
  const [mode, setMode] = useState<'today' | 'replay'>('today')   // start choice
  const [startDate, setStartDate] = useState<string>(today)

  // ── Live picks (REAL). A replay attempts a point-in-time signal_date fetch. ─
  const [data, setData]       = useState<Top20Response | null>(seed)
  const [loading, setLoading] = useState(false)
  const [replayPending, setReplayPending] = useState(false)

  // ── Trading-rules slide-over (the ONE place all rules content lives) ────────
  const [rulesOpen, setRulesOpen] = useState(false)

  // ── REAL backtest performance (persona simulator). NEVER hardcoded. ────────
  const [persona, setPersona]       = useState<PersonaBacktestResponse | null>(null)
  const [personaErr, setPersonaErr] = useState(false)
  const [personaLoading, setPersonaLoading] = useState(true)
  useEffect(() => {
    const ac = new AbortController()
    setPersonaLoading(true); setPersonaErr(false)
    PowerAPI.persona(TOP10_PERSONA_SLUG, ac.signal)
      .then(p => { setPersona(p); setPersonaLoading(false) })
      .catch(() => { setPersonaErr(true); setPersonaLoading(false) })
    return () => ac.abort()
  }, [])

  const style = STYLES.find(s => s.id === styleId)!
  const isReplay = mode === 'replay'
  // Fixed per-trade ₹, scaled proportionally from the ₹5 L base to user capital.
  const perTrade = Math.max(0, (capital / BASE_CAPITAL) * BASE_PER_TRADE)

  // ── Build the allocation rows (REAL picks + FIXED ₹/trade math) ────────────
  const rows: AllocRow[] = useMemo(() => {
    if (!data || style.id !== 'swing') return []
    return data.picks.slice(0, TOP_N).map(p => {
      const entry = entryPriceOf(p)
      const qty = entry && entry > 0 && perTrade > 0 ? Math.floor(perTrade / entry) : 0
      const cap = entry && entry > 0 ? qty * entry : 0
      const slPctRaw = p.action?.stop_loss_pct
      const slPct = typeof slPctRaw === 'number' && slPctRaw !== 0 ? slPctRaw : STANDARD_SL_PCT
      const slPrice = entry ? +(entry * (1 + slPct / 100)).toFixed(2) : null
      return { pick: p, entry, qty, capital: cap, slPrice, slPct }
    })
  }, [data, style.id, perTrade])

  const committed = rows.reduce((a, r) => a + r.capital, 0)
  const cashLeft  = Math.max(0, capital - committed)
  const anyEntryMissing = rows.some(r => r.entry == null)
  const showReplayPending = isReplay && replayPending

  const canStart = style.live && !loading && capital > 0

  // ── Start handler: live → straight to result; replay → fetch then result ───
  async function handleStart() {
    if (!canStart) return
    if (isReplay) {
      setLoading(true); setReplayPending(false)
      try {
        const res = await PowerAPI.falconTop20('all500', null, startDate)
        if (res?.picks?.length) { setData(res); setReplayPending(false) }
        else setReplayPending(true)
      } catch {
        setReplayPending(true)
      } finally {
        setLoading(false)
        setStage('result')
      }
    } else {
      setData(seed)
      setStage('result')
    }
  }

  // ── RENDER ─────────────────────────────────────────────────────────────────
  return (
    <div className="relative flex flex-col min-h-screen md:min-h-0 md:h-full md:overflow-hidden"
         style={{ background: C.canvas, color: C.ink }}>
      {stage === 'setup' ? (
        <SetupStage
          firstName={firstName}
          styleId={styleId} setStyleId={setStyleId}
          capital={capital} setCapital={setCapital}
          mode={mode} setMode={(m) => { setMode(m); if (m === 'today') setStartDate(today) }}
          startDate={startDate} setStartDate={setStartDate} today={today}
          canStart={canStart} loading={loading}
          onStart={handleStart} onOpenRules={() => setRulesOpen(true)}
          onAutoTrade={() => router.push('/power/autotrade')}
        />
      ) : (
        <ResultStage
          style={style} firstName={firstName}
          capital={capital} committed={committed} cashLeft={cashLeft}
          rows={rows} data={data} isReplay={isReplay} startDate={startDate}
          today={today} perTrade={perTrade} anyEntryMissing={anyEntryMissing}
          showReplayPending={showReplayPending}
          persona={persona} personaLoading={personaLoading} personaErr={personaErr}
          onChangePlan={() => setStage('setup')}
          onOpenRules={() => setRulesOpen(true)}
          onAutoTrade={() => router.push('/power/autotrade')}
          onAnalyze={(sym) => router.push(`/power/ask?symbol=${encodeURIComponent(sym)}`)}
        />
      )}

      {/* ── Trading-rules slide-over (in-flow faux-overlay, NOT position:fixed,
          so the viewport-lock is preserved). The ONE place all rules live. ── */}
      {rulesOpen && (
        <RulesSlideOver perTrade={perTrade} capital={capital} onClose={() => setRulesOpen(false)} />
      )}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 1 — SETUP. One calm, centered screen. Two choices + one button.
// ════════════════════════════════════════════════════════════════════════════
function SetupStage({
  firstName, styleId, setStyleId, capital, setCapital, mode, setMode,
  startDate, setStartDate, today, canStart, loading, onStart, onOpenRules, onAutoTrade,
}: {
  firstName: string
  styleId: StyleId; setStyleId: (s: StyleId) => void
  capital: number; setCapital: (n: number) => void
  mode: 'today' | 'replay'; setMode: (m: 'today' | 'replay') => void
  startDate: string; setStartDate: (s: string) => void; today: string
  canStart: boolean; loading: boolean
  onStart: () => void; onOpenRules: () => void; onAutoTrade: () => void
}) {
  return (
    <div className="flex-1 min-h-0 md:overflow-y-auto [scrollbar-width:thin]">
      <div className="mx-auto w-full max-w-[1120px] px-5 md:px-8 py-5 md:py-6 flex flex-col gap-4 md:gap-5">

        {/* heading */}
        <div className="flex flex-col items-center text-center gap-1.5">
          <CompassLogo size={28} />
          <h1 className="text-[21px] md:text-[24px] font-semibold tracking-[-0.02em]" style={{ color: C.ink }}>
            Co-Trade with Falcon, {firstName}
          </h1>
          <p className="text-[12.5px] md:text-[13px] leading-snug max-w-[560px]" style={{ color: C.muted }}>
            Follow Falcon with <b style={{ color: C.ink }}>virtual capital</b>. Pick a style, add capital,
            and Falcon handles entries, stops, trailing and exits automatically.
          </p>
        </div>

        {/* "How Co-Trading works" — the running-machine mechanism strip (hook) */}
        <MechanismStrip onAutoTrade={onAutoTrade} />

        {/* STEP 1 — choose your trading style (visual cards, full-width 5-up) */}
        <Step n={1} title="Choose your trading style">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5">
            {STYLES.map(s => {
              const active = s.id === styleId
              return (
                <button
                  key={s.id} type="button"
                  onClick={() => { if (s.live) setStyleId(s.id) }}
                  disabled={!s.live}
                  className="relative text-left rounded-2xl border p-3 transition-colors disabled:cursor-not-allowed"
                  style={{
                    borderColor: active ? 'rgba(63,227,164,0.55)' : C.line2,
                    background: active ? 'rgba(63,227,164,0.07)' : 'rgba(255,255,255,0.02)',
                    opacity: s.live ? 1 : 0.6,
                  }}
                >
                  <div className="flex items-center gap-2 mb-1.5">
                    <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0"
                          style={{ background: C.mintDim, color: C.mint }}>{ICON[s.icon](15)}</span>
                    {s.live
                      ? (active && <span className="ml-auto w-2 h-2 rounded-full shrink-0" style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />)
                      : <span className="ml-auto text-[8.5px] font-mono uppercase tracking-[0.08em] shrink-0" style={{ color: C.faint }}>soon</span>}
                  </div>
                  <div className="text-[12.5px] font-semibold leading-tight" style={{ color: s.live ? C.ink : C.faint }}>{s.name}</div>
                  <div className="text-[10px] mt-0.5" style={{ color: C.faint }}>{s.hold}</div>
                </button>
              )
            })}
          </div>
          <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
            Only <b style={{ color: C.muted }}>Falcon Top 10 Swing</b> is live today — the other engines are
            Launch-Pending and we won&apos;t fake their picks.
          </p>
        </Step>

        {/* STEPS 2 + 3 — short, placed SIDE-BY-SIDE to fill the width and cut
            vertical emptiness. Stack on narrow widths. */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6 items-start">

        {/* STEP 2 — enter virtual capital */}
        <Step n={2} title="Enter virtual capital">
          <div className="flex items-center gap-2 rounded-xl px-3 py-2 border"
               style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.03)' }}>
            <span className="text-[16px] font-semibold" style={{ color: C.muted }}>₹</span>
            <input
              type="number" min={10_000} step={10_000} value={capital}
              onChange={e => setCapital(Math.max(0, Number(e.target.value) || 0))}
              className="w-full bg-transparent outline-none text-[17px] font-mono font-semibold tabular-nums"
              style={{ color: C.ink }}
            />
            <span className="text-[11px] font-mono shrink-0" style={{ color: C.faint }}>{fmtCapital(capital)}</span>
          </div>
          <div className="flex items-center gap-1.5 mt-2 flex-wrap">
            {CAPITAL_PRESETS.map(v => (
              <button key={v} type="button" onClick={() => setCapital(v)}
                      className="text-[12px] rounded-full px-3 py-1 border transition-colors"
                      style={capital === v
                        ? { borderColor: 'rgba(63,227,164,0.5)', color: C.mint, background: 'rgba(63,227,164,0.08)' }
                        : { borderColor: C.line2, color: C.muted }}>
                {fmtCapital(v)}
              </button>
            ))}
          </div>
          <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
            Pretend money — never a broker. Falcon sizes <b style={{ color: C.muted }}>{fmtINR(perTradeFor(capital))}</b> per pick
            (₹50k at ₹5 L, scaled).
          </p>
        </Step>

        {/* STEP 3 — choose start */}
        <Step n={3} title="Choose when to start">
          <div className="flex flex-col sm:flex-row gap-2.5">
            <button type="button" onClick={() => setMode('today')}
                    className="flex-1 text-left rounded-2xl border p-3 transition-colors"
                    style={{
                      borderColor: mode === 'today' ? 'rgba(63,227,164,0.55)' : C.line2,
                      background: mode === 'today' ? 'rgba(63,227,164,0.07)' : 'rgba(255,255,255,0.02)',
                    }}>
              <div className="flex items-center gap-2">
                <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.bolt(15)}</span>
                <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Start Today</span>
                {mode === 'today' && <span className="ml-auto w-2 h-2 rounded-full" style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />}
              </div>
              <p className="text-[10.5px] mt-1.5 leading-snug" style={{ color: C.faint }}>
                Follow today&apos;s live Top 10 forward from here.
              </p>
            </button>

            <button type="button" onClick={() => setMode('replay')}
                    className="flex-1 text-left rounded-2xl border p-3 transition-colors"
                    style={{
                      borderColor: mode === 'replay' ? 'rgba(230,180,80,0.55)' : C.line2,
                      background: mode === 'replay' ? 'rgba(230,180,80,0.07)' : 'rgba(255,255,255,0.02)',
                    }}>
              <div className="flex items-center gap-2">
                <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0"
                      style={{ background: 'rgba(230,180,80,0.14)', color: C.amber }}>{ICON.clock(15)}</span>
                <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Replay a past date</span>
                {mode === 'replay' && <span className="ml-auto w-2 h-2 rounded-full" style={{ background: C.amber, boxShadow: `0 0 8px ${C.amber}` }} />}
              </div>
              <p className="text-[10.5px] mt-1.5 leading-snug" style={{ color: C.faint }}>
                See how Falcon would have performed — point-in-time only.
              </p>
              {mode === 'replay' && (
                <input type="date" value={startDate} max={today}
                       onClick={e => e.stopPropagation()}
                       onChange={e => setStartDate(e.target.value || today)}
                       className="mt-2 text-[12.5px] rounded-lg px-2.5 py-1.5 border font-mono w-full"
                       style={{ borderColor: C.line2, background: 'rgba(0,0,0,0.25)', color: C.ink }} />
              )}
            </button>
          </div>
        </Step>

        </div>{/* end Steps 2+3 row */}

        {/* primary action + the ONE rules link — spans the width below */}
        <div className="flex flex-col items-center gap-2.5 mt-0.5">
          <button
            type="button" onClick={onStart} disabled={!canStart}
            className="w-full max-w-[420px] inline-flex items-center justify-center gap-2 rounded-xl px-5 py-3 text-[15px] font-semibold transition-colors disabled:opacity-50
                       shadow-[0_12px_32px_-12px_rgba(63,227,164,0.7)]"
            style={{ background: C.mint, color: '#06130c' }}>
            {loading ? 'Building…' : 'Start Co-Trading'}{!loading && ICON.arrow(15)}
          </button>
          <button type="button" onClick={onOpenRules}
                  className="inline-flex items-center gap-1.5 text-[12px]" style={{ color: C.muted }}>
            <span style={{ color: C.mint }}>{ICON.book(13)}</span>
            How Falcon manages your money
          </button>
          <p className="text-[10px] text-center max-w-[440px]" style={{ color: C.faint }}>
            Virtual capital · not financial advice. No order is placed — this mirrors Falcon so you can
            learn the strategy risk-free.
          </p>
        </div>
      </div>
    </div>
  )
}

function Step({ n, title, children }: { n: number; title: string; children: React.ReactNode }) {
  return (
    <section>
      <div className="flex items-center gap-2 mb-2.5">
        <span className="grid place-items-center w-5 h-5 rounded-full text-[11px] font-mono font-semibold shrink-0"
              style={{ background: C.mintDim, color: C.mint }}>{n}</span>
        <h2 className="text-[14px] font-semibold" style={{ color: C.ink }}>{title}</h2>
      </div>
      {children}
    </section>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 2 — RESULT. The hero. Summary strip → portfolio/performance → expand.
// ════════════════════════════════════════════════════════════════════════════
function ResultStage({
  style, capital, committed, cashLeft, rows, data, isReplay, startDate, today,
  perTrade, anyEntryMissing, showReplayPending, persona, personaLoading,
  personaErr, onChangePlan, onOpenRules, onAutoTrade, onAnalyze,
}: {
  style: Style; firstName: string
  capital: number; committed: number; cashLeft: number
  rows: AllocRow[]; data: Top20Response | null; isReplay: boolean; startDate: string
  today: string; perTrade: number; anyEntryMissing: boolean; showReplayPending: boolean
  persona: PersonaBacktestResponse | null; personaLoading: boolean; personaErr: boolean
  onChangePlan: () => void; onOpenRules: () => void; onAutoTrade: () => void
  onAnalyze: (s: string) => void
}) {
  // REAL selected-period numbers for the summary strip on a replay (year grain).
  const yearly = personaYearly(persona)
  const periodYear = parseInt((isReplay ? startDate : today).slice(0, 4), 10)
  const yr = yearly.find(y => y.year === periodYear) ?? null
  const periodRet = yr?.return_pct ?? null
  const periodPnl = periodRet != null ? (periodRet / 100) * capital : null
  const periodEnd = periodPnl != null ? capital + periodPnl : null
  const periodDD  = yr?.max_dd_pct ?? null
  const periodOpen = yr?.n_open_at_year_end ?? null
  const periodClosed = yr?.n_closed ?? null
  const periodWR  = yr?.win_rate_pct ?? null

  return (
    <div className="flex-1 min-h-0 flex flex-col md:overflow-hidden">
      {/* FIXED header (shrink-0): title · change-plan · summary strip */}
      <div className="shrink-0 px-5 md:px-7 pt-2.5 md:pt-3 pb-3 border-b" style={{ borderColor: C.line }}>
        <div className="flex items-center gap-2.5 mb-2.5">
          <button type="button" onClick={onChangePlan}
                  className="grid place-items-center w-8 h-8 rounded-lg border shrink-0"
                  style={{ borderColor: C.line2, color: C.muted }} title="Change plan">
            {ICON.back(15)}
          </button>
          <CompassLogo size={22} />
          <div className="min-w-0">
            <div className="text-[18px] font-semibold tracking-[-0.02em] leading-tight" style={{ color: C.ink }}>Co-Trading</div>
            <div className="text-[11.5px] truncate" style={{ color: C.faint }}>
              {style.name} · {fmtCapital(capital)} · {isReplay ? `replay @ ${startDate}` : 'live today'}
            </div>
          </div>
          <button type="button" onClick={onChangePlan}
                  className="ml-auto shrink-0 text-[11.5px] rounded-lg px-2.5 py-1.5 border inline-flex items-center gap-1.5"
                  style={{ borderColor: C.line2, color: C.muted }}>
            {ICON.back(12)} Change plan
          </button>
        </div>

        {/* slim running-machine reminder (keeps the result uncluttered) */}
        <div className="hidden sm:block mb-2"><MechanismStrip onAutoTrade={onAutoTrade} slim /></div>

        <SummaryStrip
          starting={capital}
          endValue={isReplay ? periodEnd : null}
          pnl={isReplay ? periodPnl : null}
          retPct={isReplay ? periodRet : null}
          open={isReplay ? periodOpen : rows.length}
          cash={cashLeft}
          ddPct={isReplay ? periodDD : null}
          isReplay={isReplay}
          deployedPct={capital > 0 ? (committed / capital) * 100 : 0}
        />
      </div>

      {/* SCROLLABLE body */}
      <div className="flex-1 min-h-0 md:overflow-y-auto px-5 md:px-7 pt-4 pb-8 [scrollbar-width:thin]">
        {!style.live ? (
          <StylePendingCard style={style} />
        ) : showReplayPending ? (
          <ReplayPendingCard startDate={startDate} />
        ) : isReplay ? (
          // ── REPLAY: the performance HERO (REAL persona backtest + equity curve) ──
          <div className="flex flex-col gap-5 max-w-[1000px] mx-auto">
            <ReplayHero
              persona={persona} loading={personaLoading} err={personaErr}
              capital={capital} startDate={startDate}
              periodYear={periodYear} periodRet={periodRet} periodPnl={periodPnl}
              periodEnd={periodEnd} periodDD={periodDD} periodClosed={periodClosed}
              periodOpen={periodOpen} periodWR={periodWR}
            />
            <InspectDeeper persona={persona} loading={personaLoading} err={personaErr} onOpenRules={onOpenRules} />
            <AutoTradeBridge onAutoTrade={onAutoTrade} />
          </div>
        ) : !data || rows.length === 0 ? (
          <NoPicksCard />
        ) : (
          // ── LIVE ("Start Today"): the visual portfolio ──
          <div className="flex flex-col gap-5 max-w-[1000px] mx-auto">
            <LivePortfolio
              rows={rows} perTrade={perTrade} anyEntryMissing={anyEntryMissing}
              signalDate={data.signal_date} entryDate={data.entry_date ?? data.next_trading_day}
              holdDays={style.holdDays} onAnalyze={onAnalyze}
            />
            <InspectDeeper persona={persona} loading={personaLoading} err={personaErr} onOpenRules={onOpenRules} />
            <AutoTradeBridge onAutoTrade={onAutoTrade} />
          </div>
        )}
      </div>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// SUMMARY STRIP — compact. REAL computed values; honest "—/pending" otherwise.
// ════════════════════════════════════════════════════════════════════════════
function SummaryStrip({
  starting, endValue, pnl, retPct, open, cash, ddPct, isReplay, deployedPct,
}: {
  starting: number; endValue: number | null; pnl: number | null; retPct: number | null
  open: number | null; cash: number; ddPct: number | null; isReplay: boolean; deployedPct: number
}) {
  return (
    <div>
      <div className="grid grid-cols-4 lg:grid-cols-7 rounded-[10px] border overflow-hidden"
           style={{ borderColor: C.line, background: 'rgba(255,255,255,0.02)' }}>
        <Cell label="Starting" value={fmtINR(starting)} real />
        <Cell label={isReplay ? 'Ending value' : 'Current value'}
              value={endValue == null ? '—' : fmtINR(endValue)}
              note={endValue == null ? 'pending' : undefined} real={endValue != null} />
        <Cell label="Total P&L" value={pnl == null ? '—' : signedINR(pnl)}
              note={pnl == null ? 'pending' : undefined}
              real={pnl != null} tone={pnl == null ? undefined : pctTone(pnl)} />
        <Cell label="Return %" value={fmtPct(retPct)} note={retPct == null ? 'pending' : undefined}
              real={retPct != null} tone={retPct == null ? undefined : pctTone(retPct)} />
        <Cell label="Open" value={open == null ? '—' : String(open)}
              note={isReplay ? 'at period end' : `${deployedPct.toFixed(0)}% deployed`} real={open != null} />
        <Cell label="Cash" value={fmtINR(cash)} real={!isReplay} note={isReplay ? 'pending' : undefined} />
        <Cell label="Max Drawdown" value={fmtPct(ddPct)} note={ddPct == null ? 'pending' : undefined}
              real={ddPct != null} tone={ddPct == null ? undefined : C.red} />
      </div>
      {!isReplay && (
        <div className="mt-1.5 h-1 rounded-full overflow-hidden" style={{ background: 'rgba(255,255,255,0.06)' }}>
          <div className="h-full rounded-full" style={{ width: `${Math.min(100, deployedPct)}%`, background: C.mint }} />
        </div>
      )}
    </div>
  )
}
function Cell({
  label, value, note, real = false, tone,
}: { label: string; value: string; note?: string; real?: boolean; tone?: string }) {
  return (
    <div className="px-2.5 py-1.5 border-t border-l first:border-l-0 [&:nth-child(5)]:border-t-0 lg:border-t-0 lg:[&:nth-child(5)]:border-l"
         style={{ background: real ? 'rgba(63,227,164,0.05)' : undefined, borderColor: C.line }}>
      <div className="text-[8.5px] uppercase tracking-[0.04em] leading-tight truncate" style={{ color: C.faint }}>{label}</div>
      <div className="text-[13px] font-mono font-semibold tabular-nums leading-tight mt-0.5"
           style={{ color: tone ?? (real ? C.ink : C.faint) }}>{value}</div>
      {note && <div className="text-[8px] leading-tight" style={{ color: real ? C.mint : C.faint }}>{note}</div>}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// LIVE PORTFOLIO — "Falcon selected your Top 10" — clean visual cards.
//   Per stock: symbol · tier · capital · entry · stop · STATUS pill.
//   Status is HONEST: pre-open = "Queued for 9:15" / "Waiting"; live tracking
//   (Hold/Trailing/Exit/P&L) is a Backend need — NEVER fabricated.
// ════════════════════════════════════════════════════════════════════════════
function LivePortfolio({
  rows, perTrade, anyEntryMissing, signalDate, entryDate, holdDays, onAnalyze,
}: {
  rows: AllocRow[]; perTrade: number; anyEntryMissing: boolean
  signalDate: string | null; entryDate: string | null; holdDays: number
  onAnalyze: (s: string) => void
}) {
  return (
    <div className="rounded-2xl border overflow-hidden" style={{ borderColor: C.line2, background: C.panel }}>
      <div className="px-4 py-3 border-b flex items-center gap-2 flex-wrap" style={{ borderColor: C.line }}>
        <h3 className="text-[15px] font-semibold" style={{ color: C.ink }}>Falcon selected your Top 10</h3>
        <span className="text-[11px]" style={{ color: C.faint }}>
          {fmtINR(perTrade)}/pick · ranked @ {signalDate ?? 'today'} EOD
        </span>
      </div>

      {/* one-line Falcon caption (NOT a rules table) */}
      <div className="px-4 py-2 border-b flex items-center gap-2 text-[11.5px]"
           style={{ borderColor: C.line, color: C.muted, background: 'rgba(63,227,164,0.04)' }}>
        <span style={{ color: C.mint }}>{ICON.flame(13)}</span>
        Falcon will: enter at 9:15 ({entryDate ?? 'next open'}) · −7% stop · trail after +12% · exit by day {holdDays}.
      </div>

      {anyEntryMissing && (
        <div className="px-4 py-2 text-[11px] flex items-center gap-2 border-b"
             style={{ borderColor: C.line, color: C.amber, background: 'rgba(230,180,80,0.06)' }}>
          {ICON.info(13)}
          <span style={{ color: C.ink2 }}>
            Some entry prices aren&apos;t in the signal payload yet — those cards show &quot;—&quot;. A live quote
            feed (Backend need) fills them in.
          </span>
        </div>
      )}

      {/* visual portfolio grid */}
      <div className="p-3 grid grid-cols-1 sm:grid-cols-2 gap-2.5">
        {rows.map(r => <PositionCard key={r.pick.symbol} r={r} holdDays={holdDays} onAnalyze={onAnalyze} />)}
      </div>
    </div>
  )
}

function PositionCard({ r, holdDays, onAnalyze }: { r: AllocRow; holdDays: number; onAnalyze: (s: string) => void }) {
  const p = r.pick
  const band = tierBand(p.signal_tier)
  const ts = TIER_STYLE[BAND_COLORKEY[band ?? ''] ?? 'gray'] ?? TIER_STYLE.gray
  return (
    <div className="rounded-xl border p-3" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <div className="flex items-center gap-2 mb-2">
        <span className="font-mono text-[10px] shrink-0" style={{ color: C.faint }}>#{p.rank}</span>
        <span className="text-[14px] font-semibold truncate" style={{ color: C.ink }}>{p.symbol}</span>
        {band && (
          <span className="text-[8.5px] font-semibold uppercase tracking-[0.04em] rounded px-1.5 py-0.5 shrink-0"
                title={p.signal_tier_reason ?? undefined}
                style={{ color: ts.color, background: ts.bg, boxShadow: `inset 0 0 0 1px ${ts.ring}` }}>
            {band}
          </span>
        )}
        {/* HONEST status pill — pre-open, not a fabricated live state */}
        <span className="ml-auto shrink-0 text-[9px] font-semibold uppercase tracking-[0.05em] rounded-full px-2 py-0.5"
              style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
          {r.entry == null ? 'Waiting' : 'Queued · 9:15'}
        </span>
      </div>
      <div className="text-[10.5px] truncate mb-2.5" style={{ color: C.faint }}>{p.sector}</div>

      <div className="grid grid-cols-3 gap-1.5">
        <Mini label="Capital" value={r.capital > 0 ? fmtINR(r.capital) : '—'} />
        <Mini label="Entry" value={r.entry == null ? '—' : `₹${fmtNum(r.entry)}`} sub={r.qty > 0 ? `${r.qty} sh` : undefined} />
        <Mini label="Stop" value={r.slPrice == null ? '—' : `₹${fmtNum(r.slPrice)}`} sub={`${r.slPct}%`} tone={C.red} />
      </div>

      <button type="button" onClick={() => onAnalyze(p.symbol)}
              className="mt-2.5 w-full text-[11px] rounded-lg px-2 py-1.5 border inline-flex items-center justify-center gap-1"
              style={{ borderColor: C.line2, color: C.muted }}>
        Why this pick {ICON.arrow(11)}
      </button>
    </div>
  )
}
function Mini({ label, value, sub, tone }: { label: string; value: string; sub?: string; tone?: string }) {
  return (
    <div className="rounded-lg px-2 py-1.5" style={{ background: 'rgba(0,0,0,0.2)' }}>
      <div className="text-[8.5px] uppercase tracking-[0.04em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[12px] font-mono font-semibold tabular-nums leading-tight mt-0.5" style={{ color: tone ?? C.ink }}>{value}</div>
      {sub && <div className="text-[8.5px] leading-tight" style={{ color: C.faint }}>{sub}</div>}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// REPLAY HERO — big scannable performance result (REAL persona backtest) +
//   an EQUITY CURVE built from the real monthly end_equity series.
// ════════════════════════════════════════════════════════════════════════════
function ReplayHero({
  persona, loading, err, capital, startDate, periodYear, periodRet, periodPnl,
  periodEnd, periodDD, periodClosed, periodOpen, periodWR,
}: {
  persona: PersonaBacktestResponse | null; loading: boolean; err: boolean
  capital: number; startDate: string
  periodYear: number; periodRet: number | null; periodPnl: number | null
  periodEnd: number | null; periodDD: number | null; periodClosed: number | null
  periodOpen: number | null; periodWR: number | null
}) {
  if (loading) {
    return <div className="rounded-2xl border px-4 py-10 text-[12px]" style={{ borderColor: C.line2, background: C.panel, color: C.faint }}>Loading the real backtest…</div>
  }
  if (err || !persona) {
    return (
      <div className="rounded-2xl border px-4 py-5 text-[12px] leading-relaxed" style={{ borderColor: C.line2, background: C.panel, color: C.ink2 }}>
        The backtest couldn&apos;t be loaded from the engine right now. Performance numbers come only from{' '}
        <span className="font-mono" style={{ color: C.muted }}>GET /api/power/personas/falcon-top-10</span> — we never hardcode them.
      </div>
    )
  }

  // Equity curve from the REAL monthly series, scaled to the user's capital.
  // The backtest reports a ₹5 L book; we scale end_equity by capital/₹5 L so the
  // curve is shown in the user's units (honest linear scaling of the real curve).
  const monthly = personaMonthly(persona).filter(m => m.year === periodYear)
    .sort((a, b) => a.month.localeCompare(b.month))
  const scale = capital / BASE_CAPITAL
  const points = monthly.map(m => ({ trade_date: `${m.month}-01`, total_equity: m.end_equity * scale }))
  const haveYear = periodRet != null

  return (
    <div className="rounded-2xl border overflow-hidden" style={{ borderColor: C.line2, background: C.panel }}>
      <div className="px-4 py-3 border-b flex items-center gap-2 flex-wrap" style={{ borderColor: C.line }}>
        <h3 className="text-[15px] font-semibold" style={{ color: C.ink }}>How Falcon would have performed</h3>
        <span className="text-[10.5px] rounded-full px-2 py-0.5" style={{ color: C.mint, background: C.mintDim }}>real backtest · falcon-top-10</span>
        <span className="ml-auto text-[10.5px] font-mono" style={{ color: C.faint }}>{periodYear}</span>
      </div>

      {!haveYear ? (
        <div className="px-4 py-6 text-[12.5px] leading-relaxed" style={{ color: C.ink2 }}>
          The backtest window doesn&apos;t cover <b style={{ color: C.ink }}>{periodYear}</b> (your start date {startDate}),
          so there&apos;s nothing real to show for that period. A full point-in-time walk-forward replay is a Backend need.
        </div>
      ) : (
        <>
          {/* big hero numbers: start → ending value */}
          <div className="px-4 pt-4 pb-2 flex items-end gap-3 flex-wrap">
            <div>
              <div className="text-[9.5px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Starting</div>
              <div className="text-[22px] font-mono font-semibold tabular-nums leading-none" style={{ color: C.muted }}>{fmtCapital(capital)}</div>
            </div>
            <span className="text-[18px] pb-0.5" style={{ color: C.faint }}>→</span>
            <div>
              <div className="text-[9.5px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Ending value</div>
              <div className="text-[30px] md:text-[34px] font-mono font-semibold tabular-nums leading-none tracking-[-0.02em]"
                   style={{ color: pctTone(periodRet) }}>
                {periodEnd == null ? '—' : fmtCapital(periodEnd)}
              </div>
            </div>
            <div className="ml-auto text-right">
              <div className="text-[20px] md:text-[24px] font-mono font-semibold tabular-nums leading-none" style={{ color: pctTone(periodRet) }}>
                {fmtPct(periodRet)}
              </div>
              <div className="text-[12px] font-mono tabular-nums mt-0.5" style={{ color: pctTone(periodPnl) }}>
                {periodPnl == null ? '—' : signedINR(periodPnl)}
              </div>
            </div>
          </div>

          {/* the EQUITY CURVE (real monthly end_equity series) */}
          <div className="px-3 pb-1">
            {points.length >= 2 ? (
              <EquityChart points={points} capitalStart={capital} className="w-full" />
            ) : (
              <div className="px-2 py-6 text-[11.5px]" style={{ color: C.faint }}>
                The monthly equity series for {periodYear} isn&apos;t granular enough to chart here yet — the
                year-level result above is real; a richer point-in-time curve is a Backend need.
              </div>
            )}
          </div>

          {/* secondary scannable stats */}
          <div className="grid grid-cols-3 lg:grid-cols-4 border-t" style={{ borderColor: C.line }}>
            <Stat label="Max drawdown" value={fmtPct(periodDD)} tone={periodDD == null ? undefined : C.red} />
            <Stat label="Win rate" value={periodWR == null ? '—' : `${periodWR.toFixed(0)}%`} />
            <Stat label="Completed trades" value={periodClosed == null ? '—' : String(periodClosed)} />
            <Stat label="Open at end" value={periodOpen == null ? '—' : String(periodOpen)} />
          </div>

          <div className="px-4 py-2 border-t text-[9.5px] flex items-center gap-1.5" style={{ borderColor: C.line, color: C.amber }}>
            {ICON.info(11)}
            <span style={{ color: C.faint }}>
              Calendar-year grain (the backtest reports yearly/monthly). A true per-date walk-forward from {startDate}
              {' '}is a Backend need; equity scaled linearly from the real ₹5 L book.
            </span>
          </div>
        </>
      )}
    </div>
  )
}
function Stat({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className="px-3 py-2.5 border-l first:border-l-0" style={{ borderColor: C.line }}>
      <div className="text-[14px] md:text-[16px] font-mono font-semibold tabular-nums leading-none" style={{ color: tone ?? C.ink }}>{value}</div>
      <div className="text-[9px] uppercase tracking-[0.04em] mt-1" style={{ color: C.faint }}>{label}</div>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// INSPECT DEEPER — the SINGLE expandable (collapsed). Year-by-year (real) +
//   month drill (real) + a link to the rulebook slide-over. Nothing dense until
//   the user opens it.
// ════════════════════════════════════════════════════════════════════════════
function InspectDeeper({
  persona, loading, err, onOpenRules,
}: { persona: PersonaBacktestResponse | null; loading: boolean; err: boolean; onOpenRules: () => void }) {
  return (
    <details className="group rounded-2xl border overflow-hidden" style={{ borderColor: C.line2, background: C.panel }}>
      <summary className="list-none cursor-pointer flex items-center gap-2 px-4 py-3 select-none">
        <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Inspect deeper</span>
        <span className="text-[10.5px]" style={{ color: C.faint }}>year-by-year · months · the rulebook</span>
        <span className="ml-auto transition-transform group-open:rotate-180" style={{ color: C.faint }}>{ICON.chevron(16)}</span>
      </summary>
      <div className="px-4 pb-4 border-t" style={{ borderColor: C.line }}>
        {/* rulebook link */}
        <button type="button" onClick={onOpenRules}
                className="w-full mt-3 mb-1 flex items-center gap-2.5 rounded-xl px-3 py-2.5 border transition-colors text-left"
                style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
          <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.book(15)}</span>
          <span className="min-w-0">
            <span className="block text-[12.5px] font-semibold leading-tight" style={{ color: C.ink }}>How Falcon manages your money</span>
            <span className="block text-[10.5px]" style={{ color: C.faint }}>Entry · sizing · stop · trailing · the full cycle.</span>
          </span>
          <span className="ml-auto shrink-0" style={{ color: C.faint }}>{ICON.arrow(13)}</span>
        </button>

        <div className="mt-3 text-[11px] uppercase tracking-[0.05em] mb-1" style={{ color: C.faint }}>Year-by-year</div>
        <YearByYearTable persona={persona} loading={loading} err={err} />

        <details className="group/r mt-3 border-t" style={{ borderColor: C.line }}>
          <summary className="list-none cursor-pointer flex items-center gap-2 py-2.5 select-none">
            <span className="text-[11.5px] font-semibold" style={{ color: C.muted }}>Risk &amp; how to read this</span>
            <span className="ml-auto transition-transform group-open/r:rotate-180" style={{ color: C.faint }}>{ICON.chevron(14)}</span>
          </summary>
          <p className="text-[10.5px] leading-snug pb-2" style={{ color: C.faint }}>{RISK_DISCLOSURE}</p>
        </details>
      </div>
    </details>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// TRADING-RULES SLIDE-OVER — the ONE place all rules content lives. In-flow
//   faux-overlay (absolute inset-0, NOT position:fixed) so the viewport-lock is
//   untouched. 100% STATIC documented strategy facts — no performance numbers.
// ════════════════════════════════════════════════════════════════════════════
function RulesSlideOver({
  perTrade, capital, onClose,
}: { perTrade: number; capital: number; onClose: () => void }) {
  return (
    <div className="absolute inset-0 z-40 flex justify-end">
      <button type="button" aria-label="Close trading rules" onClick={onClose}
              className="absolute inset-0" style={{ background: 'rgba(0,0,0,0.55)' }} />
      <div className="relative h-full w-full max-w-[520px] border-l flex flex-col shadow-[0_0_80px_-20px_rgba(0,0,0,0.8)]"
           style={{ background: C.panel, borderColor: C.line2 }}>
        <div className="shrink-0 flex items-center gap-2.5 px-5 py-3.5 border-b" style={{ borderColor: C.line }}>
          <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.book(15)}</span>
          <div className="min-w-0">
            <div className="text-[15px] font-semibold leading-tight" style={{ color: C.ink }}>How Falcon manages your money</div>
            <div className="text-[11px]" style={{ color: C.faint }}>Exactly how Falcon Top 10 Swing works.</div>
          </div>
          <button type="button" onClick={onClose} className="ml-auto shrink-0 grid place-items-center w-8 h-8 rounded-lg border"
                  style={{ borderColor: C.line2, color: C.muted }}>{ICON.close(15)}</button>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto px-5 py-4 flex flex-col gap-5 [scrollbar-width:thin]">
          <section>
            <RulesSectionHead n={1} title="The rules" />
            <div className="flex flex-col rounded-xl border overflow-hidden" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
              {VIRTUAL_RULES.map((r, i) => (
                <div key={r.title} className="flex items-start gap-2.5 px-3 py-2.5" style={i > 0 ? { borderTop: `1px solid ${C.line}` } : undefined}>
                  <span className="grid place-items-center w-6 h-6 rounded-md shrink-0 mt-px" style={{ background: C.mintDim, color: C.mint }}>{ICON[r.icon](12)}</span>
                  <div className="min-w-0">
                    <div className="text-[12.5px] font-semibold leading-tight" style={{ color: C.ink }}>{r.title}</div>
                    <div className="text-[11.5px] leading-snug mt-0.5" style={{ color: C.ink2 }}>{r.body}</div>
                  </div>
                </div>
              ))}
            </div>
          </section>
          <section>
            <RulesSectionHead n={2} title="Capital model — fixed allocation" />
            <CapitalModel perTrade={perTrade} capital={capital} />
          </section>
          <section>
            <RulesSectionHead n={3} title="What this trader does" />
            <WhatThisTraderDoes />
          </section>
          <section>
            <RulesSectionHead n={4} title="The trading cycle" />
            <TradingCycle />
          </section>
        </div>

        <div className="shrink-0 px-5 py-3 border-t" style={{ borderColor: C.line }}>
          <button type="button" onClick={onClose} className="w-full rounded-xl px-4 py-2 text-[13px] font-semibold" style={{ background: C.mint, color: '#06130c' }}>
            Got it
          </button>
        </div>
      </div>
    </div>
  )
}
function RulesSectionHead({ n, title }: { n: number; title: string }) {
  return (
    <div className="flex items-baseline gap-2 mb-2">
      <span className="grid place-items-center w-4 h-4 rounded-full text-[10px] font-mono font-semibold shrink-0" style={{ background: C.mintDim, color: C.mint }}>{n}</span>
      <h4 className="text-[12.5px] font-semibold" style={{ color: C.ink }}>{title}</h4>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// AUTOTRADE BRIDGE
// ════════════════════════════════════════════════════════════════════════════
function AutoTradeBridge({ onAutoTrade }: { onAutoTrade: () => void }) {
  return (
    <div className="rounded-2xl border p-4" style={{ borderColor: 'rgba(63,227,164,0.25)', background: 'rgba(63,227,164,0.05)' }}>
      <div className="flex items-center gap-2 mb-1.5">
        <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.bot(15)}</span>
        <h3 className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Ready to automate?</h3>
      </div>
      <p className="text-[12px] leading-[1.5] mb-3" style={{ color: C.ink2 }}>
        Connect your broker to automate eligible Falcon trades. AutoTrade places and manages these
        entries for you — once you&apos;ve seen the strategy work.
      </p>
      <button type="button" onClick={onAutoTrade}
              className="w-full inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold transition-colors"
              style={{ background: C.mint, color: '#06130c' }}>
        Connect a broker {ICON.arrow(13)}
      </button>
      <p className="text-[10px] mt-2 text-center" style={{ color: C.faint }}>AutoTrade is Launch-Pending.</p>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// MECHANISM STRIP — "How Co-Trading works" gear-train banner. STATIC explanatory
//   content (no numbers/P&L). Gently-rotating inline-SVG gears read as a running
//   machine: you set it ONCE, Kanida.AI then runs continuously. Compact, mint/F2.
//   `slim` renders a 1-line version for the RESULT header. Animation is disabled
//   under prefers-reduced-motion (scoped <style>; globals.css untouched).
// ════════════════════════════════════════════════════════════════════════════
const MECHANISM_STEPS: { you?: boolean; title: string; body: string }[] = [
  { you: true, title: 'You',       body: 'Choose your trading style + capital' },
  {            title: 'Kanida.AI', body: 'Picks the daily Top 10 stocks' },
  {            title: 'Kanida.AI', body: 'Decides entry, stop-loss & exit' },
  {            title: 'Kanida.AI', body: 'Reports performance' },
]

/** A single gently-rotating inline-SVG cog. `dir` flips spin so meshing reads. */
function Gear({ size = 22, dir = 1, color = C.mint, dim = false }: { size?: number; dir?: 1 | -1; color?: string; dim?: boolean }) {
  // 8-tooth cog built from a rounded gear path + center hub.
  const teeth = Array.from({ length: 8 }, (_, i) => i * 45)
  return (
    <span
      className="ct-gear inline-grid place-items-center shrink-0"
      style={{ width: size, height: size, animationDirection: dir === -1 ? 'reverse' : 'normal', opacity: dim ? 0.85 : 1 }}
    >
      <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="1.5" aria-hidden>
        {teeth.map(a => (
          <rect key={a} x="10.7" y="0.7" width="2.6" height="4.4" rx="0.8" fill={color} stroke="none"
                transform={`rotate(${a} 12 12)`} />
        ))}
        <circle cx="12" cy="12" r="7" fill="none" />
        <circle cx="12" cy="12" r="2.5" fill={color} stroke="none" />
      </svg>
    </span>
  )
}

const MECHANISM_CSS = `
@keyframes ct-gear-spin { to { transform: rotate(360deg); } }
.ct-gear { animation: ct-gear-spin 11s linear infinite; transform-origin: 50% 50%; }
@media (prefers-reduced-motion: reduce) { .ct-gear { animation: none; } }
`

function MechanismStrip({ onAutoTrade, slim = false }: { onAutoTrade: () => void; slim?: boolean }) {
  if (slim) {
    // 1-line version for the RESULT header — one running cog + the loop sentence.
    return (
      <div className="flex items-center gap-2 text-[11px]" style={{ color: C.muted }}>
        <style>{MECHANISM_CSS}</style>
        <Gear size={15} dir={1} />
        <Gear size={12} dir={-1} dim />
        <span>
          Set once · Falcon picks, enters, manages &amp; exits — <b style={{ color: C.mint }}>every trading day, automatically</b>.
        </span>
      </div>
    )
  }

  return (
    <div className="rounded-2xl border overflow-hidden"
         style={{ borderColor: 'rgba(63,227,164,0.22)', background: 'linear-gradient(180deg, rgba(63,227,164,0.06), rgba(255,255,255,0.015))' }}>
      <style>{MECHANISM_CSS}</style>

      {/* headline row: gear cluster + copy */}
      <div className="flex items-center gap-3 px-4 pt-3 pb-2.5">
        <div className="flex items-center -space-x-1 shrink-0">
          <Gear size={26} dir={1} />
          <Gear size={18} dir={-1} dim />
        </div>
        <div className="min-w-0">
          <div className="text-[13.5px] md:text-[14.5px] font-semibold leading-tight" style={{ color: C.ink }}>
            Set it once. Falcon runs the machine.
          </div>
          <div className="text-[11px] md:text-[11.5px] leading-snug mt-0.5" style={{ color: C.muted }}>
            You choose the style and capital — Kanida.AI handles the picks, entry, exits and reporting, every trading day.
          </div>
        </div>
      </div>

      {/* the 4-step gear-train — fills the full strip width responsively.
          Cells size to fit (grid, equal columns); arrows sit BETWEEN cells.
          The ↻ loop-back chip wraps to its own full-width line below so its
          label is never truncated/clipped. */}
      <div className="px-3 pb-3">
        <div className="grid grid-cols-2 sm:grid-cols-[1fr_auto_1fr_auto_1fr_auto_1fr] items-stretch gap-1.5">
          {MECHANISM_STEPS.map((s, i) => (
            <Fragment key={i}>
              <div className="flex flex-col rounded-xl border px-2.5 py-2 justify-center min-w-0"
                   style={{
                     borderColor: s.you ? 'rgba(63,227,164,0.5)' : C.line2,
                     background: s.you ? 'rgba(63,227,164,0.08)' : 'rgba(255,255,255,0.02)',
                   }}>
                <div className="flex items-center gap-1.5 mb-1">
                  {s.you
                    ? <span className="grid place-items-center w-4 h-4 rounded-md shrink-0" style={{ background: C.mint, color: '#06130c' }}>{ICON.user(10)}</span>
                    : <Gear size={14} dir={i % 2 === 0 ? 1 : -1} />}
                  <span className="text-[10px] font-semibold uppercase tracking-[0.05em]"
                        style={{ color: s.you ? C.mint : C.ink }}>{s.title}</span>
                  {s.you && (
                    <span className="ml-auto text-[7.5px] font-mono uppercase tracking-[0.06em] shrink-0" style={{ color: C.mint }}>input</span>
                  )}
                </div>
                <div className="text-[10.5px] leading-tight" style={{ color: s.you ? C.ink2 : C.muted }}>{s.body}</div>
              </div>
              {i < MECHANISM_STEPS.length - 1 && (
                <span className="hidden sm:flex items-center justify-center shrink-0 px-0.5" style={{ color: C.faint }}>{ICON.arrow(13)}</span>
              )}
            </Fragment>
          ))}
        </div>

        {/* loop-back: repeats continuously — own full-width line, never clipped */}
        <div className="flex items-center justify-center gap-2 mt-1.5 rounded-full border px-3 py-1.5"
             style={{ borderColor: 'rgba(63,227,164,0.32)', background: 'rgba(63,227,164,0.05)' }}>
          <span className="shrink-0 grid place-items-center w-5 h-5 rounded-full" style={{ color: C.mint }}>
            {ICON.loop(14)}
          </span>
          <span className="text-[10.5px] leading-tight text-center" style={{ color: C.muted }}>
            Repeats every trading day — <b style={{ color: C.mint }}>automatically</b>
          </span>
        </div>
      </div>

      {/* AutoTrade bridge line (compact, secondary) */}
      <button type="button" onClick={onAutoTrade}
              className="w-full flex items-center gap-1.5 px-4 py-2 border-t text-[11px] transition-colors text-left"
              style={{ borderColor: 'rgba(63,227,164,0.18)', color: C.muted, background: 'rgba(63,227,164,0.03)' }}>
        <span style={{ color: C.mint }}>{ICON.bot(13)}</span>
        AutoTrade does exactly this with your <b style={{ color: C.ink }}>real broker</b>
        <span className="ml-auto shrink-0" style={{ color: C.mint }}>{ICON.arrow(12)}</span>
      </button>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// PERSONA (REAL BACKTEST) HELPERS — narrow the typed summary/yearly/monthly.
// ════════════════════════════════════════════════════════════════════════════
function personaYearly(p: PersonaBacktestResponse | null): PersonaYearly[] {
  return (p?.yearly ?? []) as unknown as PersonaYearly[]
}
function personaMonthly(p: PersonaBacktestResponse | null): PersonaMonthly[] {
  return (p?.monthly ?? []) as unknown as PersonaMonthly[]
}
/** Winning months per year — DERIVED from the real monthly returns (count > 0). */
function winningMonths(months: PersonaMonthly[], year: number): { wins: number; total: number } {
  const ms = months.filter(m => m.year === year)
  return { wins: ms.filter(m => m.return_pct > 0).length, total: ms.length }
}
function pctTone(v: number | null | undefined): string {
  if (v == null) return C.faint
  return v >= 0 ? C.mint : C.red
}
function fmtPct(v: number | null | undefined, dp = 1): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`
}

// The operator-supplied RISK DISCLOSURE, shown VERBATIM as a static caveat.
const RISK_DISCLOSURE =
  'Risk disclosure: backtested on 2021–2026 data only. It has NOT been crash-tested ' +
  'against 2008, 2015 or 2020 conditions. 2021 is a small 15-trade sample. Returns ' +
  'include real intra-year drawdowns (e.g. −15.59% in 2025). Past simulated performance ' +
  'is not a guarantee of future results. This is virtual capital, not financial advice.'

// ════════════════════════════════════════════════════════════════════════════
// YEAR-BY-YEAR TABLE — REAL `yearly`; click a year → REAL months.
// ════════════════════════════════════════════════════════════════════════════
function YearByYearTable({
  persona, loading, err,
}: { persona: PersonaBacktestResponse | null; loading: boolean; err: boolean }) {
  const [openYear, setOpenYear] = useState<number | null>(null)
  const yearly  = personaYearly(persona)
  const monthly = personaMonthly(persona)

  if (loading) return <div className="py-4 text-[12px]" style={{ color: C.faint }}>Loading the real backtest…</div>
  if (err || !persona || yearly.length === 0)
    return <div className="py-4 text-[12px]" style={{ color: C.ink2 }}>No year-by-year backtest available right now.</div>

  return (
    <div className="mt-2">
      <div className="grid grid-cols-[0.7fr_1fr_1fr_1fr_0.8fr] gap-2 px-2 py-1.5 text-[10px] uppercase tracking-[0.04em]" style={{ color: C.faint }}>
        <span>Year</span><span className="text-right">Return</span><span className="text-right">Max DD</span>
        <span className="text-right">Win months</span><span className="text-right">Trades</span>
      </div>
      <div className="flex flex-col">
        {yearly.map(y => {
          const wm = winningMonths(monthly, y.year)
          const isOpen = openYear === y.year
          return (
            <div key={y.year} className="border-t" style={{ borderColor: C.line }}>
              <button type="button" onClick={() => setOpenYear(o => (o === y.year ? null : y.year))}
                      className="w-full grid grid-cols-[0.7fr_1fr_1fr_1fr_0.8fr] gap-2 px-2 py-2 items-center text-left">
                <span className="text-[12.5px] font-semibold flex items-center gap-1" style={{ color: C.ink }}>
                  <span style={{ color: C.faint, transition: 'transform .15s', transform: isOpen ? 'rotate(90deg)' : undefined }}>{ICON.chevronR(11)}</span>
                  {y.year}
                </span>
                <span className="text-right font-mono tabular-nums text-[12.5px]" style={{ color: pctTone(y.return_pct) }}>{fmtPct(y.return_pct)}</span>
                <span className="text-right font-mono tabular-nums text-[12.5px]" style={{ color: C.red }}>{fmtPct(y.max_dd_pct)}</span>
                <span className="text-right font-mono tabular-nums text-[12.5px]" style={{ color: C.ink2 }}>{wm.total ? `${wm.wins}/${wm.total}` : '—'}</span>
                <span className="text-right font-mono tabular-nums text-[12.5px]" style={{ color: C.ink2 }}>{y.n_closed}</span>
              </button>
              {isOpen && (
                <div className="px-2 pb-2.5">
                  <div className="rounded-lg border overflow-hidden" style={{ borderColor: C.line, background: 'rgba(255,255,255,0.02)' }}>
                    <div className="grid grid-cols-[1fr_1fr_1fr] gap-2 px-2.5 py-1.5 text-[9.5px] uppercase tracking-[0.04em] border-b" style={{ borderColor: C.line, color: C.faint }}>
                      <span>Month</span><span className="text-right">Return</span><span className="text-right">End equity</span>
                    </div>
                    {monthly.filter(m => m.year === y.year).sort((a, b) => a.month.localeCompare(b.month)).map(m => (
                      <div key={m.month} className="grid grid-cols-[1fr_1fr_1fr] gap-2 px-2.5 py-1.5 text-[11.5px] border-t" style={{ borderColor: C.line }}>
                        <span className="font-mono" style={{ color: C.muted }}>{m.month}</span>
                        <span className="text-right font-mono tabular-nums" style={{ color: pctTone(m.return_pct) }}>{fmtPct(m.return_pct)}</span>
                        <span className="text-right font-mono tabular-nums" style={{ color: C.ink2 }}>{fmtINR(m.end_equity)}</span>
                      </div>
                    ))}
                  </div>
                  <p className="text-[9.5px] mt-1.5" style={{ color: C.faint }}>
                    Per-position drawdown isn&apos;t in the monthly rows — month-level max DD is a Backend need.
                  </p>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// CAPITAL MODEL / WHAT THIS TRADER DOES / TRADING CYCLE — STATIC (rules slide-over).
// ════════════════════════════════════════════════════════════════════════════
function CapitalModel({ perTrade, capital }: { perTrade: number; capital: number }) {
  const lines = [
    'Fixed ₹ per trade — every pick gets the same allocation, regardless of rank.',
    'No intra-year compounding — profits do NOT raise the per-stock allocation mid-year.',
    'Each yearly backtest starts fresh from a ₹5,00,000 book.',
    'Your virtual capital scales that fixed allocation proportionally (₹50k per pick at ₹5 L).',
  ]
  return (
    <div className="mt-3 flex flex-col gap-1.5">
      {lines.map((l, i) => (
        <div key={i} className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
          <span className="mt-[6px] w-[5px] h-[5px] rounded-full shrink-0" style={{ background: C.mint }} />
          <span>{l}</span>
        </div>
      ))}
      <div className="mt-2 rounded-lg border px-3 py-2 text-[11.5px]" style={{ borderColor: C.line, background: 'rgba(63,227,164,0.04)', color: C.muted }}>
        At your <b style={{ color: C.ink }}>{fmtCapital(capital)}</b> capital that is{' '}
        <b style={{ color: C.mint }}>{fmtINR(perTrade)}</b> per pick × up to 10 names ={' '}
        <b style={{ color: C.ink }}>{fmtINR(perTrade * TOP_N)}</b> fully deployed.
      </div>
    </div>
  )
}
function WhatThisTraderDoes() {
  return (
    <p className="mt-3 text-[12.5px] leading-[1.6]" style={{ color: C.ink2 }}>
      This trader ranks every stock by <b style={{ color: C.ink }}>avg_lift</b> — the average edge a name shows per
      pattern fire (sum_lift ÷ pattern fires) — and only takes names that clear at least{' '}
      <b style={{ color: C.ink }}>10 confluent patterns</b>. It buys all ten at{' '}
      <b style={{ color: C.ink }}>₹50k each</b>, puts a <b style={{ color: C.ink }}>−7% stop-loss</b> on every
      position, lets winners run with a <b style={{ color: C.ink }}>+12% trailing stop</b>, and never holds past{' '}
      <b style={{ color: C.ink }}>7 trading days</b>. Simple, disciplined, repeatable.
    </p>
  )
}
function TradingCycle() {
  return (
    <ol className="mt-3 flex flex-col gap-2 m-0 p-0 list-none">
      {TRADING_CYCLE.map((step, i) => (
        <li key={i} className="flex items-start gap-2.5">
          <span className="grid place-items-center w-5 h-5 rounded-full text-[10px] font-mono font-semibold shrink-0 mt-px" style={{ background: C.mintDim, color: C.mint }}>{i + 1}</span>
          <span className="text-[12px] leading-snug" style={{ color: C.ink2 }}>{step}</span>
        </li>
      ))}
    </ol>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// Empty / replay-pending / style-pending states
// ════════════════════════════════════════════════════════════════════════════
function StylePendingCard({ style }: { style: Style }) {
  return (
    <div className="rounded-2xl border p-6 max-w-[640px] mx-auto" style={{ borderColor: C.line2, background: C.panel }}>
      <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.08em] rounded-md px-2 py-1 mb-3"
            style={{ color: C.mint, background: C.mintDim, border: '1px solid rgba(63,227,164,0.3)' }}>
        <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.mint }} />
        Launch Pending
      </span>
      <h3 className="text-[16px] font-semibold mb-2" style={{ color: C.ink }}>{style.name}</h3>
      <p className="text-[13px] leading-[1.6]" style={{ color: C.ink2 }}>
        A {style.hold} engine for this style isn&apos;t live yet — we won&apos;t allocate virtual capital against
        picks we haven&apos;t validated. Go back and choose <b style={{ color: C.ink }}>Falcon Top 10 Swing</b>.
      </p>
    </div>
  )
}
function NoPicksCard() {
  return (
    <div className="rounded-2xl border p-6 max-w-[640px] mx-auto" style={{ borderColor: C.line2, background: C.panel }}>
      <h3 className="text-[15px] font-semibold mb-2" style={{ color: C.ink }}>No live Top 10 to allocate</h3>
      <p className="text-[13px] leading-[1.6]" style={{ color: C.ink2 }}>
        We couldn&apos;t load today&apos;s Falcon Top 10 from the engine, so there&apos;s nothing to allocate yet.
        This is a live-data view — try again after the next end-of-day cycle.
      </p>
    </div>
  )
}
function ReplayPendingCard({ startDate }: { startDate: string }) {
  return (
    <div className="rounded-2xl border p-6 max-w-[640px] mx-auto" style={{ borderColor: 'rgba(230,180,80,0.3)', background: 'rgba(230,180,80,0.04)' }}>
      <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.08em] rounded-md px-2 py-1 mb-3"
            style={{ color: C.amber, background: 'rgba(230,180,80,0.12)' }}>
        replay · backend pending
      </span>
      <h3 className="text-[15px] font-semibold mb-2" style={{ color: C.ink }}>Replay for {startDate} isn&apos;t available yet</h3>
      <p className="text-[13px] leading-[1.6]" style={{ color: C.ink2 }}>
        The engine doesn&apos;t have point-in-time Top 10 picks for that date through this view. A full
        walk-forward replay — how Falcon would have managed your capital from that day, using only
        information available then (no look-ahead) — needs the Co-Trading simulation backend.
      </p>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// Formatting helpers
// ════════════════════════════════════════════════════════════════════════════
function perTradeFor(capital: number): number {
  return Math.max(0, (capital / BASE_CAPITAL) * BASE_PER_TRADE)
}
function fmtINR(v: number): string {
  if (!Number.isFinite(v)) return '—'
  return '₹' + Math.round(v).toLocaleString('en-IN')
}
function signedINR(v: number): string {
  if (!Number.isFinite(v)) return '—'
  return (v >= 0 ? '+' : '−') + '₹' + Math.abs(Math.round(v)).toLocaleString('en-IN')
}
function fmtNum(v: number): string {
  return v.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
function fmtCapital(v: number): string {
  if (v >= 1e7) return `₹${(v / 1e7).toFixed(v % 1e7 === 0 ? 0 : 1)} Cr`
  if (v >= 1e5) return `₹${(v / 1e5).toFixed(v % 1e5 === 0 ? 0 : 1)} L`
  return '₹' + v.toLocaleString('en-IN')
}
function istTodayISO(): string {
  const now = new Date()
  const istMs = now.getTime() + (now.getTimezoneOffset() + 330) * 60_000
  return new Date(istMs).toISOString().slice(0, 10)
}

// ── Inline icons (match AskFalconHome style) ─────────────────────────────────
const ICON = {
  flame:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3c1 3 4 4.5 4 8a4 4 0 0 1-8 0c0-1.5.6-2.5 1.2-3.3C9.8 9 11.5 8 12 3z" strokeLinejoin="round"/></svg>,
  clock:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2" strokeLinecap="round"/></svg>,
  bolt:    (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M13 3L5 13h6l-1 8 8-10h-6l1-8z" strokeLinejoin="round"/></svg>,
  trend:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M4 15l5-5 3 3 7-7" strokeLinecap="round" strokeLinejoin="round"/><path d="M16 6h4v4" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  shield:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3l7 3v5c0 4.5-3 8-7 10-4-2-7-5.5-7-10V6l7-3z" strokeLinejoin="round"/></svg>,
  chevron: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  chevronR:(n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M9 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  wallet:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="3" y="6" width="18" height="13" rx="2.5"/><path d="M16 12h3" strokeLinecap="round"/><path d="M3 9h13a2 2 0 0 1 2 2" /></svg>,
  arrow: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M5 12h14M13 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  back:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M15 18l-6-6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  info:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01" strokeLinecap="round"/></svg>,
  bot:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="4" y="8" width="16" height="11" rx="2.5"/><path d="M12 8V4.5M9 13h.01M15 13h.01" strokeLinecap="round"/><circle cx="12" cy="4" r="1"/></svg>,
  book:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v15H6.5A2.5 2.5 0 0 0 4 20.5z" strokeLinejoin="round"/><path d="M4 20.5A2.5 2.5 0 0 1 6.5 18H20" /><path d="M8 7.5h7M8 11h7" strokeLinecap="round"/></svg>,
  close: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9"><path d="M6 6l12 12M18 6L6 18" strokeLinecap="round"/></svg>,
  user:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="8" r="3.4"/><path d="M5.5 19a6.5 6.5 0 0 1 13 0" strokeLinecap="round"/></svg>,
  loop:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M4 12a8 8 0 0 1 13.5-5.8L20 8" strokeLinecap="round" strokeLinejoin="round"/><path d="M20 4v4h-4" strokeLinecap="round" strokeLinejoin="round"/><path d="M20 12a8 8 0 0 1-13.5 5.8L4 16" strokeLinecap="round" strokeLinejoin="round"/><path d="M4 20v-4h4" strokeLinecap="round" strokeLinejoin="round"/></svg>,
}
