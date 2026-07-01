'use client'

/**
 * AutoTradeExperience — the Kanida.AI AutoTrade flow. MIRRORS the Co-Trading
 * 2-stage flow + the gear "mechanism strip" so it feels like the same product
 * family — BUT with STRICT HONESTY because this is a REAL broker + REAL money.
 *
 * HARD SCOPE / SAFETY (money is involved):
 *   • AutoTrade for the USER'S account is NOT BUILT and is HIGH-RISK. This page
 *     is UX + LAUNCH-PENDING ONLY. It MUST NOT place or simulate a real order.
 *   • There is NO "execute" button. The primary action is
 *     "Join the AutoTrade waitlist / Notify me".
 *   • The broker-connect step is shown as the STRUCTURE, disabled / "soon".
 *   • The readiness checklist is shown as the structure with honest "pending".
 *   • "What Falcon would trade today" = today's REAL Top 10 (PowerAPI.falconTop20)
 *     clearly labelled "preview — not executing". No fabricated fills/P&L.
 *
 * Reuses the shared cotrade-kit (palette, icons, formatters, MechanismStrip,
 * tier-band colouring) + the shared PlanSwitcher — DRY with Co-Trading.
 */
import { useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { CompassLogo } from '@/components/power/CompassLogo'
import type { Top20Pick, Top20Response } from '@/lib/falcon-top20-types'
import { PlanSwitcher, AllPlansAggregate, ALL_PLANS, type Plan } from '@/components/power/shared/PlanSwitcher'
import {
  C, ICON, TIER_STYLE, BAND_COLORKEY, tierBand, MechanismStrip,
  fmtINR, fmtNum, fmtCapital,
} from '@/components/power/shared/cotrade-kit'
import { BrokerAccountsPanel } from '@/components/power/autotrade/BrokerAccountsPanel'

// ── Same fixed sizing model as Co-Trading (REAL math, no fabrication). ───────
const BASE_CAPITAL = 500_000
const BASE_PER_TRADE = 50_000
const TOP_N = 10
const STANDARD_SL_PCT = -7
const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000]

// ── Trading styles — only Swing is LIVE as an engine (rest Launch-Pending) ───
type StyleId = 'swing' | 'btst' | 'intraday' | 'weekly' | 'longterm'
type Style = { id: StyleId; name: string; hold: string; live: boolean }
const STYLES: Style[] = [
  { id: 'swing',    name: 'Falcon Top 10 Swing', hold: '~7 trading days', live: true  },
  { id: 'btst',     name: 'BTST',                hold: '1–2 days',        live: false },
  { id: 'intraday', name: 'Intraday',           hold: 'Same day',        live: false },
  { id: 'weekly',   name: 'Weekly Swing',       hold: '1–4 weeks',       live: false },
  { id: 'longterm', name: 'Long-Term',          hold: '3+ months',       live: false },
]

// ── The readiness checklist — STRUCTURE, with honest pending states. ─────────
type Readiness = { icon: keyof typeof ICON; title: string; body: string; state: 'pending' | 'soon' | 'ok' }

function entryPriceOf(p: Top20Pick): number | null {
  const v = p.action?.entry_price_rs
  return typeof v === 'number' && v > 0 ? v : null
}

type Props = { data: Top20Response | null; firstName: string; userId: number | string }
type Stage = 'setup' | 'preview'

export function AutoTradeExperience({ data, firstName, userId }: Props) {
  const router = useRouter()
  const [stage, setStage] = useState<Stage>('setup')
  const [styleId, setStyleId] = useState<StyleId>('swing')
  const [capital, setCapital] = useState<number>(500_000)
  const [waitlisted, setWaitlisted] = useState(false)

  const style = STYLES.find(s => s.id === styleId)!
  const perTrade = Math.max(0, (capital / BASE_CAPITAL) * BASE_PER_TRADE)

  // REAL preview rows — exactly what Falcon WOULD trade (not executing).
  const rows = useMemo(() => {
    if (!data || style.id !== 'swing') return []
    return data.picks.slice(0, TOP_N).map(p => {
      const entry = entryPriceOf(p)
      const qty = entry && perTrade > 0 ? Math.floor(perTrade / entry) : 0
      const cap = entry ? qty * entry : 0
      const slPctRaw = p.action?.stop_loss_pct
      const slPct = typeof slPctRaw === 'number' && slPctRaw !== 0 ? slPctRaw : STANDARD_SL_PCT
      const slPrice = entry ? +(entry * (1 + slPct / 100)).toFixed(2) : null
      return { pick: p, entry, qty, capital: cap, slPrice, slPct }
    })
  }, [data, style.id, perTrade])

  // Plan switcher — same structural model as Co-Trading (one live plan today).
  const [planView, setPlanView] = useState<string>('current')
  const plans: Plan[] = useMemo(() => [{
    id: 'current', styleId: style.id, styleName: style.name, capital,
    pnl: null, open: rows.length, live: style.live,
  }], [style.id, style.name, capital, rows.length, style.live])

  return (
    <div className="relative flex flex-col min-h-screen md:min-h-0 md:h-full md:overflow-hidden"
         style={{ background: C.canvas, color: C.ink }}>
      {stage === 'setup' ? (
        <SetupStage
          firstName={firstName} userId={userId} styleId={styleId} setStyleId={setStyleId}
          capital={capital} setCapital={setCapital} perTrade={perTrade}
          plans={plans}
          onAddStyle={(sid) => { const s = STYLES.find(x => x.id === sid); if (s?.live) setStyleId(s.id) }}
          onPreview={() => setStage('preview')}
          onCoTrade={() => router.push('/power/co-trading')}
        />
      ) : (
        <PreviewStage
          style={style} capital={capital} perTrade={perTrade} rows={rows} data={data}
          waitlisted={waitlisted} setWaitlisted={setWaitlisted}
          plans={plans} planView={planView} setPlanView={setPlanView}
          onAddStyle={(sid) => { const s = STYLES.find(x => x.id === sid); if (s?.live) { setStyleId(s.id); setPlanView('current') } }}
          onBack={() => setStage('setup')}
          onCoTrade={() => router.push('/power/co-trading')}
          onAnalyze={(sym) => router.push(`/power/ask?symbol=${encodeURIComponent(sym)}`)}
        />
      )}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 1 — SETUP (mirrors Co-Trading): style → capital → connect broker (soon)
//   → readiness checklist → "Join the AutoTrade waitlist".
// ════════════════════════════════════════════════════════════════════════════
function SetupStage({
  firstName, userId, styleId, setStyleId, capital, setCapital, perTrade,
  plans, onAddStyle, onPreview, onCoTrade,
}: {
  firstName: string; userId: number | string
  styleId: StyleId; setStyleId: (s: StyleId) => void
  capital: number; setCapital: (n: number) => void; perTrade: number
  plans: Plan[]; onAddStyle: (sid: string) => void
  onPreview: () => void; onCoTrade: () => void
}) {
  const READINESS: Readiness[] = [
    { icon: 'link',   title: 'Broker connected',  body: 'Securely link your broker account so Falcon can act on it.',   state: 'ok' },
    { icon: 'wallet', title: 'Funds available',   body: 'Enough cash/margin to place the day’s eligible entries.',     state: 'pending' },
    { icon: 'shield', title: 'Risk config',       body: 'Per-trade size, daily caps and the stop/trailing rules.',     state: 'pending' },
    { icon: 'clock',  title: 'Market hours',      body: 'Entries fire at 9:15 IST on trading days only.',              state: 'pending' },
  ]
  return (
    <div className="flex-1 min-h-0 md:overflow-y-auto [scrollbar-width:thin]">
      <div className="mx-auto w-full max-w-[1120px] px-5 md:px-8 py-5 md:py-6 flex flex-col gap-4 md:gap-5">

        {plans.length > 1 && (
          <div className="flex justify-center">
            <PlanSwitcher
              plans={plans} activeId={plans.find(p => p.styleId === styleId)?.id ?? 'current'}
              onSelect={(id) => { const p = plans.find(x => x.id === id); if (p) onAddStyle(p.styleId) }}
              addable={STYLES.map(s => ({ id: s.id, name: s.name, live: s.live }))}
              onAdd={onAddStyle} accent="amber"
            />
          </div>
        )}

        {/* heading */}
        <div className="flex flex-col items-center text-center gap-1.5">
          <CompassLogo size={28} />
          <h1 className="text-[21px] md:text-[24px] font-semibold tracking-[-0.02em]" style={{ color: C.ink }}>
            AutoTrade with Falcon, {firstName}
          </h1>
          <p className="text-[12.5px] md:text-[13px] leading-snug max-w-[600px]" style={{ color: C.muted }}>
            Falcon trades for you, <b style={{ color: C.ink }}>automatically</b>, with your real broker — picks,
            entries, stops, trailing and exits, hands-off.
          </p>
          {/* honesty banner — the FEATURE is launch-pending */}
          <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1 mt-0.5"
                style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.42)' }}>
            <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.amber }} />
            Launching soon for your account — nothing is traded yet
          </span>
        </div>

        {/* mechanism strip — AutoTrade variant + launch badge */}
        <MechanismStrip variant="autotrade" launching onBridge={onCoTrade} />

        {/* STEP 1 — choose style */}
        <Step n={1} title="Choose your trading style">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5">
            {STYLES.map(s => {
              const active = s.id === styleId
              return (
                <button key={s.id} type="button" disabled={!s.live}
                        onClick={() => { if (s.live) setStyleId(s.id) }}
                        className="relative text-left rounded-2xl border p-3 transition-colors disabled:cursor-not-allowed"
                        style={{
                          borderColor: active ? 'rgba(63,227,164,0.55)' : C.line2,
                          background: active ? 'rgba(63,227,164,0.07)' : 'rgba(255,255,255,0.02)',
                          opacity: s.live ? 1 : 0.6,
                        }}>
                  <div className="flex items-center gap-2 mb-1.5">
                    <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: C.mintDim, color: C.mint }}>{ICON.flame(15)}</span>
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
            Only <b style={{ color: C.muted }}>Falcon Top 10 Swing</b> is live today — the other engines are Launch-Pending.
          </p>
        </Step>

        {/* STEP 2 — capital */}
        <Step n={2} title="Set your capital">
          <div className="max-w-[520px]">
            <div className="flex items-center gap-2 rounded-xl px-3 py-2 border"
                 style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.03)' }}>
              <span className="text-[16px] font-semibold" style={{ color: C.muted }}>₹</span>
              <input type="number" min={10_000} step={10_000} value={capital}
                     onChange={e => setCapital(Math.max(0, Number(e.target.value) || 0))}
                     className="w-full bg-transparent outline-none text-[17px] font-mono font-semibold tabular-nums" style={{ color: C.ink }} />
              <span className="text-[11px] font-mono shrink-0" style={{ color: C.faint }}>{fmtCapital(capital)}</span>
            </div>
            <div className="flex items-center gap-1.5 mt-2 flex-wrap">
              {CAPITAL_PRESETS.map(v => (
                <button key={v} type="button" onClick={() => setCapital(v)}
                        className="text-[12px] rounded-full px-3 py-1 border transition-colors"
                        style={capital === v
                          ? { borderColor: 'rgba(63,227,164,0.5)', color: C.mint, background: 'rgba(63,227,164,0.08)' }
                          : { borderColor: C.line2, color: C.muted }}>{fmtCapital(v)}</button>
              ))}
            </div>
            <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
              Falcon would size <b style={{ color: C.muted }}>{fmtINR(perTrade)}</b> per pick (₹50k at ₹5 L, scaled). When live,
              this uses your real broker margin/cash.
            </p>
          </div>
        </Step>

        {/* STEP 3 — connect your broker (REAL, per-user connect flow) */}
        <Step n={3} title="Connect your broker">
          <div className="rounded-2xl border p-3 sm:p-4 mb-3 flex items-start gap-2.5"
               style={{ borderColor: 'rgba(63,227,164,0.3)', background: 'rgba(63,227,164,0.05)' }}>
            <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.shield(16)}</span>
            <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
              This connects your broker account only — it <b style={{ color: C.ink }}>never places a real order here</b>.
              Automated execution is <b style={{ color: C.mint }}>paper by default</b> and live trading stays gated
              server-side. Your API secret is sent once and stored encrypted; it&apos;s never shown again.
            </p>
          </div>
          <BrokerAccountsPanel userId={userId} />
        </Step>

        {/* READINESS CHECKLIST — structure with honest pending states */}
        <Step n={4} title="Readiness checklist">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
            {READINESS.map(r => <ReadinessRow key={r.title} r={r} />)}
          </div>
          <p className="text-[10.5px] mt-2" style={{ color: C.faint }}>
            This is the structure AutoTrade checks before every automated session. Broker connect is live above;
            automated live execution stays gated server-side (paper by default) until enabled for your account.
          </p>
        </Step>

        {/* primary action — NOT "execute". Waitlist + see the preview. */}
        <div className="flex flex-col items-center gap-2.5 mt-0.5">
          <button type="button" onClick={onPreview}
                  className="w-full max-w-[460px] inline-flex items-center justify-center gap-2 rounded-xl px-5 py-3 text-[15px] font-semibold transition-colors
                             shadow-[0_12px_32px_-12px_rgba(230,180,80,0.6)]"
                  style={{ background: C.amber, color: '#1a1205' }}>
            Join the AutoTrade waitlist {ICON.arrow(15)}
          </button>
          <p className="text-[11px] text-center max-w-[560px] leading-snug" style={{ color: C.muted }}>
            Once per-user automation is live, Falcon will auto-enter the daily Top 10 at <b style={{ color: C.ink }}>9:15 IST</b>,
            manage the stop-loss, trailing and exits, and report — all on your real broker, hands-off.
          </p>
          <p className="text-[10px] text-center max-w-[460px]" style={{ color: C.faint }}>
            No order is placed and nothing is simulated against your account. See exactly what Falcon would trade
            today on the next screen — preview only.
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

function ReadinessRow({ r }: { r: Readiness }) {
  const label = r.state === 'ok' ? 'ready' : r.state === 'soon' ? 'soon' : 'pending'
  const col = r.state === 'ok' ? C.mint : C.amber
  return (
    <div className="flex items-start gap-2.5 rounded-xl border px-3 py-2.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0 mt-px" style={{ background: C.mintDim, color: C.mint }}>{ICON[r.icon](14)}</span>
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-[12.5px] font-semibold leading-tight" style={{ color: C.ink }}>{r.title}</span>
          <span className="text-[8.5px] font-mono uppercase tracking-[0.06em] rounded px-1.5 py-0.5 shrink-0"
                style={{ color: col, background: 'rgba(230,180,80,0.1)', boxShadow: `inset 0 0 0 1px rgba(230,180,80,0.35)` }}>{label}</span>
        </div>
        <div className="text-[11px] leading-snug mt-0.5" style={{ color: C.ink2 }}>{r.body}</div>
      </div>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 2 — PREVIEW. "What Falcon would trade today" (REAL Top 10) + the
//   honest waitlist confirmation. NOTHING is executed.
// ════════════════════════════════════════════════════════════════════════════
type PreviewRow = {
  pick: Top20Pick; entry: number | null; qty: number; capital: number; slPrice: number | null; slPct: number
}

function PreviewStage({
  style, capital, perTrade, rows, data, waitlisted, setWaitlisted,
  plans, planView, setPlanView, onAddStyle, onBack, onCoTrade, onAnalyze,
}: {
  style: Style; capital: number; perTrade: number; rows: PreviewRow[]; data: Top20Response | null
  waitlisted: boolean; setWaitlisted: (v: boolean) => void
  plans: Plan[]; planView: string; setPlanView: (v: string) => void; onAddStyle: (sid: string) => void
  onBack: () => void; onCoTrade: () => void; onAnalyze: (s: string) => void
}) {
  const showAggregate = planView === ALL_PLANS
  return (
    <div className="flex-1 min-h-0 flex flex-col md:overflow-hidden">
      {/* FIXED header */}
      <div className="shrink-0 px-5 md:px-7 pt-2.5 md:pt-3 pb-3 border-b" style={{ borderColor: C.line }}>
        <div className="flex items-center gap-2.5 mb-2.5">
          <button type="button" onClick={onBack} className="grid place-items-center w-8 h-8 rounded-lg border shrink-0"
                  style={{ borderColor: C.line2, color: C.muted }} title="Back">{ICON.back(15)}</button>
          <CompassLogo size={22} />
          <div className="min-w-0">
            <div className="text-[18px] font-semibold tracking-[-0.02em] leading-tight" style={{ color: C.ink }}>AutoTrade</div>
            <div className="text-[11.5px] truncate" style={{ color: C.faint }}>
              {style.name} · {fmtCapital(capital)} · preview only — not executing
            </div>
          </div>
          <span className="ml-auto shrink-0 inline-flex items-center gap-1.5 text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1"
                style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.42)' }}>
            <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.amber }} />
            Launching soon
          </span>
        </div>

        <div className="mb-2.5">
          <PlanSwitcher plans={plans} activeId={showAggregate ? ALL_PLANS : 'current'}
                        onSelect={setPlanView}
                        addable={STYLES.map(s => ({ id: s.id, name: s.name, live: s.live }))}
                        onAdd={onAddStyle} accent="amber" />
        </div>

        <div className="hidden sm:block"><MechanismStrip variant="autotrade" slim launching /></div>
      </div>

      {/* SCROLLABLE body */}
      <div className="flex-1 min-h-0 md:overflow-y-auto px-5 md:px-7 pt-4 pb-8 [scrollbar-width:thin]">
        {showAggregate ? (
          <div className="flex flex-col gap-5 max-w-[1000px] mx-auto">
            <AllPlansAggregate plans={plans} accent="amber" />
            <WaitlistCard waitlisted={waitlisted} setWaitlisted={setWaitlisted} onCoTrade={onCoTrade} />
          </div>
        ) : !style.live ? (
          <StylePendingCard style={style} />
        ) : !data || rows.length === 0 ? (
          <NoPicksCard />
        ) : (
          <div className="flex flex-col gap-5 max-w-[1000px] mx-auto">
            <PreviewPortfolio rows={rows} perTrade={perTrade} data={data} onAnalyze={onAnalyze} />
            <WaitlistCard waitlisted={waitlisted} setWaitlisted={setWaitlisted} onCoTrade={onCoTrade} />
          </div>
        )}
      </div>
    </div>
  )
}

function PreviewPortfolio({
  rows, perTrade, data, onAnalyze,
}: { rows: PreviewRow[]; perTrade: number; data: Top20Response; onAnalyze: (s: string) => void }) {
  return (
    <div className="rounded-2xl border overflow-hidden" style={{ borderColor: C.line2, background: C.panel }}>
      <div className="px-4 py-3 border-b flex items-center gap-2 flex-wrap" style={{ borderColor: C.line }}>
        <h3 className="text-[15px] font-semibold" style={{ color: C.ink }}>What Falcon would trade today</h3>
        <span className="text-[10.5px] rounded-full px-2 py-0.5" style={{ color: C.amber, background: 'rgba(230,180,80,0.12)' }}>
          preview — not executing
        </span>
        <span className="ml-auto text-[11px]" style={{ color: C.faint }}>
          {fmtINR(perTrade)}/pick · ranked @ {data.signal_date ?? 'today'} EOD
        </span>
      </div>

      <div className="px-4 py-2 border-b flex items-center gap-2 text-[11.5px]"
           style={{ borderColor: C.line, color: C.muted, background: 'rgba(230,180,80,0.04)' }}>
        <span style={{ color: C.amber }}>{ICON.info(13)}</span>
        These are the REAL Top 10 Falcon would auto-enter at 9:15 once your account is live. <b style={{ color: C.ink }}>No order
        is placed</b> — this is a preview of the plan, not a fill.
      </div>

      <div className="p-3 grid grid-cols-1 sm:grid-cols-2 gap-2.5">
        {rows.map(r => <PreviewCard key={r.pick.symbol} r={r} onAnalyze={onAnalyze} />)}
      </div>
    </div>
  )
}

function PreviewCard({ r, onAnalyze }: { r: PreviewRow; onAnalyze: (s: string) => void }) {
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
                style={{ color: ts.color, background: ts.bg, boxShadow: `inset 0 0 0 1px ${ts.ring}` }}>{band}</span>
        )}
        {/* honest status — would-be, not a fabricated live state */}
        <span className="ml-auto shrink-0 text-[9px] font-semibold uppercase tracking-[0.05em] rounded-full px-2 py-0.5"
              style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
          Would auto-enter
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

function WaitlistCard({
  waitlisted, setWaitlisted, onCoTrade,
}: { waitlisted: boolean; setWaitlisted: (v: boolean) => void; onCoTrade: () => void }) {
  return (
    <div className="rounded-2xl border p-4" style={{ borderColor: 'rgba(230,180,80,0.3)', background: 'rgba(230,180,80,0.05)' }}>
      <div className="flex items-center gap-2 mb-1.5">
        <span className="grid place-items-center w-7 h-7 rounded-lg shrink-0" style={{ background: 'rgba(230,180,80,0.14)', color: C.amber }}>{ICON.bell(15)}</span>
        <h3 className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Be first when AutoTrade goes live</h3>
      </div>
      <p className="text-[12px] leading-[1.5] mb-3" style={{ color: C.ink2 }}>
        Per-user automation isn’t live yet. Join the waitlist and we’ll notify you the moment Falcon can place and
        manage these trades on your own broker — automatically.
      </p>
      {waitlisted ? (
        <div className="w-full inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold"
             style={{ background: 'rgba(63,227,164,0.12)', color: C.mint, boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
          {ICON.check(15)} You’re on the list — we’ll notify you
        </div>
      ) : (
        <button type="button" onClick={() => setWaitlisted(true)}
                className="w-full inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold transition-colors"
                style={{ background: C.amber, color: '#1a1205' }}>
          {ICON.bell(14)} Notify me when it’s live
        </button>
      )}
      <p className="text-[10px] mt-2 text-center" style={{ color: C.faint }}>
        Joining the waitlist places no order and connects no broker. Saving your interest server-side is a Backend need.
      </p>
      <button type="button" onClick={onCoTrade}
              className="w-full mt-3 inline-flex items-center justify-center gap-1.5 rounded-xl px-4 py-2 text-[12px] border transition-colors"
              style={{ borderColor: 'rgba(63,227,164,0.3)', color: C.mint }}>
        {ICON.bot(13)} Try it risk-free first with virtual capital in Co-Trading {ICON.arrow(12)}
      </button>
    </div>
  )
}

function StylePendingCard({ style }: { style: Style }) {
  return (
    <div className="rounded-2xl border p-6 max-w-[640px] mx-auto" style={{ borderColor: C.line2, background: C.panel }}>
      <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.08em] rounded-md px-2 py-1 mb-3"
            style={{ color: C.mint, background: C.mintDim, border: '1px solid rgba(63,227,164,0.3)' }}>
        <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.mint }} />Launch Pending
      </span>
      <h3 className="text-[16px] font-semibold mb-2" style={{ color: C.ink }}>{style.name}</h3>
      <p className="text-[13px] leading-[1.6]" style={{ color: C.ink2 }}>
        An engine for this style isn’t live yet — go back and choose <b style={{ color: C.ink }}>Falcon Top 10 Swing</b>.
      </p>
    </div>
  )
}

function NoPicksCard() {
  return (
    <div className="rounded-2xl border p-6 max-w-[640px] mx-auto" style={{ borderColor: C.line2, background: C.panel }}>
      <h3 className="text-[15px] font-semibold mb-2" style={{ color: C.ink }}>No live Top 10 to preview</h3>
      <p className="text-[13px] leading-[1.6]" style={{ color: C.ink2 }}>
        We couldn’t load today’s Falcon Top 10 from the engine, so there’s nothing to preview yet. This is a
        live-data view — try again after the next end-of-day cycle.
      </p>
    </div>
  )
}
