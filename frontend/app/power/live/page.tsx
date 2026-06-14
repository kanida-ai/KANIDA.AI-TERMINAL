/**
 * /power/live — ENTER / WAIT / SKIP decisions from the live tier scheduler.
 *
 * Reads pre-computed rows from falcon_live_decisions (set by scheduler at
 * 09:30:30 / 09:45 / 10:00 IST). Sub-200ms response — no Kite call per request.
 *
 * Lock semantics surfaced:
 *   decision.decided_at_cycle tells the UI WHEN the action was finalised.
 *   If a row in cycle='0945' has decided_at_cycle='0930', the UI shows the
 *   decision was LOCKED at 9:30 — the trader can trust it didn't flip.
 */
import { PowerAPI, PowerAPIError, type LiveCycle, type LiveDecision } from '@/lib/power-api'
import { requireSession } from '@/lib/power-auth'
import { PaywallNotice } from '@/components/power/PaywallNotice'
import { CyclePicker } from './CyclePicker'

export const dynamic = 'force-dynamic'
export const revalidate = 0

type SearchParams = Promise<{ cycle?: string }>

export default async function LivePage({ searchParams }: { searchParams: SearchParams }) {
  const { jwt } = await requireSession()
  const sp = await searchParams
  const requestedCycle = (sp.cycle as LiveCycle | 'latest' | undefined) ?? 'latest'
  const validCycle: LiveCycle | 'latest' =
    requestedCycle === '0930' || requestedCycle === '0945' || requestedCycle === '1000'
      ? requestedCycle
      : 'latest'

  let data: Awaited<ReturnType<typeof PowerAPI.liveDecisions>> | null = null
  let fetchError: string | null = null
  let paywallCheckoutUrl: string | null | undefined = undefined  // undefined = not paywalled

  try {
    data = await PowerAPI.liveDecisions(jwt, validCycle)
  } catch (e) {
    if (e instanceof PowerAPIError && e.isPaymentRequired()) {
      paywallCheckoutUrl = e.checkoutUrl()
    } else {
      fetchError = e instanceof Error ? e.message : 'Failed to load live decisions.'
    }
  }

  if (paywallCheckoutUrl !== undefined) {
    return <PaywallNotice feature="the live overlay" checkoutUrl={paywallCheckoutUrl} />
  }

  return (
    <div className="space-y-6">
      <header className="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <h1 className="text-2xl md:text-3xl font-bold">Live decisions</h1>
          <p className="text-sm text-neutral-400 mt-1">
            Pre-computed at <span className="font-mono text-neutral-300">09:30:30</span>,{' '}
            <span className="font-mono text-neutral-300">09:45:00</span>, and{' '}
            <span className="font-mono text-neutral-300">10:00:00 IST</span>.
            9:30 ENTER/SKIP decisions are <span className="text-mint-300 font-semibold">locked</span> —
            later cycles only re-evaluate WAITs.
          </p>
        </div>
        <CyclePicker
          available={['0930','0945','1000','latest']}
          current={validCycle}
          activeCycle={data?.cycle ?? null}
          computedAt={data?.computed_at ?? null}
        />
      </header>

      {fetchError && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200
                                       border border-red-500/40 text-sm">
          {fetchError}
        </div>
      )}

      {data?.degraded && (
        <DegradedBanner />
      )}

      {data && data.decisions.length === 0 && !fetchError && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center">
          <p className="text-neutral-300">No live decisions for the {validCycle} cycle.</p>
          <p className="text-xs text-neutral-500 mt-2">
            Decisions are computed automatically at 9:30:30, 9:45, and 10:00 IST on trading days.
            If you're checking off-hours or before today's first cycle, this is expected.
          </p>
        </div>
      )}

      {data && data.decisions.length > 0 && (
        <>
          <SummaryBar summary={data.summary} />
          <DecisionBuckets decisions={data.decisions} />
        </>
      )}
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Degraded banner — shown when Zerodha auto-auth failed past 09:30 IST.
// EOD picks stay valid; only the intraday ENTER/WAIT/SKIP overlay is down.
// ─────────────────────────────────────────────────────────────────────────

function DegradedBanner() {
  return (
    <div role="status"
         className="px-4 py-3 rounded border border-mint-500/40 bg-mint-500/10
                    text-mint-100 text-sm flex items-start gap-3">
      <span aria-hidden className="text-mint-300 mt-0.5">⚠</span>
      <div className="space-y-1">
        <p className="font-semibold text-mint-200">
          Intraday decisions unavailable today
        </p>
        <p className="text-mint-100/90 leading-snug">
          Live broker connection is offline since this morning, so the 9:30 /
          9:45 / 10:00 IST ENTER/WAIT/SKIP overlay can't be computed for today.
          <span className="font-semibold"> Your EOD picks remain valid</span> —
          see{' '}
          <a href="/power/today" className="underline hover:text-amber-50">
            today's picks
          </a>{' '}
          for entries you can place manually. We'll be back online tomorrow at 06:30 IST.
        </p>
      </div>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Summary band — ENTER / WAIT / SKIP / locked counts
// ─────────────────────────────────────────────────────────────────────────

function SummaryBar({ summary }: { summary: { enter: number; wait: number; skip: number; locked: number } }) {
  return (
    <section className="grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
      <Stat label="ENTER" value={summary.enter} color="green" />
      <Stat label="WAIT"  value={summary.wait}  color="yellow" />
      <Stat label="SKIP"  value={summary.skip}  color="gray" />
      <Stat label="Locked" value={summary.locked} color="amber"
            tooltip="Decisions copied forward from an earlier cycle (will not flip)" />
    </section>
  )
}

function Stat({ label, value, color, tooltip }: {
  label: string; value: number; color: 'green'|'yellow'|'gray'|'amber'; tooltip?: string
}) {
  const cls = {
    green:  'text-green-300 border-green-500/30',
    yellow: 'text-yellow-300 border-yellow-500/30',
    gray:   'text-neutral-300 border-neutral-700',
    amber:  'text-mint-300 border-mint-500/30',
  }[color]
  return (
    <div title={tooltip} className={`bg-neutral-900 border rounded p-3 ${cls}`}>
      <div className="text-[10px] tracking-wider uppercase text-neutral-500">{label}</div>
      <div className="text-2xl font-bold font-mono">{value}</div>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Buckets — ENTER first (actionable), then WAIT, then SKIP
// ─────────────────────────────────────────────────────────────────────────

function DecisionBuckets({ decisions }: { decisions: LiveDecision[] }) {
  const enters = decisions.filter(d => d.action === 'ENTER')
  const waits  = decisions.filter(d => d.action === 'WAIT')
  const skips  = decisions.filter(d => d.action === 'SKIP')
  return (
    <div className="space-y-6">
      <Bucket title="ENTER" subtitle="Open these on Kite — rule passed + decision finalised"
              accent="green" rows={enters} />
      <Bucket title="WAIT" subtitle="Needs more intraday confirmation; check next cycle"
              accent="yellow" rows={waits} />
      <Bucket title="SKIP" subtitle="Engine says pass — rule did not pass at the cutoff"
              accent="gray" rows={skips} collapsed />
    </div>
  )
}

function Bucket({ title, subtitle, accent, rows, collapsed }: {
  title: string; subtitle: string
  accent: 'green'|'yellow'|'gray'
  rows: LiveDecision[]
  collapsed?: boolean
}) {
  if (rows.length === 0) return null
  const accentCls = {
    green:  'border-green-500/20',
    yellow: 'border-yellow-500/20',
    gray:   'border-neutral-700',
  }[accent]
  return (
    <details open={!collapsed} className={`border-l-2 pl-4 ${accentCls}`}>
      <summary className="cursor-pointer list-none flex items-baseline justify-between mb-3">
        <div>
          <h2 className="text-base md:text-lg font-bold text-neutral-100">
            {title} <span className="text-neutral-500 font-mono text-sm ml-1">({rows.length})</span>
          </h2>
          <p className="text-xs text-neutral-500">{subtitle}</p>
        </div>
      </summary>
      <div className="space-y-1.5">
        {rows.map(d => <DecisionRow key={`${d.symbol}-${d.rank}`} d={d} />)}
      </div>
    </details>
  )
}


function DecisionRow({ d }: { d: LiveDecision }) {
  const actionColor = {
    ENTER: 'bg-green-500/20 text-green-200 border-green-500/50',
    WAIT:  'bg-yellow-500/20 text-yellow-200 border-yellow-500/50',
    SKIP:  'bg-neutral-700/40 text-neutral-300 border-neutral-600',
  }[d.action]
  const isLocked = d.decided_at_cycle && d.decided_at_cycle !== '1000'  // shown after the row's display

  return (
    <div className="grid grid-cols-[2rem_minmax(0,1fr)_auto_auto] md:grid-cols-[2rem_minmax(0,1fr)_8rem_5rem_auto] gap-3 items-center
                     bg-neutral-900 border border-neutral-800 rounded px-3 py-2 text-sm">
      <span className="text-xs text-neutral-500 font-mono">#{d.rank}</span>
      <div className="min-w-0">
        <div className="flex items-baseline gap-2">
          <span className="font-mono font-medium text-neutral-100">{d.symbol}</span>
          {d.sector && <span className="text-xs text-neutral-500 truncate">{d.sector}</span>}
        </div>
        <p className="text-xs text-neutral-500 truncate">{d.reason}</p>
      </div>
      <span className={`inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border font-mono`}>
        {d.tier}
      </span>
      <span className="text-xs text-neutral-500 font-mono text-right hidden md:block">
        {Math.round(d.score)}
      </span>
      <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-mono font-semibold border ${actionColor}`}
            title={d.decided_at_cycle ? `Decided at ${d.decided_at_cycle} IST` : ''}>
        {d.action}
        {d.decided_at_cycle && (
          <span className="text-[9px] opacity-80">@{d.decided_at_cycle}</span>
        )}
      </span>
    </div>
  )
}
