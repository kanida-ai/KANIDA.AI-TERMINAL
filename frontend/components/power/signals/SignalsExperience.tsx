'use client'

/**
 * SignalsExperience — the daily picks hub inside the AI-native shell.
 *
 * MIGRATION (2026-06-23): mounts the proven legacy surfaces inside the shell:
 *   • Tab "Today's Top 10" → reuses Top20Card + Top20Filters (the /power/today
 *     surface): 3-bucket explainability, universe/sector/date filters.
 *   • Tab "Live" → reuses CyclePicker + the ENTER/WAIT/SKIP decision buckets
 *     (the /power/live surface).
 *   • A footer link into Performance → replay outcomes for the viewed date.
 *
 * The heavy display components (Top20Card, Top20Filters, CyclePicker) are reused
 * verbatim — this is the shell wrapper that gives them the tab switch + the
 * viewport-lock scroll region (shrink-0 header + flex-1 min-h-0 overflow-y-auto).
 * No data is fabricated; everything is the real Top20Response / liveDecisions
 * payload fetched by the server page.
 */
import { useState, useEffect } from 'react'
import Link from 'next/link'
import { Top20Card } from '@/components/power/Top20Card'
import { Top20Filters } from '@/components/power/Top20Filters'
import { SignalTierBadge } from '@/components/power/PickCard'
import { CyclePicker } from '@/app/power/live/CyclePicker'
import type { Top20Response, Top20Universe, Top20Pick } from '@/lib/falcon-top20-types'
import { PowerAPI, type LiveCycle, type LiveDecision, type FalconRankedPick } from '@/lib/power-api'

type LivePayload = {
  cycle:       string | null
  computed_at: string | null
  degraded?:   boolean
  summary:     { enter: number; wait: number; skip: number; locked: number }
  decisions:   LiveDecision[]
} | null

type Props = {
  top20:        Top20Response | null
  top20Error:   string | null
  universe:     Top20Universe
  sector:       string | null
  live:         LivePayload
  liveError:    string | null
  liveCycle:    LiveCycle | 'latest'
}

type TabKey = 'today' | 'live'

export function SignalsExperience({
  top20, top20Error, universe, sector, live, liveError, liveCycle,
}: Props) {
  const [tab, setTab] = useState<TabKey>('today')

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* ── Fixed header (shrink-0) ── */}
      <header className="shrink-0 px-4 md:px-8 lg:px-10 pt-20 md:pt-8 pb-4 border-b border-neutral-900">
        <div className="max-w-5xl mx-auto w-full">
          <h1 className="text-2xl md:text-3xl font-semibold text-neutral-100">Signals</h1>
          <p className="text-sm text-neutral-400 mt-1 max-w-2xl leading-relaxed">
            Today&apos;s Falcon Top 10 with full explainability, the deeper ranked
            list (to 50) with signal-time tiers, plus the live
            9:30 / 9:45 / 10:00 IST ENTER / WAIT / SKIP overlay.
          </p>

          {/* Tab switch */}
          <div className="mt-4 inline-flex items-center gap-1 rounded-lg border border-neutral-800 bg-neutral-950/60 p-1">
            <TabButton active={tab === 'today'} onClick={() => setTab('today')}>
              Today&apos;s Top 50
            </TabButton>
            <TabButton active={tab === 'live'} onClick={() => setTab('live')}>
              Live decisions
              {live && live.summary.enter > 0 && (
                <span className="ml-1.5 inline-flex items-center justify-center rounded-full bg-green-500/20 text-green-200 text-[10px] font-mono px-1.5 py-0.5">
                  {live.summary.enter}
                </span>
              )}
            </TabButton>
          </div>
        </div>
      </header>

      {/* ── Scroll region (flex-1 min-h-0 overflow-y-auto) ── */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="max-w-5xl mx-auto w-full px-4 md:px-8 lg:px-10 pt-6 pb-16">
          {tab === 'today'
            ? <TodayTab top20={top20} top20Error={top20Error} universe={universe} sector={sector} />
            : <LiveTab live={live} liveError={liveError} liveCycle={liveCycle} />
          }
        </div>
      </div>
    </div>
  )
}

function TabButton({ active, onClick, children }: {
  active: boolean; onClick: () => void; children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={[
        'inline-flex items-center rounded-md px-3.5 py-1.5 text-sm font-semibold transition-colors',
        active
          ? 'bg-mint-400/10 text-mint-300'
          : 'text-neutral-400 hover:text-neutral-100 hover:bg-neutral-900',
      ].join(' ')}
    >
      {children}
    </button>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// TODAY tab — reuses Top20Card + Top20Filters (the /power/today surface)
// ─────────────────────────────────────────────────────────────────────────

function TodayTab({ top20, top20Error, universe, sector }: {
  top20: Top20Response | null; top20Error: string | null
  universe: Top20Universe; sector: string | null
}) {
  const picks = top20?.picks ?? []
  const sectorOptions = uniqueSortedSectors(picks.map(p => p.sector))

  // Deeper ranked list (ranks 11–50): rank + signal-time tier, no heavy
  // explainability. Same LOCKED persona ranking as the Top-10 above. Fetched
  // from the fast /today/falcon-ranked endpoint so it never blocks the page.
  const signalDate = top20?.signal_date ?? null
  const [ranked, setRanked] = useState<FalconRankedPick[]>([])
  useEffect(() => {
    if (!signalDate) { setRanked([]); return }
    const ctrl = new AbortController()
    let alive = true
    PowerAPI.falconRanked(universe, sector, 50, signalDate, ctrl.signal)
      .then(res => { if (alive) setRanked(res.picks ?? []) })
      .catch(() => { if (alive) setRanked([]) })
    return () => { alive = false; ctrl.abort() }
  }, [universe, sector, signalDate])
  const tail = ranked.filter(p => p.rank > picks.length)

  return (
    <div className="space-y-6">
      <Top20Filters
        universe={universe}
        sector={sector}
        sectorOptions={sectorOptions}
        signalDate={top20?.signal_date ?? null}
        availableDates={top20?.available_signal_dates ?? []}
        latestDate={top20?.latest_signal_date ?? null}
      />

      {top20?.signal_date && (
        <p className="text-sm text-neutral-400">
          Engine emitted at{' '}
          <span className="font-mono text-neutral-300">{top20.signal_date}</span> EOD.
          {top20.entry_date && (
            <>
              {' '}Qualified for entry on{' '}
              <span className="font-mono text-neutral-300">{top20.entry_date}</span>.
            </>
          )}
          {top20.is_latest === false && top20.latest_signal_date && (
            <span className="ml-2 text-neutral-500">
              (Latest: <span className="font-mono">{top20.latest_signal_date}</span>)
            </span>
          )}
        </p>
      )}

      {top20Error && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {top20Error} <span className="text-red-300/70">Refresh to retry.</span>
        </div>
      )}

      {!top20Error && picks.length === 0 && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center text-neutral-400">
          <p className="text-white/85 font-semibold">
            No qualifying picks{sector ? ` in ${sector}` : ''} on {top20?.signal_date ?? 'this date'}.
          </p>
          <p className="text-xs text-neutral-500 mt-2">
            Try widening the universe (e.g. All 500) or clearing the sector filter.
          </p>
        </div>
      )}

      {picks.length > 0 && picks.length < 10 && (
        <div role="status" className="px-4 py-3 rounded-lg bg-amber-400/[0.08] border border-amber-400/30 text-sm">
          <p className="text-amber-200">
            <span className="font-semibold">Low-signal day —</span>{' '}
            only <span className="font-mono">{picks.length}</span> stock{picks.length === 1 ? '' : 's'} passed today&apos;s
            high-confluence gate (normally 10). The engine is choosing not to dilute the list.
          </p>
        </div>
      )}

      {picks.length > 0 && (
        <div className="space-y-3 md:space-y-4">
          {picks.map(p => (
            <Top20Card key={`${p.symbol}-${p.rank}`} pick={p} defaultExpanded={p.rank <= 3} />
          ))}
        </div>
      )}

      {/* Deeper ranked list — ranks (Top-10 + 1) … 50, compact rows with the
          signal-time tier. The rich explainability above stays the locked
          Top-10; this is the "browse the full ranked set" surface. */}
      {tail.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-baseline justify-between">
            <h3 className="text-sm font-semibold text-neutral-200">
              More Falcon signals · ranks {picks.length + 1}–{picks.length + tail.length}
            </h3>
            <span className="text-xs text-neutral-500">
              same ranking, quick view
            </span>
          </div>
          <div className="rounded-xl border border-neutral-800 divide-y divide-neutral-800/70 overflow-hidden">
            {tail.map(p => (
              <TailRow key={`${p.symbol}-${p.rank}`} pick={p}
                       universe={universe} signalDate={signalDate} />
            ))}
          </div>
        </div>
      )}

      {/* Entry to replay outcomes (lives in Performance). */}
      {top20?.signal_date && (
        <ReplayLink signalDate={top20.signal_date} nPicks={picks.length} isLatest={top20.is_latest ?? true} />
      )}
    </div>
  )
}

// One expandable ranks-11–50 row. Collapsed = compact rank + tier + avg-lift.
// Expanded = fetch the FULL 3-bucket explainability for this one symbol
// (/today/falcon-explain, ~1s warm) and render it as a Top20Card, exactly like
// a Top-10 card. Lazy: nothing is fetched until the row is first opened.
function TailRow({ pick, universe, signalDate }: {
  pick: FalconRankedPick; universe: Top20Universe; signalDate: string | null
}) {
  const [open, setOpen] = useState(false)
  const [full, setFull] = useState<Top20Pick | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const toggle = () => {
    const next = !open
    setOpen(next)
    if (next && !full && !loading) {
      setLoading(true); setErr(null)
      PowerAPI.falconExplain(pick.symbol, universe, signalDate)
        .then(res => {
          const one = (res.picks ?? [])[0] ?? null
          if (one) setFull(one as Top20Pick)
          else setErr('No explainability available for this stock.')
        })
        .catch(() => setErr('Couldn’t load the full analysis. Try again.'))
        .finally(() => setLoading(false))
    }
  }

  return (
    <div>
      <button type="button" onClick={toggle} aria-expanded={open}
              className="w-full flex items-center gap-3 px-3 py-2 text-left hover:bg-neutral-900/60">
        <span className="w-7 shrink-0 text-right font-mono text-xs text-neutral-500 tabular-nums">
          #{pick.rank}
        </span>
        <span className="font-mono text-sm font-medium text-neutral-100 truncate max-w-[9rem]">
          {pick.symbol}
        </span>
        <SignalTierBadge tier={pick.signal_tier} color={pick.signal_tier_color}
                         reason={pick.signal_tier_reason} />
        {pick.sector && (
          <span className="hidden md:inline text-[11px] text-neutral-500 truncate">
            {pick.sector}
          </span>
        )}
        <span className="ml-auto shrink-0 font-mono text-xs text-neutral-400 tabular-nums">
          {pick.avg_lift != null ? `avg lift ${pick.avg_lift > 0 ? '+' : ''}${pick.avg_lift.toFixed(1)}%` : ''}
        </span>
        <span className="shrink-0 text-xs text-neutral-500 w-4 text-center">{open ? '▴' : '▾'}</span>
      </button>
      {open && (
        <div className="px-2 pb-3 pt-1">
          {loading && (
            <div className="px-3 py-4 text-sm text-neutral-400 animate-pulse">
              Loading full analysis for {pick.symbol}…
            </div>
          )}
          {err && !loading && (
            <div className="px-3 py-3 text-sm text-red-300">{err}</div>
          )}
          {full && !loading && (
            <Top20Card pick={full} defaultExpanded />
          )}
        </div>
      )}
    </div>
  )
}

function ReplayLink({ signalDate, nPicks, isLatest }: {
  signalDate: string; nPicks: number; isLatest: boolean
}) {
  return (
    <section className="mt-6 p-4 md:p-5 bg-mint-400/[0.05] border border-mint-400/30 rounded-lg flex flex-col md:flex-row md:items-center md:justify-between gap-3">
      <div>
        <h3 className="text-sm md:text-base font-semibold text-mint-300">
          {isLatest
            ? 'Want to see how past picks actually played out?'
            : `Curious how these ${nPicks} picks played out?`}
        </h3>
        <p className="text-xs md:text-sm text-white/70 mt-1">
          Performance replays D+1, D+3, D+5, D+10 and D+15 outcomes for every pick on any date.
        </p>
      </div>
      <Link
        href={`/power/performance?date=${encodeURIComponent(signalDate)}`}
        className="shrink-0 px-4 py-2 bg-mint-400 text-neutral-950 rounded font-semibold text-sm hover:bg-mint-300 transition-colors"
      >
        See realised outcomes →
      </Link>
    </section>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// LIVE tab — reuses CyclePicker + the ENTER/WAIT/SKIP buckets (/power/live)
// ─────────────────────────────────────────────────────────────────────────

function LiveTab({ live, liveError, liveCycle }: {
  live: LivePayload; liveError: string | null; liveCycle: LiveCycle | 'latest'
}) {
  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <p className="text-sm text-neutral-400 max-w-xl leading-relaxed">
          Pre-computed at <span className="font-mono text-neutral-300">09:30:30</span>,{' '}
          <span className="font-mono text-neutral-300">09:45:00</span>, and{' '}
          <span className="font-mono text-neutral-300">10:00:00 IST</span>.
          9:30 ENTER/SKIP decisions are <span className="text-mint-300 font-semibold">locked</span> —
          later cycles only re-evaluate WAITs.
        </p>
        <CyclePicker
          available={['0930', '0945', '1000', 'latest']}
          current={liveCycle}
          activeCycle={live?.cycle ?? null}
          computedAt={live?.computed_at ?? null}
        />
      </div>

      {liveError && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {liveError}
        </div>
      )}

      {live?.degraded && <DegradedBanner />}

      {live && live.decisions.length === 0 && !liveError && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center">
          <p className="text-neutral-300">No live decisions for the {liveCycle} cycle.</p>
          <p className="text-xs text-neutral-500 mt-2">
            Decisions are computed automatically at 9:30:30, 9:45, and 10:00 IST on trading days.
            If you&apos;re checking off-hours or before today&apos;s first cycle, this is expected.
          </p>
        </div>
      )}

      {live && live.decisions.length > 0 && (
        <>
          <SummaryBar summary={live.summary} />
          <DecisionBuckets decisions={live.decisions} />
        </>
      )}
    </div>
  )
}

function DegradedBanner() {
  return (
    <div role="status"
         className="px-4 py-3 rounded border border-mint-500/40 bg-mint-500/10 text-mint-100 text-sm flex items-start gap-3">
      <span aria-hidden className="text-mint-300 mt-0.5">⚠</span>
      <div className="space-y-1">
        <p className="font-semibold text-mint-200">Intraday decisions unavailable today</p>
        <p className="text-mint-100/90 leading-snug">
          Live broker connection is offline since this morning, so the 9:30 / 9:45 / 10:00 IST
          ENTER/WAIT/SKIP overlay can&apos;t be computed for today.
          <span className="font-semibold"> Your EOD picks remain valid</span> — see the
          Today&apos;s Top 10 tab for entries you can place manually. We&apos;ll be back online
          tomorrow at 06:30 IST.
        </p>
      </div>
    </div>
  )
}

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
  label: string; value: number; color: 'green' | 'yellow' | 'gray' | 'amber'; tooltip?: string
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
  accent: 'green' | 'yellow' | 'gray'
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
      <span className="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border font-mono">
        {d.tier}
      </span>
      <span className="text-xs text-neutral-500 font-mono text-right hidden md:block">
        {Math.round(d.score)}
      </span>
      <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-mono font-semibold border ${actionColor}`}
            title={d.decided_at_cycle ? `Decided at ${d.decided_at_cycle} IST` : ''}>
        {d.action}
        {d.decided_at_cycle && <span className="text-[9px] opacity-80">@{d.decided_at_cycle}</span>}
      </span>
    </div>
  )
}

function uniqueSortedSectors(items: string[]): string[] {
  return [...new Set(items.filter(Boolean))].sort((a, b) => a.localeCompare(b))
}
