/**
 * /power/today — Falcon Top 20 with 3-bucket institutional-grade explainability.
 *
 * Architecture:
 *   • Server component — auth gate (requireSession) + initial data fetch
 *   • Universe + sector filters read from URL searchParams (shareable)
 *   • Each pick is a <Top20Card>; top 3 expanded by default, rest collapsed
 *   • Bucket 2 honestly surfaces the bootstrap-period fallback
 *     (hit_rate = lifetime_baseline when historical outcomes haven't realised)
 *
 * The endpoint itself is public; the page-level requireSession is the v1
 * audience gate. Phase 1b swaps Google OAuth → email + invite secret code,
 * which is a separate task — the page contract here doesn't depend on the
 * auth method, only that a session exists.
 */
import { PowerAPI } from '@/lib/power-api'
import { requireSession } from '@/lib/power-auth'
import { Top20Card } from '@/components/power/Top20Card'
import { Top20Filters } from '@/components/power/Top20Filters'
import type { Top20Universe, Top20Response } from '@/lib/falcon-top20-types'

export const dynamic   = 'force-dynamic'
export const revalidate = 0

const ALLOWED_UNIVERSES = new Set<Top20Universe>([
  'all500', 'nifty50', 'nifty100', 'nifty200', 'fno',
])

type SearchParams = Promise<Record<string, string | string[] | undefined>>


export default async function TodayPage({
  searchParams,
}: { searchParams: SearchParams }) {
  // Auth (Phase 1b swaps Google OAuth → invite-code without changing this line)
  await requireSession()

  const sp = await searchParams
  const universe = pickUniverse(sp.universe)
  const sector   = pickStr(sp.sector)
  const reqDate  = pickDate(sp.signal_date)

  let data: Top20Response | null = null
  let fetchError: string | null = null
  try {
    data = await PowerAPI.falconTop20(universe, sector, reqDate)
  } catch (e) {
    fetchError = e instanceof Error ? e.message : 'Failed to load Top 20.'
    console.error('[/power/today] falconTop20 failed:', e)
  }

  const picks = data?.picks ?? []
  const sectorOptions = uniqueSortedSectors(picks.map(p => p.sector))

  return (
    <div className="space-y-6">
      <Header
        signalDate={data?.signal_date ?? null}
        entryDate={data?.entry_date ?? null}
        nPicks={picks.length}
        universe={universe}
        sector={sector}
        isLatest={data?.is_latest ?? true}
        latestDate={data?.latest_signal_date ?? null}
      />

      <Top20Filters
        universe={universe}
        sector={sector}
        sectorOptions={sectorOptions}
        signalDate={data?.signal_date ?? null}
        availableDates={data?.available_signal_dates ?? []}
        latestDate={data?.latest_signal_date ?? null}
      />

      {fetchError && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {fetchError} <span className="text-red-300/70">Refresh to retry.</span>
        </div>
      )}

      {!fetchError && picks.length === 0 && (
        <EmptyState universe={universe} sector={sector} signalDate={data?.signal_date ?? null} />
      )}

      {picks.length > 0 && (
        <div className="space-y-3 md:space-y-4">
          {picks.map(p => (
            <Top20Card
              key={`${p.symbol}-${p.rank}`}
              pick={p}
              defaultExpanded={p.rank <= 3}
            />
          ))}
        </div>
      )}

      {/* When viewing a past date, cross-link to the existing replay infra
          that already computes D+1/+3/+5/+10/+15 outcomes for every pick. */}
      {data?.signal_date && !data.is_latest && (
        <WhatHappenedNextLink signalDate={data.signal_date} nPicks={picks.length} />
      )}

      <Footer builtInMs={data?._built_in_ms} />
    </div>
  )
}

// ── Header ────────────────────────────────────────────────────────────────

function Header({
  signalDate, entryDate, nPicks, universe, sector, isLatest, latestDate,
}: {
  signalDate: string | null
  entryDate:  string | null
  nPicks:     number
  universe:   Top20Universe
  sector:     string | null
  isLatest:   boolean
  latestDate: string | null
}) {
  const universeLabel = UNIVERSE_LABELS[universe]
  return (
    <header className="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
      <div>
        <div className="flex items-center gap-2 flex-wrap">
          <h1 className="text-2xl md:text-3xl font-bold text-white">Falcon Top 10</h1>
          {!isLatest && (
            <span className="px-2 py-0.5 rounded bg-amber-400/[0.12] border border-amber-400/30 text-amber-200 text-[11px] uppercase tracking-wide font-semibold">
              historical view
            </span>
          )}
        </div>
        <p className="text-sm text-neutral-400 mt-1">
          {signalDate ? (
            <>
              Engine emitted at{' '}
              <span className="font-mono text-neutral-300">{signalDate}</span> EOD.
              {entryDate && (
                <>
                  {' '}Qualified for entry on{' '}
                  <span className="font-mono text-neutral-300">{entryDate}</span>.
                </>
              )}
              {!isLatest && latestDate && (
                <span className="ml-2 text-neutral-500">
                  (Latest available: <span className="font-mono">{latestDate}</span>)
                </span>
              )}
            </>
          ) : 'Signal status pending.'}
        </p>
      </div>
      <div className="flex items-center gap-2 text-xs text-neutral-500 flex-wrap">
        <span className="px-2 py-1 bg-neutral-900 border border-neutral-800 rounded font-mono">
          {nPicks} picks
        </span>
        <span className="px-2 py-1 bg-neutral-900 border border-neutral-800 rounded">
          {universeLabel}
        </span>
        {sector && (
          <span className="px-2 py-1 bg-neutral-900 border border-neutral-800 rounded">
            {sector}
          </span>
        )}
      </div>
    </header>
  )
}


/** Cross-link to /power/replay/[date] which already renders D+1/+3/+5/+10/+15
 *  outcomes for every pick. Avoids re-implementing the outcome view here. */
function WhatHappenedNextLink({ signalDate, nPicks }: { signalDate: string; nPicks: number }) {
  return (
    <section className="mt-6 p-4 md:p-5 bg-mint-400/[0.05] border border-mint-400/30 rounded-lg flex flex-col md:flex-row md:items-center md:justify-between gap-3">
      <div>
        <h3 className="text-sm md:text-base font-semibold text-mint-300">
          Curious how these {nPicks} picks actually played out?
        </h3>
        <p className="text-xs md:text-sm text-white/70 mt-1">
          The replay view computes D+1, D+3, D+5, D+10 and D+15 outcomes for every pick on this signal date.
        </p>
      </div>
      <a href={`/power/replay/${signalDate}`}
         className="shrink-0 px-4 py-2 bg-mint-400 text-neutral-950 rounded font-semibold text-sm hover:bg-mint-300 transition-colors">
        See realised outcomes {'→'}
      </a>
    </section>
  )
}

// ── Empty state ───────────────────────────────────────────────────────────

function EmptyState({
  universe, sector, signalDate,
}: { universe: Top20Universe; sector: string | null; signalDate: string | null }) {
  const universeLabel = UNIVERSE_LABELS[universe]
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center text-neutral-400">
      <p className="text-white/85 font-semibold">
        No qualifying picks in {universeLabel}{sector ? ` × ${sector}` : ''} on {signalDate ?? 'this date'}.
      </p>
      <p className="text-xs text-neutral-500 mt-2">
        Try widening the universe (e.g. All 500) or clearing the sector filter.
      </p>
    </div>
  )
}

// ── Footer ────────────────────────────────────────────────────────────────

function Footer({ builtInMs }: { builtInMs?: number }) {
  return (
    <footer className="text-xs text-neutral-600 text-center pt-4 border-t border-neutral-900 space-y-1">
      <p>
        Top 3 picks expanded by default. Click any header strip to expand/collapse the full 3-bucket explanation.
      </p>
      {typeof builtInMs === 'number' && (
        <p className="text-neutral-700 font-mono">built in {builtInMs}ms</p>
      )}
    </footer>
  )
}

// ── Helpers ───────────────────────────────────────────────────────────────

const UNIVERSE_LABELS: Record<Top20Universe, string> = {
  all500:   'All 500',
  nifty50:  'Nifty 50',
  nifty100: 'Nifty 100',
  nifty200: 'Nifty 200',
  fno:      'F&O',
}

function pickUniverse(raw: string | string[] | undefined): Top20Universe {
  const v = Array.isArray(raw) ? raw[0] : raw
  if (v && ALLOWED_UNIVERSES.has(v as Top20Universe)) return v as Top20Universe
  return 'all500'
}

function pickStr(raw: string | string[] | undefined): string | null {
  const v = Array.isArray(raw) ? raw[0] : raw
  return v && v.length > 0 ? v : null
}

function pickDate(raw: string | string[] | undefined): string | null {
  const v = Array.isArray(raw) ? raw[0] : raw
  if (!v) return null
  // Strict YYYY-MM-DD shape — backend regex also enforces, but reject early
  return /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : null
}

function uniqueSortedSectors(items: string[]): string[] {
  return [...new Set(items.filter(Boolean))].sort((a, b) => a.localeCompare(b))
}
