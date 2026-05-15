/**
 * /power/today — authed, full top-100 picks.
 *
 * SSR with server-side auth check. List uses ExpandablePickRow (compact +
 * click-to-expand) so 100 rows render performantly. content-visibility:auto
 * on each row skips layout/paint when off-screen.
 *
 * Reuses PickCard for the expanded body — no inline pick rendering.
 */
import { PowerAPI, assertPickVersion, type Pick } from '@/lib/power-api'
import { requireSession } from '@/lib/power-auth'
import { ExpandablePickRow } from '@/components/power/ExpandablePickRow'

export const dynamic = 'force-dynamic'
export const revalidate = 0


export default async function TodayPage() {
  const { jwt } = await requireSession()

  let picks: Pick[] = []
  let signalDate: string | null = null
  let entryDate:  string | null = null
  let totalAvailable = 0
  let fetchError: string | null = null

  try {
    const r = await PowerAPI.todayFull(jwt)
    picks = r.picks
    signalDate = r.signal_date
    entryDate  = r.entry_date
    totalAvailable = r.total_available
    picks.forEach(assertPickVersion)
  } catch (e) {
    fetchError = e instanceof Error ? e.message : 'Failed to load today\'s picks.'
    console.error('[today] fetch failed:', e)
  }

  return (
    <div className="space-y-6">
      <header className="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <h1 className="text-2xl md:text-3xl font-bold">Today's picks</h1>
          <p className="text-sm text-neutral-400 mt-1">
            {signalDate ? (
              <>
                Engine emitted at{' '}
                <span className="font-mono text-neutral-300">{signalDate}</span> EOD.
                {entryDate && (
                  <>
                    {' '}Entry on <span className="font-mono text-neutral-300">{entryDate}</span>.
                  </>
                )}
              </>
            ) : (
              "Today's signal status pending."
            )}
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs text-neutral-500">
          {totalAvailable > 0 && (
            <span className="px-2 py-1 bg-neutral-900 border border-neutral-800 rounded font-mono">
              {totalAvailable} picks
            </span>
          )}
        </div>
      </header>

      {fetchError && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {fetchError} <span className="text-red-300/70">Refresh to retry.</span>
        </div>
      )}

      {!fetchError && picks.length === 0 && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center text-neutral-400">
          <p>The engine hasn't emitted today's signals yet.</p>
          <p className="text-xs text-neutral-500 mt-1">
            Picks land at 16:35 IST on every trading day.
          </p>
        </div>
      )}

      {picks.length > 0 && (
        <div className="space-y-1.5">
          {picks.map(p => (
            <ExpandablePickRow key={`${p.symbol}-${p.rank}`} pick={p} />
          ))}
        </div>
      )}

      <footer className="text-xs text-neutral-600 text-center pt-4 border-t border-neutral-900">
        Each row collapses by default. Tap a symbol to read the full story, patterns, and expected outcomes.
      </footer>
    </div>
  )
}
