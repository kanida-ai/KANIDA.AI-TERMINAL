/**
 * /power/replay/[date] — full historical replay view.
 *
 * Public for the 3 featured dates (no auth wall — investor demo critical).
 * Authed users can replay any date.
 *
 * Renders:
 *   - Header: date, title (for featured), hook, aggregate WR/avg per horizon
 *   - List: every pick via ExpandablePickRow (with actual outcomes)
 *   - Footer: date picker (authed) + 🎲 random button
 */
import Link from 'next/link'
import { redirect } from 'next/navigation'
import { PowerAPI, assertPickVersion, PowerAPIError } from '@/lib/power-api'
import { getSessionJWT } from '@/lib/power-auth'
import { ExpandablePickRow } from '@/components/power/ExpandablePickRow'
import { RandomReplayButton } from '@/components/power/RandomReplayButton'
import { DateNavigator } from './DateNavigator'

export const dynamic = 'force-dynamic'
export const revalidate = 0

type Props = {
  params: Promise<{ date: string }>
}

const HORIZON_LABEL: Record<string, string> = {
  d1:  'D+1', d3:  'D+3', d5:  'D+5', d10: 'D+10', d15: 'D+15',
}

export default async function ReplayPage({ params }: Props) {
  const { date } = await params
  const jwt = await getSessionJWT()

  let payload: Awaited<ReturnType<typeof PowerAPI.replayForDate>> | null = null
  let needsAuth = false
  let fetchError: string | null = null

  try {
    payload = await PowerAPI.replayForDate(date, jwt)
    payload.picks.forEach(assertPickVersion)
  } catch (e) {
    if (e instanceof PowerAPIError) {
      if (e.isUnauthorized()) {
        needsAuth = true
      } else {
        fetchError = e.message || 'Replay unavailable.'
      }
    } else {
      fetchError = 'Network error — refresh to retry.'
    }
  }

  // Custom (non-featured) date + not authed → bounce to login
  if (needsAuth) {
    redirect(`/power/login?from=replay-${encodeURIComponent(date)}`)
  }

  if (fetchError) {
    return (
      <div className="max-w-md mx-auto py-12 text-center">
        <h1 className="text-2xl font-bold mb-2">Replay unavailable</h1>
        <p className="text-sm text-neutral-400 mb-6">{fetchError}</p>
        <Link href="/power" className="text-amber-400 hover:text-amber-300 underline">
          ← Back to home
        </Link>
      </div>
    )
  }

  if (!payload) {
    return <div className="text-center text-neutral-400 py-20">Loading replay…</div>
  }

  const { aggregate, picks, is_featured, title, hook, entry_date } = payload

  return (
    <div className="space-y-6">
      {/* Header */}
      <header>
        <div className="flex items-baseline justify-between flex-wrap gap-2">
          <div>
            {is_featured && title && (
              <p className="text-sm text-amber-400 font-semibold uppercase tracking-wide">
                Featured replay
              </p>
            )}
            <h1 className="text-2xl md:text-3xl font-bold">
              {title || `Replay — ${date}`}
            </h1>
            <p className="mt-1 text-sm text-neutral-400 font-mono">
              Signal {date}{entry_date && <> · Entry {entry_date}</>} · {aggregate.n_picks} picks
            </p>
          </div>
          <Link href="/power" className="text-sm text-neutral-400 hover:text-neutral-200">
            ← All featured
          </Link>
        </div>
        {hook && (
          <p className="mt-3 text-neutral-300 text-sm md:text-base">{hook}</p>
        )}
      </header>

      {/* Aggregate band */}
      {aggregate.n_picks > 0 && (
        <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
          <h2 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-3">
            What actually happened across all {aggregate.n_picks} picks
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-sm">
            {(['d1','d3','d5','d10','d15'] as const).map(h => {
              const v = aggregate.horizons[h]
              if (!v) return (
                <div key={h} className="text-center">
                  <div className="text-xs text-neutral-500 mb-1">{HORIZON_LABEL[h]}</div>
                  <div className="text-neutral-600">—</div>
                </div>
              )
              return (
                <div key={h} className="text-center">
                  <div className="text-xs text-neutral-500 mb-1">{HORIZON_LABEL[h]}</div>
                  <div className="text-base md:text-lg font-bold font-mono text-green-300">
                    {v.wr.toFixed(0)}%
                  </div>
                  <div className="text-[10px] text-neutral-500">
                    avg {v.avg_ret >= 0 ? '+' : ''}{v.avg_ret.toFixed(2)}%
                  </div>
                </div>
              )
            })}
          </div>
        </section>
      )}

      {/* Pick list */}
      {picks.length > 0 ? (
        <div className="space-y-1.5">
          {picks.map(p => (
            <ExpandablePickRow key={`${p.symbol}-${p.rank}`} pick={p} />
          ))}
        </div>
      ) : (
        <p className="text-center text-neutral-400 py-12">
          No picks for this date.
        </p>
      )}

      {/* Navigator footer */}
      <footer className="pt-6 border-t border-neutral-900 flex flex-col md:flex-row items-stretch md:items-center justify-between gap-4">
        <DateNavigator currentDate={date} authed={jwt !== null} />
        <RandomReplayButton />
      </footer>
    </div>
  )
}
