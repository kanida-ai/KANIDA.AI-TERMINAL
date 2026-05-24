'use client'

/**
 * /power/replay/proof — public viewer for the result of 🎲 Random Replay.
 *
 * Why a separate route:
 *   /power/replay/[date] requires sign-in for non-featured dates per the
 *   operator's new gating rule (Sprint 5c re-test, design change 3).
 *   But Random Replay must REMAIN PUBLIC — it's the anti-cherry-pick proof
 *   for first-time visitors. We can't both auth-gate the date page and let
 *   anonymous users see a random replay through it.
 *
 * Flow:
 *   1. RandomReplayButton POSTs /api/power/picks/replay/random → full payload
 *   2. Button stores the payload in sessionStorage under RANDOM_KEY
 *   3. Button router.push('/power/replay/proof')
 *   4. This page reads sessionStorage, renders inline. If empty, redirects
 *      back to the landing CTA so refreshes don't dead-end.
 *
 * The payload key is namespaced + tied to a single page-load to avoid stale
 * data confusing a returning visitor.
 */
import { useEffect, useState } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { assertPickVersion, type ReplayPayload } from '@/lib/power-api'
import { ExpandablePickRow } from '@/components/power/ExpandablePickRow'
import { RandomReplayButton } from '@/components/power/RandomReplayButton'

export const RANDOM_REPLAY_KEY = 'kanida_random_replay_v1'

type Stage =
  | { kind: 'loading' }
  | { kind: 'empty' }
  | { kind: 'invalid' }
  | { kind: 'ready'; payload: ReplayPayload }

export default function ProofPage() {
  const router = useRouter()
  const [stage, setStage] = useState<Stage>({ kind: 'loading' })

  useEffect(() => {
    let raw: string | null = null
    try {
      raw = sessionStorage.getItem(RANDOM_REPLAY_KEY)
    } catch {
      // Private mode / storage disabled
    }
    if (!raw) { setStage({ kind: 'empty' }); return }

    try {
      const payload = JSON.parse(raw) as ReplayPayload
      payload.picks.forEach(assertPickVersion)
      setStage({ kind: 'ready', payload })
    } catch (e) {
      console.error('[replay/proof] invalid stored payload:', e)
      setStage({ kind: 'invalid' })
    }
  }, [router])

  if (stage.kind === 'loading') {
    return <div className="text-center text-neutral-400 py-20">Loading random replay…</div>
  }
  if (stage.kind === 'empty' || stage.kind === 'invalid') {
    return (
      <div className="max-w-md mx-auto py-16 text-center space-y-4">
        <h1 className="text-2xl font-bold">No random replay loaded</h1>
        <p className="text-sm text-neutral-400">
          {stage.kind === 'invalid'
            ? 'The stored replay data was unreadable — let&apos;s pick a new random day.'
            : 'Random replays load from the home page so we can rate-limit fairly.'}
        </p>
        <div className="flex flex-wrap justify-center gap-3 pt-2">
          <RandomReplayButton />
          <Link href="/power" className="px-4 py-2.5 border border-neutral-700 text-neutral-300 rounded-md hover:bg-neutral-900 transition-colors">
            ← Back to home
          </Link>
        </div>
      </div>
    )
  }

  return <RenderedReplay payload={stage.payload} />
}


function RenderedReplay({ payload }: { payload: ReplayPayload }) {
  const { aggregate, picks, replay_date, entry_date } = payload
  return (
    <div className="space-y-6">
      <header>
        <p className="text-sm text-mint-400 font-semibold uppercase tracking-wide">
          🎲 Random replay — anti-cherry-pick proof
        </p>
        <h1 className="text-2xl md:text-3xl font-bold mt-1">
          {replay_date}
          {entry_date && (
            <span className="text-base text-neutral-400 font-normal ml-3">
              entry {entry_date}
            </span>
          )}
        </h1>
        <p className="text-sm text-neutral-400 mt-2">
          The engine picked this day randomly from the last 2 years. {picks.length} picks below
          — every actual outcome shown is real history, not a simulation.
        </p>
      </header>

      <section className="grid grid-cols-2 md:grid-cols-5 gap-2 text-center text-xs">
        {(['d1','d3','d5','d10','d15'] as const).map(h => {
          const row = aggregate.horizons[h]
          if (!row) return null
          return (
            <div key={h} className="bg-neutral-900 border border-neutral-800 rounded p-2">
              <div className="text-[10px] uppercase tracking-wider text-neutral-500">{h.toUpperCase()}</div>
              <div className="font-mono text-sm text-neutral-100">
                WR {row.wr != null ? `${Math.round(row.wr)}%` : '—'}
              </div>
              <div className="font-mono text-xs text-mint-300">
                avg {row.avg_ret != null ? `${row.avg_ret >= 0 ? '+' : ''}${row.avg_ret.toFixed(1)}%` : '—'}
              </div>
            </div>
          )
        })}
      </section>

      <section className="space-y-1.5">
        {picks.map(p => (
          <ExpandablePickRow key={`${p.symbol}-${p.rank}`} pick={p} />
        ))}
      </section>

      <footer className="pt-4 border-t border-neutral-800 flex flex-wrap justify-between items-center gap-3">
        <RandomReplayButton />
        <Link href="/power/login" className="text-sm text-mint-400 hover:text-mint-300 underline">
          Sign in to see today&apos;s top 14 picks →
        </Link>
      </footer>
    </div>
  )
}
