'use client'

/**
 * RandomReplayButton — the anti-cherry-pick proof button.
 *
 * Investor hits 🎲 → backend uniform-picks any trading day from last 2yr →
 * shows real engine output for that date.
 *
 * Rate-limit UX (per operator spec point #4):
 *   Anonymous: 3/hr per IP-hash. 4th click → 429.
 *   We render a friendly explanation (not just "429 Too Many Requests"):
 *     "You've hit our anonymous limit (3 random replays per hour).
 *      Sign in for unlimited, or come back in <retry_after>."
 *   Plus a sign-in CTA link.
 */
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import Link from 'next/link'
import { PowerAPI, PowerAPIError } from '@/lib/power-api'

// Must match the key /power/replay/proof reads from. Kept here as a string
// (not imported) so this client bundle doesn't pull the proof page module.
const RANDOM_REPLAY_KEY = 'kanida_random_replay_v1'

export function RandomReplayButton() {
  const router = useRouter()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<{ msg: string; isRateLimited: boolean } | null>(null)

  const onClick = async () => {
    setError(null)
    setLoading(true)
    try {
      const payload = await PowerAPI.replayRandom()
      // Sprint 5c re-test: the random-replay destination must stay PUBLIC,
      // but /power/replay/[date] now auth-gates non-featured dates. We
      // sidestep that by handing the already-fetched payload to a dedicated
      // client-only viewer via sessionStorage.
      try {
        sessionStorage.setItem(RANDOM_REPLAY_KEY, JSON.stringify(payload))
      } catch {
        // Private mode / storage disabled — fall back to the legacy route;
        // anon users will see the sign-in gate. Better than a silent no-op.
        router.push(`/power/replay/${encodeURIComponent(payload.replay_date)}`)
        return
      }
      router.push('/power/replay/proof')
    } catch (e) {
      if (e instanceof PowerAPIError) {
        if (e.isRateLimited()) {
          setError({
            msg: e.message || "You've hit our anonymous limit. Sign in for unlimited access.",
            isRateLimited: true,
          })
        } else {
          setError({ msg: e.message || 'Random replay unavailable. Try again in a moment.',
                      isRateLimited: false })
        }
      } else {
        setError({ msg: 'Network error — check your connection.', isRateLimited: false })
      }
      setLoading(false)
    } finally {
      // success path: navigation unmounts us; loading stays true until then.
      // failure path handled in catch.
    }
  }

  return (
    <div className="space-y-2">
      <button
        type="button"
        onClick={onClick}
        disabled={loading}
        className={[
          'inline-flex items-center gap-2 px-4 py-2.5 rounded-md font-semibold',
          'bg-mint-400 text-neutral-950 hover:bg-mint-300',
          'disabled:opacity-50 disabled:cursor-wait transition-colors',
        ].join(' ')}
      >
        {loading
          ? <><Spinner /> Picking a random day…</>
          : <><span className="text-lg">🎲</span> Show me a random day</>}
      </button>

      {error && (
        <div
          role="alert"
          className={[
            'text-xs px-3 py-2 rounded border',
            error.isRateLimited
              ? 'bg-yellow-500/10 text-yellow-200 border-yellow-500/40'
              : 'bg-red-500/10 text-red-200 border-red-500/40',
          ].join(' ')}
        >
          {error.msg}
          {error.isRateLimited && (
            <>
              {' '}
              <Link href="/power/login" className="underline font-semibold text-mint-300">
                Sign in for unlimited →
              </Link>
            </>
          )}
        </div>
      )}
    </div>
  )
}

function Spinner() {
  return (
    <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none" aria-hidden>
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" className="opacity-25" />
      <path d="M22 12a10 10 0 01-10 10" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  )
}
