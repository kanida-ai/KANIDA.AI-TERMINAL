'use client'

/**
 * SelectDateButton — operator-locked transparency feature (Sprint 5d Fix 8).
 *
 * Pairs with the existing RandomReplayButton on /power.  The user picks any
 * date in the backtest window (2021-01-01 → today), the backend returns the
 * engine's picks for that day (rate-limited per IP to 20/hr for anon users),
 * and we hand the payload to the existing /power/replay/proof viewer.
 *
 * Picker constraints:
 *   - 2021-01-01 lower bound
 *   - today (in IST) upper bound — we clamp via the HTML date input's max attr
 *   - Weekends are visually allowed but the backend returns
 *     REPLAY_UNAVAILABLE for them; we surface the friendly error.
 *
 * No external date-picker dependency: the native <input type="date"> ships
 * with sensible UX on every modern browser and works without JS chunks.
 */
import { useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { PowerAPI, PowerAPIError, type ReplayPayload } from '@/lib/power-api'

const RANDOM_REPLAY_KEY = 'kanida_random_replay_v1'    // shared with RandomReplayButton

// 2021-01-01 is the operator's audited backtest start. We never go earlier.
const MIN_DATE = '2021-01-01'

function todayISTISO(): string {
  // Build YYYY-MM-DD in IST without importing any tz lib.
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(new Date())
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? ''
  return `${get('year')}-${get('month')}-${get('day')}`
}


export function SelectDateButton() {
  const router = useRouter()
  const today  = useMemo(todayISTISO, [])
  const [date, setDate]       = useState<string>('')
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState<{ msg: string; isRateLimited: boolean } | null>(null)

  const onGo = async () => {
    if (!date) {
      setError({ msg: 'Pick a date first.', isRateLimited: false }); return
    }
    setError(null); setLoading(true)
    try {
      const payload: ReplayPayload = await PowerAPI.replayForDate(date)
      try {
        sessionStorage.setItem(RANDOM_REPLAY_KEY, JSON.stringify(payload))
      } catch {
        // Fallback: route directly. /replay/[date] handles auth if needed.
        router.push(`/power/replay/${encodeURIComponent(payload.replay_date)}`)
        return
      }
      router.push('/power/replay/proof')
    } catch (e) {
      if (e instanceof PowerAPIError) {
        if (e.isRateLimited()) {
          setError({
            msg: "You've hit our anonymous limit (20 lookups per hour). Try again later or sign in.",
            isRateLimited: true,
          })
        } else if (e.status === 404) {
          setError({
            msg: `No engine output for ${date}. NSE holiday or weekend? Try a different date.`,
            isRateLimited: false,
          })
        } else {
          setError({ msg: e.message || 'Lookup failed. Try again.', isRateLimited: false })
        }
      } else {
        setError({ msg: 'Network error — check your connection.', isRateLimited: false })
      }
      setLoading(false)
    }
  }

  return (
    <div className="space-y-2">
      <div className="inline-flex items-center gap-2 flex-wrap">
        <input
          type="date"
          value={date}
          min={MIN_DATE}
          max={today}
          onChange={e => setDate(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && onGo()}
          aria-label="Pick a date to audit"
          className="bg-neutral-950 border border-neutral-700 rounded-md px-3 py-2 text-sm
                      text-neutral-100 font-mono focus:outline-none focus:border-mint-500/60
                      [color-scheme:dark]"
        />
        <button
          type="button"
          onClick={onGo}
          disabled={loading || !date}
          className={[
            'inline-flex items-center gap-2 px-4 py-2 rounded-md font-semibold text-sm',
            'border border-mint-500/40 text-mint-300 hover:bg-mint-500/10',
            'disabled:opacity-40 disabled:cursor-not-allowed transition-colors',
          ].join(' ')}
        >
          {loading ? 'Loading…' : 'Select Date'}
        </button>
      </div>
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
