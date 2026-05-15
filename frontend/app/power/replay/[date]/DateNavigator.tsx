'use client'

/**
 * DateNavigator — date picker for power-user replay drill-down.
 *
 * Public users see a hint to sign in for custom dates; authed users get a
 * functional <input type="date"> that routes to the replay for that date.
 */
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import Link from 'next/link'

type Props = {
  currentDate: string
  authed:      boolean
}

export function DateNavigator({ currentDate, authed }: Props) {
  const router = useRouter()
  const [date, setDate] = useState(currentDate)

  if (!authed) {
    return (
      <div className="text-xs text-neutral-500">
        Want to replay any date?{' '}
        <Link href="/power/login" className="text-amber-400 underline">
          Sign in for custom replays →
        </Link>
      </div>
    )
  }

  const onGo = (e: React.FormEvent) => {
    e.preventDefault()
    if (date && date !== currentDate) {
      router.push(`/power/replay/${encodeURIComponent(date)}`)
    }
  }

  return (
    <form onSubmit={onGo} className="flex items-center gap-2 text-sm">
      <label htmlFor="replay-date" className="text-neutral-400">Pick any date:</label>
      <input
        id="replay-date"
        type="date"
        value={date}
        onChange={e => setDate(e.target.value)}
        max={new Date().toISOString().slice(0, 10)}
        min="2020-01-01"
        className="bg-neutral-950 border border-neutral-700 rounded px-2 py-1
                    text-sm font-mono text-neutral-100"
      />
      <button
        type="submit"
        disabled={date === currentDate}
        className="px-3 py-1 bg-amber-500 text-neutral-950 rounded text-sm font-medium
                    hover:bg-amber-400 disabled:opacity-40 disabled:cursor-not-allowed"
      >
        Replay
      </button>
    </form>
  )
}
