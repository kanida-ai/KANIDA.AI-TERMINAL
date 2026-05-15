'use client'

/**
 * CyclePicker — tabs for switching between live tier cycles.
 *
 * Cycles: 0930, 0945, 1000, 'latest' (most recent cycle with data).
 * On client navigation (push), the server component re-renders with the new
 * cycle's data — no SPA-state hand-rolling needed.
 */
import { useRouter } from 'next/navigation'

type Props = {
  available:    ReadonlyArray<'0930' | '0945' | '1000' | 'latest'>
  current:      string
  activeCycle:  string | null      // which cycle actually has data right now
  computedAt:   string | null      // ISO timestamp from server
}

const LABEL: Record<string, string> = {
  '0930':   '09:30',
  '0945':   '09:45',
  '1000':   '10:00',
  'latest': 'Latest',
}

export function CyclePicker({ available, current, activeCycle, computedAt }: Props) {
  const router = useRouter()

  return (
    <div className="flex flex-col items-end gap-1">
      <div className="inline-flex border border-neutral-800 rounded overflow-hidden">
        {available.map(c => (
          <button
            key={c}
            type="button"
            onClick={() => router.push(`/power/live${c === 'latest' ? '' : `?cycle=${c}`}`)}
            className={[
              'px-3 py-1.5 text-xs font-mono border-r border-neutral-800 last:border-r-0',
              current === c
                ? 'bg-amber-500/20 text-amber-200'
                : 'text-neutral-400 hover:bg-neutral-900 hover:text-neutral-200',
            ].join(' ')}
          >
            {LABEL[c]}
          </button>
        ))}
      </div>
      {activeCycle && computedAt && (
        <div className="text-[10px] text-neutral-500 font-mono">
          showing {activeCycle} · computed {formatTime(computedAt)}
        </div>
      )}
    </div>
  )
}

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString('en-IN', {
      timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false,
    }) + ' IST'
  } catch { return iso }
}
