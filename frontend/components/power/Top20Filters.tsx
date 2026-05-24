/**
 * Top20Filters — universe + sector filter strip for the /power/today page.
 *
 * Client component. Drives navigation via router.push with searchParams so
 * the page re-renders server-side with the new filter and the URL is shareable.
 *
 * Universe chips: All 500 | Nifty 50 | Nifty 100 | Nifty 200 | F&O
 * Sector dropdown: free-form, populated from the picks themselves so we
 * don't ship a hard-coded list that drifts from PROD.
 */
'use client'

import { useRouter, useSearchParams, usePathname } from 'next/navigation'
import { useTransition } from 'react'
import type { Top20Universe } from '@/lib/falcon-top20-types'

const UNIVERSE_OPTIONS: Array<{ key: Top20Universe; label: string }> = [
  { key: 'all500',   label: 'All 500'   },
  { key: 'nifty50',  label: 'Nifty 50'  },
  { key: 'nifty100', label: 'Nifty 100' },
  { key: 'nifty200', label: 'Nifty 200' },
  { key: 'fno',      label: 'F&O'       },
]

type Props = {
  universe:         Top20Universe
  sector:           string | null
  sectorOptions:    string[]   // distinct sectors seen in today's picks
  signalDate:       string | null
  availableDates:   string[]   // YYYY-MM-DD, newest first
  latestDate:       string | null
}

export function Top20Filters({
  universe, sector, sectorOptions,
  signalDate, availableDates, latestDate,
}: Props) {
  const router = useRouter()
  const pathname = usePathname()
  const params = useSearchParams()
  const [pending, startTransition] = useTransition()

  function navigate(next: {
    universe?:    Top20Universe
    sector?:      string | null
    signal_date?: string | null
  }) {
    const sp = new URLSearchParams(params.toString())
    if (next.universe !== undefined) sp.set('universe', next.universe)
    if (next.sector !== undefined) {
      if (next.sector === null || next.sector === '') sp.delete('sector')
      else                                            sp.set('sector', next.sector)
    }
    if (next.signal_date !== undefined) {
      if (next.signal_date === null || next.signal_date === '' || next.signal_date === latestDate) {
        sp.delete('signal_date')          // omit when picking latest (URL stays clean)
      } else {
        sp.set('signal_date', next.signal_date)
      }
    }
    const qs = sp.toString()
    startTransition(() => {
      router.push(qs ? `${pathname}?${qs}` : pathname)
    })
  }

  return (
    <div className={['flex flex-col md:flex-row md:items-center gap-3 md:gap-4 transition-opacity', pending ? 'opacity-60' : 'opacity-100'].join(' ')}>
      <div className="flex items-center gap-2 flex-wrap">
        {UNIVERSE_OPTIONS.map(opt => {
          const active = opt.key === universe
          return (
            <button
              key={opt.key}
              type="button"
              onClick={() => navigate({ universe: opt.key })}
              className={['px-3 py-1.5 rounded-full text-xs font-semibold uppercase tracking-wide border transition-colors', active ? 'bg-mint-400/[0.12] border-mint-400/40 text-mint-300' : 'bg-neutral-900 border-neutral-800 text-white/70 hover:border-mint-400/30 hover:text-white'].join(' ')}
              aria-pressed={active}
            >
              {opt.label}
            </button>
          )
        })}
      </div>

      <div className="flex items-center gap-3 md:ml-auto flex-wrap">
        <div className="flex items-center gap-2">
          <label htmlFor="signal-date" className="text-[11px] uppercase tracking-wider text-white/45">
            EOD Date
          </label>
          <select
            id="signal-date"
            value={signalDate ?? ''}
            onChange={(e) => navigate({ signal_date: e.target.value || null })}
            className="bg-neutral-900 border border-neutral-800 text-white/90 text-sm rounded-md px-2 py-1.5 font-mono focus:outline-none focus:border-mint-400/40"
          >
            {availableDates.length === 0 && <option value="">No data</option>}
            {availableDates.map(d => (
              <option key={d} value={d}>
                {d}{d === latestDate ? ' (latest)' : ''}
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <label htmlFor="sector-filter" className="text-[11px] uppercase tracking-wider text-white/45">
            Sector
          </label>
          <select
            id="sector-filter"
            value={sector ?? ''}
            onChange={(e) => navigate({ sector: e.target.value || null })}
            className="bg-neutral-900 border border-neutral-800 text-white/90 text-sm rounded-md px-2 py-1.5 focus:outline-none focus:border-mint-400/40"
          >
            <option value="">All sectors</option>
            {sectorOptions.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>
    </div>
  )
}
