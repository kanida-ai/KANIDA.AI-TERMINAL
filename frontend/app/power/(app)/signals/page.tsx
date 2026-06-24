/**
 * /power/signals — the daily picks hub (AI-native shell).
 *
 * MIGRATION (2026-06-23): replaced the launch-pending shell with REAL content by
 * reusing the proven legacy surfaces:
 *   • Today's Falcon Top 10 — Top20Card + Top20Filters (the /power/today surface),
 *     data via PowerAPI.falconTop20(universe, sector?, signal_date?).
 *   • Live ENTER/WAIT/SKIP — CyclePicker + decision buckets (the /power/live
 *     surface), data via PowerAPI.liveDecisions(jwt, cycle).
 *   • An entry into Performance → replay outcomes for the viewed date.
 *
 * Both surfaces are presented as tabs inside the shell's viewport-lock by
 * SignalsExperience. The legacy /power/today and /power/live routes stay intact
 * as fallback. Honesty preserved 1:1 — only the real payloads are rendered.
 */
import { PowerAPI, type LiveCycle, type LiveDecisionsResponse } from '@/lib/power-api'
import { getSessionJWT } from '@/lib/power-auth'
import { SignalsExperience } from '@/components/power/signals/SignalsExperience'
import type { Top20Universe, Top20Response } from '@/lib/falcon-top20-types'

export const dynamic = 'force-dynamic'
export const revalidate = 0

const ALLOWED_UNIVERSES = new Set<Top20Universe>([
  'all500', 'nifty50', 'nifty100', 'nifty200', 'fno',
])

type SearchParams = Promise<Record<string, string | string[] | undefined>>

export default async function SignalsPage({ searchParams }: { searchParams: SearchParams }) {
  const sp = await searchParams
  const universe = pickUniverse(sp.universe)
  const sector   = pickStr(sp.sector)
  const reqDate  = pickDate(sp.signal_date)
  const liveCycle = pickCycle(sp.cycle)

  const jwt = await getSessionJWT()

  // Fetch the two surfaces in parallel. Live needs a JWT — if there isn't one
  // (e.g. preview-no-auth), we skip it honestly rather than fabricating rows.
  const [top20Res, liveRes] = await Promise.allSettled([
    PowerAPI.falconTop20(universe, sector, reqDate),
    jwt ? PowerAPI.liveDecisions(jwt, liveCycle) : Promise.resolve(null),
  ])

  let top20: Top20Response | null = null
  let top20Error: string | null = null
  if (top20Res.status === 'fulfilled') {
    top20 = top20Res.value
  } else {
    top20Error = top20Res.reason instanceof Error ? top20Res.reason.message : 'Failed to load Top 10.'
    console.error('[/power/signals] falconTop20 failed:', top20Res.reason)
  }

  let live: LiveDecisionsResponse | null = null
  let liveError: string | null = null
  if (liveRes.status === 'fulfilled') {
    live = liveRes.value
  } else {
    liveError = liveRes.reason instanceof Error ? liveRes.reason.message : 'Failed to load live decisions.'
    console.error('[/power/signals] liveDecisions failed:', liveRes.reason)
  }

  return (
    <SignalsExperience
      top20={top20}
      top20Error={top20Error}
      universe={universe}
      sector={sector}
      live={live}
      liveError={liveError}
      liveCycle={liveCycle}
    />
  )
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
  return /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : null
}

function pickCycle(raw: string | string[] | undefined): LiveCycle | 'latest' {
  const v = Array.isArray(raw) ? raw[0] : raw
  return v === '0930' || v === '0945' || v === '1000' ? v : 'latest'
}
