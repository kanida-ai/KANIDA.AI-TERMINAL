/**
 * /power/performance — proof + replay outcomes (AI-native shell).
 *
 * MIGRATION (2026-06-23): replaced the launch-pending shell with REAL content by
 * reusing the proven legacy surfaces:
 *   • The ₹30L→₹1.05Cr walk-forward credibility content (from /power/credibility,
 *     static, operator-locked).
 *   • A replay/outcomes viewer for any date — ExpandablePickRow + PowerAPI
 *     .replayForDate(date, jwt), with an in-shell date navigator. Shows D+1/3/5/
 *     10/15 outcomes for every pick.
 *
 * Both reused surfaces' legacy routes (/power/credibility, /power/replay/[date])
 * stay intact as fallback. Honesty 1:1 — only the real replay payload is rendered.
 */
import { PowerAPI, PowerAPIError, assertPickVersion, type ReplayPayload } from '@/lib/power-api'
import { getSessionJWT } from '@/lib/power-auth'
import { PerformanceExperience } from '@/components/power/performance/PerformanceExperience'

export const dynamic = 'force-dynamic'
export const revalidate = 0

type SearchParams = Promise<{ date?: string | string[] }>

export default async function PerformancePage({ searchParams }: { searchParams: SearchParams }) {
  const sp = await searchParams
  const rawDate = Array.isArray(sp.date) ? sp.date[0] : sp.date
  const replayDate = rawDate && /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : null

  const jwt = await getSessionJWT()

  let replay: ReplayPayload | null = null
  let replayError: string | null = null

  if (replayDate) {
    try {
      replay = await PowerAPI.replayForDate(replayDate, jwt)
      replay.picks.forEach(assertPickVersion)
    } catch (e) {
      if (e instanceof PowerAPIError && e.isUnauthorized()) {
        replayError = 'Sign in to replay this date.'
      } else if (e instanceof PowerAPIError) {
        replayError = e.message || 'Replay unavailable for this date.'
      } else {
        replayError = 'Network error — try again.'
      }
      console.error('[/power/performance] replayForDate failed:', e)
    }
  }

  return (
    <PerformanceExperience
      replay={replay}
      replayDate={replayDate}
      replayError={replayError}
      authed={jwt !== null}
    />
  )
}
