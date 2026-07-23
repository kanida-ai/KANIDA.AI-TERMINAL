/**
 * /power/ask — Ask-Falcon home (F2 master-detail).
 *
 * Server component: resolves the greeting first name from the real session and
 * fetches the LIVE Falcon Top 10 in ONE call — PowerAPI.falconTop20('all500').
 * The whole interactive home (persona selector, Top-10 list, guided prompt,
 * greeting clock, explanation card) lives in AskFalconHome and reads ONLY from
 * the real Top20Response passed down here.
 *
 * Honesty: NO mock/static content. If the engine is unreachable or returns no
 * picks, `data` is null/empty and the client renders an honest loading/empty
 * state — never a stub. Fields the API doesn't carry (company name, 30-day
 * return, market-pulse line) are omitted, not fabricated.
 */
import { PowerAPI } from '@/lib/power-api'
import { getCurrentUser } from '@/lib/power-auth'
import { AskFalconHome } from '@/components/power/ask/AskFalconHome'
import type { Top20Response } from '@/lib/falcon-top20-types'

export const dynamic = 'force-dynamic'

function firstNameOf(displayName: string | null, email: string | null): string {
  if (displayName && displayName.trim()) return displayName.trim().split(/\s+/)[0]
  if (email && email.includes('@'))      return email.split('@')[0]
  return 'trader'
}

export default async function AskFalconHomePage() {
  // Layout already gated auth; cheap re-read of the cached user for the greeting.
  const user = await getCurrentUser()
  const firstName = firstNameOf(user?.display_name ?? null, user?.email ?? null)

  let data: Top20Response | null = null
  try {
    data = await PowerAPI.falconTop20('all500')
  } catch (e) {
    console.error('[/power/ask] falconTop20 fetch failed:', e)
  }

  return <AskFalconHome firstName={firstName} data={data} />
}
