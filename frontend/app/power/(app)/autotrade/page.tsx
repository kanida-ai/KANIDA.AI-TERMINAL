/**
 * /power/autotrade — AutoTrade (UX + LAUNCH-PENDING ONLY).
 *
 * Server component: resolves the greeting first name from the real session and
 * fetches the LIVE Falcon Top 10 in ONE call — PowerAPI.falconTop20('all500') —
 * used ONLY to PREVIEW what Falcon would trade today. The page MUST NOT place or
 * simulate a real order: per-user broker connect + real execution is NOT built
 * and is high-risk, so AutoTradeExperience is a launch-pending UX (waitlist +
 * preview). The interactive flow lives in AutoTradeExperience.
 *
 * Honesty: NO fabricated fills/P&L. If the engine is unreachable, `data` is null
 * and the client renders an honest empty state. The preview allocation math
 * (qty / capital / SL) is REAL — computed from the real picks + entry price.
 */
import { PowerAPI } from '@/lib/power-api'
import { getCurrentUser } from '@/lib/power-auth'
import { AutoTradeExperience } from '@/components/power/autotrade/AutoTradeExperience'
import type { Top20Response } from '@/lib/falcon-top20-types'

export const dynamic = 'force-dynamic'

function firstNameOf(displayName: string | null, email: string | null): string {
  if (displayName && displayName.trim()) return displayName.trim().split(/\s+/)[0]
  if (email && email.includes('@'))      return email.split('@')[0]
  return 'trader'
}

export default async function AutoTradePage() {
  const user = await getCurrentUser()
  const firstName = firstNameOf(user?.display_name ?? null, user?.email ?? null)

  let data: Top20Response | null = null
  try {
    data = await PowerAPI.falconTop20('all500')
  } catch (e) {
    console.error('[/power/autotrade] falconTop20 fetch failed:', e)
  }

  return <AutoTradeExperience data={data} firstName={firstName} />
}
