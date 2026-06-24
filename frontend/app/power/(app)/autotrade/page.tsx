/**
 * /power/autotrade — role-branched AutoTrade surface.
 *
 *  • OPERATOR (user.role === 'admin'): renders AutoTradeConsoleHub — a PURE
 *    navigation launcher (mint/F2 card grid) into the real, LIVE Falcon operator
 *    console at app/falcon/* (Trade / Pre-Market / Positions / Config / Admin +
 *    the /falcon overview). That console already exists and is protected by the
 *    site-wide HTTP Basic Auth, OUTSIDE the /power invite-auth zone — we link to
 *    it (plain <a href>), we do NOT move it and we fetch NO /api/falcon data here.
 *
 *  • EVERYONE ELSE: the existing AutoTradeExperience — a launch-pending UX
 *    (style / capital / readiness / waitlist preview). Per-user broker connect +
 *    real execution is NOT built for end-users and is high-risk, so this page
 *    never places or simulates a real order. It fetches the LIVE Falcon Top 10
 *    (PowerAPI.falconTop20('all500')) ONLY to PREVIEW what Falcon would trade.
 *
 * Honesty: NO fabricated fills/P&L. If the engine is unreachable, `data` is null
 * and the client renders an honest empty state. The preview allocation math
 * (qty / capital / SL) is REAL — computed from the real picks + entry price.
 */
import { PowerAPI } from '@/lib/power-api'
import { getCurrentUser } from '@/lib/power-auth'
import { AutoTradeExperience } from '@/components/power/autotrade/AutoTradeExperience'
import { AutoTradeConsoleHub } from '@/components/power/autotrade/AutoTradeConsoleHub'
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

  // Operator branch: launcher into the live console — same admin check the shell
  // uses for isAdmin. No /api/falcon fetch here; links only.
  const isOperator = user?.role === 'admin'
  if (isOperator) {
    return <AutoTradeConsoleHub firstName={firstName} />
  }

  // Non-operator branch: unchanged launch-pending experience.
  let data: Top20Response | null = null
  try {
    data = await PowerAPI.falconTop20('all500')
  } catch (e) {
    console.error('[/power/autotrade] falconTop20 fetch failed:', e)
  }

  return <AutoTradeExperience data={data} firstName={firstName} />
}
