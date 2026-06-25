/**
 * /power/autotrade — role-branched AutoTrade surface.
 *
 *  • OPERATOR (user.role === 'admin'): renders ONE unified AutoTradePanel — a
 *    single sub-tabbed surface where EVERYTHING AutoTrade lives (no separate
 *    legacy panel, no "Operator Console" launcher). Tabs:
 *      Sessions (HOME)  — the NEW multi-broker /api/autotrade/* system
 *                         (create / list / RESUME / status / kill).
 *      Pre-Market       — the LIVE legacy app/falcon/premarket screen, reused.
 *      Positions        — the LIVE legacy app/falcon/positions screen, reused.
 *      Config           — the LIVE legacy app/falcon/config screen, reused.
 *      Engine           — the LIVE legacy app/falcon/admin screen + embedded
 *                         app/falcon/trade (manual preview→smoke→place), reused.
 *    The reused legacy screens call FalconAPI, which routes through the same-
 *    origin /api/falcon-proxy (operator token injected server-side), so they
 *    work unchanged inside /power. The /falcon/* routes stay live as a fallback.
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
import { AutoTradePanel } from '@/components/power/autotrade/AutoTradePanel'
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

  // Operator branch: the single unified panel — same admin check the shell uses
  // for isAdmin. The panel's children fetch their own data client-side.
  const isOperator = user?.role === 'admin'
  if (isOperator) {
    return <AutoTradePanel firstName={firstName} />
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
