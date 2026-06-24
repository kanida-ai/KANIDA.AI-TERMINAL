/**
 * /power/co-trading — Co-Trading (Phase 1).
 *
 * Server component: resolves the greeting first name from the real session and
 * fetches the LIVE Falcon Top 10 in ONE call — PowerAPI.falconTop20('all500').
 * The interactive experience (setup → REAL allocation preview → portfolio shell
 * → actions scaffold → controls → AutoTrade bridge) lives in CoTradingExperience
 * and reads ONLY from the real Top20Response seeded here.
 *
 * Honesty: NO mock/static content and NO fabricated P&L/prices. If the engine is
 * unreachable, `data` is null and the client renders an honest empty state. The
 * allocation math (qty / capital / SL) is REAL — computed from the real picks +
 * the pick's entry price. Live tracking / replay / persistence are Backend needs,
 * surfaced as honest pending states inside the experience.
 */
import { PowerAPI, type PortfolioSummary, type PortfolioEquityPoint } from '@/lib/power-api'
import { getCurrentUser } from '@/lib/power-auth'
import { CoTradingExperience } from '@/components/power/cotrading/CoTradingExperience'
import type { Top20Response } from '@/lib/falcon-top20-types'

export const dynamic = 'force-dynamic'

function firstNameOf(displayName: string | null, email: string | null): string {
  if (displayName && displayName.trim()) return displayName.trim().split(/\s+/)[0]
  if (email && email.includes('@'))      return email.split('@')[0]
  return 'trader'
}

export default async function CoTradingPage() {
  const user = await getCurrentUser()
  const firstName = firstNameOf(user?.display_name ?? null, user?.email ?? null)

  let data: Top20Response | null = null
  try {
    data = await PowerAPI.falconTop20('all500')
  } catch (e) {
    console.error('[/power/co-trading] falconTop20 fetch failed:', e)
  }

  // MIGRATION (2026-06-23): the REAL co-trading personas + equity sparklines for
  // the "Browse live AI co-traders" listing. Degrades gracefully — if any fetch
  // fails the section is simply omitted (never faked).
  let personas: PortfolioSummary[] = []
  const sparklines: Record<string, PortfolioEquityPoint[]> = {}
  try {
    const r = await PowerAPI.portfolios()
    personas = r.portfolios
    if (personas.length > 0) {
      const results = await Promise.allSettled(
        personas.map(p => PowerAPI.portfolioEquity(p.slug)),
      )
      results.forEach((res, i) => {
        if (res.status === 'fulfilled') sparklines[personas[i].slug] = res.value.points
      })
    }
  } catch (e) {
    console.error('[/power/co-trading] portfolios fetch failed:', e)
  }

  return (
    <CoTradingExperience
      data={data}
      firstName={firstName}
      personas={personas}
      sparklines={sparklines}
    />
  )
}
