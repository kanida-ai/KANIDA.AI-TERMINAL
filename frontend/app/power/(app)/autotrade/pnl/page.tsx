/**
 * /power/autotrade/pnl — AutoTrade · Performance (strategy-level P&L).
 *
 * A reporting surface under the AutoTrade mode: which strategy made money,
 * which lost, and the real net after charges. It is NOT the /power/performance
 * proof/replay mode (that answers "are the picks any good?"); this answers
 * "what did my automation actually net?".
 *
 * Auth: the (app) layout already gates access (getCurrentUser → redirect). We
 * additionally requireSession() here to hand the JWT to the client dashboard —
 * the P&L endpoint + CSV export both need a Bearer header, and the summary
 * refetches client-side on every period/custom-range change. The token never
 * reaches the client any other way (the cookie is HTTPOnly and the same-origin
 * rewrite forwards it, but the backend reads Authorization, not the cookie).
 */
import { requireSession } from '@/lib/power-auth'
import { PnlDashboard } from './PnlDashboard'

export const dynamic = 'force-dynamic'
export const revalidate = 0

export default async function AutoTradePnlPage() {
  const { jwt } = await requireSession()
  return <PnlDashboard jwt={jwt} />
}
