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
 *  • EVERYONE ELSE: the PowerUserAutoTrade shell — a clean 4-tab surface (Connect
 *    Your Broker · Start Auto Trade · Live & Scheduled Campaigns · Trade Journal),
 *    one clear next action per tab. It reuses the SAME BrokerAccountsPanel +
 *    PortfolioAutoTrade (view-split into create/dashboard) + TradeJournalPanel the
 *    operator uses. Connecting places no order; automated execution on the user's
 *    own account stays gated server-side (paper by default) and, for a LIVE start,
 *    requires their own ACTIVE broker account. This page never places or simulates
 *    a real order.
 */
import { getCurrentUser } from '@/lib/power-auth'
import { PowerUserAutoTrade } from '@/components/power/autotrade/PowerUserAutoTrade'
import { AutoTradePanel } from '@/components/power/autotrade/AutoTradePanel'

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
    // user.id is the logged-in operator's numeric id — reused as the per-account
    // owner for the new broker-accounts panel + per-account session scoping.
    return <AutoTradePanel firstName={firstName} userId={user!.id} />
  }

  // Non-operator branch: the 4-tab power-user AutoTrade shell (Connect Your Broker ·
  // Start Auto Trade · Live & Scheduled Campaigns · Trade Journal). Reuses the same
  // BrokerAccountsPanel + PortfolioAutoTrade + TradeJournalPanel the operator uses,
  // in view-split (create/dashboard) mounts. Real-money gating (own ACTIVE account
  // for a live start) stays enforced inside PortfolioAutoTrade (isAdmin=false).
  return <PowerUserAutoTrade firstName={firstName} userId={user!.id} />
}
