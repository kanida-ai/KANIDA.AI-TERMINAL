/**
 * /power/portfolios/[slug] — full Co-Trader portfolio dashboard.
 *
 * Operator UX revision 2026-05-16:
 *   1. Persona name + plain-English tagline at top
 *   2. Zerodha-style header: Total Investment | Current Value | Today's P&L | Total P&L
 *   3. YTD / MTD / Active positions strip
 *   4. Compound | Simple toggle (client-only state, default Compound)
 *   5. Equity curve chart
 *   6. Open positions table — Zerodha column layout (Qty/Avg cost/LTP/Invested/Cur.val/P&L/Net%/Day%)
 *   7. Engine Performance section — verified-over-8-years numbers
 *   8. Recent trades — Zerodha-style row
 *   9. Locked rules footer
 */
import Link from 'next/link'
import { notFound } from 'next/navigation'
import {
  PowerAPI,
  PowerAPIError,
  type PortfolioSummary,
  type PortfolioEquityPoint,
  type PortfolioPosition,
  type PortfolioTrade,
} from '@/lib/power-api'
import { PortfolioDashboardClient } from './DashboardClient'

export const dynamic = 'force-dynamic'
export const revalidate = 0

type Props = { params: Promise<{ slug: string }> }


export default async function PortfolioDashboard({ params }: Props) {
  const { slug } = await params

  let summary:     PortfolioSummary       | null = null
  let positions:   PortfolioPosition[]    = []
  let trades:      PortfolioTrade[]       = []
  let equity:      PortfolioEquityPoint[] = []
  let capitalStart = 5_000_000
  let tradesTotal  = 0

  try {
    const [s, pos, tr, eq] = await Promise.all([
      PowerAPI.portfolioDetail(slug),
      PowerAPI.portfolioPositions(slug),
      PowerAPI.portfolioTrades(slug, 25, 0),
      PowerAPI.portfolioEquity(slug),
    ])
    summary      = s
    positions    = pos.positions
    trades       = tr.trades
    tradesTotal  = tr.total
    equity       = eq.points
    capitalStart = eq.capital_start
  } catch (e) {
    if (e instanceof PowerAPIError && e.status === 404) return notFound()
    console.error('[portfolio dashboard] fetch failed:', e)
    return (
      <div className="max-w-md mx-auto py-16 text-center space-y-4">
        <h1 className="text-2xl font-bold">Portfolio unavailable</h1>
        <p className="text-sm text-neutral-400">
          {e instanceof Error ? e.message : 'Refresh in a moment.'}
        </p>
        <Link href="/power/portfolios" className="text-mint-400 hover:text-mint-300 underline">
          ← Back to portfolios
        </Link>
      </div>
    )
  }
  if (!summary) return notFound()

  return (
    <PortfolioDashboardClient
      summary={summary}
      positions={positions}
      trades={trades}
      tradesTotal={tradesTotal}
      equity={equity}
      capitalStart={capitalStart}
    />
  )
}
