/**
 * /power/co-trading/[slug] — co-trading persona detail dashboard (AI-native shell).
 *
 * MIGRATION (2026-06-23): reuses PortfolioDashboardClient verbatim (the
 * /power/portfolios/[slug] surface — Zerodha-style P&L, open positions, recent
 * trades, equity curve, year-by-year) inside the shell. Data via the same
 * PowerAPI.portfolioDetail / portfolioPositions / portfolioTrades / portfolioEquity
 * calls. The legacy /power/portfolios/[slug] route stays intact as fallback.
 *
 * Wrapped in the shell's viewport-lock (shrink-0 header + flex-1 min-h-0
 * overflow-y-auto) with a back link to the co-trading hub.
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
import { PortfolioDashboardClient } from '@/app/power/portfolios/[slug]/DashboardClient'
import { requireSession } from '@/lib/power-auth'

export const dynamic = 'force-dynamic'
export const revalidate = 0

type Props = { params: Promise<{ slug: string }> }

export default async function CoTradingPersonaDetail({ params }: Props) {
  await requireSession()
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
    summary     = s
    positions   = pos.positions
    trades      = tr.trades
    tradesTotal = tr.total
    equity      = eq.points
    capitalStart = eq.capital_start
  } catch (e) {
    if (e instanceof PowerAPIError && e.status === 404) return notFound()
    console.error('[/power/co-trading/[slug]] fetch failed:', e)
    return (
      <ShellWrap>
        <div className="max-w-md mx-auto py-16 text-center space-y-4">
          <h1 className="text-2xl font-bold">Persona unavailable</h1>
          <p className="text-sm text-neutral-400">
            {e instanceof Error ? e.message : 'Refresh in a moment.'}
          </p>
          <Link href="/power/co-trading" className="text-mint-400 hover:text-mint-300 underline">
            ← Back to Co-Trading
          </Link>
        </div>
      </ShellWrap>
    )
  }
  if (!summary) return notFound()

  return (
    <ShellWrap>
      <div className="mb-4">
        <Link href="/power/co-trading" className="text-sm text-neutral-400 hover:text-neutral-200">
          ← Back to Co-Trading
        </Link>
      </div>
      <PortfolioDashboardClient
        summary={summary}
        positions={positions}
        trades={trades}
        tradesTotal={tradesTotal}
        equity={equity}
        capitalStart={capitalStart}
      />
    </ShellWrap>
  )
}

/** Shell viewport-lock wrapper: a single flex-1 min-h-0 overflow-y-auto scroll
 *  region so the wide dashboard scrolls internally, not the whole shell. */
function ShellWrap({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="max-w-5xl mx-auto w-full px-4 md:px-8 lg:px-10 pt-20 md:pt-8 pb-16">
          {children}
        </div>
      </div>
    </div>
  )
}
