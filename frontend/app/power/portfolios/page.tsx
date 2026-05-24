/**
 * /power/portfolios — Co-Trader listing (Sprint 5d).
 *
 * Public, no sign-in required. 5 cards, each showing the live cumulative
 * return + a sparkline of the equity curve. Server-rendered (no client JS
 * other than Link). Sparkline data is fetched per-portfolio in parallel.
 */
import Link from 'next/link'
import { PowerAPI, type PortfolioSummary, type PortfolioEquityPoint } from '@/lib/power-api'
import { PortfolioCard } from '@/components/power/PortfolioCard'

export const dynamic = 'force-dynamic'
export const revalidate = 0


export default async function PortfoliosPage() {
  let portfolios: PortfolioSummary[] = []
  let fetchError: string | null = null

  try {
    const r = await PowerAPI.portfolios()
    portfolios = r.portfolios
  } catch (e) {
    console.error('[/power/portfolios] fetch failed:', e)
    fetchError = e instanceof Error ? e.message : 'Co-Trader is warming up. Refresh in a moment.'
  }

  // Fetch sparklines in parallel — degrades gracefully if any one fails
  const sparklines: Record<string, PortfolioEquityPoint[]> = {}
  if (portfolios.length > 0) {
    const results = await Promise.allSettled(
      portfolios.map(p => PowerAPI.portfolioEquity(p.slug))
    )
    results.forEach((res, i) => {
      if (res.status === 'fulfilled') {
        sparklines[portfolios[i].slug] = res.value.points
      }
    })
  }

  return (
    <div className="space-y-8 md:space-y-10">
      <Hero />
      {fetchError ? (
        <p role="alert" className="px-4 py-3 rounded border border-red-500/40 bg-red-500/10 text-red-200 text-sm">
          {fetchError}
        </p>
      ) : (
        <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {portfolios.map(p => (
            <PortfolioCard
              key={p.slug}
              summary={p}
              sparklinePoints={sparklines[p.slug]}
            />
          ))}
        </section>
      )}
      <BottomBanner />
    </div>
  )
}


function Hero() {
  return (
    <header className="space-y-3 md:space-y-4 max-w-3xl">
      <p className="text-xs md:text-sm tracking-[0.25em] uppercase text-mint-300 font-semibold">
        AI Co-Trading Intelligence
      </p>
      <h1 className="text-3xl md:text-5xl font-bold leading-[1.1] tracking-tight text-neutral-50">
        Meet the AI that trades like a real trader
      </h1>
      <div className="space-y-2 text-base md:text-lg text-neutral-200 leading-relaxed">
        <p className="font-medium">Beyond stock picks. Beyond backtests.</p>
        <p>
          Kanida.AI Co-Trading feature lets you watch AI-managed portfolios
          trade live using proven trading configurations and historical
          intelligence — with virtual capital.
        </p>
        <p>
          Choose the AI trading style that matches your personality and track
          its performance in real time, with complete transparency.
        </p>
      </div>
    </header>
  )
}


function BottomBanner() {
  return (
    <section className="rounded-xl border border-mint-500/30 bg-gradient-to-br from-mint-500/[0.06] to-transparent p-5 md:p-6 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
      <div>
        <h2 className="text-base md:text-lg font-bold text-neutral-100">
          How much would this look like at your capital?
        </h2>
        <p className="text-sm text-neutral-400 mt-1 max-w-xl">
          Try the position sizing assistant — type your capital, see the per-trade
          size, the deployment, and the worst single month you&apos;d feel.
        </p>
      </div>
      <Link
        href="/power/sizing"
        className="px-5 py-2.5 bg-mint-400 text-neutral-950 rounded-md font-semibold hover:bg-mint-300 transition-colors whitespace-nowrap"
      >
        Size it for me →
      </Link>
    </section>
  )
}
