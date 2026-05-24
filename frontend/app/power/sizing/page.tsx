'use client'

/**
 * /power/sizing — Position Sizing Assistant (Sprint 5d operator priority).
 *
 * Public, no sign-in required. The user types their capital, sees per-portfolio
 * projections for trade size, deployed capital, monthly P&L range, and worst-
 * case drawdown — all scaled from the validated backtest metrics.
 *
 * Design rules:
 *   - One screen, no scroll on desktop. Quick to grok.
 *   - Big readable numbers. ₹ formatted with Indian comma grouping.
 *   - Per-portfolio rows are clickable and route to /power/portfolios/{slug}.
 *   - Default to ₹15 L ("Pudhuraja's typical retail capital" anchor).
 */
import { useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { PowerAPI, type SizingResult } from '@/lib/power-api'
import { RiskBadge } from '@/components/power/PortfolioCard'

function WarningChip() {
  return (
    <span className="inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border
                      bg-red-500/15 text-red-300 border-red-500/50 font-mono font-bold">
      ⚠ high risk
    </span>
  )
}

const inrSigned = (n: number | null | undefined, forcePositiveSign = false) => {
  if (n == null) return '—'
  const abs    = Math.abs(Math.round(n))
  const prefix = n < 0 ? '−' : forcePositiveSign ? '+' : ''
  return prefix + '₹' + abs.toLocaleString('en-IN')
}

const DEFAULT_CAPITAL = 500_000      // ₹5 L — operator-locked default 2026-05-16
const QUICK_CAPITALS: Array<{ label: string; value: number }> = [
  { label: '₹2 L',  value:    200_000 },
  { label: '₹5 L',  value:    500_000 },
  { label: '₹10 L', value:  1_000_000 },
  { label: '₹25 L', value:  2_500_000 },
  { label: '₹50 L', value:  5_000_000 },
  { label: '₹1 Cr', value: 10_000_000 },
]

const inr = (n: number) => {
  const abs = Math.abs(Math.round(n))
  const sign = n < 0 ? '−' : ''
  return sign + '₹' + abs.toLocaleString('en-IN')
}

export default function SizingPage() {
  const [capitalInput, setCapitalInput] = useState<string>(String(DEFAULT_CAPITAL))
  const capital = useMemo(() => {
    const n = parseInt(capitalInput.replace(/[^0-9]/g, ''), 10)
    return Number.isFinite(n) && n > 0 ? n : 0
  }, [capitalInput])

  const [data,    setData]    = useState<SizingResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState<string | null>(null)

  useEffect(() => {
    if (capital < 10_000) { setData(null); return }
    const ctrl = new AbortController()
    setLoading(true); setError(null)
    PowerAPI.portfolioSizing(capital, ctrl.signal)
      .then(r => { setData(r); setLoading(false) })
      .catch(e => {
        if ((e as DOMException)?.name === 'AbortError') return
        setError(e instanceof Error ? e.message : 'Unable to compute sizing.')
        setLoading(false)
      })
    return () => ctrl.abort()
  }, [capital])

  return (
    <div className="max-w-4xl mx-auto space-y-8 py-2 md:py-6">
      <Hero />
      <CapitalInput
        rawInput={capitalInput}
        capital={capital}
        onChange={setCapitalInput}
      />
      {capital < 10_000 ? (
        <div className="text-sm text-neutral-500 text-center py-12">
          Enter at least ₹10,000 to see allocations.
        </div>
      ) : error ? (
        <div role="alert" className="px-4 py-3 rounded border border-red-500/40 bg-red-500/10 text-red-200 text-sm">
          {error}
        </div>
      ) : data ? (
        <PortfolioTable result={data} loading={loading} />
      ) : (
        <SkeletonTable />
      )}
      <FinePrint />
    </div>
  )
}


function Hero() {
  return (
    <header className="space-y-2 md:space-y-3">
      <p className="text-xs tracking-[0.2em] uppercase text-mint-300 font-semibold">
        Position sizing assistant
      </p>
      <h1 className="text-3xl md:text-4xl font-bold leading-tight">
        How much would you have to trade alongside the engine?
      </h1>
      <p className="text-sm md:text-base text-neutral-400 max-w-2xl">
        Enter your trading capital. We&apos;ll show you, per portfolio, the
        per-trade size, the deployment you&apos;d expect to see, and how the worst
        month historically would feel in your rupees.
      </p>
    </header>
  )
}


function CapitalInput({ rawInput, capital, onChange }: {
  rawInput: string; capital: number; onChange: (v: string) => void
}) {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-4 md:p-5 space-y-3">
      <label className="block">
        <span className="text-xs uppercase tracking-wider text-neutral-500">Your capital (₹)</span>
        <div className="mt-1 flex items-center gap-2">
          <span className="text-2xl text-neutral-500">₹</span>
          <input
            type="text" inputMode="numeric" pattern="[0-9,]*"
            value={rawInput}
            onChange={e => onChange(e.target.value)}
            placeholder="15,00,000"
            className="flex-1 bg-neutral-950 border border-neutral-700 rounded px-3 py-2.5
                        text-2xl md:text-3xl font-mono text-neutral-100 tracking-wide
                        focus:outline-none focus:border-mint-500/60"
          />
        </div>
        {capital > 0 && (
          <p className="mt-1.5 text-xs text-neutral-500 font-mono">
            = {inr(capital)} <span className="text-neutral-700">·</span>{' '}
            {capital >= 10_000_000
              ? `${(capital / 10_000_000).toFixed(2)} Cr`
              : `${(capital / 100_000).toFixed(1)} L`}
          </p>
        )}
      </label>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs text-neutral-500">Quick fills:</span>
        {QUICK_CAPITALS.map(q => (
          <button
            key={q.value}
            type="button"
            onClick={() => onChange(String(q.value))}
            className={[
              'text-xs px-3 py-1 rounded border transition-colors',
              capital === q.value
                ? 'bg-mint-500 text-neutral-950 border-mint-500'
                : 'border-neutral-700 text-neutral-300 hover:border-mint-500/40 hover:text-mint-300',
            ].join(' ')}
          >
            {q.label}
          </button>
        ))}
      </div>
    </section>
  )
}


function PortfolioTable({ result, loading }: { result: SizingResult; loading: boolean }) {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg overflow-hidden">
      <div className="px-4 py-3 border-b border-neutral-800 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold text-neutral-300">
          For {inr(result.user_capital_rs)} of capital
        </h2>
        {loading && <span className="text-xs text-neutral-500">Updating…</span>}
      </div>

      <div className="divide-y divide-neutral-800">
        {result.portfolios.map(p => (
          <Link
            key={p.slug}
            href={`/power/portfolios/${p.slug}`}
            className="block px-4 py-4 hover:bg-neutral-850 transition-colors group"
          >
            <div className="flex flex-wrap items-baseline justify-between gap-2 mb-2">
              <div className="min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <h3 className="text-base font-bold text-neutral-50 group-hover:text-mint-300 transition-colors">
                    {p.name}
                  </h3>
                  <RiskBadge tier={p.risk_tier} color={p.risk_tier_color} />
                  {p.has_disclosure && <WarningChip />}
                </div>
                <p className="text-xs text-neutral-300 mt-0.5">{p.tagline}</p>
              </div>
              <div className="text-right">
                <div className="text-xs uppercase tracking-wider text-neutral-400">Avg yearly return</div>
                <div className="text-lg font-bold font-mono text-green-300">
                  +{p.avg_yearly_return_pct}%
                </div>
                <div className="text-[10px] text-neutral-400 font-mono mt-0.5">{p.backtest_window}</div>
              </div>
            </div>

            <dl className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
              <Cell label="Per-trade size"
                    value={inr(p.per_trade_size_rs)}
                    sub={`Fixed ₹ per trade · top ${p.expected_n_positions} positions`} />
              <Cell label="Active positions"
                    value={`~${p.expected_n_positions}`}
                    sub={`~${p.invested_right_now_pct}% invested at any time`} />
              <Cell label="Worst year"
                    value={inrSigned(p.worst_year_rs)}
                    sub={`${p.worst_year_pct >= 0 ? '+' : ''}${p.worst_year_pct}% ${p.worst_year_label ?? ''}`}
                    accent={p.worst_year_pct < 0 ? 'red' : 'green'} />
              <Cell label="Best year"
                    value={inrSigned(p.best_year_rs, true)}
                    sub={`+${p.best_year_pct}% ${p.best_year_label ?? ''}`}
                    accent="green" />
            </dl>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-neutral-400">
              <span>
                <span className="text-neutral-200 font-mono">{p.avg_trades_per_month}</span> trades / month
                {p.win_rate_pct != null && (
                  <> · <span className="text-neutral-200 font-mono">{p.win_rate_pct}%</span> win rate</>
                )}
                {p.positive_years && (
                  <> · <span className="text-green-300">{p.positive_years} positive years</span></>
                )}
              </span>
              <span className="text-mint-300 group-hover:text-mint-200">
                See live portfolio →
              </span>
            </div>
          </Link>
        ))}
      </div>
    </section>
  )
}


function Cell({ label, value, sub, accent }: {
  label: string; value: string; sub?: string; accent?: 'red' | 'green'
}) {
  const valueCls = accent === 'red'   ? 'text-red-300'
                 : accent === 'green' ? 'text-green-300'
                 : 'text-neutral-100'
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-neutral-500 mb-0.5">{label}</div>
      <div className={`font-mono text-sm md:text-base ${valueCls}`}>{value}</div>
      {sub && <div className="text-[10px] text-neutral-500 mt-0.5 font-mono">{sub}</div>}
    </div>
  )
}


function SkeletonTable() {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-4 animate-pulse space-y-3">
      {[0, 1, 2, 3, 4].map(i => (
        <div key={i} className="h-20 bg-neutral-950/60 rounded border border-neutral-800" />
      ))}
    </section>
  )
}


function FinePrint() {
  return (
    <footer className="text-xs text-neutral-500 leading-relaxed border-t border-neutral-800 pt-4">
      Projections scale validated backtest metrics linearly to your capital. Monthly
      range = best & worst single month in the backtest. Drawdown = the worst peak-to-trough
      loss observed in the backtest. Past performance does not guarantee future returns —
      Indian markets can and will surprise both ways. Sized at 1× cash, no leverage.
    </footer>
  )
}
