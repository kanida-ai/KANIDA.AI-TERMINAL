/**
 * PortfolioCard — landing-page tile for one of the 5 V3-locked personas.
 *
 * Operator UX lock 2026-05-16:
 *   - Persona name + plain-English tagline
 *   - Risk-tier badge (Lowest / Low-Med / Medium / Med-High / Highest)
 *   - Big LIVE return % (since Jan 1, 2026) — the audit-locked "Historical
 *     Average" number lives on the detail page, not here
 *   - Mini sparkline
 *   - One-line live status (open positions + win rate)
 *   - Disclosure flag on the card if persona carries one (P5 BTST)
 */
import Link from 'next/link'
import type { PortfolioSummary } from '@/lib/power-api'
import { EquitySparkline } from './EquitySparkline'

const fmtPct = (n: number | null | undefined, opts: { sign?: boolean } = {}) => {
  if (n == null) return '—'
  const s = n.toFixed(1)
  return opts.sign && n >= 0 ? `+${s}%` : `${s}%`
}

type Props = {
  summary:       PortfolioSummary
  sparklinePoints?: Array<{ trade_date: string; total_equity: number }>
  /**
   * Base route the card links to. Default = the legacy /power/portfolios listing.
   * The AI-native shell passes /power/co-trading so the card opens the in-shell
   * persona detail (co-trading/[slug]) without leaving the shell. Backward-
   * compatible: the legacy listing omits it and keeps its original target.
   */
  hrefBase?:     string
}

export function PortfolioCard({ summary: p, sparklinePoints, hrefBase = '/power/portfolios' }: Props) {
  const live    = p.live
  const ret     = live?.reinvest.total_return_pct ?? null
  const retCls  = ret == null
    ? 'text-neutral-300'
    : ret >= 0 ? 'text-green-300' : 'text-red-300'

  // Fix 2 (2026-05-16): Risk-tier badges removed from cards (visual noise).
  // BTST gets a single plain-text warning line instead — operator-locked copy.
  const isBtst = p.slug === 'btst-trader'

  // Fix 7 (2026-05-16): Daily + BTST get an "Auto Trade — Coming Soon" CTA
  // because they benefit most from automation. Patient / Weekly / Monthly skip.
  const showAutoTradeCTA = p.slug === 'daily-trader' || isBtst

  return (
    <Link
      href={`${hrefBase}/${p.slug}`}
      className="block bg-neutral-900 border border-neutral-800 rounded-lg p-4 md:p-5
                  hover:bg-neutral-850 hover:border-mint-500/40 transition-all duration-200
                  group space-y-3"
    >
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <h3 className="text-base md:text-lg font-bold text-neutral-50 group-hover:text-mint-300 transition-colors">
            {p.name}
          </h3>
          <p className="text-sm text-neutral-300 leading-relaxed mt-0.5">{p.tagline}</p>
        </div>
        <div className="text-right shrink-0">
          <div className="text-[10px] uppercase tracking-wider text-neutral-400 mb-0.5">
            Live return
          </div>
          <div className={`text-2xl md:text-3xl font-bold font-mono leading-none ${retCls}`}>
            {fmtPct(ret, { sign: true })}
          </div>
          <div className="text-[10px] text-neutral-400 mt-0.5">since {p.start_date}</div>
        </div>
      </header>

      {isBtst && (
        <p className="text-[12px] md:text-[13px] italic text-mint-300/80 leading-snug">
          ⚠️ Highest-return strategy. Also lost 54% in 2021. Not for risk-averse traders.
        </p>
      )}

      {sparklinePoints && sparklinePoints.length > 1 && (
        <div className="h-12">
          <EquitySparkline points={sparklinePoints}
                            positive={ret == null ? true : ret >= 0} />
        </div>
      )}

      <footer className="flex items-baseline justify-between text-sm text-neutral-300 border-t border-neutral-800 pt-3 gap-3 flex-wrap">
        <span>
          {live
            ? <>
                <span className="text-neutral-100 font-mono">{live.n_open_positions}</span> open ·{' '}
                <span className="text-neutral-100 font-mono">{p.validated.win_rate_pct ?? '—'}%</span> win rate
              </>
            : 'Engine starts trading on next EOD cycle'}
        </span>
        <span className="flex items-center gap-3">
          {showAutoTradeCTA && <AutoTradeCTA />}
          <span className="text-mint-300 group-hover:text-mint-200 text-sm font-semibold">
            See live trades →
          </span>
        </span>
      </footer>
    </Link>
  )
}


// Fix 7 — muted "Auto Trade — Coming Soon" pill with hover tooltip. Server-
// component-safe (no event handler — the explainer is in title="" only).
// Clicking it follows the parent <Link> to the dashboard; that's fine.
function AutoTradeCTA() {
  return (
    <span
      className="text-[11px] px-2 py-1 rounded border border-neutral-700 text-neutral-400
                  font-mono cursor-help hover:border-mint-500/40 hover:text-mint-300 transition-colors"
      title={
        "Auto Trade will execute entries, stops, and trailing rules automatically — "
        + "no manual screen time needed. Currently in roadmap. Sign in for waitlist priority."
      }
    >
      Auto Trade — Coming Soon
    </span>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// RiskBadge — small chip with colour per tier
// ──────────────────────────────────────────────────────────────────────────

export function RiskBadge({ tier, color }: {
  tier: string | null | undefined
  color: string | null | undefined
}) {
  if (!tier) return null
  const cls = {
    green:  'bg-green-500/15  text-green-300  border-green-500/40',
    amber:  'bg-mint-500/15  text-mint-300  border-mint-500/40',
    orange: 'bg-orange-500/15 text-orange-300 border-orange-500/40',
    red:    'bg-red-500/15    text-red-300    border-red-500/40',
  }[color ?? 'amber'] ?? 'bg-neutral-700/40 text-neutral-200 border-neutral-600'
  return (
    <span className={`inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border font-mono font-semibold uppercase tracking-wider ${cls}`}
          title={`Risk tier: ${tier}`}>
      {tier}
    </span>
  )
}


function WarningChip() {
  return (
    <span className="inline-flex items-center gap-0.5 text-[10px] px-1.5 py-0.5 rounded border
                      bg-red-500/15 text-red-300 border-red-500/50 font-mono font-bold"
          title="High-risk persona — see disclosure on detail page">
      ⚠ high risk
    </span>
  )
}
