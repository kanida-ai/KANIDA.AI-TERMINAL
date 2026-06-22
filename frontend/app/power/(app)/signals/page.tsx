/**
 * /power/signals — persona-aware Signals (thin real shell for Phase 1a).
 *
 * The full persona-aware Top 10 + 7/14/20/30d tracker is Phase 1b. For now this
 * is a REAL shell (not Launch-Pending): it states the one LIVE engine honestly
 * and routes to the existing, working Top 10 surface (/power/today) so the daily
 * job is one click away. No faked persona switches.
 */
import Link from 'next/link'
import { IconArrowRight } from '@/components/power/shell/icons'

export const dynamic = 'force-dynamic'

export default function SignalsPage() {
  return (
    <div className="space-y-8">
      <header className="space-y-2">
        <h1 className="text-2xl md:text-3xl font-semibold text-neutral-100">Signals</h1>
        <p className="text-neutral-400 max-w-2xl leading-relaxed">
          Today&apos;s Top 10 for your trading style — rank, quality tier,
          why-selected, and what happened to past signals.
        </p>
      </header>

      {/* LIVE engine */}
      <section className="rounded-xl border border-neutral-800 bg-neutral-950/60 p-5 space-y-4">
        <div className="flex items-center gap-2">
          <span className="inline-flex items-center gap-1.5 text-[11px] font-mono uppercase tracking-wider
                           text-mint-300 bg-mint-400/10 border border-mint-400/30 rounded-full px-2.5 py-1">
            <span className="w-1.5 h-1.5 rounded-full bg-mint-400" aria-hidden /> Live
          </span>
          <h2 className="text-lg font-semibold text-neutral-100">Falcon Top 10 Swing</h2>
        </div>
        <p className="text-sm text-neutral-400 leading-relaxed">
          The validated engine: a ~7-day-hold swing strategy with a -7% stop, full
          per-pick explainability (why it fired, the stock&apos;s own track record,
          sector context) and qualitative quality tiers. Weekly Swing is a variant
          of the same engine.
        </p>
        <Link
          href="/power/today"
          className="inline-flex items-center gap-2 rounded-lg bg-mint-400 text-neutral-950 px-4 py-2.5
                     text-sm font-semibold hover:bg-mint-300 transition-colors"
        >
          See today&apos;s Top 10 <IconArrowRight size={15} />
        </Link>
      </section>

      {/* What's coming in 1b */}
      <section className="rounded-xl border border-neutral-800 bg-neutral-950/40 p-5 space-y-2">
        <h3 className="text-xs font-mono uppercase tracking-wider text-neutral-500">Coming next (1b)</h3>
        <ul className="space-y-2 text-sm text-neutral-400">
          <li className="flex gap-2.5"><span className="mt-1.5 w-1.5 h-1.5 rounded-full bg-mint-400/70 shrink-0" />Persona-aware ranking inside this page (no page-hop).</li>
          <li className="flex gap-2.5"><span className="mt-1.5 w-1.5 h-1.5 rounded-full bg-mint-400/70 shrink-0" />Signal tracker — what happened 7 / 14 / 20 / 30 days after each signal.</li>
          <li className="flex gap-2.5"><span className="mt-1.5 w-1.5 h-1.5 rounded-full bg-mint-400/70 shrink-0" />Whether a setup is strengthening or weakening after it fired.</li>
        </ul>
        <p className="text-xs text-neutral-600 pt-1">
          BTST, Intraday and Long-Term personas are Launch-Pending — they need engines we haven&apos;t validated yet.
        </p>
      </section>
    </div>
  )
}
