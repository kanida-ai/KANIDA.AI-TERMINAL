/**
 * FeaturedReplayCard — landing-page tile for one of the 3 featured replays.
 *
 * Operator-locked metrics with tooltips for non-trader investors:
 *   WR  = Win Rate (% of picks profitable)
 *   D+5 = 5 days after entry
 *   D+15 = 15 days after entry
 *
 * Clicking the card routes to /power/replay/<date> for the full replay view.
 */
import Link from 'next/link'
import type { FeaturedReplaySummary } from '@/lib/power-api'
import { InfoTooltip } from './InfoTooltip'

const fmt = (n: number | null | undefined, suffix = '') =>
  n == null ? '—' : `${n.toFixed(n % 1 === 0 ? 0 : 1)}${suffix}`

const fmtSigned = (n: number | null | undefined, suffix = '%') => {
  if (n == null) return '—'
  return (n >= 0 ? '+' : '') + n.toFixed(2) + suffix
}

type Props = {
  replay: FeaturedReplaySummary
  /** Show a distinguishing accent — first card is the showcase */
  emphasis?: 'showcase' | 'standard'
}

export function FeaturedReplayCard({ replay, emphasis = 'standard' }: Props) {
  const accent =
    emphasis === 'showcase'
      ? 'border-amber-500/50 shadow-lg shadow-amber-500/5'
      : 'border-neutral-800 hover:border-neutral-700'
  return (
    <Link
      href={`/power/replay/${encodeURIComponent(replay.replay_date)}`}
      className={[
        'group block bg-neutral-900 border rounded-lg p-5',
        'transition-all duration-200 hover:bg-neutral-850',
        accent,
      ].join(' ')}
    >
      <header className="flex items-baseline justify-between mb-1">
        <h3 className="text-base md:text-lg font-bold text-amber-300 group-hover:text-amber-200">
          {replay.title ?? replay.replay_date}
        </h3>
        <time className="text-xs text-neutral-500 font-mono">{replay.replay_date}</time>
      </header>

      {replay.hook && (
        <p className="text-sm text-neutral-400 mb-4 leading-relaxed">{replay.hook}</p>
      )}

      <dl className="grid grid-cols-3 gap-2 text-center text-sm border-t border-neutral-800 pt-3">
        <Metric
          label={<>WR <span className="text-neutral-600">D+5</span></>}
          value={fmt(replay.wr_d5, '%')}
          tooltip={<>Win Rate at D+5: percent of picks that closed positive 5 trading days after entry.</>}
        />
        <Metric
          label={<>WR <span className="text-neutral-600">D+15</span></>}
          value={fmt(replay.wr_d15, '%')}
          tooltip={<>Win Rate at D+15: percent of picks that closed positive 15 trading days after entry.</>}
        />
        <Metric
          label={<>Avg <span className="text-neutral-600">D+15</span></>}
          value={fmtSigned(replay.avg_d15)}
          tooltip={<>Average return across all picks at D+15. The headline outcome — how the day actually played out.</>}
        />
      </dl>

      <footer className="mt-3 text-xs text-neutral-500 flex justify-between">
        <span>{replay.n_picks} picks</span>
        <span className="text-amber-400 group-hover:text-amber-300">See full replay →</span>
      </footer>
    </Link>
  )
}

function Metric({ label, value, tooltip }: {
  label: React.ReactNode; value: string; tooltip: React.ReactNode
}) {
  return (
    <div>
      <div className="text-xs text-neutral-500 mb-0.5 flex items-center justify-center gap-1">
        {label}
        <InfoTooltip text={tooltip} />
      </div>
      <div className="text-lg md:text-xl font-bold text-neutral-100 font-mono">{value}</div>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────
// Skeleton — render while featured payload is loading
// ─────────────────────────────────────────────────────────────

export function FeaturedReplayCardSkeleton() {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded-lg p-5 animate-pulse">
      <div className="flex items-baseline justify-between mb-2">
        <div className="h-5 bg-neutral-800 rounded w-32" />
        <div className="h-3 bg-neutral-800 rounded w-20" />
      </div>
      <div className="h-3 bg-neutral-800 rounded w-full mb-1.5" />
      <div className="h-3 bg-neutral-800 rounded w-4/5 mb-4" />
      <div className="grid grid-cols-3 gap-2 border-t border-neutral-800 pt-3">
        {[0, 1, 2].map(i => (
          <div key={i} className="text-center">
            <div className="h-3 bg-neutral-800 rounded w-12 mx-auto mb-1.5" />
            <div className="h-6 bg-neutral-800 rounded w-16 mx-auto" />
          </div>
        ))}
      </div>
      <div className="mt-3 flex justify-between">
        <div className="h-3 bg-neutral-800 rounded w-12" />
        <div className="h-3 bg-neutral-800 rounded w-24" />
      </div>
    </div>
  )
}
