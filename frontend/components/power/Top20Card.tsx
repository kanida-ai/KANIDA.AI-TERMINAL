/**
 * Top10Card — plain-English Falcon Top 10 pick card.
 *
 * Rewritten 2026-05-24 per the locked UX spec. Banned from this component:
 *   "pp", "HR", "MAE", "lift", "regime", "rotation", "confluence",
 *   "multi-factor", "capitulation", LOW/MEDIUM/HIGH risk labels.
 *
 * Layout (vertical, top → bottom):
 *
 *   #N  SYMBOL · sector                            [Rating chip]
 *   ─────────────────────────────────────────────────────────────
 *   Why we're picking this
 *     [synthesis paragraph, plain English]
 *
 *   Historical track record (since 2021)
 *     • Works about 7 times out of 10
 *     • When it works → +14%.  When it doesn't → -8%.
 *     • Worst dip during the trade: -30%
 *
 *   What the sector is doing
 *     [plain-English sector paragraph]
 *
 *   [optional: "Heads up: weaker track record..." line]
 *
 *   What to do
 *     Buy at tomorrow's open (~₹161.50)
 *     Position size: ₹50,000 (~310 shares)
 *     Exit if it drops to ₹150 (-7% stop)
 *     We'll hold for up to 7 trading days
 */
'use client'

import { useState } from 'react'
import type { Top20Pick, Rating } from '@/lib/falcon-top20-types'

type Props = {
  pick:               Top20Pick
  defaultExpanded?:   boolean
}

export function Top20Card({ pick, defaultExpanded = false }: Props) {
  const [open, setOpen] = useState(defaultExpanded)
  return (
    <article className={['rounded-2xl border transition-all duration-200', open ? 'bg-neutral-900/80 border-mint-400/40 shadow-[0_0_32px_-12px_rgba(63,227,164,0.30)]' : 'bg-neutral-900 border-neutral-800 hover:border-mint-400/30'].join(' ')}>
      <HeaderStrip pick={pick} open={open} onToggle={() => setOpen(!open)} />
      {open && (
        <div className="px-4 md:px-8 pb-6 md:pb-8 space-y-7">
          <SectionWhy pick={pick} />
          <SectionTrackRecord pick={pick} />
          <SectionSector pick={pick} />
          {pick.weaker_history_message && (
            <HeadsUp message={pick.weaker_history_message} />
          )}
          <SectionWhatToDo pick={pick} />
        </div>
      )}
    </article>
  )
}

// ── Header strip (collapsed view) ────────────────────────────────────────

function HeaderStrip({ pick, open, onToggle }: { pick: Top20Pick; open: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      onClick={onToggle}
      className="w-full px-4 md:px-8 py-4 md:py-5 flex items-center gap-4 text-left"
      aria-expanded={open}
    >
      <span className="text-2xl md:text-3xl font-mono font-bold text-mint-400 w-12 md:w-14 shrink-0">
        #{pick.rank}
      </span>
      <span className="flex-1 min-w-0">
        <span className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="text-lg md:text-xl font-bold text-white">{pick.symbol}</span>
          <span className="text-xs md:text-sm text-white/55">· {pick.sector}</span>
        </span>
        <span className="mt-1 flex flex-wrap items-center gap-2">
          <RatingChip rating={pick.rating} />
          <SignalTierChip pick={pick} />
          <SignalDayMini pick={pick} />
        </span>
      </span>
      <span className="shrink-0 text-xs text-white/40 hidden md:block">
        {open ? '▴ collapse' : '▾ expand'}
      </span>
    </button>
  )
}

const SIGNAL_TIER_CHIP: Record<string, string> = {
  'PREMIUM-Pullback':    'bg-teal-400/15 text-teal-200 border-teal-400/40',
  'PREMIUM-Compression': 'bg-teal-400/15 text-teal-200 border-teal-400/40',
  'ENTERPRISE-Dryup':    'bg-green-400/15 text-green-300 border-green-400/40',
  'GOLD':                'bg-amber-400/15 text-amber-200 border-amber-400/40',
  'GOLD-baseline':       'bg-amber-400/15 text-amber-200 border-amber-400/40',
  'STANDARD':            'bg-white/10 text-white/60 border-white/20',
  'STANDARD-weak':       'bg-white/10 text-white/60 border-white/20',
  'AVOID':               'bg-red-500/15 text-red-300 border-red-500/40',
}

function SignalTierChip({ pick }: { pick: Top20Pick }) {
  const tier = pick.signal_tier
  if (!tier) return null
  const cls = SIGNAL_TIER_CHIP[tier] ?? SIGNAL_TIER_CHIP['STANDARD']
  return (
    <span title={pick.signal_tier_reason ?? undefined}
          className={['inline-flex items-center px-2.5 py-0.5 rounded text-[11px] font-semibold font-mono border', cls].join(' ')}>
      {tier.replace('-', ' ')}
    </span>
  )
}

function SignalDayMini({ pick }: { pick: Top20Pick }) {
  const sig = pick.flags?.day_return_pct
  const d2  = pick.two_day_ret_pct
  if (sig == null && d2 == null) return null
  const f = (n: number | null | undefined) =>
    n == null ? '—' : (n > 0 ? '+' : '') + n.toFixed(2) + '%'
  return (
    <span className="text-[11px] font-mono text-white/45 whitespace-nowrap"
          title="Signal-day return · trailing 2-day return (tier drivers)">
      sig <span className="text-white/75">{f(sig)}</span> · 2d <span className="text-white/75">{f(d2)}</span>
    </span>
  )
}

function RatingChip({ rating }: { rating: Rating }) {
  const cls =
    rating === 'Strong Buy'
      ? 'bg-mint-400/[0.15] text-mint-300 border-mint-400/40'
      : 'bg-amber-400/[0.10] text-amber-200 border-amber-400/30'
  const dot =
    rating === 'Strong Buy'
      ? 'bg-mint-400'
      : 'bg-amber-300'
  return (
    <span className={['inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded text-[11px] font-semibold uppercase tracking-wide border', cls].join(' ')}>
      <span className={['inline-block w-1.5 h-1.5 rounded-full', dot].join(' ')} />
      {rating}
    </span>
  )
}

// ── Section: Why ─────────────────────────────────────────────────────────

function SectionWhy({ pick }: { pick: Top20Pick }) {
  return (
    <section>
      <h3 className="text-base md:text-lg font-semibold text-white mb-2">
        Why we&apos;re picking this
      </h3>
      <p className="text-sm md:text-base text-white/85 leading-relaxed">
        {pick.bucket1.synthesis}
      </p>
    </section>
  )
}

// ── Section: Track record ────────────────────────────────────────────────

function SectionTrackRecord({ pick }: { pick: Top20Pick }) {
  const bt = pick.per_stock_backtest
  const sym = pick.symbol

  // No historical Top 10 appearances for this stock — be honest.
  if (!bt || bt.n_trades === 0) {
    return (
      <section>
        <h3 className="text-base md:text-lg font-semibold text-white mb-2">
          Historical track record
        </h3>
        <p className="text-sm md:text-base text-white/75 leading-relaxed">
          <span className="text-mint-300 font-semibold">First-time Top 10 pick for {sym}.</span>{' '}
          This stock hasn&apos;t appeared in our Falcon Top 10 picks before under the
          locked rules (7-day hold, −7% stop). No per-stock historical track record
          yet — we&apos;ll see how it plays out.
        </p>
      </section>
    )
  }

  const wr = bt.win_rate_pct ?? 0
  const wrText = wr >= 95 ? 'every time so far' : oneOutOfTen(wr)

  const winLine = bt.avg_win_pct !== null && bt.avg_win_pct !== 0
    ? `+${bt.avg_win_pct.toFixed(1)}%`
    : null
  const lossLine = bt.avg_loss_pct !== null && bt.avg_loss_pct !== 0
    ? `${bt.avg_loss_pct.toFixed(1)}%`
    : null
  const worstLossText = bt.worst_loss_pct !== null
    ? `${bt.worst_loss_pct.toFixed(1)}%`
    : null
  const stoppedOut = bt.worst_loss_pct !== null && bt.worst_loss_pct <= -6.5
  const thinSample = bt.n_trades < 5

  // Date range — show as "2021 → 2025" or just the year span
  const firstYear = bt.first_trade_date?.slice(0, 4)
  const lastYear  = bt.last_trade_date?.slice(0, 4)
  const dateSpan  = firstYear && lastYear
    ? (firstYear === lastYear ? firstYear : `${firstYear}–${lastYear}`)
    : 'since 2021'

  return (
    <section>
      <h3 className="text-base md:text-lg font-semibold text-white mb-3">
        Historical track record
      </h3>
      <p className="text-sm md:text-base text-white/85 leading-relaxed mb-3">
        When <span className="font-semibold text-white">{sym}</span> has appeared in our
        Falcon Top 10 picks before ({dateSpan}), it has worked{' '}
        <span className="text-mint-300 font-semibold">{wrText}</span>{' '}
        ({bt.n_wins} of {bt.n_trades} historical trades, {Math.round(wr)}% win rate).
        {thinSample && (
          <span className="text-amber-200/70">
            {' '}Thin sample — only {bt.n_trades} historical trade{bt.n_trades === 1 ? '' : 's'} so far; treat the numbers with caution.
          </span>
        )}
      </p>
      <ul className="space-y-1.5 text-sm md:text-base text-white/85">
        {(winLine || lossLine) && (
          <li className="flex gap-2">
            <span className="text-mint-400 shrink-0">•</span>
            <span>
              {winLine && (
                <>When it worked → <span className="text-mint-300 font-semibold">{winLine}</span> per trade.{' '}</>
              )}
              {lossLine && (
                <>When it didn&apos;t → <span className="text-red-300 font-semibold">{lossLine}</span> per trade.</>
              )}
              {!lossLine && bt.n_losses === 0 && (
                <span className="text-mint-300/70"> No losing trades on record yet.</span>
              )}
            </span>
          </li>
        )}
        {worstLossText && (
          <li className="flex gap-2">
            <span className="text-mint-400 shrink-0">•</span>
            <span className="text-white/85">
              Worst single trade:{' '}
              <span className={['font-semibold', stoppedOut ? 'text-amber-300' : 'text-red-300'].join(' ')}>
                {worstLossText}
              </span>
              {stoppedOut && (
                <span className="text-white/55"> (hit the −7% stop — exactly what the stop is there to do)</span>
              )}
              .
            </span>
          </li>
        )}
      </ul>
    </section>
  )
}

function oneOutOfTen(pct: number): string {
  // "7 times out of 10" style — round to nearest integer in [1, 10]
  const n = Math.max(1, Math.min(10, Math.round(pct / 10)))
  return `${n} times out of 10`
}

// ── Section: Sector ──────────────────────────────────────────────────────

function SectionSector({ pick }: { pick: Top20Pick }) {
  return (
    <section>
      <h3 className="text-base md:text-lg font-semibold text-white mb-2">
        What the sector is doing
      </h3>
      <p className="text-sm md:text-base text-white/85 leading-relaxed">
        {pick.bucket3.narrative}
      </p>
    </section>
  )
}

// ── Heads-up flag (only when smaller_position triggers fired) ────────────

function HeadsUp({ message }: { message: string }) {
  return (
    <section className="p-3 md:p-4 bg-amber-400/[0.08] border border-amber-400/30 rounded-lg flex gap-3">
      <span aria-hidden className="text-base shrink-0">{'⚠'}</span>
      <p className="text-sm text-amber-200 leading-relaxed">{message}</p>
    </section>
  )
}

// ── Section: What to do ──────────────────────────────────────────────────

function SectionWhatToDo({ pick }: { pick: Top20Pick }) {
  const a = pick.action
  return (
    <section className="border-t border-neutral-800 pt-5 md:pt-6">
      <h3 className="text-base md:text-lg font-semibold text-white mb-3">
        What to do
      </h3>
      <ul className="space-y-2 text-sm md:text-base">
        <li className="flex items-baseline gap-3">
          <span className="text-mint-400 shrink-0 text-lg leading-none">→</span>
          <span className="font-semibold text-mint-300">{a.entry_text}</span>
        </li>
        <li className="flex items-baseline gap-3">
          <span className="text-mint-400 shrink-0 text-lg leading-none">→</span>
          <span className="text-white/85">{a.position_text}</span>
        </li>
        <li className="flex items-baseline gap-3">
          <span className="text-mint-400 shrink-0 text-lg leading-none">→</span>
          <span className="text-white/85">{a.stop_text}</span>
        </li>
        <li className="flex items-baseline gap-3">
          <span className="text-mint-400 shrink-0 text-lg leading-none">→</span>
          <span className="text-white/85">{a.hold_text}</span>
        </li>
      </ul>
    </section>
  )
}
