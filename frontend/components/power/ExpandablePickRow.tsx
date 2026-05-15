'use client'

/**
 * ExpandablePickRow — compact row → click → expanded card (in-place).
 *
 * Perf-sensitive: a list of 100 of these renders. Optimisations:
 *   - <details> + <summary> primitive: native click toggling, no React state
 *   - Compact row inside <summary>: lazy renders the expanded body only when open
 *     (browsers don't even paint the closed <details> body in modern engines)
 *   - CSS content-visibility:auto on each row → off-screen rows skip layout/paint
 *   - max-height kept tight (~72px) so 12-15 fit per mobile viewport
 *
 * The expanded body reuses PickCard's expanded variant — single source of UI truth.
 */
import type { LiveDecision, Pick } from '@/lib/power-api'
import { PickCard } from './PickCard'

type Props = {
  pick:          Pick
  liveDecision?: LiveDecision
}

export function ExpandablePickRow({ pick, liveDecision }: Props) {
  return (
    <details
      className="group bg-neutral-900 border border-neutral-800 rounded
                  hover:border-neutral-700 [&[open]]:border-amber-500/30
                  transition-colors"
      style={{
        // skip layout/paint when off-screen — big win for 100-row lists
        contentVisibility: 'auto',
        // hint browser an estimate so scrollbar/anchors don't jump
        containIntrinsicSize: '0 72px',
      }}
    >
      <summary
        className="cursor-pointer list-none px-3 py-2 select-none
                    grid grid-cols-[2rem_minmax(0,1fr)_auto_auto_auto] md:grid-cols-[2rem_minmax(0,1fr)_8rem_6rem_5rem_auto] gap-3 items-center"
      >
        <span className="text-xs text-neutral-500 font-mono">#{pick.rank}</span>
        <div className="min-w-0">
          <div className="flex items-baseline gap-2">
            <span className="font-mono font-medium text-neutral-100">{pick.symbol}</span>
            {pick.sector && (
              <span className="text-xs text-neutral-500 truncate">{pick.sector}</span>
            )}
          </div>
          <p className="text-xs text-neutral-400 truncate group-[[open]]:hidden">
            {pick.story}
          </p>
        </div>
        <span className={[
          'inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border font-mono font-semibold',
          tierColorClasses(pick.tier_color),
        ].join(' ')}>
          {pick.tier_icon} {pick.tier}
        </span>
        <span className="text-xs text-neutral-500 font-mono text-right hidden md:block">
          {Math.round(pick.score)}
        </span>
        {liveDecision && (
          <span className={[
            'inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-mono font-semibold border',
            actionColorClasses(liveDecision.action),
          ].join(' ')}>
            {liveDecision.action}
            {liveDecision.decided_at_cycle && (
              <span className="text-[9px] opacity-75">@{liveDecision.decided_at_cycle}</span>
            )}
          </span>
        )}
        <span className="text-neutral-600 group-[[open]]:rotate-180 transition-transform" aria-hidden>
          ▾
        </span>
      </summary>

      {/* Expanded body — lazy by virtue of <details> not laying out closed content */}
      <div className="border-t border-neutral-800 p-3 md:p-4">
        <PickCard pick={pick} liveDecision={liveDecision} className="border-0 p-0 md:p-0" />
      </div>
    </details>
  )
}


function tierColorClasses(c: Pick['tier_color']): string {
  switch (c) {
    case 'amber':  return 'bg-amber-500/15 text-amber-300 border-amber-500/40'
    case 'green':  return 'bg-green-500/15 text-green-300 border-green-500/40'
    case 'yellow': return 'bg-yellow-500/15 text-yellow-300 border-yellow-500/40'
    case 'orange': return 'bg-orange-500/15 text-orange-300 border-orange-500/40'
    default:       return 'bg-neutral-700/40 text-neutral-300 border-neutral-600'
  }
}


function actionColorClasses(a: LiveDecision['action']): string {
  switch (a) {
    case 'ENTER': return 'bg-green-500/20 text-green-200 border-green-500/50'
    case 'WAIT':  return 'bg-yellow-500/20 text-yellow-200 border-yellow-500/50'
    case 'SKIP':  return 'bg-neutral-700/40 text-neutral-300 border-neutral-600'
  }
}
