/**
 * PickCard — THE locked UI component for the Pick v1 contract.
 *
 * Single shared component used by:
 *   /power           (landing top-5 preview)
 *   /power/today     (authed top-100)
 *   /power/replay/*  (historical replays w/ actual outcomes)
 *   /power/live      (live decisions w/ ENTER/WAIT/SKIP overlay)
 *
 * Lock the layout HERE — never reinvent it elsewhere. If you need a new
 * variant, add a prop here. If you need a new section, expose it via prop.
 *
 * Locked by operator review 2026-05-14:
 *   - Pick v1 payload shape (lib/power-api.ts)
 *   - tier_color → Tailwind classes (TierBadge.tsx)
 *   - story rendered as a paragraph (no JSON/debug)
 *   - actual outcomes shown only when present (historical)
 *   - liveDecision overlay (decided_at_cycle badge) when passed
 */
import type { Pick, LiveDecision, PickPattern, PickActual, TierColor } from '@/lib/power-api'

const inr = (n: number) =>
  '₹' + Math.round(n).toLocaleString('en-IN')

// v2: signal-time tier badge (distinct from the rank/conviction tier).
const SIGNAL_TIER_CLASSES: Record<string, string> = {
  amber:  'bg-amber-500/15  text-amber-300  border-amber-500/40',
  green:  'bg-green-500/15  text-green-300  border-green-500/40',
  yellow: 'bg-yellow-500/15 text-yellow-300 border-yellow-500/40',
  orange: 'bg-orange-500/15 text-orange-300 border-orange-500/40',
  gray:   'bg-neutral-700/40 text-neutral-300 border-neutral-600',
  red:    'bg-red-500/15    text-red-300    border-red-500/40',
}

export function SignalTierBadge({ tier, color, reason }: {
  tier?: string | null; color?: TierColor | null; reason?: string | null
}) {
  if (!tier) return null
  const cls = SIGNAL_TIER_CLASSES[color ?? 'gray'] ?? SIGNAL_TIER_CLASSES.gray
  return (
    <span
      title={reason ?? undefined}
      className={['inline-flex items-center font-semibold rounded border font-mono',
                  'text-[10px] px-1.5 py-0.5', cls].join(' ')}
    >
      {tier.replace('-', ' ')}
    </span>
  )
}

// v2: the signal-day price numbers that drive the tier.
export function SignalDayStrip({ pick }: { pick: Pick }) {
  if (pick.signal_day_ret_pct == null && pick.two_day_ret_pct == null) return null
  return (
    <span className="text-[11px] text-neutral-400 font-mono whitespace-nowrap"
          title="Signal-day return and trailing 2-day return (tier drivers)">
      sig {fmtPct(pick.signal_day_ret_pct, { sign: true })}
      {' · '}2d {fmtPct(pick.two_day_ret_pct, { sign: true })}
    </span>
  )
}

const fmtPct = (n: number | null | undefined, opts: { sign?: boolean } = {}) => {
  if (n == null) return '—'
  const s = n.toFixed(2)
  return opts.sign && n > 0 ? `+${s}%` : `${s}%`
}

/**
 * Sprint 5c (Bug 4): Always show the SL/trail in plain English. Stop showing
 * raw `entry × 0.9299999999999999 (-7%)` which is a floating-point precision
 * leak that scares users. Display as "Entry − 7%" or, when a multiplier helps
 * the trader translate to a Kite limit price, "Entry × 0.93".
 */
const fmtPctSigned = (pct: number) => {
  const sign = pct >= 0 ? '+' : '−'
  return `${sign}${Math.abs(pct)}%`
}
const fmtEntryMultiplier = (pct: number) =>
  (1 + pct / 100).toFixed(2)       // -7 → "0.93"; +10 → "1.10"

type Props = {
  pick:           Pick
  /** 'expanded' (default) = full card. 'compact' = single row, click to expand */
  variant?:       'expanded' | 'compact'
  /** Optional live decision overlay — when present, renders ENTER/WAIT/SKIP + decided_at badge */
  liveDecision?:  LiveDecision
  /** Hide the score column (used in /live where action matters more than score) */
  hideScore?:     boolean
  className?:     string
}

export function PickCard({ pick, variant = 'expanded', liveDecision, hideScore, className = '' }: Props) {
  if (variant === 'compact') {
    return <PickCardCompact pick={pick} liveDecision={liveDecision} hideScore={hideScore} className={className} />
  }
  return <PickCardExpanded pick={pick} liveDecision={liveDecision} className={className} />
}

// ─────────────────────────────────────────────────────────────
// EXPANDED — the full card with story + patterns + outcomes
// ─────────────────────────────────────────────────────────────

function PickCardExpanded({ pick, liveDecision, className = '' }: {
  pick: Pick; liveDecision?: LiveDecision; className?: string
}) {
  return (
    <article
      className={[
        'bg-neutral-900 border border-neutral-800 rounded-lg p-4 md:p-5 space-y-4',
        className,
      ].join(' ')}
    >
      <PickHeader pick={pick} liveDecision={liveDecision} />
      <PickStory story={pick.story} />
      <PickPatterns patterns={pick.top_patterns} />
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <PickExpected expected={pick.expected} tierActionHint={pick.tier_action_hint} />
        {pick.actual ? <PickActuals actual={pick.actual} /> : <PickRiskControl risk={pick.risk} />}
      </div>
      {pick.actual && (
        <div className="pt-2 border-t border-neutral-800">
          <PickRiskControl risk={pick.risk} compact />
        </div>
      )}
    </article>
  )
}

// ─────────────────────────────────────────────────────────────
// COMPACT — one row, summarized
// ─────────────────────────────────────────────────────────────

function PickCardCompact({ pick, liveDecision, className = '' }: {
  pick: Pick; liveDecision?: LiveDecision; hideScore?: boolean; className?: string
}) {
  // Sprint 5c design changes 1+2: tier + score no longer render on the card;
  // rank is the only ordering cue users need. Both fields stay in the Pick
  // contract — live-tier rules + score are still used internally.
  return (
    <div
      className={[
        'grid grid-cols-[2rem_minmax(0,1fr)_auto] md:grid-cols-[2rem_minmax(0,1fr)_auto] gap-3 items-center',
        'bg-neutral-900 border border-neutral-800 rounded px-3 py-2 hover:bg-neutral-850 transition-colors',
        className,
      ].join(' ')}
    >
      <span className="text-xs text-neutral-500 font-mono">#{pick.rank}</span>
      <div className="min-w-0">
        <div className="flex items-baseline gap-2">
          <span className="font-mono font-medium text-neutral-100">{pick.symbol}</span>
          {pick.sector && <span className="text-xs text-neutral-500 truncate">{pick.sector}</span>}
        </div>
        <p className="text-xs text-neutral-400 truncate">{pick.story}</p>
      </div>
      {liveDecision && (
        <LiveActionBadge action={liveDecision.action}
                          decidedAt={liveDecision.decided_at_cycle} />
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────
// Subsections (exported individually for reuse on detail pages)
// ─────────────────────────────────────────────────────────────

export function PickHeader({ pick, liveDecision }: { pick: Pick; liveDecision?: LiveDecision }) {
  // Sprint 5c design changes 1+2: tier badge + score number removed from
  // the header. Rank is the sole ordering signal we show. Live action badge
  // stays — it's the ENTER/WAIT/SKIP decision, not a generic ordering cue.
  return (
    <header className="flex items-start justify-between gap-3 flex-wrap">
      <div className="flex items-baseline gap-2 flex-wrap min-w-0">
        <span className="text-xs text-neutral-500 font-mono">#{pick.rank}</span>
        <h3 className="text-lg md:text-xl font-bold text-neutral-100 font-mono">{pick.symbol}</h3>
        {pick.sector && (
          <span className="text-sm text-neutral-400 truncate">{pick.sector}</span>
        )}
        <SignalTierBadge tier={pick.signal_tier} color={pick.signal_tier_color}
                         reason={pick.signal_tier_reason} />
        <SignalDayStrip pick={pick} />
      </div>
      {liveDecision && (
        <div className="shrink-0">
          <LiveActionBadge action={liveDecision.action}
                            decidedAt={liveDecision.decided_at_cycle} />
        </div>
      )}
    </header>
  )
}

export function PickStory({ story }: { story: string }) {
  // Plain paragraph — story is already polished prose (sentence-cap'd, no JSON).
  return <p className="text-sm md:text-[15px] text-neutral-200 leading-relaxed">{story}</p>
}

export function PickPatterns({ patterns }: { patterns: PickPattern[] }) {
  return (
    <section>
      <h4 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-2">
        What the engine noticed
      </h4>
      <ol className="space-y-2">
        {patterns.map(p => (
          <li key={p.pattern_id} className="text-sm">
            <div className="flex items-baseline gap-2">
              <span className="text-neutral-500 font-mono text-xs shrink-0">{p.position}.</span>
              <span className="text-neutral-200">{p.trader_phrase}</span>
            </div>
            <div className="ml-5 mt-0.5 text-xs text-neutral-400">
              <span className="text-mint-300/90">{p.hit_phrase}</span>
              <span className="text-neutral-500 ml-2 hidden md:inline">
                (validated {p.mined_year}, OOS +{p.oos_lift_pp.toFixed(1)}pp)
              </span>
            </div>
          </li>
        ))}
      </ol>
    </section>
  )
}

export function PickExpected({ expected, tierActionHint }: {
  expected: Pick['expected']; tierActionHint?: string
}) {
  const rng = (r: [number, number]) =>
    r[0] === r[1] ? `${r[0]}%` : `${r[0]}-${r[1]}%`
  return (
    <section className="bg-neutral-950/60 border border-neutral-800 rounded p-3">
      <h4 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-2">
        Expected outcome <span className="text-neutral-600 normal-case tracking-normal">(from 2yr of similar setups)</span>
      </h4>
      <dl className="text-sm space-y-1">
        <div className="flex justify-between">
          <dt className="text-neutral-400">Within 5 days</dt>
          <dd className="text-green-300 font-mono">{rng(expected.d5_range)} green</dd>
        </div>
        <div className="flex justify-between">
          <dt className="text-neutral-400">Within 10 days</dt>
          <dd className="text-green-300 font-mono">{rng(expected.d10_range)} +5%</dd>
        </div>
        <div className="flex justify-between">
          <dt className="text-neutral-400">D+15 avg return</dt>
          <dd className="text-mint-300 font-mono">{rng(expected.d15_avg_range)}</dd>
        </div>
      </dl>
      {tierActionHint && (
        <p className="text-[11px] text-neutral-500 mt-2 pt-2 border-t border-neutral-800">
          Live rule: <span className="text-neutral-300">{tierActionHint}</span>
        </p>
      )}
    </section>
  )
}

export function PickActuals({ actual }: { actual: PickActual }) {
  const rows: Array<[string, number | undefined]> = [
    ['D+1',  actual.d1],
    ['D+3',  actual.d3],
    ['D+5',  actual.d5],
    ['D+10', actual.d10],
    ['D+15', actual.d15],
  ]
  const tagFor = (v: number | undefined) => {
    if (v == null) return null
    if (v >=  5) return <span className="text-green-400 text-[10px] ml-2 font-semibold">BIG WIN</span>
    if (v >   0) return <span className="text-green-500 text-[10px] ml-2">win</span>
    if (v <= -5) return <span className="text-red-400 text-[10px] ml-2 font-semibold">BIG LOSS</span>
    return <span className="text-red-500 text-[10px] ml-2">loss</span>
  }
  return (
    <section className="bg-neutral-950/60 border border-green-500/20 rounded p-3">
      <h4 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-2">
        What actually happened
      </h4>
      <dl className="text-sm space-y-1">
        {rows.map(([label, v]) => (
          <div key={label} className="flex justify-between items-baseline">
            <dt className="text-neutral-400 font-mono">{label}</dt>
            <dd className="font-mono">
              {v == null
                ? <span className="text-neutral-600">—</span>
                : <span className={v > 0 ? 'text-green-300' : 'text-red-300'}>
                    {fmtPct(v, { sign: true })}
                  </span>}
              {tagFor(v)}
            </dd>
          </div>
        ))}
      </dl>
    </section>
  )
}

export function PickRiskControl({ risk, compact = false }: {
  risk: Pick['risk']; compact?: boolean
}) {
  // Sprint 5c Bug 4: format SL/trail in plain English, not raw float math.
  // The 2-decimal multiplier (e.g. "0.93") helps traders set a Kite limit
  // price; the percentage gives the intent. We never print "0.9299999..."
  const slPctText      = fmtPctSigned(risk.stop_loss_pct)            // "−7%"
  const slMultiplier   = fmtEntryMultiplier(risk.stop_loss_pct)      // "0.93"
  const trailPctText   = fmtPctSigned(risk.trail_trigger_pct)        // "+10%"

  if (compact) {
    return (
      <p className="text-[11px] text-neutral-500">
        Risk: SL {slPctText}
        {' · '}trail at {trailPctText}
        {' · '}exit by day {risk.time_exit_days}
      </p>
    )
  }
  return (
    <section className="bg-neutral-950/60 border border-neutral-800 rounded p-3">
      <h4 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-2">
        Risk control
      </h4>
      <dl className="text-sm space-y-1">
        <div className="flex justify-between">
          <dt className="text-neutral-400">Stop loss</dt>
          <dd className="text-red-300 font-mono">
            Entry {slPctText} <span className="text-neutral-500">(× {slMultiplier})</span>
          </dd>
        </div>
        <div className="flex justify-between">
          <dt className="text-neutral-400">Trail trigger</dt>
          <dd className="text-mint-300 font-mono">activates at {trailPctText}</dd>
        </div>
        <div className="flex justify-between">
          <dt className="text-neutral-400">Time exit</dt>
          <dd className="text-neutral-300 font-mono">close by day {risk.time_exit_days}</dd>
        </div>
      </dl>
    </section>
  )
}

// ─────────────────────────────────────────────────────────────
// LiveActionBadge — ENTER / WAIT / SKIP overlay
// ─────────────────────────────────────────────────────────────

function LiveActionBadge({ action, decidedAt }: {
  action: 'ENTER' | 'WAIT' | 'SKIP'; decidedAt: string | null
}) {
  const styles = {
    ENTER: 'bg-green-500/20 text-green-200 border-green-500/50',
    WAIT:  'bg-yellow-500/20 text-yellow-200 border-yellow-500/50',
    SKIP:  'bg-neutral-700/40 text-neutral-300 border-neutral-600',
  }[action]
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-mono font-semibold border ${styles}`}
          title={decidedAt ? `Decided at ${decidedAt} IST — locked for the day` : ''}>
      {action}
      {decidedAt && <span className="text-[9px] opacity-75">@{decidedAt}</span>}
    </span>
  )
}
