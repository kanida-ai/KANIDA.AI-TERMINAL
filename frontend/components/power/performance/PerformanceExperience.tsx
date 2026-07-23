'use client'

/**
 * PerformanceExperience — proof + replay outcomes inside the AI-native shell.
 *
 * MIGRATION (2026-06-23):
 *   • Tab "The proof" → the ₹30L→₹1.05Cr 3.3-year walk-forward credibility
 *     content (reused 1:1 from /power/credibility — static, operator-locked).
 *   • Tab "Replay outcomes" → reuses ExpandablePickRow (the /power/replay/[date]
 *     surface) to show D+1/3/5/10/15 outcomes for any date, with an in-shell
 *     date navigator that routes to /power/performance?date=YYYY-MM-DD.
 *
 * The legacy /power/credibility and /power/replay/[date] routes stay intact as
 * fallback. Nothing is fabricated — replay rows come from PowerAPI.replayForDate.
 */
import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { ExpandablePickRow } from '@/components/power/ExpandablePickRow'
import type { ReplayPayload } from '@/lib/power-api'

type Props = {
  /** Pre-fetched replay payload for the active date, when one was requested. */
  replay:      ReplayPayload | null
  replayDate:  string | null
  replayError: string | null
  authed:      boolean
}

type TabKey = 'proof' | 'replay'

export function PerformanceExperience({ replay, replayDate, replayError, authed }: Props) {
  // Default to the replay tab when a date was deep-linked from Signals.
  const [tab, setTab] = useState<TabKey>(replayDate ? 'replay' : 'proof')

  return (
    <div className="flex flex-col h-full min-h-0">
      <header className="shrink-0 px-4 md:px-8 lg:px-10 pt-20 md:pt-8 pb-4 border-b border-neutral-900">
        <div className="max-w-3xl mx-auto w-full">
          <h1 className="text-2xl md:text-3xl font-semibold text-neutral-100">Performance</h1>
          <p className="text-sm text-neutral-400 mt-1 max-w-2xl leading-relaxed">
            Falcon&apos;s walk-forward proof, and what actually happened to past
            signals — wins and losses both shown.
          </p>
          <div className="mt-4 inline-flex items-center gap-1 rounded-lg border border-neutral-800 bg-neutral-950/60 p-1">
            <TabButton active={tab === 'proof'} onClick={() => setTab('proof')}>The proof</TabButton>
            <TabButton active={tab === 'replay'} onClick={() => setTab('replay')}>Replay outcomes</TabButton>
          </div>
        </div>
      </header>

      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="max-w-3xl mx-auto w-full px-4 md:px-8 lg:px-10 pt-6 pb-16">
          {tab === 'proof'
            ? <ProofTab />
            : <ReplayTab replay={replay} replayDate={replayDate} replayError={replayError} authed={authed} />
          }
        </div>
      </div>
    </div>
  )
}

function TabButton({ active, onClick, children }: {
  active: boolean; onClick: () => void; children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={[
        'inline-flex items-center rounded-md px-3.5 py-1.5 text-sm font-semibold transition-colors',
        active ? 'bg-mint-400/10 text-mint-300' : 'text-neutral-400 hover:text-neutral-100 hover:bg-neutral-900',
      ].join(' ')}
    >
      {children}
    </button>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// PROOF tab — the credibility content (reused 1:1 from /power/credibility)
// ─────────────────────────────────────────────────────────────────────────

const YEARS: Array<{ year: string; return_pct: number; trades: number; max_dd_pct: number }> = [
  { year: '2023', return_pct: 83, trades: 178, max_dd_pct: -7.4 },
  { year: '2024', return_pct: 91, trades: 165, max_dd_pct: -9.1 },
  { year: '2025', return_pct: 53, trades: 142, max_dd_pct: -10.2 },
  { year: '2026', return_pct: 23, trades: 88,  max_dd_pct: -6.8 },  // partial — through May
]

function ProofTab() {
  return (
    <article className="space-y-10 md:space-y-14 py-2">
      <header>
        <p className="text-xs tracking-[0.2em] uppercase text-mint-300 mb-2">The proof</p>
        <h2 className="text-3xl md:text-4xl font-bold tracking-tight leading-tight">
          ₹30 L → ₹1.05 Cr in 3.3 years.<br />
          <span className="text-neutral-400">Audited, not optimized.</span>
        </h2>
        <p className="mt-4 text-base md:text-lg text-neutral-400">
          A walk-forward simulation of the same engine that emits today&apos;s picks —
          applied to historical NSE data, year by year, without ever peeking at the future.
        </p>
      </header>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat label="Starting capital"  value="₹30 L"    accent="neutral" />
        <Stat label="Ending capital"    value="₹1.05 Cr" accent="amber" />
        <Stat label="Total return"      value="+250%"    accent="green" />
        <Stat label="Peak drawdown"     value="-10.2%"   accent="red" />
        <Stat label="Years positive"    value="4 / 4"    accent="green" />
        <Stat label="Total trades"      value="573"      accent="neutral" />
        <Stat label="Walk-forward span" value="3.3 yr"   accent="neutral" />
        <Stat label="Hit-rate (D+5)"    value="58%"      accent="neutral" />
      </section>

      <section className="space-y-3">
        <h3 className="text-lg md:text-xl font-bold text-neutral-100">How we tested it</h3>
        <p className="text-sm md:text-base text-neutral-300 leading-relaxed">
          We used <span className="text-neutral-100 font-semibold">walk-forward</span> simulation:
          at the start of each test month, the engine was trained ONLY on patterns validated
          before that month, then it generated picks forward for the next month. Repeat for
          every month from January 2023 to May 2026.
        </p>
        <p className="text-sm md:text-base text-neutral-300 leading-relaxed">
          This is the hardest test in quant — the engine never sees the future when deciding
          what to buy. The patterns it relies on are the same ones it tells you about on each
          pick&apos;s &quot;What the engine noticed&quot; section.
        </p>
        <ul className="text-sm text-neutral-400 space-y-1.5 list-disc list-inside marker:text-mint-500/60">
          <li>Universe: 505 NSE stocks (Nifty 500)</li>
          <li>Position sizing: 6% of equity per trade, max 14 concurrent</li>
          <li>Costs included: 0.05% brokerage + 0.025% STT + 0.0175% exchange + 0.1% slippage</li>
          <li>Stop loss: 7% from entry · trail trigger: +10% · time exit: 30 days</li>
          <li>Re-mining cadence: weekly (Saturday) — patterns refreshed every 7 days</li>
        </ul>
      </section>

      <section className="space-y-3">
        <h3 className="text-lg md:text-xl font-bold text-neutral-100">Year by year</h3>
        <p className="text-sm text-neutral-400">
          Every year of the test was profitable. 2026 is partial (through May 15) — the engine is still running.
        </p>
        <div className="rounded-lg border border-neutral-800 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-neutral-950/60 text-neutral-500 text-xs uppercase tracking-wider">
                <th className="text-left p-3">Year</th>
                <th className="text-right p-3">Return</th>
                <th className="text-right p-3 hidden md:table-cell">Trades</th>
                <th className="text-right p-3">Max drawdown</th>
              </tr>
            </thead>
            <tbody>
              {YEARS.map(y => (
                <tr key={y.year} className="border-t border-neutral-800 hover:bg-neutral-900/60">
                  <td className="p-3 font-mono text-neutral-200">
                    {y.year}{y.year === '2026' && <span className="ml-2 text-[10px] text-neutral-500">(partial)</span>}
                  </td>
                  <td className="p-3 text-right font-mono text-green-300">+{y.return_pct}%</td>
                  <td className="p-3 text-right font-mono text-neutral-400 hidden md:table-cell">{y.trades}</td>
                  <td className="p-3 text-right font-mono text-red-300">{y.max_dd_pct.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-neutral-500">
          Return = compounded equity at year end vs. year start. Drawdown = peak-to-trough on
          rolling daily equity. Source: <span className="font-mono">engine/backtest/run_backtest.py</span>,
          runs auditable from the operator console.
        </p>
      </section>

      <section className="space-y-3 border border-neutral-800 rounded-lg p-5 bg-neutral-950/40">
        <h3 className="text-lg font-bold text-mint-300">What we don&apos;t hide</h3>
        <ul className="text-sm text-neutral-300 space-y-2 leading-relaxed">
          <li>
            <span className="text-neutral-100 font-semibold">Replay outcomes</span> let you inspect
            any past trading day&apos;s picks and see exactly what happened — we don&apos;t pre-screen which day you pick.
          </li>
          <li>
            The <span className="text-neutral-100 font-semibold">Win Rate</span> headline numbers are{' '}
            <span className="text-neutral-400">net of stop-outs and time exits</span> — a closed -5% loss
            counts as a loss, not a &quot;temporary mark&quot;.
          </li>
          <li>
            Two years of operator memory notes catalogue every <span className="text-neutral-400">regime
            breakdown</span> the engine has hit. Performance shown above is the V7 engine that survived those audits.
          </li>
          <li>
            Past performance is not a guarantee. Indian markets in 2023-2026 included two rate-hike cycles,
            an election, and one sectoral rotation — the engine held up across them. We can&apos;t promise
            the next regime will be the same.
          </li>
        </ul>
      </section>
    </article>
  )
}

function Stat({ label, value, accent }: {
  label: string; value: string; accent: 'amber' | 'green' | 'red' | 'neutral'
}) {
  const valueCls = {
    amber:   'text-mint-300',
    green:   'text-green-300',
    red:     'text-red-300',
    neutral: 'text-neutral-100',
  }[accent]
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded-lg p-3">
      <div className="text-[10px] uppercase tracking-wider text-neutral-500 mb-1">{label}</div>
      <div className={`text-xl md:text-2xl font-bold font-mono ${valueCls}`}>{value}</div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────
// REPLAY tab — reuses ExpandablePickRow (the /power/replay/[date] surface)
// ─────────────────────────────────────────────────────────────────────────

const HORIZON_LABEL: Record<string, string> = {
  d1: 'D+1', d3: 'D+3', d5: 'D+5', d10: 'D+10', d15: 'D+15',
}

function ReplayTab({ replay, replayDate, replayError, authed }: {
  replay: ReplayPayload | null; replayDate: string | null
  replayError: string | null; authed: boolean
}) {
  return (
    <div className="space-y-6">
      <ShellDateNavigator currentDate={replayDate} authed={authed} />

      {!replayDate && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-6 text-center text-neutral-400">
          <p className="text-white/85 font-semibold">Pick any date to replay its outcomes.</p>
          <p className="text-xs text-neutral-500 mt-2">
            For every pick on a chosen signal date, the replay computes D+1, D+3, D+5, D+10 and D+15 outcomes.
          </p>
        </div>
      )}

      {replayError && (
        <div role="alert" className="px-4 py-3 rounded bg-red-500/10 text-red-200 border border-red-500/40 text-sm">
          {replayError}
        </div>
      )}

      {replay && (
        <>
          <header>
            {replay.is_featured && replay.title && (
              <p className="text-sm text-mint-400 font-semibold uppercase tracking-wide">Featured replay</p>
            )}
            <h2 className="text-xl md:text-2xl font-bold">
              {replay.title || `Replay — ${replay.replay_date}`}
            </h2>
            <p className="mt-1 text-sm text-neutral-400 font-mono">
              Signal {replay.replay_date}
              {replay.entry_date && <> · Entry {replay.entry_date}</>} · {replay.aggregate.n_picks} picks
            </p>
            {replay.hook && <p className="mt-3 text-neutral-300 text-sm md:text-base">{replay.hook}</p>}
          </header>

          {replay.aggregate.n_picks > 0 && (
            <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
              <h3 className="text-[11px] tracking-wider uppercase text-neutral-500 font-semibold mb-3">
                What actually happened across all {replay.aggregate.n_picks} picks
              </h3>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-sm">
                {(['d1', 'd3', 'd5', 'd10', 'd15'] as const).map(h => {
                  const v = replay.aggregate.horizons[h]
                  if (!v) return (
                    <div key={h} className="text-center">
                      <div className="text-xs text-neutral-500 mb-1">{HORIZON_LABEL[h]}</div>
                      <div className="text-neutral-600">—</div>
                    </div>
                  )
                  return (
                    <div key={h} className="text-center">
                      <div className="text-xs text-neutral-500 mb-1">{HORIZON_LABEL[h]}</div>
                      <div className="text-base md:text-lg font-bold font-mono text-green-300">
                        {v.wr.toFixed(0)}%
                      </div>
                      <div className="text-[10px] text-neutral-500">
                        avg {v.avg_ret >= 0 ? '+' : ''}{v.avg_ret.toFixed(2)}%
                      </div>
                    </div>
                  )
                })}
              </div>
            </section>
          )}

          {replay.picks.length > 0 ? (
            <div className="space-y-1.5">
              {replay.picks.map(p => (
                <ExpandablePickRow key={`${p.symbol}-${p.rank}`} pick={p} />
              ))}
            </div>
          ) : (
            <p className="text-center text-neutral-400 py-12">No picks for this date.</p>
          )}
        </>
      )}
    </div>
  )
}

/**
 * In-shell date navigator — routes within Performance (/power/performance?date=…)
 * instead of the legacy /power/replay/[date] page, so the user never leaves the
 * shell. Mirrors the legacy DateNavigator UX.
 */
function ShellDateNavigator({ currentDate, authed }: { currentDate: string | null; authed: boolean }) {
  const router = useRouter()
  const [date, setDate] = useState(currentDate ?? new Date().toISOString().slice(0, 10))

  if (!authed) {
    return (
      <div className="text-xs text-neutral-500">
        Sign in to replay any date&apos;s outcomes.
      </div>
    )
  }

  const onGo = (e: React.FormEvent) => {
    e.preventDefault()
    if (date) router.push(`/power/performance?date=${encodeURIComponent(date)}`)
  }

  return (
    <form onSubmit={onGo} className="flex items-center gap-2 text-sm flex-wrap">
      <label htmlFor="perf-replay-date" className="text-neutral-400">Replay any date:</label>
      <input
        id="perf-replay-date"
        type="date"
        value={date}
        onChange={e => setDate(e.target.value)}
        max={new Date().toISOString().slice(0, 10)}
        min="2020-01-01"
        className="bg-neutral-950 border border-neutral-700 rounded px-2 py-1 text-sm font-mono text-neutral-100"
      />
      <button
        type="submit"
        disabled={!date || date === currentDate}
        className="px-3 py-1 bg-mint-500 text-neutral-950 rounded text-sm font-medium
                   hover:bg-mint-400 disabled:opacity-40 disabled:cursor-not-allowed"
      >
        Replay
      </button>
    </form>
  )
}
