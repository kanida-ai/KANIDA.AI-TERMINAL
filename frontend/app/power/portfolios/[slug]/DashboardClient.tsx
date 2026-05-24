'use client'

/**
 * Portfolio detail dashboard — V3-locked + UX revision (2026-05-16).
 *
 * Section order (operator-locked):
 *   1.  Back link
 *   2.  Persona name + tagline (no risk badge per Fix 2)
 *   3.  ⚠ Mandatory disclosure (P5 BTST only)
 *   4.  Capital selector (default ₹5L)
 *   5.  Backtested Performance Summary (renamed from "Locked Backtest Number")
 *   6.  Year-by-year table with expandable month drilldown (Fix 4)
 *   7.  ⚙ Locked Rules box (visible by default)
 *   8.  "Capital Model: Fixed Allocation" explainer + "This year's P&L"
 *       (replaces Reinvest/Cash Out toggle per Fix 5)
 *   9.  YTD / MTD / Active positions strip
 *   10. What it does + Trading cycle + Best fit / Not for / Expect
 *   11. How to execute this strategy (Fix 6, per-persona)
 *   12. Equity curve chart
 *   13. Open positions (Zerodha-style)
 *   14. Recent trades
 *   15. Methodology footnote (verbatim)
 */
import { useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import {
  PowerAPI,
  adaptPersonaToPerformance,
  type PortfolioSummary,
  type PortfolioEquityPoint,
  type PortfolioPosition,
  type PortfolioTrade,
  type PortfolioPerformanceResponse,
  type PortfolioYearlyRow,
  type PortfolioMonthlyRow,
} from '@/lib/power-api'
import { EquityChart } from '@/components/power/EquitySparkline'
import {
  useUserCapital, QUICK_CAPITALS_RS,
} from '@/lib/use-user-capital'


// ──────────────────────────────────────────────────────────────────────────
// Format helpers
// ──────────────────────────────────────────────────────────────────────────

const inrSigned = (n: number | null | undefined, sign = false) => {
  if (n == null) return '—'
  const abs    = Math.abs(Math.round(n))
  const prefix = n < 0 ? '−' : sign ? '+' : ''
  return prefix + '₹' + abs.toLocaleString('en-IN')
}
const inr = (n: number | null | undefined) => {
  if (n == null) return '—'
  return '₹' + Math.round(n).toLocaleString('en-IN')
}
const pct = (n: number | null | undefined, opts: { sign?: boolean } = {}) => {
  if (n == null) return '—'
  const s = n.toFixed(2)
  return opts.sign && n >= 0 ? `+${s}%` : `${s}%`
}
const dirCls = (n: number | null | undefined) =>
  n == null ? 'text-neutral-300' : n >= 0 ? 'text-green-300' : 'text-red-300'

const fmtINR_compact = (n: number) => {
  if (Math.abs(n) >= 1e7)  return `₹${(n / 1e7).toFixed(2)} Cr`
  if (Math.abs(n) >= 1e5)  return `₹${(n / 1e5).toFixed(2)} L`
  return '₹' + Math.round(n).toLocaleString('en-IN')
}

const METHODOLOGY_FOOTNOTE = (
  "How these numbers were calculated. Every persona was tested using our V3 " +
  "audit-ready Indian cash-equity simulator. Rules: integer shares only (no " +
  "fractional), no leverage, idle cash tracked separately, gap-down stops " +
  "honoured (exit at gap price if worse than stop). Capital model: fixed " +
  "rupees per trade (no compounding within year). Each calendar year resets " +
  "to ₹5,00,000 — so returns shown are what a fresh ₹5 L account would have " +
  "made each year independently, not a compounding multi-year track record. " +
  "Walk-forward: at any point during the backtest, the engine could only use " +
  "patterns mined in the prior 4 years (mirrors how live production refreshes " +
  "weekly via publish_patterns.py). Pattern source: same production database " +
  "the live engine uses. All P&L reconciles year-by-year to ₹0 gap. All 12-14 " +
  "QA checks PASS on every persona file."
)


type Props = {
  summary:     PortfolioSummary
  positions:   PortfolioPosition[]
  trades:      PortfolioTrade[]
  tradesTotal: number
  equity:      PortfolioEquityPoint[]
  capitalStart: number
}


export function PortfolioDashboardClient({
  summary, positions, trades, tradesTotal, equity, capitalStart,
}: Props) {
  const { capital: userCapital, setCapital } = useUserCapital()
  const scale = userCapital / capitalStart

  const live = summary.live

  const chartPoints = useMemo(() =>
    equity.map(p => ({
      trade_date:   p.trade_date,
      total_equity: p.total_equity * scale,
    })),
    [equity, scale],
  )

  // Year + month backtest performance — source-of-truth swap (2026-05-16):
  // pulls from the new persona-simulator (cached 1 hr) instead of the legacy
  // Excel-import pipeline. The new API returns yearly + monthly in one shot;
  // `adaptPersonaToPerformance()` reshapes it into the existing UI contract
  // so the table component below stays untouched.
  //
  // Failure handling (2026-05-17): previously this catch was silent — if BOTH
  // the simulator and the legacy fallback failed, the "loading…" banners
  // stayed on screen forever with no signal to the user. Now we surface the
  // failure to a `perfError` state so the section can render an actionable
  // error message + retry button, and log to console for debugging.
  const [perf, setPerf]           = useState<PortfolioPerformanceResponse | null>(null)
  const [perfError, setPerfError] = useState<string | null>(null)
  const [perfReloadKey, setPerfReloadKey] = useState(0)
  useEffect(() => {
    const ctrl = new AbortController()
    setPerfError(null)
    PowerAPI.persona(summary.slug, ctrl.signal)
      .then(p => {
        try {
          setPerf(adaptPersonaToPerformance(p))
        } catch (adaptErr) {
          console.error('[persona] adapter failed:', adaptErr, 'raw:', p)
          setPerfError('Adapter error. The backend responded but the data shape changed. Refresh the page.')
        }
      })
      .catch(simErr => {
        // Don't surface AbortError — it fires when the user navigates away.
        if (simErr?.name === 'AbortError') return
        console.warn('[persona] simulator endpoint failed, trying legacy:', simErr)
        // Fallback: try the legacy Excel-backed endpoint. Use a FRESH signal
        // because the original ctrl.signal would have already been aborted if
        // the simulator call was the one to throw an AbortError.
        const fb = new AbortController()
        PowerAPI.portfolioPerformance(summary.slug, fb.signal)
          .then(setPerf)
          .catch(legacyErr => {
            if (legacyErr?.name === 'AbortError') return
            console.error('[persona] both simulator + legacy failed:', { simErr, legacyErr })
            setPerfError(
              simErr instanceof Error
                ? `Backtest unavailable: ${simErr.message}. Refresh in a moment.`
                : 'Backtest unavailable. Refresh in a moment.'
            )
          })
      })
    return () => ctrl.abort()
  }, [summary.slug, perfReloadKey])
  const retryPerf = () => setPerfReloadKey(k => k + 1)

  // ── YTD / MTD / Profit derived from persona simulator (NOT the live engine).
  //
  // Operator-flagged bug 2026-05-17: the year-table (top) and the YTD/MTD/
  // Profit cards (bottom) were diverging because they read from two different
  // sources — the persona simulator (audit-clean fixed-₹ yearly-reset) and
  // the legacy live engine (%-of-equity compounding since Jan 1). They both
  // claim to show "YTD" but compute it differently → mismatch by construction.
  //
  // Fix: derive YTD/MTD/Profit from the SAME `perf` object that drives the
  // year-table above. Now the two sections come from one source — divergence
  // is structurally impossible.
  const personaStats = useMemo(() => {
    if (!perf || perf.yearly.length === 0) return null
    const lastYear   = perf.yearly[perf.yearly.length - 1]    // partial-year if mid-year
    const yearStr    = String(lastYear.year)
    const monthsForY = perf.monthly[yearStr] ?? []
    const lastMonth  = monthsForY.length > 0 ? monthsForY[monthsForY.length - 1] : null
    const startCap   = lastYear.start_cap_rs                  // always ₹5 L in V3 audit
    return {
      year:               lastYear.year,
      ytd_return_pct:     lastYear.return_pct,
      // pnl_rs at the ₹5L audit base; scaled to user capital at render time.
      ytd_pnl_rs_at_base: lastYear.end_equity_rs - startCap,
      mtd_return_pct:     lastMonth?.return_pct ?? null,
      mtd_month_label:    lastMonth ? MONTH_NAMES[lastMonth.month - 1] : null,
      is_partial:         lastYear.is_partial === 1,
      start_cap_rs:       startCap,
    }
  }, [perf])

  return (
    <div className="space-y-6 md:space-y-8">
      <BackLink />
      <PersonaHeader summary={summary} />
      <DisclosureBlock disclosure={summary.narrative?.disclosure ?? null} />
      <CapitalSelector userCapital={userCapital} onChange={setCapital} />
      <BacktestedPerformanceSummary summary={summary} userCapital={userCapital} />
      <YearMonthBreakdown performance={perf} error={perfError} onRetry={retryPerf} />
      <LockedRulesBox summary={summary} />
      <CapitalModelExplainer
        summary={summary}
        stats={personaStats}
        scale={scale}
        userCapital={userCapital}
        error={perfError}
        onRetry={retryPerf}
      />
      <YTDStrip stats={personaStats} live={live} scale={scale} />
      <NarrativeBlocks summary={summary} />
      <ExecutionGuidance summary={summary} />

      <section>
        <header className="flex items-baseline justify-between mb-3">
          <h2 className="text-base font-bold text-neutral-50">
            Equity curve — {summary.backtest_window ?? 'live'}
          </h2>
          {live && (
            <span className="text-xs text-neutral-400 font-mono">as of {live.as_of_date}</span>
          )}
        </header>
        <div className="bg-neutral-900 border border-neutral-800 rounded-lg p-3 md:p-4">
          <EquityChart points={chartPoints} capitalStart={userCapital} className="h-56 md:h-72" />
        </div>
      </section>

      <OpenPositions positions={positions} scale={scale} />
      <RecentTrades  trades={trades} totalClosed={tradesTotal} scale={scale} />

      <footer className="border-t border-neutral-800 pt-4">
        <p className="text-[11px] text-neutral-400 leading-relaxed max-w-4xl">
          <span className="font-semibold text-neutral-200">Methodology — </span>
          {METHODOLOGY_FOOTNOTE}
        </p>
      </footer>
    </div>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Header bits
// ──────────────────────────────────────────────────────────────────────────

function BackLink() {
  return (
    <Link href="/power/portfolios" className="inline-block text-xs text-mint-300 hover:text-mint-200 underline">
      ← All trader portfolios
    </Link>
  )
}


function PersonaHeader({ summary }: { summary: PortfolioSummary }) {
  // Fix 2 (2026-05-16): risk-tier badge removed. BTST disclosure block lives
  // below this header (DisclosureBlock).
  return (
    <header className="space-y-2">
      <h1 className="text-2xl md:text-3xl font-bold text-neutral-50">{summary.name}</h1>
      <p className="text-base text-neutral-200 max-w-3xl leading-relaxed">{summary.tagline}</p>
    </header>
  )
}


function DisclosureBlock({ disclosure }: { disclosure: string | null }) {
  if (!disclosure) return null
  return (
    <section role="alert"
             className="border-2 border-red-500/50 bg-red-500/10 rounded-lg p-4 md:p-5 space-y-2">
      <div className="flex items-center gap-2">
        <span className="text-xl" aria-hidden>⚠️</span>
        <h2 className="text-base font-bold text-red-200 uppercase tracking-wider">
          Read this before considering this strategy
        </h2>
      </div>
      <p className="text-sm text-red-100 leading-relaxed">{disclosure}</p>
    </section>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Capital selector
// ──────────────────────────────────────────────────────────────────────────

function CapitalSelector({ userCapital, onChange }: {
  userCapital: number
  onChange: (v: number) => void
}) {
  const [customOpen, setCustomOpen]   = useState(false)
  const [customInput, setCustomInput] = useState<string>('')
  const isQuickValue = QUICK_CAPITALS_RS.some(q => q.value === userCapital)

  const submitCustom = () => {
    const n = parseInt(customInput.replace(/[^0-9]/g, ''), 10)
    if (Number.isFinite(n) && n >= 10_000) {
      onChange(n); setCustomOpen(false)
    }
  }
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-3 md:p-4">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-2">
        <span className="text-sm text-neutral-300">
          See this portfolio at <span className="text-neutral-100 font-semibold">{fmtINR_compact(userCapital)}</span> of capital
        </span>
        <span className="text-xs text-neutral-400">% returns stay the same · ₹ amounts rescale</span>
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        {QUICK_CAPITALS_RS.map(q => (
          <button key={q.value} type="button"
            onClick={() => { setCustomOpen(false); onChange(q.value) }}
            className={[
              'text-xs px-3 py-1.5 rounded border transition-colors font-mono',
              userCapital === q.value
                ? 'bg-mint-500 text-neutral-950 border-mint-500'
                : 'border-neutral-700 text-neutral-200 hover:border-mint-500/40 hover:text-mint-300',
            ].join(' ')}>
            {q.label}
          </button>
        ))}
        {!customOpen ? (
          <button type="button"
            onClick={() => { setCustomOpen(true); setCustomInput(String(userCapital)) }}
            className={[
              'text-xs px-3 py-1.5 rounded border transition-colors font-mono',
              !isQuickValue
                ? 'bg-mint-500 text-neutral-950 border-mint-500'
                : 'border-neutral-700 text-neutral-200 hover:border-mint-500/40 hover:text-mint-300',
            ].join(' ')}>
            {!isQuickValue ? fmtINR_compact(userCapital) : 'Custom…'}
          </button>
        ) : (
          <span className="inline-flex items-center gap-2">
            <span className="text-sm text-neutral-400">₹</span>
            <input type="text" inputMode="numeric"
              value={customInput} onChange={e => setCustomInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && submitCustom()} autoFocus
              placeholder="e.g. 7,50,000"
              className="bg-neutral-950 border border-mint-500/40 rounded px-2 py-1 text-sm font-mono text-neutral-50 w-32 focus:outline-none focus:border-mint-400" />
            <button type="button" onClick={submitCustom}
                    className="text-xs px-2 py-1 bg-mint-500 text-neutral-950 rounded font-semibold">Set</button>
            <button type="button" onClick={() => setCustomOpen(false)}
                    className="text-xs text-neutral-400 hover:text-neutral-200">cancel</button>
          </span>
        )}
      </div>
    </section>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Fix 3 — Backtested Performance Summary (renamed from "Locked Backtest Number")
// ──────────────────────────────────────────────────────────────────────────

function BacktestedPerformanceSummary({ summary, userCapital }: {
  summary: PortfolioSummary; userCapital: number
}) {
  const v = summary.validated
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-4 md:p-5 space-y-3">
      <header>
        <h2 className="text-base font-bold text-neutral-50">Backtested Performance Summary</h2>
        <p className="text-xs text-neutral-400 mt-0.5">
          Based on {summary.backtest_window ?? 'the full backtest window'}. Each year tested independently with ₹5,00,000 starting capital.
        </p>
      </header>

      <dl className="grid grid-cols-2 md:grid-cols-4 gap-2 md:gap-3">
        <Stat label="Average yearly return"
              big={pct(v.avg_yearly_return_pct, { sign: true })}
              bigCls={dirCls(v.avg_yearly_return_pct)}
              sub={`≈${inrSigned(userCapital * (v.avg_yearly_return_pct ?? 0) / 100, true)} on ${inr(userCapital)}`} />
        <Stat label="Worst year"
              big={pct(v.worst_year_pct, { sign: true })}
              bigCls={dirCls(v.worst_year_pct)}
              sub={v.worst_year_label ?? ''} />
        <Stat label="Win rate"
              big={pct(v.win_rate_pct)}
              sub={`${v.trades_per_year ?? '—'} trades / year`} />
        <Stat label="Positive years"
              big={v.positive_years ?? '—'}
              bigCls={v.positive_years?.includes('6 of 6') ? 'text-green-300' : 'text-neutral-100'} />
      </dl>

      {v.headline_callout && (
        <p className="text-sm text-mint-200 font-semibold border-l-2 border-mint-500/60 pl-3">
          ★ {v.headline_callout}
        </p>
      )}
      {v.backtest_caveat && (
        <p className="text-xs text-mint-100/80 border-l-2 border-mint-500/40 pl-3">
          ⓘ {v.backtest_caveat}
        </p>
      )}
    </section>
  )
}


function Stat({ label, big, sub, bigCls = 'text-neutral-50' }: {
  label: string; big: string; sub?: string; bigCls?: string
}) {
  return (
    <div className="bg-neutral-950/50 border border-neutral-800 rounded p-3 text-center">
      <div className="text-[10px] uppercase tracking-wider text-neutral-400 mb-0.5">{label}</div>
      <div className={`text-xl md:text-2xl font-bold font-mono ${bigCls}`}>{big}</div>
      {sub && <div className="text-[10px] text-neutral-400 font-mono mt-1 truncate" title={sub}>{sub}</div>}
    </div>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Fix 4 — Year-by-year table with expandable month drilldown
// ──────────────────────────────────────────────────────────────────────────

const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

function returnCls(p: number) {
  return p > 0 ? 'text-green-300' : p < 0 ? 'text-red-300' : 'text-neutral-300'
}
function winMonthsCls(w: number | null) {
  if (w == null) return 'text-neutral-400'
  if (w >= 7) return 'text-green-300'
  if (w >= 4) return 'text-neutral-200'
  return 'text-red-300'
}

function YearMonthBreakdown({ performance, error, onRetry }: {
  performance: PortfolioPerformanceResponse | null
  error:       string | null
  onRetry:     () => void
}) {
  if (error) return (
    <section className="bg-red-500/5 border border-red-500/30 rounded-lg p-4 flex items-center justify-between gap-4">
      <div className="text-sm text-red-200">
        <strong className="font-semibold">Performance breakdown unavailable.</strong>{' '}
        <span className="text-red-300/80">{error}</span>
      </div>
      <button onClick={onRetry}
              className="text-xs px-3 py-1.5 rounded border border-red-400/40 text-red-100 hover:bg-red-500/10 transition-colors whitespace-nowrap">
        Retry
      </button>
    </section>
  )
  if (!performance) return (
    <section className="text-xs text-neutral-500">
      Performance breakdown loading…
    </section>
  )
  if (performance.yearly.length === 0) return null

  return (
    <section>
      <header className="mb-2">
        <h2 className="text-base font-bold text-neutral-50">Year-by-year performance</h2>
        <p className="text-xs text-neutral-400 mt-0.5">
          Click a year to see month-by-month detail. Numbers from the V3 audit Excel.
        </p>
      </header>
      <div className="bg-neutral-900 border border-neutral-800 rounded-lg overflow-hidden">
        <table className="w-full text-xs">
          <thead className="bg-neutral-950/60 text-neutral-300 uppercase tracking-wider text-[10px]">
            <tr>
              <th className="text-left p-2.5">Year</th>
              <th className="text-right p-2.5">Return</th>
              <th className="text-right p-2.5">Max DD</th>
              <th className="text-center p-2.5">Winning months</th>
              <th className="text-right p-2.5 hidden md:table-cell">Trades</th>
            </tr>
          </thead>
          <tbody>
            {performance.yearly.map(y => (
              <YearRow key={y.year}
                       year={y}
                       monthly={performance.monthly[String(y.year)] ?? []} />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}


function YearRow({ year, monthly }: { year: PortfolioYearlyRow; monthly: PortfolioMonthlyRow[] }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <tr onClick={() => setOpen(o => !o)}
          className="border-t border-neutral-800 hover:bg-neutral-850 cursor-pointer">
        <td className="p-2.5">
          <span className={`text-neutral-500 text-[10px] mr-2 transition-transform inline-block ${open ? 'rotate-180' : ''}`} aria-hidden>▾</span>
          <span className="font-mono text-neutral-100">{year.year}</span>
          {year.is_partial ? <span className="text-[10px] text-neutral-500 ml-2">(partial)</span> : null}
        </td>
        <td className={`p-2.5 text-right font-mono ${returnCls(year.return_pct)}`}>
          {year.return_pct >= 0 ? '+' : ''}{year.return_pct.toFixed(2)}%
        </td>
        <td className="p-2.5 text-right font-mono text-red-300">
          {year.max_drawdown_pct.toFixed(2)}%
        </td>
        <td className={`p-2.5 text-center font-mono ${winMonthsCls(year.winning_months)}`}>
          {year.winning_months != null ? `${year.winning_months} / 12` : '—'}
        </td>
        <td className="p-2.5 text-right font-mono text-neutral-300 hidden md:table-cell">
          {year.n_closed_trades}
        </td>
      </tr>
      {open && (
        <tr className="bg-neutral-950/60 border-t border-neutral-800">
          <td colSpan={5} className="p-0">
            <MonthSubTable rows={monthly} />
          </td>
        </tr>
      )}
    </>
  )
}


function MonthSubTable({ rows }: { rows: PortfolioMonthlyRow[] }) {
  if (rows.length === 0) {
    return <div className="p-4 text-xs text-neutral-500 italic">No monthly data for this year.</div>
  }
  return (
    <div className="px-4 md:px-6 py-3">
      <table className="w-full text-xs">
        <thead className="text-neutral-400 uppercase tracking-wider text-[10px]">
          <tr>
            <th className="text-left py-1.5">Month</th>
            <th className="text-right py-1.5">Return</th>
            <th className="text-right py-1.5 hidden md:table-cell">End equity (₹5L base)</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={`${r.year}-${r.month}`} className="border-t border-neutral-800/60">
              <td className="py-1.5 font-mono text-neutral-200">{MONTH_NAMES[r.month - 1]}</td>
              <td className={`py-1.5 text-right font-mono ${returnCls(r.return_pct)}`}>
                {r.return_pct >= 0 ? '+' : ''}{r.return_pct.toFixed(2)}%
              </td>
              <td className="py-1.5 text-right font-mono text-neutral-400 hidden md:table-cell">
                {inr(r.end_equity_rs)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Locked Rules box
// ──────────────────────────────────────────────────────────────────────────

function LockedRulesBox({ summary }: { summary: PortfolioSummary }) {
  const par = summary.parameters
  return (
    <details open
             className="bg-neutral-900 border border-mint-500/30 rounded-lg p-3 md:p-4 space-y-2 group">
      <summary className="cursor-pointer list-none flex items-center justify-between">
        <span className="text-sm font-bold text-mint-300">
          ⚙ Locked rules — exactly what this trader does
        </span>
        <span className="text-mint-300 text-xs font-mono group-open:rotate-180 transition-transform">▾</span>
      </summary>

      <ul className="text-sm text-neutral-100 space-y-1.5 mt-3">
        <li>
          <span className="text-neutral-400 mr-2">Entry:</span>
          <span className="text-neutral-50">{par.entry_time_label}</span>{', '}
          top-<span className="text-neutral-50 font-mono">{par.top_n}</span> stocks by engine score
        </li>
        <li>
          <span className="text-neutral-400 mr-2">Trade size:</span>
          <span className="text-neutral-50 font-mono">{inr(par.fixed_per_trade_rs)}</span> fixed per stock
          {' '}<span className="text-neutral-500 text-xs">(at ₹5 L base; scales with your selected capital above)</span>
        </li>
        <li>
          <span className="text-neutral-400 mr-2">Hold:</span>
          max <span className="text-neutral-50 font-mono">{par.hold_days_max}</span> trading days
        </li>
        <li>
          <span className="text-neutral-400 mr-2">Stop loss:</span>
          <span className="text-red-300 font-mono">{par.sl_pct}%</span> from entry
          {' '}<span className="text-neutral-500 text-xs">(if the stock gaps down past this, exit at the gap price)</span>
        </li>
        {par.target_pct != null && (
          <li>
            <span className="text-neutral-400 mr-2">Target:</span>
            <span className="text-green-300 font-mono">+{par.target_pct}%</span> fixed — no trail
          </li>
        )}
        {par.trail_trigger_pct != null && (
          <li>
            <span className="text-neutral-400 mr-2">Smart trailing stop:</span>
            arms when the stock closes at +<span className="text-mint-300 font-mono">{par.trail_trigger_pct}%</span> above entry,
            then locks profit at the higher of entry-price or 10-day low. Never moves down.
          </li>
        )}
        {par.intraday_filter === 'wait_15min_volume_check' && (
          <li>
            <span className="text-neutral-400 mr-2">Morning confirmation:</span>
            only enter at 9:30 IST if first 15 min show positive return AND volume {'>'} 5% of yesterday&apos;s total
          </li>
        )}
        <li className="text-xs text-neutral-500">
          Cash only — no borrowed money or leverage · integer shares only · idle cash tracked
        </li>
      </ul>
    </details>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Fix 5 — Capital Model: Fixed Allocation (replaces Reinvest/Cash Out toggle)
// ──────────────────────────────────────────────────────────────────────────

type PersonaStats = {
  year:               number
  ytd_return_pct:     number
  ytd_pnl_rs_at_base: number
  mtd_return_pct:     number | null
  mtd_month_label:    string | null
  is_partial:         boolean
  start_cap_rs:       number
}


function CapitalModelExplainer({ summary, stats, scale, userCapital, error, onRetry }: {
  summary:     PortfolioSummary
  stats:       PersonaStats | null
  scale:       number
  userCapital: number
  error:       string | null
  onRetry:     () => void
}) {
  if (error) {
    return (
      <section className="bg-red-500/5 border border-red-500/30 rounded-lg p-6 flex flex-col items-center gap-3 text-center">
        <p className="text-base text-red-200 font-semibold">Backtest numbers unavailable</p>
        <p className="text-sm text-red-300/80 max-w-md">{error}</p>
        <button onClick={onRetry}
                className="text-xs px-4 py-2 rounded border border-red-400/40 text-red-100 hover:bg-red-500/10 transition-colors">
          Retry
        </button>
      </section>
    )
  }
  if (!stats) {
    return (
      <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-6 text-center text-neutral-300 text-base">
        Backtest numbers loading…
      </section>
    )
  }
  // scale is userCapital / capitalStart (where capitalStart = portfolio's
  // virtual_capital_start = ₹50 L from the OLD engine). For persona math we
  // want userCapital / ₹5 L (the V3 audit base). Recompute that ratio.
  const scaleFromAuditBase = userCapital / stats.start_cap_rs
  const per_trade_rs        = summary.parameters.fixed_per_trade_rs * scaleFromAuditBase
  const ytdPnlRs            = stats.ytd_pnl_rs_at_base * scaleFromAuditBase
  const profitCls           = ytdPnlRs >= 0 ? 'text-green-300' : 'text-red-300'

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-xl p-4 md:p-5 space-y-3">
      <header>
        <h2 className="text-base font-bold text-neutral-50">Capital Model: Fixed Allocation</h2>
        <p className="text-xs text-neutral-300 mt-1 max-w-3xl leading-relaxed">
          Each trade uses a fixed rupee amount (<span className="text-neutral-50 font-mono">{inr(per_trade_rs)}</span>{' '}
          for this persona at <span className="text-neutral-50 font-mono">{inr(userCapital)}</span> capital).
          The backtest does <span className="text-neutral-100 font-semibold">not compound</span> — profits don&apos;t
          grow your per-trade size within the year. Each calendar year starts fresh with{' '}
          <span className="text-neutral-50 font-mono">{inr(userCapital)}</span>.
        </p>
      </header>

      <div className="pt-2">
        <div className="bg-neutral-950/40 border border-neutral-800 rounded p-3">
          <div className="text-[10px] uppercase tracking-wider text-neutral-400 mb-1">
            {stats.year}&apos;s P&amp;L on {inr(userCapital)} base
            {stats.is_partial && <span className="ml-2 normal-case tracking-normal text-neutral-500">(partial year)</span>}
          </div>
          <div className={`text-2xl md:text-3xl font-bold font-mono ${profitCls}`}>
            {inrSigned(ytdPnlRs, true)}
            <span className="text-base ml-2">({pct(stats.ytd_return_pct, { sign: true })})</span>
          </div>
        </div>
      </div>
    </section>
  )
}


function YTDStrip({ stats, live, scale }: {
  stats:  PersonaStats | null
  live:   PortfolioSummary['live']
  scale:  number
}) {
  if (!stats) return null
  return (
    <section className="grid grid-cols-3 gap-2 md:gap-3">
      <Stat label={`${stats.year} return (YTD)`}
            big={pct(stats.ytd_return_pct, { sign: true })}
            bigCls={dirCls(stats.ytd_return_pct)}
            sub={stats.is_partial ? 'partial year' : 'full year'} />
      <Stat label={stats.mtd_month_label
                    ? `${stats.mtd_month_label} ${stats.year} (MTD)`
                    : 'Latest month'}
            big={pct(stats.mtd_return_pct, { sign: true })}
            bigCls={dirCls(stats.mtd_return_pct)} />
      <Stat label="Open at backtest end"
            big={live ? String(live.n_open_positions) : '—'}
            sub={live ? `${inr(live.invested_right_now_rs * scale)} held overnight` : ''} />
    </section>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Narrative blocks
// ──────────────────────────────────────────────────────────────────────────

function NarrativeBlocks({ summary }: { summary: PortfolioSummary }) {
  const n = summary.narrative
  if (!n) return null
  return (
    <section className="space-y-5">
      {n.what_it_does && (
        <div>
          <h2 className="text-base font-bold text-neutral-50 mb-2">What this trader does</h2>
          <p className="text-sm md:text-base text-neutral-200 leading-relaxed max-w-3xl">
            {n.what_it_does}
          </p>
        </div>
      )}
      {n.trading_cycle && n.trading_cycle.length > 0 && (
        <div>
          <h3 className="text-sm font-bold text-neutral-100 uppercase tracking-wider mb-2">Trading cycle</h3>
          <ol className="space-y-1.5 text-sm text-neutral-200 max-w-3xl">
            {n.trading_cycle.map((step, i) => (
              <li key={i} className="flex gap-2">
                <span className="text-mint-300 font-mono shrink-0">{i + 1}.</span>
                <span className="leading-relaxed">{step}</span>
              </li>
            ))}
          </ol>
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <FitCard title="Best fit"      body={n.best_fit}      accent="green" />
        <FitCard title="Not for"       body={n.not_for}       accent="red" />
        <FitCard title="What to expect" body={n.what_to_expect} accent="amber" />
      </div>
    </section>
  )
}


function FitCard({ title, body, accent }: {
  title: string; body?: string; accent: 'green' | 'red' | 'amber'
}) {
  if (!body) return null
  const cls = {
    green: 'border-green-500/30 bg-green-500/5',
    red:   'border-red-500/30 bg-red-500/5',
    amber: 'border-mint-500/30 bg-mint-500/5',
  }[accent]
  const titleCls = {
    green: 'text-green-300', red: 'text-red-300', amber: 'text-mint-300',
  }[accent]
  return (
    <div className={`border rounded-lg p-3 md:p-4 ${cls}`}>
      <h4 className={`text-[10px] uppercase tracking-wider font-bold mb-1.5 ${titleCls}`}>{title}</h4>
      <p className="text-sm text-neutral-100 leading-relaxed">{body}</p>
    </div>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Fix 6 — Execution guidance (per-persona)
// ──────────────────────────────────────────────────────────────────────────

function ExecutionGuidance({ summary }: { summary: PortfolioSummary }) {
  const text = summary.narrative?.execution_guidance
  if (!text) return null
  return (
    <section className="bg-neutral-950/40 border border-neutral-800 rounded-lg p-4 md:p-5">
      <h2 className="text-base font-bold text-neutral-50 mb-2">How to execute this strategy</h2>
      <p className="text-sm text-neutral-200 leading-relaxed max-w-3xl">{text}</p>
    </section>
  )
}


// ──────────────────────────────────────────────────────────────────────────
// Open positions + recent trades (unchanged from prior pass)
// ──────────────────────────────────────────────────────────────────────────

function OpenPositions({ positions, scale }: { positions: PortfolioPosition[]; scale: number }) {
  if (positions.length === 0) {
    return (
      <section>
        <h2 className="text-base font-bold text-neutral-50 mb-3">Open positions</h2>
        <p className="text-sm text-neutral-300">No open positions — fully in cash right now.</p>
      </section>
    )
  }
  return (
    <section>
      <header className="flex items-baseline justify-between mb-3">
        <h2 className="text-base font-bold text-neutral-50">
          Open positions <span className="text-neutral-400 font-mono text-sm">({positions.length})</span>
        </h2>
        <span className="text-xs text-neutral-400">Click a row for the engine&apos;s reason</span>
      </header>
      <div className="bg-neutral-900 border border-neutral-800 rounded-lg overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-neutral-950/60 text-neutral-300 uppercase tracking-wider text-[10px]">
            <tr>
              <th className="text-left p-2.5">Instrument</th>
              <th className="text-right p-2.5 hidden md:table-cell">Qty</th>
              <th className="text-right p-2.5 hidden md:table-cell">Avg cost</th>
              <th className="text-right p-2.5">LTP</th>
              <th className="text-right p-2.5 hidden md:table-cell">Invested</th>
              <th className="text-right p-2.5 hidden md:table-cell">Cur. val</th>
              <th className="text-right p-2.5">P&L</th>
              <th className="text-right p-2.5">Net %</th>
            </tr>
          </thead>
          <tbody>
            {positions.map(p => <PositionRow key={p.id} p={p} scale={scale} />)}
          </tbody>
        </table>
      </div>
    </section>
  )
}


function PositionRow({ p, scale }: { p: PortfolioPosition; scale: number }) {
  const [open, setOpen] = useState(false)
  const cur     = p.current_price ?? p.entry_price
  const investedRs = p.capital_committed * scale
  const curValRs   = cur * p.qty * scale
  const pnlRs      = (p.unrealized_pnl_rs ?? (cur * p.qty - p.capital_committed)) * scale
  const pnlPct     = p.unrealized_pnl_pct ?? ((cur - p.entry_price) / p.entry_price * 100)

  return (
    <>
      <tr onClick={() => setOpen(o => !o)}
          className="border-t border-neutral-800 hover:bg-neutral-850 cursor-pointer">
        <td className="p-2.5">
          <div className="flex items-center gap-2">
            <span className="font-mono font-medium text-neutral-50">{p.symbol}</span>
            <span className={`text-neutral-500 text-[10px] transition-transform ${open ? 'rotate-180' : ''}`} aria-hidden>▾</span>
          </div>
          {p.sector && <div className="text-[11px] text-neutral-400 truncate max-w-[10rem]">{p.sector}</div>}
        </td>
        <td className="p-2.5 text-right font-mono text-neutral-200 hidden md:table-cell">{p.qty}</td>
        <td className="p-2.5 text-right font-mono text-neutral-200 hidden md:table-cell">{p.entry_price.toFixed(2)}</td>
        <td className="p-2.5 text-right font-mono text-neutral-100">
          {p.current_price != null ? p.current_price.toFixed(2) : '—'}
        </td>
        <td className="p-2.5 text-right font-mono text-neutral-300 hidden md:table-cell">
          {Math.round(investedRs).toLocaleString('en-IN')}
        </td>
        <td className="p-2.5 text-right font-mono text-neutral-100 hidden md:table-cell">
          {Math.round(curValRs).toLocaleString('en-IN')}
        </td>
        <td className={`p-2.5 text-right font-mono ${dirCls(pnlRs)}`}>
          {pnlRs >= 0 ? '+' : '−'}{Math.abs(Math.round(pnlRs)).toLocaleString('en-IN')}
        </td>
        <td className={`p-2.5 text-right font-mono ${dirCls(pnlPct)}`}>
          {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(2)}%
        </td>
      </tr>
      {open && (
        <tr className="bg-neutral-950/60 border-t border-neutral-800">
          <td colSpan={8} className="p-3 md:p-4">
            <div className="space-y-3 text-sm">
              {p.story_on_entry && (
                <div>
                  <h4 className="text-[10px] uppercase tracking-wider text-mint-400 mb-1 font-semibold">
                    Why the engine bought this
                  </h4>
                  <p className="text-neutral-100 leading-relaxed">{p.story_on_entry}</p>
                </div>
              )}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                <KV label="Signal date"  value={p.signal_date} />
                <KV label="Entry date"    value={p.entry_date} />
                <KV label="Stop loss"     value={`₹${p.sl_level.toFixed(2)}`} accent="red" />
                <KV label="Target"        value={p.target_level != null ? `₹${p.target_level.toFixed(2)}` : 'smart trail'} accent="green" />
              </div>
              {Boolean(p.trail_active) && (
                <p className="text-[11px] text-mint-300 font-mono">
                  Smart trailing stop active · high-water ₹{p.trail_high_water?.toFixed(2) ?? '—'}
                </p>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}


function KV({ label, value, accent }: { label: string; value: string; accent?: 'red' | 'green' }) {
  const cls = accent === 'red' ? 'text-red-300' : accent === 'green' ? 'text-green-300' : 'text-neutral-100'
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-neutral-400">{label}</div>
      <div className={`font-mono ${cls}`}>{value}</div>
    </div>
  )
}


function RecentTrades({ trades, totalClosed, scale }: {
  trades: PortfolioTrade[]; totalClosed: number; scale: number
}) {
  if (trades.length === 0) return null
  return (
    <section>
      <header className="flex items-baseline justify-between mb-3">
        <h2 className="text-base font-bold text-neutral-50">
          Recent trades <span className="text-neutral-400 font-mono text-sm">
            ({trades.length} of {totalClosed})
          </span>
        </h2>
      </header>
      <div className="bg-neutral-900 border border-neutral-800 rounded-lg overflow-hidden">
        <table className="w-full text-xs">
          <thead className="bg-neutral-950/60 text-neutral-300 uppercase tracking-wider text-[10px]">
            <tr>
              <th className="text-left p-2.5">Exit date</th>
              <th className="text-left p-2.5">Instrument</th>
              <th className="text-right p-2.5 hidden md:table-cell">Entry → Exit</th>
              <th className="text-right p-2.5">P&L</th>
              <th className="text-right p-2.5">Net %</th>
              <th className="text-left p-2.5 hidden md:table-cell">Reason</th>
            </tr>
          </thead>
          <tbody>
            {trades.map((t, i) => (
              <tr key={`${t.exit_date}-${t.symbol}-${i}`} className="border-t border-neutral-800 hover:bg-neutral-850">
                <td className="p-2.5 font-mono text-neutral-200">{t.exit_date}</td>
                <td className="p-2.5 font-mono text-neutral-50">{t.symbol}</td>
                <td className="p-2.5 text-right font-mono text-neutral-300 hidden md:table-cell">
                  ₹{t.entry_price.toFixed(2)} → ₹{t.exit_price.toFixed(2)}
                </td>
                <td className={`p-2.5 text-right font-mono ${dirCls(t.pnl_rs)}`}>
                  {t.pnl_rs >= 0 ? '+' : '−'}{Math.abs(Math.round(t.pnl_rs * scale)).toLocaleString('en-IN')}
                </td>
                <td className={`p-2.5 text-right font-mono ${dirCls(t.pnl_pct)}`}>
                  {t.pnl_pct >= 0 ? '+' : ''}{t.pnl_pct.toFixed(2)}%
                </td>
                <td className="p-2.5 hidden md:table-cell">
                  <ExitBadge reason={t.exit_reason} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalClosed > trades.length && (
        <p className="mt-2 text-xs text-neutral-400 text-right">
          Showing latest {trades.length} of {totalClosed}.
        </p>
      )}
    </section>
  )
}


function ExitBadge({ reason }: { reason: PortfolioTrade['exit_reason'] }) {
  const map = {
    SL:     ['bg-red-500/10 text-red-300 border-red-500/30',     'Stop Loss'],
    TARGET: ['bg-green-500/10 text-green-300 border-green-500/30', 'Target hit'],
    TRAIL:  ['bg-mint-500/10 text-mint-300 border-mint-500/30', 'Trail Exit (profit captured)'],
    TIME:   ['bg-neutral-700/40 text-neutral-200 border-neutral-600','Time Exit (hold period reached)'],
    MANUAL: ['bg-neutral-700/40 text-neutral-200 border-neutral-600','Manual'],
  } as const
  const [cls, label] = map[reason] ?? map.MANUAL
  return (
    <span className={`inline-flex items-center text-[10px] px-1.5 py-0.5 rounded border font-mono ${cls}`}>
      {label}
    </span>
  )
}
