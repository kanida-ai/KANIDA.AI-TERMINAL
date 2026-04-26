'use client'

import { useCallback, useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import {
  getDashboardHealth,
  getInsights,
  getTop,
  isMarketOpen,
  pct,
  signedPct,
  TIER_COLORS,
  TIER_LABEL,
  type DashboardHealth,
  type InsightBucket,
  type MarketInsights,
  type TopRow,
} from '@/lib/dashboard-api'

// ─────────────────────────────────────────────────────────────────────────────
// KANIDA.AI Terminal — Autonomous Dashboard
// Headlines-first layout. Kanida.AI tells you what it's watching; compact
// ranked lists sit below the fold for power users.
// ─────────────────────────────────────────────────────────────────────────────

type Market = 'NSE' | 'US'

const ACCENT_CLASSES: Record<InsightBucket['accent'], {
  bar: string; text: string; dot: string; ring: string; chipBg: string; chipFg: string; chipBorder: string
}> = {
  emerald: {
    bar: 'bg-emerald-500', text: 'text-emerald-300', dot: 'bg-emerald-400', ring: 'hover:ring-emerald-500/40',
    chipBg: 'bg-emerald-500/10', chipFg: 'text-emerald-300', chipBorder: 'border-emerald-500/30',
  },
  violet: {
    bar: 'bg-violet-500', text: 'text-violet-300', dot: 'bg-violet-400', ring: 'hover:ring-violet-500/40',
    chipBg: 'bg-violet-500/10', chipFg: 'text-violet-300', chipBorder: 'border-violet-500/30',
  },
  rose: {
    bar: 'bg-rose-500', text: 'text-rose-300', dot: 'bg-rose-400', ring: 'hover:ring-rose-500/40',
    chipBg: 'bg-rose-500/10', chipFg: 'text-rose-300', chipBorder: 'border-rose-500/30',
  },
  amber: {
    bar: 'bg-amber-500', text: 'text-amber-300', dot: 'bg-amber-400', ring: 'hover:ring-amber-500/40',
    chipBg: 'bg-amber-500/10', chipFg: 'text-amber-300', chipBorder: 'border-amber-500/30',
  },
  sky: {
    bar: 'bg-sky-500', text: 'text-sky-300', dot: 'bg-sky-400', ring: 'hover:ring-sky-500/40',
    chipBg: 'bg-sky-500/10', chipFg: 'text-sky-300', chipBorder: 'border-sky-500/30',
  },
  zinc: {
    bar: 'bg-zinc-500', text: 'text-zinc-300', dot: 'bg-zinc-400', ring: 'hover:ring-zinc-500/40',
    chipBg: 'bg-zinc-700/40', chipFg: 'text-zinc-300', chipBorder: 'border-zinc-600/40',
  },
}

export default function DashboardPage() {
  const [market, setMarket] = useState<Market>('NSE')

  const [health, setHealth]     = useState<DashboardHealth | null>(null)
  const [insights, setInsights] = useState<MarketInsights | null>(null)
  const [bullish, setBullish]   = useState<TopRow[]>([])
  const [bearish, setBearish]   = useState<TopRow[]>([])
  const [loading, setLoading]   = useState(true)
  const [err, setErr]           = useState<string | null>(null)
  const [lastFetch, setLastFetch] = useState<Date | null>(null)

  const loadAll = useCallback(async () => {
    setLoading(true); setErr(null)
    try {
      const [h, ins, b, s] = await Promise.all([
        getDashboardHealth(),
        getInsights(market) as Promise<MarketInsights>,
        getTop({ market, bias: 'bullish', limit: 10 }),
        getTop({ market, bias: 'bearish', limit: 10 }),
      ])
      setHealth(h)
      setInsights(ins)
      setBullish(b.rows)
      setBearish(s.rows)
      setLastFetch(new Date())
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [market])

  useEffect(() => { loadAll() }, [loadAll])
  useEffect(() => {
    const id = setInterval(loadAll, 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [loadAll])

  const snap = health?.markets[market]
  const marketOpen = isMarketOpen(market)

  const scanLine = useMemo(() => {
    if (!insights) return ''
    const bull = insights.latest_bullish_signal_date
    const bear = insights.latest_bearish_signal_date
    if (!bull && !bear) return ''
    if (bull === bear) return `Last scan: ${bull}`
    return `Bull scan ${bull ?? '–'}  ·  Bear scan ${bear ?? '–'}`
  }, [insights])

  return (
    <main className="mx-auto max-w-[1400px] px-4 pb-16 pt-6 sm:px-6 lg:px-8">

      {/* ════════════ HEADER ═══════════════════════════════════════ */}
      <header className="mb-8 border-b border-zinc-800 pb-6">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-2xl font-semibold tracking-tight text-zinc-100">
                KANIDA<span className="text-emerald-400">.AI</span>
                <span className="ml-2 text-zinc-500 font-normal">Terminal</span>
              </h1>
              <span className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px]
                ${marketOpen
                  ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300'
                  : 'border-zinc-700 bg-zinc-900 text-zinc-400'}`}>
                <span className={`h-1.5 w-1.5 rounded-full ${marketOpen ? 'bg-emerald-400 animate-pulse' : 'bg-zinc-500'}`} />
                {market} {marketOpen ? 'Open' : 'Closed'}
              </span>
            </div>
            <p className="mt-1.5 text-base text-zinc-300">
              {loading && !insights
                ? 'Scanning the market…'
                : insights?.headline ?? 'No insights to report.'}
            </p>
            <p className="mt-0.5 text-xs text-zinc-500">{scanLine}</p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <SegControl<Market>
              value={market}
              options={[{ v: 'NSE', label: 'NSE' }, { v: 'US', label: 'US' }]}
              onChange={setMarket}
            />
            <button
              onClick={loadAll}
              disabled={loading}
              className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs
                         text-zinc-200 hover:bg-zinc-800 disabled:opacity-50">
              {loading ? 'Refreshing…' : 'Refresh'}
            </button>
            <span className="text-[11px] text-zinc-500">
              {lastFetch ? lastFetch.toLocaleTimeString() : '—'}
            </span>
          </div>
        </div>

        {/* Quick stats strip */}
        <div className="mt-5 flex flex-wrap items-center gap-2">
          <StatChip label="Bullish firing" value={snap?.firing_bullish ?? 0} tone="emerald" />
          <StatChip label="Bearish firing" value={snap?.firing_bearish ?? 0} tone="rose" />
          <StatChip label="Core Signals"   value={snap?.core_signals   ?? 0} tone="emerald" />
          <StatChip label="Rare Edges"     value={snap?.rare_edges     ?? 0} tone="violet" />
          <StatChip label="Active roster"  value={snap?.active_roster  ?? 0} tone="zinc" />
        </div>
      </header>

      {err && (
        <div className="mb-6 rounded-lg border border-rose-600/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">
          {err}
        </div>
      )}

      {/* ════════════ INSIGHT CARDS ═══════════════════════════════ */}
      <section className="mb-10">
        <div className="mb-3 flex items-baseline justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-400">
            Today's Highlights
          </h2>
          <span className="text-[11px] text-zinc-600">
            Click any ticker for a conversation with KANIDA.AI
          </span>
        </div>

        {loading && !insights ? (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {[0,1,2,3,4,5].map(i => (
              <div key={i} className="h-56 animate-pulse rounded-xl border border-zinc-800 bg-zinc-900/40" />
            ))}
          </div>
        ) : insights && insights.insights.length > 0 ? (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {insights.insights.map(b => (
              <InsightCard key={b.id} bucket={b} market={market} />
            ))}
          </div>
        ) : (
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-6 text-center text-sm text-zinc-500">
            No highlights to report right now.
          </div>
        )}
      </section>

      {/* ════════════ COMPACT RANKED LISTS (below the fold) ═══════ */}
      <section className="mb-6">
        <div className="mb-3 flex items-baseline justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-400">
            Ranked Ladder
          </h2>
          <span className="text-[11px] text-zinc-600">
            For the detail-oriented — same edges, ranked Top 10.
          </span>
        </div>
        <div className="grid gap-4 lg:grid-cols-2">
          <RankedColumn title="Top 10 Bullish" accent="emerald" rows={bullish} loading={loading} />
          <RankedColumn title="Top 10 Bearish" accent="rose"    rows={bearish} loading={loading} />
        </div>
      </section>

      <footer className="mt-12 text-center text-[11px] text-zinc-600">
        KANIDA.AI · autonomous quant intelligence · auto-refreshes every 5 minutes ·
        historical data, not financial advice.
      </footer>
    </main>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// INSIGHT CARD
// ─────────────────────────────────────────────────────────────────────────────

function InsightCard({ bucket, market }: { bucket: InsightBucket; market: Market }) {
  const a = ACCENT_CLASSES[bucket.accent]
  return (
    <div className={`group flex flex-col rounded-xl border border-zinc-800 bg-zinc-900/40
                     transition hover:bg-zinc-900/70 hover:ring-1 ${a.ring}`}>
      <div className="flex items-start gap-3 border-b border-zinc-800/80 px-4 py-3">
        <span className={`mt-1 h-8 w-1 rounded-full ${a.bar}`} />
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <h3 className={`text-sm font-semibold ${a.text}`}>{bucket.title}</h3>
            <span className={`inline-flex items-center gap-1 rounded-full border px-1.5 py-0 text-[10px]
                              ${a.chipBg} ${a.chipFg} ${a.chipBorder}`}>
              <span className={`h-1 w-1 rounded-full ${a.dot}`} />
              {bucket.count}
            </span>
          </div>
          <p className="mt-1 text-[13px] leading-snug text-zinc-400">{bucket.one_liner}</p>
        </div>
      </div>

      <ul className="flex-1 divide-y divide-zinc-800/70">
        {bucket.tickers.slice(0, 6).map(t => (
          <li key={t.ticker}>
            <Link
              href={`/dashboard/stock/${encodeURIComponent(t.ticker)}?market=${market}&prompt=${t.drill_prompt_id}`}
              className="flex items-start justify-between gap-3 px-4 py-2 text-sm
                         transition hover:bg-zinc-800/60">
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-sm font-semibold text-zinc-100">{t.ticker}</span>
                  {t.company_name && (
                    <span className="truncate text-[11px] text-zinc-500">{t.company_name}</span>
                  )}
                </div>
                <div className="mt-0.5 truncate text-[11px] text-zinc-400">
                  {t.one_line_reason}
                </div>
              </div>
              <span className="mt-0.5 shrink-0 text-[10px] text-zinc-600 opacity-0 transition
                               group-hover:opacity-100">
                Ask KANIDA.AI →
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPACT RANKED LIST
// ─────────────────────────────────────────────────────────────────────────────

function RankedColumn({
  title, accent, rows, loading,
}: { title: string; accent: 'emerald' | 'rose'; rows: TopRow[]; loading: boolean }) {
  const bar = accent === 'emerald' ? 'bg-emerald-500' : 'bg-rose-500'
  const text = accent === 'emerald' ? 'text-emerald-300' : 'text-rose-300'
  return (
    <div className="rounded-xl border border-zinc-800 bg-zinc-900/30">
      <div className="flex items-center gap-3 border-b border-zinc-800 px-4 py-2.5">
        <span className={`h-4 w-1 rounded-full ${bar}`} />
        <h3 className={`text-sm font-semibold ${text}`}>{title}</h3>
        <span className="ml-auto rounded border border-zinc-700 bg-zinc-950 px-1.5 py-0.5 text-[10px] text-zinc-400">
          {rows.length}
        </span>
      </div>
      {loading && rows.length === 0 ? (
        <div className="p-6 text-center text-sm text-zinc-500">Loading…</div>
      ) : rows.length === 0 ? (
        <div className="p-6 text-center text-sm text-zinc-500">No setups surfaced.</div>
      ) : (
        <ul className="divide-y divide-zinc-800/70">
          {rows.map(r => <CompactRow key={`${r.ticker}-${r.timeframe}-${r.strategy_name}-${r.bias}`} row={r} />)}
        </ul>
      )}
    </div>
  )
}

function CompactRow({ row }: { row: TopRow }) {
  const c = TIER_COLORS[row.tier] ?? TIER_COLORS.emerging
  const firing = row.firing_today === 1
  return (
    <li>
      <Link
        href={`/dashboard/stock/${encodeURIComponent(row.ticker)}?market=${row.market}`}
        className="block px-4 py-2 transition hover:bg-zinc-800/50"
      >
        <div className="flex items-center gap-3">
          <span className="w-6 text-[11px] tabular-nums text-zinc-600">#{row.rank}</span>
          <div className="flex-1 min-w-0">
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="font-mono text-sm font-semibold text-zinc-100">{row.ticker}</span>
              <span className={`rounded border px-1 py-0 text-[10px] ${c.bg} ${c.fg} ${c.border}`}>
                {TIER_LABEL[row.tier] ?? row.tier}
              </span>
              <span className="text-[10px] text-zinc-500">
                {row.timeframe === '1D' ? 'D' : 'W'}
              </span>
              {firing && (
                <span className="inline-flex items-center gap-1 rounded border border-amber-500/40
                                 bg-amber-500/10 px-1 py-0 text-[10px] text-amber-300">
                  <span className="h-1 w-1 rounded-full bg-amber-400 animate-pulse" />
                  firing
                </span>
              )}
            </div>
            <div className="truncate text-[10px] text-zinc-500">{row.company_name ?? row.strategy_name}</div>
          </div>
          <div className="hidden shrink-0 gap-3 text-right sm:flex">
            <Mini label="win" value={pct(row.win_rate_15d)} />
            <Mini label="15d" value={signedPct(row.avg_ret_15d)} tone={(row.avg_ret_15d ?? 0) >= 0 ? 'pos' : 'neg'} />
            <Mini label="fit" value={row.fitness_score.toFixed(0)} />
          </div>
        </div>
      </Link>
    </li>
  )
}

function Mini({ label, value, tone }: { label: string; value: string; tone?: 'pos' | 'neg' }) {
  const color = tone === 'pos' ? 'text-emerald-300' : tone === 'neg' ? 'text-rose-300' : 'text-zinc-200'
  return (
    <div className="w-12 text-right">
      <div className={`font-mono text-[11px] tabular-nums ${color}`}>{value}</div>
      <div className="text-[9px] uppercase tracking-wider text-zinc-600">{label}</div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SHARED COMPONENTS
// ─────────────────────────────────────────────────────────────────────────────

function SegControl<T extends string>({
  value, options, onChange,
}: {
  value: T
  options: { v: T; label: string }[]
  onChange: (v: T) => void
}) {
  return (
    <div className="inline-flex rounded-md border border-zinc-700 bg-zinc-900 p-0.5">
      {options.map(o => (
        <button
          key={o.v}
          onClick={() => onChange(o.v)}
          className={`rounded px-3 py-1 text-xs transition
            ${value === o.v ? 'bg-zinc-700 text-zinc-50' : 'text-zinc-400 hover:text-zinc-200'}`}>
          {o.label}
        </button>
      ))}
    </div>
  )
}

function StatChip({
  label, value, tone,
}: { label: string; value: number; tone: 'emerald' | 'rose' | 'violet' | 'zinc' }) {
  const colorMap = {
    emerald: 'text-emerald-300 border-emerald-500/30',
    rose:    'text-rose-300 border-rose-500/30',
    violet:  'text-violet-300 border-violet-500/30',
    zinc:    'text-zinc-300 border-zinc-600/30',
  }
  return (
    <span className={`inline-flex items-baseline gap-1.5 rounded-md border bg-zinc-900/60
                       px-2.5 py-1 text-xs ${colorMap[tone]}`}>
      <span className="font-mono text-sm font-semibold tabular-nums">{value.toLocaleString()}</span>
      <span className="text-zinc-500">{label}</span>
    </span>
  )
}
