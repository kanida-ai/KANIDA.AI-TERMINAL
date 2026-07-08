'use client'

/**
 * AutoTrade · Performance — strategy-level P&L dashboard.
 *
 * Faithful rebuild of the approved mockup (scratchpad/autotrade_pnl.html) as a
 * real portal page: theme tokens only (T + mint CSS vars), monospace tabular
 * numbers, profit=green / loss=red / charges=amber / mint=interactive.
 *
 * Data: PowerAPI.autotradePnlSummary(period, opts, jwt) — period switch refetches
 * server-side; Strategy/Segment/Product/search filter CLIENT-side on the returned
 * strategies and RE-COMPUTE the summary totals from the visible set; column sort
 * is client-side. CSV export streams through the Bearer-authed endpoint.
 *
 * Strategy names/segments/products come entirely from the API — nothing is
 * hardcoded. "Futures" is a SEGMENT, not a strategy.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { T } from '@/lib/theme'
import { useBreakpoint } from '@/lib/terminal-ui'
import {
  PowerAPI, PowerAPIError,
  type AutotradePnlSummary, type PnlPeriod, type PnlStrategy,
} from '@/lib/power-api'
import { sampleAutotradePnl } from '@/lib/fixtures/autotrade-pnl'

// ── design tokens (mint = interactive only) ─────────────────────────────────
const MINT     = 'var(--color-mint-400)'      // #3FE3A4
const MINT_DIM = 'rgba(63,227,164,0.14)'      // mint-400 @ .14 — selected/active tint
const MINT_BD  = 'rgba(63,227,164,0.30)'
const USE_FIXTURE = process.env.NEXT_PUBLIC_PNL_FIXTURE === '1'

// ── format helpers ──────────────────────────────────────────────────────────
const INR  = (n: number) => (n < 0 ? '−' : '') + '₹' + Math.abs(Math.round(n)).toLocaleString('en-IN')
const sgn  = (n: number) => (n > 0 ? '+' : '')
const pnlC = (n: number) => (n >= 0 ? T.g : T.r)
const numFont: React.CSSProperties = { fontFamily: T.mono, fontVariantNumeric: 'tabular-nums', letterSpacing: '-.2px' }

type SortKey = 'name' | 'net' | 'gross' | 'charges' | 'trades' | 'win_rate' | 'avg' | 'best'

const PERIODS: { p: PnlPeriod; label: string }[] = [
  { p: 'yesterday', label: 'Yesterday' },
  { p: '1w',        label: '1 Week' },
  { p: '2w',        label: '2 Weeks' },
  { p: 'mtd',       label: 'MTD' },
  { p: 'ytd',       label: 'YTD' },
]

const fmtRange = (from: string | null, to: string | null) => {
  const d = (s: string) => {
    const dt = new Date(s + 'T00:00:00')
    return dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
  }
  if (!from || !to) return ''
  return `${d(from)} – ${d(to)} ${new Date(to + 'T00:00:00').getFullYear()}`
}


export function PnlDashboard({ jwt }: { jwt: string }) {
  const { width } = useBreakpoint()
  const narrow = width < 900

  const [period, setPeriod]   = useState<PnlPeriod>('mtd')
  const [range, setRange]     = useState<{ from: string; to: string }>({ from: '2026-06-21', to: '2026-07-07' })
  const [pickerOpen, setPickerOpen] = useState(false)

  const [data, setData]       = useState<AutotradePnlSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)
  const [paywall, setPaywall] = useState<{ url: string | null } | null>(null)

  // filters + sort (client-side)
  const [fStrategy, setFStrategy] = useState<Set<string>>(new Set())
  const [fSegment,  setFSegment]  = useState<Set<string>>(new Set())
  const [fProduct,  setFProduct]  = useState<Set<string>>(new Set())
  const [query, setQuery]         = useState('')
  const [sortKey, setSortKey]     = useState<SortKey>('net')
  const [sortDir, setSortDir]     = useState<-1 | 1>(-1)
  const [openIds, setOpenIds]     = useState<Set<string>>(new Set())

  // ── fetch on period / applied range change ──────────────────────────────
  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true); setError(null); setPaywall(null)
    const opts = period === 'custom'
      ? { from: range.from, to: range.to, mode: 'live' as const }
      : { mode: 'live' as const }
    try {
      const res = USE_FIXTURE
        ? sampleAutotradePnl(period, opts)
        : await PowerAPI.autotradePnlSummary(period, opts, jwt, signal)
      setData(res)
    } catch (e) {
      if ((e as { name?: string })?.name === 'AbortError') return
      if (e instanceof PowerAPIError && (e.status === 402 || e.code === 'PAYMENT_REQUIRED')) {
        const u = e.detail?.checkout_url
        setPaywall({ url: typeof u === 'string' && u.length > 0 ? u : null })
      } else {
        setError(e instanceof Error ? e.message : 'Could not load P&L.')
      }
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [period, range.from, range.to, jwt])

  useEffect(() => {
    const ctrl = new AbortController()
    load(ctrl.signal)
    return () => ctrl.abort()
  }, [load])

  // ── filter option lists — derived from the API (never hardcoded) ─────────
  const strategies = data?.strategies ?? []
  const optStrategy = useMemo(() => strategies.map(s => ({ v: s.id, label: s.name })), [strategies])
  const optSegment  = useMemo(() => uniq(strategies.flatMap(s => s.segments)), [strategies])
  const optProduct  = useMemo(() => uniq(strategies.flatMap(s => s.products)), [strategies])

  const passes = useCallback((s: PnlStrategy) => {
    if (fStrategy.size && !fStrategy.has(s.id)) return false
    if (fSegment.size  && !s.segments.some(g => fSegment.has(g))) return false
    if (fProduct.size  && !s.products.some(p => fProduct.has(p))) return false
    if (query) {
      const q = query.toLowerCase()
      if (!s.name.toLowerCase().includes(q) &&
          !s.sessions.some(se => se.name.toLowerCase().includes(q))) return false
    }
    return true
  }, [fStrategy, fSegment, fProduct, query])

  const visible = useMemo(() => {
    const rows = strategies.filter(passes)
    return [...rows].sort((a, b) => {
      const av = sortKey === 'best' ? a.best.pnl : a[sortKey]
      const bv = sortKey === 'best' ? b.best.pnl : b[sortKey]
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return cmp * sortDir
    })
  }, [strategies, passes, sortKey, sortDir])

  // ── summary re-computed from the VISIBLE set (matches the mockup) ─────────
  const totals = useMemo(() => {
    const t = visible.reduce(
      (o, s) => ({
        net: o.net + s.net, gross: o.gross + s.gross, charges: o.charges + s.charges,
        trades: o.trades + s.trades, wins: o.wins + s.wins, losses: o.losses + s.losses,
      }),
      { net: 0, gross: 0, charges: 0, trades: 0, wins: 0, losses: 0 },
    )
    const win_rate = t.trades ? Math.round((t.wins / t.trades) * 100) : 0
    return { ...t, win_rate }
  }, [visible])

  const best  = useMemo(() => [...visible].sort((a, b) => b.net - a.net)[0] ?? null, [visible])
  const worst = useMemo(() => [...visible].sort((a, b) => a.net - b.net)[0] ?? null, [visible])

  const toggleSet = (set: Set<string>, setter: (s: Set<string>) => void, v: string) => {
    const next = new Set(set); next.has(v) ? next.delete(v) : next.add(v); setter(next)
  }
  const onSort = (k: SortKey) => {
    if (k === sortKey) setSortDir(d => (d * -1) as -1 | 1)
    else { setSortKey(k); setSortDir(-1) }
  }

  return (
    <div style={{ color: T.t, fontSize: 14, lineHeight: 1.45 }}>
      <style>{`@keyframes pnlShimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}`}</style>
      <Header
        period={period}
        rangeLabel={period === 'custom' ? fmtRange(data?.from ?? range.from, data?.to ?? range.to) : null}
        onPeriod={(p) => {
          if (p === 'custom') { setPeriod('custom'); setPickerOpen(o => !o) }
          else { setPeriod(p); setPickerOpen(false) }
        }}
        pickerOpen={pickerOpen}
        range={range}
        onApplyRange={(from, to) => { setRange({ from, to }); setPeriod('custom'); setPickerOpen(false) }}
        onCloseRange={() => setPickerOpen(false)}
      />

      {paywall ? (
        <PaywallCard url={paywall.url} />
      ) : error ? (
        <ErrorCard message={error} onRetry={() => load()} />
      ) : (
        <>
          <SummaryBand
            loading={loading}
            narrow={narrow}
            totals={totals}
            capital={data?.capital_deployed ?? 0}
            best={best}
            worst={worst}
            spark={visible.map(s => s.net)}
          />

          <Toolbar
            optStrategy={optStrategy} optSegment={optSegment} optProduct={optProduct}
            fStrategy={fStrategy} fSegment={fSegment} fProduct={fProduct}
            onToggleStrategy={(v) => toggleSet(fStrategy, setFStrategy, v)}
            onToggleSegment={(v) => toggleSet(fSegment, setFSegment, v)}
            onToggleProduct={(v) => toggleSet(fProduct, setFProduct, v)}
            query={query} onQuery={setQuery}
            onExport={() => PowerAPI.autotradePnlExport(
              period,
              period === 'custom' ? { from: range.from, to: range.to, mode: 'live' } : { mode: 'live' },
              jwt,
            )}
            fixtureExport={USE_FIXTURE ? () => sampleAutotradePnl(period, {}).strategies : null}
          />

          <StrategyTable
            loading={loading}
            rows={visible}
            sortKey={sortKey} sortDir={sortDir} onSort={onSort}
            openIds={openIds}
            onToggleOpen={(id) => toggleSet(openIds, setOpenIds, id)}
          />

          <Footnote asOf={data?.as_of ?? null} />
        </>
      )}
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────────
// Header + period control + custom range picker
// ─────────────────────────────────────────────────────────────────────────────
function Header({
  period, rangeLabel, onPeriod, pickerOpen, range, onApplyRange, onCloseRange,
}: {
  period: PnlPeriod
  rangeLabel: string | null
  onPeriod: (p: PnlPeriod) => void
  pickerOpen: boolean
  range: { from: string; to: string }
  onApplyRange: (from: string, to: string) => void
  onCloseRange: () => void
}) {
  return (
    <header style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 20, flexWrap: 'wrap', marginBottom: 22 }}>
      <div>
        <div style={{ fontSize: 11, letterSpacing: '.22em', textTransform: 'uppercase', color: MINT, fontWeight: 600, marginBottom: 6 }}>
          AutoTrade · Performance
        </div>
        <h1 style={{ margin: 0, fontSize: 25, fontWeight: 640, letterSpacing: '-.3px' }}>Strategy P&amp;L</h1>
        <div style={{ color: T.t2, fontSize: 12.5, marginTop: 5 }}>
          {period === 'custom' && rangeLabel
            ? <>Custom range · <b style={{ color: T.t, fontWeight: 600 }}>{rangeLabel}</b> — pick any window.</>
            : <>Which strategy made money, which lost — and your <b style={{ color: T.t, fontWeight: 600 }}>real net after charges</b>.</>}
        </div>
      </div>

      <div style={{ position: 'relative' }}>
        <div role="tablist" style={{ display: 'inline-flex', background: T.s1, border: `1px solid ${T.b}`, borderRadius: 11, padding: 3, gap: 2, flexWrap: 'wrap' }}>
          {PERIODS.map(({ p, label }) => (
            <SegBtn key={p} active={period === p} onClick={() => onPeriod(p)}>{label}</SegBtn>
          ))}
          <SegBtn active={period === 'custom'} onClick={() => onPeriod('custom')} custom>Custom ▾</SegBtn>
        </div>
        {pickerOpen && (
          <RangePicker range={range} onApply={onApplyRange} onClose={onCloseRange} />
        )}
      </div>
    </header>
  )
}

function SegBtn({ active, custom, onClick, children }: {
  active: boolean; custom?: boolean; onClick: () => void; children: React.ReactNode
}) {
  return (
    <button
      role="tab" aria-selected={active} onClick={onClick}
      style={{
        fontFamily: 'inherit', fontSize: 12.5, fontWeight: active ? 600 : 500,
        color: active ? MINT : custom ? T.t3 : T.t2,
        background: active ? MINT_DIM : 'transparent',
        border: 0, padding: '7px 13px', borderRadius: 8, cursor: 'pointer',
        whiteSpace: 'nowrap', transition: '.15s',
      }}
    >
      {children}
    </button>
  )
}

function RangePicker({ range, onApply, onClose }: {
  range: { from: string; to: string }
  onApply: (from: string, to: string) => void
  onClose: () => void
}) {
  const [from, setFrom] = useState(range.from)
  const [to, setTo]     = useState(range.to)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) onClose() }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [onClose])
  const valid = from && to && from <= to
  const inputStyle: React.CSSProperties = {
    background: T.bg, border: `1px solid ${T.b}`, borderRadius: 8, color: T.t,
    fontFamily: T.mono, fontSize: 12.5, padding: '7px 9px', width: '100%', colorScheme: 'dark',
  }
  return (
    <div ref={ref} style={{
      position: 'absolute', zIndex: 30, top: 'calc(100% + 8px)', right: 0, minWidth: 240,
      background: T.s2, border: `1px solid ${T.b2}`, borderRadius: 12, padding: 14,
      boxShadow: '0 18px 40px rgba(0,0,0,.5)',
    }}>
      <div style={{ display: 'grid', gap: 10 }}>
        <label style={{ fontSize: 10.5, letterSpacing: '.08em', textTransform: 'uppercase', color: T.t3 }}>
          From
          <input type="date" value={from} max={to} onChange={e => setFrom(e.target.value)} style={{ ...inputStyle, marginTop: 5 }} />
        </label>
        <label style={{ fontSize: 10.5, letterSpacing: '.08em', textTransform: 'uppercase', color: T.t3 }}>
          To
          <input type="date" value={to} min={from} onChange={e => setTo(e.target.value)} style={{ ...inputStyle, marginTop: 5 }} />
        </label>
        <button
          disabled={!valid}
          onClick={() => valid && onApply(from, to)}
          style={{
            marginTop: 2, fontFamily: 'inherit', fontSize: 12.5, fontWeight: 600,
            color: valid ? '#04140d' : T.t3, background: valid ? MINT : T.s3,
            border: 'none', borderRadius: 8, padding: '8px 12px', cursor: valid ? 'pointer' : 'not-allowed',
          }}
        >
          Apply range
        </button>
      </div>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────────
// Summary band
// ─────────────────────────────────────────────────────────────────────────────
function SummaryBand({ loading, narrow, totals, capital, best, worst, spark }: {
  loading: boolean; narrow: boolean
  totals: { net: number; gross: number; charges: number; trades: number; wins: number; losses: number; win_rate: number }
  capital: number
  best: PnlStrategy | null; worst: PnlStrategy | null
  spark: number[]
}) {
  const gridStyle: React.CSSProperties = {
    display: 'grid',
    gridTemplateColumns: narrow ? '1fr 1fr' : '1.55fr 1fr 1fr',
    gap: 12, marginBottom: 14,
  }
  const heroStyle: React.CSSProperties = narrow
    ? { gridColumn: 'span 2' }
    : { gridRow: 'span 2' }

  if (loading && totals.trades === 0) return <SummarySkeleton narrow={narrow} />

  return (
    <section style={gridStyle}>
      {/* Hero — Net P&L */}
      <Card style={{ ...heroStyle, position: 'relative', overflow: 'hidden', display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
        <div style={{ position: 'absolute', inset: '0 0 auto 0', height: 2, background: `linear-gradient(90deg, ${MINT}, transparent 70%)` }} />
        <div>
          <Lbl>Net P&amp;L · after charges</Lbl>
          <div style={{ ...numFont, fontSize: 40, fontWeight: 600, letterSpacing: '-1px', lineHeight: 1, marginTop: 12, color: pnlC(totals.net) }}>
            {sgn(totals.net)}{INR(totals.net)}
          </div>
          <div style={{ display: 'flex', gap: 16, color: T.t2, fontSize: 12.5, marginTop: 12, flexWrap: 'wrap' }}>
            <span>Gross <b style={{ ...numFont, fontWeight: 600, color: T.t2 }}>{sgn(totals.gross)}{INR(totals.gross)}</b></span>
            <span>Charges &amp; taxes <b style={{ ...numFont, fontWeight: 600, color: T.a }}>{INR(totals.charges)}</b></span>
            <span>on <b style={{ ...numFont, fontWeight: 600, color: T.t2 }}>{INR(capital)}</b> deployed</span>
          </div>
        </div>
        <Spark values={spark} />
      </Card>

      {/* Trades */}
      <Card>
        <Lbl>Trades</Lbl>
        <div style={{ ...numFont, fontSize: 23, fontWeight: 600, letterSpacing: '-.5px', marginTop: 9 }}>
          {totals.trades.toLocaleString('en-IN')}
        </div>
        <div style={{ fontSize: 12, color: T.t2, marginTop: 5 }}>
          <span style={{ ...numFont, color: T.g }}>{totals.wins}</span> won ·{' '}
          <span style={{ ...numFont, color: T.r }}>{totals.losses}</span> lost
        </div>
      </Card>

      {/* Win rate */}
      <Card>
        <Lbl>Win rate</Lbl>
        <div style={{ display: 'flex', alignItems: 'center', gap: 13, marginTop: 9 }}>
          <Ring pct={totals.win_rate} />
          <div>
            <div style={{ ...numFont, fontSize: 23, fontWeight: 600, letterSpacing: '-.5px' }}>{totals.win_rate}%</div>
            <div style={{ fontSize: 12, color: T.t2, marginTop: 5 }}>of closed trades</div>
          </div>
        </div>
      </Card>

      {/* Best strategy */}
      <Card>
        <Lbl>Best strategy</Lbl>
        <div style={{ ...numFont, fontSize: 16, fontWeight: 600, marginTop: 9, color: T.g, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {best ? best.name : '—'}
        </div>
        <div style={{ fontSize: 12, marginTop: 5, ...numFont, color: best ? pnlC(best.net) : T.t2 }}>
          {best ? `${sgn(best.net)}${INR(best.net)}` : '—'}
        </div>
      </Card>

      {/* Needs attention */}
      <Card>
        <Lbl>Needs attention</Lbl>
        <div style={{ ...numFont, fontSize: 16, fontWeight: 600, marginTop: 9, color: T.r, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {worst ? worst.name : '—'}
        </div>
        <div style={{ fontSize: 12, marginTop: 5, ...numFont, color: worst ? pnlC(worst.net) : T.t2 }}>
          {worst ? `${sgn(worst.net)}${INR(worst.net)}` : '—'}
        </div>
      </Card>
    </section>
  )
}

function Card({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 14, padding: '16px 18px', boxSizing: 'border-box', ...style }}>
      {children}
    </div>
  )
}
function Lbl({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 11, letterSpacing: '.11em', textTransform: 'uppercase', color: T.t3, fontWeight: 600 }}>
      {children}
    </div>
  )
}

function Ring({ pct }: { pct: number }) {
  return (
    <div style={{
      position: 'relative', width: 46, height: 46, borderRadius: '50%', flex: 'none',
      background: `conic-gradient(${T.g} ${pct}%, ${T.s3} 0)`,
      display: 'grid', placeItems: 'center',
    }}>
      <div style={{ width: 34, height: 34, borderRadius: '50%', background: T.s1 }} />
      <span style={{ position: 'absolute', ...numFont, fontSize: 12, fontWeight: 600, color: T.t }}>{pct}%</span>
    </div>
  )
}

function Spark({ values }: { values: number[] }) {
  const mx = Math.max(...values.map(v => Math.abs(v)), 1)
  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 3, height: 44, marginTop: 16 }} title="Net by strategy">
      {values.length === 0
        ? <div style={{ color: T.t3, fontSize: 12 }}>No P&amp;L in this window.</div>
        : values.map((v, i) => (
            <div key={i} title={`${sgn(v)}${INR(v)}`} style={{
              flex: 1, borderRadius: '2px 2px 0 0', minHeight: 2, opacity: 0.85,
              height: 10 + (Math.abs(v) / mx) * 34, background: pnlC(v),
            }} />
          ))}
    </div>
  )
}

function SummarySkeleton({ narrow }: { narrow: boolean }) {
  const cells = narrow ? 5 : 5
  return (
    <section style={{ display: 'grid', gridTemplateColumns: narrow ? '1fr 1fr' : '1.55fr 1fr 1fr', gap: 12, marginBottom: 14 }}>
      {Array.from({ length: cells }).map((_, i) => (
        <div key={i} style={{
          background: T.s1, border: `1px solid ${T.b}`, borderRadius: 14, height: i === 0 ? 132 : 92,
          gridColumn: i === 0 && narrow ? 'span 2' : undefined,
          gridRow: i === 0 && !narrow ? 'span 2' : undefined,
          ...shimmer,
        }} />
      ))}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────────
// Toolbar — filters + search + export
// ─────────────────────────────────────────────────────────────────────────────
function Toolbar({
  optStrategy, optSegment, optProduct,
  fStrategy, fSegment, fProduct,
  onToggleStrategy, onToggleSegment, onToggleProduct,
  query, onQuery, onExport, fixtureExport,
}: {
  optStrategy: { v: string; label: string }[]
  optSegment: string[]; optProduct: string[]
  fStrategy: Set<string>; fSegment: Set<string>; fProduct: Set<string>
  onToggleStrategy: (v: string) => void
  onToggleSegment: (v: string) => void
  onToggleProduct: (v: string) => void
  query: string; onQuery: (v: string) => void
  onExport: () => Promise<Blob>
  fixtureExport: (() => PnlStrategy[]) | null
}) {
  const [openMenu, setOpenMenu] = useState<string | null>(null)
  const [dl, setDl] = useState<'idle' | 'busy' | 'done' | 'err'>('idle')
  const barRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const h = (e: MouseEvent) => { if (barRef.current && !barRef.current.contains(e.target as Node)) setOpenMenu(null) }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  const doExport = async () => {
    setDl('busy')
    try {
      let blob: Blob
      if (fixtureExport) {
        const rows = fixtureExport()
        const header = 'Strategy,Segment,Product,Net_PnL,Gross_PnL,Charges,Trades,Win_Rate,Avg_Per_Trade\n'
        const body = rows.map(s => `"${s.name}",${s.segment},"${s.product}",${s.net},${s.gross},${s.charges},${s.trades},${s.win_rate}%,${s.avg}`).join('\n')
        blob = new Blob([header + body], { type: 'text/csv' })
      } else {
        blob = await onExport()
      }
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = 'autotrade_pnl.csv'; a.click()
      URL.revokeObjectURL(url)
      setDl('done'); setTimeout(() => setDl('idle'), 1600)
    } catch {
      setDl('err'); setTimeout(() => setDl('idle'), 2200)
    }
  }

  return (
    <div ref={barRef} style={{ display: 'flex', alignItems: 'center', gap: 9, flexWrap: 'wrap', margin: '20px 0 12px' }}>
      <FilterMenu label="Strategy" open={openMenu === 'strategy'} onOpen={() => setOpenMenu(m => m === 'strategy' ? null : 'strategy')}
        options={optStrategy} selected={fStrategy} onToggle={onToggleStrategy} />
      <FilterMenu label="Segment" open={openMenu === 'segment'} onOpen={() => setOpenMenu(m => m === 'segment' ? null : 'segment')}
        options={optSegment.map(s => ({ v: s, label: s }))} selected={fSegment} onToggle={onToggleSegment} />
      <FilterMenu label="Product" open={openMenu === 'product'} onOpen={() => setOpenMenu(m => m === 'product' ? null : 'product')}
        options={optProduct.map(s => ({ v: s, label: s }))} selected={fProduct} onToggle={onToggleProduct} />

      <div style={{ flex: 1, minWidth: 150, display: 'flex', alignItems: 'center', gap: 8, background: T.s1, border: `1px solid ${T.b}`, borderRadius: 9, padding: '8px 12px' }}>
        <span style={{ color: T.t3 }}>⌕</span>
        <input
          value={query} onChange={e => onQuery(e.target.value)}
          placeholder="Find a session or campaign…"
          style={{ background: 'none', border: 0, outline: 'none', color: T.t, fontFamily: 'inherit', fontSize: 13, width: '100%' }}
        />
      </div>

      <button
        onClick={doExport} disabled={dl === 'busy'}
        style={{
          fontFamily: 'inherit', fontSize: 12.5, fontWeight: 600,
          color: dl === 'err' ? T.r : MINT, background: MINT_DIM,
          border: `1px solid ${dl === 'err' ? 'rgba(255,77,109,.4)' : MINT_BD}`,
          borderRadius: 9, padding: '8px 14px', cursor: dl === 'busy' ? 'wait' : 'pointer',
          display: 'flex', alignItems: 'center', gap: 7, whiteSpace: 'nowrap',
        }}
      >
        {dl === 'busy' ? '… Exporting' : dl === 'done' ? '✓ Downloaded' : dl === 'err' ? '× Export failed' : '↓ Trade log'}
      </button>
    </div>
  )
}

function FilterMenu({ label, open, onOpen, options, selected, onToggle }: {
  label: string; open: boolean; onOpen: () => void
  options: { v: string; label: string }[]
  selected: Set<string>; onToggle: (v: string) => void
}) {
  const n = selected.size
  return (
    <div style={{ position: 'relative' }}>
      <button
        onClick={onOpen}
        style={{
          fontFamily: 'inherit', fontSize: 12.5, color: T.t,
          background: n ? MINT_DIM : T.s1,
          border: `1px solid ${n ? MINT_BD : T.b}`,
          borderRadius: 9, padding: '8px 12px', cursor: 'pointer',
          display: 'flex', alignItems: 'center', gap: 8, transition: '.15s',
        }}
      >
        <span style={{ color: T.t3, fontSize: 11, textTransform: 'uppercase', letterSpacing: '.06em' }}>{label}</span>
        <span>{n ? `${n} selected` : 'All'}</span>
        <span style={{ color: T.t3, fontSize: 10 }}>▾</span>
      </button>
      {open && (
        <div style={{
          position: 'absolute', zIndex: 20, top: 'calc(100% + 6px)', left: 0, minWidth: 180,
          background: T.s2, border: `1px solid ${T.b2}`, borderRadius: 11, padding: 6,
          boxShadow: '0 18px 40px rgba(0,0,0,.5)',
        }}>
          {options.length === 0
            ? <div style={{ padding: '8px 9px', fontSize: 12.5, color: T.t3 }}>No options</div>
            : options.map(o => (
              <label key={o.v} style={{ display: 'flex', alignItems: 'center', gap: 9, padding: '8px 9px', borderRadius: 7, cursor: 'pointer', fontSize: 13, color: selected.has(o.v) ? T.t : T.t2 }}>
                <input type="checkbox" checked={selected.has(o.v)} onChange={() => onToggle(o.v)}
                  style={{ accentColor: '#3FE3A4', width: 14, height: 14 }} />
                {o.label}
              </label>
            ))}
        </div>
      )}
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────────
// Strategy table + drill-down
// ─────────────────────────────────────────────────────────────────────────────
const COLS: { key: SortKey; label: string }[] = [
  { key: 'name',     label: 'Strategy' },
  { key: 'net',      label: 'Net P&L' },
  { key: 'gross',    label: 'Gross' },
  { key: 'charges',  label: 'Charges' },
  { key: 'trades',   label: 'Trades' },
  { key: 'win_rate', label: 'Win %' },
  { key: 'avg',      label: 'Avg / trade' },
  { key: 'best',     label: 'Best / Worst session' },
]

function StrategyTable({ loading, rows, sortKey, sortDir, onSort, openIds, onToggleOpen }: {
  loading: boolean
  rows: PnlStrategy[]
  sortKey: SortKey; sortDir: -1 | 1; onSort: (k: SortKey) => void
  openIds: Set<string>; onToggleOpen: (id: string) => void
}) {
  return (
    <div style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 14, overflow: 'hidden' }}>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 820 }}>
          <thead>
            <tr>
              {COLS.map((c, i) => {
                const active = c.key === sortKey
                return (
                  <th key={c.key} onClick={() => onSort(c.key)} style={{
                    fontSize: 11, letterSpacing: '.06em', textTransform: 'uppercase',
                    color: active ? MINT : T.t3, fontWeight: 600,
                    textAlign: i === 0 ? 'left' : 'right', padding: '13px 14px',
                    borderBottom: `1px solid ${T.b}`, cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap',
                  }}>
                    {c.label}
                    <span style={{ fontSize: 9, marginLeft: 3, opacity: active ? 1 : 0, color: MINT }}>
                      {sortDir < 0 ? '▼' : '▲'}
                    </span>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {loading && rows.length === 0
              ? <SkeletonRows />
              : rows.length === 0
                ? <tr><td colSpan={8}><div style={{ padding: 44, textAlign: 'center', color: T.t3, fontSize: 13 }}>No strategies match these filters.</div></td></tr>
                : rows.map(s => (
                  <StrategyRow key={s.id} s={s} open={openIds.has(s.id)} onToggle={() => onToggleOpen(s.id)} />
                ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const td: React.CSSProperties = { padding: 14, borderBottom: `1px solid ${T.b}`, textAlign: 'right', whiteSpace: 'nowrap' }
const tdL: React.CSSProperties = { ...td, textAlign: 'left' }

function StrategyRow({ s, open, onToggle }: { s: PnlStrategy; open: boolean; onToggle: () => void }) {
  const [hover, setHover] = useState(false)
  const wrColor = s.win_rate >= 60 ? T.g : s.win_rate < 50 ? T.r : T.t
  return (
    <>
      <tr
        onClick={onToggle}
        onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
        style={{ cursor: 'pointer', background: hover ? T.s2 : 'transparent', transition: 'background .12s' }}
      >
        <td style={tdL}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 11 }}>
            <span style={{ color: open ? MINT : T.t3, fontSize: 11, width: 10, display: 'inline-block', transform: open ? 'rotate(90deg)' : 'none', transition: 'transform .2s' }}>▸</span>
            <div>
              <div style={{ fontWeight: 600, fontSize: 13.5 }}>{s.name}</div>
              <div style={{ marginTop: 3, display: 'flex', alignItems: 'center', gap: 7 }}>
                <span style={{ fontSize: 10.5, letterSpacing: '.04em', color: T.t2, background: T.s3, border: `1px solid ${T.b}`, borderRadius: 5, padding: '2px 7px', fontWeight: 500 }}>{s.segment}</span>
                <span style={{ color: T.t3, fontSize: 11, ...numFont }}>{s.product}</span>
              </div>
            </div>
          </div>
        </td>
        <td style={td}><span style={{ ...numFont, fontSize: 14.5, fontWeight: 600, color: pnlC(s.net) }}>{sgn(s.net)}{INR(s.net)}</span></td>
        <td style={{ ...td, ...numFont, color: T.t2 }}>{sgn(s.gross)}{INR(s.gross)}</td>
        <td style={{ ...td, ...numFont, color: T.a }}>{INR(s.charges)}</td>
        <td style={{ ...td, ...numFont }}>{s.trades}</td>
        <td style={{ ...td, ...numFont, color: wrColor }}>{s.win_rate}%</td>
        <td style={{ ...td, ...numFont, color: pnlC(s.avg) }}>{sgn(s.avg)}{INR(s.avg)}</td>
        <td style={{ ...td, fontSize: 11.5, color: T.t2 }}>
          <span style={{ ...numFont, color: T.g }}>+{INR(Math.abs(s.best.pnl))}</span>
          {' · '}
          <span style={{ ...numFont, color: T.r }}>−{INR(Math.abs(s.worst.pnl))}</span>
          <br />
          <span style={{ color: T.t3, fontSize: 10.5 }}>{s.best.label} / {s.worst.label}</span>
        </td>
      </tr>
      {open && (
        <tr style={{ background: T.bg }}>
          <td colSpan={8} style={{ padding: 0, borderBottom: `1px solid ${T.b}` }}>
            <Drill s={s} />
          </td>
        </tr>
      )}
    </>
  )
}

function Drill({ s }: { s: PnlStrategy }) {
  const gridCols = '1.6fr repeat(5, 1fr)'
  const headCell: React.CSSProperties = { textAlign: 'right' }
  return (
    <div>
      <div style={{ padding: '9px 14px 9px 39px', fontSize: 10, letterSpacing: '.08em', textTransform: 'uppercase', color: T.t3, display: 'grid', gridTemplateColumns: gridCols, gap: 10, background: T.s1 }}>
        <div style={{ textAlign: 'left' }}>{s.sessions[0]?.kind === 'campaign' ? 'Campaign' : 'Session'}</div>
        <div style={headCell}>Net</div><div style={headCell}>Gross</div><div style={headCell}>Charges</div><div style={headCell}>Trades</div><div style={headCell}>Win %</div>
      </div>
      {s.sessions.length === 0
        ? <div style={{ padding: '14px 39px', color: T.t3, fontSize: 12.5 }}>No sessions in this window.</div>
        : s.sessions.map((se, i) => (
          <div key={se.id} style={{ display: 'grid', gridTemplateColumns: gridCols, gap: 10, padding: '11px 14px 11px 39px', borderTop: i === 0 ? 'none' : `1px solid ${T.b}`, fontSize: 12.5, alignItems: 'center' }}>
            <div>
              <div style={{ color: T.t }}>{se.name}</div>
              <div style={{ color: T.t3, fontSize: 11, ...numFont }}>{se.date_label}</div>
            </div>
            <div style={{ textAlign: 'right', ...numFont, color: pnlC(se.net) }}>{sgn(se.net)}{INR(se.net)}</div>
            <div style={{ textAlign: 'right', ...numFont, color: T.t2 }}>{sgn(se.gross)}{INR(se.gross)}</div>
            <div style={{ textAlign: 'right', ...numFont, color: T.a }}>{INR(se.charges)}</div>
            <div style={{ textAlign: 'right', ...numFont }}>{se.trades}</div>
            <div style={{ textAlign: 'right', ...numFont, color: T.t2 }}>{se.win_rate}%</div>
          </div>
        ))}
    </div>
  )
}

function SkeletonRows() {
  return (
    <>
      {Array.from({ length: 5 }).map((_, i) => (
        <tr key={i}>
          <td colSpan={8} style={{ borderBottom: `1px solid ${T.b}`, padding: 0 }}>
            <div style={{ height: 56, margin: 0, ...shimmer }} />
          </td>
        </tr>
      ))}
    </>
  )
}


// ─────────────────────────────────────────────────────────────────────────────
// Footnote + error + paywall
// ─────────────────────────────────────────────────────────────────────────────
function Footnote({ asOf }: { asOf: string | null }) {
  const asOfLabel = asOf
    ? new Date(asOf).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false })
    : null
  return (
    <div style={{ color: T.t3, fontSize: 11.5, marginTop: 16, display: 'flex', gap: 8, alignItems: 'flex-start', lineHeight: 1.5 }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: MINT, marginTop: 6, flex: 'none' }} />
      <div>
        <b style={{ color: T.t2, fontWeight: 600 }}>Net = gross − charges.</b> Gross is exact (real broker fills, attributed
        per session by order-id). Charges &amp; taxes are Zerodha-rate estimates (brokerage, STT, exchange, GST, stamp, DP).
        Manual trades in the same symbol are excluded. Figures are AutoTrade-only — not your account-level broker P&amp;L.
        {asOfLabel && <> · As of <span style={{ ...numFont }}>{asOfLabel} IST</span>.</>}
        {USE_FIXTURE && <> · <span style={{ color: T.a }}>Sample data (NEXT_PUBLIC_PNL_FIXTURE=1).</span></>}
      </div>
    </div>
  )
}

function ErrorCard({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div style={{ background: 'rgba(255,77,109,0.06)', border: '1px solid rgba(255,77,109,0.3)', borderRadius: 14, padding: 24, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
      <div style={{ color: T.t2, fontSize: 13 }}>
        <b style={{ color: T.r }}>Couldn&apos;t load P&amp;L.</b> {message}
      </div>
      <button onClick={onRetry} style={{ fontFamily: 'inherit', fontSize: 12.5, color: T.t, background: T.s2, border: `1px solid ${T.b2}`, borderRadius: 9, padding: '8px 14px', cursor: 'pointer' }}>
        Retry
      </button>
    </div>
  )
}

function PaywallCard({ url }: { url: string | null }) {
  return (
    <div style={{ background: T.s1, border: `1px solid ${MINT_BD}`, borderRadius: 14, padding: 28, textAlign: 'center' }}>
      <div style={{ fontSize: 11, letterSpacing: '.16em', textTransform: 'uppercase', color: MINT, fontWeight: 600, marginBottom: 8 }}>Enterprise</div>
      <h2 style={{ margin: '0 0 8px', fontSize: 20, fontWeight: 640 }}>Strategy P&amp;L is part of AutoTrade</h2>
      <p style={{ color: T.t2, fontSize: 13.5, maxWidth: 440, margin: '0 auto 18px', lineHeight: 1.5 }}>
        The strategy-level P&amp;L report — real net after charges, per strategy and session — unlocks with an AutoTrade plan.
      </p>
      {url && (
        <a href={url} style={{ display: 'inline-block', fontFamily: 'inherit', fontSize: 13, fontWeight: 700, color: '#04140d', background: MINT, borderRadius: 10, padding: '11px 22px', textDecoration: 'none' }}>
          Continue to checkout
        </a>
      )}
    </div>
  )
}


// ── misc ─────────────────────────────────────────────────────────────────────
const shimmer: React.CSSProperties = {
  background: `linear-gradient(90deg, ${T.s1} 0%, ${T.s2} 50%, ${T.s1} 100%)`,
  backgroundSize: '200% 100%',
  animation: 'pnlShimmer 1.3s ease-in-out infinite',
}
function uniq(arr: string[]): string[] {
  return Array.from(new Set(arr))
}
