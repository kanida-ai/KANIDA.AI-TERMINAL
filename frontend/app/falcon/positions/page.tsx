'use client'

// Falcon — Positions tab (wired to live backend)
// Spec: backend/falcon/trade/SPEC/Requirements.md §12 + Design.md §3.6
// Polls /api/falcon/trade/positions every 5s.

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  FalconAPI,
  type BulkAdoptItem,
  type BulkAdoptItemResult,
  type TickerStatus,
  type TradeAdoptPreview,
  type TradeBulkAdoptResult,
  type TradeEvent,
  type TradeMonitorStatus,
  type TradePosition,
  type TradePositionsResponse,
} from '../../../lib/falcon-api'

const inr = (n: number) => '₹' + Math.round(n).toLocaleString('en-IN')
const inrLakh = (n: number) => {
  if (n >= 1e7) return `₹${(n / 1e7).toFixed(2)} Cr`
  if (n >= 1e5) return `₹${(n / 1e5).toFixed(2)} L`
  return inr(n)
}

type ProductFilter = 'all' | 'MTF' | 'CNC' | 'MIXED'
type PnlFilter     = 'all' | 'winners' | 'losers'

export default function FalconPositionsPage() {
  const [data, setData] = useState<TradePositionsResponse | null>(null)
  const [events, setEvents] = useState<TradeEvent[]>([])
  const [monitor, setMonitor] = useState<TradeMonitorStatus | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [exitingSymbol, setExitingSymbol] = useState<string | null>(null)
  const [exitInFlight, setExitInFlight] = useState(false)
  const [exitMessage, setExitMessage] = useState<string | null>(null)
  const [productFilter, setProductFilter] = useState<ProductFilter>('all')
  const [pnlFilter, setPnlFilter] = useState<PnlFilter>('all')
  const [adoptPreview, setAdoptPreview] = useState<TradeAdoptPreview | null>(null)
  const [adoptInFlight, setAdoptInFlight] = useState(false)
  const [adoptMessage, setAdoptMessage] = useState<string | null>(null)
  const [bulkOpen, setBulkOpen] = useState(false)
  const [bulkResult, setBulkResult] = useState<TradeBulkAdoptResult | null>(null)
  const [ticker, setTicker] = useState<TickerStatus | null>(null)

  // Initial fetch + 5s polling for positions, events, monitor status, ticker
  useEffect(() => {
    let cancelled = false
    const tick = () => {
      Promise.all([
        FalconAPI.tradePositions().catch(e => {
          if (!cancelled) setError(e instanceof Error ? e.message : String(e))
          return null
        }),
        FalconAPI.tradeEvents(50).catch(() => ({ events: [] })),
        FalconAPI.tradeMonitor().catch(() => null),
        FalconAPI.tradeTickerStatus().catch(() => null),
      ]).then(([d, ev, mon, tk]) => {
        if (cancelled) return
        if (d) { setData(d); setError(null) }
        setEvents(ev?.events ?? [])
        setMonitor(mon)
        setTicker(tk)
      })
    }
    tick()
    const id = setInterval(tick, 5000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  const onAdoptPreview = async (symbol: string) => {
    setAdoptMessage(null)
    setAdoptInFlight(true)
    try {
      const r = await FalconAPI.tradeAdoptPreview(symbol)
      setAdoptPreview(r)
    } catch (e: unknown) {
      setAdoptMessage(`✗ Adopt preview failed: ${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setAdoptInFlight(false)
    }
  }

  const onAdoptConfirm = async (symbol: string, slPrice: number) => {
    // STAGE to pre-market (instead of firing Kite SL immediately).
    // Deployer at 9:15 IST handles the actual placement.
    setAdoptInFlight(true)
    try {
      const r = await FalconAPI.premarketStageAdopts([{ symbol, sl_price: slPrice }])
      if (r.n_staged > 0) {
        setAdoptMessage(`✓ Staged ${symbol} to Pre-Market for ${r.target_date} (SL ₹${slPrice}). Review & Confirm at /falcon/premarket — deploys at 9:15 IST.`)
      } else if (r.skipped.length > 0) {
        const reason = r.skipped[0]?.reason || 'unknown'
        setAdoptMessage(`✗ Could not stage ${symbol}: ${reason}`)
      }
      setAdoptPreview(null)
      // Refresh positions
      FalconAPI.tradePositions().then(setData).catch(() => {})
    } catch (e: unknown) {
      setAdoptMessage(`✗ Stage failed: ${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setAdoptInFlight(false)
    }
  }

  const onExit = async (symbol: string) => {
    setExitInFlight(true)
    setExitMessage(null)
    try {
      const r = await FalconAPI.tradePositionsExit(symbol)
      setExitMessage(`✓ Exited ${symbol}: ${r.exit_qty} sh @ ~₹${r.exit_price.toFixed(2)} · kite_order_id ${r.exit_kite_order_id}`)
      setExitingSymbol(null)
      // Refresh positions immediately
      FalconAPI.tradePositions().then(setData).catch(() => {})
    } catch (e: unknown) {
      setExitMessage(`✗ Exit failed: ${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setExitInFlight(false)
    }
  }

  if (error) {
    return (
      <div className="space-y-4">
        <header>
          <h1 className="text-2xl font-bold">Positions</h1>
        </header>
        <div className="bg-red-500/5 border border-red-500/30 rounded p-4 text-sm text-red-300">
          ✗ {error}
          <div className="mt-2 text-xs text-red-300/80">
            If token is invalid, refresh at <Link href="/admin" className="underline">/admin</Link>.
          </div>
        </div>
      </div>
    )
  }

  if (!data) {
    return <div className="text-neutral-400 text-sm">Loading positions…</div>
  }

  const positions = data.positions
  const tw = data.trigger_watch

  return (
    <div className="space-y-6">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">Positions</h1>
          <p className="text-sm text-neutral-400">
            Live positions held · SL / Target / Trail levels · what's about to fire
          </p>
        </div>
        <div className="text-xs text-neutral-500">
          updated {new Date(data.as_of).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' })} IST
        </div>
      </header>

      {exitMessage && (
        <div className={[
          'rounded border p-3 text-sm',
          exitMessage.startsWith('✓')
            ? 'bg-green-500/10 border-green-500/30 text-green-200'
            : 'bg-red-500/10 border-red-500/30 text-red-200',
        ].join(' ')}>
          {exitMessage}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-sm bg-neutral-900 border border-neutral-800 rounded p-4">
        <Stat label="Open positions"  value={String(data.n_positions)} accent />
        <Stat label="Notional held"   value={inrLakh(data.total_notional)} accent />
        <Stat label="Avg entry value" value={inrLakh(data.total_entry_value)} />
        <Stat label="Unrealized P&L"  value={inrLakh(data.unrealized_pnl)} accent={data.unrealized_pnl >= 0} err={data.unrealized_pnl < 0} />
        <Stat label="Return %"        value={`${data.pnl_pct >= 0 ? '+' : ''}${data.pnl_pct.toFixed(2)} %`} accent={data.pnl_pct >= 0} err={data.pnl_pct < 0} />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <TriggerCard
          title="Closest to SL"
          accent="red"
          rows={tw.near_sl.length === 0
            ? [{ symbol: '', line: 'No SL near firing' }]
            : tw.near_sl.map(r => ({ symbol: r.symbol, line: `${r.distance_pct.toFixed(2)}% away` }))}
        />
        <TriggerCard
          title="Closest to Target"
          accent="green"
          rows={tw.near_target.length === 0
            ? [{ symbol: '', line: 'No target near firing' }]
            : tw.near_target.map(r => ({ symbol: r.symbol, line: `${r.distance_pct.toFixed(2)}% away` }))}
        />
        <TriggerCard
          title="Closest to time-stop"
          accent="amber"
          rows={tw.near_time_stop.length === 0
            ? [{ symbol: '', line: 'No positions near time-stop' }]
            : tw.near_time_stop.map(r => ({ symbol: r.symbol, line: `held ${r.days_held} days` }))}
        />
      </div>

      {monitor && (
        <MonitorPanel monitor={monitor} events={events} ticker={ticker} />
      )}

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
        <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
          <h2 className="text-sm font-semibold">All open positions</h2>
          <div className="flex items-center gap-3 flex-wrap">
            <TradebookUpload onDone={() => FalconAPI.tradePositions().then(setData).catch(() => {})} />
            <button
              onClick={() => setBulkOpen(true)}
              className="px-3 py-1 text-xs bg-amber-500/20 text-amber-300 border border-amber-500/40 rounded font-medium hover:bg-amber-500/30"
              title="Stage external positions to Pre-Market — Falcon SLs fire at 9:15 IST after you confirm"
            >
              Stage Bulk Adopt
            </button>
            <ProductFilterBar
              current={productFilter}
              onChange={setProductFilter}
              counts={countByProduct(positions)}
            />
            <PnlFilterBar
              current={pnlFilter}
              onChange={setPnlFilter}
              counts={countByPnl(positions)}
            />
          </div>
        </div>
        {(() => {
          const filtered = filterPositions(positions, productFilter, pnlFilter)
          return positions.length === 0 ? (
          <div className="text-neutral-400 text-sm py-4">
            No open positions tracked by Falcon. Place a batch via <Link href="/falcon/trade" className="text-amber-400 underline">/falcon/trade</Link>.
          </div>
        ) : (
        <div className="overflow-x-auto -mx-4 px-4">
          <table className="w-full text-xs">
            <thead className="text-neutral-400">
              <tr>
                <th className="text-left py-2">Symbol</th>
                <th className="text-right">Qty</th>
                <th className="text-right">Avg entry</th>
                <th className="text-right">Current</th>
                <th className="text-right">P&L %</th>
                <th className="text-right">SL ₹</th>
                <th className="text-right">Δ to SL</th>
                <th className="text-right">Target ₹</th>
                <th className="text-right">Δ to Target</th>
                <th className="text-center">Trail</th>
                <th className="text-center">Entry date</th>
                <th className="text-center">Days</th>
                <th className="text-center">SL type</th>
                <th className="text-center">Action</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 ? (
                <tr><td colSpan={14} className="text-center text-neutral-500 py-6 text-sm">
                  No positions matching filter <code>{productFilter}</code>.
                </td></tr>
              ) : filtered.map(p => (
                <PositionRow key={p.symbol} p={p}
                  onExit={() => setExitingSymbol(p.symbol)}
                  onAdopt={() => onAdoptPreview(p.symbol)} />
              ))}
            </tbody>
          </table>
        </div>
        )
        })()}
        <div className="mt-3 text-xs text-neutral-500">
          Δ columns turn yellow at &lt;5%, red at &lt;3% (close to firing).
          Days column turns yellow at ≤3 days remaining, red at ≤1.
          Auto-refresh every 5s.
        </div>
      </section>

      {exitingSymbol && (
        <ExitConfirmModal
          symbol={exitingSymbol}
          position={positions.find(p => p.symbol === exitingSymbol)!}
          inFlight={exitInFlight}
          onClose={() => setExitingSymbol(null)}
          onConfirm={() => onExit(exitingSymbol)}
        />
      )}

      {adoptMessage && !adoptPreview && (
        <div className={[
          'fixed bottom-4 right-4 max-w-md rounded border p-3 text-sm shadow-lg z-50',
          adoptMessage.startsWith('✓')
            ? 'bg-green-500/10 border-green-500/30 text-green-200'
            : 'bg-red-500/10 border-red-500/30 text-red-200',
        ].join(' ')}>
          <div className="flex items-start justify-between gap-3">
            <span>{adoptMessage}</span>
            <button onClick={() => setAdoptMessage(null)} className="text-neutral-500 hover:text-neutral-200">×</button>
          </div>
        </div>
      )}

      {adoptPreview && (
        <AdoptConfirmModal
          preview={adoptPreview}
          inFlight={adoptInFlight}
          onClose={() => setAdoptPreview(null)}
          onConfirm={(slPrice) => onAdoptConfirm(adoptPreview.symbol, slPrice)}
        />
      )}

      {bulkOpen && (
        <BulkAdoptModal
          positions={positions}
          result={bulkResult}
          onClose={() => { setBulkOpen(false); setBulkResult(null) }}
          onSubmit={async (items, confirm) => {
            if (!confirm) {
              // Preview path: still uses bulk-adopt endpoint to compute SL/circuit
              const r = await FalconAPI.tradeBulkAdoptPreview(items)
              setBulkResult(r)
              return r
            }
            // Confirm path: STAGE to pre-market instead of firing Kite SLs.
            // Map premarket-stage-adopts result → BulkAdoptResult shape so the
            // modal's success view keeps working unchanged.
            const stageR = await FalconAPI.premarketStageAdopts(items)
            const placed: BulkAdoptItemResult[] = stageR.staged.map(s => ({
              symbol:        s.symbol,
              qty:           s.qty,
              kite_order_id: undefined,
              sl_price:      s.sl_price,
              status:        'ADOPTED' as const,
              product:       s.product,
            }))
            const failed: BulkAdoptItemResult[] = stageR.skipped.map(s => ({
              symbol: s.symbol,
              error:  s.reason,
            }))
            const r: TradeBulkAdoptResult = {
              preview:     false,
              batch_id:    null,
              n_requested: stageR.n_requested,
              previews:    [],
              placed,
              failed,
            }
            setBulkResult(r)
            FalconAPI.tradePositions().then(setData).catch(() => {})
            return r
          }}
        />
      )}
    </div>
  )
}

function countByProduct(positions: TradePosition[]) {
  const out = { all: positions.length, MTF: 0, CNC: 0, MIXED: 0 }
  for (const p of positions) {
    if (p.product === 'MTF') out.MTF++
    else if (p.product === 'CNC') out.CNC++
    else if (p.product === 'MIXED') out.MIXED++
  }
  return out
}

function filterByProduct(positions: TradePosition[], f: ProductFilter): TradePosition[] {
  if (f === 'all') return positions
  return positions.filter(p => p.product === f)
}

function countByPnl(positions: TradePosition[]) {
  return {
    all:     positions.length,
    winners: positions.filter(p => p.pnl_pct > 0).length,
    losers:  positions.filter(p => p.pnl_pct < 0).length,
  }
}

function filterPositions(positions: TradePosition[], product: ProductFilter, pnl: PnlFilter): TradePosition[] {
  let out = product === 'all' ? positions : positions.filter(p => p.product === product)
  if (pnl === 'winners') out = out.filter(p => p.pnl_pct > 0)
  else if (pnl === 'losers') out = out.filter(p => p.pnl_pct < 0)
  return out
}

function PnlFilterBar({ current, onChange, counts }: {
  current: PnlFilter
  onChange: (f: PnlFilter) => void
  counts: { all: number; winners: number; losers: number }
}) {
  const opts: { key: PnlFilter; label: string; count: number; tone: 'neutral' | 'green' | 'red' }[] = [
    { key: 'all',     label: 'P&L any', count: counts.all,     tone: 'neutral' },
    { key: 'winners', label: 'Winners', count: counts.winners, tone: 'green' },
    { key: 'losers',  label: 'Losers',  count: counts.losers,  tone: 'red' },
  ]
  const toneCls = (tone: 'neutral' | 'green' | 'red', selected: boolean): string => {
    if (selected) {
      if (tone === 'green') return 'bg-green-500/20 text-green-300 border-green-500/40'
      if (tone === 'red')   return 'bg-red-500/20 text-red-300 border-red-500/40'
      return 'bg-amber-500/20 text-amber-300 border-amber-500/40'
    }
    return 'bg-neutral-900 text-neutral-400 border-neutral-800 hover:text-neutral-100'
  }
  return (
    <div className="flex items-center gap-1 text-xs">
      {opts.map(o => (
        <button key={o.key} type="button" onClick={() => onChange(o.key)}
          className={'px-2.5 py-1 rounded border transition ' + toneCls(o.tone, current === o.key)}
        >
          {o.label} <span className="text-[10px] text-neutral-500 ml-1">({o.count})</span>
        </button>
      ))}
    </div>
  )
}

function ProductFilterBar({ current, onChange, counts }: {
  current: ProductFilter
  onChange: (f: ProductFilter) => void
  counts: { all: number; MTF: number; CNC: number; MIXED: number }
}) {
  const opts: { key: ProductFilter; label: string; count: number }[] = [
    { key: 'all', label: 'All',   count: counts.all },
    { key: 'MTF', label: 'MTF',   count: counts.MTF },
    { key: 'CNC', label: 'CNC',   count: counts.CNC },
  ]
  if (counts.MIXED > 0) opts.push({ key: 'MIXED', label: 'Mixed', count: counts.MIXED })
  return (
    <div className="flex items-center gap-1 text-xs">
      {opts.map(o => (
        <button
          key={o.key}
          type="button"
          onClick={() => onChange(o.key)}
          className={[
            'px-2.5 py-1 rounded border transition',
            current === o.key
              ? 'bg-amber-500/20 text-amber-300 border-amber-500/40'
              : 'bg-neutral-900 text-neutral-400 border-neutral-800 hover:text-neutral-100',
          ].join(' ')}
        >
          {o.label} <span className="text-[10px] text-neutral-500 ml-1">({o.count})</span>
        </button>
      ))}
    </div>
  )
}

function PositionRow({ p, onExit, onAdopt }: { p: TradePosition; onExit: () => void; onAdopt: () => void }) {
  const slDistCls = p.sl_distance_pct < 3 ? 'text-red-400'
                   : p.sl_distance_pct < 5 ? 'text-yellow-400'
                   : 'text-neutral-500'
  const tgtDistCls = p.target_distance_pct < 3 ? 'text-amber-400' : 'text-neutral-500'
  const knownDays = p.days_held !== null
  const daysLeft = knownDays ? p.hold_days_max - (p.days_held ?? 0) : null
  const daysCls = daysLeft === null ? 'text-neutral-600'
                 : daysLeft <= 1 ? 'text-red-400'
                 : daysLeft <= 3 ? 'text-yellow-400'
                 : 'text-neutral-300'
  const isExternal = p.managed_by === 'external'
  return (
    <tr className={'border-t border-neutral-800' + (isExternal ? ' bg-yellow-500/[0.03]' : '')}>
      <td className="py-1.5 font-medium">
        {p.symbol}
        {isExternal && (
          <span title="Position not placed via Falcon — no live SL order on Kite. SL/Target shown are advisory only."
                className="ml-1 inline-block bg-yellow-500/15 text-yellow-300 text-[10px] px-1 rounded">
            ext
          </span>
        )}
      </td>
      <td className="text-right font-mono">{p.qty.toLocaleString('en-IN')}</td>
      <td className="text-right font-mono">{inr(p.avg_entry)}</td>
      <td className="text-right font-mono">{inr(p.current_price)}</td>
      <td className={'text-right font-mono ' + (p.pnl_pct >= 0 ? 'text-green-400' : 'text-red-400')}>
        {p.pnl_pct >= 0 ? '+' : ''}{p.pnl_pct.toFixed(2)} %
      </td>
      <td className="text-right font-mono text-red-300">{p.sl_price.toFixed(2)}</td>
      <td className={'text-right font-mono ' + slDistCls}>{p.sl_distance_pct.toFixed(2)} %</td>
      <td className="text-right font-mono text-green-300">{p.target_price.toFixed(2)}</td>
      <td className={'text-right font-mono ' + tgtDistCls}>{p.target_distance_pct.toFixed(2)} %</td>
      <td className="text-center">
        {p.trail_active
          ? <span className="text-green-400 text-[10px]">trailing @ {p.trail_low_10d ? inr(p.trail_low_10d) : '—'}</span>
          : <span className="text-neutral-600 text-[10px]">not yet</span>}
      </td>
      <td className="text-center text-[10px] font-mono">
        {p.entry_date
          ? <span className="text-neutral-300">{p.entry_date}</span>
          : <span className="text-neutral-600" title="No trade log on file. Upload Zerodha tradebook CSV to set this.">unknown</span>}
      </td>
      <td className="text-center">
        <span className={daysCls}>
          {knownDays ? `${p.days_held} / ${p.hold_days_max}` : '—'}
        </span>
      </td>
      <td className="text-center font-mono text-neutral-400">{p.sl_type}</td>
      <td className="text-center text-[11px]">
        <button onClick={onExit} className="text-red-400 hover:text-red-200 underline mr-2">
          Exit now
        </button>
        {p.managed_by === 'external' && (
          <button onClick={onAdopt}
                  className="text-amber-400 hover:text-amber-200 underline"
                  title="Stage this position to Pre-Market. Falcon SL fires at 9:15 IST after you Confirm.">
            Stage
          </button>
        )}
      </td>
    </tr>
  )
}

function Stat({ label, value, accent, err }: { label: string; value: string; accent?: boolean; err?: boolean }) {
  const cls = err ? 'text-red-400' : accent ? 'text-amber-300' : 'text-neutral-100'
  return (
    <div>
      <div className="text-xs text-neutral-500">{label}</div>
      <div className={'text-base font-semibold ' + cls}>{value}</div>
    </div>
  )
}

function MonitorPanel({ monitor, events, ticker }: {
  monitor: TradeMonitorStatus
  events: TradeEvent[]
  ticker: TickerStatus | null
}) {
  const sevCls = (s: TradeEvent['severity']) =>
    s === 'critical' ? 'bg-red-500/15 text-red-300 border-red-500/40'
  : s === 'warn'     ? 'bg-yellow-500/15 text-yellow-300 border-yellow-500/40'
                     : 'bg-blue-500/15 text-blue-300 border-blue-500/40'
  // Ticker badge — green when connected with recent ticks, amber when connected
  // but stale (>30s since last tick during market hours), gray when offline.
  const tickerCls = !ticker
    ? 'bg-neutral-800 text-neutral-400 border-neutral-700'
    : ticker.connected
        ? (ticker.last_tick_at && (Date.now() - new Date(ticker.last_tick_at).getTime() < 30_000)
            ? 'bg-green-500/15 text-green-300 border-green-500/40'
            : 'bg-amber-500/15 text-amber-300 border-amber-500/40')
        : 'bg-red-500/10 text-red-300 border-red-500/40'
  const tickerLabel = !ticker
    ? 'ticker: …'
    : ticker.connected
        ? `ticker: live · ${ticker.subscribed_count} subs · ${ticker.tick_count.toLocaleString('en-IN')} ticks`
        : `ticker: offline${ticker.last_error ? ' (' + ticker.last_error + ')' : ''}`
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <div className="flex items-baseline justify-between mb-3 flex-wrap gap-y-2">
        <h2 className="text-sm font-semibold flex items-center gap-2">
          <span className="inline-block w-2 h-2 rounded-full bg-green-400 animate-pulse" />
          Live Monitor
          <span className="text-neutral-500 font-normal text-xs">
            tracking {monitor.n_tracked} · poll {monitor.interval_sec}s
          </span>
        </h2>
        <div className="flex items-center gap-2">
          <span className={'text-xs px-2 py-0.5 rounded border font-mono ' + tickerCls}>
            {tickerLabel}
          </span>
          <span className={'text-xs px-2 py-0.5 rounded border ' + (
            monitor.auto_exit_enabled
              ? 'bg-amber-500/15 text-amber-300 border-amber-500/40'
              : 'bg-neutral-800 text-neutral-400 border-neutral-700'
          )}>
            auto-exit: {monitor.auto_exit_enabled ? 'ON' : 'OFF (manual only)'}
          </span>
        </div>
      </div>
      {events.length === 0 ? (
        <div className="text-xs text-neutral-500 py-2">
          No trigger events yet. Monitor logs SL/target/HW/time-stop signals as they fire.
        </div>
      ) : (
        <ul className="space-y-1 max-h-56 overflow-y-auto">
          {events.slice(0, 30).map(e => (
            <li key={e.id} className="text-xs flex items-center gap-2 font-mono">
              <span className="text-neutral-500 min-w-[7em]">
                {new Date(e.detected_at).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' })}
              </span>
              <span className="text-neutral-100 font-semibold min-w-[7em]">{e.symbol}</span>
              <span className={'inline-block px-2 py-0.5 rounded border text-[10px] ' + sevCls(e.severity)}>
                {e.kind}
              </span>
              <span className="text-neutral-400 truncate">{e.detail}</span>
              {!!e.auto_action_taken && (
                <span className="text-amber-400 text-[10px]">· auto-acted</span>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

function TriggerCard({ title, accent, rows }: { title: string; accent: 'red' | 'green' | 'amber'; rows: { symbol: string; line: string }[] }) {
  const colorMap = {
    red:   'border-red-500/30 bg-red-500/5',
    green: 'border-green-500/30 bg-green-500/5',
    amber: 'border-amber-500/30 bg-amber-500/5',
  }
  const titleColorMap = {
    red:   'text-red-300',
    green: 'text-green-300',
    amber: 'text-amber-300',
  }
  return (
    <div className={'rounded border p-3 ' + colorMap[accent]}>
      <h3 className={'text-xs font-semibold mb-2 ' + titleColorMap[accent]}>{title}</h3>
      <ul className="space-y-1 text-xs">
        {rows.map((r, i) => (
          <li key={i} className="font-mono">
            {r.symbol && <span className="text-neutral-100 font-semibold">{r.symbol}</span>}
            {r.symbol && ' · '}
            <span className="text-neutral-400">{r.line}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function BulkAdoptModal({ positions, result, onClose, onSubmit }: {
  positions: TradePosition[]
  result: TradeBulkAdoptResult | null
  onClose: () => void
  onSubmit: (items: BulkAdoptItem[], confirm: boolean) => Promise<TradeBulkAdoptResult>
}) {
  type Filter = 'all' | 'winners' | 'losers' | 'MTF' | 'CNC'
  const [filter, setFilter] = useState<Filter>('MTF')
  // Stageable = managed_by !== 'falcon' (already-managed positions can't be re-staged
  // because they have live SLs at Kite and a Falcon position_state row).
  const isStageable = (p: TradePosition) => p.managed_by !== 'falcon'
  // initial selection: every STAGEABLE MTF winner
  const initialSelected = new Set<string>(
    positions.filter(p => isStageable(p) && p.product === 'MTF' && p.pnl_pct > 0).map(p => p.symbol)
  )
  const [selected, setSelected] = useState<Set<string>>(initialSelected)
  // user-edited SL per symbol (defaults to entry × 0.93 from backend, kept blank → backend computes)
  const [slEdits, setSlEdits] = useState<Record<string, string>>({})
  const [inFlight, setInFlight] = useState(false)

  const filtered = positions
    .filter(p => {
      if (filter === 'MTF')     return p.product === 'MTF'
      if (filter === 'CNC')     return p.product === 'CNC'
      if (filter === 'winners') return p.pnl_pct > 0
      if (filter === 'losers')  return p.pnl_pct < 0
      return true
    })
    .sort((a, b) => {
      // Stageable items first, then already-adopted at the bottom (sorted by P&L within each group)
      const sa = isStageable(a) ? 0 : 1
      const sb = isStageable(b) ? 0 : 1
      if (sa !== sb) return sa - sb
      return b.pnl_pct - a.pnl_pct
    })

  // Counts for the header and select-all behavior
  const filteredStageable = filtered.filter(isStageable)
  const filteredAlreadyAdopted = filtered.filter(p => !isStageable(p))

  const toggleSelected = (sym: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(sym)) next.delete(sym); else next.add(sym)
      return next
    })
  }

  // Select-all only picks up STAGEABLE rows (already-adopted ones are disabled)
  const selectAllFiltered = () => setSelected(new Set(filteredStageable.map(p => p.symbol)))
  const deselectAll       = () => setSelected(new Set())

  const buildItems = (): BulkAdoptItem[] =>
    Array.from(selected).map(sym => {
      const slStr = slEdits[sym]
      const slNum = slStr ? Number(slStr) : NaN
      return slNum > 0 ? { symbol: sym, sl_price: slNum } : { symbol: sym }
    })

  const onPreview = async () => {
    setInFlight(true)
    try { await onSubmit(buildItems(), false) } finally { setInFlight(false) }
  }
  const onConfirm = async () => {
    setInFlight(true)
    try { await onSubmit(buildItems(), true) } finally { setInFlight(false) }
  }

  // After confirm, show results screen
  if (result && !result.preview) {
    // Split placed[] into freshly-placed (status=ADOPTED) vs no-op-adopted
    // (ALREADY_ADOPTED or ADOPTED_EXISTING_KITE_SL — Falcon now tracks them
    // but no new Kite order was placed).
    const freshlyPlaced = result.placed.filter(p => p.status === 'ADOPTED')
    const alreadyLive   = result.placed.filter(p => p.status === 'ALREADY_ADOPTED' || p.status === 'ADOPTED_EXISTING_KITE_SL')
    return (
      <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
        <div className="bg-neutral-900 border border-amber-500/40 rounded p-6 max-w-3xl w-full max-h-[85vh] overflow-y-auto">
          <h3 className="text-lg font-bold text-amber-300 mb-3">
            Bulk Adopt — Staged for Pre-Market ({freshlyPlaced.length} staged · {result.failed.length} failed)
          </h3>
          <p className="text-xs text-amber-300/80 mb-3">
            Items have been staged to Pre-Market.{' '}
            <a href="/falcon/premarket" className="underline">Review & Confirm</a> — deploys at 9:15 IST.
          </p>
          {freshlyPlaced.length > 0 && (
            <>
              <h4 className="text-sm font-semibold text-green-400 mt-4 mb-2">✓ Staged ({freshlyPlaced.length})</h4>
              <table className="w-full text-xs mb-4">
                <thead className="text-neutral-400">
                  <tr>
                    <th className="text-left py-1">Symbol</th>
                    <th className="text-right">Qty</th>
                    <th className="text-right">SL ₹</th>
                    <th className="text-right">Δ to SL</th>
                    <th className="text-center">Trail will activate</th>
                    <th className="text-left">Kite Order ID</th>
                  </tr>
                </thead>
                <tbody>
                  {freshlyPlaced.map(p => (
                    <tr key={p.symbol} className="border-t border-neutral-800">
                      <td className="py-1 font-medium">{p.symbol}</td>
                      <td className="text-right font-mono">{p.qty ?? '—'}</td>
                      <td className="text-right font-mono text-red-300">{p.sl_price != null ? `₹${p.sl_price.toFixed(2)}` : '—'}</td>
                      <td className="text-right font-mono">{p.distance_to_sl_pct != null ? `${p.distance_to_sl_pct.toFixed(2)}%` : '—'}</td>
                      <td className="text-center">
                        {p.will_trail_activate
                          ? <span className="text-green-400 text-[10px]">YES — within 60s</span>
                          : <span className="text-neutral-600 text-[10px]">no (below activate)</span>}
                      </td>
                      <td className="text-left font-mono text-neutral-500">{p.kite_order_id ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
          {alreadyLive.length > 0 && (
            <>
              <h4 className="text-sm font-semibold text-blue-400 mt-4 mb-2">
                ✓ Already adopted ({alreadyLive.length}) — existing Kite SL kept, no duplicate order placed
              </h4>
              <ul className="grid grid-cols-2 md:grid-cols-3 gap-x-4 gap-y-1 text-xs mb-4">
                {alreadyLive.map(p => (
                  <li key={p.symbol} className="font-mono flex justify-between border-b border-neutral-900 py-1">
                    <span className="text-neutral-100 font-semibold">{p.symbol}</span>
                    <span className="text-neutral-500">{p.kite_order_id ?? '—'}</span>
                  </li>
                ))}
              </ul>
            </>
          )}
          {result.failed.length > 0 && (
            <>
              <h4 className="text-sm font-semibold text-red-400 mt-4 mb-2">✗ Failed ({result.failed.length})</h4>
              <ul className="space-y-1 text-xs">
                {result.failed.map(f => (
                  <li key={f.symbol} className="font-mono">
                    <span className="text-neutral-100 font-semibold">{f.symbol}</span>
                    <span className="text-red-300 ml-2">{f.error}</span>
                  </li>
                ))}
              </ul>
            </>
          )}
          <div className="flex justify-end mt-6">
            <button onClick={onClose} className="px-4 py-2 bg-neutral-800 text-neutral-100 rounded">Close</button>
          </div>
        </div>
      </div>
    )
  }

  // Otherwise: selection screen (with optional preview overlay)
  const previewBySym: Record<string, BulkAdoptItemResult> = {}
  if (result?.preview) {
    for (const p of result.previews) previewBySym[p.symbol] = p
  }

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
      <div className="bg-neutral-900 border border-amber-500/40 rounded p-6 max-w-5xl w-full max-h-[90vh] flex flex-col">
        <div className="flex items-baseline justify-between mb-3">
          <h3 className="text-lg font-bold text-amber-300">Stage Bulk Adopt to Pre-Market</h3>
          <span className="text-xs text-neutral-500">
            Select rows to stage. Default SL = entry × (1 + initial_sl_pct/100). Already-adopted positions are shown but disabled.
          </span>
        </div>

        <div className="flex items-center gap-2 mb-3 text-xs flex-wrap">
          {(['all', 'MTF', 'CNC', 'winners', 'losers'] as Filter[]).map(f => (
            <button key={f} onClick={() => setFilter(f)}
              className={[
                'px-2.5 py-1 rounded border',
                filter === f
                  ? 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                  : 'bg-neutral-900 text-neutral-400 border-neutral-800 hover:text-neutral-100',
              ].join(' ')}>
              {f}
            </button>
          ))}
          <span className="text-neutral-500 mx-2">|</span>
          <button onClick={selectAllFiltered} className="text-amber-400 hover:text-amber-200 underline">select all (filtered)</button>
          <button onClick={deselectAll}       className="text-neutral-400 hover:text-neutral-100 underline">deselect all</button>
          <span className="text-neutral-500 ml-auto">
            <strong className="text-amber-300">{selected.size}</strong> selected
            {' · '}{filteredStageable.length} stageable
            {filteredAlreadyAdopted.length > 0 && (
              <> {' · '}<span className="text-green-400">{filteredAlreadyAdopted.length} already adopted</span></>
            )}
          </span>
        </div>

        <div className="flex-1 overflow-y-auto -mx-2 px-2">
          <table className="w-full text-xs">
            <thead className="text-neutral-400 sticky top-0 bg-neutral-900">
              <tr>
                <th className="text-center w-8"></th>
                <th className="text-left py-2">Symbol</th>
                <th className="text-center w-12">Prod</th>
                <th className="text-right">Qty</th>
                <th className="text-right">Entry</th>
                <th className="text-right">Current</th>
                <th className="text-right">P&L %</th>
                <th className="text-right">SL ₹ (editable)</th>
                <th className="text-right">Δ</th>
                <th className="text-center">Trail will fire</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(p => {
                const stageable = isStageable(p)
                const isSel = selected.has(p.symbol)
                const slStr = slEdits[p.symbol] ?? ''
                const defaultSl = (p.avg_entry * 0.93)  // matches backend default
                const slNum = slStr ? Number(slStr) : defaultSl
                const dist = p.current_price ? ((p.current_price - slNum) / p.current_price) * 100 : 0
                const willTrigger = p.current_price <= slNum
                // activate threshold = entry × 1.12 (MTF default) or 1.10 (CNC default)
                const activateMul = p.product === 'MTF' ? 1.12 : 1.10
                const willTrail = p.current_price >= p.avg_entry * activateMul
                return (
                  <tr key={p.symbol}
                      className={['border-t border-neutral-800',
                                  !stageable ? 'opacity-50' : '',
                                  isSel ? 'bg-amber-500/5' : '',
                                  willTrigger && isSel ? 'bg-red-500/10' : ''].join(' ')}>
                    <td className="text-center">
                      <input type="checkbox" checked={isSel}
                        disabled={!stageable}
                        onChange={() => stageable && toggleSelected(p.symbol)}
                        className="accent-amber-500 w-4 h-4 cursor-pointer disabled:cursor-not-allowed" />
                    </td>
                    <td className="py-1 font-medium">
                      {p.symbol}
                      {!stageable && (
                        <span className="ml-2 text-[10px] text-green-400 font-normal" title="This position already has a live Falcon-managed SL on Kite. Use Trail Config or the Exit button to manage it.">
                          ✓ already adopted
                        </span>
                      )}
                    </td>
                    <td className="text-center text-[10px] text-neutral-400">{p.product}</td>
                    <td className="text-right font-mono">{p.qty.toLocaleString('en-IN')}</td>
                    <td className="text-right font-mono">₹{p.avg_entry.toFixed(2)}</td>
                    <td className="text-right font-mono">₹{p.current_price.toFixed(2)}</td>
                    <td className={'text-right font-mono ' + (p.pnl_pct >= 0 ? 'text-green-400' : 'text-red-400')}>
                      {p.pnl_pct >= 0 ? '+' : ''}{p.pnl_pct.toFixed(2)}%
                    </td>
                    <td className="text-right">
                      <input type="number" step={0.05}
                        value={slStr}
                        placeholder={defaultSl.toFixed(2)}
                        onChange={e => setSlEdits(prev => ({ ...prev, [p.symbol]: e.target.value }))}
                        className="w-24 bg-neutral-950 border border-neutral-800 rounded px-1 py-0.5 text-right font-mono text-red-300 disabled:opacity-30"
                        disabled={!stageable || !isSel} />
                    </td>
                    <td className={'text-right font-mono ' + (
                      !stageable ? 'text-neutral-700' :
                      willTrigger ? 'text-red-400' : dist < 3 ? 'text-yellow-400' : 'text-neutral-500'
                    )}>
                      {stageable ? `${dist.toFixed(2)}%` : '—'}
                    </td>
                    <td className="text-center text-[10px]">
                      {!stageable
                        ? <span className="text-neutral-700">—</span>
                        : willTrail
                          ? <span className="text-green-400">YES — instant</span>
                          : <span className="text-neutral-600">—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {result?.preview && (
          <div className="mt-3 bg-amber-500/10 border border-amber-500/30 rounded p-3 text-xs text-amber-200">
            Preview computed for {result.previews.length} positions. {' '}
            {result.previews.filter(p => p.will_trigger_immediately).length} would fire immediately.
            {' '}{result.previews.filter(p => p.will_trail_activate).length} would trail-activate within 60s.
            Click <strong>Stage {selected.size}</strong> to send these to Pre-Market — they'll be reviewed there and deploy at 9:15 IST.
          </div>
        )}

        <div className="flex justify-end gap-2 mt-4 pt-4 border-t border-neutral-800">
          <button onClick={onClose} disabled={inFlight} className="px-4 py-2 text-neutral-400 hover:text-neutral-100">Cancel</button>
          <button onClick={onPreview} disabled={inFlight || selected.size === 0}
                  className="px-4 py-2 bg-neutral-800 text-neutral-100 rounded text-sm disabled:opacity-50">
            {inFlight ? 'Computing…' : 'Preview SLs'}
          </button>
          <button onClick={onConfirm} disabled={inFlight || selected.size === 0}
                  className={inFlight || selected.size === 0
                    ? 'px-4 py-2 bg-neutral-800 text-neutral-500 rounded text-sm cursor-not-allowed'
                    : 'px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold text-sm'}>
            {inFlight ? 'Staging…' : `Stage ${selected.size} to Pre-Market`}
          </button>
        </div>
      </div>
    </div>
  )
}

function AdoptConfirmModal({ preview, inFlight, onClose, onConfirm }: {
  preview: TradeAdoptPreview
  inFlight: boolean
  onClose: () => void
  onConfirm: (slPrice: number) => void
}) {
  const [slEdit, setSlEdit] = useState<string>(String(preview.computed_sl_price))
  const slNum = Number(slEdit) || preview.computed_sl_price
  const willTrigger = preview.current_price <= slNum
  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
      <div className="bg-neutral-900 border border-amber-500/40 rounded p-6 max-w-md w-full">
        <h3 className="text-lg font-bold text-amber-300 mb-3">Stage {preview.symbol} to Pre-Market</h3>
        <div className="text-sm text-neutral-300 space-y-1 mb-4">
          <div>Position: <span className="font-mono text-neutral-100">{preview.qty} sh @ {inr(preview.avg_entry)}</span> ({preview.product})</div>
          <div>Current: <span className="font-mono text-neutral-100">{inr(preview.current_price)}</span></div>
        </div>
        <label className="block mb-3">
          <span className="block text-xs text-neutral-400 mb-1">SL trigger price (₹) — default = entry × (1 + initial_sl_pct/100)</span>
          <input type="number" step={0.05}
            value={slEdit} onChange={e => setSlEdit(e.target.value)}
            className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2 text-neutral-100 font-mono" />
          <div className="text-xs text-neutral-500 mt-1">
            Distance from current: {(((preview.current_price - slNum) / preview.current_price) * 100).toFixed(2)}%
          </div>
        </label>
        {willTrigger && (
          <div className="bg-red-500/10 border border-red-500/30 rounded p-2 text-xs text-red-300 mb-3">
            ⚠ Current price (₹{preview.current_price.toFixed(2)}) is at/below this SL. When deployed at 9:15, it will fire BREACHED_SL on the next monitor poll → auto-exit at market.
          </div>
        )}
        <p className="text-xs text-amber-300/80 mb-4">
          This stages the adopt to Pre-Market. Review at <a href="/falcon/premarket" className="underline">/falcon/premarket</a>, click Confirm, and the deployer fires it at 9:15 IST.
        </p>
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} disabled={inFlight} className="px-4 py-2 text-neutral-400 hover:text-neutral-100">Cancel</button>
          <button onClick={() => onConfirm(slNum)} disabled={inFlight}
                  className={inFlight
                    ? 'px-4 py-2 bg-neutral-800 text-neutral-500 rounded cursor-not-allowed'
                    : 'px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold'}>
            {inFlight ? 'Staging…' : 'Stage to Pre-Market'}
          </button>
        </div>
      </div>
    </div>
  )
}

function ExitConfirmModal({ symbol, position, inFlight, onClose, onConfirm }: {
  symbol: string; position: TradePosition; inFlight: boolean;
  onClose: () => void; onConfirm: () => void
}) {
  const realisedPnL = (position.current_price - position.avg_entry) * position.qty
  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
      <div className="bg-neutral-900 border border-red-500/40 rounded p-6 max-w-md w-full">
        <h3 className="text-lg font-bold text-red-300 mb-3">Manual exit: {symbol}</h3>
        <div className="text-sm text-neutral-300 mb-4 space-y-1">
          <div>Position: <span className="font-mono text-neutral-100">{position.qty} sh @ {inr(position.avg_entry)}</span></div>
          <div>Current:  <span className="font-mono text-neutral-100">{inr(position.current_price)}</span></div>
          <div>P&L:      <span className={'font-mono ' + (position.pnl_pct >= 0 ? 'text-green-400' : 'text-red-400')}>
            {position.pnl_pct >= 0 ? '+' : ''}{position.pnl_pct.toFixed(2)}% · {inr(realisedPnL)}
          </span></div>
        </div>
        <p className="text-sm text-amber-300 mb-4">
          This cancels the existing SL order and places a market SELL.
          Slippage may differ from current quote.
        </p>
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} disabled={inFlight} className="px-4 py-2 text-neutral-400 hover:text-neutral-100">Cancel</button>
          <button onClick={onConfirm} disabled={inFlight}
                  className={inFlight
                    ? 'px-4 py-2 bg-neutral-800 text-neutral-500 rounded cursor-not-allowed'
                    : 'px-4 py-2 bg-red-500 text-neutral-50 rounded font-semibold'}>
            {inFlight ? 'Exiting…' : 'Exit now'}
          </button>
        </div>
      </div>
    </div>
  )
}

function TradebookUpload({ onDone }: { onDone: () => void }) {
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)

  const onPick = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setBusy(true)
    setMsg(null)
    try {
      const r = await FalconAPI.tradeTradebookUpload(file)
      setMsg(`✓ ${r.symbols_dated} dated · ${r.inserted} new · ${r.updated} refreshed`)
      onDone()
    } catch (err) {
      setMsg('✗ ' + (err instanceof Error ? err.message : String(err)))
    } finally {
      setBusy(false)
      // Reset input so the same file can be re-picked
      e.target.value = ''
      // Auto-clear success message after 6 seconds
      setTimeout(() => setMsg(null), 6000)
    }
  }

  return (
    <label
      className={busy
        ? 'px-3 py-1 text-xs bg-neutral-800 text-neutral-500 border border-neutral-700 rounded font-medium cursor-not-allowed'
        : 'px-3 py-1 text-xs bg-neutral-800 text-neutral-200 border border-neutral-600 rounded font-medium hover:bg-neutral-700 cursor-pointer'}
      title="Upload Zerodha tradebook .xlsx to backfill entry dates from FIFO trade history"
    >
      {busy ? 'Importing…' : 'Import tradebook'}
      <input
        type="file"
        accept=".xlsx"
        disabled={busy}
        onChange={onPick}
        className="hidden"
      />
      {msg && (
        <span className={'ml-2 text-[10px] ' + (msg.startsWith('✓') ? 'text-green-400' : 'text-red-400')}>
          {msg}
        </span>
      )}
    </label>
  )
}
