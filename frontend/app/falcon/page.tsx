'use client'

// Falcon — Today (operator dashboard)
//
// Single-glance view: token health, market status, today's signals count,
// open positions P&L, pre-market queue size, and live monitor activity.
// Each panel handles empty/error states gracefully — page never breaks.

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { FalconAPI, type FalconStatus, type FalconSignalsToday,
         type TradePositionsResponse, type TradeMonitorStatus,
         type TickerStatus, type PremarketListResponse, type TradeEvent } from '../../lib/falcon-api'
import { fetchKiteStatus, type KiteStatus } from '@/lib/admin-api'

// ── IST clock helpers ───────────────────────────────────────────────────────
const IST_OFFSET_MIN = 5 * 60 + 30
function nowIST(): Date {
  // Convert local now → IST
  const utc = Date.now() + new Date().getTimezoneOffset() * 60_000
  return new Date(utc + IST_OFFSET_MIN * 60_000)
}
function marketSession(now: Date): 'pre' | 'open' | 'closed' | 'weekend' {
  const day = now.getDay()
  if (day === 0 || day === 6) return 'weekend'
  const m = now.getHours() * 60 + now.getMinutes()
  const open = 9 * 60 + 15
  const close = 15 * 60 + 30
  if (m < open) return 'pre'
  if (m < close) return 'open'
  return 'closed'
}

const inrLakh = (v: number | null | undefined) => {
  if (v == null) return '—'
  const a = Math.abs(v)
  const sign = v < 0 ? '-' : ''
  if (a >= 1e7) return `${sign}₹${(a / 1e7).toFixed(2)} Cr`
  if (a >= 1e5) return `${sign}₹${(a / 1e5).toFixed(2)} L`
  return `${sign}₹${a.toFixed(0)}`
}

// ── Page ────────────────────────────────────────────────────────────────────

export default function FalconHome() {
  const [status, setStatus]               = useState<FalconStatus | null>(null)
  const [signals, setSignals]             = useState<FalconSignalsToday | null>(null)
  const [positions, setPositions]         = useState<TradePositionsResponse | null>(null)
  const [monitor, setMonitor]             = useState<TradeMonitorStatus | null>(null)
  const [ticker, setTicker]               = useState<TickerStatus | null>(null)
  const [premarket, setPremarket]         = useState<PremarketListResponse | null>(null)
  const [events, setEvents]               = useState<TradeEvent[]>([])
  const [token, setToken]                 = useState<KiteStatus | null>(null)
  const [errors, setErrors]               = useState<Record<string, string>>({})
  const [now, setNow]                     = useState<Date>(nowIST())

  // Tick the clock every 30s — drives the market-status pill
  useEffect(() => {
    const id = setInterval(() => setNow(nowIST()), 30_000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    const loadAll = () => {
      const safe = <T,>(p: Promise<T>, key: string, set: (v: T) => void) => {
        p.then(v => {
          set(v)
          setErrors(e => { const { [key]: _, ...rest } = e; return rest })
        }).catch(err => {
          setErrors(e => ({ ...e, [key]: err instanceof Error ? err.message : String(err) }))
        })
      }
      safe(FalconAPI.adminStatus(),       'status',   setStatus)
      safe(FalconAPI.signalsToday(10),    'signals',  setSignals)
      safe(FalconAPI.tradePositions(),    'positions', setPositions)
      safe(FalconAPI.tradeMonitor(),      'monitor',  setMonitor)
      safe(FalconAPI.tradeTickerStatus(), 'ticker',   setTicker)
      safe(FalconAPI.premarketList(),     'premarket', setPremarket)
      safe(FalconAPI.tradeEvents(8),      'events',   r => setEvents(r.events))
      safe(fetchKiteStatus(),             'token',    setToken)
    }
    loadAll()
    const id = setInterval(loadAll, 30_000)
    return () => clearInterval(id)
  }, [])

  const session = marketSession(now)
  const istLabel = now.toLocaleTimeString('en-IN', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  })

  return (
    <div className="space-y-5">
      <header className="flex items-baseline justify-between flex-wrap gap-2">
        <div>
          <h1 className="text-2xl font-bold">Falcon — Today</h1>
          <p className="text-neutral-400 text-sm">
            {now.toLocaleDateString('en-IN', { weekday: 'long', day: 'numeric', month: 'short' })}
            {' · '}<span className="font-mono">{istLabel} IST</span>
            {' · '}<MarketStatusPill session={session} />
          </p>
        </div>
        <Link href="/falcon/admin" className="text-xs text-neutral-500 hover:text-neutral-300">
          Engine v{status?.engine_version ?? '—'} · {status?.n_promoted_patterns ?? '—'} patterns →
        </Link>
      </header>

      {/* Top status row — 4 cards */}
      <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatusCard
          label="Kite Token"
          value={token == null ? 'Checking…' : token.valid ? `✓ Valid` : '✗ Refresh required'}
          subtext={token?.user ? `${token.user}` : token?.reason || ''}
          tone={token == null ? 'neutral' : token.valid ? 'good' : 'bad'}
          link="/falcon/admin"
          linkLabel={token?.valid ? 'Manage →' : 'Refresh →'}
        />
        <StatusCard
          label="Auto-exit"
          value={monitor?.auto_exit_enabled ? 'ON' : 'OFF (manual)'}
          subtext={monitor ? `Polling ${monitor.interval_sec}s · ${monitor.n_tracked} tracked` : 'Monitor offline'}
          tone={monitor?.auto_exit_enabled ? 'good' : 'warn'}
          link="/falcon/config"
          linkLabel="Trail Config →"
        />
        <StatusCard
          label="Live Ticker"
          value={ticker == null ? '—' : ticker.connected ? 'Live' : 'Offline'}
          subtext={ticker ? `${ticker.subscribed_count} subs · ${ticker.tick_count.toLocaleString()} ticks` : ''}
          tone={ticker?.connected ? 'good' : 'warn'}
        />
        <StatusCard
          label="Pre-Market Queue"
          value={premarketQueueLabel(premarket)}
          subtext={premarket ? `Target ${premarket.target_date} · 9:15 IST` : ''}
          tone={(premarket && (premarket.summary.NEW_ENTRY?.QUEUED || premarket.summary.BULK_ADOPT?.QUEUED)) ? 'good' : 'neutral'}
          link="/falcon/premarket"
          linkLabel="Review →"
        />
      </section>

      {/* Three big quick-link panels */}
      <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <PositionsPanel pos={positions} err={errors.positions} />
        <SignalsPanel sig={signals} err={errors.signals} />
        <EventsPanel events={events} err={errors.events} />
      </section>
    </div>
  )
}

// ── Component pieces ────────────────────────────────────────────────────────

function MarketStatusPill({ session }: { session: ReturnType<typeof marketSession> }) {
  const map = {
    pre:     { label: 'Market opens 9:15 IST', cls: 'text-amber-400' },
    open:    { label: 'Market OPEN',           cls: 'text-green-400' },
    closed:  { label: 'Market closed',         cls: 'text-neutral-500' },
    weekend: { label: 'Weekend',               cls: 'text-neutral-500' },
  } as const
  const m = map[session]
  return <span className={m.cls}>{m.label}</span>
}

function premarketQueueLabel(pm: PremarketListResponse | null): string {
  if (!pm) return '—'
  const ne = pm.summary.NEW_ENTRY || {}
  const ba = pm.summary.BULK_ADOPT || {}
  const queued = (ne.QUEUED || 0) + (ba.QUEUED || 0)
  const staged = (ne.STAGED || 0) + (ba.STAGED || 0)
  if (queued === 0 && staged === 0) return 'Empty'
  if (queued > 0) return `${queued} queued`
  return `${staged} staged`
}

function StatusCard({ label, value, subtext, tone, link, linkLabel }: {
  label: string; value: string; subtext?: string;
  tone: 'good' | 'bad' | 'warn' | 'neutral';
  link?: string; linkLabel?: string;
}) {
  const ring = {
    good:    'border-green-500/30  bg-green-500/5',
    bad:     'border-red-500/30    bg-red-500/5',
    warn:    'border-amber-500/30  bg-amber-500/5',
    neutral: 'border-neutral-800   bg-neutral-900',
  }[tone]
  const valueCls = {
    good:    'text-green-300',
    bad:     'text-red-300',
    warn:    'text-amber-300',
    neutral: 'text-neutral-200',
  }[tone]
  return (
    <div className={`border rounded p-3 ${ring} flex flex-col`}>
      <div className="text-[11px] uppercase tracking-wider text-neutral-500 mb-1">{label}</div>
      <div className={`text-base font-semibold ${valueCls}`}>{value}</div>
      {subtext && <div className="text-[11px] text-neutral-500 mt-0.5 truncate">{subtext}</div>}
      {link && linkLabel && (
        <Link href={link} className="text-[11px] text-amber-400 hover:text-amber-300 mt-2">
          {linkLabel}
        </Link>
      )}
    </div>
  )
}

function PositionsPanel({ pos, err }: { pos: TradePositionsResponse | null; err: string | undefined }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-4 flex flex-col">
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-semibold">Open Positions</h2>
        <Link href="/falcon/positions" className="text-xs text-amber-400">All →</Link>
      </div>
      {err ? (
        <div className="text-xs text-red-300">✗ {err}</div>
      ) : pos == null ? (
        <div className="text-xs text-neutral-500">Loading…</div>
      ) : pos.n_positions === 0 ? (
        <div className="text-xs text-neutral-500">No open positions</div>
      ) : (
        <>
          <div className="text-2xl font-semibold text-neutral-100">{pos.n_positions}</div>
          <div className="text-xs text-neutral-500">positions · {inrLakh(pos.total_notional)} notional</div>
          <div className={
            'text-base font-semibold mt-2 ' +
            (pos.unrealized_pnl >= 0 ? 'text-green-300' : 'text-red-300')
          }>
            {pos.unrealized_pnl >= 0 ? '+' : ''}{inrLakh(pos.unrealized_pnl)}
            <span className="text-xs ml-1">({pos.pnl_pct >= 0 ? '+' : ''}{pos.pnl_pct.toFixed(2)}%)</span>
          </div>
          {pos.trigger_watch?.near_sl?.length > 0 && (
            <div className="mt-3 text-[11px] text-amber-300/80">
              ⚠ {pos.trigger_watch.near_sl.length} near SL · closest {pos.trigger_watch.near_sl[0].symbol} ({pos.trigger_watch.near_sl[0].distance_pct.toFixed(1)}%)
            </div>
          )}
        </>
      )}
    </div>
  )
}

function SignalsPanel({ sig, err }: { sig: FalconSignalsToday | null; err: string | undefined }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-4 flex flex-col">
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-semibold">Latest Signals</h2>
        <Link href="/falcon/signals" className="text-xs text-amber-400">All →</Link>
      </div>
      {err ? (
        <div className="text-xs text-red-300">✗ {err}</div>
      ) : sig == null ? (
        <div className="text-xs text-neutral-500">Loading…</div>
      ) : !sig.picks?.length ? (
        <div className="text-xs text-neutral-500">No signals — daily pipeline may not have run yet</div>
      ) : (
        <>
          <div className="text-2xl font-semibold text-neutral-100">{sig.n_picks}</div>
          <div className="text-xs text-neutral-500">picks for {sig.entry_date ?? sig.signal_date}</div>
          <div className="mt-3 space-y-0.5 text-xs font-mono">
            {sig.picks.slice(0, 5).map(p => (
              <div key={p.symbol} className="flex justify-between text-neutral-300">
                <span>{p.rank}. {p.symbol}</span>
                <span className="text-amber-400">{p.score.toFixed(0)}</span>
              </div>
            ))}
          </div>
          <Link href="/falcon/trade" className="text-[11px] text-amber-400 hover:text-amber-300 mt-3">
            Stage picks for tomorrow →
          </Link>
        </>
      )}
    </div>
  )
}

function EventsPanel({ events, err }: { events: TradeEvent[]; err: string | undefined }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-4 flex flex-col">
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-semibold">Live Monitor</h2>
        <Link href="/falcon/positions" className="text-xs text-amber-400">Stream →</Link>
      </div>
      {err ? (
        <div className="text-xs text-red-300">✗ {err}</div>
      ) : !events?.length ? (
        <div className="text-xs text-neutral-500">No recent events</div>
      ) : (
        <ul className="space-y-1 text-xs">
          {events.slice(0, 6).map(e => (
            <li key={e.id} className="flex gap-2">
              <span className={'font-mono text-[10px] px-1 rounded ' +
                (e.severity === 'critical' ? 'bg-red-500/20 text-red-300' :
                 e.severity === 'warn'     ? 'bg-amber-500/20 text-amber-300' :
                                              'bg-neutral-800 text-neutral-400')}>
                {e.kind}
              </span>
              <span className="font-medium">{e.symbol}</span>
              <span className="text-neutral-500 truncate flex-1">{e.detail || ''}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
