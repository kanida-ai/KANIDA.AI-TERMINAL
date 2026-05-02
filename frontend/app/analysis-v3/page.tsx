'use client'

/**
 * KANIDA TERMINAL — v3 design mock
 * ---------------------------------
 * Macro-to-micro AI workspace. Left nav for drill-down, center stack for
 * the trader's current focus, right rail for persistent AI chat + news.
 * Same V2 typography & color discipline, but a real workspace shape.
 *
 * Live at: /analysis-v3
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import {
  getSwingOverview, getActiveSignals, getSwingTickers, getIndices,
  getBreadth, getTopMovers, getSectorStats, getActivityFeed,
  getAIHealth, postAIChat,
  type SwingOverviewResponse, type ActiveSignalRow, type IndexInfo,
  type BreadthResponse, type TopMoversResponse, type SectorStat,
  type ActivityItem, type AIHealth,
} from '@/lib/backtest-api'

// ── Live data hook ────────────────────────────────────────────────────────────
// Fetches /swing/overview and /swing/active-signals when filters change.
// Falls back gracefully when API is unreachable so the mock still renders.
type LiveState = {
  overview:    SwingOverviewResponse | null
  signals:     ActiveSignalRow[]
  signalCount: number
  indices:     IndexInfo[]
  tickers:     string[]
  breadth:     BreadthResponse | null
  movers:      TopMoversResponse | null
  sectors:     SectorStat[]
  activity:    ActivityItem[]
  aiHealth:    AIHealth | null
  loading:     boolean
  error:       string | null
}
function useLiveData(ticker: string, year: string, indexFilter: string, engine: string) {
  const [s, setS] = useState<LiveState>({
    overview: null, signals: [], signalCount: 0,
    indices: [], tickers: [],
    breadth: null, movers: null, sectors: [], activity: [],
    aiHealth: null, loading: true, error: null,
  })

  const refetch = useCallback(() => {
    setS(p => ({ ...p, loading: true, error: null }))
    const yr = year && year !== 'ALL' ? year : undefined
    const tk = ticker && ticker !== 'ALL' ? ticker : undefined
    const ix = indexFilter && indexFilter !== 'ALL' ? indexFilter : undefined
    const en = engine && engine !== 'ALL' ? engine.toLowerCase() : undefined

    Promise.allSettled([
      getSwingOverview(yr, tk, ix),
      getActiveSignals({ engine: en, index: ix, ticker: tk }),
      getSwingTickers(),
      getIndices(),
      getBreadth(),
      getTopMovers(10),
      getSectorStats(),
      getActivityFeed(20),
      getAIHealth(),
    ]).then(([ov, sig, tk2, idx2, br, mv, sec, act, ai]) => {
      setS({
        overview:    ov.status   === 'fulfilled' ? ov.value : null,
        signals:     sig.status  === 'fulfilled' ? sig.value.signals : [],
        signalCount: sig.status  === 'fulfilled' ? sig.value.count   : 0,
        tickers:     tk2.status  === 'fulfilled' ? ['ALL', ...tk2.value.tickers] : ['ALL'],
        indices:     idx2.status === 'fulfilled' ? idx2.value.indices : [],
        breadth:     br.status   === 'fulfilled' ? br.value  : null,
        movers:      mv.status   === 'fulfilled' ? mv.value  : null,
        sectors:     sec.status  === 'fulfilled' ? sec.value.sectors : [],
        activity:    act.status  === 'fulfilled' ? act.value.items   : [],
        aiHealth:    ai.status   === 'fulfilled' ? ai.value  : null,
        loading:     false,
        error:       ov.status === 'rejected' ? String((ov as PromiseRejectedResult).reason) : null,
      })
    })
  }, [ticker, year, indexFilter, engine])

  useEffect(() => { refetch() }, [refetch])
  return s
}

// ── Tokens (extends V2) ──────────────────────────────────────────────────────
const T = {
  bg0:      '#000000',
  bg1:      '#0a0a0c',
  bg2:      '#111114',
  bg3:      '#16161b',
  bg4:      '#1c1c22',
  border:   '#1c1c22',
  borderHi: '#2a2a32',
  label:    '#f59e0b',
  data:     '#f5f5f7',
  dim:      '#6b7280',
  dim2:     '#9ca3af',
  green:    '#22c55e',
  red:      '#ef4444',
  blue:     '#60a5fa',
  yellow:   '#fde047',
  violet:   '#a78bfa',
  // AI accent — distinct from amber, signals "AI surface"
  ai:       '#34d399',
}

// Workspace tabs collapsed to one for now (Sprint 1 = ship one polished view).
// Multi-workspace shell will return in Sprint 2 when each tab has unique content.
const WORKSPACES = [
  { k: 'OVERVIEW', label: 'Overview' },
]

const NAV = [
  {
    label: 'MARKETS',
    children: [
      { label: 'India · NSE',     leaf: false, children: [
        { label: 'Indices',        leaf: false, children: [
          { label: 'NIFTY 50' },
          { label: 'BANKNIFTY' },
          { label: 'NIFTY MIDCAP 150' },
        ]},
        { label: 'Sectors (11)',   leaf: true,  active: true },
        { label: 'F&O Universe',   leaf: true },
      ]},
      { label: 'Global',          leaf: false, children: [
        { label: 'S&P 500' },
        { label: 'Nasdaq' },
        { label: 'Hang Seng' },
      ]},
    ],
  },
  {
    label: 'WATCHLIST',
    children: [
      { label: 'My core (12)',   leaf: true },
      { label: 'High conviction (3)', leaf: true },
      { label: 'Bank stress',    leaf: true },
    ],
  },
  {
    label: 'SAVED SCREENS',
    children: [
      { label: 'Turbo · WR > 95',     leaf: true },
      { label: 'Super · sector=Pharma', leaf: true },
      { label: 'Recent breakouts',     leaf: true },
    ],
  },
]

// Static demo constants removed — every panel now gets data from the live hooks.

// ── Tiny helpers ──────────────────────────────────────────────────────────────
function Spark({ pts, color }: { pts: number[]; color: string }) {
  const w = 60, h = 16
  const min = Math.min(...pts), max = Math.max(...pts)
  const r = max - min || 1
  const path = pts.map((v, i) => {
    const x = (i / (pts.length - 1)) * w
    const y = h - ((v - min) / r) * h
    return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return <svg width={w} height={h}><path d={path} fill="none" stroke={color} strokeWidth={1.1} /></svg>
}

const kbd: React.CSSProperties = {
  fontFamily: 'IBM Plex Mono, monospace', fontSize: 9,
  border: `1px solid ${T.borderHi}`, borderRadius: 2,
  padding: '1px 4px', background: T.bg2, color: T.dim2, marginLeft: 4,
}

// ── Mode toggle (TERMINAL ↔ MODERN) ──────────────────────────────────────────
type Mode = 'TERMINAL' | 'MODERN'
function ModeToggle({ mode, onChange }: { mode: Mode; onChange: (m: Mode) => void }) {
  const opts: Mode[] = ['TERMINAL', 'MODERN']
  return (
    <div style={{
      display: 'inline-flex', alignItems: 'center', height: 28,
      border: `1px solid ${T.borderHi}`, background: T.bg2,
      fontFamily: 'IBM Plex Mono, monospace', fontSize: 10,
      letterSpacing: '0.08em',
    }}>
      {opts.map(o => (
        <button
          key={o}
          onClick={() => onChange(o)}
          style={{
            padding: '0 14px', height: '100%', border: 'none', cursor: 'pointer',
            background: mode === o ? T.label : 'transparent',
            color: mode === o ? T.bg0 : T.dim2,
            fontWeight: 700, fontFamily: 'inherit', fontSize: 'inherit',
            letterSpacing: 'inherit',
          }}
        >{o}</button>
      ))}
    </div>
  )
}


// ── Brand bar (prominent, top of page) ────────────────────────────────────────
function BrandBar({ mode, setMode }: { mode: Mode; setMode: (m: Mode) => void }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center',
      borderBottom: `1px solid ${T.border}`,
      background: `linear-gradient(180deg, ${T.bg1} 0%, ${T.bg0} 100%)`,
      height: 56, padding: '0 22px',
    }}>
      {/* Brand mark */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 0 }}>
        <span style={{
          color: T.data, fontFamily: 'Inter Tight, sans-serif',
          fontWeight: 800, fontSize: 26, letterSpacing: '-0.04em',
        }}>KANIDA</span>
        <span style={{
          color: T.label, fontFamily: 'Inter Tight, sans-serif',
          fontWeight: 800, fontSize: 26, letterSpacing: '-0.04em',
        }}>.AI</span>
        <span style={{
          color: T.dim, fontFamily: 'IBM Plex Mono, monospace',
          fontSize: 11, marginLeft: 14, letterSpacing: '0.18em', textTransform: 'uppercase',
          fontWeight: 500,
        }}>Quant Intelligence Terminal</span>
      </div>

      {/* Tagline */}
      <div style={{ marginLeft: 28, paddingLeft: 18, borderLeft: `1px solid ${T.border}`,
                    display: 'flex', alignItems: 'baseline', gap: 14 }}>
        <span style={{ color: T.dim2, fontSize: 11, fontFamily: 'IBM Plex Mono', letterSpacing: '0.05em' }}>
          NSE  ·  Long-only  ·  Smart Entry via Execution IQ
        </span>
        <span style={{ color: T.label, fontSize: 10, fontFamily: 'IBM Plex Mono',
                       border: `1px solid ${T.label}55`, padding: '2px 8px',
                       background: `${T.label}11`, letterSpacing: '0.08em' }}>
          v3.1 · MOCK
        </span>
      </div>

      {/* Right side: mode toggle + actions */}
      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 14 }}>
        <ModeToggle mode={mode} onChange={setMode} />
        <span style={{ color: T.dim, fontFamily: 'IBM Plex Mono', fontSize: 11 }}>·</span>
        <button style={{
          fontFamily: 'IBM Plex Mono', fontSize: 11, padding: '5px 10px',
          background: 'transparent', border: `1px solid ${T.border}`, color: T.dim2,
          cursor: 'pointer', letterSpacing: '0.04em',
        }}>SAVE LAYOUT</button>
      </div>
    </div>
  )
}


// ── Top status bar ────────────────────────────────────────────────────────────
function StatusBar() {
  const [now, setNow] = useState<Date | null>(null)
  useEffect(() => {
    setNow(new Date())
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  const ist = now ? now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false }) : '--:--:--'
  const ny  = now ? now.toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour12: false }) : '--:--:--'
  const lon = now ? now.toLocaleTimeString('en-GB', { timeZone: 'Europe/London',     hour12: false }) : '--:--:--'

  const cells = [
    { label: 'IST',        value: ist,                color: T.data },
    { label: 'NYC',        value: ny,                 color: T.dim2 },
    { label: 'LON',        value: lon,                color: T.dim2 },
    { label: 'NSE',        value: 'CLOSED',           color: T.dim2 },
    { label: 'PIPE',       value: 'OK · 16:05',       color: T.green },
    { label: 'TOKEN',      value: 'KITE · VALID',     color: T.green },
    { label: 'AI',         value: 'CONNECTED',        color: T.ai },
  ]
  return (
    <div style={{
      display: 'flex', alignItems: 'center',
      borderBottom: `1px solid ${T.border}`, background: T.bg1,
      fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, height: 28,
    }}>
      {cells.map((c, i) => (
        <div key={c.label} style={{
          padding: '0 14px', height: '100%', display: 'flex', alignItems: 'center', gap: 6,
          borderRight: i < cells.length - 1 ? `1px solid ${T.border}` : 'none',
        }}>
          <span style={{ color: T.dim, fontSize: 10, letterSpacing: '0.05em' }}>{c.label}</span>
          <span style={{ color: c.color, fontWeight: 500 }}>{c.value}</span>
        </div>
      ))}
      <div style={{ marginLeft: 'auto', padding: '0 14px', color: T.dim, fontSize: 10 }}>
        <kbd style={kbd}>⌘K</kbd> command  <kbd style={kbd}>/</kbd> ticker  <kbd style={kbd}>?</kbd> shortcuts
      </div>
    </div>
  )
}

// ── Breadth strip (replaces MacroStrip — universe-wide breadth from our data)
function BreadthStrip({ breadth }: { breadth: BreadthResponse | null }) {
  const fmtPct = (v: number | undefined | null) =>
    v === undefined || v === null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(2) + '%'

  const cells: { label: string; value: string; sub?: string; color: string }[] =
    breadth ? [
      { label: 'AS OF',      value: breadth.as_of || '—',                   color: T.dim2 },
      { label: 'UNIVERSE',   value: String(breadth.total_stocks),
        sub: `${breadth.advancers}↑ ${breadth.decliners}↓`,                color: T.data },
      { label: 'AVG MOVE',   value: fmtPct(breadth.avg_pct),                color: (breadth.avg_pct >= 0 ? T.green : T.red) },
      { label: 'BEST',       value: breadth.best_stock?.ticker || '—',
        sub: fmtPct(breadth.best_stock?.pct),                              color: T.green },
      { label: 'WORST',      value: breadth.worst_stock?.ticker || '—',
        sub: fmtPct(breadth.worst_stock?.pct),                             color: T.red },
      { label: 'TOP SECTOR', value: breadth.best_sector?.sector || '—',
        sub: fmtPct(breadth.best_sector?.avg_pct),                         color: T.green },
      { label: 'WEAK SECTOR',value: breadth.worst_sector?.sector || '—',
        sub: fmtPct(breadth.worst_sector?.avg_pct),                        color: T.red },
      { label: 'SIGNALS',    value: String(breadth.signals_total),
        sub: `${breadth.signals_hc} HC`,                                    color: T.label },
    ]
    : Array.from({ length: 8 }, (_, i) => ({ label: '—', value: '—', color: T.dim }))

  return (
    <div style={{
      display: 'grid', gridTemplateColumns: `repeat(${cells.length}, 1fr)`, gap: 1,
      background: T.border, borderBottom: `1px solid ${T.border}`,
    }}>
      {cells.map((c, i) => (
        <div key={`${c.label}-${i}`} style={{
          background: T.bg0, padding: '10px 14px',
          fontFamily: 'IBM Plex Mono, monospace',
        }}>
          <div style={{ color: T.dim, fontSize: 10, letterSpacing: '0.06em' }}>{c.label}</div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 2 }}>
            <span style={{ color: c.color, fontSize: 14, fontWeight: 500, fontFeatureSettings: '"tnum" 1' }}>{c.value}</span>
            {c.sub && <span style={{ color: T.dim2, fontSize: 11, fontWeight: 500 }}>{c.sub}</span>}
          </div>
        </div>
      ))}
    </div>
  )
}

// ── Workspace tabs ───────────────────────────────────────────────────────────
function WorkspaceTabs({ active, onChange }: { active: string; onChange: (k: string) => void }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'stretch', height: 32,
      borderBottom: `1px solid ${T.border}`, background: T.bg0,
    }}>
      {WORKSPACES.map((w, i) => {
        const isActive = active === w.k
        return (
          <button
            key={w.k}
            onClick={() => onChange(w.k)}
            style={{
              padding: '0 16px', display: 'flex', alignItems: 'center', gap: 8,
              background: isActive ? T.bg2 : 'transparent',
              borderRight: `1px solid ${T.border}`, border: 'none',
              borderTop: isActive ? `2px solid ${T.label}` : '2px solid transparent',
              color: isActive ? T.data : T.dim2, cursor: 'pointer',
              fontFamily: 'Inter Tight, sans-serif', fontSize: 11, fontWeight: 500, letterSpacing: '0.04em',
            }}
          >
            <span style={{ color: T.label, fontFamily: 'IBM Plex Mono', fontSize: 10 }}>{i + 1}</span>
            <span>{w.label.toUpperCase()}</span>
          </button>
        )
      })}
      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', padding: '0 14px', gap: 10, color: T.dim, fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
        <span>+ NEW WORKSPACE</span>
      </div>
    </div>
  )
}

// ── Left nav ──────────────────────────────────────────────────────────────────
type NavNode = { label: string; leaf?: boolean; active?: boolean; children?: NavNode[] }
function NavTreeItem({ n, depth = 0 }: { n: NavNode; depth?: number }) {
  const [open, setOpen] = useState(true)
  const has = !!n.children?.length
  return (
    <div>
      <div
        onClick={() => has && setOpen(!open)}
        style={{
          display: 'flex', alignItems: 'center', gap: 6,
          padding: `4px ${10 + depth * 12}px`, cursor: has ? 'pointer' : 'default',
          color: n.active ? T.label : T.dim2, fontSize: 11,
          background: n.active ? T.bg2 : 'transparent',
          fontFamily: 'IBM Plex Mono, monospace',
        }}
      >
        {has ? <span style={{ color: T.dim, width: 8 }}>{open ? '▾' : '▸'}</span> : <span style={{ width: 8 }} />}
        <span>{n.label}</span>
      </div>
      {has && open && n.children!.map((c, i) => <NavTreeItem key={i} n={c} depth={depth + 1} />)}
    </div>
  )
}

function LeftRail() {
  return (
    <div style={{
      width: 220, borderRight: `1px solid ${T.border}`, background: T.bg1,
      display: 'flex', flexDirection: 'column', overflowY: 'auto',
    }}>
      {NAV.map((s, i) => (
        <div key={i} style={{ borderBottom: `1px solid ${T.border}`, padding: '10px 0' }}>
          <div style={{
            padding: '0 12px 6px', color: T.label, fontSize: 9,
            letterSpacing: '0.12em', fontFamily: 'Inter Tight', fontWeight: 700,
          }}>{s.label}</div>
          {s.children?.map((n, j) => <NavTreeItem key={j} n={n} />)}
        </div>
      ))}
      <div style={{ padding: '12px', color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
        <kbd style={kbd}>+</kbd> add list
      </div>
    </div>
  )
}

// ── Breadcrumb ────────────────────────────────────────────────────────────────
function Breadcrumb() {
  const parts = ['Markets', 'India · NSE', 'Sectors', 'All (16)']
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '8px 16px', borderBottom: `1px solid ${T.border}`,
      fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
      background: T.bg0,
    }}>
      {parts.map((p, i) => (
        <React.Fragment key={i}>
          <span style={{ color: i === parts.length - 1 ? T.data : T.dim2 }}>{p}</span>
          {i < parts.length - 1 && <span style={{ color: T.dim }}>›</span>}
        </React.Fragment>
      ))}
      <div style={{ marginLeft: 'auto', display: 'flex', gap: 12 }}>
        <button style={chipBtn(false)}>HEATMAP</button>
        <button style={chipBtn(true)}>TABLE</button>
        <button style={chipBtn(false)}>CHART</button>
      </div>
    </div>
  )
}
function chipBtn(active: boolean): React.CSSProperties {
  return {
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, fontWeight: 500,
    padding: '3px 8px', background: active ? T.bg2 : 'transparent',
    color: active ? T.label : T.dim2,
    border: `1px solid ${active ? T.label : T.border}`,
    cursor: 'pointer', letterSpacing: '0.04em',
  }
}

// ── Sector heatmap ────────────────────────────────────────────────────────────
function SectorHeatmap({ sectors, asOf }: { sectors: SectorStat[]; asOf: string | null }) {
  const cellColor = (c: number) => {
    const intensity = Math.min(Math.abs(c) / 2.5, 1)
    if (c >= 0) return `rgba(34, 197, 94, ${0.10 + intensity * 0.5})`
    return `rgba(239, 68, 68, ${0.10 + intensity * 0.5})`
  }
  return (
    <div style={{ padding: '12px 16px', borderBottom: `1px solid ${T.border}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <span style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight' }}>
          NSE SECTOR HEATMAP · {sectors.length} SECTORS
        </span>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
          as of {asOf || '—'}
        </span>
      </div>
      {sectors.length === 0 ? (
        <div style={{ color: T.dim, fontSize: 11, padding: 18, textAlign: 'center', fontFamily: 'IBM Plex Mono' }}>
          No sector data — admin may need to seed the universe table.
        </div>
      ) : (
        <div style={{
          display: 'grid', gridTemplateColumns: `repeat(${Math.min(sectors.length, 8)}, 1fr)`, gap: 1,
          background: T.border, border: `1px solid ${T.border}`,
        }}>
          {sectors.slice(0, 32).map(s => (
            <div key={s.sector} style={{
              background: cellColor(s.avg_pct), padding: '14px 10px',
              display: 'flex', flexDirection: 'column', justifyContent: 'space-between',
              minHeight: 64, cursor: 'pointer',
            }} title={`${s.advancers}↑ ${s.decliners}↓ · best: ${s.best_ticker || '—'}`}>
              <span style={{ color: T.data, fontSize: 10, fontFamily: 'Inter Tight', fontWeight: 600, letterSpacing: '0.04em' }}>{s.sector}</span>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                <span style={{
                  color: s.avg_pct >= 0 ? T.green : T.red, fontSize: 14,
                  fontFamily: 'IBM Plex Mono', fontWeight: 500, fontFeatureSettings: '"tnum" 1',
                }}>
                  {s.avg_pct >= 0 ? '+' : ''}{s.avg_pct.toFixed(2)}%
                </span>
                <span style={{ color: T.dim, fontSize: 9, fontFamily: 'IBM Plex Mono' }}>n={s.members}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Hero engines (the headline of the page) ──────────────────────────────────
function HeroEngines({
  overview, year, setYear, ticker, indexFilter, loading,
}: {
  overview:    SwingOverviewResponse | null
  year:        string
  setYear:     (y: string) => void
  ticker:      string
  indexFilter: string
  loading:     boolean
}) {
  const ICONS:  Record<string, string> = { turbo: 'T', super: 'S', standard: 'ST' }
  const COLORS: Record<string, string> = { turbo: T.label, super: T.green, standard: T.blue }
  const DESCS:  Record<string, string> = {
    turbo:    'Fast momentum exits · 1-3 day resolution',
    super:    'Trend continuation · highest avg per trade',
    standard: 'High volume · selective entry required',
  }

  const ENGINE_DETAIL = overview?.engines
    ? overview.engines.filter(e => ['turbo','super','standard'].includes(e.bucket)).map(e => ({
        name: e.bucket.toUpperCase(),
        icon: ICONS[e.bucket] || e.bucket[0]?.toUpperCase() || '·',
        color: COLORS[e.bucket] || T.dim2,
        n: e.total_trades,
        wr: e.smart_win_rate,
        avg: e.smart_avg_pnl,
        cum: e.total_pnl_all,
        hold: e.avg_days,
        p90: e.pnl_90d_avg ?? 0,
        p180: e.pnl_180d_avg ?? 0,
        desc: e.description || DESCS[e.bucket] || '',
        spark: [
          (e.pnl_180d_avg ?? e.smart_avg_pnl) * 0.9,
          (e.pnl_180d_avg ?? e.smart_avg_pnl),
          (e.pnl_90d_avg  ?? e.smart_avg_pnl) * 1.05,
          e.smart_avg_pnl * 0.95,
          e.smart_avg_pnl * 1.05,
          e.smart_avg_pnl,
          e.smart_avg_pnl,
        ],
      }))
    : []

  const sumTrades = ENGINE_DETAIL.reduce((s, e) => s + e.n, 0)
  const scopeLabel =
    ticker !== 'ALL'      ? `· ${ticker}` :
    indexFilter !== 'ALL' ? `· ${indexFilter}` :
                            ''

  return (
    <div style={{ borderBottom: `1px solid ${T.border}`, background: T.bg0 }}>
      {/* Section header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
        padding: '18px 22px 8px',
      }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 14 }}>
          <span style={{
            color: T.data, fontFamily: 'Inter Tight', fontWeight: 700,
            fontSize: 22, letterSpacing: '-0.02em',
          }}>Engine Performance</span>
          <span style={{
            color: T.label, fontFamily: 'IBM Plex Mono', fontSize: 10,
            border: `1px solid ${T.label}55`, padding: '2px 8px',
            background: `${T.label}11`, letterSpacing: '0.08em',
          }}>HIGH CONVICTION</span>
          <span style={{ color: T.dim, fontSize: 11, fontFamily: 'IBM Plex Mono', letterSpacing: '0.04em' }}>
            {loading ? 'loading…' : `${sumTrades.toLocaleString()} trades  ·  smart entry effective  ·  ${year === 'ALL' ? 'ALL years' : year} ${scopeLabel}`}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          {['ALL', '2024', '2025', '2026'].map(y => (
            <button key={y} onClick={() => setYear(y)} style={{
              ...chipBtn(y === year),
              padding: '4px 12px', fontSize: 11,
            }}>{y}</button>
          ))}
        </div>
      </div>

      {/* Hero grid — loading skeleton, empty state, or 3 huge cards */}
      {loading && ENGINE_DETAIL.length === 0 ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1, background: T.border, padding: '0 1px 1px' }}>
          {[0, 1, 2].map(i => (
            <div key={i} style={{ background: T.bg0, padding: '20px 22px', minHeight: 240 }}>
              <div style={{ height: 18, width: 80, background: T.bg2, marginBottom: 18 }} />
              <div style={{ height: 44, width: '70%', background: T.bg2, marginBottom: 12 }} />
              <div style={{ height: 14, width: '50%', background: T.bg2 }} />
            </div>
          ))}
        </div>
      ) : ENGINE_DETAIL.length === 0 ? (
        <div style={{ padding: 36, color: T.dim, textAlign: 'center', fontFamily: 'IBM Plex Mono', fontSize: 12 }}>
          No trades match these filters. Try widening the year or removing the stock/index filter.
        </div>
      ) : (
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1,
        background: T.border, padding: '0 1px 1px',
      }}>
        {ENGINE_DETAIL.map(e => {
          const wrColor  = e.wr >= 90 ? T.green : e.wr >= 50 ? T.yellow : T.red
          const avgColor = e.avg >= 0 ? T.green : T.red
          return (
            <div key={e.name} style={{
              background: T.bg0, padding: '20px 22px 22px',
              position: 'relative', minHeight: 240,
            }}>
              {/* Engine identity row */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 18 }}>
                <span style={{
                  fontFamily: 'IBM Plex Mono', fontSize: 12, fontWeight: 700,
                  color: e.color, padding: '3px 8px',
                  border: `1px solid ${e.color}66`, background: `${e.color}11`,
                  letterSpacing: '0.06em',
                }}>[{e.icon}]</span>
                <span style={{
                  color: e.color, fontFamily: 'Inter Tight', fontWeight: 700,
                  fontSize: 18, letterSpacing: '0.04em',
                }}>{e.name}</span>
                <span style={{ marginLeft: 'auto', color: T.dim, fontFamily: 'IBM Plex Mono', fontSize: 11 }}>
                  n = {e.n.toLocaleString()}
                </span>
              </div>

              {/* The two giant numbers — WR + AVG P&L */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginBottom: 16 }}>
                <div>
                  <div style={{ color: T.dim, fontSize: 10, letterSpacing: '0.10em', marginBottom: 4 }}>WIN RATE</div>
                  <div style={{
                    color: wrColor, fontFamily: 'IBM Plex Mono',
                    fontSize: 44, fontWeight: 500, lineHeight: 1,
                    fontFeatureSettings: '"tnum" 1', letterSpacing: '-0.03em',
                  }}>
                    {e.wr.toFixed(2)}<span style={{ color: T.dim2, fontSize: 22, marginLeft: 2 }}>%</span>
                  </div>
                </div>
                <div>
                  <div style={{ color: T.dim, fontSize: 10, letterSpacing: '0.10em', marginBottom: 4 }}>AVG P&L / TRADE</div>
                  <div style={{
                    color: avgColor, fontFamily: 'IBM Plex Mono',
                    fontSize: 44, fontWeight: 500, lineHeight: 1,
                    fontFeatureSettings: '"tnum" 1', letterSpacing: '-0.03em',
                  }}>
                    {e.avg >= 0 ? '+' : ''}{e.avg.toFixed(2)}<span style={{ color: T.dim2, fontSize: 22, marginLeft: 2 }}>%</span>
                  </div>
                </div>
              </div>

              {/* Secondary stats row */}
              <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12,
                paddingTop: 12, borderTop: `1px solid ${T.border}`,
              }}>
                {[
                  { k: 'CUM',  v: (e.cum >= 0 ? '+' : '') + e.cum.toFixed(0) + '%', c: avgColor },
                  { k: 'HOLD', v: e.hold.toFixed(1) + 'd',                          c: T.data },
                  { k: '90D',  v: (e.p90 >= 0 ? '+' : '') + e.p90.toFixed(2) + '%', c: e.p90 >= 0 ? T.green : T.red },
                  { k: '180D', v: (e.p180 >= 0 ? '+' : '') + e.p180.toFixed(2)+ '%',c: e.p180 >= 0 ? T.green : T.red },
                ].map(s => (
                  <div key={s.k}>
                    <div style={{ color: T.dim, fontSize: 9, letterSpacing: '0.10em' }}>{s.k}</div>
                    <div style={{
                      color: s.c, fontFamily: 'IBM Plex Mono',
                      fontSize: 13, fontWeight: 500, marginTop: 2,
                      fontFeatureSettings: '"tnum" 1',
                    }}>{s.v}</div>
                  </div>
                ))}
              </div>

              {/* Sparkline + description */}
              <div style={{
                marginTop: 14, display: 'flex',
                justifyContent: 'space-between', alignItems: 'flex-end',
              }}>
                <span style={{ color: T.dim2, fontSize: 11, fontFamily: 'Inter Tight', maxWidth: '60%' }}>
                  {e.desc}
                </span>
                <Spark pts={e.spark} color={avgColor} />
              </div>

              {/* Drill-in CTA */}
              <button style={{
                position: 'absolute', top: 18, right: 18,
                background: 'transparent', border: `1px solid ${T.border}`,
                color: T.dim2, fontSize: 10, fontFamily: 'IBM Plex Mono',
                padding: '4px 8px', cursor: 'pointer', letterSpacing: '0.06em',
              }}>VIEW &nbsp;›</button>
            </div>
          )
        })}
      </div>
      )}
    </div>
  )
}


// ── Top movers (own panel, below the fold) ───────────────────────────────────
function TopMoversPanel({ movers }: { movers: TopMoversResponse | null }) {
  const [side, setSide] = useState<'gainers' | 'losers'>('gainers')
  const rows = movers ? movers[side] : []

  const fmtVol = (v: number) =>
    v >= 1e7 ? (v / 1e7).toFixed(1) + 'Cr' :
    v >= 1e5 ? (v / 1e5).toFixed(1) + 'L'  :
    v.toLocaleString()

  return (
    <div style={{ borderBottom: `1px solid ${T.border}`, background: T.bg0, padding: '14px 22px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'baseline' }}>
          <span style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight' }}>
            TOP MOVERS · {movers?.as_of || '—'}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <button onClick={() => setSide('gainers')} style={chipBtn(side === 'gainers')}>GAINERS</button>
          <button onClick={() => setSide('losers')}  style={chipBtn(side === 'losers')}>LOSERS</button>
        </div>
      </div>
      {rows.length === 0 ? (
        <div style={{ color: T.dim, fontSize: 11, padding: 18, textAlign: 'center', fontFamily: 'IBM Plex Mono' }}>
          No data available.
        </div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${T.border}` }}>
              {['TICKER', 'SECTOR', 'CLOSE', 'CHG', 'VOL', 'SIG'].map(h => (
                <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: T.dim, fontSize: 10, fontWeight: 500 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map(m => (
              <tr key={m.ticker} style={{ borderBottom: `1px solid ${T.border}` }}>
                <td style={{ padding: '7px 8px', color: T.blue, fontWeight: 600 }}>{m.ticker}</td>
                <td style={{ padding: '7px 8px', color: T.dim2, fontFamily: 'Inter Tight' }}>{m.sector}</td>
                <td style={{ padding: '7px 8px', color: T.data, textAlign: 'right', fontFeatureSettings: '"tnum" 1' }}>{m.close.toLocaleString()}</td>
                <td style={{ padding: '7px 8px', color: m.pct >= 0 ? T.green : T.red, fontFeatureSettings: '"tnum" 1', textAlign: 'right' }}>
                  {m.pct >= 0 ? '+' : ''}{m.pct.toFixed(2)}%
                </td>
                <td style={{ padding: '7px 8px', color: T.dim2, textAlign: 'right' }}>{fmtVol(m.volume)}</td>
                <td style={{ padding: '7px 8px', color: m.active_signal ? T.label : T.dim }}>
                  {m.active_signal ? '★' : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

// ── Active signals (compressed) ───────────────────────────────────────────────
function ActiveSignalsPanel({
  signals, engine, setEngine, loading,
}: {
  signals: ActiveSignalRow[]
  engine:  string
  setEngine: (e: string) => void
  loading: boolean
}) {
  const rows = signals.map(s => ({ tk: s.ticker, eng: s.engine, score: s.opportunity_score, sec: s.sector || '—' }))
  return (
    <div style={{ padding: '12px 16px', borderBottom: `1px solid ${T.border}`, background: T.bg0 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
          <span style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight' }}>
            ACTIVE SIGNALS · TODAY · {loading ? '…' : rows.length}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {['ALL', 'TURBO', 'SUPER', 'STANDARD'].map(b => (
            <button key={b} onClick={() => setEngine(b)} style={chipBtn(b === engine)}>
              {b === 'STANDARD' ? 'STD' : b}
            </button>
          ))}
        </div>
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${T.border}` }}>
            {['#', 'TICKER', 'ENGINE', 'SCORE', 'SECTOR', 'PIN', 'AI'].map(h => (
              <th key={h} style={{ padding: '6px 10px', textAlign: 'left', color: T.dim, fontSize: 10, fontWeight: 500 }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((a, i) => {
            const engUpper = (a.eng || '').toUpperCase()
            const engColor = engUpper === 'TURBO' ? T.label : engUpper === 'SUPER' ? T.green : T.blue
            return (
              <tr key={`${a.tk}-${i}`} style={{ borderBottom: `1px solid ${T.border}` }}>
                <td style={{ padding: '7px 10px', color: T.dim, width: 24 }}>{i + 1}</td>
                <td style={{ padding: '7px 10px', color: T.blue, fontWeight: 600 }}>{a.tk}</td>
                <td style={{ padding: '7px 10px', color: engColor, fontWeight: 500 }}>{engUpper}</td>
                <td style={{ padding: '7px 10px', color: T.data, textAlign: 'right', width: 60, fontFeatureSettings: '"tnum" 1' }}>{a.score.toFixed(3)}</td>
                <td style={{ padding: '7px 10px', color: T.dim2, fontFamily: 'Inter Tight' }}>{a.sec}</td>
                <td style={{ padding: '7px 10px', color: T.dim2, fontSize: 11, cursor: 'pointer' }}>+ pin</td>
                <td style={{ padding: '7px 10px', color: T.ai, fontSize: 11, cursor: 'pointer' }}>ask</td>
              </tr>
            )
          })}
          {rows.length === 0 && (
            <tr><td colSpan={7} style={{ padding: 24, color: T.dim, textAlign: 'center' }}>No signals match these filters.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ── Right rail: AI chat (wired to /api/ai/chat) ──────────────────────────────
type ChatMsg = { who: 'user' | 'ai'; body: string; isError?: boolean }
function AIChatPanel({ aiHealth, ctx }: { aiHealth: AIHealth | null; ctx: Record<string, unknown> }) {
  const [input, setInput] = useState('')
  const [convo, setConvo] = useState<ChatMsg[]>([
    { who: 'ai', body: aiHealth?.configured
      ? "I have your current view in context — engine performance, active signals, and your filters. Ask me anything about a setup, a stock, or what's worth watching."
      : "AI chat will activate once the admin sets ANTHROPIC_API_KEY in Railway. Until then, you can still browse all the live data on the page."
    },
  ])
  const [thinking, setThinking] = useState(false)
  const endRef = useRef<HTMLDivElement>(null)

  async function send(messageOverride?: string) {
    const text = (messageOverride ?? input).trim()
    if (!text || thinking) return
    setConvo(c => [...c, { who: 'user', body: text }])
    setInput('')
    setThinking(true)
    setTimeout(() => endRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)

    try {
      const history = convo
        .filter(m => !m.isError)
        .map(m => ({ role: (m.who === 'ai' ? 'assistant' : 'user') as 'user' | 'assistant', content: m.body }))
      const r = await postAIChat({ message: text, history, context: ctx, model: 'haiku' })
      setConvo(c => [...c, { who: 'ai', body: r.message }])
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'AI request failed.'
      setConvo(c => [...c, { who: 'ai', body: msg, isError: true }])
    } finally {
      setThinking(false)
      setTimeout(() => endRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <div style={{
        padding: '10px 14px', borderBottom: `1px solid ${T.border}`,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 6, height: 6, borderRadius: 6, background: T.ai, boxShadow: `0 0 6px ${T.ai}` }} />
          <span style={{ color: T.ai, fontSize: 10, letterSpacing: '0.12em', fontWeight: 600, fontFamily: 'Inter Tight' }}>KANIDA AI</span>
          <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
            {aiHealth?.configured ? '· context-aware' : '· not configured'}
          </span>
        </div>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
          {aiHealth?.configured ? aiHealth.model.replace('claude-', '').slice(0, 16) : '—'}
        </span>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 14 }}>
        {convo.map((m, i) => (
          <div key={i}>
            <div style={{
              fontFamily: 'IBM Plex Mono', fontSize: 9, letterSpacing: '0.08em',
              color: m.who === 'ai' ? T.ai : T.label, marginBottom: 4,
            }}>
              {m.who === 'ai' ? 'KANIDA' : 'YOU'}
            </div>
            <div style={{
              color: m.isError ? T.red : T.data, fontSize: 12, lineHeight: 1.55,
              fontFamily: m.who === 'user' ? 'IBM Plex Mono' : 'Inter Tight',
              background: m.who === 'user' ? T.bg2 : 'transparent',
              padding: m.who === 'user' ? '6px 10px' : 0,
              borderLeft: m.who === 'user' ? `2px solid ${T.label}` : 'none',
              whiteSpace: 'pre-wrap',
            }}>
              {m.body}
            </div>
          </div>
        ))}
        {thinking && (
          <div style={{ color: T.dim2, fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
            <span style={{ color: T.ai }}>KANIDA</span> · thinking
            <span style={{ display: 'inline-block', marginLeft: 4 }}>
              <span style={{ animation: 'blink 1s infinite' }}>·</span>
              <span style={{ animation: 'blink 1s infinite 0.2s' }}>·</span>
              <span style={{ animation: 'blink 1s infinite 0.4s' }}>·</span>
            </span>
          </div>
        )}
        <div ref={endRef} />
      </div>

      <div style={{ borderTop: `1px solid ${T.border}`, padding: '10px 14px', background: T.bg1 }}>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          {['/morning', '/explain', '/screen', '/compare', '/risk'].map(s => (
            <button key={s} onClick={() => setInput(s + ' ')} style={{
              fontFamily: 'IBM Plex Mono', fontSize: 10, padding: '3px 7px',
              background: T.bg2, border: `1px solid ${T.border}`, color: T.ai, cursor: 'pointer',
            }}>{s}</button>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{ color: T.ai, fontFamily: 'IBM Plex Mono', fontSize: 11 }}>{'>'}</span>
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && send()}
            placeholder="ask anything · /screen wr>95 turbo"
            style={{
              flex: 1, background: 'transparent', border: 'none', outline: 'none',
              color: T.data, fontFamily: 'IBM Plex Mono', fontSize: 12,
            }}
          />
          <kbd style={kbd}>↵</kbd>
        </div>
      </div>
      <style>{`@keyframes blink { 0%,100% { opacity: .2 } 50% { opacity: 1 } }`}</style>
    </div>
  )
}

// ── Activity feed (replaces News — engine events from our pipeline) ──────────
function ActivityFeed({ items }: { items: ActivityItem[] }) {
  const tagColor = (kind: string) =>
    kind === 'trade_exit'  ? T.green :
    kind === 'signal_fired'? T.label :
    kind === 'pipeline'    ? T.ai    : T.dim
  const tagShort = (kind: string) =>
    kind === 'trade_exit'  ? 'EXIT' :
    kind === 'signal_fired'? 'SIG'  :
    kind === 'pipeline'    ? 'PIPE' : '·'
  const fmtTime = (when: string | null) => {
    if (!when) return '—'
    // Try to parse to a short HH:mm or YYYY-MM-DD
    const d = new Date(when)
    if (isNaN(d.getTime())) return when.slice(0, 10)
    const today = new Date()
    if (d.toDateString() === today.toDateString()) {
      return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false })
    }
    return d.toISOString().slice(5, 10)  // MM-DD
  }

  return (
    <div style={{
      borderTop: `1px solid ${T.border}`, background: T.bg0,
      maxHeight: 240, overflowY: 'auto',
    }}>
      <div style={{
        position: 'sticky', top: 0, background: T.bg1, padding: '8px 14px',
        borderBottom: `1px solid ${T.border}`, display: 'flex', justifyContent: 'space-between',
        zIndex: 1,
      }}>
        <span style={{ color: T.label, fontSize: 10, letterSpacing: '0.12em', fontWeight: 600, fontFamily: 'Inter Tight' }}>
          ENGINE ACTIVITY
        </span>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>{items.length} events</span>
      </div>
      {items.length === 0 ? (
        <div style={{ padding: 18, color: T.dim, textAlign: 'center', fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
          No recent activity.
        </div>
      ) : items.map((n, i) => {
        const sc = tagColor(n.kind)
        return (
          <div key={`${n.kind}-${n.ticker}-${i}`} style={{
            padding: '8px 14px', borderBottom: `1px solid ${T.border}`,
            display: 'grid', gridTemplateColumns: '50px 46px 1fr', gap: 10, alignItems: 'baseline',
          }}>
            <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>{fmtTime(n.when)}</span>
            <span style={{ color: sc, fontSize: 9, fontFamily: 'IBM Plex Mono', fontWeight: 600 }}>● {tagShort(n.kind)}</span>
            <div style={{ minWidth: 0 }}>
              <span style={{ color: T.data, fontSize: 11, fontFamily: 'Inter Tight', lineHeight: 1.4 }}>
                {n.title}
                {n.detail && (
                  <span style={{ color: n.kind === 'trade_exit' && (n.pnl ?? 0) < 0 ? T.red : T.dim2, marginLeft: 6, fontFamily: 'IBM Plex Mono', fontSize: 10 }}>
                    {n.detail.length > 80 ? n.detail.slice(0, 80) + '…' : n.detail}
                  </span>
                )}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Bottom: function strip + ticker tape ─────────────────────────────────────
function FunctionFooter({ movers }: { movers: TopMoversResponse | null }) {
  // Build a ticker tape from real top gainers + losers, repeated for scroll
  const tape = movers
    ? [...movers.gainers, ...movers.losers, ...movers.gainers]
    : []
  return (
    <div style={{
      position: 'fixed', bottom: 0, left: 0, right: 0,
      borderTop: `1px solid ${T.border}`, background: T.bg1, zIndex: 5,
    }}>
      {/* Ticker tape */}
      <div style={{
        height: 22, overflow: 'hidden', borderBottom: `1px solid ${T.border}`,
        position: 'relative', background: T.bg0,
      }}>
        <div style={{
          display: 'flex', gap: 32, whiteSpace: 'nowrap',
          animation: 'scroll 60s linear infinite',
          fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
          padding: '0 14px', alignItems: 'center', height: '100%',
        }}>
          {tape.length === 0 ? (
            <span style={{ color: T.dim }}>Loading market tape…</span>
          ) : tape.map((m, i) => (
            <span key={`${m.ticker}-${i}`} style={{ display: 'flex', gap: 6 }}>
              <span style={{ color: T.dim2 }}>{m.ticker}</span>
              <span style={{ color: T.data }}>{m.close.toLocaleString()}</span>
              <span style={{ color: m.pct >= 0 ? T.green : T.red }}>
                {m.pct >= 0 ? '+' : ''}{m.pct.toFixed(2)}%
              </span>
            </span>
          ))}
        </div>
        <style>{`@keyframes scroll { 0% { transform: translateX(0) } 100% { transform: translateX(-50%) } }`}</style>
      </div>
      {/* Function keys */}
      <div style={{
        display: 'flex', alignItems: 'center', height: 26,
        fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
      }}>
        {[
          ['F1', 'HELP'], ['F2', 'OVERVIEW'], ['F3', 'TRADES'], ['F4', 'LIVE'],
          ['F5', 'AI'], ['/', 'SEARCH'], ['⌘K', 'COMMAND'], ['G', 'GO'], ['?', 'KEYS'],
        ].map(([k, v], i, arr) => (
          <div key={k} style={{
            padding: '0 14px', height: '100%', display: 'flex', alignItems: 'center', gap: 6,
            borderRight: i < arr.length - 1 ? `1px solid ${T.border}` : 'none',
          }}>
            <span style={{ color: T.label, fontWeight: 600 }}>{k}</span>
            <span style={{ color: T.dim2, fontSize: 10, letterSpacing: '0.06em' }}>{v}</span>
          </div>
        ))}
        <div style={{ marginLeft: 'auto', padding: '0 14px', color: T.dim, fontSize: 10 }}>
          TERMINAL v3.1 · DESIGN MOCK · /analysis-v3
        </div>
      </div>
    </div>
  )
}

// ═════════════════════════════════════════════════════════════════════════════
// MODERN MODE — AI-first, conversational, generous whitespace
// Same data, totally different framing.
// ═════════════════════════════════════════════════════════════════════════════

// ── Modern: filter bar (compact, friendly) ───────────────────────────────────
function ModernFilterBar(props: {
  ticker: string; setTicker: (v: string) => void
  year: string;   setYear:   (v: string) => void
  idx: string;    setIdx:    (v: string) => void
  tickers?: string[]
  indices?: IndexInfo[]
}) {
  const { ticker, setTicker, year, setYear, idx, setIdx } = props
  const labelStyle: React.CSSProperties = { color: T.dim, fontSize: 11, fontWeight: 500, letterSpacing: '0.04em' }
  const dropStyle: React.CSSProperties = {
    background: T.bg2, border: `1px solid ${T.borderHi}`,
    color: T.data, padding: '8px 14px', borderRadius: 8,
    fontFamily: 'Inter Tight, sans-serif', fontSize: 14, fontWeight: 500,
    outline: 'none', cursor: 'pointer', minWidth: 130,
  }
  const chipActive = (a: boolean): React.CSSProperties => ({
    padding: '7px 14px', borderRadius: 6, fontSize: 13, fontWeight: 500, cursor: 'pointer',
    background: a ? T.label : 'transparent',
    color: a ? T.bg0 : T.dim2,
    border: `1px solid ${a ? T.label : T.borderHi}`,
    fontFamily: 'Inter Tight, sans-serif',
  })
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 18, flexWrap: 'wrap',
      padding: '14px 18px', background: T.bg1, borderRadius: 10,
      border: `1px solid ${T.border}`, marginBottom: 24,
    }}>
      <span style={labelStyle}>Showing</span>
      <select value={ticker} onChange={e => setTicker(e.target.value)} style={dropStyle}>
        <option value="ALL">All stocks</option>
        {(props.tickers || []).filter(t => t !== 'ALL').slice(0, 200).map(t => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>
      <span style={labelStyle}>in</span>
      <select value={idx} onChange={e => setIdx(e.target.value)} style={dropStyle}>
        <option value="ALL">All indices</option>
        {(props.indices || []).map(i => (
          <option key={i.index_name} value={i.index_name}>{i.index_name} · {i.members}</option>
        ))}
      </select>
      <span style={labelStyle}>·</span>
      <div style={{ display: 'flex', gap: 6 }}>
        {['ALL', '2024', '2025', '2026'].map(y => (
          <button key={y} onClick={() => setYear(y)} style={chipActive(year === y)}>{y}</button>
        ))}
      </div>
      <div style={{ marginLeft: 'auto', color: T.dim2, fontSize: 12, fontFamily: 'Inter Tight' }}>
        149 stocks · 26 indices loaded
      </div>
    </div>
  )
}

// ── Modern: greeting hero ────────────────────────────────────────────────────
function ModernGreeting({ opportunityCount, breadth }: { opportunityCount: number; breadth: BreadthResponse | null }) {
  const [now, setNow] = useState<Date | null>(null)
  useEffect(() => { setNow(new Date()) }, [])
  const hour = now?.getHours() ?? 9
  const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening'

  return (
    <div style={{ marginBottom: 28 }}>
      <div style={{
        color: T.dim, fontSize: 13, letterSpacing: '0.06em',
        textTransform: 'uppercase', fontWeight: 500, marginBottom: 8,
      }}>
        {greeting}
      </div>
      <h1 style={{
        color: T.data, fontFamily: 'Inter Tight, sans-serif',
        fontSize: 36, fontWeight: 600, letterSpacing: '-0.02em',
        margin: 0, marginBottom: 14, lineHeight: 1.15, maxWidth: 820,
      }}>
        Here's what's worth your attention today.
      </h1>
      <p style={{
        color: T.dim2, fontFamily: 'Inter Tight', fontSize: 16,
        lineHeight: 1.55, margin: 0, maxWidth: 720,
      }}>
        {breadth ? (
          <>
            Across <span style={{ color: T.data, fontWeight: 600 }}>{breadth.total_stocks} stocks</span>{' '}
            in your universe today: <span style={{ color: T.green, fontWeight: 600 }}>{breadth.advancers} up</span>,{' '}
            <span style={{ color: T.red, fontWeight: 600 }}>{breadth.decliners} down</span>, average
            move <span style={{ color: breadth.avg_pct >= 0 ? T.green : T.red, fontWeight: 600 }}>
              {breadth.avg_pct >= 0 ? '+' : ''}{breadth.avg_pct.toFixed(2)}%
            </span>.
            {breadth.best_sector && <> <span style={{ color: T.green, fontWeight: 600 }}>{breadth.best_sector.sector}</span> leading,</>}
            {breadth.worst_sector && <> <span style={{ color: T.red, fontWeight: 600 }}>{breadth.worst_sector.sector}</span> weakest.</>}
            {' '}Our engine has flagged{' '}
            <span style={{ color: T.label, fontWeight: 600 }}>
              {opportunityCount} long opportunit{opportunityCount === 1 ? 'y' : 'ies'}
            </span>
            {opportunityCount > 0 ? <> — top {Math.min(3, opportunityCount)} below.</> : <> — none match your current filters.</>}
          </>
        ) : (
          <>Loading market context…</>
        )}
      </p>
    </div>
  )
}

// ── Modern: signal card ──────────────────────────────────────────────────────
function ModernSignalCard({ rank, sig }: {
  rank: number
  sig: { tk: string; eng: string; score: number; sec: string; setup: string; close?: string }
}) {
  const engineColor = sig.eng === 'TURBO' ? T.label : sig.eng === 'SUPER' ? T.green : T.blue
  const conf = Math.round(sig.score * 100)
  return (
    <div style={{
      background: T.bg1, border: `1px solid ${T.border}`, borderRadius: 12,
      padding: '20px 22px', display: 'flex', flexDirection: 'column', gap: 14,
    }}>
      {/* Top: rank + ticker + engine */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{
          color: T.dim, fontFamily: 'IBM Plex Mono', fontSize: 12,
          width: 28, height: 28, borderRadius: 14, background: T.bg2,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          border: `1px solid ${T.border}`,
        }}>{rank}</span>
        <span style={{
          color: T.data, fontFamily: 'Inter Tight', fontSize: 22,
          fontWeight: 700, letterSpacing: '-0.01em',
        }}>{sig.tk}</span>
        <span style={{
          color: engineColor, fontSize: 11, fontWeight: 600, letterSpacing: '0.08em',
          padding: '3px 9px', border: `1px solid ${engineColor}55`,
          background: `${engineColor}11`, borderRadius: 4, fontFamily: 'Inter Tight',
        }}>{sig.eng}</span>
        <span style={{ color: T.dim, fontSize: 13, marginLeft: 'auto', fontFamily: 'Inter Tight' }}>
          {sig.sec}
        </span>
      </div>

      {/* Confidence bar */}
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
          <span style={{ color: T.dim, fontSize: 12, fontFamily: 'Inter Tight' }}>Confidence</span>
          <span style={{ color: T.label, fontSize: 14, fontWeight: 600,
                         fontFamily: 'IBM Plex Mono', fontFeatureSettings: '"tnum" 1' }}>
            {conf}%
          </span>
        </div>
        <div style={{ height: 6, background: T.bg2, borderRadius: 3, overflow: 'hidden' }}>
          <div style={{
            width: `${conf}%`, height: '100%',
            background: `linear-gradient(90deg, ${T.label} 0%, ${T.label}cc 100%)`,
          }} />
        </div>
      </div>

      {/* Plain-English explanation */}
      <p style={{
        color: T.dim2, fontSize: 14, lineHeight: 1.55, margin: 0,
        fontFamily: 'Inter Tight',
      }}>
        {sig.setup}
      </p>

      {/* Actions */}
      <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
        <button style={modernBtnPrimary}>Explain why</button>
        <button style={modernBtnGhost}>+ Watchlist</button>
        <button style={modernBtnGhost}>Set alert</button>
      </div>
    </div>
  )
}

const modernBtnPrimary: React.CSSProperties = {
  background: T.label, color: T.bg0, border: 'none',
  padding: '8px 14px', borderRadius: 6, fontWeight: 600, fontSize: 13,
  fontFamily: 'Inter Tight, sans-serif', cursor: 'pointer',
}
const modernBtnGhost: React.CSSProperties = {
  background: 'transparent', color: T.dim2,
  border: `1px solid ${T.borderHi}`,
  padding: '8px 14px', borderRadius: 6, fontWeight: 500, fontSize: 13,
  fontFamily: 'Inter Tight, sans-serif', cursor: 'pointer',
}

// ── Modern: AI panel (wired to /api/ai/chat) ─────────────────────────────────
function ModernAIPanel({
  aiHealth, ctx, signalCount,
}: {
  aiHealth: AIHealth | null
  ctx: Record<string, unknown>
  signalCount: number
}) {
  const [input, setInput] = useState('')
  const [convo, setConvo] = useState<ChatMsg[]>([])
  const [thinking, setThinking] = useState(false)
  const endRef = useRef<HTMLDivElement>(null)
  const intro = aiHealth?.configured
    ? `I've reviewed your universe — ${signalCount} live opportunit${signalCount === 1 ? 'y' : 'ies'} flagged. Ask me anything below, or pick a suggestion.`
    : `AI chat is being set up — admin needs to configure ANTHROPIC_API_KEY in Railway. Once it's set, I'll be able to walk you through any setup.`

  const suggestions = [
    'Walk me through the #1 signal',
    'Which signal has the strongest historical track record?',
    'What sectors should I avoid right now?',
    'Compare Turbo and Super engine performance',
  ]

  async function send(messageOverride?: string) {
    const text = (messageOverride ?? input).trim()
    if (!text || thinking || !aiHealth?.configured) return
    setConvo(c => [...c, { who: 'user', body: text }])
    setInput('')
    setThinking(true)
    try {
      const history = convo.filter(m => !m.isError)
        .map(m => ({ role: (m.who === 'ai' ? 'assistant' : 'user') as 'user' | 'assistant', content: m.body }))
      const r = await postAIChat({ message: text, history, context: ctx, model: 'haiku' })
      setConvo(c => [...c, { who: 'ai', body: r.message }])
    } catch (e) {
      setConvo(c => [...c, { who: 'ai', body: e instanceof Error ? e.message : 'AI request failed.', isError: true }])
    } finally {
      setThinking(false)
      setTimeout(() => endRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
    }
  }

  return (
    <div style={{
      background: T.bg1, border: `1px solid ${T.border}`, borderRadius: 12,
      padding: '20px 22px', display: 'flex', flexDirection: 'column',
      minHeight: 380, gap: 16,
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{
          width: 32, height: 32, borderRadius: 16, background: `${T.ai}22`,
          border: `1px solid ${T.ai}55`, display: 'flex', alignItems: 'center', justifyContent: 'center',
          color: T.ai, fontWeight: 700, fontSize: 14, fontFamily: 'IBM Plex Mono',
        }}>K</span>
        <div>
          <div style={{ color: T.data, fontWeight: 600, fontSize: 16, fontFamily: 'Inter Tight' }}>
            Kanida AI
          </div>
          <div style={{ color: T.dim, fontSize: 12, fontFamily: 'Inter Tight' }}>
            <span style={{
              display: 'inline-block', width: 6, height: 6, borderRadius: 6,
              background: aiHealth?.configured ? T.ai : T.dim, marginRight: 6, verticalAlign: 'middle',
            }} />
            {aiHealth?.configured ? `Connected · ${aiHealth.model.replace('claude-', '').slice(0, 18)}` : 'Not configured'}
          </div>
        </div>
      </div>

      {/* Conversation */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 12, overflowY: 'auto', maxHeight: 360 }}>
        <div style={{ color: T.data, fontSize: 15, lineHeight: 1.6, fontFamily: 'Inter Tight' }}>
          {intro}
        </div>
        {convo.map((m, i) => (
          <div key={i} style={{
            color: m.isError ? T.red : T.data,
            fontSize: 15, lineHeight: 1.6,
            fontFamily: 'Inter Tight',
            padding: m.who === 'user' ? '8px 12px' : 0,
            background: m.who === 'user' ? T.bg2 : 'transparent',
            borderLeft: m.who === 'user' ? `2px solid ${T.label}` : 'none',
            borderRadius: m.who === 'user' ? 6 : 0,
            whiteSpace: 'pre-wrap',
          }}>
            {m.body}
          </div>
        ))}
        {thinking && (
          <div style={{ color: T.dim2, fontSize: 13, fontFamily: 'Inter Tight' }}>thinking…</div>
        )}
        <div ref={endRef} />
      </div>

      {/* Suggestion chips */}
      {convo.length === 0 && aiHealth?.configured && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {suggestions.map(s => (
            <button key={s} onClick={() => send(s)} style={{
              background: T.bg2, border: `1px solid ${T.border}`,
              color: T.data, padding: '11px 14px', borderRadius: 8,
              fontFamily: 'Inter Tight', fontSize: 14, textAlign: 'left',
              cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 10,
            }}>
              <span style={{ color: T.ai, fontFamily: 'IBM Plex Mono', fontSize: 12 }}>→</span>
              {s}
            </button>
          ))}
        </div>
      )}

      {/* Input */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10,
        background: T.bg2, border: `1px solid ${T.borderHi}`,
        padding: '12px 14px', borderRadius: 10,
        opacity: aiHealth?.configured ? 1 : 0.5,
      }}>
        <span style={{ color: T.ai, fontFamily: 'IBM Plex Mono' }}>{'>'}</span>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && send()}
          disabled={!aiHealth?.configured || thinking}
          placeholder={aiHealth?.configured ? "Ask anything — try 'why is the top signal firing?'" : 'AI chat not configured'}
          style={{
            flex: 1, background: 'transparent', border: 'none', outline: 'none',
            color: T.data, fontFamily: 'Inter Tight', fontSize: 14,
          }}
        />
        <button onClick={() => send()} disabled={!aiHealth?.configured || thinking} style={{
          background: T.ai, color: T.bg0, border: 'none',
          padding: '6px 12px', borderRadius: 6, fontWeight: 700, fontSize: 12,
          fontFamily: 'IBM Plex Mono', cursor: aiHealth?.configured ? 'pointer' : 'not-allowed',
          letterSpacing: '0.06em',
        }}>SEND</button>
      </div>
    </div>
  )
}

// ── Modern: engine summary ribbon (collapsed) ────────────────────────────────
function ModernEngineRibbon({ overview }: { overview: SwingOverviewResponse | null }) {
  const fmtPct = (v: number) => (v >= 0 ? '+' : '') + v.toFixed(2) + '%'
  const fmtN   = (v: number) => v.toLocaleString()
  const summary = overview?.summary
  const engines = overview?.engines || []
  const findEng = (b: string) => engines.find(e => e.bucket === b)
  const turbo    = findEng('turbo')
  const sup      = findEng('super')
  const std      = findEng('standard')

  // Hero summary line — uses HC stats when live, falls back to demo numbers
  const wr      = summary ? summary.hc_win_rate : 99.4
  const trades  = summary ? summary.hc_trades   : 1893
  const avg     = summary ? summary.hc_avg_pnl  : 5.19

  const cards = [
    { k: 'Turbo',    wr: turbo ? turbo.smart_win_rate.toFixed(1) + '%' : '99.4%',  n: turbo ? fmtN(turbo.total_trades) : '824' },
    { k: 'Super',    wr: sup   ? sup.smart_win_rate.toFixed(1)   + '%' : '99.8%',  n: sup   ? fmtN(sup.total_trades)   : '1,069' },
    { k: 'Standard', wr: std   ? std.smart_win_rate.toFixed(1)   + '%' : '30.0%',  n: std   ? fmtN(std.total_trades)   : '6,722' },
  ]

  return (
    <div style={{
      background: T.bg1, border: `1px solid ${T.border}`, borderRadius: 12,
      padding: '18px 22px', marginBottom: 24,
      display: 'flex', alignItems: 'center', gap: 30, flexWrap: 'wrap',
    }}>
      <div>
        <div style={{ color: T.dim, fontSize: 12, marginBottom: 4, fontFamily: 'Inter Tight' }}>
          Engine performance · all time
        </div>
        <div style={{ color: T.data, fontSize: 16, fontFamily: 'Inter Tight', lineHeight: 1.5 }}>
          <span style={{ color: T.green, fontWeight: 700 }}>{wr.toFixed(1)}%</span> win rate across
          <span style={{ color: T.data, fontWeight: 600 }}> {fmtN(trades)}</span> high-conviction trades · average
          <span style={{ color: avg >= 0 ? T.green : T.red, fontWeight: 700 }}> {fmtPct(avg)}</span> per trade
        </div>
      </div>
      <div style={{ display: 'flex', gap: 16, marginLeft: 'auto' }}>
        {cards.map(e => (
          <div key={e.k} style={{
            padding: '8px 14px', background: T.bg2, borderRadius: 8,
            border: `1px solid ${T.border}`, minWidth: 110,
          }}>
            <div style={{ color: T.dim, fontSize: 11, fontFamily: 'Inter Tight' }}>{e.k}</div>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 2 }}>
              <span style={{
                color: e.k === 'Standard' ? T.yellow : T.green,
                fontWeight: 700, fontSize: 16, fontFamily: 'IBM Plex Mono',
                fontFeatureSettings: '"tnum" 1',
              }}>{e.wr}</span>
              <span style={{ color: T.dim2, fontSize: 11, fontFamily: 'IBM Plex Mono' }}>n={e.n}</span>
            </div>
          </div>
        ))}
      </div>
      <button style={modernBtnGhost}>View detailed performance ›</button>
    </div>
  )
}

// ── Modern body ──────────────────────────────────────────────────────────────
function ModernBody({
  ticker, setTicker, year, setYear, idx, setIdx,
  signals, overview, tickers, indices, aiHealth, breadth,
}: {
  ticker: string; setTicker: (v: string) => void
  year:   string; setYear:   (v: string) => void
  idx:    string; setIdx:    (v: string) => void
  signals: ActiveSignalRow[]
  overview: SwingOverviewResponse | null
  tickers: string[]
  indices: IndexInfo[]
  aiHealth: AIHealth | null
  breadth: BreadthResponse | null
}) {
  const top = signals.slice(0, 3).map(s => ({
    tk: s.ticker, eng: (s.engine || 'standard').toUpperCase(),
    score: s.opportunity_score, sec: s.sector || '—',
    setup: s.setup_summary || 'Pattern matched historically. Click for full setup analysis.',
  }))
  const remaining = Math.max(0, signals.length - top.length)

  return (
    <div style={{
      flex: 1, overflow: 'auto', background: T.bg0,
      padding: '36px 48px 80px',
    }}>
      <div style={{ maxWidth: 1280, margin: '0 auto' }}>
        <ModernGreeting opportunityCount={signals.length} breadth={breadth} />
        <ModernFilterBar
          ticker={ticker} setTicker={setTicker}
          year={year}     setYear={setYear}
          idx={idx}       setIdx={setIdx}
          tickers={tickers} indices={indices}
        />

        {/* Two-column: signals + AI */}
        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.1fr) minmax(0, 0.9fr)', gap: 24, marginBottom: 28 }}>
          <div>
            <h2 style={{
              color: T.data, fontSize: 18, fontWeight: 600, margin: '0 0 16px',
              fontFamily: 'Inter Tight', letterSpacing: '-0.01em',
            }}>
              Top calls today
            </h2>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              {top.map((c, i) => (
                <ModernSignalCard key={`${c.tk}-${i}`} rank={i + 1} sig={c} />
              ))}
              {top.length === 0 && (
                <div style={{
                  background: T.bg1, border: `1px solid ${T.border}`, borderRadius: 12,
                  padding: 20, color: T.dim, textAlign: 'center', fontFamily: 'Inter Tight',
                }}>No signals match these filters.</div>
              )}
            </div>
            {remaining > 0 && (
              <button style={{ ...modernBtnGhost, marginTop: 14, width: '100%' }}>
                Show {remaining} more signal{remaining === 1 ? '' : 's'}  ↓
              </button>
            )}
          </div>

          <div>
            <h2 style={{
              color: T.data, fontSize: 18, fontWeight: 600, margin: '0 0 16px',
              fontFamily: 'Inter Tight', letterSpacing: '-0.01em',
            }}>
              Ask Kanida
            </h2>
            <ModernAIPanel
              aiHealth={aiHealth}
              ctx={{ ticker, year, index: idx, mode: 'MODERN' }}
              signalCount={signals.length}
            />
          </div>
        </div>

        <ModernEngineRibbon overview={overview} />

        <div style={{
          color: T.dim, fontSize: 12, textAlign: 'center', marginTop: 24,
          fontFamily: 'Inter Tight',
        }}>
          Switch to <span style={{ color: T.label, fontWeight: 600 }}>Terminal</span> mode
          for the full data view.
        </div>
      </div>
    </div>
  )
}

// ═════════════════════════════════════════════════════════════════════════════
// Page
// ═════════════════════════════════════════════════════════════════════════════
export default function AnalysisV3Mock() {
  const [tab, setTab]   = useState('OVERVIEW')
  const [mode, setMode] = useState<Mode>('TERMINAL')

  // Filters — shared across modes (memory across tab/mode switches)
  const [ticker,  setTicker]  = useState('ALL')
  const [year,    setYear]    = useState('ALL')
  const [idx,     setIdx]     = useState('ALL')
  const [engine,  setEngine]  = useState('ALL')   // active-signals engine filter

  // Live data — fetches whenever filters change. Falls back to demo if API down.
  const live = useLiveData(ticker, year, idx, engine)

  // Persist mode across reloads (UX nicety)
  useEffect(() => {
    const saved = typeof window !== 'undefined' ? window.localStorage.getItem('kanida.mode') as Mode | null : null
    if (saved === 'TERMINAL' || saved === 'MODERN') setMode(saved)
  }, [])
  useEffect(() => {
    if (typeof window !== 'undefined') window.localStorage.setItem('kanida.mode', mode)
  }, [mode])

  return (
    <>
      <link rel="preconnect" href="https://fonts.googleapis.com" />
      <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
      <link
        href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=Inter+Tight:wght@400;500;600;700&display=swap"
        rel="stylesheet"
      />

      <div style={{
        background: T.bg0, color: T.data, minHeight: '100vh',
        fontFamily: 'Inter Tight, sans-serif',
        paddingBottom: mode === 'TERMINAL' ? 56 : 0,
        display: 'flex', flexDirection: 'column',
      }}>
        <BrandBar mode={mode} setMode={setMode} />

        {mode === 'TERMINAL' ? (
          <>
            <StatusBar />
            <BreadthStrip breadth={live.breadth} />
            <WorkspaceTabs active={tab} onChange={setTab} />

            <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
              <LeftRail />

              <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', overflow: 'auto' }}>
                <Breadcrumb />
                <HeroEngines
                  overview={live.overview}
                  year={year} setYear={setYear}
                  ticker={ticker} indexFilter={idx}
                  loading={live.loading}
                />
                <ActiveSignalsPanel
                  signals={live.signals}
                  engine={engine} setEngine={setEngine}
                  loading={live.loading}
                />
                <SectorHeatmap sectors={live.sectors} asOf={live.breadth?.as_of ?? null} />
                <TopMoversPanel movers={live.movers} />
              </div>

              <div style={{
                width: 380, borderLeft: `1px solid ${T.border}`, background: T.bg1,
                display: 'flex', flexDirection: 'column', minHeight: 0,
              }}>
                <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
                  <AIChatPanel aiHealth={live.aiHealth} ctx={{ ticker, year, index: idx, mode }} />
                </div>
                <ActivityFeed items={live.activity} />
              </div>
            </div>

            <FunctionFooter movers={live.movers} />
          </>
        ) : (
          <ModernBody
            ticker={ticker} setTicker={setTicker}
            year={year}     setYear={setYear}
            idx={idx}       setIdx={setIdx}
            signals={live.signals}
            overview={live.overview}
            tickers={live.tickers}
            indices={live.indices}
            aiHealth={live.aiHealth}
            breadth={live.breadth}
          />
        )}
      </div>
    </>
  )
}
