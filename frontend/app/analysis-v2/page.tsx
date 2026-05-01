'use client'

/**
 * KANIDA TERMINAL — v2 design mock
 * ---------------------------------
 * Modern terminal aesthetic (Path B): Linear/ASKB density + Bloomberg discipline,
 * minus the 1980s baggage. Pure black, IBM Plex Mono for all numerics,
 * Inter Tight for labels, single amber accent, no emojis, no gradients,
 * 1px borders only. Static numbers — this is a visual mock.
 *
 * Live at: /analysis-v2  (kanida.ai/analysis-v2 once deployed)
 */

import React, { useState, useEffect } from 'react'

// ── Design tokens ─────────────────────────────────────────────────────────────
const T = {
  bg0:      '#000000',
  bg1:      '#0a0a0c',
  bg2:      '#111114',
  bg3:      '#1a1a1f',
  border:   '#1c1c22',
  borderHi: '#2a2a32',
  label:    '#f59e0b',   // amber — labels, accents, the "signature" color
  data:     '#f5f5f7',   // white — numbers
  dim:      '#6b7280',   // grey — chrome, helper text
  dim2:     '#9ca3af',
  green:    '#22c55e',
  red:      '#ef4444',
  blue:     '#60a5fa',   // links, ticker codes
  yellow:   '#fde047',   // change indicator
  violet:   '#a78bfa',   // sparingly
}

// ── Static mock data (real shape, real-feeling numbers) ───────────────────────
const HERO = {
  scope: 'TURBO + SUPER  · ALL YEARS · ALL STOCKS',
  metrics: [
    { k: 'WR',         v: '99.63',  unit: '%',  delta: null,        accent: T.data },
    { k: 'AVG P&L',    v: '+5.19',  unit: '%',  delta: '+0.40 vs prior yr', accent: T.green },
    { k: 'CUM P&L',    v: '+9,828.66', unit: '%', delta: null,      accent: T.green },
    { k: 'TRADES',     v: '1,893',  unit: '',   delta: null,        accent: T.data },
    { k: 'HOLD',       v: '3.6',    unit: 'd',  delta: null,        accent: T.data },
  ],
}

const ENGINES = [
  { name: 'TURBO',    n: 824,  wr: 99.39, avg: 5.07, hold: 1.8, p90: '+5.12', p180: '+5.04', sig: 'high momentum, 1-3d resolution' },
  { name: 'SUPER',    n: 1069, wr: 99.81, avg: 5.28, hold: 2.4, p90: '+5.41', p180: '+5.21', sig: 'trend continuation' },
  { name: 'STANDARD', n: 6722, wr: 30.04, avg: 0.21, hold: 8.1, p90: '+0.38', p180: '+0.29', sig: 'high volume, selective entry' },
]

const ACTIVE = [
  { tk: 'ADANIENT',  eng: 'TURBO',    score: 0.943, cred: 'thin',     sec: 'Conglomerate', d: '2026-04-28', setup: 'breakout above 60-day range with volume divergence' },
  { tk: 'POWERGRID', eng: 'TURBO',    score: 0.917, cred: 'exploratory', sec: 'Power',     d: '2026-04-28', setup: 'short-term slope turning up inside contracting range' },
  { tk: 'BPCL',      eng: 'SUPER',    score: 0.878, cred: 'exploratory', sec: 'Energy',    d: '2026-04-24', setup: 'rejection wick at lower boundary, vol rising' },
  { tk: 'NTPC',      eng: 'SUPER',    score: 0.852, cred: 'thin',      sec: 'Power',      d: '2026-04-25', setup: 'multi-timeframe alignment, MA position favorable' },
  { tk: 'ZOMATO',    eng: 'SUPER',    score: 0.831, cred: 'exploratory', sec: 'Internet', d: '2026-04-24', setup: 'flat to up shift after 14-day base' },
  { tk: 'AUBANK',    eng: 'STANDARD', score: 0.741, cred: 'thin',      sec: 'Banks',      d: '2026-04-23', setup: 'inside-range reversal pattern' },
  { tk: 'COFORGE',   eng: 'STANDARD', score: 0.722, cred: 'thin',      sec: 'IT',         d: '2026-04-23', setup: 'volume confirmation on break of 20-day high' },
  { tk: 'TATAMOTORS','eng':'STANDARD', score: 0.685, cred: 'exploratory', sec: 'Auto',    d: '2026-04-22', setup: 'inside-range bias up, watch 9:15 open' },
  { tk: 'DRREDDY',   eng: 'STANDARD', score: 0.671, cred: 'exploratory', sec: 'Pharma',   d: '2026-04-22', setup: 'flat-to-up shift in tight range' },
] as { tk: string; eng: string; score: number; cred: string; sec: string; d: string; setup: string }[]

// ── Tiny sparkline ────────────────────────────────────────────────────────────
function Spark({ pts, color }: { pts: number[]; color: string }) {
  const w = 96, h = 22
  const min = Math.min(...pts), max = Math.max(...pts)
  const range = max - min || 1
  const path = pts.map((v, i) => {
    const x = (i / (pts.length - 1)) * w
    const y = h - ((v - min) / range) * h
    return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return (
    <svg width={w} height={h} style={{ display: 'block' }}>
      <path d={path} fill="none" stroke={color} strokeWidth={1.25} strokeLinecap="round" />
    </svg>
  )
}

// ── Status bar (always-on) ────────────────────────────────────────────────────
function StatusBar() {
  const [now, setNow] = useState<Date | null>(null)
  useEffect(() => {
    setNow(new Date())
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  const timeStr = now
    ? now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false })
    : '--:--:--'

  const cells = [
    { label: 'IST',        value: timeStr,                color: T.data },
    { label: 'MKT',        value: 'CLOSED · NSE',         color: T.dim2 },
    { label: 'PIPE',       value: 'OK · 16:05 IST',       color: T.green },
    { label: 'TOKEN',      value: 'KITE · VALID',         color: T.green },
    { label: 'UNIVERSE',   value: '149 active',           color: T.data },
    { label: 'INDICES',    value: '26 loaded',            color: T.data },
    { label: 'BUILD',      value: 'v3.1.0-mock',          color: T.dim2 },
  ]
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 0,
      borderBottom: `1px solid ${T.border}`, background: T.bg1,
      fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, height: 28,
    }}>
      <div style={{
        padding: '0 14px', height: '100%', display: 'flex', alignItems: 'center',
        background: T.label, color: T.bg0, fontWeight: 700, letterSpacing: '0.08em',
        fontFamily: 'Inter Tight, sans-serif',
      }}>KANIDA</div>
      {cells.map((c, i) => (
        <div key={c.label} style={{
          padding: '0 14px', height: '100%', display: 'flex', alignItems: 'center', gap: 8,
          borderRight: i < cells.length - 1 ? `1px solid ${T.border}` : 'none',
        }}>
          <span style={{ color: T.dim, fontSize: 10, letterSpacing: '0.05em' }}>{c.label}</span>
          <span style={{ color: c.color, fontWeight: 500 }}>{c.value}</span>
        </div>
      ))}
      <div style={{ marginLeft: 'auto', padding: '0 14px', color: T.dim, fontSize: 10 }}>
        ⌘K  command
      </div>
    </div>
  )
}

// ── Numbered action strip (Bloomberg-style) ───────────────────────────────────
const ACTIONS = ['OVERVIEW', 'TRADES', 'COMBINATIONS', 'MPI', 'LIVE', 'EXEC IQ', 'STRATEGY LAB', 'ADMIN']
function ActionStrip({ active, onChange }: { active: number; onChange: (i: number) => void }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'stretch',
      borderBottom: `1px solid ${T.border}`, background: T.bg0,
      fontFamily: 'Inter Tight, sans-serif', fontSize: 11, height: 36,
    }}>
      {ACTIONS.map((a, i) => {
        const isActive = active === i
        return (
          <button
            key={a}
            onClick={() => onChange(i)}
            style={{
              padding: '0 18px', display: 'flex', alignItems: 'center', gap: 8,
              background: isActive ? T.bg2 : 'transparent',
              borderRight: `1px solid ${T.border}`, border: 'none',
              borderTop: isActive ? `1px solid ${T.label}` : '1px solid transparent',
              color: isActive ? T.data : T.dim2, cursor: 'pointer',
              fontWeight: 500, letterSpacing: '0.04em',
            }}
          >
            <span style={{ color: T.label, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{i + 1}</span>
            <span>{a}</span>
          </button>
        )
      })}
      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', padding: '0 14px', color: T.dim, fontSize: 10 }}>
        type ticker  ·  press <kbd style={kbdStyle}>/</kbd> to search
      </div>
    </div>
  )
}

const kbdStyle: React.CSSProperties = {
  fontFamily: 'IBM Plex Mono, monospace', fontSize: 10,
  border: `1px solid ${T.borderHi}`, borderRadius: 2,
  padding: '1px 4px', background: T.bg2, color: T.dim2, marginLeft: 4,
}

// ── Filter bar (per-tab) ──────────────────────────────────────────────────────
function FilterBar() {
  const [stock, setStock] = useState('ALL')
  const [year, setYear]   = useState('ALL')
  const [idx, setIdx]     = useState('ALL')
  const cellBox: React.CSSProperties = {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '0 16px', borderRight: `1px solid ${T.border}`, height: '100%',
  }
  const labelStyle: React.CSSProperties = { color: T.dim, fontSize: 10, letterSpacing: '0.06em' }
  const inputStyle: React.CSSProperties = {
    background: 'transparent', border: 'none', outline: 'none',
    color: stock === 'ALL' ? T.dim2 : T.label,
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 12, width: 110, fontWeight: 500,
  }
  return (
    <div style={{
      display: 'flex', alignItems: 'center', height: 36,
      borderBottom: `1px solid ${T.border}`, background: T.bg1,
    }}>
      <div style={cellBox}>
        <span style={labelStyle}>STOCK</span>
        <input value={stock} onChange={e => setStock(e.target.value.toUpperCase())} style={inputStyle} placeholder="ALL" />
      </div>
      <div style={cellBox}>
        <span style={labelStyle}>YEAR</span>
        {['ALL', '2024', '2025', '2026'].map(y => (
          <button key={y} onClick={() => setYear(y)} style={{
            ...chipStyle, color: year === y ? T.label : T.dim2,
            borderColor: year === y ? T.label : T.border,
          }}>{y}</button>
        ))}
      </div>
      <div style={cellBox}>
        <span style={labelStyle}>INDEX</span>
        <select value={idx} onChange={e => setIdx(e.target.value)} style={{
          background: T.bg2, border: `1px solid ${T.border}`, color: idx === 'ALL' ? T.dim2 : T.label,
          fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, padding: '4px 8px', outline: 'none',
        }}>
          <option>ALL</option>
          <option>NIFTY 50</option>
          <option>NIFTY 100</option>
          <option>NIFTY 500</option>
          <option>NIFTY MIDCAP 150</option>
          <option>NIFTY BANK</option>
        </select>
      </div>
      <div style={{ marginLeft: 'auto', padding: '0 16px', color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono, monospace' }}>
        {stock === 'ALL' && year === 'ALL' && idx === 'ALL' ? 'no filters' : 'filtered scope'}
      </div>
    </div>
  )
}

const chipStyle: React.CSSProperties = {
  fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, fontWeight: 500,
  padding: '3px 8px', background: 'transparent',
  border: '1px solid', cursor: 'pointer', letterSpacing: '0.04em',
}

// ── Hero panel ────────────────────────────────────────────────────────────────
function HeroPanel() {
  return (
    <div style={{
      borderBottom: `1px solid ${T.border}`, background: T.bg0,
      padding: '20px 18px',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 14 }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
          <span style={{ color: T.label, fontSize: 11, fontFamily: 'Inter Tight', fontWeight: 600, letterSpacing: '0.12em' }}>
            HIGH CONVICTION
          </span>
          <span style={{ color: T.dim, fontSize: 11, fontFamily: 'IBM Plex Mono', letterSpacing: '0.04em' }}>
            {HERO.scope}
          </span>
        </div>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>updated 16:05 IST · {new Date().toISOString().slice(0,10)}</span>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 1, background: T.border }}>
        {HERO.metrics.map(m => (
          <div key={m.k} style={{ background: T.bg0, padding: '14px 16px' }}>
            <div style={{ color: T.dim, fontSize: 10, letterSpacing: '0.08em', marginBottom: 6 }}>{m.k}</div>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 4 }}>
              <span style={{
                color: m.accent, fontSize: 28, fontWeight: 500,
                fontFamily: 'IBM Plex Mono', fontFeatureSettings: '"tnum" 1', letterSpacing: '-0.02em',
              }}>{m.v}</span>
              {m.unit && <span style={{ color: T.dim2, fontSize: 14, fontFamily: 'IBM Plex Mono' }}>{m.unit}</span>}
            </div>
            {m.delta && (
              <div style={{ color: T.dim, fontSize: 10, marginTop: 4, fontFamily: 'IBM Plex Mono' }}>{m.delta}</div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Engine grid ───────────────────────────────────────────────────────────────
function EnginePanels() {
  const sparkSeries: Record<string, number[]> = {
    TURBO:    [4.8, 5.1, 4.9, 5.3, 5.0, 5.2, 5.07],
    SUPER:    [5.0, 5.1, 5.4, 5.2, 5.5, 5.3, 5.28],
    STANDARD: [0.1, 0.3, 0.2, 0.4, 0.0, 0.3, 0.21],
  }
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1, background: T.border, borderBottom: `1px solid ${T.border}` }}>
      {ENGINES.map(e => {
        const wrColor = e.wr >= 90 ? T.green : e.wr >= 50 ? T.yellow : T.red
        const avgColor = e.avg >= 0 ? T.green : T.red
        return (
          <div key={e.name} style={{ background: T.bg0, padding: '18px 18px 16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 14 }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                <span style={{ color: T.label, fontFamily: 'IBM Plex Mono', fontSize: 10 }}>[{e.name[0]}]</span>
                <span style={{ color: T.data, fontFamily: 'Inter Tight', fontWeight: 600, fontSize: 13, letterSpacing: '0.06em' }}>{e.name}</span>
              </div>
              <span style={{ color: T.dim, fontFamily: 'IBM Plex Mono', fontSize: 10 }}>n={e.n.toLocaleString()}</span>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 14 }}>
              {[
                { k: 'WR',       v: e.wr.toFixed(2), u: '%', c: wrColor },
                { k: 'AVG',      v: (e.avg >= 0 ? '+' : '') + e.avg.toFixed(2), u: '%', c: avgColor },
                { k: 'HOLD',     v: e.hold.toFixed(1), u: 'd', c: T.data },
              ].map(s => (
                <div key={s.k}>
                  <div style={{ color: T.dim, fontSize: 9, letterSpacing: '0.08em' }}>{s.k}</div>
                  <div style={{ color: s.c, fontFamily: 'IBM Plex Mono', fontSize: 18, fontWeight: 500, marginTop: 3, letterSpacing: '-0.01em' }}>
                    {s.v}<span style={{ color: T.dim2, fontSize: 11, marginLeft: 1 }}>{s.u}</span>
                  </div>
                </div>
              ))}
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: 12 }}>
              <div style={{ fontFamily: 'IBM Plex Mono', fontSize: 10, color: T.dim }}>
                <div>90D  <span style={{ color: T.green }}>{e.p90}%</span></div>
                <div>180D <span style={{ color: T.green }}>{e.p180}%</span></div>
              </div>
              <Spark pts={sparkSeries[e.name]} color={avgColor} />
            </div>

            <div style={{ borderTop: `1px solid ${T.border}`, paddingTop: 10, color: T.dim2, fontSize: 11, fontFamily: 'Inter Tight' }}>
              {e.sig}
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Active signals (ASKB-style table) ─────────────────────────────────────────
function ActiveSignalsPanel() {
  const [engine, setEngine] = useState('ALL')
  const filtered = engine === 'ALL' ? ACTIVE : ACTIVE.filter(a => a.eng === engine)

  return (
    <div style={{ borderTop: `1px solid ${T.border}`, background: T.bg0 }}>
      {/* Sub-header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '14px 18px 10px', borderBottom: `1px solid ${T.border}` }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
          <span style={{ color: T.label, fontSize: 11, fontFamily: 'Inter Tight', fontWeight: 600, letterSpacing: '0.12em' }}>
            ACTIVE SIGNALS
          </span>
          <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
            live opportunities  ·  rally only  ·  {filtered.length} of {ACTIVE.length}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {['ALL', 'TURBO', 'SUPER', 'STANDARD'].map(e => (
            <button key={e} onClick={() => setEngine(e)} style={{
              ...chipStyle,
              color: engine === e ? T.label : T.dim2,
              borderColor: engine === e ? T.label : T.border,
            }}>{e}</button>
          ))}
        </div>
      </div>
      {/* Table */}
      <div style={{ overflow: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, fontFamily: 'IBM Plex Mono, monospace' }}>
          <thead>
            <tr style={{ background: T.bg1, borderBottom: `1px solid ${T.border}` }}>
              {['#', 'TICKER', 'ENGINE', 'SCORE', 'CRED', 'SECTOR', 'DATE', 'SETUP'].map((h, i) => (
                <th key={h} style={{
                  padding: '8px 14px', textAlign: i >= 3 && i <= 4 ? 'right' : 'left',
                  color: T.dim, fontWeight: 500, fontSize: 10, letterSpacing: '0.08em',
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((a, i) => {
              const engColor = a.eng === 'TURBO' ? T.label : a.eng === 'SUPER' ? T.green : T.blue
              return (
                <tr key={a.tk} style={{ borderBottom: `1px solid ${T.border}`, background: i % 2 === 0 ? T.bg0 : T.bg1 }}>
                  <td style={{ padding: '8px 14px', color: T.dim, width: 30 }}>{i + 1}</td>
                  <td style={{ padding: '8px 14px' }}>
                    <span style={{ color: T.blue, fontWeight: 600, letterSpacing: '0.04em' }}>{a.tk}</span>
                    <span style={{ color: T.dim, marginLeft: 6, fontSize: 10 }}>NS Equity</span>
                  </td>
                  <td style={{ padding: '8px 14px', color: engColor, fontWeight: 500 }}>{a.eng}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: T.data, fontWeight: 500, fontFeatureSettings: '"tnum" 1' }}>
                    {a.score.toFixed(3)}
                  </td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: T.dim2, fontSize: 11 }}>{a.cred}</td>
                  <td style={{ padding: '8px 14px', color: T.dim2, fontSize: 11, fontFamily: 'Inter Tight' }}>{a.sec}</td>
                  <td style={{ padding: '8px 14px', color: T.dim }}>{a.d}</td>
                  <td style={{ padding: '8px 14px', color: T.dim2, fontSize: 11, fontFamily: 'Inter Tight', maxWidth: 380, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {a.setup}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Function key footer ───────────────────────────────────────────────────────
function FunctionFooter() {
  const keys = [
    ['F1',  'HELP'],
    ['F2',  'OVERVIEW'],
    ['F3',  'TRADES'],
    ['F4',  'LIVE'],
    ['/',   'SEARCH'],
    ['⌘K',  'COMMAND'],
    ['G',   'GO'],
    ['E',   'EXPORT'],
    ['?',   'KEYS'],
  ]
  return (
    <div style={{
      position: 'fixed', bottom: 0, left: 0, right: 0,
      borderTop: `1px solid ${T.border}`, background: T.bg1,
      display: 'flex', alignItems: 'center', height: 28,
      fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
    }}>
      {keys.map(([k, v], i) => (
        <div key={k} style={{
          padding: '0 14px', height: '100%', display: 'flex', alignItems: 'center', gap: 8,
          borderRight: i < keys.length - 1 ? `1px solid ${T.border}` : 'none',
        }}>
          <span style={{ color: T.label, fontWeight: 600 }}>{k}</span>
          <span style={{ color: T.dim2, fontSize: 10, letterSpacing: '0.06em' }}>{v}</span>
        </div>
      ))}
      <div style={{ marginLeft: 'auto', padding: '0 14px', color: T.dim, fontSize: 10 }}>
        TERMINAL v3.1 · DESIGN MOCK · /analysis-v2
      </div>
    </div>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AnalysisV2Mock() {
  const [tab, setTab] = useState(0)
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
        fontFamily: 'Inter Tight, sans-serif', paddingBottom: 40,
      }}>
        <StatusBar />
        <ActionStrip active={tab} onChange={setTab} />
        <FilterBar />
        <HeroPanel />
        <EnginePanels />
        <ActiveSignalsPanel />
        <FunctionFooter />
      </div>
    </>
  )
}
