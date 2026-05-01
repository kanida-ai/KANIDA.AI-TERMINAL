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

import React, { useEffect, useRef, useState } from 'react'

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

// ── Static data ───────────────────────────────────────────────────────────────
const MACRO = [
  { sym: 'NIFTY 50',     val: '24,328.95', chg: '+0.42%',  pos: true  },
  { sym: 'BANKNIFTY',    val: '52,108.30', chg: '-0.18%',  pos: false },
  { sym: 'USD/INR',      val: '83.42',     chg: '-0.06%',  pos: true  },
  { sym: 'BRENT',        val: '$84.12',    chg: '+1.30%',  pos: true  },
  { sym: 'US 10Y',       val: '4.21%',     chg: '+3 bps',  pos: false },
  { sym: 'INDIA VIX',    val: '13.84',     chg: '-2.10%',  pos: true  },
  { sym: 'GOLD',         val: '$2,318',    chg: '+0.80%',  pos: true  },
  { sym: 'BTC/USD',      val: '$67,420',   chg: '+1.20%',  pos: true  },
]

const WORKSPACES = [
  { k: 'MACRO',   label: 'Morning Macro' },
  { k: 'SECTOR',  label: 'Sector Lens' },
  { k: 'STOCK',   label: 'Stock Deep' },
  { k: 'RISK',    label: 'Risk Review' },
  { k: 'CUSTOM',  label: 'Custom' },
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

const SECTORS = [
  { name: 'BANKS',          chg: -0.42, n: 17 },
  { name: 'IT',             chg: +1.18, n: 11 },
  { name: 'PHARMA',         chg: +0.84, n: 14 },
  { name: 'AUTO',           chg: +2.10, n: 13 },
  { name: 'METALS',         chg: -1.30, n: 8  },
  { name: 'FMCG',           chg: +0.21, n: 13 },
  { name: 'ENERGY',         chg: -0.61, n: 10 },
  { name: 'NBFC',           chg: -0.18, n: 12 },
  { name: 'CAP GOODS',      chg: +0.92, n: 7  },
  { name: 'CONSUMER',       chg: +0.34, n: 9  },
  { name: 'TELECOM',        chg: +1.41, n: 4  },
  { name: 'INSURANCE',      chg: -0.05, n: 5  },
  { name: 'CHEMICALS',      chg: -0.71, n: 8  },
  { name: 'CEMENT',         chg: +0.62, n: 6  },
  { name: 'POWER',          chg: +1.83, n: 7  },
  { name: 'REAL ESTATE',    chg: +0.41, n: 4  },
]

const MOVERS = [
  { tk: 'POWERGRID', sec: 'Power',     chg: +4.21, vol: '12.4M', sig: 'TURBO' },
  { tk: 'TVSMOTOR',  sec: 'Auto',      chg: +3.84, vol: '6.1M',  sig: 'SUPER' },
  { tk: 'TATAPOWER', sec: 'Power',     chg: +2.91, vol: '18.2M', sig: 'TURBO' },
  { tk: 'EICHERMOT', sec: 'Auto',      chg: +2.42, vol: '0.9M',  sig: 'SUPER' },
  { tk: 'DRREDDY',   sec: 'Pharma',    chg: +2.18, vol: '1.4M',  sig: '—'     },
  { tk: 'JSWSTEEL',  sec: 'Metals',    chg: -1.82, vol: '8.7M',  sig: '—'     },
  { tk: 'TATASTEEL', sec: 'Metals',    chg: -1.71, vol: '14.3M', sig: '—'     },
  { tk: 'AXISBANK',  sec: 'Banks',     chg: -0.84, vol: '9.4M',  sig: '—'     },
]

const ENGINES = [
  { name: 'TURBO',    n: 824,  wr: 99.39, avg: 5.07, hold: 1.8 },
  { name: 'SUPER',    n: 1069, wr: 99.81, avg: 5.28, hold: 2.4 },
  { name: 'STANDARD', n: 6722, wr: 30.04, avg: 0.21, hold: 8.1 },
]

const ACTIVE = [
  { tk: 'ADANIENT',  eng: 'TURBO', score: 0.943, sec: 'Conglomerate' },
  { tk: 'POWERGRID', eng: 'TURBO', score: 0.917, sec: 'Power'        },
  { tk: 'BPCL',      eng: 'SUPER', score: 0.878, sec: 'Energy'       },
  { tk: 'NTPC',      eng: 'SUPER', score: 0.852, sec: 'Power'        },
  { tk: 'ZOMATO',    eng: 'SUPER', score: 0.831, sec: 'Internet'     },
]

const NEWS = [
  { t: '08:42', tag: 'EARN',  src: 'Bloomberg', headline: 'HDFC Bank Q4 NII beats by 3.2%, asset quality stable',  senti: 'pos', tk: 'HDFCBANK' },
  { t: '08:31', tag: 'POL',   src: 'Reuters',   headline: 'RBI keeps repo rate unchanged at 6.50%, MPC vote 5-1',    senti: 'neu', tk: null     },
  { t: '08:14', tag: 'M&A',   src: 'Mint',      headline: 'Tata Group in talks to acquire 26% stake in Vistara II',  senti: 'pos', tk: 'TATAMOTORS' },
  { t: '07:52', tag: 'GLOB',  src: 'WSJ',       headline: 'Brent crude tests $84 as OPEC+ extends voluntary cuts',   senti: 'neu', tk: null     },
  { t: '07:31', tag: 'TECH',  src: 'BS',        headline: 'Infosys announces $250M GenAI fund, partners with NVIDIA','senti': 'pos', tk: 'INFY'  },
]

const AI_CONVERSATION: { who: 'user' | 'ai'; body: string | React.ReactNode }[] = [
  { who: 'user', body: '/morning India' },
  {
    who: 'ai',
    body: (
      <>
        <div style={{ marginBottom: 8 }}>India open — three things:</div>
        <ol style={{ paddingLeft: 18, margin: 0, lineHeight: 1.6 }}>
          <li><span style={{ color: T.label }}>Power sector</span> leading +1.83% — POWERGRID and TATAPOWER both flagged TURBO this morning. Volume 2.1× 20-day avg.</li>
          <li><span style={{ color: T.red }}>Metals -1.30%</span> on China demand softness; JSWSTEEL and TATASTEEL fading on no fresh signals.</li>
          <li>RBI unchanged → bank stocks neutral; HDFC Q4 beat is doing the lifting alone.</li>
        </ol>
        <div style={{ marginTop: 10, color: T.dim2 }}>5 active signals, all rally. Worth a look: <span style={{ color: T.blue }}>ADANIENT</span> (0.943) and <span style={{ color: T.blue }}>POWERGRID</span> (0.917).</div>
      </>
    ),
  },
  { who: 'user', body: '/explain POWERGRID' },
  {
    who: 'ai',
    body: (
      <>
        POWERGRID — TURBO, score <span style={{ color: T.data, fontFamily: 'IBM Plex Mono' }}>0.917</span>:
        <ul style={{ margin: '6px 0 0 18px', padding: 0, lineHeight: 1.6 }}>
          <li>Short-term slope turning up inside contracting range</li>
          <li>Volume rising for 4 sessions</li>
          <li>Pattern matched 14× historically · 13 reached +5% target within 3 days</li>
          <li>Smart entry: <span style={{ color: T.label, fontFamily: 'IBM Plex Mono' }}>09:18 IST · ₹308.5 → ₹312.7</span></li>
        </ul>
      </>
    ),
  },
]

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

// ── Brand bar (prominent, top of page) ────────────────────────────────────────
function BrandBar() {
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

      {/* Right side: workspace + actions */}
      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 14 }}>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono', letterSpacing: '0.06em' }}>
          WORKSPACE
        </span>
        <span style={{ color: T.data, fontSize: 12, fontFamily: 'Inter Tight', fontWeight: 600, letterSpacing: '0.06em' }}>
          OVERVIEW
        </span>
        <span style={{ color: T.dim, fontFamily: 'IBM Plex Mono', fontSize: 11 }}>·</span>
        <button style={{
          fontFamily: 'IBM Plex Mono', fontSize: 11, padding: '5px 10px',
          background: 'transparent', border: `1px solid ${T.border}`, color: T.dim2,
          cursor: 'pointer', letterSpacing: '0.04em',
        }}>SAVE LAYOUT</button>
        <button style={{
          fontFamily: 'IBM Plex Mono', fontSize: 11, padding: '5px 10px',
          background: T.label, border: 'none', color: T.bg0, fontWeight: 600,
          cursor: 'pointer', letterSpacing: '0.04em',
        }}>+ ADD WIDGET</button>
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

// ── Macro strip ──────────────────────────────────────────────────────────────
function MacroStrip() {
  return (
    <div style={{
      display: 'grid', gridTemplateColumns: `repeat(${MACRO.length}, 1fr)`, gap: 1,
      background: T.border, borderBottom: `1px solid ${T.border}`,
    }}>
      {MACRO.map(m => (
        <div key={m.sym} style={{
          background: T.bg0, padding: '10px 14px',
          fontFamily: 'IBM Plex Mono, monospace',
        }}>
          <div style={{ color: T.dim, fontSize: 10, letterSpacing: '0.06em' }}>{m.sym}</div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 2 }}>
            <span style={{ color: T.data, fontSize: 14, fontWeight: 500, fontFeatureSettings: '"tnum" 1' }}>{m.val}</span>
            <span style={{ color: m.pos ? T.green : T.red, fontSize: 11, fontWeight: 500 }}>{m.chg}</span>
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
function SectorHeatmap() {
  const cellColor = (c: number) => {
    const intensity = Math.min(Math.abs(c) / 2.5, 1)
    if (c >= 0) return `rgba(34, 197, 94, ${0.10 + intensity * 0.5})`
    return `rgba(239, 68, 68, ${0.10 + intensity * 0.5})`
  }
  return (
    <div style={{ padding: '12px 16px', borderBottom: `1px solid ${T.border}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <span style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight' }}>
          NSE SECTOR HEATMAP
        </span>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>
          intraday · 11:42 IST
        </span>
      </div>
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(8, 1fr)', gap: 1,
        background: T.border, border: `1px solid ${T.border}`,
      }}>
        {SECTORS.map(s => (
          <div key={s.name} style={{
            background: cellColor(s.chg), padding: '14px 10px',
            display: 'flex', flexDirection: 'column', justifyContent: 'space-between',
            minHeight: 64, cursor: 'pointer',
          }}>
            <span style={{ color: T.data, fontSize: 10, fontFamily: 'Inter Tight', fontWeight: 600, letterSpacing: '0.04em' }}>{s.name}</span>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span style={{
                color: s.chg >= 0 ? T.green : T.red, fontSize: 14,
                fontFamily: 'IBM Plex Mono', fontWeight: 500, fontFeatureSettings: '"tnum" 1',
              }}>
                {s.chg >= 0 ? '+' : ''}{s.chg.toFixed(2)}%
              </span>
              <span style={{ color: T.dim, fontSize: 9, fontFamily: 'IBM Plex Mono' }}>n={s.n}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Hero engines (the headline of the page) ──────────────────────────────────
function HeroEngines() {
  const ENGINE_DETAIL = [
    { name: 'TURBO',    icon: 'T', color: T.label, n: 824,  wr: 99.39, avg: 5.07, cum: 4178.0, hold: 1.8, p90: 5.12, p180: 5.04,
      desc: 'Fast momentum exits · 1-3 day resolution',
      spark: [4.6, 4.9, 5.1, 4.8, 5.2, 5.0, 5.07] },
    { name: 'SUPER',    icon: 'S', color: T.green, n: 1069, wr: 99.81, avg: 5.28, cum: 5644.3, hold: 2.4, p90: 5.41, p180: 5.21,
      desc: 'Trend continuation · highest avg per trade',
      spark: [5.0, 5.1, 5.4, 5.2, 5.5, 5.3, 5.28] },
    { name: 'STANDARD', icon: 'ST', color: T.blue, n: 6722, wr: 30.04, avg: 0.21, cum: 1411.6, hold: 8.1, p90: 0.38, p180: 0.29,
      desc: 'High volume · selective entry required',
      spark: [0.1, 0.3, 0.2, 0.4, 0.0, 0.3, 0.21] },
  ]
  const sumTrades = ENGINE_DETAIL.reduce((s, e) => s + e.n, 0)

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
            {sumTrades.toLocaleString()} trades  ·  smart entry effective  ·  ALL · ALL years
          </span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          {['ALL', '2024', '2025', '2026'].map(y => (
            <button key={y} style={{
              ...chipBtn(y === 'ALL'),
              padding: '4px 12px', fontSize: 11,
            }}>{y}</button>
          ))}
        </div>
      </div>

      {/* Hero grid — 3 huge cards */}
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
    </div>
  )
}


// ── Top movers (own panel, below the fold) ───────────────────────────────────
function TopMoversPanel() {
  return (
    <div style={{ borderBottom: `1px solid ${T.border}`, background: T.bg0, padding: '14px 22px' }}>
      <div style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight', marginBottom: 10 }}>
        TOP MOVERS · INTRADAY
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: 'IBM Plex Mono' }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${T.border}` }}>
            {['TICKER', 'SECTOR', 'CHG', 'VOL', 'SIG'].map(h => (
              <th key={h} style={{ padding: '6px 8px', textAlign: 'left', color: T.dim, fontSize: 10, fontWeight: 500 }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {MOVERS.map(m => (
            <tr key={m.tk} style={{ borderBottom: `1px solid ${T.border}` }}>
              <td style={{ padding: '7px 8px', color: T.blue, fontWeight: 600 }}>{m.tk}</td>
              <td style={{ padding: '7px 8px', color: T.dim2, fontFamily: 'Inter Tight' }}>{m.sec}</td>
              <td style={{ padding: '7px 8px', color: m.chg >= 0 ? T.green : T.red, fontFeatureSettings: '"tnum" 1', textAlign: 'right' }}>
                {m.chg >= 0 ? '+' : ''}{m.chg.toFixed(2)}%
              </td>
              <td style={{ padding: '7px 8px', color: T.data, textAlign: 'right' }}>{m.vol}</td>
              <td style={{ padding: '7px 8px', color: m.sig === 'TURBO' ? T.label : m.sig === 'SUPER' ? T.green : T.dim }}>
                {m.sig}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Active signals (compressed) ───────────────────────────────────────────────
function ActiveSignalsPanel() {
  return (
    <div style={{ padding: '12px 16px', borderBottom: `1px solid ${T.border}`, background: T.bg0 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <span style={{ color: T.label, fontSize: 11, fontWeight: 600, letterSpacing: '0.12em', fontFamily: 'Inter Tight' }}>
          ACTIVE SIGNALS · TODAY · {ACTIVE.length}
        </span>
        <div style={{ display: 'flex', gap: 6 }}>
          {['ALL', 'TURBO', 'SUPER', 'STD'].map(b => (
            <button key={b} style={chipBtn(b === 'ALL')}>{b}</button>
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
          {ACTIVE.map((a, i) => (
            <tr key={a.tk} style={{ borderBottom: `1px solid ${T.border}` }}>
              <td style={{ padding: '7px 10px', color: T.dim, width: 24 }}>{i + 1}</td>
              <td style={{ padding: '7px 10px', color: T.blue, fontWeight: 600 }}>{a.tk}</td>
              <td style={{ padding: '7px 10px', color: a.eng === 'TURBO' ? T.label : T.green, fontWeight: 500 }}>{a.eng}</td>
              <td style={{ padding: '7px 10px', color: T.data, textAlign: 'right', width: 60, fontFeatureSettings: '"tnum" 1' }}>{a.score.toFixed(3)}</td>
              <td style={{ padding: '7px 10px', color: T.dim2, fontFamily: 'Inter Tight' }}>{a.sec}</td>
              <td style={{ padding: '7px 10px', color: T.dim2, fontSize: 11 }}>+ pin</td>
              <td style={{ padding: '7px 10px', color: T.ai, fontSize: 11 }}>ask</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Right rail: AI chat + News ───────────────────────────────────────────────
function AIChatPanel() {
  const [input, setInput] = useState('')
  const [convo, setConvo] = useState(AI_CONVERSATION)
  const [thinking, setThinking] = useState(false)
  const endRef = useRef<HTMLDivElement>(null)

  function send() {
    if (!input.trim()) return
    setConvo(c => [...c, { who: 'user', body: input }])
    setThinking(true)
    setInput('')
    setTimeout(() => {
      setConvo(c => [...c, { who: 'ai', body: <em style={{ color: T.dim2 }}>Mock — wire me to /api/ai/chat. Real AI will see the current ticker, filter scope, and active signals.</em> }])
      setThinking(false)
      setTimeout(() => endRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
    }, 700)
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
          <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>· context-aware</span>
        </div>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>opus-4.7</span>
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
              color: T.data, fontSize: 12, lineHeight: 1.55,
              fontFamily: m.who === 'user' ? 'IBM Plex Mono' : 'Inter Tight',
              background: m.who === 'user' ? T.bg2 : 'transparent',
              padding: m.who === 'user' ? '6px 10px' : 0,
              borderLeft: m.who === 'user' ? `2px solid ${T.label}` : 'none',
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

function NewsStream() {
  return (
    <div style={{
      borderTop: `1px solid ${T.border}`, background: T.bg0,
      maxHeight: 220, overflowY: 'auto',
    }}>
      <div style={{
        position: 'sticky', top: 0, background: T.bg1, padding: '8px 14px',
        borderBottom: `1px solid ${T.border}`, display: 'flex', justifyContent: 'space-between',
      }}>
        <span style={{ color: T.label, fontSize: 10, letterSpacing: '0.12em', fontWeight: 600, fontFamily: 'Inter Tight' }}>NEWS · LIVE</span>
        <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>5 today</span>
      </div>
      {NEWS.map((n, i) => {
        const sc = n.senti === 'pos' ? T.green : n.senti === 'neg' ? T.red : T.yellow
        return (
          <div key={i} style={{
            padding: '8px 14px', borderBottom: `1px solid ${T.border}`,
            display: 'grid', gridTemplateColumns: '40px 38px 50px 1fr', gap: 8, alignItems: 'baseline',
          }}>
            <span style={{ color: T.dim, fontSize: 10, fontFamily: 'IBM Plex Mono' }}>{n.t}</span>
            <span style={{ color: sc, fontSize: 9, fontFamily: 'IBM Plex Mono', fontWeight: 600 }}>● {n.tag}</span>
            <span style={{ color: T.dim2, fontSize: 9, fontFamily: 'IBM Plex Mono' }}>{n.src}</span>
            <span style={{ color: T.data, fontSize: 11, fontFamily: 'Inter Tight', lineHeight: 1.4 }}>
              {n.headline}
              {n.tk && <span style={{ color: T.blue, marginLeft: 6, fontFamily: 'IBM Plex Mono' }}>· {n.tk}</span>}
            </span>
          </div>
        )
      })}
    </div>
  )
}

// ── Bottom: function strip + ticker tape ─────────────────────────────────────
function FunctionFooter() {
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
          {[...MACRO, ...MOVERS, ...MACRO].map((m: any, i) => (
            <span key={i} style={{ display: 'flex', gap: 6 }}>
              <span style={{ color: T.dim2 }}>{m.sym || m.tk}</span>
              <span style={{ color: T.data }}>{m.val || (m.chg + '%')}</span>
              <span style={{ color: (m.pos || m.chg >= 0) ? T.green : T.red }}>
                {m.chg ?? ''}
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

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AnalysisV3Mock() {
  const [tab, setTab] = useState('MACRO')
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
        fontFamily: 'Inter Tight, sans-serif', paddingBottom: 56,
        display: 'flex', flexDirection: 'column',
      }}>
        <BrandBar />
        <StatusBar />
        <MacroStrip />
        <WorkspaceTabs active={tab} onChange={setTab} />

        {/* 3-column workspace */}
        <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
          <LeftRail />

          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', overflow: 'auto' }}>
            <Breadcrumb />
            {/* Hero: engines first, big and bold — the headline */}
            <HeroEngines />
            {/* Active signals second — the call to action */}
            <ActiveSignalsPanel />
            {/* Supporting context below the fold */}
            <SectorHeatmap />
            <TopMoversPanel />
          </div>

          <div style={{
            width: 380, borderLeft: `1px solid ${T.border}`, background: T.bg1,
            display: 'flex', flexDirection: 'column', minHeight: 0,
          }}>
            <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
              <AIChatPanel />
            </div>
            <NewsStream />
          </div>
        </div>

        <FunctionFooter />
      </div>
    </>
  )
}
