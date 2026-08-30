'use client'

/**
 * CandleChart — the reusable candlestick + pattern-overlay primitive for the
 * Chart Agent deep-dive. ALL nine patterns render through this single component;
 * they differ only in which lines / shape / levels the /setup geometry provides.
 *
 * It draws real OHLC candles from GET /api/agents/chart/bars, then overlays the
 * detector geometry from GET /api/agents/chart/setup:
 *   • trend lines   (upper / lower for wedges·triangles·channels), extended to
 *                    the breakout via the line's own slope,
 *   • level lines   (flat horizontals for horizontal-trendline · cup rim/base ·
 *                    watch-plan confirmation/warning/invalidation),
 *   • touch markers (the real swing highs/lows the detector anchored on),
 *   • a shaded body (the polygon between upper & lower — the wedge/triangle body),
 *   • a highlighted breakout candle.
 *
 * HONESTY: it renders only what it is given. No bars → honest empty state; no
 * geometry → candles alone. Nothing is invented.
 */
import { T } from '@/lib/theme'
import type { Bar } from '@/lib/agents-api'

export type ChartPoint = { date: string; price: number }
export type ChartLine = {
  from: ChartPoint
  to: ChartPoint
  extendToDate?: string | null   // extend along the a→b slope to this bar (e.g. the breakout)
  color?: string
  dash?: boolean
  width?: number
}
export type ChartLevel = { price: number; label?: string; color?: string; dash?: boolean }
export type ChartMarker = { date: string; price: number; color?: string; ring?: boolean; label?: string }

export function CandleChart({
  bars,
  height = 300,
  lines = [],
  levels = [],
  markers = [],
  shade = null,
  highlightDate = null,
  emptyLabel = 'Point-in-time candles unavailable for this setup yet.',
}: {
  bars: Bar[]
  height?: number
  lines?: ChartLine[]
  levels?: ChartLevel[]
  markers?: ChartMarker[]
  shade?: { upper: ChartLine; lower: ChartLine; color?: string } | null
  highlightDate?: string | null
  emptyLabel?: string
}) {
  const W = 720
  const H = height
  const padL = 8
  const padR = 58          // room for price axis / level labels
  const padT = 14
  const padB = 26          // room for a couple of date ticks
  const plotL = padL
  const plotR = W - padR
  const plotT = padT
  const plotB = H - padB
  const plotW = plotR - plotL
  const plotH = plotB - plotT

  if (!bars || bars.length === 0) {
    return (
      <div
        style={{
          height, display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: T.s2, border: `1px solid ${T.b}`, borderRadius: 14,
          color: T.t3, fontSize: 12.5, textAlign: 'center', padding: 20, lineHeight: 1.6,
        }}
      >
        {emptyLabel}
      </div>
    )
  }

  const n = bars.length
  const cw = plotW / n
  const bodyW = Math.max(1.5, Math.min(11, cw * 0.62))
  const cx = (i: number) => plotL + (i + 0.5) * cw

  // ── price domain: every candle low/high + every overlay price, padded ──
  const prices: number[] = []
  for (const b of bars) { prices.push(b.l, b.h) }
  for (const l of levels) prices.push(l.price)
  for (const m of markers) prices.push(m.price)
  const collectLine = (ln?: ChartLine | null) => { if (ln) { prices.push(ln.from.price, ln.to.price) } }
  lines.forEach(collectLine)
  if (shade) { collectLine(shade.upper); collectLine(shade.lower) }
  let lo = Math.min(...prices)
  let hi = Math.max(...prices)
  if (!isFinite(lo) || !isFinite(hi) || lo === hi) { lo = lo - 1; hi = hi + 1 }
  const pad = (hi - lo) * 0.06
  lo -= pad; hi += pad
  const y = (p: number) => plotT + (1 - (p - lo) / (hi - lo)) * plotH

  // ── date → x index (exact, else nearest by chronological position) ──
  const idxByDate = new Map<string, number>()
  bars.forEach((b, i) => idxByDate.set(b.date, i))
  const xIndexFor = (date: string): number => {
    const exact = idxByDate.get(date)
    if (exact != null) return exact
    if (date <= bars[0].date) return 0
    if (date >= bars[n - 1].date) return n - 1
    let lastBelow = 0
    for (let i = 0; i < n; i++) { if (bars[i].date <= date) lastBelow = i; else break }
    return lastBelow
  }

  // resolve a line to two screen points, extending along the a→b slope if asked
  const resolveLine = (ln: ChartLine) => {
    const iA = xIndexFor(ln.from.date)
    const iB = xIndexFor(ln.to.date)
    let x1 = cx(iA), y1v = y(ln.from.price)
    let x2 = cx(iB), y2v = y(ln.to.price)
    if (ln.extendToDate) {
      const iE = xIndexFor(ln.extendToDate)
      if (iB !== iA) {
        const slope = (ln.to.price - ln.from.price) / (iB - iA)
        const priceE = ln.from.price + slope * (iE - iA)
        x2 = cx(iE); y2v = y(priceE)
      }
    }
    return { x1, y1: y1v, x2, y2: y2v }
  }

  // shade polygon = region between upper & lower across their shared x-span
  let shadePoly: string | null = null
  if (shade) {
    const u = resolveLine(shade.upper)
    const l = resolveLine(shade.lower)
    const xL = Math.min(u.x1, l.x1)
    const xR = Math.max(u.x2, l.x2)
    // interpolate each line's y at xL and xR
    const yAt = (ln: { x1: number; y1: number; x2: number; y2: number }, x: number) => {
      if (ln.x2 === ln.x1) return ln.y1
      return ln.y1 + ((ln.y2 - ln.y1) * (x - ln.x1)) / (ln.x2 - ln.x1)
    }
    shadePoly = [
      `${xL},${yAt(u, xL)}`, `${xR},${yAt(u, xR)}`,
      `${xR},${yAt(l, xR)}`, `${xL},${yAt(l, xL)}`,
    ].join(' ')
  }

  const highlightI = highlightDate ? xIndexFor(highlightDate) : -1

  // sparse date ticks (first, middle, last)
  const tickIdxs = n <= 2 ? [0, n - 1] : [0, Math.floor(n / 2), n - 1]

  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 14, padding: 8 }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }} preserveAspectRatio="none">
        {/* shaded pattern body */}
        {shadePoly && (
          <polygon points={shadePoly} fill={shade!.color || 'rgba(0,201,138,0.09)'} stroke="none" />
        )}

        {/* breakout candle highlight band */}
        {highlightI >= 0 && (
          <rect
            x={plotL + highlightI * cw} y={plotT} width={cw} height={plotH}
            fill="rgba(0,201,138,0.10)" stroke="rgba(0,201,138,0.28)" strokeWidth={1}
          />
        )}

        {/* level lines (flat horizontals) */}
        {levels.map((lv, i) => {
          const yy = y(lv.price)
          const c = lv.color || T.b2
          return (
            <g key={`lv-${i}`}>
              <line
                x1={plotL} y1={yy} x2={plotR} y2={yy}
                stroke={c} strokeWidth={1.4} strokeDasharray={lv.dash === false ? undefined : '5 4'}
              />
              {lv.label && (
                <text x={plotR + 4} y={yy + 3.5} fontSize={9.5} fill={c} fontFamily="monospace">{lv.label}</text>
              )}
            </g>
          )
        })}

        {/* candles */}
        {bars.map((b, i) => {
          const up = b.c >= b.o
          const c = up ? T.g : T.r
          const xc = cx(i)
          const yO = y(b.o), yC = y(b.c)
          const top = Math.min(yO, yC)
          const bh = Math.max(1, Math.abs(yC - yO))
          return (
            <g key={`c-${i}`}>
              <line x1={xc} y1={y(b.h)} x2={xc} y2={y(b.l)} stroke={c} strokeWidth={1} opacity={0.9} />
              <rect x={xc - bodyW / 2} y={top} width={bodyW} height={bh} fill={c} opacity={up ? 0.95 : 0.9} rx={0.5} />
            </g>
          )
        })}

        {/* trend lines (sloped upper/lower, extended to breakout) */}
        {lines.map((ln, i) => {
          const r = resolveLine(ln)
          return (
            <line
              key={`ln-${i}`} x1={r.x1} y1={r.y1} x2={r.x2} y2={r.y2}
              stroke={ln.color || T.a} strokeWidth={ln.width || 2}
              strokeDasharray={ln.dash ? '6 4' : undefined} strokeLinecap="round"
            />
          )
        })}

        {/* touch markers */}
        {markers.map((m, i) => {
          const xi = xIndexFor(m.date)
          const mx = cx(xi), my = y(m.price)
          const c = m.color || T.a
          return (
            <g key={`m-${i}`}>
              {m.ring
                ? <circle cx={mx} cy={my} r={5} fill="none" stroke={c} strokeWidth={2} />
                : <circle cx={mx} cy={my} r={3.4} fill={c} stroke="#04120c" strokeWidth={1} />}
              {m.label && <text x={mx + 6} y={my - 5} fontSize={9} fill={c} fontFamily="monospace">{m.label}</text>}
            </g>
          )
        })}

        {/* date ticks */}
        {tickIdxs.map((i) => (
          <text key={`t-${i}`} x={cx(i)} y={H - 8} fontSize={9} fill={T.t3} fontFamily="monospace"
            textAnchor={i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle'}>
            {(bars[i].date || '').slice(5)}
          </text>
        ))}
      </svg>
    </div>
  )
}
