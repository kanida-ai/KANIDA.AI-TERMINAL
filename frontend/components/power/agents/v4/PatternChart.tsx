'use client'

/**
 * PatternChart (v4) — the big annotated candlestick for the Evidence column,
 * plus the win/loss path chart and the T+5 distribution histogram. All three
 * are SVG drawn from REAL data:
 *   • candles           ← GET /api/agents/chart/bars (point-in-time OHLC)
 *   • fitted boundaries / touches / breakout ← GET /api/agents/chart/setup.geometry
 *   • watch-plan level lines ← setup.watch_plan
 *   • path series       ← setup.paths (winners/losers cohort means)
 *   • histogram         ← per-case T+5 returns derived from setup.evidence.paths
 *
 * Re-uses the date→x / slope-extend logic proven in the older CandleChart, re-skinned
 * to the v4 navy/cyan palette and enriched with the handoff's annotations. It draws
 * only what it is given — no geometry → candles alone; no bars → honest empty state.
 */
import { V, FONT } from './tokens'
import type { Bar } from '@/lib/agents-api'

export type Pt = { date: string; price: number }
export type FitLine = { a: Pt; b: Pt }
export type Callout = { date?: string; price?: number; text: string }
export type LevelLine = { label: string; price: number; color: string }

export type ChartOverlay = {
  patternStartDate?: string | null
  upper?: FitLine | null
  lower?: FitLine | null
  level?: number | null           // flat horizontal boundary (horizontal-trendline / rectangle)
  touches?: Pt[]
  breakout?: Pt | null
  support?: number | null
  bullish?: boolean
  callouts?: Callout[]
  historicalFlag?: string | null
  watchLevels?: LevelLine[]
}

// ── the annotated candlestick ────────────────────────────────────────────────
export function CandleV4({ bars, overlay, showLastPill = true, emptyLabel }: {
  bars: Bar[]
  overlay?: ChartOverlay
  showLastPill?: boolean
  emptyLabel?: string
}) {
  const W = 900, H = 340
  const padL = 8, padR = 74, padT = 14
  const volBand = 52, dateAxis = 22
  const plotL = padL, plotR = W - padR
  const plotT = padT, plotB = H - dateAxis - volBand
  const plotW = plotR - plotL, plotH = plotB - plotT
  const volBot = H - dateAxis

  if (!bars || bars.length === 0) {
    return (
      <div style={{
        height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: V.inset, border: `1px solid ${V.border}`, borderRadius: 9,
        color: V.faint, fontSize: 12, textAlign: 'center', padding: 20, lineHeight: 1.6, fontFamily: FONT.sans,
      }}>
        {emptyLabel || 'Point-in-time candles unavailable for this setup yet.'}
      </div>
    )
  }

  const o = overlay || {}
  const n = bars.length
  const slot = plotW / n
  const bodyW = Math.max(1.5, Math.min(11, slot * 0.62))
  const cx = (i: number) => plotL + (i + 0.5) * slot

  // price domain
  const prices: number[] = []
  for (const b of bars) { prices.push(b.l, b.h) }
  const addPt = (p?: Pt | null) => { if (p) prices.push(p.price) }
  if (o.upper) { addPt(o.upper.a); addPt(o.upper.b) }
  if (o.lower) { addPt(o.lower.a); addPt(o.lower.b) }
  if (o.level != null) prices.push(o.level)
  if (o.support != null) prices.push(o.support)
  ;(o.touches || []).forEach(addPt)
  addPt(o.breakout)
  ;(o.watchLevels || []).forEach((l) => prices.push(l.price))
  let lo = Math.min(...prices), hi = Math.max(...prices)
  if (!isFinite(lo) || !isFinite(hi) || lo === hi) { lo -= 1; hi += 1 }
  const pad = (hi - lo) * 0.07; lo -= pad; hi += pad
  const y = (p: number) => plotT + (1 - (p - lo) / (hi - lo)) * plotH

  // volume domain
  const vmax = Math.max(...bars.map((b) => b.v || 0), 1)
  const vy = (v: number) => volBot - (v / vmax) * (volBand - 4)

  // date → index
  const idxByDate = new Map<string, number>()
  bars.forEach((b, i) => idxByDate.set(b.date, i))
  const xIndex = (date: string): number => {
    const e = idxByDate.get(date); if (e != null) return e
    if (date <= bars[0].date) return 0
    if (date >= bars[n - 1].date) return n - 1
    let last = 0; for (let i = 0; i < n; i++) { if (bars[i].date <= date) last = i; else break }
    return last
  }
  const patternStartI = o.patternStartDate ? xIndex(o.patternStartDate) : -1

  // resolve a fitted line to endpoints, extended to breakout along its slope
  const resolve = (ln: FitLine) => {
    const iA = xIndex(ln.a.date), iB = xIndex(ln.b.date)
    let x2 = cx(iB), y2 = y(ln.b.price)
    if (o.breakout) {
      const iE = xIndex(o.breakout.date)
      if (iB !== iA) {
        const slope = (ln.b.price - ln.a.price) / (iB - iA)
        x2 = cx(iE); y2 = y(ln.a.price + slope * (iE - iA))
      }
    }
    return { x1: cx(iA), y1: y(ln.a.price), x2, y2 }
  }

  // gridlines + right-edge price labels
  const grids = Array.from({ length: 7 }, (_, i) => lo + (hi - lo) * (i / 6))
  // date ticks (~5, month-ish)
  const tickCount = Math.min(5, n)
  const tickIdx = Array.from({ length: tickCount }, (_, i) => Math.round((i / (tickCount - 1 || 1)) * (n - 1)))

  const upC = V.green, dnC = V.red
  const last = bars[n - 1]
  const breakoutI = o.breakout ? xIndex(o.breakout.date) : -1

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block', height: 'auto' }}>
      {/* gridlines + price labels */}
      {grids.map((p, i) => (
        <g key={`g${i}`}>
          <line x1={plotL} y1={y(p)} x2={plotR} y2={y(p)} stroke={V.grid} strokeWidth={1} />
          <text x={plotR + 5} y={y(p) + 3.5} fontSize={10.5} fill={V.faint} fontFamily={FONT.mono}>{p.toFixed(1)}</text>
        </g>
      ))}

      {/* pattern zone tint */}
      {patternStartI >= 0 && (
        <rect x={cx(patternStartI) - slot / 2} y={plotT} width={plotR - (cx(patternStartI) - slot / 2)} height={plotH}
          fill={V.cyan} opacity={0.03} />
      )}

      {/* volume histogram */}
      {bars.map((b, i) => {
        const up = b.c >= b.o
        const pre = patternStartI >= 0 && i < patternStartI
        return (
          <rect key={`v${i}`} x={cx(i) - bodyW / 2} y={vy(b.v || 0)} width={bodyW} height={Math.max(0, volBot - vy(b.v || 0))}
            fill={up ? upC : dnC} opacity={pre ? 0.16 : 0.38} />
        )
      })}

      {/* candles */}
      {bars.map((b, i) => {
        const up = b.c >= b.o
        const c = up ? upC : dnC
        const pre = patternStartI >= 0 && i < patternStartI
        const xc = cx(i)
        const top = Math.min(y(b.o), y(b.c))
        const bh = Math.max(1, Math.abs(y(b.c) - y(b.o)))
        return (
          <g key={`c${i}`} opacity={pre ? 0.4 : 1}>
            <line x1={xc} y1={y(b.h)} x2={xc} y2={y(b.l)} stroke={c} strokeWidth={1} opacity={pre ? 0.4 : 0.9} />
            <rect x={xc - bodyW / 2} y={top} width={bodyW} height={bh} fill={c} opacity={pre ? 0.32 : 0.95} />
          </g>
        )
      })}

      {/* fitted boundaries (upper/lower or a flat level) */}
      {[o.upper, o.lower].map((ln, i) => {
        if (!ln) return null
        const r = resolve(ln)
        return (
          <g key={`fit${i}`}>
            <line x1={r.x1} y1={r.y1} x2={r.x2} y2={r.y2} stroke="#cbd5e1" strokeWidth={1.3}
              strokeDasharray="6 4" opacity={0.72} strokeLinecap="round" />
            <circle cx={r.x1} cy={r.y1} r={2} fill={V.cyan} />
            <circle cx={r.x2} cy={r.y2} r={2} fill={V.cyan} />
          </g>
        )
      })}
      {o.upper == null && o.lower == null && o.level != null && patternStartI >= 0 && (
        <g>
          <line x1={cx(patternStartI)} y1={y(o.level)} x2={plotR} y2={y(o.level)} stroke="#cbd5e1"
            strokeWidth={1.3} strokeDasharray="6 4" opacity={0.72} strokeLinecap="round" />
          <circle cx={cx(patternStartI)} cy={y(o.level)} r={2} fill={V.cyan} />
          <circle cx={plotR} cy={y(o.level)} r={2} fill={V.cyan} />
        </g>
      )}

      {/* support line */}
      {o.support != null && (
        <g>
          <line x1={plotL} y1={y(o.support)} x2={plotR} y2={y(o.support)} stroke={V.cyan} strokeWidth={1}
            strokeDasharray="4 4" opacity={0.55} />
          <text x={plotR - 2} y={y(o.support) - 4} fontSize={10} fill={V.cyan} fontFamily={FONT.mono} textAnchor="end">
            Support {o.support.toFixed(0)}
          </text>
        </g>
      )}

      {/* breakout projection arrow */}
      {breakoutI >= 0 && o.breakout && (() => {
        const bx = cx(breakoutI), by = y(o.breakout.price)
        const dir = o.bullish === false ? 1 : -1     // up for bullish
        const ex = Math.min(bx + 44, plotR), ey = by + dir * 40
        const c = o.bullish === false ? dnC : upC
        return (
          <g>
            <line x1={bx} y1={by} x2={ex} y2={ey} stroke={c} strokeWidth={2} strokeDasharray="5 3" />
            <polygon points={`${ex},${ey} ${ex - 7},${ey - dir * -6 - dir * 0} ${ex - 9},${ey - dir * 8}`}
              fill={c} transform={`rotate(${dir < 0 ? -30 : 30} ${ex} ${ey})`} />
            <circle cx={bx} cy={by} r={4} fill="none" stroke={c} strokeWidth={2} />
          </g>
        )
      })()}

      {/* touch markers */}
      {(o.touches || []).map((t, i) => {
        const xi = xIndex(t.date)
        return <circle key={`t${i}`} cx={cx(xi)} cy={y(t.price)} r={2.6} fill={V.cyan} stroke={V.bg} strokeWidth={1} />
      })}

      {/* watch-plan level lines */}
      {(o.watchLevels || []).map((l, i) => (
        <g key={`wl${i}`}>
          <line x1={plotL} y1={y(l.price)} x2={plotR} y2={y(l.price)} stroke={l.color} strokeWidth={1.2} strokeDasharray="5 4" opacity={0.85} />
          <text x={plotL + 3} y={y(l.price) - 4} fontSize={10} fill={l.color} fontFamily={FONT.mono}>{l.label} {l.price.toFixed(1)}</text>
        </g>
      ))}

      {/* annotation callouts */}
      {(o.callouts || []).map((c, i) => {
        const ax = plotL + 14 + (i % 2) * 190
        const ay = plotT + 16 + Math.floor(i / 2) * 26
        const w = Math.max(70, c.text.length * 5.6 + 14)
        return (
          <g key={`ca${i}`}>
            <rect x={ax} y={ay} width={w} height={18} rx={5} fill="#0d2233" stroke={V.borderActive} strokeWidth={1} />
            <text x={ax + 7} y={ay + 12.5} fontSize={10} fill={V.muted} fontFamily={FONT.sans}>{c.text}</text>
          </g>
        )
      })}

      {/* historical flag top-right */}
      {o.historicalFlag && (
        <g>
          <rect x={plotR - 214} y={plotT + 2} width={210} height={19} rx={5} fill="#0b2320" stroke="#1c5c48" strokeWidth={1} />
          <text x={plotR - 106} y={plotT + 15} fontSize={10.5} fill={V.greenHi} fontFamily={FONT.sans} textAnchor="middle">{o.historicalFlag}</text>
        </g>
      )}

      {/* last-price pill */}
      {showLastPill && (() => {
        const up = last.c >= last.o
        const c = up ? upC : dnC
        return (
          <g>
            <rect x={plotR + 1} y={y(last.c) - 9} width={padR - 4} height={18} rx={4} fill={c} />
            <text x={plotR + (padR - 4) / 2} y={y(last.c) + 4} fontSize={11} fontWeight={600} fill="#05121c"
              fontFamily={FONT.mono} textAnchor="middle">{last.c.toFixed(1)}</text>
          </g>
        )
      })()}

      {/* date ticks */}
      {tickIdx.map((i, k) => (
        <text key={`d${k}`} x={cx(i)} y={H - 6} fontSize={10.5} fill={V.faint} fontFamily={FONT.mono}
          textAnchor={k === 0 ? 'start' : k === tickIdx.length - 1 ? 'end' : 'middle'}>
          {(bars[i]?.date || '').slice(5)}
        </text>
      ))}
    </svg>
  )
}

// ── win/loss path chart ──────────────────────────────────────────────────────
export function PathChartV4({ winners, losers, nWin, nLoss }: {
  winners: number[] | null; losers: number[] | null; nWin: number | null; nLoss: number | null
}) {
  const W = 900, H = 220, padL = 40, padR = 60, padT = 16, padB = 26
  const series = [
    { data: winners, color: V.green, label: `Winning · ${nWin ?? '—'}` },
    { data: losers, color: V.red, label: `Losing · ${nLoss ?? '—'}` },
  ].filter((s) => s.data && s.data.length)
  if (series.length === 0) {
    return <Empty>No resolved winning/losing trajectories yet — the path chart needs precedents.</Empty>
  }
  const all: number[] = [0]
  series.forEach((s) => s.data!.forEach((v) => all.push(v)))
  let lo = Math.min(...all), hi = Math.max(...all)
  if (lo === hi) { lo -= 1; hi += 1 }
  const span = hi - lo
  const len = Math.max(...series.map((s) => s.data!.length))
  const x = (i: number) => padL + (i / (len - 1 || 1)) * (W - padL - padR)
  const y = (v: number) => padT + (1 - (v - lo) / span) * (H - padT - padB)
  const ticks = [0, 3, 5, 10].filter((t) => t < len)
  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block', height: 'auto' }}>
      <line x1={padL} y1={y(0)} x2={W - padR} y2={y(0)} stroke={V.borderStrong} strokeWidth={1} />
      {series.map((s, si) => {
        const d = s.data!.map((v, i) => `${i === 0 ? 'M' : 'L'} ${x(i)} ${y(v)}`).join(' ')
        return (
          <g key={si}>
            <path d={d} fill="none" stroke={s.color} strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round" />
            {[0, 3, 5, s.data!.length - 1].filter((i) => i < s.data!.length).map((i) => (
              <circle key={i} cx={x(i)} cy={y(s.data![i])} r={2.8} fill={s.color} />
            ))}
            <text x={x(s.data!.length - 1) + 5} y={y(s.data![s.data!.length - 1]) + 3.5} fontSize={11} fontWeight={600}
              fill={s.color} fontFamily={FONT.mono}>
              {(s.data![s.data!.length - 1] >= 0 ? '+' : '') + s.data![s.data!.length - 1].toFixed(1)}%
            </text>
          </g>
        )
      })}
      {ticks.map((t) => (
        <text key={t} x={x(t)} y={H - 8} fontSize={10} fill={V.faint} fontFamily={FONT.mono} textAnchor="middle">T+{t}</text>
      ))}
      {series.map((s, si) => (
        <text key={`lg${si}`} x={padL + si * 150} y={12} fontSize={10} fill={s.color} fontFamily={FONT.mono}>■ {s.label}</text>
      ))}
    </svg>
  )
}

// ── T+5 distribution histogram ───────────────────────────────────────────────
export function HistogramV4({ buckets }: { buckets: { label: string; count: number; positive: boolean }[] }) {
  const W = 900, H = 210, padL = 20, padR = 20, padT = 22, padB = 26
  const max = Math.max(...buckets.map((b) => b.count), 1)
  const bw = (W - padL - padR) / buckets.length
  const y = (c: number) => padT + (1 - c / max) * (H - padT - padB)
  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block', height: 'auto' }}>
      <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke={V.hairline} strokeWidth={1} />
      {buckets.map((b, i) => {
        const bx = padL + i * bw + bw * 0.12
        const w = bw * 0.76
        const top = b.count > 0 ? y(b.count) : H - padB
        return (
          <g key={i}>
            <rect x={bx} y={top} width={w} height={Math.max(0, H - padB - top)} rx={3}
              fill={b.positive ? V.green : V.red} opacity={0.6} />
            {b.count > 0 && <text x={bx + w / 2} y={top - 4} fontSize={9.5} fill={V.dim} fontFamily={FONT.mono} textAnchor="middle">{b.count}</text>}
            <text x={bx + w / 2} y={H - 8} fontSize={10} fill={V.faint} fontFamily={FONT.mono} textAnchor="middle">{b.label}</text>
          </g>
        )
      })}
    </svg>
  )
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ padding: '30px 16px', textAlign: 'center', color: V.faint, fontSize: 12, lineHeight: 1.6, fontFamily: FONT.sans }}>
      {children}
    </div>
  )
}
