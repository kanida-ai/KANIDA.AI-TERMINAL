/**
 * EquitySparkline + EquityChart — pure SVG, no external chart lib.
 *
 * Sparkline (small, card-inline): no axes, just a smoothed line.
 * Chart (full): same line + min/max labels + y-axis baseline at start equity.
 *
 * Operator design rule: green when ending >= start, red otherwise. Subtle
 * gradient fill underneath the line. No animation — performance/print-clarity
 * over flourish.
 */
type Point = { trade_date: string; total_equity: number }


export function EquitySparkline({ points, positive = true, className = '' }: {
  points: Point[]
  positive?: boolean
  className?: string
}) {
  return (
    <Sparkline points={points} positive={positive} className={className} compact />
  )
}


export function EquityChart({ points, capitalStart, className = '' }: {
  points: Point[]
  capitalStart: number
  className?: string
}) {
  if (points.length < 2) {
    return (
      <div className={`flex items-center justify-center text-sm text-neutral-500 h-48 ${className}`}>
        Equity curve will appear after the first EOD cycle.
      </div>
    )
  }
  const last      = points[points.length - 1]
  const first     = points[0]
  const positive  = last.total_equity >= first.total_equity
  return (
    <div className={`relative ${className}`}>
      <Sparkline points={points} positive={positive} baselineRs={capitalStart} className="w-full h-64" />
    </div>
  )
}


function Sparkline({ points, positive, compact = false, baselineRs, className = '' }: {
  points: Point[]
  positive: boolean
  compact?: boolean
  baselineRs?: number
  className?: string
}) {
  const values = points.map(p => p.total_equity)
  const yMin = Math.min(...values, baselineRs ?? Infinity)
  const yMax = Math.max(...values, baselineRs ?? -Infinity)
  const yPad = (yMax - yMin) * 0.08 || (yMax * 0.02)
  const yLo  = yMin - yPad
  const yHi  = yMax + yPad

  // Coordinate system — fixed viewBox; CSS resizes to container
  const W = 600
  const H = compact ? 60 : 240
  const margin = compact
    ? { top: 4,  right: 4,  bottom: 4,  left: 4 }
    : { top: 10, right: 10, bottom: 24, left: 70 }
  const innerW = W - margin.left - margin.right
  const innerH = H - margin.top  - margin.bottom

  const n = points.length
  const x = (i: number) => margin.left + (i / Math.max(1, n - 1)) * innerW
  const y = (v: number) => margin.top + (1 - (v - yLo) / (yHi - yLo)) * innerH

  const path = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${x(i).toFixed(1)} ${y(p.total_equity).toFixed(1)}`).join(' ')
  const fillPath = `${path} L ${x(n - 1).toFixed(1)} ${y(yLo).toFixed(1)} L ${x(0).toFixed(1)} ${y(yLo).toFixed(1)} Z`

  const lineCls = positive ? 'stroke-green-400' : 'stroke-red-400'
  const fillCls = positive ? 'fill-green-400/15' : 'fill-red-400/15'

  // Y-axis ticks for full chart
  const yTicks = compact ? [] : [yLo, (yLo + yHi) / 2, yHi]
  const xTicks = compact ? [] : [
    { i: 0,                       label: points[0].trade_date.slice(5) },
    { i: Math.floor((n - 1) / 2), label: points[Math.floor((n - 1) / 2)].trade_date.slice(5) },
    { i: n - 1,                   label: points[n - 1].trade_date.slice(5) },
  ]
  const fmtY = (v: number) =>
    v >= 1e7 ? `${(v / 1e7).toFixed(2)} Cr`
    : v >= 1e5 ? `${(v / 1e5).toFixed(2)} L`
    :            `₹${Math.round(v).toLocaleString('en-IN')}`

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className={`block w-full ${className}`} preserveAspectRatio="none">
      {/* baseline rule (start capital) */}
      {!compact && baselineRs != null && (
        <line
          x1={margin.left} x2={margin.left + innerW}
          y1={y(baselineRs)} y2={y(baselineRs)}
          stroke="rgba(255,255,255,0.15)" strokeDasharray="3 5"
        />
      )}
      {/* y-ticks */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line x1={margin.left} x2={margin.left + innerW}
                y1={y(t)}        y2={y(t)}
                stroke="rgba(255,255,255,0.05)" />
          <text x={margin.left - 6} y={y(t)} fill="rgba(255,255,255,0.4)"
                fontSize="10" fontFamily="ui-monospace,monospace"
                textAnchor="end" dominantBaseline="middle">
            {fmtY(t)}
          </text>
        </g>
      ))}
      {/* x-ticks (mm-dd) */}
      {xTicks.map((t, i) => (
        <text key={i} x={x(t.i)} y={H - 6}
              fill="rgba(255,255,255,0.4)"
              fontSize="10" fontFamily="ui-monospace,monospace"
              textAnchor={i === 0 ? 'start' : i === xTicks.length - 1 ? 'end' : 'middle'}>
          {t.label}
        </text>
      ))}
      {/* fill */}
      <path d={fillPath} className={fillCls} />
      {/* line */}
      <path d={path} className={`${lineCls} fill-none`} strokeWidth={compact ? 1.5 : 2}
            strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  )
}
