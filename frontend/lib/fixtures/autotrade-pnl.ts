/**
 * Local-dev fixture for the AutoTrade P&L dashboard.
 *
 * Produces the EXACT `AutotradePnlSummary` contract shape the backend team is
 * implementing in parallel (GET /api/power/autotrade/pnl/summary). Used ONLY
 * when NEXT_PUBLIC_PNL_FIXTURE === '1' so the page can be built + reviewed
 * before the endpoint is live. It is never rendered in production (the flag is
 * unset on Vercel) — the page hits the real endpoint there.
 *
 * The numbers mirror the approved mockup: net = gross − charges throughout, a
 * realistic mix of green + red strategies, and per-strategy session/campaign
 * drill-down. Strategy NAMES come from this fixture (not hardcoded in the page)
 * — the real page renders whatever strategies the API returns.
 */
import type {
  AutotradePnlSummary, PnlPeriod, PnlStrategy, PnlSession, PnlMode,
} from '@/lib/power-api'

type Base = {
  id: string; name: string
  segment: string; segments: string[]
  product: string; products: string[]
  gross: number; charges: number; trades: number; win_rate: number
  best: number; worst: number; bestLabel: string; worstLabel: string
  kind: 'session' | 'campaign'
}

// YTD baseline; every period is derived by scaling. Net is ALWAYS gross − charges.
const BASE: Base[] = [
  { id: 'pos',  name: 'Positional Basket',      segment: 'Equity',  segments: ['Equity'],  product: 'CNC · MTF', products: ['CNC', 'MTF'],
    gross: 302400, charges: 17800, trades: 146, win_rate: 71, best: 41200, worst: -18600, bestLabel: 'Jun 24 · CNC',      worstLabel: 'May 09 · MTF',           kind: 'session'  },
  { id: 'intr', name: 'Intraday Basket',        segment: 'Equity',  segments: ['Equity'],  product: 'MIS',       products: ['MIS'],
    gross: 171900, charges: 29600, trades: 388, win_rate: 63, best: 22400, worst: -14100, bestLabel: 'Jul 03 · MIS',      worstLabel: 'Jun 28 · MIS',           kind: 'session'  },
  { id: 'lad',  name: 'Auto-Ladder (Monthly)',  segment: 'Equity',  segments: ['Equity'],  product: 'CNC',       products: ['CNC'],
    gross: 108700, charges: 12300, trades: 214, win_rate: 66, best: 18900, worst:  -9200, bestLabel: 'Jun · Campaign 6',  worstLabel: 'Apr · Campaign 4',       kind: 'campaign' },
  { id: 'fut',  name: 'F&O Futures',            segment: 'Futures', segments: ['Futures'], product: 'NRML',      products: ['NRML'],
    gross:  71400, charges: 13200, trades:  92, win_rate: 58, best: 19700, worst: -12400, bestLabel: 'Jun 17 · NIFTY',    worstLabel: 'May 22 · BANKNIFTY',     kind: 'session'  },
  { id: 'kill', name: 'Kill-Switch Basket',     segment: 'Equity',  segments: ['Equity'],  product: 'MIS',       products: ['MIS'],
    gross: -18400, charges:  4400, trades:  64, win_rate: 44, best:  6100, worst: -11800, bestLabel: 'Jul 01 · MIS',      worstLabel: 'Jun 12 · MIS',           kind: 'session'  },
]

// Period scaling. Net recomputed = gross − charges so it always reconciles.
const PMULT: Record<Exclude<PnlPeriod, 'yesterday' | 'custom'>, number> = {
  ytd: 1, mtd: 0.17, '2w': 0.075, '1w': 0.038,
}
// One concrete day, realistic mix of green + red: [gross, charges, trades, wr]
const YEST: Record<string, [number, number, number, number]> = {
  pos: [6760, 520, 5, 80], intr: [-1240, 600, 5, 40], lad: [3460, 350, 6, 67],
  fut: [2940, 520, 3, 67], kill: [-760, 220, 2, 50],
}
const CAP: Record<PnlPeriod, number> = {
  ytd: 1500000, mtd: 1500000, '2w': 900000, '1w': 700000, yesterday: 520000, custom: 520000,
}
const DATES = ['Jul 07', 'Jul 04', 'Jul 03', 'Jul 02', 'Jun 27', 'Jun 20']
const LAD_MONTHS = ['Jun', 'May', 'Apr', 'Mar', 'Feb', 'Jan']

function mkSessions(b: Base, net: number, trades: number, wr: number, period: PnlPeriod): PnlSession[] {
  const n = period === 'yesterday' || period === 'custom' ? 1
          : period === '1w' ? 2 : period === '2w' ? 3 : period === 'mtd' ? 4 : 6
  const out: PnlSession[] = []
  let rem = net, remT = trades
  for (let i = 0; i < n; i++) {
    const last = i === n - 1
    const part = last ? rem : Math.round((net / n) * (0.6 + (i % 3) * 0.4) * (i % 2 ? 1 : 1.15))
    const t    = last ? remT : Math.max(1, Math.round(trades / n))
    rem -= part; remT -= t
    const c    = part >= 0 ? Math.round(part * 0.12) : Math.round(-part * 0.06)
    out.push({
      id:         `${b.id}-s${i}`,
      kind:       b.kind,
      name:       b.kind === 'campaign' ? `Campaign ${6 - i}` : `${b.name.split(' ')[0]} · ${DATES[i]}`,
      date_label: b.kind === 'campaign' ? `${LAD_MONTHS[i]} · 3 children` : `${DATES[i]} · ${t} legs`,
      net:        part,
      gross:      part + c,
      charges:    c,
      trades:     t,
      win_rate:   Math.max(30, Math.min(92, wr + ((i % 3) - 1) * 7)),
      segment:    b.segment,
      product:    b.products[0],
    })
  }
  return out
}

export function sampleAutotradePnl(
  period: PnlPeriod,
  opts: { from?: string | null; to?: string | null; mode?: PnlMode } = {},
): AutotradePnlSummary {
  const strategies: PnlStrategy[] = BASE.map(b => {
    let gross: number, charges: number, trades: number, wr: number
    if (period === 'yesterday' || period === 'custom') {
      const y = YEST[b.id]; gross = y[0]; charges = y[1]; trades = y[2]; wr = y[3]
    } else {
      const m = PMULT[period]
      gross = Math.round(b.gross * m); charges = Math.round(b.charges * m)
      trades = Math.max(1, Math.round(b.trades * m)); wr = b.win_rate
    }
    const net  = gross - charges
    const wins = Math.round(trades * wr / 100)
    const sc   = period === 'yesterday' || period === 'custom' ? 1 : PMULT[period]
    const scl  = period === 'ytd' ? 1 : (period === 'yesterday' ? 0.14 : Math.min(1, sc * 3.2))
    return {
      id: b.id, name: b.name,
      segment: b.segment, segments: b.segments,
      product: b.product, products: b.products,
      net, gross, charges, trades, wins, losses: trades - wins, win_rate: wr,
      avg: Math.round(net / trades),
      best:  { pnl: Math.round(b.best  * scl), label: b.bestLabel },
      worst: { pnl: Math.round(b.worst * scl), label: b.worstLabel },
      sessions: mkSessions(b, net, trades, wr, period),
    }
  })

  const totals = strategies.reduce(
    (o, s) => ({
      net: o.net + s.net, gross: o.gross + s.gross, charges: o.charges + s.charges,
      trades: o.trades + s.trades, wins: o.wins + s.wins, losses: o.losses + s.losses,
    }),
    { net: 0, gross: 0, charges: 0, trades: 0, wins: 0, losses: 0 },
  )
  const win_rate = totals.trades ? Math.round((totals.wins / totals.trades) * 100) : 0

  return {
    period,
    from: opts.from ?? (period === 'custom' ? '2026-06-21' : null),
    to:   opts.to   ?? (period === 'custom' ? '2026-07-07' : null),
    as_of: '2026-07-07T15:45:00+05:30',
    capital_deployed: CAP[period],
    totals: { ...totals, win_rate },
    strategies,
  }
}
