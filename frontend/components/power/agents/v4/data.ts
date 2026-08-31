// Chart Agent v4 — data layer. Maps the REAL backend payloads (agents-api.ts /
// api.kanida.ai) onto the shapes the v4 UI renders. Nothing here fabricates a
// number: where the backend has no field (sector, market-cap, market-alignment,
// a pre-built distribution) we return null and the UI honestly shows "—" / omits.
//
// The live /setup shape differs from the (older) declared SetupResp type in
// agents-api.ts — decision.decision (not .verdict), evidence.summary, subscores
// on a 0–1 scale — so we read it through the loose LiveSetup view below.

import type { ScanRow, ScanResp, Bar } from '@/lib/agents-api'
import { patternShort, patternFamily, type PatternFamily } from '@/lib/agents-api'
import type { Status } from './tokens'

// ── loose view over the live /setup payload ─────────────────────────────────
export type LiveHorizon = { win?: number; mean?: number; median?: number; p25?: number; p75?: number; mfe?: number; mae?: number }
export type LiveGeomPt = { date: string; price: number }
export type LiveGeomLine = { a?: LiveGeomPt | null; b?: LiveGeomPt | null } | null
export type LiveSetup = {
  ok?: boolean
  symbol?: string
  pattern?: string
  stage?: string | null
  direction?: string
  level?: number | null
  context?: { distance_to_level_pct?: number; volume_x?: number; as_of_date?: string | null }
  geometry?: {
    upper?: LiveGeomLine; lower?: LiveGeomLine
    touches?: LiveGeomPt[]; breakout?: LiveGeomPt | null
    level_line?: { from?: LiveGeomPt; to?: LiveGeomPt } | null
    apex?: LiveGeomPt | null
  } | null
  quality?: { score?: number | null; subscores?: Record<string, number | null>; weights?: Record<string, number>; note?: string } | null
  evidence?: {
    paths?: number[][] | null
    horizons?: Record<string, LiveHorizon>
    summary?: { n?: number; ref_h?: number; pct_up?: number; pct_down?: number; avg_up?: number; avg_down?: number } | null
    ref_h?: number
  } | null
  paths?: { winners?: number[] | null; losers?: number[] | null; n_win?: number | null; n_loss?: number | null; n_total?: number | null; small_n?: boolean; note?: string } | null
  decision?: {
    decision?: string | null      // TRADE | WATCH | NO_TRADE  (verdict source of truth)
    reason?: string; basis?: string | null; spec_note?: string | null
    etv?: number | null; edge?: number | null
    gates?: { gate: string; pass: boolean | null; reason?: string }[]
    policy?: Record<string, unknown>
    strategy?: { version?: string; n?: number; etv?: number; win?: number; payoff?: number; avg_win?: number; avg_loss?: number; mae?: number; ci_low?: number; avg_holding?: number } | null
  } | null
  watch_plan?: { confirmation?: number | null; warning?: number | null; invalidation?: number | null; direction?: string; note?: string } | null
  note?: string
  error?: string
}

// ── Detection (list-level) — everything the middle column & agent desk need ──
export type Detection = {
  id: string
  row: ScanRow            // the raw scan row (source of truth for the click-through)
  symbol: string
  pattern: string         // display name
  patternId: string
  family: PatternFamily
  direction: 'bullish' | 'bearish'
  stage: string
  volumeX: number | null
  touches: number
  level: number | null
  distancePct: number | null
  // list-time status: honestly 'watch' by default — qualification requires the
  // evidence bundle (fetched on click). We never colour a row 'qualified' here.
  status: Status
}

export function toDetection(r: ScanRow): Detection {
  const id = `${r.stock}::${r.pattern}`
  return {
    id, row: r,
    symbol: r.stock,
    pattern: patternShort(r.pattern),
    patternId: r.pattern,
    family: patternFamily(r.pattern),
    direction: r.direction === 'short' ? 'bearish' : 'bullish',
    stage: r.stage,
    volumeX: r.volume_x ?? null,
    touches: r.touches ?? 0,
    level: r.level ?? null,
    distancePct: r.distance_pct ?? null,
    status: 'watch',
  }
}

// list-time ranking score from the fields the SCAN actually carries (no faked
// quality/etv): stage weight → volume → touches → proximity to level.
const STAGE_W: Record<string, number> = { BREAKOUT: 3, RETEST: 2, APPROACHING: 1, FAILED: 0 }
export function rankScore(d: Detection): number {
  const st = STAGE_W[d.stage] ?? 0
  const vol = Math.min(d.volumeX ?? 0, 20)
  const near = d.distancePct == null ? 0 : Math.max(0, 8 - Math.abs(d.distancePct)) / 8
  return st * 100 + vol * 3 + d.touches * 1.5 + near * 2
}

export function rankDetections(list: Detection[]): Detection[] {
  return [...list].sort((a, b) => rankScore(b) - rankScore(a))
}

// ── ScanRun summary (real counts; unknowns stay null → UI shows "—") ─────────
export type ScanSummary = {
  universe: number | null
  scanned: number | null
  detections: number | null
  meaningful: number | null   // backend statistically_meaningful (usually null today)
  qualified: number | null    // backend qualified (usually null today)
  byPattern: Record<string, number>
}

export function toSummary(scan: ScanResp | null, dets: Detection[]): ScanSummary {
  const byPattern: Record<string, number> = scan?.by_pattern ? { ...scan.by_pattern } : {}
  if (!scan?.by_pattern) dets.forEach((d) => { byPattern[d.patternId] = (byPattern[d.patternId] || 0) + 1 })
  return {
    universe: scan?.universe_size ?? null,
    scanned: scan?.scanned ?? null,
    detections: scan?.count ?? dets.length,
    meaningful: scan?.statistically_meaningful ?? null,
    qualified: scan?.qualified ?? null,
    byPattern,
  }
}

// ── evidence-derived status (only when the setup bundle is loaded) ───────────
// Verdict is the SOURCE OF TRUTH (honesty rule). We map the backend verdict to a
// display status; we do NOT run a parallel gate rule that could disagree with it.
export function statusFromVerdict(verdict?: string | null, gatesPassed?: number, gatesTotal?: number): Status {
  const v = (verdict || '').toUpperCase()
  if (v === 'TRADE' || v === 'QUALIFIED') return 'qualified'
  if (v === 'NO_TRADE' || v === 'REJECTED' || v === 'AVOID') return 'rejected'
  // WATCH → strong if most gates pass, else watch; logged only if nothing known
  if (v === 'WATCH') {
    if (gatesTotal && gatesPassed != null && gatesPassed >= Math.max(1, gatesTotal - 1)) return 'strong'
    return 'watch'
  }
  return 'watch'
}

export function verdictLabel(verdict?: string | null): 'TRADE' | 'WATCH' | 'NO TRADE' | '—' {
  const v = (verdict || '').toUpperCase()
  if (v === 'TRADE' || v === 'QUALIFIED') return 'TRADE'
  if (v === 'NO_TRADE' || v === 'REJECTED' || v === 'AVOID') return 'NO TRADE'
  if (v === 'WATCH') return 'WATCH'
  return '—'
}

// ── the six canonical decision gates, filled from backend gates by keyword ───
// Any slot with no matching backend gate is left null (unknown) — never a faked
// pass. The backend short-circuits, so most setups show Context failing (thin n).
export type GateSlot = { name: string; pass: boolean | null; detail: string | null }
const GATE_KEYS: { name: string; match: RegExp }[] = [
  { name: 'Context',     match: /sample|context|g1/i },
  { name: 'Structure',   match: /structure|quality|g2/i },
  { name: 'Breakout',    match: /breakout|confirm|g3/i },
  { name: 'Edge',        match: /edge|expectanc|etv|g4/i },
  { name: 'Risk/Reward', match: /risk|reward|payoff|g5/i },
  { name: 'Volume',      match: /volume|g6/i },
]
export function sixGates(setup: LiveSetup | null): { slots: GateSlot[]; passed: number; total: number } {
  const raw = setup?.decision?.gates ?? []
  const slots: GateSlot[] = GATE_KEYS.map((k) => {
    const hit = raw.find((g) => k.match.test(g.gate) || (g.reason ? k.match.test(g.reason) : false))
    return hit
      ? { name: k.name, pass: hit.pass ?? null, detail: hit.reason ?? null }
      : { name: k.name, pass: null, detail: null }
  })
  const evaluated = slots.filter((s) => s.pass != null)
  return { slots, passed: evaluated.filter((s) => s.pass).length, total: evaluated.length }
}

// ── quality bars (7) — subscores are 0–1 in the backend; ×100, clamp 0–100 ────
// Market Alignment is NOT in the backend subscores → returned as null (omit/"—").
export type QualityBar = { label: string; value: number | null }
const clamp = (v: number) => Math.max(0, Math.min(100, Math.round(v)))
export function qualityBars(setup: LiveSetup | null): { ring: number | null; bars: QualityBar[]; caption: string } {
  const sub = setup?.quality?.subscores ?? {}
  const b = (k: string): number | null => {
    const v = sub[k]
    return v == null ? null : clamp(v * 100)
  }
  const bars: QualityBar[] = [
    { label: 'Structure Quality',     value: b('structure') },
    { label: 'Touch Quality',         value: b('touch_quality') },
    { label: 'Contraction Tightness', value: b('contraction') },
    { label: 'Breakout Strength',     value: b('breakout_strength') },
    { label: 'Volume Confirmation',   value: b('volume_confirmation') },
    { label: 'Level Quality',         value: b('level_quality') },
    { label: 'Market Alignment',      value: null },  // not in backend — honest "—"
  ]
  const ring = setup?.quality?.score == null ? null : clamp(setup.quality.score)
  const caption =
    ring == null ? 'Quality score pending the evidence bundle.'
    : ring >= 85 ? 'Excellent · high-conviction structure'
    : ring >= 70 ? 'Good · clean structure'
    : ring >= 55 ? 'Fair · developing structure'
    : 'Weak · loose structure'
  return { ring, bars, caption }
}

// ── historical-evidence figures (real, from the strategy-replay + summary) ────
export type Edge = { n: number | null; winT5: number | null; etv: number | null }
export function edgeFor(setup: LiveSetup | null): Edge {
  const s = setup?.decision?.strategy
  const sum = setup?.evidence?.summary
  const h5 = setup?.evidence?.horizons?.['5']
  return {
    n: s?.n ?? sum?.n ?? null,
    winT5: h5?.win ?? (s?.win ?? null),
    etv: s?.etv ?? setup?.decision?.etv ?? null,
  }
}

// horizons table (T+1/T+3/T+5/T+10) straight from backend horizons map
export type HRow = { h: string; win: number | null; med: number | null; mfe: number | null; mae: number | null }
export function horizonRows(setup: LiveSetup | null): HRow[] {
  const hz = setup?.evidence?.horizons
  if (!hz) return []
  return ['1', '3', '5', '10'].filter((k) => hz[k]).map((k) => {
    const r = hz[k]
    return { h: `T+${k}`, win: r.win ?? null, med: r.median ?? r.mean ?? null, mfe: r.mfe ?? null, mae: r.mae ?? null }
  })
}

// ── T+5 return distribution — DERIVED from the real per-case evidence.paths ──
// Each inner array is a case's cumulative % by horizon index (0=T0 … 10=T+10).
// We bucket the T+5 value of every case into the handoff's 8 buckets. If the
// backend gives no per-case paths, we return null and the histogram is omitted.
export type HistBucket = { label: string; count: number; positive: boolean }
export function distribution(setup: LiveSetup | null): { buckets: HistBucket[]; n: number } | null {
  const paths = setup?.evidence?.paths
  if (!paths || paths.length === 0) return null
  const refH = setup?.evidence?.ref_h ?? 5
  const idx = Math.min(refH, 5)  // T+5 return
  // evidence.paths are FRACTIONAL cumulative returns (verified: the median at
  // idx 5 equals horizons["5"].median once ×100), so scale to percent here.
  const vals: number[] = []
  for (const p of paths) { if (Array.isArray(p) && p.length > idx && typeof p[idx] === 'number') vals.push(p[idx] * 100) }
  if (vals.length === 0) return null
  const edges = [-6, -4, -2, 0, 2, 4, 6, 8, Infinity]
  const labels = ['<-4%', '-4%', '-2%', '0%', '+2%', '+4%', '+6%', '+8%']
  const buckets: HistBucket[] = labels.map((label, i) => ({ label, count: 0, positive: edges[i] >= 0 }))
  for (const v of vals) {
    let bi = 0
    for (let i = 0; i < edges.length - 1; i++) { if (v < edges[i + 1]) { bi = i; break } bi = edges.length - 2 }
    buckets[bi].count++
  }
  return { buckets, n: vals.length }
}

// ── win/loss trajectory series (real) ─────────────────────────────────────────
export function pathSeries(setup: LiveSetup | null): { winners: number[] | null; losers: number[] | null; nWin: number | null; nLoss: number | null } {
  const p = setup?.paths
  return {
    winners: p?.winners && p.winners.length ? p.winners : null,
    losers: p?.losers && p.losers.length ? p.losers : null,
    nWin: p?.n_win ?? null,
    nLoss: p?.n_loss ?? null,
  }
}

// symbol-level facts from the REAL point-in-time bars (not invented)
export function symbolFacts(bars: Bar[]): { ltp: number | null; change: number | null; changePct: number | null; ohlc: Bar | null } {
  if (!bars || bars.length === 0) return { ltp: null, change: null, changePct: null, ohlc: null }
  const last = bars[bars.length - 1]
  // find the previous DISTINCT trading bar (the feed sometimes duplicates a date)
  let prev = null as Bar | null
  for (let i = bars.length - 2; i >= 0; i--) { if (bars[i].date !== last.date) { prev = bars[i]; break } }
  const change = prev ? last.c - prev.c : null
  const changePct = prev && prev.c ? (change! / prev.c) * 100 : null
  return { ltp: last.c, change, changePct, ohlc: last }
}
