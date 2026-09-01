// Chart Agent surface — shared tokens + status/format helpers.
//
// ONE design system: these values mirror the existing Kanida dark + mint identity
// already declared in app/globals.css (the --f2-* / --color-mint-* tokens the rest
// of the /power shell uses). This is NOT a parallel palette — it is the same mint
// (#3FE3A4) + near-black system, exposed as an inline-style object because the
// candlestick chart draws with SVG fills/strokes that cannot use Tailwind classes.
import * as A from '@/lib/agents-api'

export const AT = {
  // structure (mirror --f2-canvas/panel/card + neutral rail)
  bg:     '#0a100e',
  panel:  '#0c1310',
  card:   '#111b16',
  card2:  '#0f1814',
  raised: '#14201a',
  line:   'rgba(255,255,255,0.065)',
  line2:  'rgba(255,255,255,0.10)',
  // accents (Kanida mint = the operator-locked green accent)
  mint:    '#3fe3a4',
  mintHi:  '#5aecb5',
  mintDim: 'rgba(63,227,164,0.10)',
  amber:   '#e6b450',
  amberDim:'rgba(230,180,80,0.10)',
  red:     '#e8736b',
  redDim:  'rgba(232,115,107,0.10)',
  teal:    '#4bcbe0',
  // text ramp (white → grey → dim)
  ink:   '#e9f2ec',
  ink2:  '#c2d0c9',
  muted: '#859990',
  faint: '#5b6c64',
} as const

export type FeedStatus = 'QUALIFIED' | 'WATCH' | 'NO_TRADE'

export const STATUS_META: Record<FeedStatus, { label: string; glyph: string; color: string; dim: string }> = {
  QUALIFIED: { label: 'QUALIFIED', glyph: '⚡', color: AT.mint,  dim: AT.mintDim },
  WATCH:     { label: 'WATCH',     glyph: '◉', color: AT.amber, dim: AT.amberDim },
  NO_TRADE:  { label: 'NO TRADE',  glyph: '△', color: AT.red,   dim: AT.redDim },
}

// tier → the 3-way feed status. This is consistent with the scan summary:
//   qualified-tier count === scan.qualified. weak → NO TRADE. everything else → WATCH.
// The per-setup source of truth (decision.decision) is shown in the expanded MY DECISION
// section; the collapsed badge derives from the scan tier the same job produced.
export function statusFromTier(tier?: A.Tier | null): FeedStatus {
  if (tier === 'qualified') return 'QUALIFIED'
  if (tier === 'weak') return 'NO_TRADE'
  return 'WATCH'
}

// map a live decision verdict string → a feed status (for the expanded MY DECISION tone)
export function statusFromVerdict(v?: string | null): FeedStatus {
  const u = (v || '').toUpperCase()
  if (u === 'TRADE' || u === 'QUALIFIED') return 'QUALIFIED'
  if (u === 'NO_TRADE' || u === 'NO TRADE' || u === 'REJECTED' || u === 'AVOID') return 'NO_TRADE'
  return 'WATCH'
}

// ── format helpers (tabular numbers) ──
export const pctS = (v?: number | null, dp = 1) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
export const pct  = (v?: number | null, dp = 0) => (v == null ? '—' : v.toFixed(dp) + '%')
export const rupee = (v?: number | null) => (v == null ? '—' : '₹' + v.toLocaleString('en-IN', { maximumFractionDigits: 2 }))
export const num = (v?: number | null) => (v == null ? '—' : String(v))
export const titleCase = (s: string) => s.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())

// ── pattern categories for the left nav (grouped families with real counts) ──
export type CatKey = 'all' | 'Horizontal' | 'Triangle' | 'Channel' | 'Wedge' | 'Cup' | 'Other'
export const CATEGORY_LABEL: Record<Exclude<CatKey, 'all'>, string> = {
  Horizontal: 'Breakouts',
  Triangle:   'Triangles',
  Channel:    'Channels',
  Wedge:      'Wedges',
  Cup:        'Cup & Handle',
  Other:      'Other',
}
// the display order of the four headline families in the spec
export const CATEGORY_ORDER: Exclude<CatKey, 'all'>[] = ['Horizontal', 'Triangle', 'Channel', 'Wedge', 'Cup', 'Other']
