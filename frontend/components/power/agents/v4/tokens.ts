// Chart Agent v4 design tokens — the navy/cyan agent-network palette from the
// design handoff (design_handoff_chart_pattern_agent/README.md §"Design tokens").
// This surface deliberately does NOT use the terminal green theme (lib/theme.ts):
// the v4 handoff specifies its own colour system, and only the /power/agents page
// consumes these. Exact hex values are copied from the handoff colour table.

export const V = {
  // structure
  bg: '#050d15',          // page
  panel: '#081420',       // the three column panels, top bar
  inset: '#0a1b29',       // cards inside panels
  raised: '#0b1c2c',      // secondary buttons
  sel: '#12304a',         // selected rows, icon tiles
  hover: '#0e2436',       // row hover
  chip: '#0c2134',        // tool-trace chips

  // borders
  border: '#14304a',      // panel borders
  borderStrong: '#1b3c58',// buttons
  borderActive: '#1f4a6b',// selected rows
  hairline: '#12293d',    // table rules, grid gaps
  grid: '#0f2434',        // chart gridlines
  thinkBorder: '#17334a', // thinking block border

  // accents
  cyan: '#38bdf8',        // agent identity, active tab, links
  cyanHi: '#7dd3fc',      // link hover
  blue: '#3b82f6',
  blueDeep: '#1d4ed8',
  green: '#22c55e',       // up candles, pass
  greenHi: '#4ade80',     // qualified, live status
  amber: '#f59e0b',       // watch, warning
  red: '#ef4444',         // down candles, fail, rejected
  violet: '#a78bfa',      // qualified count, target level, armed

  // text (floor for <12px text is #7d93aa; #8fa6bb is the floor for 8-9px labels)
  text: '#f1f6fb',        // headings, selected
  body: '#c3d2e0',        // storyline prose
  tick: '#dbe7f2',        // tickers
  muted: '#a9bdd1',       // row text, labels
  dim: '#8fa6bb',         // captions, small labels
  faint: '#7d93aa',       // timestamps, micro-labels — the floor for text
} as const

// Fonts — CSS variables defined in app/layout.tsx (next/font IBM Plex).
export const FONT = {
  sans: 'var(--font-plex-sans), system-ui, sans-serif',
  mono: 'var(--font-plex-mono), ui-monospace, monospace',
} as const

// Status → colour + label + glyph. Status is derived from the backend decision
// verdict (see data.ts); qualified is honestly rare with sparse precedents.
export type Status = 'qualified' | 'strong' | 'watch' | 'rejected' | 'logged'

export const STATUS_META: Record<Status, {
  label: string; text: string; bg: string; border: string; glyph: string
}> = {
  qualified: { label: 'QUALIFIED SETUP', text: V.greenHi, bg: '#0b2320', border: '#1c5c48', glyph: '⚡' },
  strong:    { label: 'STRONG SETUP',    text: V.cyan,    bg: '#0a2233', border: '#1b4a6b', glyph: '⚡' },
  watch:     { label: 'WATCHING',        text: V.amber,   bg: '#241c07', border: '#5c4413', glyph: '◉' },
  rejected:  { label: 'REJECTED',        text: V.red,     bg: '#2a1214', border: '#5c1f24', glyph: '△' },
  logged:    { label: 'LOGGED',          text: V.faint,   bg: '#0f1d29', border: '#1b3c58', glyph: '○' },
}
