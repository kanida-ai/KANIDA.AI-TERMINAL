// KANIDA.AI Terminal — design tokens
// Single source of truth for all color, spacing, and typography values.
// Import this in every page/component. Never hardcode these values elsewhere.

export const T = {
  bg:     '#07070d',
  s1:     '#0c0c18',
  s2:     '#101022',
  s3:     '#14142a',
  b:      'rgba(255,255,255,0.08)',
  b2:     'rgba(255,255,255,0.14)',
  g:      '#00c98a',          // brand green — toned, not neon
  gd:     'rgba(0,201,138,0.08)',
  gb:     'rgba(0,201,138,0.20)',
  r:      '#ff4d6d',
  a:      '#ffd166',
  t:      '#f4f4fc',          // primary text
  t2:     '#d6d6ea',          // secondary text — clearly readable
  t3:     '#8888a8',          // meta labels only
  mono:   "'Courier New', monospace",
} as const

export const brand = {
  name: 'KANIDA',
  ai:   '.AI',
  mode: 'Terminal',
} as const
