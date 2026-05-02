'use client'

/**
 * Shared utilities for V3 ported workspaces.
 * - Color palette tuned for V3 dark terminal aesthetic
 * - Constants reused across multiple tabs
 * - Tiny presentational components: Chip, StatBox, KV
 */
import React from 'react'

// ── Color palette ─────────────────────────────────────────────────────────────
// Mapped from /analysis page's C object → V3 tokens. Same semantic names.
export const C = {
  bg:  '#000000',
  card:'#0a0a0c',
  border: '#1c1c22',
  b2:  '#2a2a32',
  t:   '#f5f5f7',
  t2:  '#9ca3af',
  t3:  '#6b7280',
  // Engine/state semantics
  green:  '#22c55e',
  red:    '#ef4444',
  amber:  '#f59e0b',   // V3's amber accent
  sky:    '#60a5fa',
  violet: '#a78bfa',
  orange: '#fb923c',
  indigo: '#a78bfa',
  // Backgrounds for chips
  gd: 'rgba(34,197,94,0.10)',
  rd: 'rgba(239,68,68,0.10)',
}

// ── Constants ─────────────────────────────────────────────────────────────────
export const BUCKET_META: Record<string, { label: string; color: string; icon: string }> = {
  turbo:    { label: 'TURBO',    color: C.amber, icon: '⚡' },
  super:    { label: 'SUPER',    color: C.green, icon: '◆' },
  standard: { label: 'STANDARD', color: C.sky,   icon: '◇' },
  trap:     { label: 'TRAP',     color: C.red,   icon: '×' },
}

export const REASON_COLORS: Record<string, string> = {
  FRESH_SIGNAL:     C.green,
  MULTI_PATTERN_2:  C.sky,
  MULTI_PATTERN_3:  C.amber,
}
export function reasonColor(r: string): string {
  return REASON_COLORS[r] || (r?.startsWith('MULTI') ? C.orange : C.t3)
}

export const STATUS_META: Record<string, { label: string; color: string; icon: string; pulse?: boolean }> = {
  pending_entry: { label: 'PENDING ENTRY', color: '#ffd166', icon: '⏳' },
  open:          { label: 'OPEN',          color: '#38bdf8', icon: '◉' },
  near_target:   { label: 'NEAR TARGET',   color: '#00c98a', icon: '✦', pulse: true },
  near_stop:     { label: 'NEAR STOP',     color: '#ff4d6d', icon: '⚠', pulse: true },
  target_hit:    { label: 'TARGET HIT',    color: '#00c98a', icon: '✓' },
  stop_hit:      { label: 'STOP HIT',      color: '#ff4d6d', icon: '✕' },
  expired:       { label: 'EXPIRED',       color: '#7878a0', icon: '—' },
}

export const EXEC_CODE_LABELS: Record<string, string> = {
  EARLY_ENTRY:          'Early Entry (9:15)',
  DELAYED_9_30:         'Delayed ~9:30',
  DELAYED_10_00:        'Delayed ~10:00',
  DELAYED_11_00:        'Delayed ~11:00',
  PULLBACK_ENTRY:       'Pullback Entry',
  RECLAIM_ENTRY_10:     'Reclaim ~10:00',
  RECLAIM_ENTRY_11:     'Reclaim ~11:00',
  NO_TRADE_GAP_CHASE:   'No Trade — Gap Chase',
  NO_TRADE_WEAK_OPEN:   'No Trade — Weak Open',
  NO_TRADE_INDEX_WEAK:  'No Trade — Index Weak',
  NO_TRADE_VOLATILE:    'No Trade — Volatile',
}
export const EXEC_CODE_COLORS: Record<string, string> = {
  EARLY_ENTRY:          C.green,
  DELAYED_9_30:         C.sky,
  DELAYED_10_00:        C.sky,
  DELAYED_11_00:        C.amber,
  PULLBACK_ENTRY:       C.violet,
  RECLAIM_ENTRY_10:     C.violet,
  RECLAIM_ENTRY_11:     C.violet,
  NO_TRADE_GAP_CHASE:   C.red,
  NO_TRADE_WEAK_OPEN:   C.red,
  NO_TRADE_INDEX_WEAK:  C.orange,
  NO_TRADE_VOLATILE:    C.orange,
}
export const GAP_CAT_COLORS: Record<string, string> = {
  BIG_GAP_UP:    C.green,
  GAP_UP:        '#66efb0',
  FLAT:          C.sky,
  GAP_DOWN:      C.amber,
  BIG_GAP_DOWN:  C.red,
}

// ── Formatters ───────────────────────────────────────────────────────────────
export const pct  = (v: number | null | undefined, d = 1) => v == null ? '—' : `${v.toFixed(d)}%`
export const pctS = (v: number | null | undefined, d = 1) => v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(d)}%`
export const price = (v: number | null | undefined) => v == null ? '—' : v >= 1000
  ? v.toLocaleString('en-IN', { maximumFractionDigits: 0 })
  : v.toFixed(2)

// ── Mini-components ──────────────────────────────────────────────────────────
export function Chip({ val, color, small }: { val: string; color: string; small?: boolean }) {
  return (
    <span style={{
      background: `${color}22`, color, border: `1px solid ${color}55`,
      padding: small ? '1px 6px' : '3px 9px',
      fontSize: small ? 10 : 11, fontWeight: 600, letterSpacing: '0.04em',
      whiteSpace: 'nowrap', display: 'inline-block',
    }}>{val}</span>
  )
}

export function StatBox({ label, value, color, sub }: {
  label: string; value: string; color?: string; sub?: string
}) {
  return (
    <div style={{
      background: C.card, border: `1px solid ${C.border}`,
      padding: '12px 16px', minWidth: 140, flex: 1,
    }}>
      <div style={{ fontSize: 10, color: C.t3, letterSpacing: '0.06em' }}>{label}</div>
      <div style={{
        fontSize: 22, fontWeight: 700, color: color || C.t,
        marginTop: 4, fontFamily: 'IBM Plex Mono, monospace', fontFeatureSettings: '"tnum" 1',
      }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: C.t3, marginTop: 3 }}>{sub}</div>}
    </div>
  )
}

export function KV({ label, val, color }: { label: string; val: string; color?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
      <span style={{ color: C.t3, fontSize: 11 }}>{label}</span>
      <span style={{ color: color || C.t2, fontSize: 11, fontWeight: 600, fontFamily: 'IBM Plex Mono, monospace' }}>{val}</span>
    </div>
  )
}
