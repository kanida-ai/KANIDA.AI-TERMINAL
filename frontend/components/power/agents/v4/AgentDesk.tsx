'use client'

/**
 * Column 1 — Agent desk. "What should I look at?"
 *   • Market Scan Summary (4 tiles)  ← real ScanSummary counts (unknown → "—")
 *   • Pattern Categories (4 families + "View all 9 detectors")  ← manifest + by_pattern
 *   • Agent Network (23 roster)  ← HONEST statuses: only Chart Pattern is Active;
 *     every other agent is Soon / Queued / coming — never a faked Active.
 *   • Collapse control → 60px initials rail (authored state, all 23 stay present).
 */
import { useState } from 'react'
import { V, FONT } from './tokens'
import type { ScanSummary } from './data'
import type { PatternManifest } from '@/lib/agents-api'
import { patternShort } from '@/lib/agents-api'

// honest roster: Chart Pattern Agent LIVE, everything else not-yet-active.
type AgentStatus = 'active' | 'queued' | 'idle'
type Agent = { name: string; status: AgentStatus }
const ROSTER: Agent[] = [
  { name: 'Chart Pattern Agent', status: 'active' },
  { name: 'Options Flow', status: 'queued' }, { name: 'Earnings', status: 'queued' },
  { name: 'Sector Rotation', status: 'queued' }, { name: 'Gap Behaviour', status: 'idle' },
  { name: 'Volume Profile', status: 'queued' }, { name: 'Event & Filings', status: 'idle' },
  { name: 'Index Structure', status: 'idle' }, { name: 'News Flow', status: 'queued' },
  { name: 'Support / Resistance', status: 'queued' }, { name: 'Trend Regime', status: 'idle' },
  { name: 'Momentum Divergence', status: 'idle' }, { name: 'Volatility Squeeze', status: 'idle' },
  { name: 'Relative Strength', status: 'idle' }, { name: 'Delivery Quality', status: 'idle' },
  { name: 'FII / DII Flow', status: 'idle' }, { name: 'Breadth', status: 'queued' },
  { name: 'Risk Guardian', status: 'idle' }, { name: 'Position Sizing', status: 'idle' },
  { name: 'AutoTrade', status: 'idle' }, { name: 'Outcome Review', status: 'idle' },
  { name: 'Learning Loop', status: 'idle' }, { name: 'Portfolio Health', status: 'idle' },
]
const STATUS_META: Record<AgentStatus, { dot: string; text: string; word: string }> = {
  active: { dot: V.green, text: V.greenHi, word: 'Active' },
  queued: { dot: V.amber, text: V.amber, word: 'Soon' },
  idle:   { dot: '#2a3f52', text: V.faint, word: 'Queued' },
}

// initials: two uppercase alphanumerics, unique across the fleet (README rule).
function computeInitials(names: string[]): Record<string, string> {
  const out: Record<string, string> = {}
  const used = new Set<string>()
  const take = (cand: string) => { const c = cand.toUpperCase().slice(0, 2); return c.length === 2 && !used.has(c) ? c : null }
  for (const raw of names) {
    const name = raw.replace(/\s+Agent$/i, '')
    const words = name.split(/[^A-Za-z0-9]+/).filter(Boolean)
    const w1 = words[0] || 'X', w2 = words[1] || '', w3 = words[2] || ''
    const cands = [
      (w1[0] || '') + (w2[0] || w1[1] || 'X'),
      w1.slice(0, 2),
      (w2[0] || '') + (w2[1] || ''),
      (w1[0] || '') + (w3[0] || ''),
      (w1[0] || '') + (w1[2] || ''),
    ]
    let chosen: string | null = null
    for (const c of cands) { const t = take(c); if (t) { chosen = t; break } }
    if (!chosen) { for (let i = 65; i <= 90 && !chosen; i++) chosen = take((w1[0] || 'X') + String.fromCharCode(i)) }
    chosen = chosen || (w1[0] || 'X') + 'X'
    used.add(chosen); out[raw] = chosen
  }
  return out
}
const INITIALS = computeInitials(ROSTER.map((a) => a.name))

const FAMILIES: { label: string; ids: string[] }[] = [
  { label: 'Breakout Patterns', ids: ['horizontal_trendline', 'rectangle'] },
  { label: 'Continuation Patterns', ids: ['channel', 'cup_and_handle'] },
  { label: 'Triangle Patterns', ids: ['ascending_triangle', 'descending_triangle', 'symmetrical_triangle'] },
  { label: 'Wedge Patterns', ids: ['rising_wedge', 'falling_wedge'] },
]

export function AgentDesk({
  summary, patterns, selectedCat, onSelectCat, selectedAgent, onSelectAgent, collapsed, onToggleCollapse, loading,
}: {
  summary: ScanSummary
  patterns: PatternManifest[]
  selectedCat: string                 // 'all' | pattern id | family label
  onSelectCat: (c: string) => void
  selectedAgent: string
  onSelectAgent: (a: string) => void
  collapsed: boolean
  onToggleCollapse: () => void
  loading: boolean
}) {
  const [allCats, setAllCats] = useState(false)
  const [allAgents, setAllAgents] = useState(false)
  const fmt = (v?: number | null) => (loading ? '…' : v == null ? '—' : v.toLocaleString('en-IN'))
  const catCount = (ids: string[]) => ids.reduce((s, id) => s + (summary.byPattern[id] || 0), 0)

  if (collapsed) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        <div style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 4, padding: '8px 4px' }}>
          {ROSTER.map((a) => {
            const sel = selectedAgent === a.name
            const m = STATUS_META[a.status]
            return (
              <button key={a.name} title={a.name} onClick={() => onSelectAgent(a.name)}
                style={{ position: 'relative', width: 36, height: 28, margin: '0 auto', borderRadius: 6, cursor: 'pointer',
                  border: 'none', background: sel ? V.sel : 'transparent', color: sel ? V.text : V.dim,
                  fontFamily: FONT.mono, fontSize: 9.5 }}>
                {INITIALS[a.name]}
                <span style={{ position: 'absolute', top: 4, right: 4, width: 4, height: 4, borderRadius: '50%', background: m.dot }} />
              </button>
            )
          })}
        </div>
        <button onClick={onToggleCollapse} style={rowBtn(false)}>›</button>
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      <div style={{ flex: 1, overflowY: 'auto', padding: 11, display: 'flex', flexDirection: 'column', gap: 11 }}>
        {/* Market Scan Summary */}
        <Card>
          <Eyebrow>MARKET SCAN SUMMARY</Eyebrow>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0,1fr))', gap: 6, marginTop: 8 }}>
            <Tile v={fmt(summary.scanned)} l="Stocks Scanned" c={V.cyan} bg="#0a2233" bd="#1b4a6b" />
            <Tile v={fmt(summary.detections)} l="Patterns Detected" c={V.greenHi} bg="#0b2320" bd="#1c5c48" />
            <Tile v={fmt(summary.meaningful)} l="Statistically Meaningful" c={V.amber} bg="#241c07" bd="#5c4413" />
            <Tile v={fmt(summary.qualified)} l="Qualified Setups" c={V.violet} bg="#1a1630" bd="#3b3163" />
          </div>
        </Card>

        {/* Pattern Categories */}
        <div>
          <Eyebrow>PATTERN CATEGORIES</Eyebrow>
          <div style={{ marginTop: 6 }}>
            <CatRow label="All Opportunities" count={summary.detections ?? 0} sel={selectedCat === 'all'} onClick={() => onSelectCat('all')} />
            {!allCats
              ? FAMILIES.map((f) => (
                  <CatRow key={f.label} label={f.label} count={catCount(f.ids)} sel={selectedCat === f.label} onClick={() => onSelectCat(f.label)} />
                ))
              : patterns.map((p) => (
                  <CatRow key={p.pattern_id} label={patternShort(p.pattern_id, p.name)} count={summary.byPattern[p.pattern_id] || 0}
                    sel={selectedCat === p.pattern_id} onClick={() => onSelectCat(p.pattern_id)} />
                ))}
          </div>
          <button onClick={() => setAllCats((v) => !v)}
            style={{ background: 'none', border: 'none', color: V.cyan, fontFamily: FONT.sans, fontSize: 11.5, cursor: 'pointer', padding: '6px 4px' }}>
            {allCats ? 'Show fewer categories' : 'View all 9 detectors'}
          </button>
        </div>

        {/* Agent Network */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
            <Eyebrow>AGENT NETWORK</Eyebrow>
            <span style={{ fontFamily: FONT.sans, fontSize: 9.5, color: V.faint }}>23 Specialized AI Agents</span>
          </div>
          <div style={{ marginTop: 6 }}>
            {(allAgents ? ROSTER : ROSTER.slice(0, 10)).map((a) => {
              const sel = selectedAgent === a.name
              const m = STATUS_META[a.status]
              return (
                <button key={a.name} onClick={() => onSelectAgent(a.name)}
                  style={{ width: '100%', display: 'grid', gridTemplateColumns: '22px minmax(0,1fr) auto 8px', gap: 9, alignItems: 'center',
                    padding: '7px 9px', borderRadius: 7, cursor: 'pointer', textAlign: 'left', marginBottom: 1,
                    border: 'none', borderLeft: `2px solid ${sel ? V.cyan : 'transparent'}`,
                    background: sel ? V.sel : 'transparent' }}>
                  <span style={{ width: 18, height: 18, borderRadius: 5, background: V.inset, color: V.dim, fontFamily: FONT.mono, fontSize: 8.5,
                    display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{INITIALS[a.name]}</span>
                  <span style={{ fontFamily: FONT.sans, fontSize: 12, color: sel ? V.text : V.muted, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{a.name}</span>
                  <span style={{ fontFamily: FONT.sans, fontSize: 10, color: m.text }}>{m.word}</span>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: m.dot, justifySelf: 'end' }} />
                </button>
              )
            })}
          </div>
          <button onClick={() => setAllAgents((v) => !v)}
            style={{ width: '100%', marginTop: 6, padding: '9px 10px', borderRadius: 8, border: `1px solid ${V.borderStrong}`,
              background: V.raised, color: V.muted, fontFamily: FONT.sans, fontSize: 11.5, cursor: 'pointer' }}>
            {allAgents ? 'Show top 10 agents' : 'Explore 13 More Agents ›'}
          </button>
        </div>
      </div>

      {/* Collapse control — its own full-width row */}
      <div style={{ padding: 8 }}>
        <button onClick={onToggleCollapse} style={rowBtn(true)}>‹ COLLAPSE</button>
      </div>
    </div>
  )
}

// ── small pieces ──
function rowBtn(wide: boolean): React.CSSProperties {
  return { width: '100%', padding: '8px 0', borderRadius: 8, border: `1px solid ${V.border}`, background: V.panel,
    color: V.faint, fontFamily: FONT.mono, fontSize: wide ? 11 : 13, letterSpacing: wide ? '0.12em' : undefined, cursor: 'pointer' }
}
function Card({ children }: { children: React.ReactNode }) {
  return <div style={{ border: `1px solid ${V.border}`, borderRadius: 11, background: V.panel, padding: 13 }}>{children}</div>
}
function Eyebrow({ children }: { children: React.ReactNode }) {
  return <div style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.14em', color: V.faint }}>{children}</div>
}
function Tile({ v, l, c, bg, bd }: { v: string; l: string; c: string; bg: string; bd: string }) {
  return (
    <div style={{ borderRadius: 8, padding: '9px 5px', background: bg, border: `1px solid ${bd}`, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
      <span style={{ fontFamily: FONT.mono, fontSize: 17, fontWeight: 600, color: c, lineHeight: 1 }}>{v}</span>
      <span style={{ fontFamily: FONT.sans, fontSize: 8.5, color: V.dim, textAlign: 'center', lineHeight: 1.25 }}>{l}</span>
    </div>
  )
}
function CatRow({ label, count, sel, onClick }: { label: string; count: number; sel: boolean; onClick: () => void }) {
  return (
    <button onClick={onClick}
      style={{ width: '100%', display: 'grid', gridTemplateColumns: '22px minmax(0,1fr) auto', gap: 9, alignItems: 'center',
        padding: '7px 9px', borderRadius: 7, cursor: 'pointer', textAlign: 'left', marginBottom: 1,
        background: sel ? V.sel : 'transparent', border: `1px solid ${sel ? V.borderActive : 'transparent'}` }}>
      <span style={{ width: 18, height: 18, borderRadius: 5, background: sel ? '#1b4a6b' : '#0f2434', color: sel ? V.cyan : V.faint,
        fontFamily: FONT.mono, fontSize: 9, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>◧</span>
      <span style={{ fontFamily: FONT.sans, fontSize: 12, color: sel ? V.text : V.muted, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{label}</span>
      <span style={{ fontFamily: FONT.mono, fontSize: 11, color: sel ? V.cyan : V.faint }}>{count}</span>
    </button>
  )
}
