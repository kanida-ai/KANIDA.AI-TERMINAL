'use client'

/**
 * Column 2 — AI Storyline. "What is the AI telling me?"
 * A self-typing newswire driven by the module-level narration engine. Blocks:
 * say / thinking (with tool-trace chips) / section / rows (revealed one at a time)
 * / more (progressive reveal) / user. Follow-up chips + Replay when narration ends.
 *
 * Every line is DERIVED from real scan data via the handoff copy templates
 * (narration.ts). Row clicks drive Column 3.
 */
import { useMemo, useState } from 'react'
import { V, FONT, STATUS_META } from './tokens'
import type { Detection } from './data'
import { useNarration, replay, skipToEnd, appendTurn, branch, type Block } from './narration'

const FOLLOWUPS: { id: string; label: string }[] = [
  { id: 'why', label: 'How did you decide?' },
  { id: 'rejected', label: 'What did you reject today?' },
  { id: 'tomorrow', label: 'What are you watching tomorrow?' },
  { id: 'yesterday', label: "How is yesterday's call doing?" },
]

export function Storyline({ ranked, hookFor, selectedId, onSelect, ts }: {
  ranked: Detection[]
  hookFor: (d: Detection) => string
  selectedId: string | null
  onSelect: (d: Detection) => void
  ts: string
}) {
  const { state, done, typingText, partialTyping, currentRows } = useNarration()
  const [extra, setExtra] = useState(0)
  const byId = useMemo(() => { const m = new Map<string, Detection>(); ranked.forEach((d) => m.set(d.id, d)); return m }, [ranked])

  const ask = (id: string) => {
    const { user, answer } = branch(id, ranked, hookFor, ts)
    appendTurn(user, answer as Block[], id)
  }

  // ids already scripted in rows blocks (avoid dupes in progressive reveal)
  const scriptedIds = new Set<string>()
  state.blocks.forEach((b) => { if (b.kind === 'rows') b.ids.forEach((id) => scriptedIds.add(id)) })
  const extraRows = ranked.filter((d) => !scriptedIds.has(d.id)).slice(0, extra)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      {/* header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '12px 14px', borderBottom: `1px solid ${V.border}` }}>
        <span style={{ width: 22, height: 22, borderRadius: 6, background: V.sel, color: V.cyan, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 13 }}>◈</span>
        <span style={{ fontFamily: FONT.sans, fontSize: 13, fontWeight: 600, letterSpacing: '0.03em', color: V.text }}>AI STORYLINE</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 8.5, letterSpacing: '0.1em', padding: '2px 6px', borderRadius: 4, background: '#0b2320', border: '1px solid #1c5c48', color: V.greenHi }}>LIVE</span>
        <span style={{ fontFamily: FONT.sans, fontSize: 11, color: V.faint, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>Real-time pattern intelligence from Chart Pattern Agent</span>
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 10 }}>
          <svg width="34" height="18" aria-hidden>
            {[7, 12, 9, 16, 11, 6, 13].map((h, i) => (
              <rect key={i} x={i * 5} y={18 - h} width={2} height={h} fill={V.cyan} opacity={0.4 + (i % 3) * 0.2} />
            ))}
          </svg>
          <button onClick={done ? replay : skipToEnd}
            style={{ padding: '5px 10px', borderRadius: 14, border: `1px solid ${V.borderStrong}`, background: V.raised, color: V.dim, fontFamily: FONT.sans, fontSize: 10.5, cursor: 'pointer' }}>
            {done ? '↻ Replay' : '↻ Writing…'}
          </button>
        </div>
      </div>

      {/* feed */}
      <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: '10px 12px 16px', display: 'flex', flexDirection: 'column', gap: 3 }}>
        {state.blocks.map((b, i) => {
          if (i > state.shown) return null
          const isCurrent = i === state.shown
          return <BlockView key={i} b={b} isCurrent={isCurrent} typingText={typingText} partial={partialTyping}
            rowsShown={isCurrent ? currentRows : (b.kind === 'rows' ? b.ids.length : 0)}
            byId={byId} hookFor={hookFor} selectedId={selectedId} onSelect={onSelect}
            onMore={() => setExtra((e) => e + 20)} extraCount={extra} />
        })}

        {/* progressive-reveal extra rows (real, beyond the scripted set) */}
        {extraRows.map((d) => (
          <FindingRow key={`x-${d.id}`} d={d} hook={hookFor(d)} selected={selectedId === d.id} onClick={() => onSelect(d)} rank={null} />
        ))}

        {/* follow-up chips */}
        {done && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginTop: 12 }}>
            {FOLLOWUPS.filter((f) => !state.asked.includes(f.id)).slice(0, 3).map((f) => (
              <button key={f.id} onClick={() => ask(f.id)}
                style={{ padding: '7px 12px', borderRadius: 16, border: `1px solid ${V.borderStrong}`, background: V.raised, color: V.muted, fontFamily: FONT.sans, fontSize: 12.5, cursor: 'pointer' }}>
                {f.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* footer */}
      <div style={{ padding: 11, borderTop: `1px solid ${V.border}`, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
        <div style={{ display: 'flex', gap: 4 }}>
          {[0, 1, 2].map((i) => <span key={i} style={{ width: 5, height: 5, borderRadius: '50%', background: V.cyan, animation: `kaDot 1.4s ${i * 0.2}s infinite` }} />)}
        </div>
        <span style={{ fontFamily: FONT.sans, fontSize: 10.5, color: V.faint }}>{done ? 'AI is continuously scanning markets…' : 'AI is writing the storyline…'}</span>
      </div>
    </div>
  )
}

function BlockView({ b, isCurrent, typingText, partial, rowsShown, byId, hookFor, selectedId, onSelect, onMore, extraCount }: {
  b: Block; isCurrent: boolean; typingText: string; partial: boolean; rowsShown: number
  byId: Map<string, Detection>; hookFor: (d: Detection) => string
  selectedId: string | null; onSelect: (d: Detection) => void; onMore: () => void; extraCount: number
}) {
  const rise: React.CSSProperties = { animation: 'kaRise 340ms ease-out' }

  if (b.kind === 'say') {
    const text = isCurrent ? typingText : b.text
    return (
      <div style={{ ...rise, display: 'grid', gridTemplateColumns: '52px 20px minmax(0,1fr)', gap: 8, padding: '7px 6px' }}>
        <span style={{ fontFamily: FONT.mono, fontSize: 10.5, color: V.faint }}>{b.ts || ''}</span>
        <span style={{ width: 16, height: 16, borderRadius: '50%', border: `1.5px solid ${isCurrent && partial ? V.cyan : V.faint}`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 8, color: V.faint }}>✓</span>
        <span style={{ fontFamily: FONT.sans, fontSize: 13, lineHeight: 1.6, color: V.body }}>
          {text}{isCurrent && partial && <span style={{ display: 'inline-block', width: 6, height: 14, marginLeft: 1, background: V.cyan, verticalAlign: 'text-bottom', animation: 'kaCaret 1s step-end infinite' }} />}
        </span>
      </div>
    )
  }

  if (b.kind === 'thinking') {
    if (!isCurrent) {
      return (
        <div style={{ ...rise, display: 'grid', gridTemplateColumns: '52px 20px minmax(0,1fr)', gap: 8, padding: '7px 6px' }}>
          <span />
          <span style={{ width: 16, height: 16, borderRadius: '50%', border: `1.5px solid ${V.faint}`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 8, color: V.faint }}>✓</span>
          <span style={{ fontFamily: FONT.sans, fontSize: 13, lineHeight: 1.6, color: V.dim }}>{b.label.replace(/…$/, '')} — done.</span>
        </div>
      )
    }
    return (
      <div style={{ ...rise, margin: '6px 4px', padding: '10px 12px', border: `1px solid ${V.thinkBorder}`, borderRadius: 9, background: V.inset, position: 'relative', overflow: 'hidden' }}>
        <span style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 1, background: `linear-gradient(90deg, transparent, ${V.cyan}, transparent)`, animation: 'kaSweep 1.7s linear infinite' }} />
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 5, height: 5, borderRadius: '50%', background: V.cyan, animation: 'kaPulse 1.4s infinite' }} />
          <span style={{ fontFamily: FONT.sans, fontSize: 12, color: V.dim }}>{b.label}</span>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginTop: 8 }}>
          {b.chips.map((c) => (
            <span key={c} style={{ fontFamily: FONT.mono, fontSize: 9.5, padding: '3px 7px', borderRadius: 4, background: V.chip, border: `1px solid ${V.thinkBorder}`, color: V.dim }}>{c}</span>
          ))}
        </div>
      </div>
    )
  }

  if (b.kind === 'section') {
    return (
      <div style={{ ...rise, display: 'flex', alignItems: 'center', gap: 8, padding: '14px 4px 8px' }}>
        <span style={{ color: b.color }}>{b.glyph}</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 10, letterSpacing: '0.12em', color: b.color }}>{b.label}</span>
        <span style={{ fontFamily: FONT.sans, fontSize: 10.5, color: V.faint }}>{b.note}</span>
        <span style={{ flex: 1, height: 1, background: V.hairline }} />
      </div>
    )
  }

  if (b.kind === 'rows') {
    return (
      <div style={rise}>
        {b.ids.slice(0, rowsShown).map((id, k) => {
          const d = byId.get(id)
          if (!d) return null
          return <FindingRow key={id} d={d} hook={hookFor(d)} selected={selectedId === id} onClick={() => onSelect(d)} rank={b.ranked ? k + 1 : null} />
        })}
      </div>
    )
  }

  if (b.kind === 'more') {
    return (
      <button onClick={onMore}
        style={{ ...rise, width: '100%', padding: '9px 0', borderRadius: 8, border: `1px solid ${V.thinkBorder}`, background: V.inset, color: V.cyan, fontFamily: FONT.sans, fontSize: 11.5, cursor: 'pointer', marginTop: 4 }}>
        {extraCount > 0 ? `Showing ${extraCount} more — view further ›` : b.label + ' ›'}
      </button>
    )
  }

  if (b.kind === 'user') {
    const text = isCurrent ? typingText : b.text
    return (
      <div style={{ ...rise, display: 'flex', justifyContent: 'flex-end' }}>
        <span style={{ maxWidth: '80%', padding: '9px 14px', borderRadius: '14px 14px 4px 14px', background: V.sel, fontFamily: FONT.sans, fontSize: 12.5, color: V.tick }}>{text}</span>
      </div>
    )
  }
  return null
}

function FindingRow({ d, hook, selected, onClick, rank }: { d: Detection; hook: string; selected: boolean; onClick: () => void; rank: number | null }) {
  const m = STATUS_META[d.status]
  return (
    <button onClick={onClick}
      style={{ width: '100%', display: 'grid', gridTemplateColumns: '52px 20px 20px minmax(0,1fr) 12px', gap: 8, alignItems: 'center',
        padding: '9px 8px', borderRadius: 8, cursor: 'pointer', textAlign: 'left', marginBottom: 1,
        border: `1px solid ${selected ? V.borderActive : 'transparent'}`, background: selected ? V.hover : 'transparent' }}
      onMouseEnter={(e) => { if (!selected) e.currentTarget.style.background = V.hover }}
      onMouseLeave={(e) => { if (!selected) e.currentTarget.style.background = 'transparent' }}>
      <span style={{ fontFamily: FONT.mono, fontSize: 10.5, color: V.faint }}> </span>
      <span style={{ fontSize: 11, color: m.text, textAlign: 'center' }}>{m.glyph}</span>
      <span style={{ width: 18, height: 18, borderRadius: 5, background: selected ? V.sel : V.inset, color: V.dim, fontFamily: FONT.mono, fontSize: 9.5, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{rank ?? m.glyph}</span>
      <span style={{ fontFamily: FONT.sans, fontSize: 12.5, color: selected ? V.text : V.muted, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        <b style={{ color: V.tick, fontWeight: 600 }}>{d.symbol}</b> — {hook}
      </span>
      <span style={{ fontFamily: FONT.mono, fontSize: 11, color: V.faint }}>›</span>
    </button>
  )
}
