'use client'

/**
 * Narration engine for the AI Storyline (Column 2). The storyline TYPES ITSELF
 * out from the real scan data using the handoff's copy templates.
 *
 * Two handoff-mandated correctness rules are honoured here (README §Interactions):
 *   1. ELAPSED-TIME clock — we advance `floor((now-last)/STEP)` steps per frame
 *      (capped) rather than one step per frame, so a throttled/background tab does
 *      not freeze the typing.
 *   2. Progress lives in a MODULE-LEVEL store (below), not component state, so a
 *      re-mount does not reset the narration mid-stream.
 *
 * All copy is derived from real backend numbers — no fabricated observations.
 */
import { useEffect, useState } from 'react'
import type { Detection, ScanSummary } from './data'
import { V } from './tokens'

export type Block =
  | { kind: 'say'; ts?: string; text: string }
  | { kind: 'thinking'; label: string; chips: string[] }
  | { kind: 'section'; glyph: string; label: string; note: string; color: string }
  | { kind: 'rows'; ids: string[]; ranked: boolean }
  | { kind: 'more'; label: string }
  | { kind: 'user'; text: string }

const STEP = 26            // ms per narration step (elapsed-time based)
const SPEED = 3            // chars per step for say/user
const HOLD_TEXT = 10       // steps to hold after a line finishes typing
const HOLD_THINK = 26      // steps a thinking block holds before collapsing
const ROW_EVERY = 6        // steps between revealing successive rows

type State = {
  blocks: Block[]
  shown: number            // count of fully-revealed blocks
  chars: number            // chars revealed in the current typing block
  rows: number             // rows revealed in the current rows block
  hold: number             // hold-step counter within the current block
  last: number             // ms timestamp of previous tick
  asked: string[]          // follow-up ids already used
}

const store: { s: State } = {
  s: { blocks: [], shown: 0, chars: 0, rows: 0, hold: 0, last: 0, asked: [] },
}
const listeners = new Set<() => void>()
function emit() { listeners.forEach((l) => l()) }

export function isDone(s: State): boolean {
  return s.shown >= s.blocks.length
}

function currentTextLen(s: State): number {
  const b = s.blocks[s.shown]
  return b && (b.kind === 'say' || b.kind === 'user') ? b.text.length : 0
}

// advance the cursor by `steps` elapsed steps
function advance(steps: number) {
  const s = store.s
  let left = steps
  while (left > 0 && !isDone(s)) {
    const b = s.blocks[s.shown]
    if (!b) break
    if (b.kind === 'say' || b.kind === 'user') {
      if (s.chars < b.text.length) {
        s.chars = Math.min(b.text.length, s.chars + SPEED)
      } else if (s.hold < HOLD_TEXT) {
        s.hold++
      } else { s.shown++; s.chars = 0; s.hold = 0 }
    } else if (b.kind === 'thinking') {
      if (s.hold < HOLD_THINK) s.hold++
      else { s.shown++; s.hold = 0 }
    } else if (b.kind === 'rows') {
      if (s.rows < b.ids.length) {
        if (s.hold >= ROW_EVERY) { s.rows++; s.hold = 0 } else s.hold++
      } else { s.shown++; s.rows = 0; s.hold = 0 }
    } else { s.shown++; s.hold = 0 }  // section / more reveal instantly
    left--
  }
  emit()
}

// single rAF loop (started lazily) driving the elapsed-time clock
let running = false
function loop() {
  const s = store.s
  const now = performance.now()
  if (s.last === 0) s.last = now
  if (!isDone(s)) {
    const steps = Math.min(60, Math.floor((now - s.last) / STEP))
    if (steps > 0) { s.last = now; advance(steps) }
  } else {
    s.last = now
  }
  if (running) requestAnimationFrame(loop)
}
function ensureRunning() {
  if (running) return
  running = true
  if (typeof requestAnimationFrame !== 'undefined') requestAnimationFrame(loop)
}

export function setScript(blocks: Block[]) {
  store.s = { blocks, shown: 0, chars: 0, rows: 0, hold: 0, last: 0, asked: [] }
  emit(); ensureRunning()
}
export function replay() {
  const s = store.s
  store.s = { ...s, shown: 0, chars: 0, rows: 0, hold: 0, last: 0, asked: [] }
  emit(); ensureRunning()
}
export function skipToEnd() {
  const s = store.s
  s.shown = s.blocks.length; s.chars = 0; s.rows = 0; s.hold = 0
  emit()
}
export function appendTurn(user: string, answer: Block[], askedId: string) {
  const s = store.s
  s.blocks = [...s.blocks, { kind: 'user', text: user }, ...answer]
  s.asked = [...s.asked, askedId]
  emit(); ensureRunning()
}

export function useNarration() {
  const [, force] = useState(0)
  useEffect(() => {
    const l = () => force((n) => n + 1)
    listeners.add(l); ensureRunning()
    return () => { listeners.delete(l) }
  }, [])
  const s = store.s
  const b = s.blocks[s.shown]
  const typingText = b && (b.kind === 'say' || b.kind === 'user') ? b.text.slice(0, s.chars) : ''
  const partialTyping = s.chars < currentTextLen(s)
  return { state: s, done: isDone(s), typingText, partialTyping, currentRows: s.rows }
}

// ── script builders (real numbers → the handoff copy templates) ──────────────
const money = (v?: number | null) => (v == null ? '—' : '₹' + v.toLocaleString('en-IN', { maximumFractionDigits: 1 }))

export function buildOpeningScript(summary: ScanSummary, ranked: Detection[], hookFor: (d: Detection) => string, ts: string): Block[] {
  const topN = ranked.slice(0, 5).map((d) => d.id)
  const allTop = ranked.slice(0, 7).map((d) => d.id)
  const patTypes = Object.keys(summary.byPattern).length
  const bull = ranked.filter((d) => d.direction === 'bullish').length
  const bear = ranked.length - bull
  const breakouts = ranked.filter((d) => d.stage === 'BREAKOUT').length
  const topPat = Object.entries(summary.byPattern).sort((a, b) => b[1] - a[1])[0]

  const blocks: Block[] = [
    { kind: 'say', ts, text: `Scan complete — ${fmt(summary.scanned)} stocks analysed of a ${fmt(summary.universe)}-name universe.` },
    { kind: 'say', ts, text: `${fmt(summary.detections)} valid chart patterns detected across ${patTypes} detectors.` },
    { kind: 'thinking', label: 'Matching formations against my historical case base…', chips: [`match_history(${fmt(summary.detections)})`, 'expectancy()', 'rank_opportunities()'] },
  ]
  if (summary.meaningful != null) {
    blocks.push({ kind: 'say', ts, text: `${fmt(summary.meaningful)} carry statistically meaningful evidence; ${fmt(summary.qualified)} clear every qualification gate.` })
  } else {
    blocks.push({ kind: 'say', ts, text: `Most setups have too few resolved precedents to qualify — at this sample depth I hold them at WATCH rather than invent an edge.` })
  }
  blocks.push(
    { kind: 'section', glyph: '⚡', label: 'TOP FINDINGS', note: '(Highest-signal setups)', color: V.greenHi },
    { kind: 'rows', ids: topN, ranked: true },
    { kind: 'section', glyph: '◉', label: 'MARKET STORY', note: '(The Bigger Picture)', color: V.cyan },
    { kind: 'say', ts, text: `Breadth: ${bull} bullish vs ${bear} bearish formations across the screen.` },
  )
  if (breakouts) blocks.push({ kind: 'say', ts, text: `${breakouts} setups are at the breakout stage right now.` })
  if (topPat) blocks.push({ kind: 'say', ts, text: `Most-active pattern today: ${label(topPat[0])} (${topPat[1]} detections).` })
  blocks.push({ kind: 'say', ts, text: `Sector rotation and market-cap tags are not yet in the scan feed — I omit them rather than guess.` })
  blocks.push(
    { kind: 'section', glyph: '▤', label: 'RANKED OBSERVATIONS', note: '(All Detections)', color: V.amber },
    { kind: 'rows', ids: allTop, ranked: true },
    { kind: 'more', label: `View next 20 of ${fmt(ranked.length)}` },
  )
  const top = ranked[0]
  if (top) blocks.push({ kind: 'say', ts, text: `My highest-signal setup is ${top.symbol}. Open it and I will show you the proof.` })
  return blocks
}

// follow-up branches — each returns its OWN content; an unknown id gets an
// explicit "no answer prepared", never another branch's script.
export function branch(id: string, ranked: Detection[], hookFor: (d: Detection) => string, ts: string): { user: string; answer: Block[] } {
  const top = ranked[0]
  if (id === 'why') {
    const sym = top?.symbol ?? 'the top setup'
    return {
      user: `How did you decide on ${sym}?`,
      answer: [
        { kind: 'thinking', label: 'Replaying my six qualification gates…', chips: ['quality_analysis()', 'historical_evidence()', 'decide()'] },
        { kind: 'say', ts, text: `Six gates, all required. Open the Decision tab on ${sym} and each gate shows against the threshold it must beat — with sparse precedents the sample gate is usually the binding one.` },
        ...(top ? [{ kind: 'rows', ids: [top.id], ranked: false } as Block] : []),
      ],
    }
  }
  if (id === 'rejected') {
    return {
      user: 'What did you reject today?',
      answer: [
        { kind: 'say', ts, text: `A clean pattern with no statistical edge is still no trade. Setups that fail on sample depth or expectancy I log for learning and take no entry.` },
        { kind: 'say', ts, text: `That is the honest majority today — the qualification bar is deliberately hard to clear.` },
      ],
    }
  }
  if (id === 'tomorrow') {
    const sym = top?.symbol ?? 'the top setup'
    const conf = top?.level
    return {
      user: 'What are you watching tomorrow?',
      answer: [
        { kind: 'say', ts, text: `Levels, not opinions. On ${sym} I am watching ${money(conf)}; a close back below the level invalidates the pattern.` },
        ...(top ? [{ kind: 'rows', ids: [top.id], ranked: false } as Block] : []),
      ],
    }
  }
  if (id === 'yesterday') {
    return {
      user: "How is yesterday's call doing?",
      answer: [
        { kind: 'thinking', label: "Comparing today's path against my winning cases…", chips: ['track(T+1)', 'path_match()', 'flag_deviation()'] },
        { kind: 'say', ts, text: `Intraday minute-tracking is post-market only today — I report the resolved outcome once each call reaches T+10 and write it back into my evidence base, win or lose.` },
      ],
    }
  }
  return { user: id, answer: [{ kind: 'say', ts, text: 'I do not have an answer prepared for that yet.' }] }
}

function fmt(v?: number | null) { return v == null ? '—' : v.toLocaleString('en-IN') }
function label(id: string) {
  return id.split('_').map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
}
