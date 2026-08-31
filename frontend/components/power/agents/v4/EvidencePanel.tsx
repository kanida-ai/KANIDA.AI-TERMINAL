'use client'

/**
 * Column 3 — Evidence & Deep Dive. "Show me the proof."
 * Fetches the two deep-dive endpoints for the selected detection and renders the
 * seven tabs over REAL data:
 *   • setup ← GET /api/agents/chart/setup (geometry · quality · evidence · paths · decision · watch_plan)
 *   • bars  ← GET /api/agents/chart/bars  (point-in-time OHLC)
 * Status/verdict come from the backend decision (source of truth). Where a field
 * is absent (sector, market-cap, market-alignment) we show "—" — never invented.
 */
import { useEffect, useMemo, useState } from 'react'
import { V, FONT, STATUS_META, type Status } from './tokens'
import * as A from '@/lib/agents-api'
import type { Detection, LiveSetup } from './data'
import {
  statusFromVerdict, verdictLabel, sixGates, qualityBars, edgeFor, horizonRows, distribution, pathSeries, symbolFacts,
} from './data'
import { CandleV4, PathChartV4, HistogramV4, type ChartOverlay, type Callout } from './PatternChart'

type Tab = 'chart' | 'why' | 'evidence' | 'paths' | 'decision' | 'watch' | 'autotrade'
const TABS: { key: Tab; label: string }[] = [
  { key: 'chart', label: 'PATTERN CHART' },
  { key: 'why', label: 'WHY IT QUALIFIES' },
  { key: 'evidence', label: 'HISTORICAL EVIDENCE' },
  { key: 'paths', label: 'WIN / LOSS PATH' },
  { key: 'decision', label: 'DECISION' },
  { key: 'watch', label: 'WATCH PLAN' },
  { key: 'autotrade', label: 'AUTOTRADE' },
]

const fmtPct = (v?: number | null, dp = 1) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
const fmtNum = (v?: number | null) => (v == null ? '—' : v.toLocaleString('en-IN', { maximumFractionDigits: 2 }))

export function EvidencePanel({ d, date, onBack }: { d: Detection; date: string; onBack: () => void }) {
  const [setup, setSetup] = useState<LiveSetup | null>(null)
  const [bars, setBars] = useState<A.Bar[]>([])
  const [busy, setBusy] = useState(true)
  const [tab, setTab] = useState<Tab>('chart')

  useEffect(() => {
    let alive = true
    setBusy(true); setTab('chart')
    Promise.all([A.fetchSetup(d.symbol, d.patternId, date), A.fetchBars(d.symbol, date, 90)])
      .then(([s, b]) => { if (!alive) return; setSetup(s as unknown as LiveSetup); setBars(Array.isArray(b.bars) ? b.bars : []) })
      .finally(() => { if (alive) setBusy(false) })
    return () => { alive = false }
  }, [d.symbol, d.patternId, date])

  const gates = useMemo(() => sixGates(setup), [setup])
  const status: Status = setup?.decision?.decision ? statusFromVerdict(setup.decision.decision, gates.passed, gates.total) : d.status
  const sm = STATUS_META[status]
  const verdict = verdictLabel(setup?.decision?.decision)
  const facts = useMemo(() => symbolFacts(bars), [bars])
  const q = useMemo(() => qualityBars(setup), [setup])
  const edge = useMemo(() => edgeFor(setup), [setup])
  const hz = useMemo(() => horizonRows(setup), [setup])
  const dist = useMemo(() => distribution(setup), [setup])
  const paths = useMemo(() => pathSeries(setup), [setup])
  const overlay = useMemo(() => buildOverlay(setup, d, edge.winT5), [setup, d, edge.winT5])

  const headline = `— ${d.pattern} ${d.stage.toLowerCase()}`
  const changeCol = facts.changePct == null ? V.dim : facts.changePct >= 0 ? V.greenHi : V.red
  const priceChart = tab === 'chart' || tab === 'watch'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      {/* header */}
      <div style={{ padding: '11px 14px', borderBottom: `1px solid ${V.border}`, display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ color: V.cyan }}>◐</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 10, letterSpacing: '0.14em', color: V.dim }}>EVIDENCE &amp; DEEP DIVE</span>
        <button onClick={onBack} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: V.cyan, fontFamily: FONT.sans, fontSize: 11.5, cursor: 'pointer' }}>‹ Back to Storyline</button>
      </div>

      <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', display: 'flex', flexDirection: 'column' }}>
        {/* symbol block */}
        <div style={{ padding: '12px 14px 10px', display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontFamily: FONT.mono, fontSize: 21, fontWeight: 600, color: V.text }}>{d.symbol}</span>
          <span style={{ fontFamily: FONT.sans, fontSize: 14, color: V.dim, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{headline}</span>
          <span style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.1em', padding: '4px 10px', borderRadius: 5, color: sm.text, background: sm.bg, border: `1px solid ${sm.border}`, flexShrink: 0 }}>{sm.label}</span>
        </div>

        {/* stat strip */}
        <div style={{ margin: '0 14px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(112px, 1fr))', border: `1px solid ${V.border}`, borderRadius: 9, background: V.inset, overflow: 'hidden', flexShrink: 0 }}>
          <StatCell l="SECTOR" v="—" />
          <StatCell l="LTP" v={facts.ltp == null ? '—' : '₹' + fmtNum(facts.ltp)} />
          <StatCell l="DAILY CHANGE" v={fmtPct(facts.changePct)} c={changeCol} />
          <StatCell l="MARKET CAP" v="—" />
          <StatCell l="VOLUME" v={d.volumeX == null ? '—' : d.volumeX.toFixed(2) + '× Avg'} c={(d.volumeX ?? 0) >= 1.2 ? V.greenHi : V.amber} />
        </div>

        {/* tab strip (wrap, not nowrap) */}
        <div style={{ borderBottom: `1px solid ${V.border}`, overflowX: 'auto', flexShrink: 0, marginTop: 10 }}>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '2px 4px', width: '100%', padding: '0 8px' }}>
            {TABS.map((t) => {
              const active = tab === t.key
              return (
                <button key={t.key} onClick={() => setTab(t.key)}
                  style={{ flex: '0 0 auto', padding: '9px 10px', whiteSpace: 'nowrap', fontFamily: FONT.mono, fontSize: 10, letterSpacing: '0.06em',
                    border: 'none', borderBottom: `2px solid ${active ? V.cyan : 'transparent'}`, background: 'none', color: active ? V.text : V.faint, cursor: 'pointer' }}>
                  {t.label}
                </button>
              )
            })}
          </div>
        </div>

        {/* body */}
        <div style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 12 }}>
          {busy ? (
            <Empty>Reading the tape for {d.symbol}…</Empty>
          ) : (
            <>
              {/* chart card — shown for chart / watch / evidence / paths with the right caption */}
              {(tab === 'chart' || tab === 'watch' || tab === 'evidence' || tab === 'paths') && (
                <div style={{ border: `1px solid ${V.border}`, borderRadius: 9, background: V.inset, padding: '11px 9px 7px', flexShrink: 0 }}>
                  <div style={{ padding: '0 4px 8px', display: 'flex', gap: 10, fontFamily: FONT.mono, fontSize: 10.5, color: V.faint }}>
                    <span>{caption(tab, d.symbol, edge.n)}</span>
                    {priceChart && facts.ohlc && (
                      <span>O {facts.ohlc.o.toFixed(1)} H {facts.ohlc.h.toFixed(1)} L {facts.ohlc.l.toFixed(1)} C {facts.ohlc.c.toFixed(1)}
                        <span style={{ color: changeCol }}> {fmtPct(facts.changePct)}</span>
                      </span>
                    )}
                  </div>
                  {tab === 'chart' && <CandleV4 bars={bars} overlay={overlay} emptyLabel="Point-in-time candles pending /bars." />}
                  {tab === 'watch' && <CandleV4 bars={bars} overlay={{ ...overlay, callouts: [], historicalFlag: null, watchLevels: watchLevels(setup) }} emptyLabel="Watch-plan levels drawn on candles once /bars lands." />}
                  {tab === 'evidence' && (dist ? <HistogramV4 buckets={dist.buckets} /> : <Empty>Distribution needs resolved per-case precedents (evidence.paths).</Empty>)}
                  {tab === 'paths' && <PathChartV4 winners={paths.winners} losers={paths.losers} nWin={paths.nWin} nLoss={paths.nLoss} />}
                </div>
              )}

              {/* WHY panel — shown on chart + why tabs (it is the centrepiece) */}
              {(tab === 'chart' || tab === 'why') && (
                <WhyPanel status={status} gates={gates} q={q} edge={edge} paths={paths} />
              )}

              {/* DECISION */}
              {tab === 'decision' && <DecisionView setup={setup} verdict={verdict} gates={gates} />}

              {/* WATCH PLAN detail */}
              {tab === 'watch' && <WatchPlanView setup={setup} />}

              {/* HISTORICAL EVIDENCE detail */}
              {tab === 'evidence' && <EvidenceDetail edge={edge} hz={hz} />}

              {/* AUTOTRADE */}
              {tab === 'autotrade' && <AutoTradeView setup={setup} status={status} />}
            </>
          )}
        </div>
      </div>

      {/* footer — gate chips + actions */}
      <div style={{ borderTop: `1px solid ${V.border}`, padding: '11px 14px', display: 'flex', flexDirection: 'column', gap: 10, flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <span style={{ fontFamily: FONT.sans, fontSize: 11, color: V.dim }}>Decision Gates</span>
          {gates.slots.map((g) => {
            const pass = g.pass
            const col = pass == null ? V.faint : pass ? V.greenHi : V.red
            const bg = pass == null ? V.inset : pass ? '#0b2320' : '#2a1214'
            const bd = pass == null ? V.borderStrong : pass ? '#1c5c48' : '#5c1f24'
            return <span key={g.name} style={{ padding: '4px 9px', borderRadius: 5, fontFamily: FONT.sans, fontSize: 10.5, color: col, background: bg, border: `1px solid ${bd}` }}>{g.name} {pass == null ? '—' : pass ? '✓' : '✗'}</span>
          })}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 8 }}>
          <button disabled title="Per-user AutoTrade is Launch-Pending"
            style={{ padding: '9px 12px', borderRadius: 7, background: V.inset, border: `1px solid ${V.borderStrong}`, color: V.faint, fontFamily: FONT.sans, fontSize: 12, cursor: 'not-allowed' }}>
            ◎ AutoTrade (Not Eligible)
          </button>
          <button style={{ padding: '9px 12px', borderRadius: 7, background: V.raised, border: `1px solid ${V.borderStrong}`, color: V.muted, fontFamily: FONT.sans, fontSize: 12, cursor: 'pointer' }}>☆ Add to Watchlist</button>
          <button onClick={() => setTab('evidence')} style={{ padding: '9px 12px', borderRadius: 7, background: V.raised, border: `1px solid ${V.borderStrong}`, color: V.muted, fontFamily: FONT.sans, fontSize: 12, cursor: 'pointer' }}>◫ View Full Evidence</button>
        </div>
      </div>
    </div>
  )
}

// ── WHY THIS QUALIFIES panel ──────────────────────────────────────────────────
function WhyPanel({ status, gates, q, edge, paths }: {
  status: Status
  gates: ReturnType<typeof sixGates>
  q: ReturnType<typeof qualityBars>
  edge: ReturnType<typeof edgeFor>
  paths: ReturnType<typeof pathSeries>
}) {
  const title = status === 'rejected' ? 'WHY THIS WAS REJECTED' : status === 'qualified' ? 'WHY THIS QUALIFIES' : 'WHY THIS IS ONLY WATCHED'
  const winShare = edge.winT5
  return (
    <div style={{ border: `1px solid ${V.border}`, borderRadius: 9, background: V.inset, overflow: 'hidden', flexShrink: 0 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '11px 12px' }}>
        <span style={{ fontFamily: FONT.mono, fontSize: 10, letterSpacing: '0.12em', color: V.dim }}>{title}</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 9.5, color: V.faint }}>{gates.passed}/{gates.total || '—'} GATES PASSED</span>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(168px, 1fr))', gap: 1, background: V.hairline }}>
        {/* 1. Pattern Quality */}
        <Cell>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <Ring score={q.ring} />
            <div style={{ flex: 1, minWidth: 0 }}>
              {q.bars.map((b) => <QBar key={b.label} label={b.label} value={b.value} />)}
            </div>
          </div>
          <div style={{ fontFamily: FONT.sans, fontSize: 10.5, color: V.dim, marginTop: 8 }}>{q.caption}</div>
        </Cell>
        {/* 2. Gate summary */}
        <Cell>
          <div style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.1em', color: V.faint, marginBottom: 8 }}>GATE SUMMARY</div>
          {gates.slots.map((g) => (
            <div key={g.name} style={{ display: 'flex', gap: 7, alignItems: 'baseline', marginBottom: 6 }}>
              <span style={{ fontSize: 9.5, color: g.pass == null ? V.faint : g.pass ? V.greenHi : V.red }}>{g.pass == null ? '–' : g.pass ? '✓' : '✗'}</span>
              <span style={{ fontFamily: FONT.sans, fontSize: 11, color: V.dim }}>{g.name}{g.detail ? ` — ${g.detail}` : ''}</span>
            </div>
          ))}
        </Cell>
        {/* 3. Historical Edge */}
        <Cell>
          <div style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.1em', color: V.faint, marginBottom: 8 }}>HISTORICAL EDGE</div>
          <EdgeFig label="Win Rate (T+5)" value={edge.winT5 == null ? '—' : edge.winT5.toFixed(0) + '%'} />
          <EdgeFig label="Historical Sample" value={edge.n == null ? '—' : String(edge.n)} />
          <EdgeFig label="ETV (Expectancy)" value={fmtPct(edge.etv, 2)} c={(edge.etv ?? 0) >= 0 ? V.greenHi : V.red} />
        </Cell>
        {/* 4. Path preview */}
        <Cell>
          <div style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.1em', color: V.faint, marginBottom: 8 }}>PATH PREVIEW (T+5)</div>
          <MiniPath series={paths.winners} color={V.green} label="Winning Path" share={winShare == null ? '—' : winShare.toFixed(0) + '%'} />
          <MiniPath series={paths.losers} color={V.red} label="Losing Path" share={winShare == null ? '—' : (100 - winShare).toFixed(0) + '%'} />
        </Cell>
      </div>
    </div>
  )
}

function DecisionView({ setup, verdict, gates }: { setup: LiveSetup | null; verdict: string; gates: ReturnType<typeof sixGates> }) {
  if (!setup?.decision) return <Empty>The gate-by-gate rationale renders here from /setup.decision.</Empty>
  const lede = verdict === 'TRADE' ? 'All conditions passed — this enters tomorrow\'s ranked plan.'
    : verdict === 'NO TRADE' ? 'It failed on sample depth or expectancy, which I treat as fatal. I log it for learning and take no entry.'
    : `${gates.passed} of ${gates.total || 6} evaluated conditions passed. The structure is valid but not every statistical condition is met, so I track it without a simulated entry.`
  const col = verdict === 'TRADE' ? V.greenHi : verdict === 'NO TRADE' ? V.red : V.amber
  return (
    <div>
      <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 10, flexWrap: 'wrap' }}>
        <span style={{ fontFamily: FONT.mono, fontSize: 13, fontWeight: 600, padding: '6px 14px', borderRadius: 6, color: col, background: `${col}18`, border: `1px solid ${col}55` }}>{verdict}</span>
        {setup.decision.basis && <span style={{ fontFamily: FONT.sans, fontSize: 11, color: V.faint }}>basis · {setup.decision.basis}</span>}
      </div>
      <div style={{ fontFamily: FONT.sans, fontSize: 12.5, color: V.body, lineHeight: 1.6, marginBottom: 12 }}>{setup.decision.reason || lede}</div>
      {setup.decision.spec_note && <div style={{ fontFamily: FONT.sans, fontSize: 10.5, color: V.faint, lineHeight: 1.5, marginBottom: 12 }}>{setup.decision.spec_note}</div>}
      <div style={{ border: `1px solid ${V.border}`, borderRadius: 9 }}>
        {gates.slots.map((g, i) => (
          <div key={g.name} style={{ display: 'grid', gridTemplateColumns: 'auto minmax(0,1fr) auto', gap: 10, padding: '10px 12px', alignItems: 'center', borderTop: i ? `1px solid ${V.hairline}` : 'none' }}>
            <span style={{ fontFamily: FONT.mono, fontSize: 9, color: g.pass == null ? V.faint : g.pass ? V.green : V.red }}>{g.pass == null ? '—' : g.pass ? 'PASS' : 'FAIL'}</span>
            <span style={{ fontFamily: FONT.sans, fontSize: 12, color: V.muted }}>{g.name}</span>
            <span style={{ fontFamily: FONT.mono, fontSize: 11.5, color: V.faint }}>{g.detail || ''}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function WatchPlanView({ setup }: { setup: LiveSetup | null }) {
  const w = setup?.watch_plan
  if (!w || (w.confirmation == null && w.warning == null && w.invalidation == null)) return <Empty>The confirmation / warning / invalidation plan renders here from /setup.watch_plan.</Empty>
  return (
    <div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 1, background: V.hairline, border: `1px solid ${V.border}`, borderRadius: 9, overflow: 'hidden' }}>
        <StatBig l="CONFIRM ABOVE" v={w.confirmation == null ? '—' : '₹' + fmtNum(w.confirmation)} c={V.green} />
        <StatBig l="WARNING" v={w.warning == null ? '—' : '₹' + fmtNum(w.warning)} c={V.amber} />
        <StatBig l="INVALIDATE" v={w.invalidation == null ? '—' : '₹' + fmtNum(w.invalidation)} c={V.red} />
      </div>
      {w.note && <div style={{ fontFamily: FONT.sans, fontSize: 11, color: V.faint, lineHeight: 1.55, marginTop: 10 }}>{w.note}</div>}
    </div>
  )
}

function EvidenceDetail({ edge, hz }: { edge: ReturnType<typeof edgeFor>; hz: ReturnType<typeof horizonRows> }) {
  if (edge.n == null && hz.length === 0) {
    return <Empty><b style={{ color: V.amber }}>Insufficient precedents.</b><br />Too few resolved historical occurrences to publish a win-rate / ETV. The agent holds it at WATCH rather than invent an edge.</Empty>
  }
  return (
    <div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 1, background: V.hairline, border: `1px solid ${V.border}`, borderRadius: 9, overflow: 'hidden', marginBottom: 12 }}>
        <StatBig l="SAMPLE (n)" v={edge.n == null ? '—' : String(edge.n)} />
        <StatBig l="WIN RATE (T+5)" v={edge.winT5 == null ? '—' : edge.winT5.toFixed(0) + '%'} />
        <StatBig l="ETV" v={fmtPct(edge.etv, 2)} c={(edge.etv ?? 0) >= 0 ? V.greenHi : V.red} />
      </div>
      {hz.length > 0 && (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: FONT.mono, fontSize: 11.5 }}>
          <thead><tr style={{ color: V.faint }}>{['HORIZON', 'WIN', 'MEDIAN', 'MFE', 'MAE'].map((c, i) => <th key={c} style={{ textAlign: i ? 'right' : 'left', padding: '5px 6px', fontWeight: 400, fontSize: 9.5, letterSpacing: '0.06em' }}>{c}</th>)}</tr></thead>
          <tbody>{hz.map((r) => (
            <tr key={r.h} style={{ borderTop: `1px solid ${V.hairline}` }}>
              <td style={{ padding: '6px', color: V.muted }}>{r.h}</td>
              <td style={{ padding: '6px', textAlign: 'right', color: V.muted }}>{r.win == null ? '—' : r.win.toFixed(0) + '%'}</td>
              <td style={{ padding: '6px', textAlign: 'right', color: (r.med ?? 0) >= 0 ? V.greenHi : V.red }}>{fmtPct(r.med)}</td>
              <td style={{ padding: '6px', textAlign: 'right', color: V.greenHi }}>{fmtPct(r.mfe)}</td>
              <td style={{ padding: '6px', textAlign: 'right', color: V.red }}>{fmtPct(r.mae)}</td>
            </tr>
          ))}</tbody>
        </table>
      )}
    </div>
  )
}

function AutoTradeView({ setup, status }: { setup: LiveSetup | null; status: Status }) {
  const p = setup?.decision?.policy as Record<string, unknown> | undefined
  return (
    <div>
      <div style={{ fontFamily: FONT.mono, fontSize: 9.5, letterSpacing: '0.12em', color: V.faint, marginBottom: 8 }}>PAYLOAD → AUTOTRADE AGENT</div>
      <div style={{ border: `1px solid ${V.border}`, borderRadius: 9, padding: '10px 12px', display: 'flex', flexDirection: 'column', gap: 6 }}>
        <KV k="verdict" v={verdictLabel(setup?.decision?.decision)} c={status === 'qualified' ? V.greenHi : V.amber} />
        <KV k="route" v="autotrade · paper" c={V.cyan} />
        {p && Object.entries(p).map(([k, val]) => <KV key={k} k={k} v={val == null ? '—' : String(val)} c={V.muted} />)}
      </div>
      <div style={{ fontFamily: FONT.sans, fontSize: 10.5, color: V.faint, lineHeight: 1.5, marginTop: 10 }}>
        Per-user AutoTrade is Launch-Pending. Intents route through the paper-default, cert-gated, operator-armed autotrade path — no live order is placed from this surface.
      </div>
    </div>
  )
}

// ── overlay + caption builders ────────────────────────────────────────────────
function buildOverlay(setup: LiveSetup | null, d: Detection, winT5: number | null): ChartOverlay {
  const g = setup?.geometry
  const ov: ChartOverlay = { bullish: d.direction === 'bullish', callouts: [], touches: [] }
  if (!g) {
    ov.callouts = realCallouts(d)
    return ov
  }
  const line = (l?: { a?: { date: string; price: number } | null; b?: { date: string; price: number } | null } | null) =>
    l && l.a && l.b ? { a: l.a, b: l.b } : null
  ov.upper = line(g.upper)
  ov.lower = line(g.lower)
  const ll = g.level_line
  if (!ov.upper && !ov.lower && ll?.from) ov.level = ll.from.price
  ov.touches = (g.touches || []).filter((t) => t && typeof t.price === 'number')
  ov.breakout = g.breakout && typeof g.breakout.price === 'number' ? g.breakout : null
  const starts = [...(g.touches || []).map((t) => t.date), ll?.from?.date].filter(Boolean) as string[]
  ov.patternStartDate = starts.length ? starts.reduce((a, b) => (a < b ? a : b)) : null
  ov.callouts = realCallouts(d)
  ov.historicalFlag = winT5 == null ? null : `Historically ${winT5.toFixed(0)}% positive by T+5`
  return ov
}
function realCallouts(d: Detection): Callout[] {
  const c: Callout[] = []
  if (d.touches) c.push({ text: `${d.touches} clean touches` })
  if (d.volumeX != null) c.push({ text: `${d.volumeX.toFixed(2)}× volume` })
  if (d.stage === 'BREAKOUT') c.push({ text: 'Close beyond level' })
  return c.slice(0, 4)
}
function watchLevels(setup: LiveSetup | null): { label: string; price: number; color: string }[] {
  const w = setup?.watch_plan
  if (!w) return []
  const out: { label: string; price: number; color: string }[] = []
  if (w.confirmation != null) out.push({ label: 'CONFIRM', price: w.confirmation, color: V.green })
  if (w.warning != null) out.push({ label: 'WARNING', price: w.warning, color: V.amber })
  if (w.invalidation != null) out.push({ label: 'INVALIDATE', price: w.invalidation, color: V.red })
  return out
}
function caption(tab: Tab, sym: string, n: number | null): string {
  if (tab === 'evidence') return `T+5 RETURN DISTRIBUTION · ${n ?? '—'} MATCHED CASES`
  if (tab === 'paths') return `MEDIAN TRAJECTORIES T0 → T+10 · ${n ?? '—'} CASES`
  return `${sym} · 1D · NSE`
}

// ── primitives ────────────────────────────────────────────────────────────────
function Empty({ children }: { children: React.ReactNode }) {
  return <div style={{ padding: '28px 16px', textAlign: 'center', color: V.faint, fontFamily: FONT.sans, fontSize: 12, lineHeight: 1.65 }}>{children}</div>
}
function StatCell({ l, v, c = V.tick }: { l: string; v: string; c?: string }) {
  return (
    <div style={{ padding: '9px 11px', borderRight: `1px solid ${V.hairline}` }}>
      <div style={{ fontFamily: FONT.mono, fontSize: 8.5, letterSpacing: '0.1em', color: V.faint }}>{l}</div>
      <div style={{ fontFamily: FONT.sans, fontSize: 12.5, color: c, marginTop: 2 }}>{v}</div>
    </div>
  )
}
function StatBig({ l, v, c = V.tick }: { l: string; v: string; c?: string }) {
  return (
    <div style={{ padding: '11px 12px', background: V.inset }}>
      <div style={{ fontFamily: FONT.mono, fontSize: 8.5, letterSpacing: '0.1em', color: V.faint }}>{l}</div>
      <div style={{ fontFamily: FONT.mono, fontSize: 17, fontWeight: 500, color: c, marginTop: 3 }}>{v}</div>
    </div>
  )
}
function Cell({ children }: { children: React.ReactNode }) {
  return <div style={{ background: V.inset, padding: '13px 12px' }}>{children}</div>
}
function Ring({ score }: { score: number | null }) {
  const s = score == null ? 0 : score
  const R = 20, C = 2 * Math.PI * R
  const col = s >= 85 ? V.green : s >= 70 ? V.cyan : s >= 55 ? V.amber : V.red
  return (
    <div style={{ position: 'relative', width: 86, height: 86, flexShrink: 0 }}>
      <svg viewBox="0 0 46 46" width="86" height="86">
        <circle cx="23" cy="23" r={R} fill="none" stroke={V.hairline} strokeWidth="5" />
        <circle cx="23" cy="23" r={R} fill="none" stroke={col} strokeWidth="5" strokeLinecap="round"
          strokeDasharray={`${(s / 100) * C} ${C}`} transform="rotate(-90 23 23)" style={{ transition: 'stroke-dashoffset 800ms ease' }} />
      </svg>
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ fontFamily: FONT.mono, fontSize: 22, fontWeight: 600, color: col }}>{score == null ? '—' : Math.round(s)}</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 10, color: V.faint }}>/100</span>
      </div>
    </div>
  )
}
function QBar({ label, value }: { label: string; value: number | null }) {
  const col = value == null ? V.faint : value >= 85 ? V.green : value >= 65 ? V.amber : V.red
  return (
    <div style={{ marginBottom: 5 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
        <span style={{ fontFamily: FONT.sans, fontSize: 9, color: V.dim }}>{label}</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 9.5, color: V.dim }}>{value == null ? '—' : value}</span>
      </div>
      <div style={{ height: 3, background: V.hairline, borderRadius: 2 }}>
        <div style={{ width: `${value ?? 0}%`, height: '100%', background: col, borderRadius: 2 }} />
      </div>
    </div>
  )
}
function EdgeFig({ label, value, c = V.text }: { label: string; value: string; c?: string }) {
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ fontFamily: FONT.sans, fontSize: 9.5, color: V.dim }}>{label}</div>
      <div style={{ fontFamily: FONT.mono, fontSize: 20, fontWeight: 600, color: c }}>{value}</div>
    </div>
  )
}
function MiniPath({ series, color, label, share }: { series: number[] | null; color: string; label: string; share: string }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
        <span style={{ fontFamily: FONT.sans, fontSize: 9.5, color: V.dim }}>{label}</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 13, fontWeight: 600, color }}>{share}</span>
      </div>
      {series && series.length ? (
        <svg viewBox="0 0 150 44" width="100%" height="28" preserveAspectRatio="none">
          {(() => {
            const lo = Math.min(0, ...series), hi = Math.max(0, ...series), sp = hi - lo || 1
            const x = (i: number) => (i / (series.length - 1)) * 150
            const y = (v: number) => 44 - ((v - lo) / sp) * 44
            const d = series.map((v, i) => `${i ? 'L' : 'M'} ${x(i)} ${y(v)}`).join(' ')
            return <><path d={`${d} L 150 44 L 0 44 Z`} fill={color} opacity={0.16} /><path d={d} fill="none" stroke={color} strokeWidth={1.6} /></>
          })()}
        </svg>
      ) : <div style={{ fontFamily: FONT.sans, fontSize: 10, color: V.faint }}>—</div>}
    </div>
  )
}
function KV({ k, v, c = V.muted }: { k: string; v: string; c?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', fontFamily: FONT.mono, fontSize: 11.5 }}>
      <span style={{ color: V.faint }}>{k}</span><span style={{ color: c }}>{v}</span>
    </div>
  )
}
