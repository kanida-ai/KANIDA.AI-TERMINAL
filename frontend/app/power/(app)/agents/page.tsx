'use client'

/**
 * /power/agents — Chart Pattern Agent, rebuilt to the v4 design handoff
 * (design_handoff_chart_pattern_agent). A three-column AI market ANALYST:
 *
 *   COLUMN 1  Agent desk      — scan summary · pattern categories · 23-agent network
 *   COLUMN 2  AI Storyline     — the self-typing newswire (primary experience)
 *   COLUMN 3  Evidence & Deep Dive — annotated candlestick · quality · evidence · decision
 *
 * WIRED TO THE REAL BACKEND (nothing fabricated):
 *   • manifest ← GET /api/agents/chart-v1
 *   • scan     ← GET /api/agents/chart/scan?date=…&full=1   (default date via findFreshestScan)
 *   • setup    ← GET /api/agents/chart/setup?symbol=&pattern=&date=
 *   • bars     ← GET /api/agents/chart/bars?symbol=&date=&lookback=
 * Where the backend has no field (sector, market-cap, market-alignment, a pre-built
 * distribution) the UI honestly shows "—" or omits it. Verdict/status come from the
 * backend decision — never a parallel client rule. The narration is DERIVED from the
 * real scan via the handoff copy templates.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useBreakpoint } from '@/lib/terminal-ui'
import * as A from '@/lib/agents-api'
import { V, FONT } from '@/components/power/agents/v4/tokens'
import { AgentDesk } from '@/components/power/agents/v4/AgentDesk'
import { Storyline } from '@/components/power/agents/v4/Storyline'
import { EvidencePanel } from '@/components/power/agents/v4/EvidencePanel'
import { toDetection, toSummary, rankDetections, type Detection } from '@/components/power/agents/v4/data'
import { setScript, buildOpeningScript } from '@/components/power/agents/v4/narration'

// category label → pattern ids (mirrors AgentDesk families)
const FAMILY_IDS: Record<string, string[]> = {
  'Breakout Patterns': ['horizontal_trendline', 'rectangle'],
  'Continuation Patterns': ['channel', 'cup_and_handle'],
  'Triangle Patterns': ['ascending_triangle', 'descending_triangle', 'symmetrical_triangle'],
  'Wedge Patterns': ['rising_wedge', 'falling_wedge'],
}

function hookFor(d: Detection): string {
  const parts = [`${d.pattern} ${d.stage.toLowerCase()}`]
  if (d.distancePct != null) parts.push(`${(d.distancePct >= 0 ? '+' : '') + d.distancePct.toFixed(1)}% past level`)
  if (d.volumeX != null) parts.push(`${d.volumeX.toFixed(1)}× vol`)
  return parts.join(' · ')
}

const KEYFRAMES = `
@keyframes kaRise { from { opacity: 0; transform: translateY(6px) } to { opacity: 1; transform: none } }
@keyframes kaCaret { 0%,45% { opacity: 1 } 55%,100% { opacity: 0 } }
@keyframes kaPulse { 0%,100% { opacity: 1 } 50% { opacity: .35 } }
@keyframes kaShimmer { 0% { background-position: -240px 0 } 100% { background-position: 240px 0 } }
@keyframes kaSweep { 0% { transform: translateX(-100%) } 100% { transform: translateX(230%) } }
@keyframes kaDot { 0%,100% { opacity: .25; transform: translateY(0) } 50% { opacity: 1; transform: translateY(-2px) } }
`

export default function AgentsPage() {
  const { width } = useBreakpoint()
  const stacked = width < 900

  const [manifest, setManifest] = useState<A.ManifestResp | null>(null)
  const [scan, setScan] = useState<A.ScanResp | null>(null)
  const [date, setDate] = useState('')
  const [busy, setBusy] = useState(true)
  const [stale, setStale] = useState(false)

  const [category, setCategory] = useState('all')
  const [agent, setAgent] = useState('Chart Pattern Agent')
  const [selected, setSelected] = useState<Detection | null>(null)
  const [navOpen, setNavOpen] = useState<boolean | null>(null)  // null = auto by width

  const land = useCallback(async () => {
    setBusy(true); setStale(false)
    try {
      const fresh = await A.findFreshestScan(7)
      if (fresh) { setDate(fresh.date); setScan(fresh.scan) }
      else {
        const fb = await A.fetchScan(A.KNOWN_POPULATED_DATE, { full: true })
        setDate(A.KNOWN_POPULATED_DATE); setStale(true); setScan(fb)
      }
    } catch {
      setScan({ ok: false, date: '', count: 0, occurrences: [], error: 'scanner unreachable' })
    } finally { setBusy(false) }
  }, [])

  useEffect(() => {
    A.fetchManifest().then(setManifest).catch(() => setManifest(null))
    land()
  }, [land])

  const allDets = useMemo(() => (scan?.occurrences ?? []).map(toDetection), [scan])
  const summary = useMemo(() => toSummary(scan, allDets), [scan, allDets])

  const ranked = useMemo(() => {
    const inCat = (d: Detection) => {
      if (category === 'all') return true
      if (FAMILY_IDS[category]) return FAMILY_IDS[category].includes(d.patternId)
      return d.patternId === category
    }
    return rankDetections(allDets.filter(inCat))
  }, [allDets, category])

  // (re)build the narration script whenever the ranked pool changes
  useEffect(() => {
    if (busy) return
    setScript(buildOpeningScript(summary, ranked, hookFor, ''))
    if (ranked.length) setSelected((prev) => prev && ranked.some((d) => d.id === prev.id) ? prev : ranked[0])
    else setSelected(null)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ranked, busy])

  // ── nav sizing ──
  const autoExpanded = width >= 1180
  const expanded = navOpen ?? autoExpanded
  const overlay = expanded && width < 1180 && !stacked
  const navWidth = stacked ? 0 : overlay ? 0 : expanded ? 272 : 60

  const scanBad = scan && scan.ok === false
  const pending = scan?.served === 'pending'
  const tsLabel = date ? `${date} · Market Closed` : ''

  const desk = (
    <AgentDesk
      summary={summary}
      patterns={manifest?.patterns ?? []}
      selectedCat={category}
      onSelectCat={setCategory}
      selectedAgent={agent}
      onSelectAgent={setAgent}
      collapsed={!expanded && !overlay}
      onToggleCollapse={() => setNavOpen((o) => (o == null ? !autoExpanded : !o))}
      loading={busy}
    />
  )

  return (
    <div style={{ height: stacked ? 'auto' : '100%', display: 'flex', flexDirection: 'column', background: V.bg, color: V.text, fontFamily: FONT.sans }}>
      <style>{KEYFRAMES}</style>

      {/* top bar */}
      <div style={{ height: 48, flexShrink: 0, background: V.panel, borderBottom: `1px solid ${V.border}`, display: 'flex', alignItems: 'center', gap: 18, padding: '10px 16px' }}>
        <span style={{ width: 26, height: 26, borderRadius: 7, background: 'linear-gradient(140deg, #22d3ee, #3b82f6)', color: '#05121c', fontFamily: FONT.mono, fontSize: 12, fontWeight: 600, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>K</span>
        <span style={{ fontFamily: FONT.sans, fontSize: 15, fontWeight: 600, color: V.text }}>KANIDA.AI</span>
        <span style={{ fontFamily: FONT.mono, fontSize: 10, letterSpacing: '0.14em', color: V.cyan }}>AI AGENT NETWORK</span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '5px 12px', borderRadius: 16, border: '1px solid #1c5c48', background: '#0b2320' }}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: V.green, animation: busy ? 'kaPulse 1.4s infinite' : 'none' }} />
          <span style={{ fontFamily: FONT.sans, fontSize: 11.5, color: V.greenHi }}>{busy ? 'Market Analysis Running' : 'Market Analysis Complete'}</span>
        </span>
        {width >= 1100 && <span style={{ fontFamily: FONT.mono, fontSize: 11, color: V.faint, flexShrink: 0 }}>{tsLabel}</span>}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          {['Filters ▽', 'View ⌄', 'Experienced ⌄'].map((c) => (
            <span key={c} style={{ padding: '6px 12px', borderRadius: 7, border: `1px solid ${V.borderStrong}`, background: V.raised, fontFamily: FONT.sans, fontSize: 11.5, color: V.muted }}>{c}</span>
          ))}
        </div>
      </div>

      {/* 3-column grid */}
      <div style={{ flex: 1, minHeight: 0, position: 'relative', display: stacked ? 'flex' : 'grid', flexDirection: stacked ? 'column' : undefined,
        gridTemplateColumns: stacked ? undefined : `${navWidth}px minmax(300px, 0.86fr) minmax(400px, 1.5fr)`, gap: 12, padding: 12,
        transition: 'grid-template-columns 300ms cubic-bezier(0.4,0,0.2,1)' }}>

        {/* Column 1 — nav */}
        {!stacked && !overlay && (
          <Panel style={{ gridColumn: '1 / 2' }}>{desk}</Panel>
        )}
        {overlay && (
          <div style={{ position: 'absolute', left: 12, top: 12, bottom: 12, width: 272, zIndex: 30, background: V.bg, border: `1px solid ${V.border}`, borderRadius: 11, boxShadow: '0 18px 48px rgba(0,0,0,0.62)', overflow: 'hidden' }}>{desk}</div>
        )}
        {stacked && <Panel style={{ height: 420 }}>{desk}</Panel>}

        {/* Column 2 — storyline */}
        <Panel style={{ ...(stacked ? { minHeight: 520 } : { gridColumn: '2 / 3' }) }}>
          {busy ? <Center>Scanning the universe…</Center>
            : scanBad ? <Center><b style={{ color: V.red }}>Scanner unavailable.</b><br />{scan?.error || 'The agent data source is offline.'}</Center>
            : pending ? <Center><b style={{ color: V.amber }}>Screen builds post-market.</b><br />No precomputed screen for {date} yet — the EOD job scans after close.</Center>
            : <>
                {stale && <div style={{ margin: 10, fontFamily: FONT.sans, fontSize: 11.5, color: V.amber, background: '#241c07', border: '1px solid #5c4413', borderRadius: 8, padding: '8px 11px' }}>No precomputed screen in the last 7 days — showing the last-known populated screen ({date}).</div>}
                <Storyline ranked={ranked} hookFor={hookFor} selectedId={selected?.id ?? null} onSelect={setSelected} ts="" />
              </>}
        </Panel>

        {/* Column 3 — evidence */}
        <Panel style={{ ...(stacked ? { minHeight: 640 } : { gridColumn: '3 / 4' }) }}>
          {selected
            ? <EvidencePanel d={selected} date={date} onBack={() => ranked[0] && setSelected(ranked[0])} />
            : <Center>Click a finding in the storyline to see the proof — the pattern chart, quality, evidence, win/loss path, decision and watch plan.</Center>}
        </Panel>
      </div>
    </div>
  )
}

function Panel({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return <div style={{ minWidth: 0, border: `1px solid ${V.border}`, borderRadius: 11, background: V.panel, overflow: 'hidden', ...style }}>{children}</div>
}
function Center({ children }: { children: React.ReactNode }) {
  return <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', textAlign: 'center', padding: 30, color: V.faint, fontFamily: FONT.sans, fontSize: 13, lineHeight: 1.7 }}>{children}</div>
}
