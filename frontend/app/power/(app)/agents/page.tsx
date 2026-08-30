'use client'

/**
 * /power/agents — the Chart Agent as a 3-column AI market ANALYST (not a screener).
 *
 *   LEFT   = what to look at   → Market-scan summary · pattern categories · my agents
 *   MIDDLE = what the AI tells me → the AI AGENT STORYLINE: a ranked newswire of one-line
 *            findings, tier-iconed (🔥 qualified · ⚡ strong · 👀 watch · ⚠ weak),
 *            progressively revealed. Visually DOMINANT.
 *   RIGHT  = show me the proof  → EVIDENCE & DEEP DIVE: candlestick chart with the detected
 *            pattern DRAWN on it + quality · historical evidence · win/loss path · decision ·
 *            watch plan (see DeepDivePanel).
 *
 * WIRED TO THE BACKEND (nothing fabricated):
 *   • pattern library  ← GET /api/agents/chart-v1                    (manifest.patterns)
 *   • market scan      ← GET /api/agents/chart/scan?date=…&full=1    (summary + ranked occurrences)
 *   • deep dive        ← GET /api/agents/chart/{setup,bars}?symbol=… (right column)
 *
 * HONESTY: tier/quality/evidence are shown only when the backend emits them; where a datum is
 * genuinely absent the UI shows a "coming / insufficient data" state — never invented numbers.
 * SECTOR market-story and INTRADAY tracking are labelled "coming" until the feed carries them.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import { useBreakpoint } from '@/lib/terminal-ui'
import * as A from '@/lib/agents-api'
import { DeepDivePanel } from '@/components/power/agents/DeepDivePanel'

// ── format ──
const pctS = (v?: number | null, dp = 1) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
const bigNum = (v?: number | null) => (v == null ? '—' : v.toLocaleString('en-IN'))

const toneColor = (t: 'green' | 'amber' | 'red' | 'neutral') =>
  t === 'green' ? T.g : t === 'amber' ? T.a : t === 'red' ? T.r : T.t2

// pretty one-liner for a finding when the backend has not pre-baked a hook
function buildHook(o: A.ScanRow): string {
  const parts: string[] = [`${A.patternShort(o.pattern)} ${String(o.stage).toLowerCase()}`]
  if (o.distance_pct != null) parts.push(`${pctS(o.distance_pct)} past level`)
  const es = o.evidence_summary
  if (es && es.etv_t5 != null && es.n != null) parts.push(`T+5 edge ${pctS(es.etv_t5)} (n=${es.n})`)
  else if (o.volume_x != null) parts.push(`${o.volume_x.toFixed(1)}× vol`)
  return parts.join(' · ')
}

// ── static "my agents" roster (honest: only the Chart agent is live; the rest are Coming Soon) ──
const MY_AGENTS = [
  { name: 'Chart Pattern Agent', live: true },
  { name: 'Options Flow Agent', live: false },
  { name: 'Earnings Agent', live: false },
  { name: 'Sector Rotation Agent', live: false },
  { name: 'Gap Agent', live: false },
  { name: 'Volume Agent', live: false },
]

type VerdictFilter = 'ALL' | 'QUALIFIED' | 'WATCH' | 'REJECTED'
type Family = 'ALL' | A.PatternFamily
type DirFilter = 'ALL' | 'BULLISH' | 'BEARISH'
type SortKey = 'rank' | 'quality' | 'edge' | 'newest'

const SORTS: { key: SortKey; label: string }[] = [
  { key: 'rank', label: 'Agent Rank' },
  { key: 'quality', label: 'Quality' },
  { key: 'edge', label: 'Historical Edge' },
  { key: 'newest', label: 'Newest' },
]
const FAMILIES: Family[] = ['ALL', 'Triangle', 'Wedge', 'Channel', 'Horizontal', 'Cup']

export default function AgentsPage() {
  const { width } = useBreakpoint()
  const stacked = width < 1100

  const [manifest, setManifest] = useState<A.ManifestResp | null>(null)
  const [scan, setScan] = useState<A.ScanResp | null>(null)
  const [date, setDate] = useState<string>('')
  const [busy, setBusy] = useState(true)
  const [stale, setStale] = useState(false)

  const [leftPattern, setLeftPattern] = useState<string>('ALL')  // exact pattern id from the LEFT categories
  const [verdict, setVerdict] = useState<VerdictFilter>('ALL')
  const [family, setFamily] = useState<Family>('ALL')
  const [dir, setDir] = useState<DirFilter>('ALL')
  const [sort, setSort] = useState<SortKey>('rank')
  const [visible, setVisible] = useState(18)
  const [selected, setSelected] = useState<A.ScanRow | null>(null)

  // land on the freshest precomputed screen (parallel probe; honest fallback)
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

  const occ = scan?.occurrences ?? []
  const patterns = manifest?.patterns ?? []

  // per-pattern counts: backend by_pattern if present, else computed
  const patternCounts = useMemo(() => {
    if (scan?.by_pattern) return scan.by_pattern
    const m: Record<string, number> = {}
    occ.forEach((o) => { m[o.pattern] = (m[o.pattern] || 0) + 1 })
    return m
  }, [scan, occ])

  const hasTiers = useMemo(() => occ.some((o) => o.tier), [occ])
  const qualifiedCount = scan?.qualified ?? (hasTiers ? occ.filter((o) => o.tier === 'qualified' || o.tier === 'strong').length : null)
  const meaningfulCount = scan?.statistically_meaningful ?? null

  // filter + rank the findings
  const findings = useMemo(() => {
    const inVerdict = (o: A.ScanRow) => {
      if (verdict === 'ALL') return true
      const t = o.tier || 'watch'
      if (verdict === 'QUALIFIED') return t === 'qualified' || t === 'strong'
      if (verdict === 'WATCH') return t === 'watch'
      return t === 'weak'
    }
    const inFamily = (o: A.ScanRow) => family === 'ALL' || A.patternFamily(o.pattern) === family
    const inDir = (o: A.ScanRow) => dir === 'ALL' || (dir === 'BULLISH' ? o.direction !== 'short' : o.direction === 'short')
    const inLeft = (o: A.ScanRow) => leftPattern === 'ALL' || o.pattern === leftPattern
    const filtered = occ.filter((o) => inVerdict(o) && inFamily(o) && inDir(o) && inLeft(o))
    const rank = (o: A.ScanRow) => A.TIER_RANK[o.tier || 'watch'] ?? 2
    return [...filtered].sort((a, b) => {
      if (sort === 'quality') return (b.quality_score ?? -1) - (a.quality_score ?? -1)
      if (sort === 'edge') return (b.evidence_summary?.etv_t5 ?? -999) - (a.evidence_summary?.etv_t5 ?? -999)
      if (sort === 'newest') return (b.as_of_date || '').localeCompare(a.as_of_date || '') || (b.volume_x ?? 0) - (a.volume_x ?? 0)
      // rank: tier, then quality, then volume
      return rank(a) - rank(b) || (b.quality_score ?? -1) - (a.quality_score ?? -1) || (b.volume_x ?? 0) - (a.volume_x ?? 0)
    })
  }, [occ, verdict, family, dir, leftPattern, sort])

  // auto-select the top finding once, so RIGHT is never empty
  useEffect(() => { setVisible(18) }, [verdict, family, dir, leftPattern, sort])
  useEffect(() => {
    if (!selected && findings.length > 0) setSelected(findings[0])
  }, [findings, selected])

  // market-story breadth lines (real; sector/intraday flagged coming)
  const story = useMemo(() => {
    const bull = occ.filter((o) => o.direction !== 'short').length
    const bear = occ.filter((o) => o.direction === 'short').length
    const breakouts = occ.filter((o) => o.stage === 'BREAKOUT').length
    const hiVol = occ.filter((o) => (o.volume_x ?? 0) >= 3).length
    const topPat = Object.entries(patternCounts).sort((a, b) => b[1] - a[1])[0]
    const lines: string[] = []
    if (occ.length) lines.push(`Breadth: ${bull} bullish vs ${bear} bearish setups across the screen.`)
    if (breakouts) lines.push(`${breakouts} setups are at the breakout stage right now.`)
    if (hiVol) lines.push(`${hiVol} confirm on heavy volume (>3× average).`)
    if (topPat) lines.push(`Most-active pattern today: ${A.patternShort(topPat[0])} (${topPat[1]}).`)
    return lines
  }, [occ, patternCounts])

  const scanBad = scan && scan.ok === false
  const pending = scan?.served === 'pending'

  // ── column styles ──
  const colScroll: React.CSSProperties = stacked
    ? { }
    : { height: '100dvh', overflowY: 'auto', minHeight: 0 }

  return (
    <div
      style={{
        display: 'flex', flexDirection: stacked ? 'column' : 'row',
        height: stacked ? 'auto' : '100dvh', minHeight: 0,
        color: T.t, fontFamily: 'var(--font-geist-sans), system-ui, sans-serif',
        background: 'linear-gradient(180deg, #080812, #06060d)',
      }}
    >
      {/* ══════════════ LEFT ══════════════ */}
      <aside
        style={{
          ...colScroll,
          width: stacked ? '100%' : 268, flexShrink: 0,
          borderRight: stacked ? 'none' : `1px solid ${T.b}`,
          borderBottom: stacked ? `1px solid ${T.b}` : 'none',
          background: T.s1, padding: '16px 14px',
        }}
      >
        {/* market scan summary */}
        <Eyebrow>Market Scan Summary</Eyebrow>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 18 }}>
          <SummaryStat label="Stocks Analyzed" value={busy ? '…' : bigNum(scan?.scanned)} />
          <SummaryStat label="Patterns Found" value={busy ? '…' : bigNum(scan?.count)} color={T.t} />
          <SummaryStat label="Stat. Meaningful" value={busy ? '…' : bigNum(meaningfulCount)} color={T.a} />
          <SummaryStat label="Qualified" value={busy ? '…' : bigNum(qualifiedCount)} color={T.g} />
        </div>

        {/* pattern categories */}
        <Eyebrow>Pattern Categories</Eyebrow>
        <div style={{ marginBottom: 18 }}>
          <CategoryRow label="All Opportunities" count={occ.length} active={leftPattern === 'ALL'} onClick={() => setLeftPattern('ALL')} strong />
          {patterns.map((p) => (
            <CategoryRow
              key={p.pattern_id}
              label={A.patternShort(p.pattern_id, p.name)}
              count={patternCounts[p.pattern_id] ?? 0}
              active={leftPattern === p.pattern_id}
              onClick={() => setLeftPattern(leftPattern === p.pattern_id ? 'ALL' : p.pattern_id)}
              soon={p.status !== 'built'}
            />
          ))}
          {patterns.length === 0 && <div style={{ color: T.t3, fontSize: 11.5, padding: '6px 4px' }}>loading library…</div>}
        </div>

        {/* my agents */}
        <Eyebrow>My Agents</Eyebrow>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {MY_AGENTS.map((a) => (
            <div key={a.name} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              padding: '9px 11px', borderRadius: 10,
              background: a.live ? 'rgba(0,201,138,0.06)' : T.s2, border: `1px solid ${a.live ? T.gb : T.b}`,
            }}>
              <span style={{ fontSize: 12.5, fontWeight: 700, color: a.live ? T.t : T.t2 }}>{a.name}</span>
              <span style={{
                fontSize: 8.5, fontWeight: 900, letterSpacing: '.06em', padding: '2px 7px', borderRadius: 999,
                color: a.live ? T.g : T.t3, background: a.live ? 'rgba(0,201,138,0.12)' : 'rgba(255,255,255,0.05)',
                border: `1px solid ${a.live ? T.gb : T.b}`,
              }}>{a.live ? 'ACTIVE' : 'SOON'}</span>
            </div>
          ))}
        </div>
      </aside>

      {/* ══════════════ MIDDLE (dominant) ══════════════ */}
      <main style={{ ...colScroll, flex: 1, minWidth: 0, padding: stacked ? '18px 16px' : '20px 26px' }}>
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8, marginBottom: 4 }}>
          <h1 style={{ fontSize: 22, fontWeight: 900, margin: 0, letterSpacing: '-.01em' }}>AI Agent Storyline</h1>
          <span style={{ fontSize: 11, color: T.t3 }}>
            {date && <>as of <b style={{ color: T.t2 }}>{date}</b>{scan?.served === 'precompute' ? ' · precomputed' : ''}</>}
          </span>
        </div>

        {stale && (
          <div style={{ margin: '10px 0', fontSize: 12, color: T.a, background: 'rgba(255,209,102,0.07)', border: `1px solid rgba(255,209,102,0.22)`, borderRadius: 10, padding: '9px 12px' }}>
            No precomputed screen in the last 7 days — showing the last-known populated screen ({date}). The post-market job builds each day after close.
          </div>
        )}

        {/* scan-summary opening lines */}
        {!busy && !scanBad && !pending && (
          <div style={{ margin: '12px 0 6px', display: 'flex', flexDirection: 'column', gap: 5 }}>
            <SummaryLine text={`Scan complete — ${bigNum(scan?.scanned)} stocks analyzed of ${bigNum(scan?.universe_size)} universe.`} />
            <SummaryLine text={`${bigNum(scan?.count)} valid patterns detected across ${Object.keys(patternCounts).length} pattern types.`} />
            <SummaryLine text={meaningfulCount != null ? `${bigNum(meaningfulCount)} clear the statistical-meaningfulness bar; ${bigNum(qualifiedCount)} fully qualify.` : `Statistical-meaningfulness & qualification tiers coming from the ranked-scan feed — most setups are small-N (WATCH).`} muted={meaningfulCount == null} />
          </div>
        )}

        {/* states */}
        {busy ? (
          <Empty>Scanning the universe…</Empty>
        ) : scanBad ? (
          <Empty><b style={{ color: T.r }}>Scanner unavailable.</b><br />{scan?.error || 'The agent data source is offline.'}</Empty>
        ) : pending ? (
          <Empty><b style={{ color: T.a }}>Screen builds post-market.</b><br />No precomputed screen for {date} yet — the EOD job scans after close.</Empty>
        ) : (
          <>
            {/* filters */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 14, alignItems: 'center', margin: '14px 0 6px', padding: '10px 0', borderTop: `1px solid ${T.b}`, borderBottom: `1px solid ${T.b}` }}>
              <ChipGroup label="" options={[['ALL', 'All'], ['QUALIFIED', 'Qualified'], ['WATCH', 'Watch'], ['REJECTED', 'Rejected']]} value={verdict} onChange={(v) => setVerdict(v as VerdictFilter)} />
              <ChipGroup label="Pattern" options={FAMILIES.map((f) => [f, f === 'ALL' ? 'All' : f] as [string, string])} value={family} onChange={(v) => setFamily(v as Family)} />
              <ChipGroup label="Dir" options={[['ALL', 'All'], ['BULLISH', 'Bullish'], ['BEARISH', 'Bearish']]} value={dir} onChange={(v) => setDir(v as DirFilter)} />
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 'auto' }}>
                <span style={{ fontSize: 10.5, color: T.t3, fontWeight: 800, letterSpacing: '.05em' }}>SORT</span>
                <select value={sort} onChange={(e) => setSort(e.target.value as SortKey)}
                  style={{ background: T.s2, border: `1px solid ${T.b2}`, color: T.t, borderRadius: 9, padding: '6px 9px', fontSize: 12, fontFamily: 'inherit', outline: 'none' }}>
                  {SORTS.map((s) => <option key={s.key} value={s.key}>{s.label}</option>)}
                </select>
              </div>
            </div>

            {/* TOP FINDINGS */}
            <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.1em', fontWeight: 800, margin: '14px 0 8px' }}>
              Top Findings <span style={{ color: T.t2, fontFamily: T.mono }}>{findings.length}</span>
            </div>
            {findings.length === 0 ? (
              <Empty>No findings match this filter — clear a filter to widen the tape.</Empty>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                {findings.slice(0, visible).map((o, i) => (
                  <FindingLine
                    key={`${o.stock}-${o.pattern}-${i}`}
                    row={o}
                    active={selected?.stock === o.stock && selected?.pattern === o.pattern}
                    onClick={() => setSelected(o)}
                  />
                ))}
                {visible < findings.length && (
                  <button
                    onClick={() => setVisible((v) => v + 20)}
                    style={{ marginTop: 10, alignSelf: 'flex-start', cursor: 'pointer', border: `1px solid ${T.b2}`, background: 'transparent', color: T.t2, borderRadius: 999, padding: '8px 16px', fontSize: 12.5, fontWeight: 700 }}
                  >
                    View next {Math.min(20, findings.length - visible)} of {findings.length}
                  </button>
                )}
              </div>
            )}

            {/* MARKET STORY */}
            <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.1em', fontWeight: 800, margin: '22px 0 8px' }}>Market Story</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {story.map((s, i) => <SummaryLine key={i} text={s} />)}
              <SummaryLine text="Sector rotation view — coming (sector tags are not yet in the scan feed)." muted />
              <SummaryLine text="Intraday minute-tracking — coming (the agent runs post-market EOD today)." muted />
            </div>
          </>
        )}
      </main>

      {/* ══════════════ RIGHT ══════════════ */}
      <aside
        style={{
          ...(stacked ? {} : { height: '100dvh', minHeight: 0 }),
          width: stacked ? '100%' : 412, flexShrink: 0,
          borderLeft: stacked ? 'none' : `1px solid ${T.b}`,
          borderTop: stacked ? `1px solid ${T.b}` : 'none',
          background: T.s1, display: 'flex', flexDirection: 'column',
          ...(stacked ? { minHeight: 560 } : {}),
        }}
      >
        <div style={{ padding: '13px 16px 0' }}>
          <Eyebrow>Evidence &amp; Deep Dive</Eyebrow>
        </div>
        <div style={{ flex: 1, minHeight: 0 }}>
          {selected ? (
            <DeepDivePanel row={selected} date={date} />
          ) : (
            <Empty>Click a finding in the storyline to see the proof — the pattern chart, quality, evidence, win/loss path, decision and watch plan.</Empty>
          )}
        </div>
      </aside>
    </div>
  )
}

// ─────────────────────────────────────────── small components
function Eyebrow({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 10.5, color: T.g, textTransform: 'uppercase', letterSpacing: '.12em', fontWeight: 800, marginBottom: 10 }}>{children}</div>
}
function Empty({ children }: { children: React.ReactNode }) {
  return <div style={{ padding: '40px 20px', textAlign: 'center', color: T.t3, fontSize: 13, lineHeight: 1.7 }}>{children}</div>
}
function SummaryStat({ label, value, color = T.t }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: '10px 11px' }}>
      <div style={{ fontSize: 9, color: T.t3, textTransform: 'uppercase', letterSpacing: '.05em', fontWeight: 800, lineHeight: 1.3 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 22, fontWeight: 900, color, marginTop: 3 }}>{value}</div>
    </div>
  )
}
function CategoryRow({ label, count, active, onClick, strong, soon }:
  { label: string; count: number; active: boolean; onClick: () => void; strong?: boolean; soon?: boolean }) {
  return (
    <button
      onClick={onClick}
      style={{
        width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8,
        padding: '7px 10px', borderRadius: 9, cursor: 'pointer', textAlign: 'left', marginBottom: 2,
        background: active ? 'rgba(0,201,138,0.10)' : 'transparent',
        border: `1px solid ${active ? T.gb : 'transparent'}`,
      }}
    >
      <span style={{ fontSize: 12.5, fontWeight: strong ? 800 : 600, color: active ? T.g : soon ? T.t3 : T.t2, display: 'flex', alignItems: 'center', gap: 6 }}>
        {label}
        {soon && <span style={{ fontSize: 8, color: T.t3, border: `1px solid ${T.b}`, borderRadius: 4, padding: '0 4px', fontWeight: 800 }}>SOON</span>}
      </span>
      <span style={{ fontFamily: T.mono, fontSize: 11.5, fontWeight: 800, color: count > 0 ? (active ? T.g : T.t2) : T.t3 }}>{count}</span>
    </button>
  )
}
function ChipGroup({ label, options, value, onChange }:
  { label: string; options: [string, string][]; value: string; onChange: (v: string) => void }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
      {label && <span style={{ fontSize: 10.5, color: T.t3, fontWeight: 800, letterSpacing: '.05em' }}>{label.toUpperCase()}</span>}
      <div style={{ display: 'flex', gap: 3, background: T.s2, border: `1px solid ${T.b}`, borderRadius: 999, padding: 2 }}>
        {options.map(([v, l]) => {
          const active = value === v
          return (
            <button key={v} onClick={() => onChange(v)}
              style={{
                cursor: 'pointer', border: 'none', borderRadius: 999, padding: '5px 11px', fontSize: 11.5, fontWeight: active ? 800 : 600,
                background: active ? 'rgba(0,201,138,0.16)' : 'transparent', color: active ? T.g : T.t3,
              }}>
              {l}
            </button>
          )
        })}
      </div>
    </div>
  )
}
function SummaryLine({ text, muted }: { text: string; muted?: boolean }) {
  return (
    <div style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 13, color: muted ? T.t3 : T.t2, lineHeight: 1.55 }}>
      <span style={{ color: T.g, fontWeight: 900, flexShrink: 0 }}>›</span>
      <span style={muted ? { fontStyle: 'italic' } : undefined}>{text}</span>
    </div>
  )
}
function FindingLine({ row, active, onClick }: { row: A.ScanRow; active: boolean; onClick: () => void }) {
  const meta = A.tierMeta(row.tier)
  const c = toneColor(meta.tone)
  return (
    <button
      onClick={onClick}
      style={{
        width: '100%', display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', textAlign: 'left',
        padding: '10px 12px', borderRadius: 10, marginBottom: 3, border: `1px solid ${active ? T.gb : 'transparent'}`,
        background: active ? 'rgba(0,201,138,0.08)' : 'transparent', transition: 'background .1s',
      }}
      onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = 'rgba(255,255,255,0.03)' }}
      onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = 'transparent' }}
    >
      <span style={{ fontSize: 14, flexShrink: 0, width: 18, textAlign: 'center' }}>{meta.glyph}</span>
      <span style={{ flex: 1, minWidth: 0, fontSize: 13, color: T.t2, lineHeight: 1.4 }}>
        <b style={{ color: T.t, fontWeight: 800 }}>{row.stock}</b>
        <span style={{ color: T.t3 }}> — </span>
        {row.hook || buildHook(row)}
      </span>
      {row.tier && (
        <span style={{ fontSize: 8.5, fontWeight: 900, letterSpacing: '.05em', padding: '2px 6px', borderRadius: 999, color: c, background: `${c}18`, border: `1px solid ${c}44`, flexShrink: 0 }}>
          {meta.label.toUpperCase()}
        </span>
      )}
      <span style={{ color: T.t3, fontSize: 14, flexShrink: 0 }}>›</span>
    </button>
  )
}
