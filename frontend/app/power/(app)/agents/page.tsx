'use client'

/**
 * /power/agents — the Chart Agent (Agent #1), scanner-first.
 *
 * THE PRODUCT: a market-wide screener. It scans the universe every post-market, classifies each
 * stock against a LIVE library of chart patterns, and — for any setup you drill into — scores it
 * against its own resolved precedents under a governed exit policy (strategy-replay ETV), then
 * explains the honest verdict. Read-only, paper, strictly point-in-time.
 *
 * WIRED TO THE LIVE BACKEND (nothing hardcoded, nothing faked):
 *   • Pattern library  ← GET /api/agents/chart-v1   (manifest.patterns; built = active, spec = soon)
 *   • Market scanner   ← GET /api/agents/chart/scan?date=…  (served precompute | pending)
 *   • Drill-down       ← GET /api/agents/chart/{storyline,decision}?symbol=…&date=…
 *
 * HONESTY: every number comes from the endpoints. At current sample sizes the per-stock decision is
 * almost always WATCH — we never invent a TRADE. There is no OHLC-bars endpoint yet, so the drill-in
 * geometry is a labelled SCHEMATIC built from the real level/distance/stage — not fabricated candles.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import { pageShellStyle, panelStyle, chipStyle, SectionEyebrow, useBreakpoint } from '@/lib/terminal-ui'
import * as A from '@/lib/agents-api'

// ─────────────────────────────────────────────────────────────────────────── format helpers
const pctSigned = (v?: number | null, dp = 2) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
const pctPlain = (v?: number | null, dp = 1) => (v == null ? '—' : v.toFixed(dp) + '%')
const num = (v?: number | null) => (v == null ? '—' : String(v))
const volX = (v?: number | null) => (v == null ? '—' : v.toFixed(2) + '×')
const rupee = (v?: number | null) =>
  v == null ? '—' : '₹' + v.toLocaleString('en-IN', { maximumFractionDigits: 2 })

type Tone = 'green' | 'red' | 'amber' | 'neutral'
const toneColor = (t: Tone) => (t === 'green' ? T.g : t === 'red' ? T.r : t === 'amber' ? T.a : T.t2)

function stageTone(s?: string): Tone {
  if (s === 'BREAKOUT') return 'green'
  if (s === 'RETEST') return 'green'
  if (s === 'APPROACHING') return 'amber'
  if (s === 'FAILED') return 'red'
  return 'neutral'
}
function decisionTone(d?: string | null): Tone {
  if (d === 'TRADE') return 'green'
  if (d === 'NO_TRADE') return 'red'
  if (d === 'WATCH') return 'amber'
  return 'neutral'
}
const dirColor = (d?: string) => (d === 'short' ? T.r : T.g)
const dirGlyph = (d?: string) => (d === 'short' ? '▼' : '▲')

// A soft stage pill (color-encoded, breakout filled vs retest outline so the two greens read apart)
function StagePill({ stage, small = true }: { stage: string; small?: boolean }) {
  const tone = stageTone(stage)
  const c = toneColor(tone)
  const filled = stage === 'BREAKOUT'
  return (
    <span
      style={{
        display: 'inline-flex', alignItems: 'center', gap: 6,
        padding: small ? '3px 9px' : '5px 12px', borderRadius: 999,
        fontSize: small ? 11 : 12.5, fontWeight: 800, letterSpacing: '.04em',
        color: filled ? '#04120c' : c,
        background: filled ? c : `${c}1a`,
        border: `1px solid ${c}${filled ? 'ff' : '55'}`,
        whiteSpace: 'nowrap',
      }}
    >
      {stage}
    </span>
  )
}

function DirTag({ dir }: { dir?: string }) {
  const c = dirColor(dir)
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: c, fontWeight: 800, fontSize: 12 }}>
      <span style={{ fontSize: 10 }}>{dirGlyph(dir)}</span>
      {(dir || 'long').toUpperCase()}
    </span>
  )
}

// ─────────────────────────────────────────────────────────────────────────── stat primitives
function StatCell({ label, value, color = T.t, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: '10px 12px' }}>
      <div style={{ fontSize: 10, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 19, fontWeight: 800, color, marginTop: 4 }}>{value}</div>
      {sub && <div style={{ color: T.t3, fontSize: 10.5, marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  background: T.s2, border: `1px solid ${T.b2}`, borderRadius: 10, color: T.t,
  padding: '9px 12px', fontSize: 13, fontFamily: 'inherit', outline: 'none',
}

// A filter chip (toggle)
function FilterChip({ label, active, tone = 'neutral', onClick, count }:
  { label: string; active: boolean; tone?: Tone; onClick: () => void; count?: number }) {
  const c = toneColor(active ? tone : 'neutral')
  return (
    <button
      onClick={onClick}
      style={{
        display: 'inline-flex', alignItems: 'center', gap: 7, cursor: 'pointer',
        padding: '6px 12px', borderRadius: 999, fontSize: 12.5, fontWeight: 700,
        color: active ? (tone === 'neutral' ? T.t : c) : T.t2,
        background: active ? (tone === 'neutral' ? 'rgba(255,255,255,0.09)' : `${c}18`) : 'transparent',
        border: `1px solid ${active ? (tone === 'neutral' ? T.b2 : `${c}55`) : T.b}`,
        transition: 'all .12s ease', whiteSpace: 'nowrap',
      }}
    >
      {label}
      {count != null && (
        <span style={{ fontFamily: T.mono, fontSize: 11, color: active ? c : T.t3, fontWeight: 800 }}>{count}</span>
      )}
    </button>
  )
}

// ─────────────────────────────────────────────────────────────────────────── setup geometry (schematic)
// HONEST: no OHLC-bars endpoint exists, so this is a schematic — the real level line and the current
// price marker placed by the real distance_pct. It is NOT candles. Labelled as such.
function SetupGeometry({ occ }: { occ: A.Occurrence | undefined }) {
  if (!occ) return null
  const level = occ.level
  const dist = occ.context?.distance_to_level_pct ?? 0
  const dir = occ.direction || 'long'
  const w = 320, h = 128, padY = 26
  const yLevel = h / 2
  // price marker: distance from level, clamped so it stays on-canvas (schematic, not to scale beyond ±8%)
  const clamped = Math.max(-8, Math.min(8, dist))
  const yPrice = yLevel - (clamped / 8) * (yLevel - padY)
  const priceUp = dist >= 0
  const c = toneColor(stageTone(occ.stage))
  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" style={{ display: 'block' }}>
        {/* level line */}
        <line x1={14} y1={yLevel} x2={w - 14} y2={yLevel} stroke={T.b2} strokeWidth={1.5} strokeDasharray="5 4" />
        <text x={16} y={yLevel - 6} fontSize={10} fill={T.t3} fontFamily="monospace">R = {rupee(level)}</text>
        {/* price path (schematic approach → marker) */}
        <path
          d={`M 30 ${yLevel + (priceUp ? 22 : -22)} C ${w * 0.4} ${yLevel + (priceUp ? 18 : -18)}, ${w * 0.62} ${yPrice}, ${w - 40} ${yPrice}`}
          fill="none" stroke={c} strokeWidth={2} opacity={0.85}
        />
        {/* current price marker */}
        <circle cx={w - 40} cy={yPrice} r={5.5} fill={c} />
        <text x={w - 30} y={yPrice + 4} fontSize={10.5} fill={c} fontFamily="monospace" fontWeight={700}>
          {pctSigned(dist)}
        </text>
        {/* direction arrow */}
        <text x={w - 22} y={priceUp ? 20 : h - 10} fontSize={13} fill={dirColor(dir)} fontFamily="monospace">
          {dirGlyph(dir)}
        </text>
      </svg>
      <div style={{ fontSize: 10.5, color: T.t3, marginTop: 6, lineHeight: 1.5 }}>
        Schematic — real level & distance ({pctSigned(dist)} from ₹{level}), {dir}. Point-in-time as of{' '}
        {occ.context?.as_of_date || 'the chosen date'}. Full OHLC candles pending a bars endpoint.
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────── drill-down overlay
function DrillDown({ row, date, onClose }: { row: A.ScanRow; date: string; onClose: () => void }) {
  const [story, setStory] = useState<A.StorylineResp | null>(null)
  const [decision, setDecision] = useState<A.DecisionResp | null>(null)
  const [busy, setBusy] = useState(true)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    let alive = true
    setBusy(true); setErr(null)
    Promise.all([A.fetchStoryline(row.stock, date), A.fetchDecision(row.stock, date)])
      .then(([st, dc]) => { if (!alive) return; setStory(st); setDecision(dc); if (!dc.ok && dc.error) setErr(dc.error) })
      .catch((e) => alive && setErr((e as Error).message || 'agent backend offline'))
      .finally(() => alive && setBusy(false))
    return () => { alive = false }
  }, [row.stock, date])

  const strat = decision?.strategy
  const fwd = decision?.pattern_forward
  const dec = decision?.decision
  const occ = decision?.occurrence

  return (
    <div
      onClick={onClose}
      style={{
        position: 'fixed', inset: 0, zIndex: 60, background: 'rgba(4,4,10,0.72)',
        backdropFilter: 'blur(4px)', display: 'flex', justifyContent: 'flex-end',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 'min(760px, 100%)', height: '100%', overflowY: 'auto',
          background: 'linear-gradient(180deg, #0c0c1a, #08080f)', borderLeft: `1px solid ${T.b2}`,
          boxShadow: '-24px 0 60px rgba(0,0,0,0.5)',
        }}
      >
        {/* header */}
        <div style={{ position: 'sticky', top: 0, zIndex: 2, background: 'rgba(10,10,22,0.94)', backdropFilter: 'blur(8px)', borderBottom: `1px solid ${T.b}`, padding: '16px 22px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12 }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 22, fontWeight: 900, letterSpacing: '.01em' }}>{row.stock}</span>
                <StagePill stage={row.stage} small={false} />
                <DirTag dir={row.direction} />
              </div>
              <div style={{ color: T.t3, fontSize: 12.5, marginTop: 4 }}>
                {A.patternShort(row.pattern)} · level {rupee(row.level)} · {pctSigned(row.distance_pct)} from level · vol {volX(row.volume_x)} · {row.touches} touches · as of {row.as_of_date || date}
              </div>
            </div>
            <button onClick={onClose} style={{ ...inputStyle, cursor: 'pointer', padding: '7px 12px', fontWeight: 800, color: T.t2 }}>✕ Close</button>
          </div>
        </div>

        <div style={{ padding: '20px 22px' }}>
          {err && (
            <div style={{ fontSize: 12.5, color: T.r, background: 'rgba(255,77,109,0.08)', border: `1px solid rgba(255,77,109,0.25)`, borderRadius: 10, padding: '10px 12px', marginBottom: 16 }}>
              {err}
            </div>
          )}

          {/* decision banner */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
            <span style={{ ...chipStyle(decisionTone(dec)), fontSize: 15, padding: '10px 18px', fontWeight: 900 }}>{dec || (busy ? '…' : '—')}</span>
            {decision?.basis && <span style={chipStyle('neutral', true)}>basis · {decision.basis}</span>}
            {strat?.version && <span style={chipStyle('neutral', true)}>policy · {strat.version}</span>}
          </div>
          {decision?.reason && <div style={{ fontSize: 13, color: T.t2, lineHeight: 1.6, marginBottom: 18 }}>{decision.reason}</div>}

          {/* geometry + storyline */}
          <div style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 16, padding: 16, marginBottom: 16 }}>
            <SectionEyebrow>Setup</SectionEyebrow>
            <SetupGeometry occ={occ} />
          </div>

          <div style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 16, padding: 16, marginBottom: 16 }}>
            <SectionEyebrow>Storyline</SectionEyebrow>
            {busy && !story ? (
              <div style={{ color: T.t3, fontSize: 13, padding: '10px 0' }}>Reading the tape…</div>
            ) : story?.ok && story.events.length > 0 ? (
              <div style={{ paddingLeft: 2 }}>
                {story.events.map((e, i) => {
                  const isDecision = e.kind === 'decision'
                  const dot = isDecision ? toneColor(decisionTone(story.decision)) : T.g
                  const last = i === story.events.length - 1
                  return (
                    <div key={i} style={{ display: 'flex', gap: 12, paddingBottom: last ? 0 : 14 }}>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                        <div style={{ width: 11, height: 11, borderRadius: 999, background: dot, marginTop: 4, flexShrink: 0 }} />
                        {!last && <div style={{ width: 2, flex: 1, background: T.b, marginTop: 2 }} />}
                      </div>
                      <div>
                        <div style={{ fontSize: 13.5, fontWeight: 700, color: isDecision ? dot : T.t }}>{e.title}</div>
                        <div style={{ fontSize: 12.5, color: T.t2, lineHeight: 1.6, marginTop: 2 }}>{e.detail}</div>
                        {e.spec_note && <div style={{ fontSize: 10.5, color: T.t3, marginTop: 4, fontStyle: 'italic' }}>SPEC: {e.spec_note}</div>}
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ color: T.t3, fontSize: 13 }}>No storyline available for this setup.</div>
            )}
          </div>

          {/* evidence */}
          <div style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 16, padding: 16 }}>
            <SectionEyebrow>Evidence</SectionEyebrow>

            {strat ? (
              <>
                <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800, marginBottom: 8 }}>
                  Strategy-replay ({strat.version || 'policy'}) · n = {num(strat.n)}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(96px, 1fr))', gap: 8, marginBottom: 10 }}>
                  <StatCell label="ETV" value={pctSigned(strat.etv)} color={(strat.etv ?? 0) >= 0 ? T.g : T.r} />
                  <StatCell label="Win" value={strat.win == null ? '—' : strat.win + '%'} />
                  <StatCell label="Payoff" value={num(strat.payoff)} />
                  <StatCell label="CI-low" value={pctSigned(strat.ci_low)} color={(strat.ci_low ?? -1) > 0 ? T.g : T.t2} sub="normal-approx SE" />
                  <StatCell label="Avg MAE" value={pctSigned(strat.mae)} color={T.t2} />
                  <StatCell label="Avg hold" value={strat.avg_holding == null ? '—' : strat.avg_holding + 'd'} />
                </div>
                {strat.exits && (
                  <div style={{ fontSize: 11, color: T.t3, marginBottom: 14 }}>
                    Exits · {Object.entries(strat.exits).map(([k, v]) => `${k} ${v}`).join(' · ')}
                  </div>
                )}
              </>
            ) : !busy && (
              <div style={{ color: T.t2, fontSize: 12.5, marginBottom: 14 }}>
                No resolved precedents to score yet — honest insufficient-evidence state.
              </div>
            )}

            {fwd && Object.keys(fwd.horizons).length > 0 && (
              <>
                <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800, marginBottom: 8 }}>
                  Pattern-forward (hold-to-close) · n = {num(fwd.n)}
                </div>
                <div style={{ overflowX: 'auto', marginBottom: 14 }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12.5 }}>
                    <thead>
                      <tr style={{ color: T.t3 }}>
                        {['H', 'Win', 'ETV', 'MFE', 'MAE'].map((c, i) => (
                          <th key={c} style={{ textAlign: i === 0 ? 'left' : 'right', padding: '4px 6px', fontWeight: 700, fontSize: 11 }}>{c}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody style={{ fontFamily: T.mono }}>
                      {['1', '3', '5', '10'].filter((h) => fwd.horizons[h]).map((h) => {
                        const r = fwd.horizons[h]
                        return (
                          <tr key={h} style={{ borderTop: `1px solid ${T.b}` }}>
                            <td style={{ padding: '6px', color: T.t2 }}>T+{h}</td>
                            <td style={{ padding: '6px', textAlign: 'right', color: T.t2 }}>{r.win == null ? '—' : r.win + '%'}</td>
                            <td style={{ padding: '6px', textAlign: 'right', color: (r.etv ?? 0) >= 0 ? T.g : T.r }}>{pctSigned(r.etv)}</td>
                            <td style={{ padding: '6px', textAlign: 'right', color: T.g }}>{pctSigned(r.mfe)}</td>
                            <td style={{ padding: '6px', textAlign: 'right', color: T.r }}>{pctSigned(r.mae)}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            {decision?.gates && decision.gates.length > 0 && (
              <>
                <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800, marginBottom: 8 }}>Gates (§9)</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {decision.gates.map((g, i) => (
                    <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 12 }}>
                      <span style={{ width: 14, textAlign: 'center', color: g.skipped ? T.t3 : g.pass ? T.g : T.r, fontWeight: 900 }}>
                        {g.skipped ? '–' : g.pass ? '✓' : '✕'}
                      </span>
                      <span style={{ color: T.t2 }}><b style={{ color: T.t }}>{g.gate}</b> — {g.reason}</span>
                    </div>
                  ))}
                </div>
              </>
            )}

            {decision?.spec_note && (
              <div style={{ fontSize: 10.5, color: T.t3, marginTop: 14, fontStyle: 'italic', lineHeight: 1.6 }}>
                SPEC · {decision.spec_note}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────── page
type SortKey = 'stock' | 'pattern' | 'stage' | 'level' | 'distance_pct' | 'volume_x' | 'touches'
const STAGE_RANK: Record<string, number> = { BREAKOUT: 0, RETEST: 1, APPROACHING: 2, FAILED: 3 }
const STAGE_ORDER = ['BREAKOUT', 'RETEST', 'APPROACHING', 'FAILED']

function todayISO() {
  return new Date().toISOString().slice(0, 10)
}

export default function AgentsPage() {
  const { isMobile } = useBreakpoint()
  const [manifest, setManifest] = useState<A.ManifestResp | null>(null)
  const [date, setDate] = useState<string>(todayISO())
  const [scan, setScan] = useState<A.ScanResp | null>(null)
  const [scanBusy, setScanBusy] = useState(true)
  const [staleFallback, setStaleFallback] = useState<string | null>(null) // set only when nothing recent was populated

  const [patternFilter, setPatternFilter] = useState<string>('ALL')
  const [stageFilter, setStageFilter] = useState<string>('ALL')
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'stage', dir: 'asc' })
  const [selected, setSelected] = useState<A.ScanRow | null>(null)

  // Manual scan for an EXACT date (from the date picker). No fallback — the honest
  // pending/empty/error state for that date is what we want to show.
  const runScan = useCallback(async (dt: string) => {
    setScanBusy(true); setStaleFallback(null)
    try {
      setScan(await A.fetchScan(dt, { full: true }))
    } catch {
      setScan({ ok: false, date: dt, count: 0, occurrences: [], error: 'scanner unreachable' })
    } finally {
      setScanBusy(false)
    }
  }, [])

  // On mount: land on the FRESHEST precomputed screen. Probe the last 7 calendar days in parallel
  // (handles weekends/holidays + the not-yet-built current day) and pick the most-recent populated
  // date. If none is populated, fall back to the last-known-good date with an honest banner.
  const landFreshest = useCallback(async () => {
    setScanBusy(true); setStaleFallback(null)
    try {
      const fresh = await A.findFreshestScan(7)
      if (fresh) {
        setDate(fresh.date)
        setScan(fresh.scan)
      } else {
        const fb = await A.fetchScan(A.KNOWN_POPULATED_DATE, { full: true })
        setDate(A.KNOWN_POPULATED_DATE)
        setStaleFallback(A.KNOWN_POPULATED_DATE)
        setScan(fb)
      }
    } catch {
      setScan({ ok: false, date: todayISO(), count: 0, occurrences: [], error: 'scanner unreachable' })
    } finally {
      setScanBusy(false)
    }
  }, [])

  useEffect(() => {
    A.fetchManifest().then(setManifest).catch(() => setManifest(null))
    landFreshest() // auto-land on the freshest available precomputed screen
  }, [landFreshest])

  const patterns = manifest?.patterns ?? []
  const occ = scan?.occurrences ?? []

  // filter-chip counts (respect the OTHER filter so counts stay meaningful)
  const patternCounts = useMemo(() => {
    const m: Record<string, number> = {}
    occ.filter((o) => stageFilter === 'ALL' || o.stage === stageFilter).forEach((o) => { m[o.pattern] = (m[o.pattern] || 0) + 1 })
    return m
  }, [occ, stageFilter])
  const stageCounts = useMemo(() => {
    const m: Record<string, number> = {}
    occ.filter((o) => patternFilter === 'ALL' || o.pattern === patternFilter).forEach((o) => { m[o.stage] = (m[o.stage] || 0) + 1 })
    return m
  }, [occ, patternFilter])

  const rows = useMemo(() => {
    const filtered = occ.filter((o) =>
      (patternFilter === 'ALL' || o.pattern === patternFilter) &&
      (stageFilter === 'ALL' || o.stage === stageFilter))
    const dirMul = sort.dir === 'asc' ? 1 : -1
    return [...filtered].sort((a, b) => {
      let av: number | string, bv: number | string
      if (sort.key === 'stage') { av = STAGE_RANK[a.stage] ?? 9; bv = STAGE_RANK[b.stage] ?? 9 }
      else if (sort.key === 'stock' || sort.key === 'pattern') { av = a[sort.key] || ''; bv = b[sort.key] || '' }
      else { av = a[sort.key] ?? -Infinity; bv = b[sort.key] ?? -Infinity }
      if (av < bv) return -1 * dirMul
      if (av > bv) return 1 * dirMul
      // stable secondary: volume desc
      return (b.volume_x ?? 0) - (a.volume_x ?? 0)
    })
  }, [occ, patternFilter, stageFilter, sort])

  const patternsInScan = useMemo(() => Array.from(new Set(occ.map((o) => o.pattern))), [occ])

  const setSortKey = (key: SortKey) =>
    setSort((s) => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: key === 'stock' || key === 'pattern' || key === 'stage' ? 'asc' : 'desc' }))

  const served = scan?.served
  const coverage = scan && scan.ok && served === 'precompute'

  return (
    <div style={{ ...pageShellStyle(), padding: isMobile ? 16 : 28 }}>
      <div style={{ maxWidth: 1280, margin: '0 auto' }}>

        {/* ═══════════════ HEADER ═══════════════ */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 14, marginBottom: 22 }}>
          <div style={{ maxWidth: 760 }}>
            <SectionEyebrow>KANIDA · Agent Platform · Agent #1</SectionEyebrow>
            <h1 style={{ fontSize: isMobile ? 26 : 32, fontWeight: 900, margin: '0 0 8px', letterSpacing: '-0.01em' }}>Chart Agent</h1>
            <p style={{ color: T.t2, fontSize: 14.5, margin: 0, lineHeight: 1.6 }}>
              A market-wide screener. Every post-market it scans the universe, classifies each stock against a live
              library of chart patterns, and scores each setup against its <b style={{ color: T.g }}>own resolved precedents</b>{' '}
              under a governed exit policy. It explains the honest verdict — and at today&apos;s sample sizes that is
              usually <b style={{ color: T.a }}>WATCH</b>. It never invents a trade.
            </p>
          </div>
          <span style={chipStyle('neutral')}>read-only · paper · point-in-time</span>
        </div>

        {/* ═══════════════ PATTERN LIBRARY (from the LIVE manifest) ═══════════════ */}
        <div style={{ ...panelStyle(isMobile ? 16 : 20), marginBottom: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8, marginBottom: 14 }}>
            <SectionEyebrow>Pattern Library</SectionEyebrow>
            <span style={{ fontSize: 11.5, color: T.t3 }}>
              {manifest ? `${patterns.filter((p) => p.status === 'built').length} of ${patterns.length} detectors live` : 'loading manifest…'}
            </span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: `repeat(auto-fill, minmax(${isMobile ? 150 : 190}px, 1fr))`, gap: 10 }}>
            {patterns.length === 0 && !manifest && Array.from({ length: 9 }).map((_, i) => (
              <div key={i} style={{ height: 68, background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, opacity: 0.5 }} />
            ))}
            {patterns.map((p) => {
              const built = p.status === 'built'
              const dir = A.PATTERN_DIRECTION[p.pattern_id]
              return (
                <div key={p.pattern_id}
                  style={{
                    background: built ? 'rgba(0,201,138,0.05)' : T.s2,
                    border: `1px solid ${built ? T.gb : T.b}`, borderRadius: 12, padding: '11px 12px',
                    display: 'flex', flexDirection: 'column', gap: 6, minHeight: 66,
                  }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 13, fontWeight: 700, color: T.t }}>{A.patternShort(p.pattern_id, p.name)}</span>
                    <span style={{
                      fontSize: 9.5, fontWeight: 900, letterSpacing: '.06em', padding: '2px 7px', borderRadius: 999,
                      color: built ? T.g : T.t3, background: built ? 'rgba(0,201,138,0.12)' : 'rgba(255,255,255,0.05)',
                      border: `1px solid ${built ? T.gb : T.b}`,
                    }}>{built ? 'ACTIVE' : 'SOON'}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: T.t3 }}>
                    {dir && (
                      <span style={{ color: dir === 'either' ? T.t3 : dirColor(dir === 'short' ? 'short' : 'long'), fontWeight: 700 }}>
                        {dir === 'either' ? '↕ either' : `${dirGlyph(dir === 'short' ? 'short' : 'long')} ${dir}`}
                      </span>
                    )}
                    <span style={{ color: T.t3 }}>daily</span>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* ═══════════════ MARKET SCANNER (the hero) ═══════════════ */}
        <div style={panelStyle(0)}>
          {/* toolbar */}
          <div style={{ padding: isMobile ? 16 : 20, borderBottom: `1px solid ${T.b}` }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', flexWrap: 'wrap', gap: 14 }}>
              <div>
                <SectionEyebrow>Market Scanner</SectionEyebrow>
                <div style={{ fontSize: 13.5, color: T.t2, maxWidth: 560, lineHeight: 1.55 }}>
                  Every stock in the universe matching a pattern today. Filter by pattern and stage; click any row for
                  the storyline, geometry and evidence.
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <label style={{ fontSize: 11, color: T.t3, fontWeight: 700 }}>SCAN DATE</label>
                <input type="date" value={date} max={todayISO()} onChange={(e) => setDate(e.target.value)} style={{ ...inputStyle, width: 158 }} />
                <button onClick={() => runScan(date)} disabled={scanBusy}
                  style={{ border: 'none', background: T.g, color: '#04120c', padding: '10px 18px', borderRadius: 10, fontSize: 13, fontWeight: 800, cursor: scanBusy ? 'wait' : 'pointer' }}>
                  {scanBusy ? 'Scanning…' : 'Scan'}
                </button>
              </div>
            </div>

            {/* served + coverage line */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginTop: 14 }}>
              {served === 'precompute' && <span style={chipStyle('green', true)}>● precomputed screen</span>}
              {served === 'live' && <span style={chipStyle('amber', true)}>● live-computed</span>}
              {served === 'pending' && <span style={chipStyle('neutral', true)}>○ not yet built</span>}
              {coverage && (
                <span style={{ fontSize: 12, color: T.t3 }}>
                  <b style={{ color: T.t2 }}>{scan!.count}</b> setups · scanned <b style={{ color: T.t2 }}>{num(scan!.scanned)}</b> of {num(scan!.universe_size)} universe
                  {' · '}skipped {num(scan!.skipped_min_bars)} min-bars, {num(scan!.skipped_stale)} stale, {num(scan!.skipped_no_window)} no-window
                </span>
              )}
            </div>

            {staleFallback && (
              <div style={{ marginTop: 12, fontSize: 12, color: T.a, background: 'rgba(255,209,102,0.07)', border: `1px solid rgba(255,209,102,0.22)`, borderRadius: 10, padding: '9px 12px' }}>
                No precomputed screen in the last 7 days — the post-market job builds each day after close. Showing the last-known populated screen: <b>{date}</b>.
              </div>
            )}

            {/* filters */}
            {coverage && scan!.count > 0 && (
              <div style={{ marginTop: 14, display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 10.5, color: T.t3, fontWeight: 800, letterSpacing: '.06em', width: 56 }}>STAGE</span>
                  <FilterChip label="All" active={stageFilter === 'ALL'} onClick={() => setStageFilter('ALL')} count={scan!.count} />
                  {STAGE_ORDER.filter((s) => stageCounts[s]).map((s) => (
                    <FilterChip key={s} label={s} active={stageFilter === s} tone={stageTone(s)} count={stageCounts[s]} onClick={() => setStageFilter(stageFilter === s ? 'ALL' : s)} />
                  ))}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 10.5, color: T.t3, fontWeight: 800, letterSpacing: '.06em', width: 56 }}>PATTERN</span>
                  <FilterChip label="All" active={patternFilter === 'ALL'} onClick={() => setPatternFilter('ALL')} />
                  {patternsInScan.map((p) => (
                    <FilterChip key={p} label={A.patternShort(p)} active={patternFilter === p} count={patternCounts[p]} onClick={() => setPatternFilter(patternFilter === p ? 'ALL' : p)} />
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* table / states */}
          <div style={{ padding: isMobile ? 6 : 8 }}>
            {scanBusy ? (
              <div style={{ padding: '60px 20px', textAlign: 'center', color: T.t3, fontSize: 14 }}>Scanning the universe…</div>
            ) : scan && scan.ok === false ? (
              <div style={{ padding: '48px 20px', textAlign: 'center' }}>
                <div style={{ fontSize: 15, fontWeight: 800, color: T.r, marginBottom: 6 }}>Scanner unavailable</div>
                <div style={{ fontSize: 13, color: T.t3 }}>{scan.error || scan.note || 'The agent data source is offline.'}</div>
              </div>
            ) : served === 'pending' ? (
              <div style={{ padding: '48px 20px', textAlign: 'center' }}>
                <div style={{ fontSize: 15, fontWeight: 800, color: T.a, marginBottom: 6 }}>Screen builds post-market</div>
                <div style={{ fontSize: 13, color: T.t3, maxWidth: 460, margin: '0 auto', lineHeight: 1.6 }}>
                  No precomputed screen for {date} yet. The post-market EOD job scans the full universe after close;
                  pick a prior trading day (e.g. {A.KNOWN_POPULATED_DATE}) to see a populated screen.
                </div>
              </div>
            ) : rows.length === 0 ? (
              <div style={{ padding: '48px 20px', textAlign: 'center', color: T.t3, fontSize: 14 }}>
                No setups match this filter{scan && scan.count > 0 ? ' — clear the pattern/stage filter.' : ` on ${date}.`}
              </div>
            ) : (
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13, minWidth: 640 }}>
                  <thead>
                    <tr style={{ color: T.t3 }}>
                      {([
                        ['stock', 'Stock', 'left'], ['pattern', 'Pattern', 'left'], ['stage', 'Stage', 'left'],
                        ['direction' as SortKey, 'Dir', 'left'], ['level', 'Level', 'right'], ['distance_pct', 'Dist', 'right'],
                        ['volume_x', 'Vol', 'right'], ['touches', 'Tch', 'right'],
                      ] as [SortKey | 'direction', string, 'left' | 'right'][]).map(([key, label, align]) => {
                        const sortable = key !== 'direction'
                        const active = sort.key === key
                        return (
                          <th key={label}
                            onClick={sortable ? () => setSortKey(key as SortKey) : undefined}
                            style={{
                              textAlign: align, padding: '11px 14px', fontWeight: 700, fontSize: 11,
                              textTransform: 'uppercase', letterSpacing: '.05em',
                              cursor: sortable ? 'pointer' : 'default', userSelect: 'none',
                              color: active ? T.g : T.t3, borderBottom: `1px solid ${T.b}`, whiteSpace: 'nowrap',
                            }}>
                            {label}{active ? (sort.dir === 'asc' ? ' ↑' : ' ↓') : ''}
                          </th>
                        )
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={`${r.stock}-${r.pattern}-${i}`}
                        onClick={() => setSelected(r)}
                        style={{ cursor: 'pointer', borderBottom: `1px solid ${T.b}`, transition: 'background .1s' }}
                        onMouseEnter={(e) => (e.currentTarget.style.background = 'rgba(255,255,255,0.03)')}
                        onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}>
                        <td style={{ padding: '11px 14px', fontWeight: 800, color: T.t, whiteSpace: 'nowrap' }}>{r.stock}</td>
                        <td style={{ padding: '11px 14px', color: T.t2, whiteSpace: 'nowrap' }}>{A.patternShort(r.pattern)}</td>
                        <td style={{ padding: '11px 14px' }}><StagePill stage={r.stage} /></td>
                        <td style={{ padding: '11px 14px' }}><DirTag dir={r.direction} /></td>
                        <td style={{ padding: '11px 14px', textAlign: 'right', fontFamily: T.mono, color: T.t }}>{rupee(r.level)}</td>
                        <td style={{ padding: '11px 14px', textAlign: 'right', fontFamily: T.mono, color: (r.distance_pct ?? 0) >= 0 ? T.g : T.r }}>{pctSigned(r.distance_pct)}</td>
                        <td style={{ padding: '11px 14px', textAlign: 'right', fontFamily: T.mono, color: (r.volume_x ?? 0) >= 2 ? T.g : T.t2 }}>{volX(r.volume_x)}</td>
                        <td style={{ padding: '11px 14px', textAlign: 'right', fontFamily: T.mono, color: T.t3 }}>{r.touches}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        <div style={{ marginTop: 16, fontSize: 11, color: T.t3, textAlign: 'center', lineHeight: 1.6 }}>
          Point-in-time · paper · read-only. Every value is served by the Chart Agent backend (/api/agents/chart/*).
          Decisions gate on strategy-replay evidence; small samples (n &lt; 20) resolve to WATCH — never a fabricated TRADE.
        </div>
      </div>

      {selected && <DrillDown row={selected} date={date} onClose={() => setSelected(null)} />}
    </div>
  )
}
