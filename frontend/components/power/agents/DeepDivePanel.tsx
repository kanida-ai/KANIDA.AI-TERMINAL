'use client'

/**
 * DeepDivePanel — the RIGHT column of the Chart Agent surface: EVIDENCE & DEEP DIVE.
 *
 * "Show me the proof." It changes whenever the operator clicks a storyline line in
 * the middle column. It fetches the two deep-dive endpoints for the selected setup —
 *   • GET /api/agents/chart/setup  → geometry · quality · evidence · paths · decision · watch_plan
 *   • GET /api/agents/chart/bars   → point-in-time OHLC candles
 * — and renders six tabs over them. The Pattern Chart tab is the centrepiece: real
 * candles with the detected pattern DRAWN on top via <CandleChart/> so the operator
 * SEES why the algorithm classified it.
 *
 * HONESTY: fetches are guarded; while /setup and /bars are unbuilt (404) every tab
 * shows a designed "coming / insufficient data" state — never invented numbers.
 */
import { useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import * as A from '@/lib/agents-api'
import { CandleChart, type ChartLine, type ChartLevel, type ChartMarker } from './CandleChart'

// ── format helpers ──
const pctS = (v?: number | null, dp = 2) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
const pct = (v?: number | null, dp = 1) => (v == null ? '—' : v.toFixed(dp) + '%')
const num = (v?: number | null) => (v == null ? '—' : String(v))
const rupee = (v?: number | null) => (v == null ? '—' : '₹' + v.toLocaleString('en-IN', { maximumFractionDigits: 2 }))
const titleCase = (s: string) => s.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())

type TabKey = 'chart' | 'quality' | 'evidence' | 'paths' | 'decision' | 'watch'
const TABS: { key: TabKey; label: string }[] = [
  { key: 'chart', label: 'Pattern Chart' },
  { key: 'quality', label: 'Quality' },
  { key: 'evidence', label: 'Historical Evidence' },
  { key: 'paths', label: 'Win/Loss Path' },
  { key: 'decision', label: 'Decision' },
  { key: 'watch', label: 'Watch Plan' },
]

const verdictTone = (v?: string | null): 'green' | 'amber' | 'red' | 'neutral' => {
  const u = (v || '').toUpperCase()
  if (u === 'TRADE' || u === 'QUALIFIED') return 'green'
  if (u === 'WATCH') return 'amber'
  if (u === 'NO_TRADE' || u === 'REJECTED' || u === 'AVOID') return 'red'
  return 'neutral'
}
const toneColor = (t: 'green' | 'amber' | 'red' | 'neutral') =>
  t === 'green' ? T.g : t === 'amber' ? T.a : t === 'red' ? T.r : T.t2

// ── small primitives ──
function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ padding: '34px 18px', textAlign: 'center', color: T.t3, fontSize: 12.5, lineHeight: 1.65 }}>
      {children}
    </div>
  )
}
function Stat({ label, value, color = T.t, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: '10px 12px' }}>
      <div style={{ fontSize: 9.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 18, fontWeight: 800, color, marginTop: 3 }}>{value}</div>
      {sub && <div style={{ color: T.t3, fontSize: 10, marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

// build the CandleChart overlay from setup geometry — same code path for all 9 patterns
function buildOverlay(g: A.SetupGeometry) {
  const lines: ChartLine[] = []
  const levels: ChartLevel[] = []
  const markers: ChartMarker[] = []
  let shade: { upper: ChartLine; lower: ChartLine; color?: string } | null = null
  let highlightDate: string | null = null
  if (!g) return { lines, levels, markers, shade, highlightDate }

  const bo = g.breakout?.date ?? null

  if (g.upper?.a && g.upper?.b) {
    lines.push({ from: g.upper.a, to: g.upper.b, extendToDate: bo, color: T.a, width: 2 })
  }
  if (g.lower?.a && g.lower?.b) {
    lines.push({ from: g.lower.a, to: g.lower.b, extendToDate: bo, color: T.a, width: 2 })
  }
  if (g.upper?.a && g.upper?.b && g.lower?.a && g.lower?.b) {
    shade = {
      upper: { from: g.upper.a, to: g.upper.b, extendToDate: bo },
      lower: { from: g.lower.a, to: g.lower.b, extendToDate: bo },
      color: 'rgba(0,201,138,0.08)',
    }
  }

  // flat level (horizontal trendline / cup rim / rectangle)
  const ll = g.level_line
  const llFrom = ll?.from ?? ll?.a
  if (llFrom) levels.push({ price: llFrom.price, label: `R ${rupee(llFrom.price)}`, color: T.a })
  if (g.apex) levels.push({ price: g.apex.price, label: 'apex', color: T.t3 })

  for (const t of g.touches || []) markers.push({ date: t.date, price: t.price, color: T.a })
  if (g.breakout) {
    markers.push({ date: g.breakout.date, price: g.breakout.price, color: T.g, ring: true, label: 'breakout' })
    highlightDate = g.breakout.date
  }
  return { lines, levels, markers, shade, highlightDate }
}

// radial score gauge
function Radial({ score }: { score?: number | null }) {
  const s = Math.max(0, Math.min(100, score ?? 0))
  const R = 46, C = 2 * Math.PI * R
  const col = s >= 70 ? T.g : s >= 45 ? T.a : T.r
  return (
    <div style={{ position: 'relative', width: 120, height: 120 }}>
      <svg viewBox="0 0 120 120" width="120" height="120">
        <circle cx="60" cy="60" r={R} fill="none" stroke={T.b} strokeWidth="10" />
        <circle
          cx="60" cy="60" r={R} fill="none" stroke={col} strokeWidth="10" strokeLinecap="round"
          strokeDasharray={`${(s / 100) * C} ${C}`} transform="rotate(-90 60 60)"
        />
      </svg>
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ fontFamily: T.mono, fontSize: 28, fontWeight: 900, color: col }}>{score == null ? '—' : Math.round(s)}</div>
        <div style={{ fontSize: 9.5, color: T.t3, letterSpacing: '.06em', fontWeight: 800 }}>QUALITY</div>
      </div>
    </div>
  )
}
function SubBar({ label, value }: { label: string; value: number }) {
  const v = Math.max(0, Math.min(100, value))
  const col = v >= 70 ? T.g : v >= 45 ? T.a : T.r
  return (
    <div style={{ marginBottom: 9 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11.5, marginBottom: 4 }}>
        <span style={{ color: T.t2 }}>{titleCase(label)}</span>
        <span style={{ fontFamily: T.mono, color: T.t3 }}>{Math.round(v)}</span>
      </div>
      <div style={{ height: 6, background: T.s2, borderRadius: 999, overflow: 'hidden' }}>
        <div style={{ width: `${v}%`, height: '100%', background: col, borderRadius: 999 }} />
      </div>
    </div>
  )
}

// tiny trajectory line chart for Win/Loss paths (t1..t10 cumulative %)
function PathChart({ series, color, label }: { series?: number[]; color: string; label: string }) {
  if (!series || series.length === 0) {
    return <Empty>No {label.toLowerCase()} trajectory yet — insufficient resolved precedents.</Empty>
  }
  const W = 320, H = 120, pad = 16
  const lo = Math.min(0, ...series), hi = Math.max(0, ...series)
  const span = hi - lo || 1
  const x = (i: number) => pad + (i / (series.length - 1)) * (W - 2 * pad)
  const y = (v: number) => pad + (1 - (v - lo) / span) * (H - 2 * pad)
  const d = series.map((v, i) => `${i === 0 ? 'M' : 'L'} ${x(i)} ${y(v)}`).join(' ')
  const zeroY = y(0)
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 11, color: T.t2, fontWeight: 700 }}>{label}</span>
        <span style={{ fontFamily: T.mono, fontSize: 12, color, fontWeight: 800 }}>{pctS(series[series.length - 1])}</span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }} preserveAspectRatio="none">
        <line x1={pad} y1={zeroY} x2={W - pad} y2={zeroY} stroke={T.b2} strokeWidth={1} strokeDasharray="4 4" />
        <path d={d} fill="none" stroke={color} strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round" />
        {series.map((v, i) => <circle key={i} cx={x(i)} cy={y(v)} r={2.2} fill={color} />)}
      </svg>
    </div>
  )
}

export function DeepDivePanel({ row, date }: { row: A.ScanRow; date: string }) {
  const [setup, setSetup] = useState<A.SetupResp | null>(null)
  const [bars, setBars] = useState<A.Bar[]>([])
  const [busy, setBusy] = useState(true)
  const [tab, setTab] = useState<TabKey>('chart')

  useEffect(() => {
    let alive = true
    setBusy(true); setTab('chart')
    Promise.all([
      A.fetchSetup(row.stock, row.pattern, date),
      A.fetchBars(row.stock, date, 90),
    ]).then(([s, b]) => {
      if (!alive) return
      setSetup(s)
      setBars(Array.isArray(b.bars) ? b.bars : [])
    }).finally(() => alive && setBusy(false))
    return () => { alive = false }
  }, [row.stock, row.pattern, date])

  const stage = setup?.stage || row.stage
  const direction = setup?.direction || row.direction
  const tier = row.tier
  const meta = A.tierMeta(tier)
  const qScore = setup?.quality?.score ?? row.quality_score ?? null
  const verdict = setup?.decision?.verdict || (tier === 'qualified' ? 'QUALIFIED' : 'WATCH')
  const vTone = verdictTone(verdict)

  const overlay = useMemo(() => buildOverlay(setup?.geometry ?? null), [setup])
  const ev = setup?.evidence
  const evHz = ev?.horizons

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      {/* header */}
      <div style={{ padding: '16px 18px 12px', borderBottom: `1px solid ${T.b}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 9, flexWrap: 'wrap' }}>
          <span style={{ fontSize: 20, fontWeight: 900, letterSpacing: '.01em' }}>{row.stock}</span>
          <span style={{ color: T.t3, fontSize: 13 }}>·</span>
          <span style={{ fontSize: 13.5, color: T.t2, fontWeight: 700 }}>{A.patternShort(row.pattern)}</span>
          <span
            style={{
              fontSize: 10, fontWeight: 800, letterSpacing: '.05em', padding: '2px 8px', borderRadius: 999,
              color: toneColor(vTone), background: `${toneColor(vTone)}1a`, border: `1px solid ${toneColor(vTone)}55`,
            }}
          >
            {String(verdict).toUpperCase()}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginTop: 7, flexWrap: 'wrap', fontSize: 12, color: T.t3 }}>
          <span>{meta.glyph} {meta.label}</span>
          <span>· {String(stage).toLowerCase()}</span>
          <span>· {direction === 'short' ? 'bearish ▼' : 'bullish ▲'}</span>
          <span>· quality <b style={{ color: T.t2, fontFamily: T.mono }}>{qScore == null ? '—' : Math.round(qScore)}</b></span>
        </div>
      </div>

      {/* tabs */}
      <div style={{ display: 'flex', gap: 2, padding: '8px 12px', borderBottom: `1px solid ${T.b}`, overflowX: 'auto', flexShrink: 0 }}>
        {TABS.map((t) => {
          const active = tab === t.key
          return (
            <button
              key={t.key} onClick={() => setTab(t.key)}
              style={{
                whiteSpace: 'nowrap', cursor: 'pointer', border: 'none', background: active ? 'rgba(0,201,138,0.12)' : 'transparent',
                color: active ? T.g : T.t3, padding: '7px 11px', borderRadius: 9, fontSize: 12, fontWeight: active ? 800 : 600,
              }}
            >
              {t.label}
            </button>
          )
        })}
      </div>

      {/* tab body */}
      <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: 16 }}>
        {busy ? (
          <Empty>Reading the tape for {row.stock}…</Empty>
        ) : (
          <>
            {/* ── PATTERN CHART ── */}
            {tab === 'chart' && (
              <div>
                <CandleChart
                  bars={bars}
                  height={300}
                  lines={overlay.lines}
                  levels={overlay.levels}
                  markers={overlay.markers}
                  shade={overlay.shade}
                  highlightDate={overlay.highlightDate}
                  emptyLabel="Point-in-time candles are being wired up (GET /bars). The pattern geometry renders here once bars land."
                />
                <div style={{ fontSize: 10.5, color: T.t3, marginTop: 8, lineHeight: 1.55 }}>
                  {bars.length > 0
                    ? `Real point-in-time candles as of ${date}. Overlay: detector trendline(s), ${(setup?.geometry?.touches?.length ?? 0)} touch anchor(s)${overlay.highlightDate ? ', breakout candle highlighted' : ''}.`
                    : 'Overlay pending the bars endpoint — geometry from /setup will draw over the candles here.'}
                </div>
              </div>
            )}

            {/* ── QUALITY ── */}
            {tab === 'quality' && (
              setup?.quality ? (
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 18, marginBottom: 16 }}>
                    <Radial score={setup.quality.score} />
                    <div style={{ fontSize: 12, color: T.t3, lineHeight: 1.6 }}>
                      Pattern-quality is a weighted blend of the sub-scores below. Higher = a cleaner, better-formed
                      structure — it is a <b style={{ color: T.t2 }}>geometry</b> score, not a profit promise.
                    </div>
                  </div>
                  {setup.quality.subscores && Object.keys(setup.quality.subscores).length > 0 ? (
                    Object.entries(setup.quality.subscores).map(([k, v]) => <SubBar key={k} label={k} value={v} />)
                  ) : (
                    <Empty>Sub-scores not provided for this setup.</Empty>
                  )}
                </div>
              ) : <Empty>Quality breakdown coming from GET /setup for this pattern.</Empty>
            )}

            {/* ── HISTORICAL EVIDENCE ── */}
            {tab === 'evidence' && (
              ev && (ev.n ?? 0) > 0 ? (
                <div>
                  <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800, marginBottom: 8 }}>
                    Resolved precedents · n = {num(ev.n)}
                    {(ev.n ?? 0) < 20 && <span style={{ color: T.a }}> · small sample</span>}
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(92px, 1fr))', gap: 8, marginBottom: 14 }}>
                    <Stat label="Win rate" value={pct(ev.win_rate)} />
                    <Stat label="ETV" value={pctS(ev.etv)} color={(ev.etv ?? 0) >= 0 ? T.g : T.r} />
                    <Stat label="Payoff" value={num(ev.payoff)} />
                    <Stat label="CI-low" value={pctS(ev.ci_low)} color={(ev.ci_low ?? -1) > 0 ? T.g : T.t2} />
                  </div>
                  {evHz && Object.keys(evHz).length > 0 && (
                    <div style={{ overflowX: 'auto' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12.5 }}>
                        <thead>
                          <tr style={{ color: T.t3 }}>
                            {['Horizon', 'Win', 'ETV', 'MFE', 'MAE'].map((c, i) => (
                              <th key={c} style={{ textAlign: i === 0 ? 'left' : 'right', padding: '5px 6px', fontWeight: 700, fontSize: 10.5 }}>{c}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody style={{ fontFamily: T.mono }}>
                          {['1', '3', '5', '10'].filter((h) => evHz[h]).map((h) => {
                            const r = evHz[h]
                            return (
                              <tr key={h} style={{ borderTop: `1px solid ${T.b}` }}>
                                <td style={{ padding: '6px', color: T.t2 }}>T+{h}</td>
                                <td style={{ padding: '6px', textAlign: 'right', color: T.t2 }}>{pct(r.win)}</td>
                                <td style={{ padding: '6px', textAlign: 'right', color: (r.etv ?? 0) >= 0 ? T.g : T.r }}>{pctS(r.etv)}</td>
                                <td style={{ padding: '6px', textAlign: 'right', color: T.g }}>{pctS(r.mfe)}</td>
                                <td style={{ padding: '6px', textAlign: 'right', color: T.r }}>{pctS(r.mae)}</td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              ) : (
                <Empty>
                  <b style={{ color: T.a }}>Insufficient precedents.</b><br />
                  This setup has too few resolved historical occurrences to publish a win-rate / ETV. That is the
                  honest norm for most patterns — the agent holds it at <b style={{ color: T.a }}>WATCH</b> rather
                  than invent an edge.
                </Empty>
              )
            )}

            {/* ── WIN / LOSS PATH ── */}
            {tab === 'paths' && (
              setup?.paths && ((setup.paths.winners?.length ?? 0) > 0 || (setup.paths.losers?.length ?? 0) > 0) ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  <div style={{ fontSize: 12, color: T.t3, lineHeight: 1.55 }}>
                    The typical trajectory of resolved precedents from entry, split by outcome.
                  </div>
                  <PathChart series={setup.paths.winners} color={T.g} label={`Typical winner (n=${num(setup.paths.n_win)})`} />
                  <PathChart series={setup.paths.losers} color={T.r} label={`Typical loser (n=${num(setup.paths.n_loss)})`} />
                </div>
              ) : <Empty>No resolved winning/losing trajectories yet — the path chart needs precedents from /setup.</Empty>
            )}

            {/* ── DECISION ── */}
            {tab === 'decision' && (
              setup?.decision ? (
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12, flexWrap: 'wrap' }}>
                    <span style={{
                      fontSize: 14, fontWeight: 900, padding: '8px 16px', borderRadius: 999,
                      color: toneColor(verdictTone(setup.decision.verdict)), background: `${toneColor(verdictTone(setup.decision.verdict))}1a`,
                      border: `1px solid ${toneColor(verdictTone(setup.decision.verdict))}55`,
                    }}>{String(setup.decision.verdict || 'WATCH').toUpperCase()}</span>
                    {setup.decision.basis && <span style={{ fontSize: 11.5, color: T.t3 }}>basis · {setup.decision.basis}</span>}
                  </div>
                  {setup.decision.reason && <div style={{ fontSize: 13, color: T.t2, lineHeight: 1.6, marginBottom: 14 }}>{setup.decision.reason}</div>}
                  {setup.decision.gates && setup.decision.gates.length > 0 && (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 7 }}>
                      <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 800 }}>Gates</div>
                      {setup.decision.gates.map((g, i) => (
                        <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 12 }}>
                          <span style={{ width: 14, textAlign: 'center', color: g.passed == null ? T.t3 : g.passed ? T.g : T.r, fontWeight: 900 }}>
                            {g.passed == null ? '–' : g.passed ? '✓' : '✕'}
                          </span>
                          <span style={{ color: T.t2 }}><b style={{ color: T.t }}>{titleCase(g.name)}</b>{g.detail ? ` — ${g.detail}` : ''}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : <Empty>The gate-by-gate decision rationale renders here from GET /setup.decision.</Empty>
            )}

            {/* ── WATCH PLAN ── */}
            {tab === 'watch' && (
              setup?.watch_plan && (setup.watch_plan.confirmation != null || setup.watch_plan.warning != null || setup.watch_plan.invalidation != null) ? (
                <div>
                  <CandleChart
                    bars={bars}
                    height={260}
                    levels={[
                      ...(setup.watch_plan.confirmation != null ? [{ price: setup.watch_plan.confirmation, label: `confirm ${rupee(setup.watch_plan.confirmation)}`, color: T.g } as ChartLevel] : []),
                      ...(setup.watch_plan.warning != null ? [{ price: setup.watch_plan.warning, label: `warn ${rupee(setup.watch_plan.warning)}`, color: T.a } as ChartLevel] : []),
                      ...(setup.watch_plan.invalidation != null ? [{ price: setup.watch_plan.invalidation, label: `invalid ${rupee(setup.watch_plan.invalidation)}`, color: T.r } as ChartLevel] : []),
                    ]}
                    emptyLabel="Watch-plan levels drawn on candles once /bars lands."
                  />
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginTop: 12 }}>
                    <Stat label="Confirm above" value={rupee(setup.watch_plan.confirmation)} color={T.g} />
                    <Stat label="Warning" value={rupee(setup.watch_plan.warning)} color={T.a} />
                    <Stat label="Invalidation" value={rupee(setup.watch_plan.invalidation)} color={T.r} />
                  </div>
                </div>
              ) : <Empty>The confirmation / warning / invalidation plan renders here from GET /setup.watch_plan.</Empty>
            )}
          </>
        )}
      </div>

      {/* actions */}
      <div style={{ display: 'flex', gap: 8, padding: 12, borderTop: `1px solid ${T.b}`, flexShrink: 0 }}>
        <button
          disabled
          title="Per-user AutoTrade is Launch-Pending"
          style={{
            flex: 1, border: `1px solid ${T.b2}`, background: 'transparent', color: T.t3, cursor: 'not-allowed',
            padding: '10px 12px', borderRadius: 10, fontSize: 12.5, fontWeight: 700,
          }}
        >
          Set AutoTrade (When Eligible)
        </button>
        <button
          style={{
            flex: 1, border: 'none', background: T.g, color: '#04120c', cursor: 'pointer',
            padding: '10px 12px', borderRadius: 10, fontSize: 12.5, fontWeight: 800,
          }}
        >
          Add to Watchlist
        </button>
      </div>
    </div>
  )
}
