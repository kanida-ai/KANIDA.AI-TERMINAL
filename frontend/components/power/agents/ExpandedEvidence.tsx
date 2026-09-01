'use client'

/**
 * ExpandedEvidence — the inline evidence that unfolds IN PLACE beneath a feed row
 * when it is opened (spec §3). Sections top→bottom:
 *   1 header · 2 CHART HERO · 3 WHY I IDENTIFIED THIS · 4 HISTORICALLY ·
 *   5 WHAT USUALLY HAPPENS NEXT · 6 MY DECISION · 7 TOMORROW I'M WATCHING · 8 ACTIONS
 *
 * Everything is real backend output from GET /api/agents/chart/setup (bars embedded,
 * precomputed → instant). The pattern is DRAWN from setup.geometry so the chart's
 * shape matches the row's pattern label. Where the backend has no field
 * (market cap, market-alignment) it is honestly omitted — never fabricated. With
 * sparse precedents most setups are honestly WATCH, and the HISTORICALLY /
 * WHAT-HAPPENS-NEXT sections show honest "insufficient precedents (n=X)" states.
 */
import { useEffect, useMemo, useState } from 'react'
import * as A from '@/lib/agents-api'
import { CandleChart, type ChartLine, type ChartLevel, type ChartMarker } from './CandleChart'
import {
  AT, STATUS_META, statusFromVerdict, statusFromTier, type FeedStatus,
  pctS, pct, rupee, num, titleCase,
} from './ui'

// build the CandleChart overlay from setup geometry — one code path for all 9 patterns
function buildOverlay(g: A.SetupGeometry) {
  const lines: ChartLine[] = []
  const levels: ChartLevel[] = []
  const markers: ChartMarker[] = []
  let shade: { upper: ChartLine; lower: ChartLine; color?: string } | null = null
  let highlightDate: string | null = null
  if (!g) return { lines, levels, markers, shade, highlightDate }

  const bo = g.breakout?.date ?? null

  if (g.upper?.a && g.upper?.b) lines.push({ from: g.upper.a, to: g.upper.b, extendToDate: bo, color: AT.mint, width: 2 })
  if (g.lower?.a && g.lower?.b) lines.push({ from: g.lower.a, to: g.lower.b, extendToDate: bo, color: AT.mint, width: 2 })
  if (g.upper?.a && g.upper?.b && g.lower?.a && g.lower?.b) {
    shade = {
      upper: { from: g.upper.a, to: g.upper.b, extendToDate: bo },
      lower: { from: g.lower.a, to: g.lower.b, extendToDate: bo },
      color: 'rgba(63,227,164,0.07)',
    }
  }

  // flat level (horizontal trendline / cup rim / rectangle) — only when there is no sloped body
  const ll = g.level_line
  const llFrom = ll?.from ?? ll?.a
  if (llFrom && !(g.upper?.a && g.lower?.a)) levels.push({ price: llFrom.price, label: `${rupee(llFrom.price)}`, color: AT.mint })

  // de-dup touch anchors (the detector often repeats an anchor); ring them
  const seen = new Set<string>()
  for (const t of g.touches || []) {
    const k = `${t.date}|${t.price}`
    if (seen.has(k)) continue
    seen.add(k)
    markers.push({ date: t.date, price: t.price, color: AT.mintHi, ring: true })
  }
  if (g.breakout) {
    markers.push({ date: g.breakout.date, price: g.breakout.price, color: AT.mint, label: 'breakout' })
    highlightDate = g.breakout.date
  }
  return { lines, levels, markers, shade, highlightDate }
}

// tiny trajectory line chart for winning/losing paths (T0→T+10 cumulative %)
function PathChart({ series, color, label, n }: { series?: number[] | null; color: string; label: string; n?: number | null }) {
  if (!series || series.length === 0) {
    return (
      <div style={{ background: AT.card2, border: `1px solid ${AT.line}`, borderRadius: 12, padding: '14px 12px', color: AT.faint, fontSize: 12, lineHeight: 1.6 }}>
        No {label.toLowerCase()} trajectory yet — no resolved precedents of this outcome.
      </div>
    )
  }
  const W = 320, H = 96, pad = 14
  const lo = Math.min(0, ...series), hi = Math.max(0, ...series)
  const span = hi - lo || 1
  const x = (i: number) => pad + (i / (series.length - 1)) * (W - 2 * pad)
  const y = (v: number) => pad + (1 - (v - lo) / span) * (H - 2 * pad)
  const d = series.map((v, i) => `${i === 0 ? 'M' : 'L'} ${x(i)} ${y(v)}`).join(' ')
  const zeroY = y(0)
  return (
    <div style={{ background: AT.card2, border: `1px solid ${AT.line}`, borderRadius: 12, padding: 10, flex: 1, minWidth: 220 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 11.5, color: AT.ink2, fontWeight: 600 }}>{label}{n != null ? ` · n=${n}` : ''}</span>
        <span style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 12.5, color, fontWeight: 700 }}>{pctS(series[series.length - 1])}</span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }} preserveAspectRatio="none">
        <line x1={pad} y1={zeroY} x2={W - pad} y2={zeroY} stroke={AT.line2} strokeWidth={1} strokeDasharray="4 4" />
        <path d={d} fill="none" stroke={color} strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round" />
        {series.map((v, i) => <circle key={i} cx={x(i)} cy={y(v)} r={2} fill={color} />)}
      </svg>
    </div>
  )
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 10, color: AT.muted, textTransform: 'uppercase', letterSpacing: '0.09em', fontWeight: 700, marginBottom: 10 }}>
      {children}
    </div>
  )
}
function StatCard({ label, value, color = AT.ink, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ background: AT.card2, border: `1px solid ${AT.line}`, borderRadius: 12, padding: '11px 13px' }}>
      <div style={{ fontSize: 9.5, color: AT.muted, textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: 700 }}>{label}</div>
      <div style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 19, fontWeight: 700, color, marginTop: 3 }}>{value}</div>
      {sub && <div style={{ color: AT.faint, fontSize: 10, marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

export function ExpandedEvidence({ row, date }: { row: A.ScanRow; date: string }) {
  const [setup, setSetup] = useState<A.SetupResp | null>(null)
  const [bars, setBars] = useState<A.Bar[]>([])
  const [busy, setBusy] = useState(true)
  const [big, setBig] = useState(false)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    let alive = true
    setBusy(true); setBig(false)
    A.fetchSetup(row.stock, row.pattern, date).then(async (s) => {
      if (!alive) return
      setSetup(s)
      if (Array.isArray(s.bars) && s.bars.length) {
        setBars(s.bars)
      } else {
        const b = await A.fetchBars(row.stock, date, 90)
        if (alive) setBars(Array.isArray(b.bars) ? b.bars : [])
      }
    }).finally(() => { if (alive) setBusy(false) })
    return () => { alive = false }
  }, [row.stock, row.pattern, date])

  const overlay = useMemo(() => buildOverlay(setup?.geometry ?? null), [setup])

  const patternName = A.patternShort(row.pattern)
  const stage = (setup?.stage || row.stage || '').toString()
  const direction = setup?.direction || row.direction
  const level = setup?.level ?? row.level
  const distPct = setup?.context?.distance_to_level_pct ?? row.distance_pct
  const volX = setup?.context?.volume_x ?? row.volume_x

  // verdict = the per-setup source of truth (decision.decision), else fall back to tier
  const verdict = A.setupVerdict(setup?.decision)
  const status: FeedStatus = verdict ? statusFromVerdict(verdict) : statusFromTier(row.tier)
  const meta = STATUS_META[status]

  const ev = setup?.evidence
  const nCases = ev?.summary?.n ?? setup?.decision?.strategy?.n ?? setup?.paths?.n_total ?? null
  const h5 = ev?.horizons?.['5']
  const thin = (nCases ?? 0) < 20

  const subHead = (() => {
    const bits: string[] = []
    if (stage) bits.push(stage.charAt(0) + stage.slice(1).toLowerCase())
    if (distPct != null && level != null) bits.push(`${pctS(distPct)} past ${rupee(level)}`)
    if (volX != null) bits.push(`${volX.toFixed(1)}× avg volume`)
    return bits.join(' · ')
  })()

  // WHY I IDENTIFIED THIS — real bullet reasons from the detector geometry/quality
  const why: string[] = []
  if (row.touches) why.push(`${row.touches} confirmed ${row.touches === 1 ? 'touch' : 'touches'} anchoring the ${A.patternShort(row.pattern).toLowerCase()}`)
  if (level != null) why.push(`Structure level at ${rupee(level)}${overlay.highlightDate ? ' broken on ' + overlay.highlightDate : ''}`)
  if (distPct != null) why.push(`Price is ${pctS(distPct)} ${distPct >= 0 ? 'past' : 'from'} the level`)
  if (volX != null) why.push(`Breakout on ${volX.toFixed(1)}× average volume`)
  const contraction = setup?.quality?.subscores?.contraction
  if (contraction != null) why.push(`Range contraction score ${Math.round((contraction as number) * 100)} / 100`)
  if (setup?.quality?.score != null) why.push(`Pattern-quality ${Math.round(setup.quality.score)} / 100 (geometry, not a profit promise)`)

  const wp = setup?.watch_plan

  return (
    <div style={{ padding: '4px 2px 8px' }}>
      {/* 1 · header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12, marginBottom: 14 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 9, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 18, fontWeight: 700, letterSpacing: '0.01em', color: AT.ink }}>{row.stock}</span>
            <span style={{ color: AT.faint }}>·</span>
            <span style={{ fontSize: 13.5, color: AT.ink2, fontWeight: 600 }}>{patternName}</span>
            <span style={{
              fontSize: 10, fontWeight: 700, letterSpacing: '0.05em', padding: '2px 9px', borderRadius: 999,
              color: meta.color, background: meta.dim, border: `1px solid ${meta.color}55`,
            }}>{meta.glyph} {meta.label}</span>
          </div>
          {subHead && <div style={{ fontSize: 12.5, color: AT.muted, marginTop: 5 }}>{subHead}</div>}
        </div>
        <div style={{ display: 'flex', gap: 6, flexShrink: 0 }}>
          <IconBtn title={saved ? 'On your watchlist' : 'Add to watchlist'} active={saved} onClick={() => setSaved((s) => !s)}>★</IconBtn>
        </div>
      </div>

      {/* 2 · CHART HERO */}
      {busy ? (
        <div style={{ height: 260, display: 'flex', alignItems: 'center', justifyContent: 'center', color: AT.faint, fontSize: 12.5, background: AT.card, border: `1px solid ${AT.line}`, borderRadius: 14 }}>
          Reading the tape for {row.stock}…
        </div>
      ) : (
        <div style={{ position: 'relative' }}>
          <CandleChart
            bars={bars}
            height={big ? 520 : 320}
            lines={overlay.lines}
            levels={overlay.levels}
            markers={overlay.markers}
            shade={overlay.shade}
            highlightDate={overlay.highlightDate}
            emptyLabel="Point-in-time candles unavailable for this setup."
          />
          {bars.length > 0 && (
            <button
              onClick={() => setBig((b) => !b)}
              style={{
                position: 'absolute', top: 16, right: 16, cursor: 'pointer',
                background: AT.panel, border: `1px solid ${AT.line2}`, color: AT.ink2,
                borderRadius: 8, padding: '4px 10px', fontSize: 11, fontWeight: 600,
              }}
            >
              {big ? 'Shrink' : 'Expand chart'}
            </button>
          )}
          <div style={{ fontSize: 10.5, color: AT.faint, marginTop: 7, lineHeight: 1.5 }}>
            {bars.length > 0
              ? `Real point-in-time candles as of ${date}. Overlay: ${overlay.lines.length ? 'detector trendlines, ' : overlay.levels.length ? 'structure level, ' : ''}${(setup?.geometry?.touches?.length ?? 0)} touch anchor(s)${overlay.highlightDate ? ', breakout highlighted' : ''}${volX != null ? ` · Vol ${volX.toFixed(1)}× avg` : ''}.`
              : 'Candles unavailable for this setup.'}
          </div>
        </div>
      )}

      {/* 3 · WHY I IDENTIFIED THIS */}
      {why.length > 0 && (
        <div style={{ marginTop: 18 }}>
          <SectionLabel>Why I identified this</SectionLabel>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 7 }}>
            {why.map((w, i) => (
              <div key={i} style={{ display: 'flex', gap: 9, alignItems: 'baseline', fontSize: 13, color: AT.ink2, lineHeight: 1.5 }}>
                <span style={{ color: AT.mint, fontSize: 13 }}>›</span><span>{w}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 4 · HISTORICALLY */}
      <div style={{ marginTop: 20 }}>
        <SectionLabel>Historically</SectionLabel>
        {thin ? (
          <div style={{ background: AT.amberDim, border: `1px solid ${AT.amber}44`, borderRadius: 12, padding: '13px 14px', fontSize: 13, color: AT.ink2, lineHeight: 1.6 }}>
            <b style={{ color: AT.amber }}>Insufficient precedents (n={nCases ?? 0}).</b> Too few resolved historical
            occurrences to publish a stable win-rate. That is the honest norm for most setups — the agent holds this
            at <b style={{ color: AT.amber }}>WATCH</b> rather than invent an edge.
            {h5 && (
              <div style={{ marginTop: 8, fontSize: 12, color: AT.muted }}>
                Indicative only (small sample): T+5 win {pct(h5.win)} · avg {pctS(h5.mean)}.
              </div>
            )}
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: 9 }}>
            <StatCard label="Cases" value={num(nCases)} />
            <StatCard label="Win rate T+5" value={pct(h5?.win)} color={(h5?.win ?? 0) >= 50 ? AT.mint : AT.amber} />
            <StatCard label="Avg return T+5" value={pctS(h5?.mean)} color={(h5?.mean ?? 0) >= 0 ? AT.mint : AT.red} />
            {setup?.decision?.strategy?.etv != null && (
              <StatCard label="Strategy ETV" value={pctS(setup.decision.strategy.etv)} color={(setup.decision.strategy.etv ?? 0) >= 0 ? AT.mint : AT.red} sub="replayed under the traded stop/target/trail" />
            )}
          </div>
        )}
      </div>

      {/* 5 · WHAT USUALLY HAPPENS NEXT */}
      <div style={{ marginTop: 20 }}>
        <SectionLabel>What usually happens next</SectionLabel>
        {setup?.paths && ((setup.paths.winners?.length ?? 0) > 0 || (setup.paths.losers?.length ?? 0) > 0) ? (
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <PathChart series={setup.paths.winners} color={AT.mint} label="Winning path" n={setup.paths.n_win} />
            <PathChart series={setup.paths.losers} color={AT.red} label="Losing path" n={setup.paths.n_loss} />
          </div>
        ) : (
          <div style={{ background: AT.card2, border: `1px solid ${AT.line}`, borderRadius: 12, padding: '13px 14px', fontSize: 12.5, color: AT.faint, lineHeight: 1.6 }}>
            No resolved winning/losing trajectories yet — needs precedents.
          </div>
        )}
      </div>

      {/* 6 · MY DECISION */}
      <div style={{ marginTop: 20 }}>
        <SectionLabel>My decision</SectionLabel>
        <div style={{ background: meta.dim, border: `1px solid ${meta.color}44`, borderRadius: 12, padding: '13px 14px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 13, fontWeight: 700, letterSpacing: '0.04em', padding: '5px 13px', borderRadius: 999, color: meta.color, background: `${meta.color}1e`, border: `1px solid ${meta.color}66` }}>
              {meta.glyph} {String(verdict || meta.label).toUpperCase().replace('_', ' ')}
            </span>
            {setup?.decision?.basis && <span style={{ fontSize: 11, color: AT.faint }}>basis · {setup.decision.basis}</span>}
          </div>
          {setup?.decision?.reason && <div style={{ fontSize: 13, color: AT.ink2, lineHeight: 1.6, marginTop: 10 }}>{setup.decision.reason}</div>}
          {setup?.decision?.gates && setup.decision.gates.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 10 }}>
              {setup.decision.gates.map((g, i) => {
                const passed = g.pass ?? g.passed
                const gname = g.gate ?? g.name ?? ''
                const gdetail = g.reason ?? g.detail
                return (
                  <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 12 }}>
                    <span style={{ width: 12, textAlign: 'center', color: passed == null ? AT.faint : passed ? AT.mint : AT.red, fontWeight: 800 }}>
                      {passed == null ? '–' : passed ? '✓' : '✕'}
                    </span>
                    <span style={{ color: AT.muted }}><b style={{ color: AT.ink2 }}>{titleCase(gname)}</b>{gdetail ? ` — ${gdetail}` : ''}</span>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>

      {/* 7 · TOMORROW I'M WATCHING */}
      {wp && (wp.confirmation != null || wp.warning != null || wp.invalidation != null) && (
        <div style={{ marginTop: 20 }}>
          <SectionLabel>Tomorrow I&apos;m watching</SectionLabel>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 9 }}>
            {wp.confirmation != null && <WatchRow color={AT.mint} label="Holds / confirms above" value={rupee(wp.confirmation)} />}
            {wp.warning != null && <WatchRow color={AT.amber} label="Warning if it slips to" value={rupee(wp.warning)} />}
            {wp.invalidation != null && <WatchRow color={AT.red} label="Invalidates below" value={rupee(wp.invalidation)} />}
          </div>
          {wp.note && <div style={{ fontSize: 11.5, color: AT.faint, marginTop: 8, lineHeight: 1.55 }}>{wp.note}</div>}
        </div>
      )}

      {/* 8 · ACTIONS */}
      <div style={{ display: 'flex', gap: 9, marginTop: 20, flexWrap: 'wrap' }}>
        <button
          onClick={() => setSaved((s) => !s)}
          style={{
            flex: 1, minWidth: 160, border: `1px solid ${saved ? AT.mint : AT.line2}`, background: saved ? AT.mintDim : 'transparent',
            color: saved ? AT.mint : AT.ink2, cursor: 'pointer', padding: '11px 14px', borderRadius: 10, fontSize: 12.5, fontWeight: 600,
          }}
        >
          {saved ? '★ On your watchlist' : '☆ Add to watchlist'}
        </button>
        <button
          disabled
          title={status === 'QUALIFIED' ? 'Per-user AutoTrade is Launch-Pending' : 'AutoTrade is offered only on QUALIFIED setups'}
          style={{
            flex: 1, minWidth: 160, border: `1px solid ${AT.line2}`, background: 'transparent', color: AT.faint, cursor: 'not-allowed',
            padding: '11px 14px', borderRadius: 10, fontSize: 12.5, fontWeight: 600,
          }}
        >
          AutoTrade this setup {status === 'QUALIFIED' ? '(Soon)' : '(Qualified only)'}
        </button>
      </div>
      <div style={{ fontSize: 10.5, color: AT.faint, marginTop: 10, lineHeight: 1.55 }}>
        For educational purposes. Chart-pattern observations are point-in-time evidence, not trade advice.
      </div>
    </div>
  )
}

function WatchRow({ color, label, value }: { color: string; label: string; value: string }) {
  return (
    <div style={{ background: AT.card2, border: `1px solid ${AT.line}`, borderLeft: `2px solid ${color}`, borderRadius: 10, padding: '10px 12px' }}>
      <div style={{ fontSize: 10.5, color: AT.muted }}>{label}</div>
      <div style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 15, fontWeight: 700, color, marginTop: 2 }}>{value}</div>
    </div>
  )
}
function IconBtn({ children, title, active, onClick }: { children: React.ReactNode; title: string; active?: boolean; onClick: () => void }) {
  return (
    <button
      title={title} onClick={onClick}
      style={{
        width: 32, height: 32, borderRadius: 9, cursor: 'pointer', fontSize: 14,
        border: `1px solid ${active ? AT.mint : AT.line2}`, background: active ? AT.mintDim : 'transparent',
        color: active ? AT.mint : AT.muted, display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
    >{children}</button>
  )
}
