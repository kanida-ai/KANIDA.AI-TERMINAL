'use client'

/**
 * AgentsSurface — the /power/agents Chart-Agent experience (spec: CHART_AGENT_UX_SPEC.md).
 *
 * LAYOUT  LEFT nav (agent roster + pattern categories w/ real counts, collapsible)
 *         · TOP tabs (ALL INSIGHTS / QUALIFIED / WATCH / NO TRADE, real counts, filter the feed)
 *         · MAIN expand-in-place feed (no separate right panel).
 *
 * INTERACTION  a one-line insight row expands IN PLACE to the full evidence (§3 via
 * <ExpandedEvidence/>); ONE open at a time; collapse via click/chevron; the single scroll
 * region means opening/closing never blanks the layout or jumps to the top.
 *
 * REAL DATA ONLY  scan (findFreshestScan default date) → feed + tabs + categories + summary;
 * setup (embedded bars) → the expanded evidence. Fields the backend lacks (sector, market cap,
 * market-alignment) are honestly omitted. Most setups are honestly WATCH.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { CompassLogo } from '@/components/power/CompassLogo'
import * as A from '@/lib/agents-api'
import { ExpandedEvidence } from './ExpandedEvidence'
import {
  AT, STATUS_META, statusFromTier, type FeedStatus,
  pctS, pct, CATEGORY_LABEL, CATEGORY_ORDER, type CatKey,
} from './ui'

// honest agent roster: only Chart Pattern is live (from the manifest); the rest are
// clearly-labelled "Soon" placeholders — never faked Active, never claiming results.
const ROSTER = [
  { key: 'chart', name: 'Chart Pattern Agent', status: 'active' as const },
  { key: 'candle', name: 'Candlestick Agent', status: 'soon' as const },
  { key: 'volume', name: 'Volume / Flow Agent', status: 'soon' as const },
  { key: 'momentum', name: 'Momentum Agent', status: 'queued' as const },
]

const KEYFRAMES = `
@keyframes agUnfold { from { opacity: 0; transform: translateY(-4px) } to { opacity: 1; transform: none } }
@keyframes agPulse { 0%,100% { opacity: 1 } 50% { opacity: .4 } }
`

const rowKey = (r: A.ScanRow) => `${r.stock}|${r.pattern}`
const TABS: { key: 'all' | FeedStatus; label: string }[] = [
  { key: 'all', label: 'ALL INSIGHTS' },
  { key: 'QUALIFIED', label: 'QUALIFIED' },
  { key: 'WATCH', label: 'WATCH' },
  { key: 'NO_TRADE', label: 'NO TRADE' },
]

export function AgentsSurface() {
  const [scan, setScan] = useState<A.ScanResp | null>(null)
  const [date, setDate] = useState('')
  const [busy, setBusy] = useState(true)
  const [stale, setStale] = useState(false)
  const [pending, setPending] = useState(false)

  const [tab, setTab] = useState<'all' | FeedStatus>('all')
  const [cat, setCat] = useState<string>('all')   // 'all' | family key | pattern id
  const [showAllDetectors, setShowAllDetectors] = useState(false)
  const [openKey, setOpenKey] = useState<string | null>(null)
  const [visible, setVisible] = useState(20)
  const [collapsed, setCollapsed] = useState(false)

  const land = useCallback(async () => {
    setBusy(true); setStale(false); setPending(false)
    try {
      const fresh = await A.findFreshestScan(7)
      if (fresh) { setDate(fresh.date); setScan(fresh.scan) }
      else {
        const fb = await A.fetchScan(A.KNOWN_POPULATED_DATE, { full: true })
        setDate(A.KNOWN_POPULATED_DATE); setStale(true); setScan(fb)
        if (!fb.count) setPending(true)
      }
    } catch {
      setScan({ ok: false, date: '', count: 0, occurrences: [], error: 'scanner unreachable' })
    } finally { setBusy(false) }
  }, [])

  useEffect(() => { land() }, [land])

  const all = useMemo(() => scan?.occurrences ?? [], [scan])

  // category counts by pattern family (real, from the occurrences themselves)
  const familyCounts = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of all) { const f = A.patternFamily(r.pattern); m[f] = (m[f] || 0) + 1 }
    return m
  }, [all])
  const detectorCounts = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of all) m[r.pattern] = (m[r.pattern] || 0) + 1
    return m
  }, [all])

  // rows in the active category
  const inCat = useCallback((r: A.ScanRow) => {
    if (cat === 'all') return true
    if (CATEGORY_ORDER.includes(cat as Exclude<CatKey, 'all'>)) return A.patternFamily(r.pattern) === cat
    return r.pattern === cat
  }, [cat])

  const catPool = useMemo(() => all.filter(inCat), [all, inCat])

  // tab counts over the category pool (so counts always match the visible feed)
  const tabCounts = useMemo(() => {
    const c = { all: catPool.length, QUALIFIED: 0, WATCH: 0, NO_TRADE: 0 } as Record<'all' | FeedStatus, number>
    for (const r of catPool) c[statusFromTier(r.tier)]++
    return c
  }, [catPool])

  // final feed = category ∩ tab, ranked (qualified → strong → watch → weak, then quality desc)
  const feed = useMemo(() => {
    const rows = catPool.filter((r) => tab === 'all' || statusFromTier(r.tier) === tab)
    return [...rows].sort((a, b) => {
      const ra = A.TIER_RANK[a.tier ?? 'watch'] ?? 2
      const rb = A.TIER_RANK[b.tier ?? 'watch'] ?? 2
      if (ra !== rb) return ra - rb
      return (b.quality_score ?? 0) - (a.quality_score ?? 0)
    })
  }, [catPool, tab])

  // reset paging / open row when the filter changes
  useEffect(() => { setVisible(20); setOpenKey(null) }, [tab, cat])

  const shown = feed.slice(0, visible)

  // scan summary numbers (real)
  const scanned = scan?.scanned ?? null
  const count = scan?.count ?? all.length
  const meaningful = scan?.statistically_meaningful ?? null
  const qualified = scan?.qualified ?? tabCounts.QUALIFIED

  const storyLines: string[] = (() => {
    const raw = scan as unknown as { market_story?: { breadth?: { lines?: string[] } } } | null
    return raw?.market_story?.breadth?.lines ?? []
  })()

  const intro = useMemo(() => {
    const lines: string[] = []
    if (scanned != null) lines.push(`I scanned ${scanned.toLocaleString('en-IN')} stocks after the close and found ${count.toLocaleString('en-IN')} chart setups.`)
    if (meaningful != null) lines.push(`${meaningful} have enough resolved history to judge; ${qualified} clear every gate today.`)
    lines.push('Most are honestly WATCH — I flag QUALIFIED only when the evidence supports it.')
    return lines
  }, [scanned, count, meaningful, qualified])

  // ── render ──
  return (
    <div style={{ display: 'flex', height: '100%', minHeight: 0, background: AT.bg, color: AT.ink, fontFamily: 'var(--font-geist-sans, system-ui, sans-serif)' }}>
      <style>{KEYFRAMES}</style>

      {/* ── LEFT NAV ── */}
      <aside style={{
        width: collapsed ? 58 : 244, flexShrink: 0, borderRight: `1px solid ${AT.line}`, background: AT.panel,
        display: 'flex', flexDirection: 'column', transition: 'width 220ms cubic-bezier(0.4,0,0.2,1)', overflow: 'hidden',
      }}>
        {/* brand */}
        <div style={{ height: 56, flexShrink: 0, display: 'flex', alignItems: 'center', gap: 9, padding: '0 14px', borderBottom: `1px solid ${AT.line}` }}>
          <CompassLogo size={22} />
          {!collapsed && (
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 13, fontWeight: 700, letterSpacing: '0.01em' }}>
                <span style={{ color: AT.ink }}>KANIDA</span><span style={{ color: AT.mint }}>.AI</span>
              </div>
              <div style={{ fontSize: 9, letterSpacing: '0.16em', color: AT.muted, fontWeight: 600 }}>CHART AGENT</div>
            </div>
          )}
        </div>

        <div style={{ flex: 1, overflowY: 'auto', padding: '12px 10px' }}>
          {/* roster */}
          {!collapsed && <NavHeading>Agents</NavHeading>}
          {ROSTER.map((a) => (
            <div key={a.key} title={a.name} style={{
              display: 'flex', alignItems: 'center', gap: 9, padding: '8px 8px', borderRadius: 9, marginBottom: 2,
              background: a.status === 'active' ? AT.mintDim : 'transparent',
            }}>
              <span style={{
                width: 7, height: 7, borderRadius: '50%', flexShrink: 0,
                background: a.status === 'active' ? AT.mint : AT.faint,
                animation: a.status === 'active' && busy ? 'agPulse 1.4s infinite' : 'none',
              }} />
              {!collapsed && (
                <>
                  <span style={{ flex: 1, fontSize: 12.5, color: a.status === 'active' ? AT.ink : AT.muted, fontWeight: a.status === 'active' ? 600 : 400 }}>{a.name}</span>
                  <span style={{ fontSize: 8.5, letterSpacing: '0.08em', textTransform: 'uppercase', color: a.status === 'active' ? AT.mint : AT.faint, fontWeight: 700 }}>
                    {a.status === 'active' ? 'Active' : a.status === 'soon' ? 'Soon' : 'Queued'}
                  </span>
                </>
              )}
            </div>
          ))}

          {/* pattern categories */}
          {!collapsed && (
            <>
              <NavHeading style={{ marginTop: 16 }}>Patterns</NavHeading>
              <CatRow label="All patterns" count={all.length} active={cat === 'all'} onClick={() => setCat('all')} />
              {CATEGORY_ORDER.filter((f) => (familyCounts[f] ?? 0) > 0).map((f) => (
                <CatRow key={f} label={CATEGORY_LABEL[f]} count={familyCounts[f]} active={cat === f} onClick={() => setCat(f)} />
              ))}
              <button
                onClick={() => setShowAllDetectors((s) => !s)}
                style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', cursor: 'pointer', color: AT.mint, fontSize: 11, padding: '6px 10px', marginTop: 2 }}
              >
                {showAllDetectors ? 'Hide detectors' : `View all ${Object.keys(detectorCounts).length || 9} detectors`}
              </button>
              {showAllDetectors && Object.entries(detectorCounts).sort((a, b) => b[1] - a[1]).map(([pid, cnt]) => (
                <CatRow key={pid} label={A.patternShort(pid)} count={cnt} active={cat === pid} onClick={() => setCat(pid)} indent />
              ))}
            </>
          )}
        </div>

        {/* collapse control */}
        <button
          onClick={() => setCollapsed((c) => !c)}
          style={{ flexShrink: 0, height: 40, borderTop: `1px solid ${AT.line}`, background: 'transparent', border: 'none',
            borderTopColor: AT.line, cursor: 'pointer', color: AT.muted, fontSize: 12, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}
        >
          <span>{collapsed ? '»' : '«'}</span>{!collapsed && <span>Collapse</span>}
        </button>
      </aside>

      {/* ── RIGHT: top bar + feed ── */}
      <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
        {/* top bar */}
        <div style={{ flexShrink: 0, borderBottom: `1px solid ${AT.line}`, background: AT.panel }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 14, padding: '12px 18px' }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: 7, padding: '5px 12px', borderRadius: 999, border: `1px solid ${AT.mint}44`, background: AT.mintDim }}>
              <span style={{ width: 6, height: 6, borderRadius: '50%', background: AT.mint, animation: busy ? 'agPulse 1.4s infinite' : 'none' }} />
              <span style={{ fontSize: 11.5, color: AT.mintHi, fontWeight: 600 }}>{busy ? 'Running post-market analysis…' : 'Post-Market Analysis Complete'}</span>
            </span>
            {date && <span style={{ fontSize: 11.5, color: AT.faint, fontFamily: 'var(--font-geist-mono, monospace)' }}>as of {date} · market closed</span>}
          </div>
          {/* tabs */}
          <div style={{ display: 'flex', gap: 2, padding: '0 12px' }}>
            {TABS.map((t) => {
              const active = tab === t.key
              const cnt = tabCounts[t.key]
              const tone = t.key === 'all' ? AT.ink : STATUS_META[t.key as FeedStatus].color
              return (
                <button
                  key={t.key} onClick={() => setTab(t.key)}
                  style={{
                    cursor: 'pointer', background: 'transparent', border: 'none', padding: '10px 13px 11px',
                    borderBottom: `2px solid ${active ? tone : 'transparent'}`, color: active ? AT.ink : AT.muted,
                    fontSize: 12, fontWeight: active ? 700 : 500, letterSpacing: '0.03em', display: 'flex', alignItems: 'center', gap: 7,
                  }}
                >
                  {t.label}
                  <span style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 11, color: active ? tone : AT.faint, fontWeight: 700 }}>{cnt}</span>
                </button>
              )
            })}
          </div>
        </div>

        {/* scroll region — the ONLY scroller (so expanding a row never jumps to top) */}
        <div style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
          <div style={{ maxWidth: 920, margin: '0 auto', padding: '18px 18px 60px' }}>
            {busy ? (
              <Centered>Scanning the universe…</Centered>
            ) : scan && scan.ok === false ? (
              <Centered><b style={{ color: AT.red }}>Scanner unavailable.</b><br />{scan.error || 'The agent data source is offline.'}</Centered>
            ) : pending || (scan && scan.served === 'pending') || feed.length === 0 && all.length === 0 ? (
              <Centered><b style={{ color: AT.amber }}>Screen builds post-market.</b><br />No precomputed screen for {date || 'today'} yet — the EOD job scans after the close.</Centered>
            ) : (
              <>
                {stale && (
                  <div style={{ fontSize: 11.5, color: AT.amber, background: AT.amberDim, border: `1px solid ${AT.amber}44`, borderRadius: 10, padding: '9px 12px', marginBottom: 14 }}>
                    No precomputed screen in the last 7 days — showing the last-known populated screen ({date}).
                  </div>
                )}

                {/* scan summary strip */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 9, marginBottom: 16 }}>
                  <Summary label="Stocks scanned" value={scanned != null ? scanned.toLocaleString('en-IN') : '—'} />
                  <Summary label="Patterns found" value={count.toLocaleString('en-IN')} />
                  <Summary label="Meaningful" value={meaningful != null ? String(meaningful) : '—'} tone={AT.teal} />
                  <Summary label="Qualified" value={String(qualified)} tone={AT.mint} />
                </div>

                {/* AI intro */}
                <div style={{ background: AT.card, border: `1px solid ${AT.line}`, borderRadius: 14, padding: '14px 16px', marginBottom: 12 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <CompassLogo size={16} />
                    <span style={{ fontSize: 10.5, color: AT.muted, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700 }}>Chart Pattern Agent</span>
                  </div>
                  {intro.map((l, i) => (
                    <div key={i} style={{ fontSize: 13.5, color: i === intro.length - 1 ? AT.muted : AT.ink2, lineHeight: 1.65 }}>{l}</div>
                  ))}
                </div>

                {/* market-story lines woven in */}
                {storyLines.length > 0 && (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
                    {storyLines.map((l, i) => (
                      <span key={i} style={{ fontSize: 11.5, color: AT.muted, background: AT.card2, border: `1px solid ${AT.line}`, borderRadius: 999, padding: '5px 12px' }}>{l}</span>
                    ))}
                  </div>
                )}

                {/* the feed */}
                {shown.length === 0 ? (
                  <Centered><b style={{ color: AT.amber }}>Nothing in this view.</b><br />No setups match this tab / category on {date}.</Centered>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {shown.map((r) => (
                      <FeedItem
                        key={rowKey(r)} row={r} date={date}
                        open={openKey === rowKey(r)}
                        onToggle={() => setOpenKey((k) => (k === rowKey(r) ? null : rowKey(r)))}
                      />
                    ))}
                  </div>
                )}

                {/* progressive load */}
                {visible < feed.length && (
                  <button
                    onClick={() => setVisible((v) => v + 20)}
                    style={{ marginTop: 16, width: '100%', background: AT.card, border: `1px solid ${AT.line2}`, color: AT.ink2,
                      cursor: 'pointer', borderRadius: 12, padding: '12px', fontSize: 12.5, fontWeight: 600 }}
                  >
                    View next {Math.min(20, feed.length - visible)} of {feed.length}
                  </button>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── feed row (collapsed line + inline expansion) ──
function FeedItem({ row, date, open, onToggle }: { row: A.ScanRow; date: string; open: boolean; onToggle: () => void }) {
  const status = statusFromTier(row.tier)
  const meta = STATUS_META[status]
  const win = row.evidence_summary?.win_t5
  const etv = row.evidence_summary?.etv_t5
  // hook already reads "{Pattern} {insight}"; fall back to a derived one-liner.
  const pname = A.patternShort(row.pattern)
  const hook = row.hook || `${pname} ${String(row.stage).toLowerCase()}`
  // avoid duplicating the pattern name when the hook already leads with it
  const insight = hook.toLowerCase().startsWith(pname.toLowerCase()) ? hook.slice(pname.length).replace(/^[\s—:-]+/, '') : hook

  return (
    <div style={{
      background: open ? AT.card : AT.card2, border: `1px solid ${open ? meta.color + '66' : AT.line}`,
      borderRadius: 14, overflow: 'hidden', transition: 'border-color 160ms, background 160ms',
      boxShadow: open ? `0 0 0 1px ${meta.color}22, 0 10px 30px rgba(0,0,0,0.35)` : 'none',
    }}>
      {/* collapsed line — always clickable */}
      <button
        onClick={onToggle}
        style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', cursor: 'pointer',
          display: 'flex', alignItems: 'center', gap: 11, padding: '13px 15px' }}
      >
        <span style={{ fontSize: 10, color: AT.faint, fontFamily: 'var(--font-geist-mono, monospace)', flexShrink: 0, width: 42 }}>
          {(row.as_of_date || date || '').slice(5) || '—'}
        </span>
        <span style={{ color: meta.color, fontSize: 13, flexShrink: 0, width: 14, textAlign: 'center' }}>{meta.glyph}</span>
        <span style={{ fontSize: 13.5, fontWeight: 700, color: AT.ink, flexShrink: 0, minWidth: 68 }}>{row.stock}</span>
        <span style={{ flex: 1, minWidth: 0, fontSize: 12.5, color: AT.ink2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          <span style={{ color: AT.muted }}>{pname}</span> — {insight}
        </span>
        <span style={{ flexShrink: 0, display: 'flex', gap: 12, alignItems: 'center', fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 11.5 }}>
          {win != null && <span style={{ color: AT.muted }} title="T+5 win rate (small sample)">{pct(win)}</span>}
          {etv != null && <span style={{ color: etv >= 0 ? AT.mint : AT.red }} title="T+5 avg return">{pctS(etv)}</span>}
        </span>
        <span style={{
          flexShrink: 0, fontSize: 9.5, fontWeight: 700, letterSpacing: '0.04em', padding: '3px 9px', borderRadius: 999,
          color: meta.color, background: meta.dim, border: `1px solid ${meta.color}44`,
        }}>{meta.label}</span>
        <span style={{ flexShrink: 0, color: AT.muted, fontSize: 12, transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 200ms' }}>⌄</span>
      </button>

      {/* expanded evidence — in place */}
      {open && (
        <div style={{ padding: '2px 15px 15px', borderTop: `1px solid ${AT.line}`, animation: 'agUnfold 220ms ease' }}>
          <ExpandedEvidence row={row} date={date} />
        </div>
      )}
    </div>
  )
}

// ── small building blocks ──
function NavHeading({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return <div style={{ fontSize: 9.5, color: AT.faint, textTransform: 'uppercase', letterSpacing: '0.11em', fontWeight: 700, padding: '0 8px 6px', ...style }}>{children}</div>
}
function CatRow({ label, count, active, onClick, indent }: { label: string; count?: number; active: boolean; onClick: () => void; indent?: boolean }) {
  return (
    <button
      onClick={onClick}
      style={{
        width: '100%', display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', border: 'none',
        background: active ? AT.mintDim : 'transparent', borderRadius: 9, padding: indent ? '6px 8px 6px 18px' : '7px 8px', marginBottom: 1,
        color: active ? AT.mint : AT.muted, fontSize: 12, fontWeight: active ? 600 : 400, textAlign: 'left',
      }}
    >
      <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{label}</span>
      {count != null && <span style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 10.5, color: active ? AT.mint : AT.faint }}>{count}</span>}
    </button>
  )
}
function Summary({ label, value, tone = AT.ink }: { label: string; value: string; tone?: string }) {
  return (
    <div style={{ background: AT.card, border: `1px solid ${AT.line}`, borderRadius: 12, padding: '11px 13px' }}>
      <div style={{ fontSize: 9.5, color: AT.muted, textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: 700 }}>{label}</div>
      <div style={{ fontFamily: 'var(--font-geist-mono, monospace)', fontSize: 21, fontWeight: 700, color: tone, marginTop: 3 }}>{value}</div>
    </div>
  )
}
function Centered({ children }: { children: React.ReactNode }) {
  return <div style={{ padding: '80px 24px', textAlign: 'center', color: AT.faint, fontSize: 13, lineHeight: 1.8 }}>{children}</div>
}
