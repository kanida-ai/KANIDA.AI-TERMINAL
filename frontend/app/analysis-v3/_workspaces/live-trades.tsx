'use client'

/**
 * Live Trades workspace — ported from /analysis page (lines 957–1403).
 * Faithful port: same state, refs, effects, and visual treatment.
 * Only standalone changes: removed parent `bucketF` dependency assumption
 * (kept as local state, which it already was), pulled shared utilities from
 * ./_shared.
 */
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  getLivePositions, getLiveHistory,
  type LivePosition, type LivePositionsResponse, type LiveHistoryResponse,
} from '@/lib/backtest-api'
import {
  C, BUCKET_META, STATUS_META,
  Chip, StatBox, KV,
  pct, pctS, price, reasonColor,
} from './_shared'

// ── StatusChip ────────────────────────────────────────────────────────────────
function StatusChip({ code, small }: { code: string; small?: boolean }) {
  const m = STATUS_META[code] || STATUS_META.closed
  return (
    <span style={{
      background: `${m.color}22`, color: m.color, border: `1px solid ${m.color}44`,
      padding: small ? '1px 5px' : '2px 8px', borderRadius: 4,
      fontSize: small ? 10 : 11, fontWeight: 700, letterSpacing: '0.03em',
      whiteSpace: 'nowrap',
      animation: m.pulse ? 'liveGlow 1.6s ease-in-out infinite' : undefined,
    }}>
      {m.icon} {m.label}
    </span>
  )
}

// ── ScoreBar ─────────────────────────────────────────────────────────────────
function ScoreBar({ score }: { score: number | null }) {
  if (score == null) return <span style={{ color: C.t3 }}>—</span>
  const pctV = Math.round(score * 100)
  const col = score >= 0.85 ? C.green : score >= 0.65 ? C.amber : C.t3
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 44, height: 5, background: 'rgba(255,255,255,0.08)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pctV}%`, height: '100%', background: col, borderRadius: 3 }} />
      </div>
      <span style={{ color: col, fontSize: 10, fontWeight: 700 }}>{pctV}</span>
    </div>
  )
}

// ── ExpiryBar ────────────────────────────────────────────────────────────────
function ExpiryBar({ daysOpen, maxDays }: { daysOpen: number; maxDays: number }) {
  const pctV = Math.min(100, Math.round((daysOpen / maxDays) * 100))
  const col  = pctV < 50 ? C.green : pctV < 80 ? C.amber : C.red
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 36, height: 4, background: 'rgba(255,255,255,0.08)', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${pctV}%`, height: '100%', background: col, borderRadius: 2 }} />
      </div>
      <span style={{ color: col, fontSize: 10 }}>{maxDays - daysOpen}d</span>
    </div>
  )
}

// ── PositionsTable — defined OUTSIDE LiveTradesTab so React never unmounts it
// on countdown ticks, preserving scroll position across re-renders.
function PositionsTable({
  rows, expanded, onExpand,
}: {
  rows: LivePosition[]
  expanded: number | null
  onExpand: (id: number | null) => void
}) {
  if (rows.length === 0) return (
    <div style={{
      color: C.t3, padding: 40, textAlign: 'center',
      background: C.card, borderRadius: 10, border: `1px solid ${C.border}`,
    }}>
      No positions in this view.
    </div>
  )
  return (
    <div style={{
      background: C.card, border: `1px solid ${C.border}`, borderRadius: 10,
      overflow: 'auto',
      height: 'clamp(420px, calc(100vh - 360px), 72vh)',
      minHeight: 420,
      overscrollBehavior: 'contain',
      scrollbarGutter: 'stable both-edges',
    }}>
      <table style={{ minWidth: 1650, width: 'max-content', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead style={{ position: 'sticky', top: 0, background: C.card, zIndex: 2 }}>
          <tr style={{ borderBottom: `1px solid ${C.border}` }}>
            {['Status','Stock','Bucket','Signal Type','Dir',
              'Signal DateTime','Entry DateTime',
              'Entry ₹','Current ₹','Live P&L %','Live P&L ₹',
              'SL ₹','Target ₹','Dist to TP','Dist to SL',
              'Days Open','Expiry','Score',
            ].map((h, hi) => (
              <th key={h} style={{
                padding: '7px 10px', color: C.t3, fontWeight: 600, textAlign: 'left',
                whiteSpace: 'nowrap', fontSize: 10,
                ...(hi < 2 ? {
                  position: 'sticky', left: hi === 0 ? 0 : 130,
                  background: C.card, zIndex: 3,
                  boxShadow: hi === 1 ? '2px 0 6px rgba(0,0,0,0.25)' : undefined,
                } : {}),
              }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(p => {
            const bm   = BUCKET_META[p.bucket.toLowerCase()]
            const sm2  = STATUS_META[p.status_code]
            const isEx = expanded === p.trade_id
            const rowBg =
              p.status_code === 'near_target'   ? `${C.green}07` :
              p.status_code === 'near_stop'     ? `${C.red}07`   :
              p.status_code === 'target_hit'    ? `${C.green}04` :
              p.status_code === 'stop_hit'      ? `${C.red}04`   :
              p.status_code === 'pending_entry' ? `${C.amber}06` :
              'transparent'
            return (
              <React.Fragment key={p.trade_id}>
                <tr
                  onClick={() => onExpand(isEx ? null : p.trade_id)}
                  style={{ borderBottom: `1px solid ${C.border}1a`, cursor: 'pointer', background: isEx ? `${C.violet}0c` : rowBg }}
                >
                  <td style={{ padding: '6px 10px', position: 'sticky', left: 0, zIndex: 1, background: isEx ? `${C.violet}0c` : rowBg || C.bg }}>
                    <span style={{
                      background: `${sm2?.color || C.t3}22`, color: sm2?.color || C.t3,
                      border: `1px solid ${sm2?.color || C.t3}44`,
                      padding: '1px 6px', borderRadius: 4, fontSize: 10, fontWeight: 700, whiteSpace: 'nowrap',
                      animation: sm2?.pulse ? 'liveGlow 1.6s ease-in-out infinite' : undefined,
                    }}>
                      {sm2?.icon} {sm2?.label || p.status_label}
                    </span>
                  </td>
                  <td style={{ padding: '6px 10px', color: C.violet, fontWeight: 800, position: 'sticky', left: 130, zIndex: 1, background: isEx ? `${C.violet}0c` : rowBg || C.bg, boxShadow: '2px 0 6px rgba(0,0,0,0.25)' }}>{p.ticker}</td>
                  <td style={{ padding: '6px 10px' }}>{bm && <Chip val={`${bm.icon} ${bm.label}`} color={bm.color} small />}</td>
                  <td style={{ padding: '6px 10px', color: C.t2, maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.signal_type}</td>
                  <td style={{ padding: '6px 10px' }}><Chip val={p.direction === 'long' ? '▲ LONG' : '▼ SHORT'} color={p.direction === 'long' ? C.green : C.red} small /></td>
                  <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10, whiteSpace: 'nowrap' }}>{p.signal_datetime}</td>
                  <td style={{ padding: '6px 10px', color: C.t2, fontFamily: 'monospace', fontSize: 10, whiteSpace: 'nowrap' }}>{p.entry_date ? p.entry_datetime : <span style={{ color: C.amber }}>Pending</span>}</td>
                  <td style={{ padding: '6px 10px', color: C.t, fontFamily: 'monospace' }}>{price(p.entry_price)}</td>
                  <td style={{ padding: '6px 10px', fontFamily: 'monospace' }}>
                    {p.current_price != null ? <span style={{ color: p.current_price >= p.entry_price ? C.green : C.red, fontWeight: 600 }}>{price(p.current_price)}</span> : <span style={{ color: C.t3 }}>—</span>}
                  </td>
                  <td style={{ padding: '6px 10px', fontWeight: 800, color: p.live_pnl_pct >= 0 ? C.green : C.red }}>{pctS(p.live_pnl_pct)}</td>
                  <td style={{ padding: '6px 10px', fontFamily: 'monospace', color: p.live_pnl_rs >= 0 ? C.green : C.red }}>{p.live_pnl_rs >= 0 ? '+' : ''}{p.live_pnl_rs.toFixed(2)}</td>
                  <td style={{ padding: '6px 10px', color: C.red,   fontFamily: 'monospace' }}>{price(p.stop_price)}</td>
                  <td style={{ padding: '6px 10px', color: C.green, fontFamily: 'monospace' }}>{price(p.target_price)}</td>
                  <td style={{ padding: '6px 10px' }}><span style={{ color: p.pct_to_tp <= 3 ? C.green : p.pct_to_tp <= 6 ? C.amber : C.t3, fontWeight: p.pct_to_tp <= 3 ? 700 : 400 }}>{pctS(p.pct_to_tp)}</span></td>
                  <td style={{ padding: '6px 10px' }}><span style={{ color: p.pct_to_sl <= 2 ? C.red : p.pct_to_sl <= 4 ? C.amber : C.t3, fontWeight: p.pct_to_sl <= 2 ? 700 : 400 }}>{pctS(p.pct_to_sl)}</span></td>
                  <td style={{ padding: '6px 10px', color: C.t3 }}>{p.days_open}d</td>
                  <td style={{ padding: '6px 10px' }}><ExpiryBar daysOpen={p.days_open} maxDays={p.max_hold_days} /></td>
                  <td style={{ padding: '6px 10px' }}><ScoreBar score={p.opportunity_score} /></td>
                </tr>
                {isEx && (
                  <tr style={{ background: `${C.violet}08` }}>
                    <td colSpan={18} style={{ padding: '14px 20px' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16, fontSize: 11 }}>
                        <div>
                          <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>POSITION DETAIL</div>
                          <KV label="Signal ID"      val={p.signal_id} />
                          <KV label="Status"         val={p.status_label} color={sm2?.color} />
                          <KV label="R:R ratio"      val={`1:${p.rr.toFixed(1)}`} />
                          <KV label="Days open"      val={`${p.days_open} of ${p.max_hold_days} max`} color={p.days_open > p.max_hold_days * 0.75 ? C.red : C.t2} />
                          <KV label="Days remaining" val={`${p.days_left}d`} color={p.days_left <= 5 ? C.red : C.sky} />
                          <KV label="Bucket"         val={p.bucket} color={BUCKET_META[p.bucket.toLowerCase()]?.color} />
                          <KV label="Conviction"     val={p.opportunity_score?.toFixed(4) || '—'} color={C.sky} />
                        </div>
                        <div>
                          <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>DATETIME FLOW</div>
                          <KV label="Signal fired"    val={p.signal_datetime} />
                          <KV label="Entry executed"  val={p.entry_date ? p.entry_datetime : 'Pending next open'} color={p.entry_date ? C.t2 : C.amber} />
                          <KV label="Execution delay" val={p.delay_label} color={C.sky} />
                          <div style={{ marginTop: 10 }}>
                            <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>SIGNAL QUALITY</div>
                            <KV label="Signal type"      val={p.signal_type} />
                            <KV label="Reason code"      val={p.reason_code} color={reasonColor(p.reason_code)} />
                            <KV label="Competing patt."  val={`${p.multi_pattern_count} patterns that day`} />
                            <KV label="Tier"             val={p.tier || '—'} />
                          </div>
                        </div>
                        <div>
                          <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>LEVELS &amp; LIVE P&amp;L</div>
                          <KV label="Entry price" val={`₹${price(p.entry_price)}`} />
                          <KV label="Stop Loss"   val={`₹${price(p.stop_price)}`}   color={C.red} />
                          <KV label="Target"      val={`₹${price(p.target_price)}`} color={C.green} />
                          <KV label="Current"     val={p.current_price != null ? `₹${price(p.current_price)}` : 'Market closed'} color={p.current_price != null && p.current_price >= p.entry_price ? C.green : C.red} />
                          <KV label="Live P&amp;L %" val={pctS(p.live_pnl_pct)} color={p.live_pnl_pct >= 0 ? C.green : C.red} />
                          <KV label="Dist to TP"  val={pctS(p.pct_to_tp)} color={p.pct_to_tp <= 3 ? C.green : C.t2} />
                          <KV label="Dist to SL"  val={pctS(p.pct_to_sl)} color={p.pct_to_sl <= 2 ? C.red : C.t2} />
                          <div style={{ color: C.t3, marginTop: 8, fontSize: 10, lineHeight: 1.5, wordBreak: 'break-word' }}>{p.pattern}</div>
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── LiveTradesTab — main component ───────────────────────────────────────────
function LiveTradesTab({ ticker }: { ticker: string }) {
  const [liveData,  setLiveData]  = useState<LivePositionsResponse | null>(null)
  const [histData,  setHistData]  = useState<LiveHistoryResponse   | null>(null)
  const [subTab,    setSubTab]    = useState<'active' | 'closed' | 'history'>('active')
  const [bucketF,   setBucketF]   = useState('ALL')
  const [loading,   setLoading]   = useState(true)
  const [expanded,  setExpanded]  = useState<number | null>(null)
  const [countdown, setCountdown] = useState(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const tk = ticker !== 'ALL' ? ticker : undefined
      const [live, hist] = await Promise.all([getLivePositions(tk), getLiveHistory(tk)])
      setLiveData(live)
      setHistData(hist)
      setCountdown(live.market.refresh_interval_s)
    } catch (_) { /* silent */ }
    setLoading(false)
  }, [ticker])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    if (!liveData) return
    const iv = liveData.market.refresh_interval_s
    if (timerRef.current) clearInterval(timerRef.current)
    timerRef.current = setInterval(() => {
      setCountdown(prev => { if (prev <= 1) { load(); return iv } return prev - 1 })
    }, 1000)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [liveData?.market.refresh_interval_s, load])

  const filterByBucket = (list: LivePosition[]) =>
    bucketF === 'ALL' ? list : list.filter(p => p.bucket.toLowerCase() === bucketF.toLowerCase())

  const activePositions = useMemo(
    () => filterByBucket(liveData?.positions || []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [liveData, bucketF],
  )
  const closedPositions = useMemo(
    () => filterByBucket(liveData?.closed || []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [liveData, bucketF],
  )

  const sm     = (liveData?.summary || {}) as Record<string, number>
  const mkt    = liveData?.market
  const prices = liveData?.current_prices || {}
  const alerts = liveData?.alerts || []
  const mktDot = mkt?.status === 'open' ? C.green : mkt?.status === 'pre' ? C.amber : C.red

  const fbtn = (val: string, active: string, _set: (v: string) => void, col = C.violet) => ({
    padding: '4px 11px', borderRadius: 5, cursor: 'pointer', fontSize: 11, fontWeight: 600,
    background: active === val ? `${col}22` : 'transparent',
    color: active === val ? col : C.t3,
    border: `1px solid ${active === val ? col + '55' : C.border}`,
  })

  if (loading && !liveData) return (
    <div style={{ color: C.t3, padding: 40, textAlign: 'center' }}>Loading live positions...</div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <style>{`
        @keyframes liveGlow { 0%,100%{opacity:1} 50%{opacity:.4} }
        @keyframes mktPulse  { 0%,100%{opacity:1} 50%{opacity:.25} }
      `}</style>

      {/* ── Market status bar ── */}
      <div style={{
        background: C.card, border: `1px solid ${C.border}`, borderRadius: 10,
        padding: '11px 20px', display: 'flex', alignItems: 'center', gap: 18, flexWrap: 'wrap',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{
            width: 9, height: 9, borderRadius: '50%', background: mktDot,
            animation: mkt?.status === 'open' ? 'mktPulse 1.2s ease-in-out infinite' : undefined,
          }} />
          <span style={{ color: mktDot, fontWeight: 700, fontSize: 12 }}>{mkt?.label}</span>
          <span style={{ color: C.t3, fontSize: 11 }}>{mkt?.ist_datetime}</span>
        </div>
        {mkt?.mins_to_close != null && <span style={{ color: C.amber, fontSize: 11 }}>{mkt.mins_to_close}m until close</span>}
        {mkt?.mins_to_open  != null && <span style={{ color: C.sky,   fontSize: 11 }}>{mkt.mins_to_open}m until open</span>}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 18, alignItems: 'center' }}>
          {Object.entries(prices).map(([tk, pr]) => (
            <div key={tk} style={{ textAlign: 'center' }}>
              <div style={{ color: C.t3, fontSize: 9, letterSpacing: '0.08em' }}>{tk}</div>
              <div style={{ color: pr ? C.t : C.t3, fontSize: 12, fontWeight: 700, fontFamily: 'monospace' }}>{pr ? price(pr as number) : '—'}</div>
            </div>
          ))}
          <div style={{ color: C.t3, fontSize: 10, borderLeft: `1px solid ${C.border}`, paddingLeft: 14 }}>
            Refresh in <span style={{ color: C.sky }}>{countdown}s</span>
            <button onClick={load} style={{
              marginLeft: 8, background: 'transparent', border: `1px solid ${C.border}`,
              color: C.t3, padding: '2px 8px', borderRadius: 4, cursor: 'pointer', fontSize: 10,
            }}>↻</button>
          </div>
        </div>
      </div>

      {/* ── Alerts ── */}
      {alerts.length > 0 && (
        <div style={{
          background: `${C.red}10`, border: `1px solid ${C.red}44`,
          borderRadius: 8, padding: '10px 16px', display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap',
        }}>
          <span style={{ color: C.red, fontWeight: 700, fontSize: 12 }}>🔔 {alerts.length} ALERT{alerts.length > 1 ? 'S' : ''}</span>
          {alerts.slice(0, 6).map(a => {
            const col = STATUS_META[a.status_code]?.color || C.amber
            return (
              <span key={a.trade_id} style={{
                background: `${col}20`, color: col, border: `1px solid ${col}44`,
                padding: '2px 10px', borderRadius: 4, fontSize: 11, fontWeight: 600,
              }}>
                {a.ticker} · {a.status_label} · {a.direction === 'long' ? '▲' : '▼'} {a.signal_type}
              </span>
            )
          })}
        </div>
      )}

      {/* ── Status summary cards ── */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {[
          { key: 'near_target',   label: 'Near Target',   color: C.green  },
          { key: 'near_stop',     label: 'Near Stop',     color: C.red    },
          { key: 'open',          label: 'Open',          color: C.sky    },
          { key: 'pending_entry', label: 'Pending Entry', color: C.amber  },
          { key: 'target_hit',    label: 'Target Hit',    color: C.green  },
          { key: 'stop_hit',      label: 'Stop Hit',      color: C.red    },
          { key: 'expired',       label: 'Expired',       color: C.t3     },
        ].map(({ key, label, color }) => (
          <div key={key} style={{
            background: C.card, border: `1px solid ${(sm[key] || 0) > 0 ? color + '44' : C.border}`,
            borderRadius: 8, padding: '10px 16px', minWidth: 90,
          }}>
            <div style={{ color: C.t3, fontSize: 10, marginBottom: 2 }}>{label}</div>
            <div style={{ color: (sm[key] || 0) > 0 ? color : C.t3, fontSize: 22, fontWeight: 800 }}>{sm[key] || 0}</div>
          </div>
        ))}
      </div>

      {/* ── Sub-tabs + bucket filter ── */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        {([
          ['active',  `◉ Active  (${liveData?.total_active ?? 0})`],
          ['closed',  `✓ Resolved  (${liveData?.total_closed ?? 0})`],
          ['history', '📋 History (90d)'],
        ] as const).map(([s, lbl]) => (
          <button key={s} onClick={() => setSubTab(s)} style={{
            padding: '6px 16px', borderRadius: 6, cursor: 'pointer', fontSize: 12, fontWeight: 600,
            border: `1px solid ${subTab === s ? C.violet + '77' : C.border}`,
            background: subTab === s ? `${C.violet}22` : 'transparent',
            color: subTab === s ? C.violet : C.t3,
          }}>{lbl}</button>
        ))}
        <div style={{ display: 'flex', gap: 5, marginLeft: 10 }}>
          <span style={{ color: C.t3, fontSize: 10, alignSelf: 'center' }}>BUCKET</span>
          {['ALL', 'Turbo', 'Super', 'Standard'].map(b => (
            <button key={b} onClick={() => setBucketF(b)} style={fbtn(b, bucketF, setBucketF)}>{b}</button>
          ))}
        </div>
      </div>

      {/* ── Active positions ── */}
      {subTab === 'active' && (
        <div>
          <div style={{ color: C.t3, fontSize: 11, marginBottom: 8 }}>
            {activePositions.length} active position{activePositions.length !== 1 ? 's' : ''} ·
            status is determined by current market price vs TP / SL levels ·
            click any row to expand
          </div>
          <PositionsTable rows={activePositions} expanded={expanded} onExpand={setExpanded} />
        </div>
      )}

      {/* ── Resolved (target hit, stop hit, expired) ── */}
      {subTab === 'closed' && (
        <div>
          <div style={{ color: C.t3, fontSize: 11, marginBottom: 8 }}>
            {closedPositions.length} resolved position{closedPositions.length !== 1 ? 's' : ''} within the last {28} days
          </div>
          <PositionsTable rows={closedPositions} expanded={expanded} onExpand={setExpanded} />
        </div>
      )}

      {/* ── History ── */}
      {subTab === 'history' && histData && (
        <div>
          <div style={{ display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
            <StatBox label="Closed (90d)" value={histData.total.toLocaleString()} />
            <StatBox label="Win Rate" value={pct(histData.win_rate)} color={histData.win_rate >= 45 ? C.green : histData.win_rate >= 33 ? C.amber : C.red} />
            <StatBox label="Wins"   value={String(histData.wins)}   color={C.green} />
            <StatBox label="Losses" value={String(histData.losses)} color={C.red} />
          </div>
          {histData.history.length === 0 ? (
            <div style={{ color: C.t3, padding: 20 }}>No closed trades in the last 90 days.</div>
          ) : (
            <div style={{
              background: C.card, border: `1px solid ${C.border}`, borderRadius: 10,
              overflow: 'auto',
              height: 'clamp(420px, calc(100vh - 360px), 72vh)',
              minHeight: 420,
              overscrollBehavior: 'contain',
              scrollbarGutter: 'stable both-edges',
            }}>
              <table style={{ minWidth: 1200, width: 'max-content', borderCollapse: 'collapse', fontSize: 11 }}>
                <thead style={{ position: 'sticky', top: 0, background: C.card, zIndex: 2 }}>
                  <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                    {['Exit Date','Stock','Signal Type','Dir','Bucket','Entry ₹','Exit ₹','P&L %','Exit Reason','Days','Current ₹','Cumulative P&L'].map(h => (
                      <th key={h} style={{ padding: '7px 10px', color: C.t3, fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap', fontSize: 10 }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {histData.history.map(h => {
                    const bm = BUCKET_META[h.bucket.toLowerCase()]
                    return (
                      <tr key={h.trade_id} style={{ borderBottom: `1px solid ${C.border}1a` }}>
                        <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>{h.exit_date}</td>
                        <td style={{ padding: '6px 10px', color: C.violet, fontWeight: 700 }}>{h.ticker}</td>
                        <td style={{ padding: '6px 10px', color: C.t2, fontSize: 10 }}>{h.signal_type}</td>
                        <td style={{ padding: '6px 10px' }}>
                          <Chip val={h.direction === 'long' ? '▲ L' : '▼ S'} color={h.direction === 'long' ? C.green : C.red} small />
                        </td>
                        <td style={{ padding: '6px 10px' }}>
                          {bm && <Chip val={`${bm.icon} ${bm.label}`} color={bm.color} small />}
                        </td>
                        <td style={{ padding: '6px 10px', color: C.t, fontFamily: 'monospace' }}>{price(h.entry_price)}</td>
                        <td style={{ padding: '6px 10px', color: C.t, fontFamily: 'monospace' }}>{price(h.exit_price)}</td>
                        <td style={{ padding: '6px 10px', fontWeight: 700, color: h.pnl_pct >= 0 ? C.green : C.red }}>{pctS(h.pnl_pct)}</td>
                        <td style={{ padding: '6px 10px' }}>
                          <span style={{ color: h.exit_reason === 'tp' ? C.green : h.exit_reason === 'sl' ? C.red : C.amber, fontWeight: 700 }}>
                            {h.exit_reason.toUpperCase()}
                          </span>
                        </td>
                        <td style={{ padding: '6px 10px', color: C.t3 }}>{h.days_held}d</td>
                        <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>
                          {'exit_price' in h ? price((h as any).exit_price) : '—'}
                        </td>
                        <td style={{ padding: '6px 10px', fontFamily: 'monospace', fontWeight: 600, color: h.cumulative_pnl >= 0 ? C.green : C.red }}>
                          {pctS(h.cumulative_pnl)}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* StatusChip is exported for completeness; kept for future inline use */}
      {false && <StatusChip code="open" />}
    </div>
  )
}

// ── Public wrapper ───────────────────────────────────────────────────────────
export function LiveTradesWorkspace({ ticker }: { ticker: string }) {
  return <LiveTradesTab ticker={ticker} />
}
