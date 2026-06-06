'use client'

import React, { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import {
  getCombinations, getMissedProfit,
  getMpiRecommendations, getLivePositions, getLiveHistory,
  getExecSummary, getExecTrades, getExecComparison,
  getSwingOverview, getSwingTrades, getSwingTickers,
  type Combination,
  type MpiTrade, type MpiRecommendation,
  type LivePosition, type LivePositionsResponse, type LiveHistoryResponse,
  type ExecSummaryResponse, type ExecTrade, type ExecComparisonResponse,
  type SwingOverviewResponse, type SwingTrade, type ActiveSignal,
} from '@/lib/backtest-api'

const C = {
  bg: '#07070d', card: '#0f0f1a', border: 'rgba(255,255,255,0.08)',
  b2: 'rgba(255,255,255,0.14)', t: '#f0f0fc', t2: '#c8c8e0', t3: '#7878a0',
  green: '#00c98a', red: '#ff4d6d', amber: '#ffd166',
  violet: '#a78bfa', sky: '#38bdf8', orange: '#fb923c',
}

const YEARS   = ['ALL', '2024', '2025', '2026']
const TABS    = ['Overview', 'Trade Log', 'Combinations', 'MPI Analysis', 'Live Trades', 'Execution IQ']

const BUCKET_META: Record<string, { label: string; color: string; icon: string }> = {
  turbo:    { label: 'TURBO',    color: C.violet, icon: '🚀' },
  super:    { label: 'SUPER',    color: C.green,  icon: '🔥' },
  standard: { label: 'STANDARD', color: C.sky,    icon: '⚡' },
  trap:     { label: 'TRAP',     color: C.red,     icon: '❌' },
}

const REASON_COLORS: Record<string, string> = {
  FRESH_SIGNAL:   C.green,
  MULTI_PATTERN_2: C.sky,
  MULTI_PATTERN_3: C.amber,
}
function reasonColor(r: string) {
  return REASON_COLORS[r] || (r.startsWith('MULTI') ? C.orange : C.t3)
}

const pct  = (v: number | null | undefined, d = 1) => v == null ? '—' : `${v.toFixed(d)}%`
const pctS = (v: number | null | undefined, d = 1) => v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(d)}%`
const price = (v: number | null | undefined) => v == null ? '—' : v >= 1000
  ? v.toLocaleString('en-IN', { maximumFractionDigits: 0 })
  : v.toFixed(2)

function Chip({ val, color, small }: { val: string; color: string; small?: boolean }) {
  return (
    <span style={{
      background: `${color}22`, color, border: `1px solid ${color}44`,
      padding: small ? '1px 5px' : '2px 8px',
      borderRadius: 4, fontSize: small ? 10 : 11, fontWeight: 700,
      letterSpacing: '0.03em', whiteSpace: 'nowrap',
    }}>{val}</span>
  )
}

function StatBox({ label, value, sub, color }: {
  label: string; value: string | number; sub?: string; color?: string
}) {
  return (
    <div style={{
      background: C.card, border: `1px solid ${C.border}`,
      borderRadius: 10, padding: '14px 20px', minWidth: 110,
    }}>
      <div style={{ color: C.t3, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>{label}</div>
      <div style={{ color: color || C.t, fontSize: 22, fontWeight: 700, lineHeight: 1.1 }}>{value}</div>
      {sub && <div style={{ color: C.t3, fontSize: 10, marginTop: 3 }}>{sub}</div>}
    </div>
  )
}

function KV({ label, val, color }: { label: string; val: string; color?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
      <span style={{ color: C.t3, fontSize: 11 }}>{label}</span>
      <span style={{ color: color || C.t2, fontSize: 11, fontWeight: 600 }}>{val}</span>
    </div>
  )
}

// ── Banner explaining datetime convention ─────────────────────────────────────
function DataNote() {
  return (
    <div style={{
      background: `${C.amber}0f`, border: `1px solid ${C.amber}33`,
      borderRadius: 8, padding: '10px 16px', marginBottom: 20,
      fontSize: 12, color: C.t2, lineHeight: 1.7,
    }}>
      <span style={{ color: C.amber, fontWeight: 700 }}>TIMEFRAME NOTE · </span>
      All signals use <strong>1D daily data</strong> (NSE).
      Signal fires at market <strong>close 15:30 IST</strong> — entry executes at next-day{' '}
      <strong>open 09:15 IST</strong>.
      Delay is typically <strong>1065 min (overnight)</strong>; Friday→Monday signals show
      a 3975-min gap (weekend). No intraday timestamps exist for this dataset.
    </div>
  )
}

// ── Swing Overview ────────────────────────────────────────────────────────────
function SwingOverviewTab({ data, onTickerClick, year = 'ALL' }: {
  data: SwingOverviewResponse
  onTickerClick: (t: string) => void
  year?: string
}) {
  const s = data.summary

  const engineColor: Record<string, string> = {
    turbo: C.violet, super: C.green, standard: C.sky, trap: C.red,
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>

      {/* ── Summary strip ── */}
      {/* Hero: High-Conviction (Turbo + Super) */}
      <div style={{
        background: `${C.violet}0a`, border: `1px solid ${C.violet}33`,
        borderRadius: 12, padding: '16px 20px',
      }}>
        <div style={{ color: C.violet, fontSize: 10, fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 12 }}>
          🚀 High Conviction Engines — Turbo + Super
        </div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <StatBox label="Win Rate"        value={pct(s.hc_win_rate)}   color={s.hc_win_rate >= 90 ? C.green : C.amber} sub="Turbo + Super only" />
          <StatBox label="Avg P&L / Trade" value={pctS(s.hc_avg_pnl)}  color={s.hc_avg_pnl >= 0 ? C.green : C.red}    sub="smart entry effective" />
          <StatBox label="Cumulative P&L"  value={pctS(s.hc_total_pnl)} color={s.hc_total_pnl >= 0 ? C.green : C.red} sub={`sum of all ${year !== 'ALL' ? year + ' ' : ''}returns`} />
          <StatBox label="HC Trades"       value={String(s.hc_trades)}  color={C.violet} sub="Turbo + Super signals" />
          <StatBox label="Active Signals"  value={String(s.active_signals)} color={C.amber} sub="live opportunities" />
          <StatBox label="Avg Hold"        value={`${s.avg_days_held.toFixed(1)}d`} color={C.sky} sub="all engines" />
        </div>
      </div>

      {/* Secondary: All engines blended context */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', padding: '4px 2px' }}>
        <span style={{ color: C.t3, fontSize: 10 }}>ALL ENGINES (excl. trap){year !== 'ALL' ? ` · ${year}` : ''} ·</span>
        <span style={{ color: C.t2, fontSize: 11 }}>{s.total_long_trades} trades</span>
        <span style={{ color: C.t3, fontSize: 10 }}>·</span>
        <span style={{ color: s.smart_win_rate >= 45 ? C.green : C.amber, fontSize: 11, fontWeight: 600 }}>{pct(s.smart_win_rate)} WR</span>
        <span style={{ color: C.t3, fontSize: 10 }}>·</span>
        <span style={{ color: s.smart_avg_pnl >= 0 ? C.green : C.red, fontSize: 11, fontWeight: 600 }}>{pctS(s.smart_avg_pnl)} avg P&L</span>
        <span style={{ color: C.t3, fontSize: 10, marginLeft: 4 }}>
          (Standard engine drags blended avg — use Execution IQ filter to improve)
        </span>
      </div>

      {/* ── Engine cards ── */}
      <div>
        <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 12 }}>
          Signal Quality Engines
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(320px,1fr))', gap: 16 }}>
          {data.engines.map(eng => {
            const clr = engineColor[eng.bucket] || C.t3
            return (
              <div key={eng.bucket} style={{
                background: C.card, border: `1px solid ${clr}33`,
                borderRadius: 12, padding: '20px 20px 16px',
                display: 'flex', flexDirection: 'column', gap: 14,
              }}>
                {/* Card header */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontSize: 20 }}>{eng.icon}</span>
                    <div>
                      <div style={{ color: clr, fontWeight: 800, fontSize: 13, letterSpacing: '0.04em' }}>{eng.label}</div>
                      <div style={{ color: C.t3, fontSize: 10, marginTop: 1 }}>{eng.description}</div>
                    </div>
                  </div>
                  {eng.active_signals > 0 && (
                    <div style={{
                      background: `${C.amber}22`, border: `1px solid ${C.amber}55`,
                      borderRadius: 20, padding: '2px 10px', fontSize: 10, color: C.amber, fontWeight: 700,
                    }}>
                      {eng.active_signals} active
                    </div>
                  )}
                </div>

                {/* Stats grid */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
                  <div style={{ background: `${clr}0a`, borderRadius: 8, padding: '10px 12px' }}>
                    <div style={{ color: C.t3, fontSize: 9, textTransform: 'uppercase', marginBottom: 3 }}>Win Rate</div>
                    <div style={{ color: eng.smart_win_rate >= 45 ? C.green : eng.smart_win_rate >= 33 ? C.amber : C.red, fontSize: 20, fontWeight: 800 }}>{pct(eng.smart_win_rate)}</div>
                    <div style={{ color: C.t3, fontSize: 9, marginTop: 1 }}>smart execution</div>
                  </div>
                  <div style={{ background: `${clr}0a`, borderRadius: 8, padding: '10px 12px' }}>
                    <div style={{ color: C.t3, fontSize: 9, textTransform: 'uppercase', marginBottom: 3 }}>Avg P&amp;L</div>
                    <div style={{ color: eng.smart_avg_pnl >= 0 ? C.green : C.red, fontSize: 20, fontWeight: 800 }}>{pctS(eng.smart_avg_pnl)}</div>
                    <div style={{ color: C.t3, fontSize: 9, marginTop: 1 }}>per trade</div>
                  </div>
                  <div style={{ background: `${clr}0a`, borderRadius: 8, padding: '10px 12px' }}>
                    <div style={{ color: C.t3, fontSize: 9, textTransform: 'uppercase', marginBottom: 3 }}>Avg Hold</div>
                    <div style={{ color: C.sky, fontSize: 20, fontWeight: 800 }}>{eng.avg_days.toFixed(1)}d</div>
                    <div style={{ color: C.t3, fontSize: 9, marginTop: 1 }}>{eng.total_trades} trades</div>
                  </div>
                </div>

                {/* Period P&L row */}
                <div style={{ display: 'flex', gap: 8 }}>
                  {eng.pnl_90d_avg != null && (
                    <div style={{ flex: 1, background: `${C.border}`, borderRadius: 6, padding: '7px 10px' }}>
                      <div style={{ color: C.t3, fontSize: 9, marginBottom: 2 }}>90D AVG P&amp;L</div>
                      <div style={{ color: eng.pnl_90d_avg >= 0 ? C.green : C.red, fontWeight: 700, fontSize: 13 }}>{pctS(eng.pnl_90d_avg)}</div>
                      <div style={{ color: C.t3, fontSize: 9 }}>{eng.pnl_90d_trades} trades</div>
                    </div>
                  )}
                  {eng.pnl_180d_avg != null && (
                    <div style={{ flex: 1, background: `${C.border}`, borderRadius: 6, padding: '7px 10px' }}>
                      <div style={{ color: C.t3, fontSize: 9, marginBottom: 2 }}>180D AVG P&amp;L</div>
                      <div style={{ color: eng.pnl_180d_avg >= 0 ? C.green : C.red, fontWeight: 700, fontSize: 13 }}>{pctS(eng.pnl_180d_avg)}</div>
                      <div style={{ color: C.t3, fontSize: 9 }}>{eng.pnl_180d_trades} trades</div>
                    </div>
                  )}
                  <div style={{ flex: 1, background: `${C.border}`, borderRadius: 6, padding: '7px 10px' }}>
                    <div style={{ color: C.t3, fontSize: 9, marginBottom: 2 }}>ALL-TIME P&amp;L</div>
                    <div style={{ color: eng.total_pnl_all >= 0 ? C.green : C.red, fontWeight: 700, fontSize: 13 }}>{pctS(eng.total_pnl_all)}</div>
                    <div style={{ color: C.t3, fontSize: 9 }}>{eng.total_trades} trades</div>
                  </div>
                </div>

                {/* Top stocks mini-table */}
                {eng.top_stocks.length > 0 && (
                  <div>
                    <div style={{ color: C.t3, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>
                      Top Stocks · ranked by 90d P&amp;L
                    </div>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                      <thead>
                        <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                          {['#','Ticker','WR%','Avg P&L','90d P&L','Hold'].map(h => (
                            <th key={h} style={{ padding: '4px 6px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 9 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {eng.top_stocks.map(st => (
                          <tr key={st.ticker} style={{ borderBottom: `1px solid ${C.border}22` }}>
                            <td style={{ padding: '5px 6px', color: C.t3, fontSize: 10 }}>{st.rank}</td>
                            <td style={{ padding: '5px 6px' }}>
                              <button onClick={() => onTickerClick(st.ticker)} style={{
                                background: 'none', border: 'none', cursor: 'pointer',
                                color: st.active ? C.amber : clr, fontWeight: 700, fontSize: 11, padding: 0,
                              }}>
                                {st.ticker}
                                {st.active && <span style={{ color: C.amber, marginLeft: 3, fontSize: 8 }}>●</span>}
                              </button>
                            </td>
                            <td style={{ padding: '5px 6px', color: st.win_rate >= 45 ? C.green : st.win_rate >= 33 ? C.amber : C.red }}>
                              {pct(st.win_rate)}
                            </td>
                            <td style={{ padding: '5px 6px', color: st.avg_pnl >= 0 ? C.green : C.red }}>
                              {pctS(st.avg_pnl)}
                            </td>
                            <td style={{ padding: '5px 6px', color: st.avg_pnl_90d != null ? (st.avg_pnl_90d >= 0 ? C.green : C.red) : C.t3 }}>
                              {st.avg_pnl_90d != null ? pctS(st.avg_pnl_90d) : '—'}
                            </td>
                            <td style={{ padding: '5px 6px', color: C.t3 }}>{st.avg_days.toFixed(1)}d</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* ── Active signals ── */}
      {data.active_signals.length > 0 && (
        <div>
          <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 10 }}>
            Active Signals Today — {data.active_signals.length} opportunities
          </div>
          <div style={{ background: C.card, border: `1px solid ${C.amber}33`, borderRadius: 10, overflow: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                  {['Ticker','Engine','Tier','Score','Credibility','Date','Setup'].map(h => (
                    <th key={h} style={{ padding: '9px 14px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 10 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.active_signals.map((sig, i) => (
                  <tr key={`${sig.ticker}-${sig.latest_date}-${i}`} style={{ borderBottom: `1px solid ${C.border}22` }}>
                    <td style={{ padding: '9px 14px' }}>
                      <button onClick={() => onTickerClick(sig.ticker)} style={{
                        background: 'none', border: 'none', cursor: 'pointer',
                        color: C.amber, fontWeight: 700, fontSize: 13, padding: 0,
                      }}>{sig.ticker}</button>
                    </td>
                    <td style={{ padding: '9px 14px' }}>
                      <Chip val={(sig.bucket || 'standard').toUpperCase()} color={engineColor[(sig.bucket || 'standard').toLowerCase()] || C.t3} small />
                    </td>
                    <td style={{ padding: '9px 14px' }}>
                      <Chip val={sig.tier.replace(/_/g,' ').toUpperCase()} color={C.violet} small />
                    </td>
                    <td style={{ padding: '9px 14px', color: C.violet, fontWeight: 700 }}>
                      {sig.opportunity_score.toFixed(3)}
                    </td>
                    <td style={{ padding: '9px 14px', color: C.sky }}>{sig.credibility}</td>
                    <td style={{ padding: '9px 14px', color: C.t3, fontSize: 11 }}>{sig.latest_date}</td>
                    <td style={{ padding: '9px 14px', color: C.t2, fontSize: 11 }}>{sig.setup_summary}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div style={{ color: C.t3, fontSize: 10, paddingTop: 4 }}>
        Data as of {data.as_of} · Long-only cash equity · Smart P&L uses execution filter (smart entry where taken, blind otherwise)
      </div>
    </div>
  )
}

// ── Trade Log ─────────────────────────────────────────────────────────────────
function TradeLogTab({ ticker, year }: { ticker: string; year: string }) {
  const [data, setData]         = useState<{ count: number; trades: SwingTrade[] } | null>(null)
  const [bucket, setBucket]     = useState('ALL')
  const [loading, setLoading]   = useState(true)
  const [expanded, setExpanded] = useState<number | null>(null)

  useEffect(() => {
    setLoading(true)
    getSwingTrades(
      ticker !== 'ALL' ? ticker : undefined,
      bucket !== 'ALL' ? bucket.toLowerCase() : undefined,
      year   !== 'ALL' ? year   : undefined,
    ).then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [ticker, year, bucket])

  const exitColor = (r: string) => r === 'tp' ? C.green : r === 'sl' ? C.red : C.amber

  const bucketBar = (
    <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap' }}>
      {['ALL', 'turbo', 'super', 'standard'].map(b => (
        <button key={b} onClick={() => setBucket(b)} style={{
          padding: '4px 12px', borderRadius: 5, fontSize: 11, fontWeight: 600, cursor: 'pointer',
          border: `1px solid ${bucket === b ? C.violet : C.border}`,
          background: bucket === b ? `${C.violet}22` : 'transparent',
          color: bucket === b ? C.violet : C.t3,
        }}>
          {BUCKET_META[b]?.icon || ''} {b.toUpperCase()}
        </button>
      ))}
    </div>
  )

  if (loading) return <div>{bucketBar}<div style={{ color: C.t3, padding: 20 }}>Loading trades...</div></div>
  if (!data || data.trades.length === 0)
    return <div>{bucketBar}<div style={{ color: C.t3, padding: 20 }}>No trades found for these filters.</div></div>

  return (
    <div>
      {bucketBar}
      <div style={{ color: C.t3, fontSize: 11, marginBottom: 10 }}>
        {data.count.toLocaleString()} long trades · smart entry price where execution engine fired · click row to expand
      </div>
      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflowX: 'auto', overflowY: 'auto', maxHeight: '70vh' }}>
        <table style={{ minWidth: 'max-content', width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.card, zIndex: 1 }}>
            <tr style={{ borderBottom: `1px solid ${C.border}` }}>
              {[
                'Signal ID','Stock','Type',
                'Signal Date','Entry Date','Delay',
                'Entry ₹','SL ₹','Target ₹','Exec Decision',
                'Exit','Days','P&L','MFE','MAE','MPI','Bucket','Reason',
              ].map(h => (
                <th key={h} style={{ padding: '8px 10px', color: C.t3, fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap', fontSize: 10 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.trades.filter(t => t.bucket?.toLowerCase() !== 'trap').slice(0, 300).map(t => {
              // Fallbacks for fields not yet returned by old API version
              const signalId   = t.signal_id   || `SIG-${String(t.id).padStart(6,'0')}`
              const signalDate = t.signal_date  || t.entry_date
              const delayLbl   = t.delay_label  || 'overnight'
              const effEntry   = (t.effective_entry_price ?? t.entry_price) || t.entry_price
              const stopP      = (t.stop_price  > 0) ? t.stop_price  : null
              const targetP    = (t.target_price > 0) ? t.target_price : null
              const rr         = t.rr           ?? 2.0
              const mc         = t.multi_pattern_count ?? 1
              const rc         = t.reason_code  || ''
              const tf         = t.timeframe    || '1D'
              const isSmart    = !!(t.trade_taken && t.smart_entry_price && t.smart_entry_price !== t.entry_price)

              const bm      = BUCKET_META[t.bucket?.toLowerCase()]
              const isEx    = expanded === t.id
              const taken   = t.trade_taken
              const execClr = taken ? C.green : t.exec_code?.startsWith('NO_TRADE') ? C.red : C.sky
              return (
                <React.Fragment key={t.id}>
                  <tr
                    onClick={() => setExpanded(isEx ? null : t.id)}
                    style={{
                      borderBottom: `1px solid ${C.border}22`,
                      cursor: 'pointer',
                      background: isEx ? `${C.violet}0a` : 'transparent',
                    }}
                  >
                    <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>{signalId}</td>
                    <td style={{ padding: '6px 10px', color: C.violet, fontWeight: 700 }}>{t.ticker}</td>
                    <td style={{ padding: '6px 10px', color: C.t2, maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{t.signal_type || 'AI Pattern'}</td>
                    <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>{signalDate}</td>
                    <td style={{ padding: '6px 10px', color: C.t2, fontFamily: 'monospace', fontSize: 10 }}>{t.entry_date}</td>
                    <td style={{ padding: '6px 10px', color: C.sky, fontSize: 10 }}>{delayLbl}</td>
                    <td style={{ padding: '6px 10px', fontFamily: 'monospace' }}>
                      <span style={{ color: isSmart ? C.violet : C.t }}>{price(effEntry)}</span>
                      {isSmart && <span style={{ color: C.t3, fontSize: 9, marginLeft: 3 }}>smart</span>}
                    </td>
                    <td style={{ padding: '6px 10px', color: C.red,   fontFamily: 'monospace' }}>{price(stopP)}</td>
                    <td style={{ padding: '6px 10px', color: C.green, fontFamily: 'monospace' }}>{price(targetP)}</td>
                    <td style={{ padding: '6px 10px' }}>
                      {t.exec_code ? (
                        <span style={{ color: execClr, fontSize: 10, fontWeight: 600 }}>
                          {t.exec_code.replace(/_/g, ' ')}
                        </span>
                      ) : <span style={{ color: C.t3 }}>—</span>}
                    </td>
                    <td style={{ padding: '6px 10px' }}>
                      <span style={{ color: exitColor(t.exit_reason), fontWeight: 700 }}>{(t.exit_reason || '').toUpperCase()}</span>
                    </td>
                    <td style={{ padding: '6px 10px', color: C.t3 }}>{t.days_held}d</td>
                    <td style={{ padding: '6px 10px', color: (t.effective_pnl ?? 0) >= 0 ? C.green : C.red, fontWeight: 700 }}>
                      {pctS(t.effective_pnl ?? t.pnl_pct)}
                    </td>
                    <td style={{ padding: '6px 10px', color: C.green }}>{pct(t.mfe_pct)}</td>
                    <td style={{ padding: '6px 10px', color: C.red }}>{pct(t.mae_pct)}</td>
                    <td style={{ padding: '6px 10px', color: t.mpi_pct && t.mpi_pct > 0 ? C.violet : C.t3 }}>
                      {t.mpi_pct && t.mpi_pct > 0 ? pct(t.mpi_pct) : '—'}
                    </td>
                    <td style={{ padding: '6px 10px' }}>
                      {bm && <Chip val={`${bm.icon} ${bm.label}`} color={bm.color} small />}
                    </td>
                    <td style={{ padding: '6px 10px' }}>
                      {rc ? <Chip val={rc} color={reasonColor(rc)} small /> : <span style={{ color: C.t3 }}>—</span>}
                    </td>
                  </tr>
                  {isEx && (
                    <tr style={{ background: `${C.violet}08` }}>
                      <td colSpan={18} style={{ padding: '12px 20px' }}>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16, fontSize: 11 }}>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>SIGNAL METADATA</div>
                            <KV label="Signal ID"          val={signalId} />
                            <KV label="Timeframe"          val={tf} color={C.sky} />
                            <KV label="Competing patterns" val={`${mc} on this day`} color={mc > 3 ? C.orange : C.sky} />
                            <KV label="Opportunity score"  val={t.opportunity_score != null ? t.opportunity_score.toFixed(4) : '—'} />
                            <KV label="Reason code"        val={rc || '—'} color={reasonColor(rc)} />
                            <KV label="Tier"               val={t.tier || '—'} />
                            <KV label="Credibility"        val={t.credibility || '—'} />
                          </div>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>EXECUTION DETAIL</div>
                            <KV label="Exec decision"  val={t.exec_code || '—'} color={execClr} />
                            <KV label="Trade taken"    val={taken === true ? 'YES' : taken === false ? 'NO' : '—'} color={taken ? C.green : C.red} />
                            <KV label="Entry window"   val={t.entry_window || '—'} color={C.sky} />
                            <KV label="Entry ₹"        val={`₹${price(t.entry_price)}`} />
                            <KV label="Smart entry ₹"  val={t.smart_entry_price ? `₹${price(t.smart_entry_price)}` : '—'} color={C.violet} />
                            <KV label="Gap %"          val={t.gap_pct != null ? pctS(t.gap_pct) : '—'} color={C.t2} />
                            <KV label="RS vs Nifty"    val={t.rs_vs_nifty != null ? pctS(t.rs_vs_nifty) : '—'} color={(t.rs_vs_nifty ?? 0) > 0 ? C.green : C.red} />
                          </div>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 6, fontWeight: 600 }}>P&amp;L BREAKDOWN</div>
                            <KV label="Effective P&L"  val={pctS(t.effective_pnl ?? t.pnl_pct)} color={(t.effective_pnl ?? t.pnl_pct ?? 0) >= 0 ? C.green : C.red} />
                            <KV label="Smart P&L"      val={t.smart_pnl_pct != null ? pctS(t.smart_pnl_pct) : '—'} color={C.violet} />
                            <KV label="R:R ratio"      val={`1:${rr.toFixed(1)}`} />
                            <KV label="Days held"      val={`${t.days_held}d`} />
                            <KV label="Exit"           val={(t.exit_reason || '').toUpperCase()} color={exitColor(t.exit_reason)} />
                            <div style={{ marginTop: 8, color: C.t2, fontSize: 10, lineHeight: 1.6, wordBreak: 'break-word' }}>{t.pattern}</div>
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
    </div>
  )
}

// ── Combinations ──────────────────────────────────────────────────────────────
function CombinationsTab({ ticker }: { ticker: string }) {
  const [data, setData]       = useState<{ count: number; combinations: Combination[] } | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    getCombinations(ticker !== 'ALL' ? ticker : undefined)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [ticker])

  if (loading) return <div style={{ color: C.t3, padding: 20 }}>Loading...</div>
  if (!data || data.combinations.length === 0)
    return <div style={{ color: C.t3, padding: 20 }}>No combination data found.</div>

  const catColor = (cat: string) =>
    cat === 'turbo' ? C.violet : cat === 'super' ? C.green : cat === 'trap' ? C.red : C.sky

  return (
    <div>
      <div style={{ color: C.t3, fontSize: 11, marginBottom: 14 }}>
        {data.count} pattern combinations ranked by win rate — top 3 atoms shown per group
      </div>
      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflowX: 'auto', overflowY: 'auto', maxHeight: '72vh' }}>
        <table style={{ minWidth: 'max-content', width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.card }}>
            <tr style={{ borderBottom: `1px solid ${C.border}` }}>
              {['#','Signal Type','Pattern Atoms (top 3)','Stocks','Trades','Win%','Avg Return','Category'].map(h => (
                <th key={h} style={{ padding: '9px 12px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 10 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.combinations.map((c, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${C.border}22` }}>
                <td style={{ padding: '8px 12px', color: C.t3 }}>{i + 1}</td>
                <td style={{ padding: '8px 12px', color: C.amber, fontWeight: 600 }}>{c.signal_type}</td>
                <td style={{ padding: '8px 12px', color: C.t2, maxWidth: 300, fontSize: 10 }}>
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={c.full_pattern}>
                    {c.pattern}
                  </div>
                </td>
                <td style={{ padding: '8px 12px', color: C.t3, fontSize: 10 }}>{c.tickers?.join(', ')}</td>
                <td style={{ padding: '8px 12px', color: C.t }}>{c.total}</td>
                <td style={{ padding: '8px 12px', fontWeight: 700,
                  color: c.win_rate >= 55 ? C.green : c.win_rate >= 35 ? C.amber : C.red }}>
                  {pct(c.win_rate)}
                </td>
                <td style={{ padding: '8px 12px', color: c.avg_return >= 0 ? C.green : C.red }}>
                  {pctS(c.avg_return)}
                </td>
                <td style={{ padding: '8px 12px' }}>
                  <Chip val={c.category.toUpperCase()} color={catColor(c.category)} small />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── MPI Analysis tab ──────────────────────────────────────────────────────────
function MpiTab({ ticker }: { ticker: string }) {
  const [mpiData,  setMpiData]  = useState<{ count: number; avg_missed_pct: number; trades: MpiTrade[] } | null>(null)
  const [recData,  setRecData]  = useState<{ count: number; recommendations: MpiRecommendation[] } | null>(null)
  const [subTab,   setSubTab]   = useState<'events' | 'recommendations'>('recommendations')
  const [loading,  setLoading]  = useState(true)

  useEffect(() => {
    setLoading(true)
    const tk = ticker !== 'ALL' ? ticker : undefined
    Promise.all([getMissedProfit(tk), getMpiRecommendations(tk)])
      .then(([m, r]) => { setMpiData(m); setRecData(r); setLoading(false) })
      .catch(() => setLoading(false))
  }, [ticker])

  if (loading) return <div style={{ color: C.t3, padding: 20 }}>Loading MPI analysis...</div>

  const actionColor = (code: string) =>
    code === 'trailing_stop' ? C.violet : code === 'extend_target' ? C.amber : C.t3

  const actionIcon = (code: string) =>
    code === 'trailing_stop' ? '🎯' : code === 'extend_target' ? '📈' : '✓'

  return (
    <div>
      {/* Explainer */}
      <div style={{
        background: `${C.violet}0f`, border: `1px solid ${C.violet}33`,
        borderRadius: 8, padding: '14px 18px', marginBottom: 20, fontSize: 12, color: C.t2, lineHeight: 1.7,
      }}>
        <div style={{ color: C.violet, fontWeight: 700, marginBottom: 4 }}>MISSED PROFIT INDEX (MPI)</div>
        The MPI measures how much additional gain was available <em>after</em> the engine booked its target.
        A trade booking +5% while the stock continued to +22% has an MPI of 17%. High MPI signals
        are candidates for <strong>trailing stops</strong> or <strong>extended targets</strong> —
        the fixed TP is leaving significant edge on the table.
      </div>

      {mpiData && mpiData.count > 0 && (
        <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
          <StatBox label="MPI Events"        value={mpiData.count.toLocaleString()} sub="TP trades w/ >2% continuation" />
          <StatBox label="Avg Missed Gain"   value={pct(mpiData.avg_missed_pct)} color={C.violet} sub="per MPI event" />
          <StatBox label="Recommendations"   value={String(recData?.count || 0)} color={C.amber} sub="signal types needing action" />
        </div>
      )}

      {/* Sub-tab toggle */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 18 }}>
        {(['recommendations', 'events'] as const).map(s => (
          <button key={s} onClick={() => setSubTab(s)} style={{
            padding: '6px 16px', borderRadius: 6, cursor: 'pointer', fontSize: 12, fontWeight: 600,
            border: `1px solid ${subTab === s ? C.violet + '77' : C.border}`,
            background: subTab === s ? `${C.violet}22` : 'transparent',
            color: subTab === s ? C.violet : C.t3,
          }}>
            {s === 'recommendations' ? '🎯 Next Steps (Recommendations)' : '📋 MPI Event Log'}
          </button>
        ))}
      </div>

      {/* Recommendations */}
      {subTab === 'recommendations' && recData && (
        recData.recommendations.length === 0
          ? <div style={{ color: C.t3, padding: 20 }}>No significant MPI patterns found.</div>
          : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {recData.recommendations.map((r, i) => (
                <div key={i} style={{
                  background: C.card, border: `1px solid ${actionColor(r.action_code)}33`,
                  borderRadius: 10, padding: '16px 20px',
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 10 }}>
                    <div>
                      <span style={{ color: C.violet, fontWeight: 700, fontSize: 13 }}>{r.ticker}</span>
                      <span style={{ color: C.t3, fontSize: 12, marginLeft: 10 }}>{r.signal_type}</span>
                    </div>
                    <Chip val={`${actionIcon(r.action_code)} ${r.action}`} color={actionColor(r.action_code)} />
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 12, marginBottom: 12 }}>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10 }}>TP Trades Analysed</div>
                      <div style={{ color: C.t, fontWeight: 700 }}>{r.total_tp_trades}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10 }}>Avg Booked</div>
                      <div style={{ color: C.green, fontWeight: 700 }}>{pctS(r.avg_booked_pct)}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10 }}>Avg Continuation (MPI)</div>
                      <div style={{ color: C.violet, fontWeight: 700 }}>{pct(r.avg_mpi_pct)}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10 }}>High MPI Rate (≥5%)</div>
                      <div style={{ color: C.amber, fontWeight: 700 }}>{pct(r.high_mpi_rate)}</div>
                    </div>
                  </div>
                  <div style={{ color: C.t2, fontSize: 11, lineHeight: 1.7, marginBottom: 10 }}>{r.rationale}</div>
                  <div style={{
                    background: `${actionColor(r.action_code)}0f`, border: `1px solid ${actionColor(r.action_code)}22`,
                    borderRadius: 6, padding: '8px 14px', display: 'flex', gap: 24, flexWrap: 'wrap',
                  }}>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Current avg gain: <strong style={{ color: C.green }}>{pctS(r.avg_booked_pct)}</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Simulated with {r.action_code === 'trailing_stop' ? 'trailing stop' : '1.5× target'}:{' '}
                      <strong style={{ color: actionColor(r.action_code) }}>{pctS(r.simulated_avg_pnl)}</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Extra gain per trade: <strong style={{ color: C.amber }}>+{r.extra_gain_pct.toFixed(2)}%</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Max single continuation: <strong style={{ color: C.violet }}>{pct(r.max_mpi_pct)}</strong>
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )
      )}

      {/* Event log */}
      {subTab === 'events' && mpiData && (
        mpiData.trades.length === 0
          ? <div style={{ color: C.t3, padding: 20 }}>No MPI events (&gt;2% continuation) found.</div>
          : (
            <div style={{
              background: C.card, border: `1px solid ${C.border}`, borderRadius: 10,
              overflow: 'auto',
              height: 'clamp(420px, calc(100vh - 360px), 72vh)',
              minHeight: 420,
              overscrollBehavior: 'contain',
              scrollbarGutter: 'stable both-edges',
            }}>
              <table style={{ minWidth: 1400, width: 'max-content', borderCollapse: 'collapse', fontSize: 11 }}>
                <thead style={{ position: 'sticky', top: 0, background: C.card, zIndex: 2 }}>
                  <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                    {['Signal ID','Stock','TF','Signal Time','Entry Time','Dir','Entry ₹','Exit ₹','Booked','Continued','Total Avail','MISSED','Signal Type'].map(h => (
                      <th key={h} style={{ padding: '8px 10px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 10, whiteSpace: 'nowrap' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {mpiData.trades.map((t, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${C.border}22` }}>
                      <td style={{ padding: '7px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>{t.signal_id}</td>
                      <td style={{ padding: '7px 10px', color: C.violet, fontWeight: 600 }}>{t.ticker}</td>
                      <td style={{ padding: '7px 10px' }}><Chip val={t.timeframe} color={C.sky} small /></td>
                      <td style={{ padding: '7px 10px', color: C.t3, fontFamily: 'monospace', fontSize: 10 }}>{t.signal_datetime}</td>
                      <td style={{ padding: '7px 10px', color: C.t2, fontFamily: 'monospace', fontSize: 10 }}>{t.entry_datetime}</td>
                      <td style={{ padding: '7px 10px' }}>
                        <Chip val={t.direction === 'long' ? 'LONG' : 'SHORT'} color={t.direction === 'long' ? C.green : C.red} small />
                      </td>
                      <td style={{ padding: '7px 10px', color: C.t, fontFamily: 'monospace' }}>{price(t.entry_price)}</td>
                      <td style={{ padding: '7px 10px', color: C.t, fontFamily: 'monospace' }}>{price(t.exit_price)}</td>
                      <td style={{ padding: '7px 10px', color: C.green, fontWeight: 600 }}>{pctS(t.booked_pct)}</td>
                      <td style={{ padding: '7px 10px', color: C.sky }}>{pctS(t.continued_pct)}</td>
                      <td style={{ padding: '7px 10px', color: C.amber, fontWeight: 700 }}>{pctS(t.total_available)}</td>
                      <td style={{ padding: '7px 10px', color: C.red, fontWeight: 700 }}>-{pct(t.missed_pct)}</td>
                      <td style={{ padding: '7px 10px', color: C.t3, fontSize: 10 }}>{t.signal_type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
      )}
    </div>
  )
}

// ── Live Trades ───────────────────────────────────────────────────────────────

const STATUS_META: Record<string, { label: string; color: string; icon: string; pulse?: boolean }> = {
  pending_entry: { label: 'PENDING ENTRY', color: '#ffd166', icon: '⏳' },
  open:          { label: 'OPEN',          color: '#38bdf8', icon: '◉'  },
  near_target:   { label: 'NEAR TARGET',   color: '#00c98a', icon: '🎯', pulse: true },
  near_stop:     { label: 'NEAR STOP',     color: '#ff4d6d', icon: '⚠',  pulse: true },
  target_hit:    { label: 'TARGET HIT',    color: '#00c98a', icon: '✓'  },
  stop_hit:      { label: 'STOP HIT',      color: '#ff4d6d', icon: '✕'  },
  expired:       { label: 'EXPIRED',       color: '#7878a0', icon: '—'  },
}

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

function ScoreBar({ score }: { score: number | null }) {
  if (score == null) return <span style={{ color: C.t3 }}>—</span>
  const pct = Math.round(score * 100)
  const col = score >= 0.85 ? C.green : score >= 0.65 ? C.amber : C.t3
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 44, height: 5, background: 'rgba(255,255,255,0.08)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: col, borderRadius: 3 }} />
      </div>
      <span style={{ color: col, fontSize: 10, fontWeight: 700 }}>{pct}</span>
    </div>
  )
}

// Expiry progress bar: shows how much of the max-hold window has been consumed
function ExpiryBar({ daysOpen, maxDays }: { daysOpen: number; maxDays: number }) {
  const pct = Math.min(100, Math.round((daysOpen / maxDays) * 100))
  const col  = pct < 50 ? C.green : pct < 80 ? C.amber : C.red
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 36, height: 4, background: 'rgba(255,255,255,0.08)', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: col, borderRadius: 2 }} />
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
    [liveData, bucketF],
  )
  const closedPositions = useMemo(
    () => filterByBucket(liveData?.closed || []),
    [liveData, bucketF],
  )

  const sm     = liveData?.summary || {}
  const mkt    = liveData?.market
  const prices = liveData?.current_prices || {}
  const alerts = liveData?.alerts || []
  const mktDot = mkt?.status === 'open' ? C.green : mkt?.status === 'pre' ? C.amber : C.red

  const fbtn = (val: string, active: string, set: (v: string) => void, col = C.violet) => ({
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
              <div style={{ color: pr ? C.t : C.t3, fontSize: 12, fontWeight: 700, fontFamily: 'monospace' }}>{pr ? price(pr) : '—'}</div>
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
    </div>
  )
}

// ── Execution IQ ─────────────────────────────────────────────────────────────

const EXEC_CODE_LABELS: Record<string, string> = {
  EARLY_ENTRY:          'Early Entry (9:15)',
  DELAYED_9_30:         'Delayed ~9:30',
  DELAYED_10_00:        'Delayed ~10:00',
  DELAYED_11_00:        'Delayed ~11:00',
  PULLBACK_ENTRY:       'Pullback Entry',
  RECLAIM_ENTRY_10:     'Reclaim ~10:00',
  RECLAIM_ENTRY_11:     'Reclaim ~11:00',
  NO_TRADE_GAP_CHASE:   'No Trade — Gap Chase',
  NO_TRADE_WEAK_OPEN:   'No Trade — Weak Open',
  NO_TRADE_INDEX_WEAK:  'No Trade — Index Weak',
  NO_TRADE_VOLATILE:    'No Trade — Volatile',
}

const EXEC_CODE_COLORS: Record<string, string> = {
  EARLY_ENTRY:          C.green,
  DELAYED_9_30:         C.sky,
  DELAYED_10_00:        C.sky,
  DELAYED_11_00:        C.amber,
  PULLBACK_ENTRY:       C.violet,
  RECLAIM_ENTRY_10:     C.violet,
  RECLAIM_ENTRY_11:     C.violet,
  NO_TRADE_GAP_CHASE:   C.red,
  NO_TRADE_WEAK_OPEN:   C.red,
  NO_TRADE_INDEX_WEAK:  C.orange,
  NO_TRADE_VOLATILE:    C.orange,
}

const GAP_CAT_COLORS: Record<string, string> = {
  BIG_GAP_UP:   C.green,
  GAP_UP:       '#66efb0',
  FLAT:         C.sky,
  GAP_DOWN:     C.amber,
  BIG_GAP_DOWN: C.red,
}

function ExecCompareCard({
  label, blindWr, smartWr, blindAvg, smartAvg, total, taken,
}: {
  label: string; blindWr: number; smartWr: number
  blindAvg: number; smartAvg: number | null; total: number; taken?: number | null
}) {
  const wrDelta  = smartWr != null ? smartWr - blindWr : null
  const avgDelta = smartAvg != null ? smartAvg - blindAvg : null
  return (
    <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: '14px 18px', minWidth: 160 }}>
      <div style={{ color: C.t3, fontSize: 11, marginBottom: 6 }}>{label}</div>
      <div style={{ color: C.t2, fontSize: 12, marginBottom: 8 }}>{total} signals{taken != null ? ` · ${taken} taken` : ''}</div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
        <div>
          <div style={{ color: C.t3, fontSize: 10 }}>Blind Win%</div>
          <div style={{ color: C.t, fontSize: 18, fontWeight: 700 }}>{blindWr.toFixed(1)}%</div>
        </div>
        <div>
          <div style={{ color: C.t3, fontSize: 10 }}>Smart Win%</div>
          <div style={{ color: wrDelta != null && wrDelta > 0 ? C.green : C.amber, fontSize: 18, fontWeight: 700 }}>
            {smartWr != null ? smartWr.toFixed(1) + '%' : '—'}
          </div>
        </div>
        <div>
          <div style={{ color: C.t3, fontSize: 10 }}>Blind Avg P&amp;L</div>
          <div style={{ color: blindAvg >= 0 ? C.green : C.red, fontSize: 14, fontWeight: 600 }}>{blindAvg >= 0 ? '+' : ''}{blindAvg.toFixed(2)}%</div>
        </div>
        <div>
          <div style={{ color: C.t3, fontSize: 10 }}>Smart Avg P&amp;L</div>
          <div style={{ color: avgDelta != null ? (avgDelta >= 0 ? C.green : C.red) : C.t3, fontSize: 14, fontWeight: 600 }}>
            {smartAvg != null ? (smartAvg >= 0 ? '+' : '') + smartAvg.toFixed(2) + '%' : '—'}
          </div>
        </div>
      </div>
      {wrDelta != null && (
        <div style={{ marginTop: 8, paddingTop: 8, borderTop: `1px solid ${C.border}`, fontSize: 11,
                      color: wrDelta > 0 ? C.green : C.red }}>
          Win% Δ {wrDelta > 0 ? '▲' : '▼'} {Math.abs(wrDelta).toFixed(1)} pts
        </div>
      )}
    </div>
  )
}

function ExecDistBar({ items, total }: { items: { label: string; count: number; color: string }[]; total: number }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {items.map(item => (
        <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ width: 180, fontSize: 11, color: C.t2, textAlign: 'right', flexShrink: 0 }}>{item.label}</div>
          <div style={{ flex: 1, height: 16, background: C.border, borderRadius: 3, overflow: 'hidden' }}>
            <div style={{ width: `${(item.count / total * 100).toFixed(1)}%`, height: '100%',
                          background: item.color, borderRadius: 3, minWidth: item.count > 0 ? 4 : 0 }} />
          </div>
          <div style={{ width: 60, fontSize: 11, color: C.t3 }}>
            {item.count} <span style={{ color: C.t3 }}>({(item.count / total * 100).toFixed(0)}%)</span>
          </div>
        </div>
      ))}
    </div>
  )
}

function ExecutionIQTab({ ticker }: { ticker: string }) {
  const t = ticker === 'ALL' ? undefined : ticker

  const [summary,    setSummary]    = useState<ExecSummaryResponse | null>(null)
  const [comparison, setComparison] = useState<ExecComparisonResponse | null>(null)
  const [trades,     setTrades]     = useState<ExecTrade[]>([])
  const [tradeCount, setTradeCount] = useState(0)
  const [loading,    setLoading]    = useState(true)
  const [error,      setError]      = useState<string | null>(null)
  const [subTab,     setSubTab]     = useState(0)  // 0=Overview 1=Trades
  const [codeFilter, setCodeFilter] = useState('')
  const [takenFilter,setTakenFilter]= useState<'' | '0' | '1'>('')

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      const [s, c, tr] = await Promise.all([
        getExecSummary(t),
        getExecComparison(t),
        getExecTrades(t, codeFilter || undefined,
                      takenFilter === '' ? undefined : Number(takenFilter)),
      ])
      setSummary(s); setComparison(c)
      setTrades(tr.trades); setTradeCount(tr.count)
    } catch (e: any) {
      setError(e.message || 'Failed to load execution data')
    } finally {
      setLoading(false)
    }
  }, [t, codeFilter, takenFilter])

  useEffect(() => { load() }, [load])

  const cell: React.CSSProperties = { padding: '8px 10px', borderBottom: `1px solid ${C.border}`, fontSize: 12 }
  const th: React.CSSProperties   = { ...cell, color: C.t3, fontSize: 11, fontWeight: 600, background: C.card }

  if (loading) return (
    <div style={{ textAlign: 'center', color: C.t3, padding: 60, fontSize: 14 }}>
      Loading execution analysis…
    </div>
  )

  if (error) return (
    <div style={{ textAlign: 'center', color: C.amber, padding: 60, fontSize: 13 }}>
      {error.includes('run run_execution') ? (
        <>
          <div style={{ fontSize: 16, marginBottom: 8 }}>No execution data yet</div>
          <div style={{ color: C.t3 }}>Run the analysis script first:</div>
          <code style={{ display: 'block', marginTop: 8, color: C.sky, background: C.card, padding: '8px 16px', borderRadius: 6 }}>
            python engine/backtest/run_execution_analysis.py
          </code>
        </>
      ) : error}
    </div>
  )

  if (!summary) return null

  const subTabStyle = (i: number): React.CSSProperties => ({
    padding: '6px 16px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 12,
    background: subTab === i ? `${C.violet}22` : 'transparent',
    color: subTab === i ? C.violet : C.t3,
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Sub-tab nav */}
      <div style={{ display: 'flex', gap: 6 }}>
        <button style={subTabStyle(0)} onClick={() => setSubTab(0)}>Overview</button>
        <button style={subTabStyle(1)} onClick={() => setSubTab(1)}>Trade Table ({tradeCount})</button>
      </div>

      {subTab === 0 && (
        <>
          {/* ── Comparison hero cards ─────────────────────────────── */}
          <div>
            <div style={{ color: C.t3, fontSize: 11, marginBottom: 10, textTransform: 'uppercase', letterSpacing: 1 }}>
              Blind Entry vs Smart Entry — All {summary.total} Signals
            </div>
            <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
              <ExecCompareCard
                label="ALL SIGNALS"
                total={summary.total} taken={summary.taken}
                blindWr={summary.blind_win_rate} smartWr={summary.smart_win_rate}
                blindAvg={summary.blind_avg_pnl} smartAvg={summary.smart_avg_pnl}
              />
              {summary.per_ticker.map(tk => (
                <ExecCompareCard
                  key={tk.ticker} label={tk.ticker}
                  total={tk.total} taken={tk.taken}
                  blindWr={tk.blind_win_rate} smartWr={tk.smart_win_rate}
                  blindAvg={tk.blind_avg_pnl} smartAvg={tk.smart_avg_pnl}
                />
              ))}
            </div>
          </div>

          {/* ── Annualized projection banner ─────────────────────── */}
          {(() => {
            // Approx trades/year = taken trades / 2.33 years (2024–Apr 2026)
            const tradesPerYear = Math.round(summary.taken / 2.33)
            const annualBlind = summary.blind_avg_pnl * tradesPerYear
            const annualSmart = summary.smart_avg_pnl * tradesPerYear
            return (
              <div style={{ background: `linear-gradient(135deg, ${C.violet}18, ${C.green}12)`,
                            border: `1px solid ${C.violet}40`, borderRadius: 12, padding: '16px 20px' }}>
                <div style={{ color: C.t3, fontSize: 11, marginBottom: 10, textTransform: 'uppercase', letterSpacing: 1 }}>
                  Annualized Return Projection (arithmetic, per-trade avg × trades/year)
                </div>
                <div style={{ display: 'flex', gap: 32, flexWrap: 'wrap' }}>
                  <div>
                    <div style={{ color: C.t3, fontSize: 11 }}>Blind Entry (~{tradesPerYear} trades/yr)</div>
                    <div style={{ color: annualBlind >= 0 ? C.sky : C.red, fontSize: 28, fontWeight: 800, lineHeight: 1.1 }}>
                      {annualBlind >= 0 ? '+' : ''}{annualBlind.toFixed(0)}%
                    </div>
                    <div style={{ color: C.t3, fontSize: 11 }}>{summary.blind_avg_pnl >= 0 ? '+' : ''}{summary.blind_avg_pnl.toFixed(2)}% avg/trade</div>
                  </div>
                  <div style={{ width: 1, background: C.border }} />
                  <div>
                    <div style={{ color: C.t3, fontSize: 11 }}>Smart Entry (~{Math.round(summary.taken / 2.33)} trades/yr)</div>
                    <div style={{ color: C.green, fontSize: 28, fontWeight: 800, lineHeight: 1.1 }}>
                      +{annualSmart.toFixed(0)}%
                    </div>
                    <div style={{ color: C.t3, fontSize: 11 }}>{summary.smart_avg_pnl >= 0 ? '+' : ''}{summary.smart_avg_pnl.toFixed(2)}% avg/trade</div>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', paddingLeft: 8 }}>
                    <div style={{ color: C.t3, fontSize: 10 }}>Avg hold</div>
                    <div style={{ color: C.amber, fontSize: 16, fontWeight: 700 }}>5.1 days</div>
                    <div style={{ color: C.t3, fontSize: 10 }}>per trade</div>
                  </div>
                </div>
                <div style={{ marginTop: 10, color: C.t3, fontSize: 10 }}>
                  Note: arithmetic projection, not compounded. Position sizing and capital allocation determine actual portfolio return.
                  TP wins average +5.08%, SL losses average -2.75%.
                </div>
              </div>
            )
          })()}

          {/* ── Key metrics strip ─────────────────────────────────── */}
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            {[
              { label: 'Trades Taken', val: `${summary.taken} / ${summary.total}`, sub: `${summary.taken_pct}%`, color: C.sky },
              { label: 'Trades Filtered Out', val: summary.skipped, sub: `${(100 - summary.taken_pct).toFixed(1)}%`, color: C.amber },
              { label: 'Avg P&L / Trade', val: `${summary.smart_avg_pnl >= 0 ? '+' : ''}${summary.smart_avg_pnl.toFixed(2)}%`, sub: `vs blind ${summary.blind_avg_pnl >= 0 ? '+' : ''}${summary.blind_avg_pnl.toFixed(2)}%`, color: summary.smart_avg_pnl >= 0 ? C.green : C.red },
              { label: 'Smart Win Rate', val: `${summary.smart_win_rate.toFixed(1)}%`, sub: `vs blind ${summary.blind_win_rate.toFixed(1)}%`, color: C.violet },
              { label: 'NIFTY Weak Days', val: summary.nifty_weak_days, sub: `${(summary.nifty_weak_days / summary.total * 100).toFixed(1)}% of signals`, color: C.orange },
            ].map(m => (
              <div key={m.label} style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: '12px 18px', minWidth: 140 }}>
                <div style={{ color: C.t3, fontSize: 10, marginBottom: 4 }}>{m.label}</div>
                <div style={{ color: m.color, fontSize: 20, fontWeight: 700 }}>{m.val}</div>
                <div style={{ color: C.t3, fontSize: 11 }}>{m.sub}</div>
              </div>
            ))}
          </div>

          {/* ── Execution distribution bar ────────────────────────── */}
          <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 18 }}>
            <div style={{ color: C.t3, fontSize: 11, marginBottom: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
              Execution Decision Distribution
            </div>
            <ExecDistBar
              total={summary.total}
              items={summary.exec_distribution.map(d => ({
                label: EXEC_CODE_LABELS[d.exec_code] || d.exec_code,
                count: d.count,
                color: EXEC_CODE_COLORS[d.exec_code] || C.t3,
              }))}
            />
          </div>

          {/* ── Gap category breakdown ────────────────────────────── */}
          {comparison && (
            <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 18 }}>
              <div style={{ color: C.t3, fontSize: 11, marginBottom: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
                Performance by Gap Category
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr>
                      {['Gap Category', 'Total', 'Blind Win%', 'Smart Win%', 'Blind Avg P&L', 'Smart Avg P&L', 'Δ Win%'].map(h => (
                        <th key={h} style={th}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {comparison.by_gap_category.map(r => {
                      const wrDelta = r.smart_wr != null ? r.smart_wr - r.blind_wr : null
                      return (
                        <tr key={r.gap_category} style={{ background: 'transparent' }}>
                          <td style={{ ...cell, color: GAP_CAT_COLORS[r.gap_category!] || C.t, fontWeight: 600 }}>
                            {r.gap_category}
                          </td>
                          <td style={{ ...cell, color: C.t2 }}>{r.total}</td>
                          <td style={{ ...cell, color: C.t }}>{r.blind_wr?.toFixed(1)}%</td>
                          <td style={{ ...cell, color: r.smart_wr != null && r.smart_wr > r.blind_wr ? C.green : C.amber }}>
                            {r.smart_wr != null ? r.smart_wr.toFixed(1) + '%' : '—'}
                          </td>
                          <td style={{ ...cell, color: (r.blind_avg || 0) >= 0 ? C.green : C.red }}>
                            {r.blind_avg != null ? ((r.blind_avg >= 0 ? '+' : '') + r.blind_avg.toFixed(2) + '%') : '—'}
                          </td>
                          <td style={{ ...cell, color: r.smart_avg != null ? (r.smart_avg >= 0 ? C.green : C.red) : C.t3 }}>
                            {r.smart_avg != null ? ((r.smart_avg >= 0 ? '+' : '') + r.smart_avg.toFixed(2) + '%') : '—'}
                          </td>
                          <td style={{ ...cell, color: wrDelta != null ? (wrDelta > 0 ? C.green : C.red) : C.t3 }}>
                            {wrDelta != null ? (wrDelta > 0 ? '▲' : '▼') + Math.abs(wrDelta).toFixed(1) + ' pts' : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Exec-code deep-dive table ─────────────────────────── */}
          {comparison && (
            <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 18 }}>
              <div style={{ color: C.t3, fontSize: 11, marginBottom: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
                Performance by Execution Code
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr>
                      {['Exec Decision', 'Total', 'Taken', 'Blind Win%', 'Smart Win%', 'Blind Avg P&L', 'Smart Avg P&L'].map(h => (
                        <th key={h} style={th}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {comparison.by_exec_code.map(r => (
                      <tr key={r.exec_code}>
                        <td style={{ ...cell, color: EXEC_CODE_COLORS[r.exec_code!] || C.t, fontWeight: 600 }}>
                          {EXEC_CODE_LABELS[r.exec_code!] || r.exec_code}
                        </td>
                        <td style={{ ...cell, color: C.t2 }}>{r.total}</td>
                        <td style={{ ...cell, color: C.t2 }}>{r.taken ?? '—'}</td>
                        <td style={{ ...cell, color: C.t }}>{r.blind_wr?.toFixed(1)}%</td>
                        <td style={{ ...cell, color: r.smart_wr != null && r.smart_wr > r.blind_wr ? C.green : C.amber }}>
                          {r.smart_wr != null ? r.smart_wr.toFixed(1) + '%' : '—'}
                        </td>
                        <td style={{ ...cell, color: (r.blind_avg || 0) >= 0 ? C.green : C.red }}>
                          {r.blind_avg != null ? ((r.blind_avg >= 0 ? '+' : '') + r.blind_avg.toFixed(2) + '%') : '—'}
                        </td>
                        <td style={{ ...cell, color: r.smart_avg != null ? (r.smart_avg >= 0 ? C.green : C.red) : C.t3 }}>
                          {r.smart_avg != null ? ((r.smart_avg >= 0 ? '+' : '') + r.smart_avg.toFixed(2) + '%') : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Monthly trend ──────────────────────────────────────── */}
          {comparison && comparison.monthly_trend.length > 0 && (
            <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: 18 }}>
              <div style={{ color: C.t3, fontSize: 11, marginBottom: 14, textTransform: 'uppercase', letterSpacing: 1 }}>
                Monthly Blind vs Smart Avg P&amp;L
              </div>
              <div style={{ display: 'flex', gap: 4, alignItems: 'flex-end', flexWrap: 'wrap' }}>
                {comparison.monthly_trend.map(m => {
                  const maxAbs = Math.max(...comparison.monthly_trend.map(x =>
                    Math.max(Math.abs(x.blind_avg), Math.abs(x.smart_avg ?? 0))), 1)
                  const bH = Math.abs(m.blind_avg) / maxAbs * 60
                  const sH = m.smart_avg != null ? Math.abs(m.smart_avg) / maxAbs * 60 : 0
                  return (
                    <div key={m.month} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
                      <div style={{ display: 'flex', gap: 2, alignItems: 'flex-end' }}>
                        <div title={`Blind: ${m.blind_avg >= 0 ? '+' : ''}${m.blind_avg}%`}
                          style={{ width: 10, height: bH, minHeight: 2,
                                   background: m.blind_avg >= 0 ? C.sky : C.red, borderRadius: '2px 2px 0 0' }} />
                        {m.smart_avg != null && (
                          <div title={`Smart: ${m.smart_avg >= 0 ? '+' : ''}${m.smart_avg}%`}
                            style={{ width: 10, height: sH, minHeight: 2,
                                     background: m.smart_avg >= 0 ? C.green : C.orange, borderRadius: '2px 2px 0 0' }} />
                        )}
                      </div>
                      <div style={{ fontSize: 9, color: C.t3, transform: 'rotate(-45deg)', whiteSpace: 'nowrap' }}>{m.month.slice(2)}</div>
                    </div>
                  )
                })}
              </div>
              <div style={{ display: 'flex', gap: 16, marginTop: 10 }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: C.t2 }}>
                  <span style={{ display: 'inline-block', width: 10, height: 10, background: C.sky, borderRadius: 2 }} /> Blind (positive)
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: C.t2 }}>
                  <span style={{ display: 'inline-block', width: 10, height: 10, background: C.green, borderRadius: 2 }} /> Smart (positive)
                </span>
              </div>
            </div>
          )}
        </>
      )}

      {subTab === 1 && (
        <>
          {/* ── Filters ──────────────────────────────────────────── */}
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
            <select value={codeFilter} onChange={e => setCodeFilter(e.target.value)}
              style={{ background: C.card, color: C.t, border: `1px solid ${C.border}`, borderRadius: 6, padding: '5px 10px', fontSize: 12 }}>
              <option value="">All Exec Codes</option>
              {Object.entries(EXEC_CODE_LABELS).map(([code, label]) => (
                <option key={code} value={code}>{label}</option>
              ))}
            </select>
            <select value={takenFilter} onChange={e => setTakenFilter(e.target.value as any)}
              style={{ background: C.card, color: C.t, border: `1px solid ${C.border}`, borderRadius: 6, padding: '5px 10px', fontSize: 12 }}>
              <option value="">All Trades</option>
              <option value="1">Taken Only</option>
              <option value="0">Filtered (No Trade)</option>
            </select>
            <button onClick={load}
              style={{ background: `${C.violet}22`, color: C.violet, border: `1px solid ${C.violet}55`,
                       borderRadius: 6, padding: '5px 14px', cursor: 'pointer', fontSize: 12 }}>
              Apply
            </button>
            <span style={{ color: C.t3, fontSize: 11 }}>{tradeCount} rows</span>
          </div>

          {/* ── Trade table ──────────────────────────────────────── */}
          <div style={{ overflowX: 'auto', background: C.card, borderRadius: 10, border: `1px solid ${C.border}` }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr>
                  {['Ticker', 'Dir', 'Signal Date', 'Entry Date', 'Exec Decision',
                    'Gap%', 'Day Move%', 'Entry Window',
                    'Blind Entry', 'Smart Entry', 'Exit',
                    'Blind P&L%', 'Smart P&L%', 'Δ P&L'].map(h => (
                    <th key={h} style={th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {trades.map(tr => {
                  const taken = tr.trade_taken
                  const pnlDelta = tr.pnl_improvement
                  return (
                    <tr key={tr.id}>
                      <td style={{ ...cell, color: C.t, fontWeight: 600 }}>{tr.ticker}</td>
                      <td style={{ ...cell, color: tr.direction === 'long' ? C.green : C.red }}>
                        {tr.direction === 'long' ? '↑' : '↓'} {tr.direction}
                      </td>
                      <td style={{ ...cell, color: C.t3 }}>{tr.signal_date}</td>
                      <td style={{ ...cell, color: C.t3 }}>{tr.entry_date}</td>
                      <td style={{ ...cell, color: EXEC_CODE_COLORS[tr.exec_code] || C.t, fontWeight: 500 }}>
                        {EXEC_CODE_LABELS[tr.exec_code] || tr.exec_code}
                      </td>
                      <td style={{ ...cell, color: tr.gap_pct > 0.5 ? C.green : tr.gap_pct < -0.5 ? C.red : C.t3 }}>
                        {tr.gap_pct >= 0 ? '+' : ''}{tr.gap_pct.toFixed(2)}%
                      </td>
                      <td style={{ ...cell, color: tr.day_move_pct > 0 ? C.green : C.red }}>
                        {tr.day_move_pct >= 0 ? '+' : ''}{tr.day_move_pct.toFixed(2)}%
                      </td>
                      <td style={{ ...cell, color: taken ? C.sky : C.t3 }}>
                        {tr.entry_window || '—'}
                      </td>
                      <td style={{ ...cell, color: C.t2 }}>₹{tr.blind_entry_price?.toFixed(2)}</td>
                      <td style={{ ...cell, color: taken ? C.violet : C.t3 }}>
                        {tr.smart_entry_price != null ? '₹' + tr.smart_entry_price.toFixed(2) : '—'}
                      </td>
                      <td style={{ ...cell, color: C.t2 }}>₹{tr.exit_price?.toFixed(2)}</td>
                      <td style={{ ...cell, color: tr.blind_pnl_pct >= 0 ? C.green : C.red, fontWeight: 600 }}>
                        {tr.blind_pnl_pct >= 0 ? '+' : ''}{tr.blind_pnl_pct.toFixed(2)}%
                      </td>
                      <td style={{ ...cell, color: tr.smart_pnl_pct != null ? (tr.smart_pnl_pct >= 0 ? C.green : C.red) : C.t3, fontWeight: 600 }}>
                        {tr.smart_pnl_pct != null ? (tr.smart_pnl_pct >= 0 ? '+' : '') + tr.smart_pnl_pct.toFixed(2) + '%' : '—'}
                      </td>
                      <td style={{ ...cell, color: pnlDelta != null ? (pnlDelta >= 0 ? C.green : C.red) : C.t3 }}>
                        {pnlDelta != null ? (pnlDelta >= 0 ? '▲+' : '▼') + pnlDelta.toFixed(2) + '%' : '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function AnalysisPage() {
  const [activeTab,     setActiveTab]     = useState(0)
  const [ticker,        setTicker]        = useState('ALL')
  const [year,          setYear]          = useState('ALL')
  const [tickers,       setTickers]       = useState<string[]>(['ALL'])
  const [tickerSearch,  setTickerSearch]  = useState('')
  const [swingOverview, setSwingOverview] = useState<SwingOverviewResponse | null>(null)
  const [loading,       setLoading]       = useState(true)
  const [error,         setError]         = useState<string | null>(null)

  const load = useCallback(() => {
    setLoading(true)
    Promise.all([
      getSwingOverview(year !== 'ALL' ? year : undefined),
      getSwingTickers(),
    ])
      .then(([overview, tkrs]) => {
        setSwingOverview(overview)
        setTickers(['ALL', ...tkrs.tickers])
        setLoading(false)
      })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [year])

  useEffect(() => { load() }, [load])

  // filtered ticker list for the search dropdown
  const filteredTickers = useMemo(() => {
    const q = tickerSearch.trim().toUpperCase()
    if (!q) return tickers
    return tickers.filter(t => t === 'ALL' || t.includes(q))
  }, [tickers, tickerSearch])

  const tabBtn = (i: number) => ({
    padding: '8px 18px', borderRadius: 6, cursor: 'pointer', fontSize: 12, fontWeight: 600,
    background: activeTab === i ? `${C.violet}22` : 'transparent',
    color: activeTab === i ? C.violet : C.t3,
    border: `1px solid ${activeTab === i ? C.violet + '55' : 'transparent'}`,
  })

  const filterBtn = (val: string, active: string, set: (v: string) => void, color = C.violet) => ({
    padding: '5px 12px', borderRadius: 5, cursor: 'pointer', fontSize: 11, fontWeight: 600,
    background: active === val ? `${color}22` : 'transparent',
    color: active === val ? color : C.t3,
    border: `1px solid ${active === val ? color + '55' : C.border}`,
  })

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.t, fontFamily: 'Inter, system-ui, sans-serif' }}>
      <div style={{ maxWidth: activeTab === 4 ? 'none' : 1500, margin: '0 auto', padding: activeTab === 4 ? '0 16px 32px' : '0 20px 60px' }}>

        {/* Header */}
        <div style={{ padding: '24px 0 18px', borderBottom: `1px solid ${C.border}` }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ color: C.violet, fontWeight: 800, fontSize: 18, letterSpacing: '-0.02em' }}>KANIDA</span>
            <span style={{ color: C.t3, fontSize: 13 }}>.AI</span>
            <span style={{ color: C.t, fontSize: 14, fontWeight: 700, marginLeft: 6, letterSpacing: '0.04em' }}>SWING TRADING TERMINAL</span>
          </div>
          <div style={{ color: C.t3, fontSize: 11, marginTop: 3 }}>
            Long-only · Cash Equity · NSE · 1D Daily · Smart Entry via Execution IQ ·
            2024 / 2025 / 2026 Backtest · scalable to 200+ stocks
          </div>
        </div>

        {/* Filters */}
        <div style={{ display: 'flex', gap: 20, padding: '14px 0', flexWrap: 'wrap', alignItems: 'center' }}>
          {/* Stock filter — searchable dropdown for 200+ tickers */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span style={{ color: C.t3, fontSize: 10 }}>STOCK</span>
            <div style={{ position: 'relative' }}>
              <input
                type="text"
                placeholder={ticker === 'ALL' ? 'ALL stocks' : ticker}
                value={tickerSearch}
                onChange={e => setTickerSearch(e.target.value)}
                onFocus={e => (e.target as HTMLInputElement).select()}
                style={{
                  background: C.card, border: `1px solid ${ticker !== 'ALL' ? C.violet + '88' : C.border}`,
                  borderRadius: 6, padding: '5px 10px', color: ticker !== 'ALL' ? C.violet : C.t2,
                  fontSize: 12, fontWeight: 600, width: 130, outline: 'none',
                }}
              />
              {tickerSearch && (
                <div style={{
                  position: 'absolute', top: '100%', left: 0, zIndex: 200,
                  background: '#12121e', border: `1px solid ${C.border}`,
                  borderRadius: 8, marginTop: 4, maxHeight: 260, overflowY: 'auto',
                  minWidth: 150, boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
                }}>
                  {filteredTickers.slice(0, 50).map(t => (
                    <div
                      key={t}
                      onClick={() => { setTicker(t); setTickerSearch('') }}
                      style={{
                        padding: '7px 12px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
                        color: t === ticker ? C.violet : C.t2,
                        background: t === ticker ? `${C.violet}18` : 'transparent',
                      }}
                      onMouseEnter={e => (e.currentTarget.style.background = `${C.violet}10`)}
                      onMouseLeave={e => (e.currentTarget.style.background = t === ticker ? `${C.violet}18` : 'transparent')}
                    >{t}</div>
                  ))}
                  {filteredTickers.length === 0 && (
                    <div style={{ padding: '8px 12px', color: C.t3, fontSize: 11 }}>No match</div>
                  )}
                </div>
              )}
            </div>
            {/* Quick-clear button when a specific stock is selected */}
            {ticker !== 'ALL' && (
              <button
                onClick={() => { setTicker('ALL'); setTickerSearch('') }}
                style={{
                  background: `${C.violet}22`, border: `1px solid ${C.violet}44`,
                  borderRadius: 5, color: C.violet, fontSize: 11, fontWeight: 700,
                  padding: '4px 8px', cursor: 'pointer',
                }}
              >{ticker} ✕</button>
            )}
            <span style={{ color: C.t3, fontSize: 10 }}>
              {tickers.length > 1 ? `${tickers.length - 1} stocks` : 'loading...'}
            </span>
          </div>
          {activeTab !== 4 && activeTab !== 5 && (
            <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              <span style={{ color: C.t3, fontSize: 10, marginRight: 2 }}>YEAR</span>
              {YEARS.map(y => <button key={y} onClick={() => setYear(y)} style={filterBtn(y, year, setYear, C.amber)}>{y}</button>)}
            </div>
          )}
        </div>

        {/* Tab bar */}
        <div style={{ display: 'flex', gap: 6, marginBottom: 24 }}>
          {TABS.map((t, i) => <button key={t} onClick={() => setActiveTab(i)} style={tabBtn(i)}>{t}</button>)}
        </div>

        {/* Error */}
        {error && (
          <div style={{
            color: C.red, background: `${C.red}11`, border: `1px solid ${C.red}33`,
            borderRadius: 8, padding: '12px 16px', marginBottom: 16, fontSize: 12,
          }}>
            {error} — check that the backend is running at localhost:8000
          </div>
        )}

        {loading && activeTab === 0 ? (
          <div style={{ color: C.t3, padding: 40, textAlign: 'center' }}>Loading swing terminal data...</div>
        ) : (
          <>
            {activeTab === 0 && swingOverview && (
              <SwingOverviewTab data={swingOverview} onTickerClick={setTicker} year={year} />
            )}
            {activeTab === 1 && <TradeLogTab ticker={ticker} year={year} />}
            {activeTab === 2 && <CombinationsTab ticker={ticker} />}
            {activeTab === 3 && <MpiTab ticker={ticker} />}
            {activeTab === 4 && <LiveTradesTab ticker={ticker} />}
            {activeTab === 5 && <ExecutionIQTab ticker={ticker} />}
          </>
        )}
      </div>
    </div>
  )
}
