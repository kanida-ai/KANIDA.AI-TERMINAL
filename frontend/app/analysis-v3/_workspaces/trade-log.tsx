'use client'

import React, { useEffect, useState } from 'react'
import { getSwingTrades, type SwingTrade } from '@/lib/backtest-api'
import { C, BUCKET_META, Chip, KV, pct, pctS, price, reasonColor } from './_shared'

export function TradeLogWorkspace({ ticker, year }: { ticker: string; year: string }) {
  const [data, setData]         = useState<{ count: number; trades: SwingTrade[] } | null>(null)
  const [bucket, setBucket]     = useState('ALL')
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState<string | null>(null)
  const [expanded, setExpanded] = useState<number | null>(null)

  useEffect(() => {
    setLoading(true); setError(null)
    getSwingTrades(
      ticker !== 'ALL' ? ticker : undefined,
      bucket !== 'ALL' ? bucket.toLowerCase() : undefined,
      year   !== 'ALL' ? year   : undefined,
    ).then(d => setData(d))
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [ticker, year, bucket])

  const exitColor = (r: string) => r === 'tp' ? C.green : r === 'sl' ? C.red : C.amber

  const bucketBar = (
    <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap' }}>
      {['ALL', 'turbo', 'super', 'standard'].map(b => (
        <button key={b} onClick={() => setBucket(b)} style={{
          padding: '5px 14px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
          fontFamily: 'IBM Plex Mono, monospace', letterSpacing: '0.04em',
          border: `1px solid ${bucket === b ? C.amber : C.border}`,
          background: bucket === b ? `${C.amber}22` : 'transparent',
          color: bucket === b ? C.amber : C.t3,
        }}>
          {BUCKET_META[b]?.icon || ''} {b.toUpperCase()}
        </button>
      ))}
    </div>
  )

  if (loading) return (
    <div style={{ padding: '20px' }}>
      {bucketBar}
      <div style={{ color: C.t3, padding: 20, fontFamily: 'IBM Plex Mono, monospace', fontSize: 12 }}>
        Loading trades…
      </div>
    </div>
  )
  if (error) return (
    <div style={{ padding: '20px' }}>
      {bucketBar}
      <div style={{ color: C.red, padding: 20, fontFamily: 'IBM Plex Mono, monospace', fontSize: 12 }}>
        Failed to load: {error}
      </div>
    </div>
  )
  if (!data || data.trades.length === 0) return (
    <div style={{ padding: '20px' }}>
      {bucketBar}
      <div style={{ color: C.t3, padding: 20 }}>No trades found for these filters.</div>
    </div>
  )

  return (
    <div style={{ padding: '16px 20px' }}>
      {bucketBar}
      <div style={{ color: C.t3, fontSize: 11, marginBottom: 10, fontFamily: 'IBM Plex Mono, monospace' }}>
        {data.count.toLocaleString()} long trades · smart entry price where execution engine fired · click row to expand
      </div>
      <div style={{ background: C.card, border: `1px solid ${C.border}`, overflowX: 'auto', overflowY: 'auto', maxHeight: '70vh' }}>
        <table style={{ minWidth: 'max-content', width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.card, zIndex: 1 }}>
            <tr style={{ borderBottom: `1px solid ${C.border}` }}>
              {[
                'Signal ID','Stock','Type',
                'Signal Date','Entry Date','Delay',
                'Entry ₹','SL ₹','Target ₹','Exec Decision',
                'Exit','Days','P&L','MFE','MAE','MPI','Bucket','Reason',
              ].map(h => (
                <th key={h} style={{ padding: '8px 10px', color: C.t3, fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap', fontSize: 10, letterSpacing: '0.04em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.trades.filter(t => t.bucket?.toLowerCase() !== 'trap').slice(0, 300).map(t => {
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
                      borderBottom: `1px solid ${C.border}`,
                      cursor: 'pointer',
                      background: isEx ? `${C.amber}0a` : 'transparent',
                    }}
                  >
                    <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{signalId}</td>
                    <td style={{ padding: '6px 10px', color: C.amber, fontWeight: 700 }}>{t.ticker}</td>
                    <td style={{ padding: '6px 10px', color: C.t2, maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{t.signal_type || 'AI Pattern'}</td>
                    <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{signalDate}</td>
                    <td style={{ padding: '6px 10px', color: C.t2, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{t.entry_date}</td>
                    <td style={{ padding: '6px 10px', color: C.sky, fontSize: 10 }}>{delayLbl}</td>
                    <td style={{ padding: '6px 10px', fontFamily: 'IBM Plex Mono, monospace' }}>
                      <span style={{ color: isSmart ? C.violet : C.t }}>{price(effEntry)}</span>
                      {isSmart && <span style={{ color: C.t3, fontSize: 9, marginLeft: 3 }}>smart</span>}
                    </td>
                    <td style={{ padding: '6px 10px', color: C.red,   fontFamily: 'IBM Plex Mono, monospace' }}>{price(stopP)}</td>
                    <td style={{ padding: '6px 10px', color: C.green, fontFamily: 'IBM Plex Mono, monospace' }}>{price(targetP)}</td>
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
                    <td style={{ padding: '6px 10px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace' }}>{t.days_held}d</td>
                    <td style={{ padding: '6px 10px', color: (t.effective_pnl ?? 0) >= 0 ? C.green : C.red, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>
                      {pctS(t.effective_pnl ?? t.pnl_pct)}
                    </td>
                    <td style={{ padding: '6px 10px', color: C.green, fontFamily: 'IBM Plex Mono, monospace' }}>{pct(t.mfe_pct)}</td>
                    <td style={{ padding: '6px 10px', color: C.red, fontFamily: 'IBM Plex Mono, monospace' }}>{pct(t.mae_pct)}</td>
                    <td style={{ padding: '6px 10px', color: t.mpi_pct && t.mpi_pct > 0 ? C.violet : C.t3, fontFamily: 'IBM Plex Mono, monospace' }}>
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
                    <tr style={{ background: `${C.amber}06` }}>
                      <td colSpan={18} style={{ padding: '14px 22px' }}>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 22, fontSize: 11 }}>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 8, fontWeight: 600, letterSpacing: '0.08em', fontSize: 10 }}>SIGNAL METADATA</div>
                            <KV label="Signal ID"          val={signalId} />
                            <KV label="Timeframe"          val={tf} color={C.sky} />
                            <KV label="Competing patterns" val={`${mc} on this day`} color={mc > 3 ? C.orange : C.sky} />
                            <KV label="Opportunity score"  val={t.opportunity_score != null ? t.opportunity_score.toFixed(4) : '—'} />
                            <KV label="Reason code"        val={rc || '—'} color={reasonColor(rc)} />
                            <KV label="Tier"               val={t.tier || '—'} />
                            <KV label="Credibility"        val={t.credibility || '—'} />
                          </div>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 8, fontWeight: 600, letterSpacing: '0.08em', fontSize: 10 }}>EXECUTION DETAIL</div>
                            <KV label="Exec decision"  val={t.exec_code || '—'} color={execClr} />
                            <KV label="Trade taken"    val={taken === true ? 'YES' : taken === false ? 'NO' : '—'} color={taken ? C.green : C.red} />
                            <KV label="Entry window"   val={t.entry_window || '—'} color={C.sky} />
                            <KV label="Entry ₹"        val={`₹${price(t.entry_price)}`} />
                            <KV label="Smart entry ₹"  val={t.smart_entry_price ? `₹${price(t.smart_entry_price)}` : '—'} color={C.violet} />
                            <KV label="Gap %"          val={t.gap_pct != null ? pctS(t.gap_pct) : '—'} color={C.t2} />
                            <KV label="RS vs Nifty"    val={t.rs_vs_nifty != null ? pctS(t.rs_vs_nifty) : '—'} color={(t.rs_vs_nifty ?? 0) > 0 ? C.green : C.red} />
                          </div>
                          <div>
                            <div style={{ color: C.t3, marginBottom: 8, fontWeight: 600, letterSpacing: '0.08em', fontSize: 10 }}>P&amp;L BREAKDOWN</div>
                            <KV label="Effective P&L"  val={pctS(t.effective_pnl ?? t.pnl_pct)} color={(t.effective_pnl ?? t.pnl_pct ?? 0) >= 0 ? C.green : C.red} />
                            <KV label="Smart P&L"      val={t.smart_pnl_pct != null ? pctS(t.smart_pnl_pct) : '—'} color={C.violet} />
                            <KV label="R:R ratio"      val={`1:${rr.toFixed(1)}`} />
                            <KV label="Days held"      val={`${t.days_held}d`} />
                            <KV label="Exit"           val={(t.exit_reason || '').toUpperCase()} color={exitColor(t.exit_reason)} />
                            <div style={{ marginTop: 10, color: C.t2, fontSize: 10, lineHeight: 1.6, wordBreak: 'break-word' }}>{t.pattern}</div>
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
