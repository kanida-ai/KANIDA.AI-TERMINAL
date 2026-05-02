'use client'

import React, { useEffect, useState } from 'react'
import {
  getMissedProfit, getMpiRecommendations,
  type MpiTrade, type MpiRecommendation,
} from '@/lib/backtest-api'
import { C, Chip, StatBox, pct, pctS, price } from './_shared'

export function MpiWorkspace({ ticker }: { ticker: string }) {
  const [mpiData,  setMpiData]  = useState<{ count: number; avg_missed_pct: number; trades: MpiTrade[] } | null>(null)
  const [recData,  setRecData]  = useState<{ count: number; recommendations: MpiRecommendation[] } | null>(null)
  const [subTab,   setSubTab]   = useState<'events' | 'recommendations'>('recommendations')
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState<string | null>(null)

  useEffect(() => {
    setLoading(true); setError(null)
    const tk = ticker !== 'ALL' ? ticker : undefined
    Promise.all([getMissedProfit(tk), getMpiRecommendations(tk)])
      .then(([m, r]) => { setMpiData(m); setRecData(r) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [ticker])

  if (loading) return (
    <div style={{ padding: 24, color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 12 }}>
      Loading MPI analysis…
    </div>
  )
  if (error) return (
    <div style={{ padding: 24, color: C.red, fontFamily: 'IBM Plex Mono, monospace', fontSize: 12 }}>
      Failed to load: {error}
    </div>
  )

  const actionColor = (code: string) =>
    code === 'trailing_stop' ? C.violet : code === 'extend_target' ? C.amber : C.t3
  const actionIcon = (code: string) =>
    code === 'trailing_stop' ? '◎' : code === 'extend_target' ? '↗' : '✓'

  return (
    <div style={{ padding: '16px 20px' }}>
      {/* Explainer */}
      <div style={{
        background: `${C.amber}0a`, border: `1px solid ${C.amber}33`,
        padding: '14px 18px', marginBottom: 20, fontSize: 12, color: C.t2, lineHeight: 1.7,
      }}>
        <div style={{ color: C.amber, fontWeight: 700, marginBottom: 4, letterSpacing: '0.08em' }}>MISSED PROFIT INDEX (MPI)</div>
        The MPI measures how much additional gain was available <em>after</em> the engine booked its target.
        A trade booking +5% while the stock continued to +22% has an MPI of 17%. High MPI signals
        are candidates for <strong>trailing stops</strong> or <strong>extended targets</strong> —
        the fixed TP is leaving significant edge on the table.
      </div>

      {mpiData && mpiData.count > 0 && (
        <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap' }}>
          <StatBox label="MPI EVENTS"        value={mpiData.count.toLocaleString()} sub="TP trades w/ >2% continuation" />
          <StatBox label="AVG MISSED GAIN"   value={pct(mpiData.avg_missed_pct)} color={C.violet} sub="per MPI event" />
          <StatBox label="RECOMMENDATIONS"   value={String(recData?.count || 0)} color={C.amber} sub="signal types needing action" />
        </div>
      )}

      {/* Sub-tab toggle */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 18 }}>
        {(['recommendations', 'events'] as const).map(s => (
          <button key={s} onClick={() => setSubTab(s)} style={{
            padding: '6px 16px', cursor: 'pointer', fontSize: 11, fontWeight: 600,
            fontFamily: 'IBM Plex Mono, monospace', letterSpacing: '0.06em',
            border: `1px solid ${subTab === s ? C.amber : C.border}`,
            background: subTab === s ? `${C.amber}22` : 'transparent',
            color: subTab === s ? C.amber : C.t3,
          }}>
            {s === 'recommendations' ? '◎ NEXT STEPS' : '☰ EVENT LOG'}
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
                  padding: '16px 20px',
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 10 }}>
                    <div>
                      <span style={{ color: C.amber, fontWeight: 700, fontSize: 13 }}>{r.ticker}</span>
                      <span style={{ color: C.t3, fontSize: 12, marginLeft: 10 }}>{r.signal_type}</span>
                    </div>
                    <Chip val={`${actionIcon(r.action_code)} ${r.action}`} color={actionColor(r.action_code)} />
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 12, marginBottom: 12 }}>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.06em' }}>TP TRADES</div>
                      <div style={{ color: C.t, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>{r.total_tp_trades}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.06em' }}>AVG BOOKED</div>
                      <div style={{ color: C.green, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(r.avg_booked_pct)}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.06em' }}>AVG CONT (MPI)</div>
                      <div style={{ color: C.violet, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>{pct(r.avg_mpi_pct)}</div>
                    </div>
                    <div>
                      <div style={{ color: C.t3, fontSize: 10, letterSpacing: '0.06em' }}>HIGH MPI RATE</div>
                      <div style={{ color: C.amber, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>{pct(r.high_mpi_rate)}</div>
                    </div>
                  </div>
                  <div style={{ color: C.t2, fontSize: 11, lineHeight: 1.7, marginBottom: 10 }}>{r.rationale}</div>
                  <div style={{
                    background: `${actionColor(r.action_code)}0a`, border: `1px solid ${actionColor(r.action_code)}33`,
                    padding: '8px 14px', display: 'flex', gap: 24, flexWrap: 'wrap',
                  }}>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Current avg gain: <strong style={{ color: C.green, fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(r.avg_booked_pct)}</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Simulated with {r.action_code === 'trailing_stop' ? 'trailing stop' : '1.5× target'}:{' '}
                      <strong style={{ color: actionColor(r.action_code), fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(r.simulated_avg_pnl)}</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Extra gain per trade: <strong style={{ color: C.amber, fontFamily: 'IBM Plex Mono, monospace' }}>+{r.extra_gain_pct.toFixed(2)}%</strong>
                    </span>
                    <span style={{ color: C.t3, fontSize: 11 }}>
                      Max single continuation: <strong style={{ color: C.violet, fontFamily: 'IBM Plex Mono, monospace' }}>{pct(r.max_mpi_pct)}</strong>
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
              background: C.card, border: `1px solid ${C.border}`,
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
                      <th key={h} style={{ padding: '8px 10px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 10, whiteSpace: 'nowrap', letterSpacing: '0.04em' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {mpiData.trades.map((t, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${C.border}` }}>
                      <td style={{ padding: '7px 10px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{t.signal_id}</td>
                      <td style={{ padding: '7px 10px', color: C.amber, fontWeight: 600 }}>{t.ticker}</td>
                      <td style={{ padding: '7px 10px' }}><Chip val={t.timeframe} color={C.sky} small /></td>
                      <td style={{ padding: '7px 10px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{t.signal_datetime}</td>
                      <td style={{ padding: '7px 10px', color: C.t2, fontFamily: 'IBM Plex Mono, monospace', fontSize: 10 }}>{t.entry_datetime}</td>
                      <td style={{ padding: '7px 10px' }}>
                        <Chip val={t.direction === 'long' ? 'LONG' : 'SHORT'} color={t.direction === 'long' ? C.green : C.red} small />
                      </td>
                      <td style={{ padding: '7px 10px', color: C.t, fontFamily: 'IBM Plex Mono, monospace' }}>{price(t.entry_price)}</td>
                      <td style={{ padding: '7px 10px', color: C.t, fontFamily: 'IBM Plex Mono, monospace' }}>{price(t.exit_price)}</td>
                      <td style={{ padding: '7px 10px', color: C.green, fontWeight: 600, fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(t.booked_pct)}</td>
                      <td style={{ padding: '7px 10px', color: C.sky, fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(t.continued_pct)}</td>
                      <td style={{ padding: '7px 10px', color: C.amber, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>{pctS(t.total_available)}</td>
                      <td style={{ padding: '7px 10px', color: C.red, fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace' }}>-{pct(t.missed_pct)}</td>
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
