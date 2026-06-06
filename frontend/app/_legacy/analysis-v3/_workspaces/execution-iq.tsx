'use client'

/**
 * ExecutionIQWorkspace — V3 port of /analysis page's ExecutionIQTab.
 * Faithful behavior; visuals to be iterated later.
 */
import React, { useCallback, useEffect, useState } from 'react'
import {
  getExecSummary, getExecComparison, getExecTrades,
  type ExecSummaryResponse, type ExecTrade, type ExecComparisonResponse,
} from '@/lib/backtest-api'
import {
  C, EXEC_CODE_LABELS, EXEC_CODE_COLORS, GAP_CAT_COLORS,
} from './_shared'

// ── Sub-components ────────────────────────────────────────────────────────────

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

// ── Main workspace ────────────────────────────────────────────────────────────

export function ExecutionIQWorkspace({ ticker }: { ticker: string }) {
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
