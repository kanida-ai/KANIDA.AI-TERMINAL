'use client'

import React, { useEffect, useState } from 'react'
import { getCombinations, type Combination } from '@/lib/backtest-api'
import { C, Chip, pct, pctS } from './_shared'

export function CombinationsWorkspace({ ticker }: { ticker: string }) {
  const [data, setData]       = useState<{ count: number; combinations: Combination[] } | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)

  useEffect(() => {
    setLoading(true); setError(null)
    getCombinations(ticker !== 'ALL' ? ticker : undefined)
      .then(d => setData(d))
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [ticker])

  if (loading) return (
    <div style={{ padding: '24px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace', fontSize: 12 }}>
      Loading pattern combinations…
    </div>
  )
  if (error) return (
    <div style={{ padding: '24px', color: C.red, fontSize: 12, fontFamily: 'IBM Plex Mono, monospace' }}>
      Failed to load: {error}
    </div>
  )
  if (!data || data.combinations.length === 0)
    return <div style={{ color: C.t3, padding: 20 }}>No combination data found.</div>

  const catColor = (cat: string) =>
    cat === 'turbo' ? C.amber : cat === 'super' ? C.green : cat === 'trap' ? C.red : C.sky

  return (
    <div style={{ padding: '16px 20px' }}>
      <div style={{ color: C.t3, fontSize: 11, marginBottom: 14, fontFamily: 'IBM Plex Mono, monospace' }}>
        {data.count} pattern combinations ranked by win rate · top 3 atoms shown per group
      </div>
      <div style={{ background: C.card, border: `1px solid ${C.border}`, overflowX: 'auto', overflowY: 'auto', maxHeight: '72vh' }}>
        <table style={{ minWidth: 'max-content', width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead style={{ position: 'sticky', top: 0, background: C.card }}>
            <tr style={{ borderBottom: `1px solid ${C.border}` }}>
              {['#','Signal Type','Pattern Atoms (top 3)','Stocks','Trades','Win%','Avg Return','Category'].map(h => (
                <th key={h} style={{ padding: '9px 12px', color: C.t3, fontWeight: 600, textAlign: 'left', fontSize: 10, letterSpacing: '0.04em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.combinations.map((c, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${C.border}` }}>
                <td style={{ padding: '8px 12px', color: C.t3, fontFamily: 'IBM Plex Mono, monospace' }}>{i + 1}</td>
                <td style={{ padding: '8px 12px', color: C.amber, fontWeight: 600 }}>{c.signal_type}</td>
                <td style={{ padding: '8px 12px', color: C.t2, maxWidth: 360, fontSize: 10 }}>
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={c.full_pattern}>
                    {c.pattern}
                  </div>
                </td>
                <td style={{ padding: '8px 12px', color: C.t3, fontSize: 10 }}>{c.tickers?.join(', ')}</td>
                <td style={{ padding: '8px 12px', color: C.t, fontFamily: 'IBM Plex Mono, monospace' }}>{c.total}</td>
                <td style={{ padding: '8px 12px', fontWeight: 700, fontFamily: 'IBM Plex Mono, monospace',
                  color: c.win_rate >= 55 ? C.green : c.win_rate >= 35 ? C.amber : C.red }}>
                  {pct(c.win_rate)}
                </td>
                <td style={{ padding: '8px 12px', color: c.avg_return >= 0 ? C.green : C.red, fontFamily: 'IBM Plex Mono, monospace' }}>
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
