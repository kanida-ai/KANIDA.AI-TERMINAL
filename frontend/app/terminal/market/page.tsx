'use client'

import React, { useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { getScreener, getSnapshotStatus, type ScreenerRow, type SnapshotStatusRow } from '@/lib/api'
import { T, brand } from '@/lib/theme'
import {
  chipStyle,
  MetricCard,
  panelStyle,
  pageShellStyle,
  SectionEyebrow,
} from '@/lib/terminal-ui'

function cleanText(text: string | null | undefined): string {
  if (!text) return ''
  return text
    .replace(/\u00e2\u20ac\u201d/g, '\u2014')
    .replace(/\u00e2\u20ac\u2013/g, '\u2013')
    .replace(/\u00e2\u20ac\u0153/g, '\u201c')
    .replace(/\u00e2\u20ac\u2122/g, '\u2019')
    .replace(/\u00e2\u20ac\u00a2/g, '\u2022')
    .replace(/\u00e2\u2020\u2019/g, '\u2192')
    .replace(/\u00c2\u00b7/g, '\u00b7')
    .replace(/\u00c2[\u0080-\u00bf\u00a0]/g, '')
    .replace(/\u00c2/g, '')
    .replace(/\s+/g, ' ')
    .trim()
}

export default function MarketPage() {
  const [rows, setRows] = useState<ScreenerRow[]>([])
  const [bias, setBias] = useState<'bullish' | 'bearish'>('bullish')
  const [market, setMarket] = useState<'NSE' | 'US'>('NSE')
  const [loading, setLoading] = useState(true)
  const [snapshotRow, setSnapshotRow] = useState<SnapshotStatusRow | null>(null)

  useEffect(() => {
    setLoading(true)
    getScreener(market, bias)
      .then(setRows)
      .catch(() => setRows([]))
      .finally(() => setLoading(false))
    getSnapshotStatus()
      .then(status => {
        const row = status.snapshots.find(s => s.market === market && s.bias === bias) || null
        setSnapshotRow(row)
      })
      .catch(() => setSnapshotRow(null))
  }, [market, bias])

  const top = useMemo(() => rows[0] || null, [rows])

  return (
    <div style={pageShellStyle()}>
      <nav style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '12px 24px', borderBottom: `1px solid ${T.b}` }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
          <span style={{ fontSize: 18, fontWeight: 900, letterSpacing: '-.04em' }}>
            {brand.name}
            <span style={{ color: T.g }}>{brand.ai}</span>
          </span>
          <span style={{ fontSize: 10, letterSpacing: '.14em', color: T.t3, textTransform: 'uppercase' }}>{brand.mode}</span>
        </div>
        <Link href="/terminal" style={{ ...chipStyle(), textDecoration: 'none' }}>
          Back to feed
        </Link>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {(['NSE', 'US'] as const).map((value) => (
            <button key={value} onClick={() => setMarket(value)} style={{ ...chipStyle(market === value ? 'green' : 'neutral'), cursor: 'pointer' }}>
              {value}
            </button>
          ))}
          {(['bullish', 'bearish'] as const).map((value) => (
            <button key={value} onClick={() => setBias(value)} style={{ ...chipStyle(bias === value ? (value === 'bullish' ? 'green' : 'red') : 'neutral'), cursor: 'pointer' }}>
              {value === 'bullish' ? 'Bullish' : 'Bearish'}
            </button>
          ))}
        </div>
      </nav>

      <div style={{ maxWidth: 1220, margin: '0 auto', padding: '24px 20px 52px' }}>
        <SectionEyebrow>Market</SectionEyebrow>
        <div style={{ fontSize: 42, fontWeight: 900, letterSpacing: '-.05em', lineHeight: 1.03, marginBottom: 12 }}>
          {loading ? 'Ranking the market now.' : rows.length ? `${rows.length} ${bias} ideas ranked by historical edge.` : 'No results for this view.'}
        </div>
        <div style={{ fontSize: 18, color: T.t2, lineHeight: 1.8, maxWidth: 860, marginBottom: 22 }}>
          {loading ? `Loading ${bias} ideas for ${market}.` : rows.length ? `${market} \u2022 sorted by win rate and edge score \u2022 ${rows.filter(r => r.win_rate >= 75).length} names above 75% win rate.` : 'Try switching the market or bias filter above.'}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 14, marginBottom: 18 }}>
          <MetricCard label="High-signal names" value={loading ? '--' : String(rows.filter(r => r.win_rate >= 75).length)} note="Names with a win rate of 75% or above." color={T.g} />
          <MetricCard label="Bias view" value={bias === 'bullish' ? 'Bullish' : 'Bearish'} note={bias === 'bullish' ? 'Scanning for long setups.' : 'Scanning for short setups.'} color={bias === 'bullish' ? T.g : T.r} />
          <MetricCard label="Market" value={market} note={market === 'NSE' ? 'NSE F&O stocks.' : 'US equities.'} />
          <MetricCard
            label="Data freshness"
            value={snapshotRow ? (snapshotRow.stale ? 'Stale' : 'Fresh') : '--'}
            note={snapshotRow?.age_minutes != null ? `${snapshotRow.age_minutes} min ago · ${snapshotRow.stocks} stocks` : 'Checking...'}
            color={snapshotRow?.stale ? T.r : T.g}
          />
        </div>

        {top && !loading && (
          <div
            style={{
              ...panelStyle(22),
              marginBottom: 18,
              background: `linear-gradient(140deg, ${bias === 'bullish' ? 'rgba(0,201,138,0.10)' : 'rgba(255,77,109,0.10)'}, rgba(12,12,24,0.97))`,
            }}
          >
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 }}>
              <span style={chipStyle(bias === 'bullish' ? 'green' : 'red')}>{bias === 'bullish' ? 'Bullish' : 'Bearish'}</span>
              <span style={chipStyle('green')}>Top ranked</span>
            </div>
            <div style={{ fontSize: 32, fontWeight: 900, lineHeight: 1.08, letterSpacing: '-.04em', marginBottom: 10 }}>
              {top.ticker} \u2014 rank #{top.rank} with {top.win_rate}% win rate across {top.signals.toLocaleString()} signals.
            </div>
            <div style={{ fontSize: 16, color: T.t2, lineHeight: 1.8, marginBottom: 16 }}>
              {`${top.conviction} conviction \u2022 ${cleanText(top.regime)} regime \u2022 avg gain ${top.avg_gain >= 0 ? '+' : ''}${top.avg_gain}% \u2022 ${rows.length} names in this scan.`}
            </div>
            <Link href={`/terminal/stock/${top.ticker}?bias=${bias}&market=${market}`} style={{ ...chipStyle(bias === 'bullish' ? 'green' : 'red'), textDecoration: 'none' }}>
              Open {top.ticker}
            </Link>
          </div>
        )}

        <div style={{ ...panelStyle(18), overflow: 'hidden' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1.4fr .8fr .8fr .8fr .8fr', padding: '12px 14px', background: T.s2, fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em' }}>
            <span>Opportunity</span>
            <span>Rank</span>
            <span>Win rate</span>
            <span>Avg gain</span>
            <span>Signals</span>
          </div>

          {loading && (
            <div style={{ padding: '24px 14px', fontSize: 15, color: T.t2 }}>
              Loading the next useful market view...
            </div>
          )}

          {!loading && !rows.length && (
            <div style={{ padding: '24px 14px', fontSize: 15, color: T.t2 }}>
              No rows are available for this view right now.
            </div>
          )}

          {!loading &&
            rows.map((row, index) => (
              <Link key={row.ticker} href={`/terminal/stock/${row.ticker}?bias=${bias}&market=${market}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1.4fr .8fr .8fr .8fr .8fr',
                    padding: '16px 14px',
                    borderTop: index === 0 ? 'none' : `1px solid ${T.b}`,
                    alignItems: 'center',
                    background: index % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)',
                  }}
                >
                  <div>
                    <div style={{ fontFamily: T.mono, fontWeight: 900, color: bias === 'bullish' ? T.g : T.r, marginBottom: 4 }}>{row.ticker}</div>
                    <div style={{ fontSize: 14, color: T.t2 }}>{cleanText(row.conviction)} conviction \u00b7 {cleanText(row.regime)} regime</div>
                  </div>
                  <span style={{ fontFamily: T.mono, fontWeight: 800 }}>#{row.rank}</span>
                  <span style={{ fontFamily: T.mono, fontWeight: 800, color: row.win_rate >= 75 ? T.g : T.a }}>{row.win_rate}%</span>
                  <span style={{ fontFamily: T.mono, fontWeight: 800, color: row.avg_gain >= 0 ? T.g : T.r }}>
                    {row.avg_gain >= 0 ? '+' : ''}
                    {row.avg_gain}%
                  </span>
                  <span style={{ fontFamily: T.mono, fontWeight: 800 }}>{row.signals.toLocaleString()}</span>
                </div>
              </Link>
            ))}
        </div>
      </div>
    </div>
  )
}
