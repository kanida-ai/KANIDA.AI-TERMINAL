'use client'

import React, { useCallback, useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { getFeed, getHealth, type FeedCard, type Health } from '@/lib/api'
import { T, brand } from '@/lib/theme'
import {
  AmbientOrb,
  chipStyle,
  GlowDivider,
  MetricCard,
  panelStyle,
  pageShellStyle,
  SearchBar,
  SectionEyebrow,
  TinyWave,
  useBreakpoint,
} from '@/lib/terminal-ui'

type FilterKey = 'all' | 'high' | 'bullish' | 'bearish'

const FILTERS: { key: FilterKey; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'high', label: 'High signal' },
  { key: 'bullish', label: 'Bullish' },
  { key: 'bearish', label: 'Bearish' },
]

const TIMEFRAME: Record<string, string> = {
  BREAKOUT_READY: 'Daily + Weekly',
  BASE_FORMING: 'Weekly',
  VOLUME_DRY: 'Daily',
  PEAK_FALL: 'Weekly',
  HIGH_EDGE: 'Daily + Weekly',
}

const SIGNAL_LABELS: Record<string, string> = {
  BREAKOUT_READY: 'Breakout Ready',
  BASE_FORMING: 'Base Forming',
  VOLUME_DRY: 'Volume Dry-Up',
  PEAK_FALL: 'Peak Fall',
  HIGH_EDGE: 'High Edge',
}

function toPercent(value: number) {
  return `${Math.round(value * 100)}%`
}

function formatSignedPercent(value: number) {
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${value.toFixed(2)}%`
}

function formatCount(value: number) {
  return value.toLocaleString()
}

// Mojibake repair table — patterns built from code points, no literal multibyte chars in source.
// CP1252 misread of UTF-8: each [search, replacement] restores the intended Unicode character.
// Mojibake repair — patterns built via String.fromCodePoint so no literal
// multibyte chars appear in source. Formatter-safe: code points are ASCII integers.
const _MJ: [RegExp, string][] = [
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x201d), 'g'), '—'], // em dash
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x2013), 'g'), '–'], // en dash
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x02dc), 'g'), '‘'], // left single quote
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x2122), 'g'), '’'], // right single quote
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x0153), 'g'), '“'], // left double quote
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x00a2), 'g'), '•'], // bullet
  [new RegExp(String.fromCodePoint(0x00e2, 0x20ac, 0x00a6), 'g'), '…'], // ellipsis
  [new RegExp(String.fromCodePoint(0x00e2, 0x2020, 0x2019), 'g'), '→'], // arrow
  [new RegExp(String.fromCodePoint(0x00c2, 0x00b7), 'g'), '·'], // middle dot
  [new RegExp(String.fromCodePoint(0x00c2) + '[' + String.fromCharCode(128) + '-' + String.fromCharCode(191) + String.fromCharCode(160) + ']', 'g'), ''], // stray continuation
  [new RegExp(String.fromCodePoint(0x00c2), 'g'), ''], // bare stray Â
  // collapse whitespace runs — built from char codes, no backslash-s needed
  [new RegExp('[' + String.fromCharCode(9, 10, 11, 12, 13, 32) + ']+', 'g'), ' '],
]
function cleanFeedText(text: string | null | undefined): string {
  if (!text) return ''
  let s = text
  for (const [pat, rep] of _MJ) s = s.replace(pat, rep)
  return s.trim()
}
function toneFromBias(card: FeedCard) {
  return card.primaryBias === 'bullish' ? 'green' : 'red'
}

function convictionTone(conviction: string) {
  return conviction === 'HIGH' ? 'green' : conviction === 'MEDIUM' ? 'amber' : 'neutral'
}

function convictionLabel(conviction: string) {
  return conviction === 'HIGH' ? 'High Signal' : conviction === 'MEDIUM' ? 'Building' : 'Watch'
}

function narrative(cards: FeedCard[]): string {
  if (!cards.length) return 'No opportunities in the current scan.'
  const high   = cards.filter((c) => c.conviction === 'HIGH').length
  const bull   = cards.filter((c) => c.primaryBias === 'bullish').length
  const bear   = cards.filter((c) => c.primaryBias === 'bearish').length
  const regime = cards[0]?.regime || 'UNKNOWN'
  return (
    regime + ' regime' +
    ' · ' + high + ' high-conviction' +
    ' · ' + bull + ' bullish / ' + bear + ' bearish' +
    ' · ' + cards.length + ' total'
  )
}

function FeaturedHero({
  health,
  cards,
  loading,
  isDesktop,
}: {
  health: Health | null
  cards: FeedCard[]
  loading: boolean
  isDesktop: boolean
}) {
  const lead = cards[0]
  const highSignal = cards.filter((card) => card.conviction === 'HIGH').length
  const bullish = cards.filter((card) => card.primaryBias === 'bullish').length
  const bearish = cards.filter((card) => card.primaryBias === 'bearish').length
  const regime = lead?.regime || 'MIXED'
  const regimeTone = regime === 'BEAR' ? T.r : regime === 'BULL' ? T.g : T.a
  const topHeadline = cleanFeedText(lead?.headline)
  const topSubline = cleanFeedText(lead?.subline)
  const topReason = cleanFeedText(lead?.triggerReason)

  return (
    <div
      style={{
        ...panelStyle(0),
        minHeight: 320,
        overflow: 'hidden',
        position: 'relative',
        background: 'linear-gradient(150deg, rgba(0,201,138,0.12), rgba(16,16,34,0.92) 42%, rgba(255,209,102,0.07))',
      }}
    >
      <AmbientOrb color={T.g} top={-10} right={40} size={180} />
      <AmbientOrb color={T.a} top={120} right={220} size={150} />
      <div
        style={{
          position: 'relative',
          zIndex: 1,
          padding: isDesktop ? 36 : 24,
          display: 'grid',
          gridTemplateColumns: isDesktop ? 'minmax(0, 1.4fr) minmax(340px, .95fr)' : '1fr',
          gap: 24,
        }}
      >
        <div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
            <span style={chipStyle('green')}>{loading ? 'Scanning now' : `${cards.length} live opportunities`}</span>
            <span style={chipStyle('amber')}>{loading ? 'Refreshing' : `${highSignal} high-signal ideas`}</span>
            <span style={chipStyle(regime === 'BEAR' ? 'red' : regime === 'BULL' ? 'green' : 'amber')}>{regime} regime</span>
          </div>

          <div style={{ fontSize: isDesktop ? 44 : 34, fontWeight: 900, letterSpacing: '-.05em', lineHeight: 1.02, marginBottom: 14 }}>
            {loading ? 'Loading feed...' : lead ? topHeadline || lead.ticker : 'Feed updating now.'}
          </div>

          <div style={{ fontSize: isDesktop ? 17 : 16, color: T.t2, lineHeight: 1.78, maxWidth: isDesktop ? 980 : '100%', marginBottom: 18 }}>
            {loading
              ? 'Fetching live data...'
              : lead
                ? `${topReason}${topReason && topSubline ? ' • ' : ''}${topSubline}`
                : 'No results. Backend may be refreshing data.'}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: isDesktop ? 'repeat(3, minmax(0, 1fr))' : '1fr', gap: 12 }}>
            <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.025)' }}>
              <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.08em', fontWeight: 700, marginBottom: 10 }}>Top setup</div>
              <div style={{ fontSize: 18, fontWeight: 800, marginBottom: 8 }}>{lead ? lead.ticker : 'Refreshing'}</div>
              <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.65 }}>
                {lead ? `${SIGNAL_LABELS[lead.signalType] || lead.signalType} \u2022 ${toPercent(lead.avgWinRate)} win rate` : '--'}
              </div>
            </div>
            <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.025)' }}>
              <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.08em', fontWeight: 700, marginBottom: 10 }}>Signal sample</div>
              <div style={{ fontSize: 18, fontWeight: 800, marginBottom: 8 }}>{lead ? formatCount(lead.totalSignals) : '--'}</div>
              <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.65 }}>
                {lead ? `${TIMEFRAME[lead.signalType] || 'Multi-timeframe'} \u2022 ${convictionLabel(lead.conviction)}` : '--'}
              </div>
            </div>
            <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.025)' }}>
              <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.08em', fontWeight: 700, marginBottom: 10 }}>Market balance</div>
              <div style={{ fontFamily: T.mono, fontSize: 24, fontWeight: 900, color: regimeTone, marginBottom: 8 }}>{regime}</div>
              <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.65 }}>{bullish} bullish ideas and {bearish} bearish ideas are in the current feed.</div>
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, alignSelf: 'stretch' }}>
          <MetricCard label="Opportunities" value={loading ? '...' : String(cards.length)} note={loading ? 'Refreshing current feed.' : `${bullish} bullish / ${bearish} bearish in view.`} color={T.g} />
          <MetricCard label="High signal" value={loading ? '...' : String(highSignal)} note={loading ? 'Ranking strongest setups.' : `${cards.length ? Math.round((highSignal / cards.length) * 100) : 0}% of the live feed is high conviction.`} color={T.a} />
          <MetricCard
            label="Data depth"
            value={health ? health.fingerprints.toLocaleString() : 'Live'}
            note={health ? `${health.tickers.toLocaleString()} tickers • ${health.snapshots.toLocaleString()} snapshots` : '--'}
          />
          <MetricCard
            label="Data freshness"
            value={health?.snapshot_stale ? 'Stale' : health?.snapshot_age_minutes != null ? `${health.snapshot_age_minutes}m ago` : lead?.snapshotDate || '--'}
            note={health?.snapshot_stale ? 'Run snapshot build to refresh data.' : health ? `${health.snapshots.toLocaleString()} snapshots \u2022 ${health.paper_trades.toLocaleString()} trades` : 'Loading freshness'}
            color={health?.snapshot_stale ? T.r : T.g}
          />
        </div>
      </div>
    </div>
  )
}

function OpportunityCard({
  card,
  open,
  onToggle,
  onDeepen,
}: {
  card: FeedCard
  open: boolean
  onToggle: () => void
  onDeepen: () => void  // caller must bake bias+market into the route
}) {
  const bullish = card.primaryBias === 'bullish'
  const accent = bullish ? T.g : T.r
  const timeframe = TIMEFRAME[card.signalType] || 'Multi-timeframe'
  const cleanedHeadline = cleanFeedText(card.headline)
  const cleanedSubline = cleanFeedText(card.subline)
  const cleanedReason = cleanFeedText(card.triggerReason)
  const outcomeLabel = formatSignedPercent(card.avgOutcome)
  const signalLabel = SIGNAL_LABELS[card.signalType] || card.signalType

  return (
    <div style={{ ...panelStyle(0), overflow: 'hidden', borderLeft: `3px solid ${accent}` }}>
      <button
        onClick={onToggle}
        style={{
          width: '100%',
          background: 'none',
          border: 'none',
          textAlign: 'left',
          cursor: 'pointer',
          padding: 22,
          fontFamily: 'inherit',
        }}
      >
          <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: 18, alignItems: 'flex-start', marginBottom: 14 }}>
            <div style={{ minWidth: 0 }}>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', marginBottom: 10 }}>
                <div style={{ fontFamily: T.mono, fontWeight: 900, fontSize: 20, color: accent }}>{card.ticker}</div>
                <span style={chipStyle(toneFromBias(card))}>{bullish ? 'Bullish' : 'Bearish'}</span>
                <span style={chipStyle(convictionTone(card.conviction))}>{convictionLabel(card.conviction)}</span>
                <span style={chipStyle()}>{timeframe}</span>
                {card.isCounterTrend && (
                  <span style={{ ...chipStyle('amber'), fontWeight: 800 }} title={`Dominant historical edge is ${card.dominantBias} (${Math.round(card.dominantWinRate * 100)}% win rate). Today's signal runs against it.`}>
                    Counter-Trend
                  </span>
                )}
              </div>
            <div style={{ fontSize: 24, fontWeight: 850, lineHeight: 1.18, letterSpacing: '-.03em', marginBottom: 12 }}>{cleanedHeadline}</div>
            <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.7, maxWidth: 860 }}>{cleanedSubline}</div>
          </div>
          <div style={{ textAlign: 'right', minWidth: 200 }}>
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
              <TinyWave bullish={bullish} />
            </div>
            <div style={{ fontFamily: T.mono, fontSize: 20, fontWeight: 900, color: accent }}>{toPercent(card.avgWinRate)}</div>
            <div style={{ fontSize: 13, color: T.t2, marginTop: 4, fontWeight: 700 }}>historical win rate</div>
            {card.price != null && <div style={{ fontFamily: T.mono, fontSize: 14, color: T.t2, marginTop: 12 }}>Rs {card.price.toLocaleString()}</div>}
            <div style={{ fontSize: 13, color: T.t3, marginTop: 4 }}>{card.snapshotDate || 'Fresh today'}</div>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 6, fontWeight: 700 }}>
              Here is one clear reason this is interesting
            </div>
            <div style={{ fontSize: 15, color: T.t, lineHeight: 1.6 }}>{cleanedReason}</div>
          </div>
          <span style={chipStyle(open ? toneFromBias(card) : 'neutral')}>{open ? 'Hide details' : 'Tap to see more'}</span>
        </div>
      </button>

      {open && (
        <div style={{ borderTop: `1px solid ${T.b}`, padding: '20px 22px 22px', background: 'rgba(255,255,255,0.02)' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1.05fr .95fr', gap: 20 }}>
            <div>
              <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.75, marginBottom: 14 }}>
                {cleanedSubline}
              </div>
              <div style={{ ...panelStyle(16), background: 'rgba(255,255,255,0.02)', marginBottom: 14 }}>
                <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 8, fontWeight: 700 }}>Win rate</div>
                <div style={{ fontSize: 28, fontWeight: 800, color: accent, marginBottom: 6 }}>{toPercent(card.avgWinRate)}</div>
                <div style={{ height: 6, borderRadius: 4, background: 'rgba(255,255,255,0.07)', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${Math.min(card.avgWinRate, 100)}%`, background: accent, borderRadius: 4, transition: 'width .4s ease' }} />
                </div>
                <div style={{ fontSize: 12, color: T.t2, marginTop: 6 }}>Edge score {card.edgeScore} · {formatCount(card.totalSignals)} signals</div>
              </div>
              <div style={{ display: 'grid', gap: 10 }}>
                {[
                  `${signalLabel} setup with ${toPercent(card.avgWinRate)} historical win rate and ${formatCount(card.totalSignals)} signals in sample.`,
                  `${bullish ? 'Bullish' : 'Bearish'} bias with ${outcomeLabel} average outcome across the historical sample.`,
                  `Snapshot date ${card.snapshotDate} • regime ${card.regime} • regime score ${card.regimeScore}.`,
                ].map((line) => (
                  <div key={line} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                    <span style={{ width: 8, height: 8, borderRadius: 999, background: accent, marginTop: 7, flexShrink: 0 }} />
                    <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.72 }}>{line}</div>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <MetricCard label="Signal type" value={signalLabel} note={timeframe} color={accent} />
              <MetricCard label="Conviction" value={convictionLabel(card.conviction)} note={`Edge score ${card.edgeScore}`} color={card.conviction === 'HIGH' ? T.g : T.a} />
              <MetricCard label="Average outcome" value={outcomeLabel} note={`${formatCount(card.totalSignals)} total signals`} />
              <MetricCard label="Trade side" value={bullish ? 'Long bias' : 'Short bias'} note={`Regime ${card.regime}`} />
            </div>
          </div>

          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginTop: 18 }}>
            <button
              onClick={onDeepen}
              style={{
                border: 'none',
                background: accent,
                color: bullish ? '#000' : '#fff',
                padding: '12px 20px',
                borderRadius: 12,
                fontSize: 14,
                fontWeight: 800,
                cursor: 'pointer',
              }}
            >
              Open full analysis
            </button>
            <Link
              href={`/terminal/chat?q=${encodeURIComponent(`Tell me about ${card.ticker} and why it matters now`)}`}
              style={{ ...chipStyle(), textDecoration: 'none', background: 'rgba(255,255,255,0.03)' }}
            >
              Ask KANIDA.AI
            </Link>
          </div>
        </div>
      )}
    </div>
  )
}

export default function TerminalHome() {
  const router = useRouter()
  const bp = useBreakpoint()
  const [cards, setCards] = useState<FeedCard[]>([])
  const [health, setHealth] = useState<Health | null>(null)
  const [market, setMarket] = useState<'NSE' | 'US'>('NSE')
  const [loading, setLoading] = useState(true)
  const [query, setQuery] = useState('')
  const [filter, setFilter] = useState<FilterKey>('all')
  const [openCard, setOpenCard] = useState<string | null>(null)

  const loadFeed = useCallback((nextMarket: string) => {
    setLoading(true)
    getFeed(nextMarket, 20)
      .then((rows) => {
        setCards(rows)
        setOpenCard(rows[0]?.ticker ?? null)
      })
      .catch(() => {
        setCards([])
        setOpenCard(null)
      })
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    getHealth().then(setHealth).catch(() => null)
  }, [])

  useEffect(() => {
    loadFeed(market)
  }, [market, loadFeed])

  useEffect(() => {
    const id = setInterval(() => loadFeed(market), 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [market, loadFeed])

  const filtered = useMemo(() => {
    return cards.filter((card) => {
      if (filter === 'high') return card.conviction === 'HIGH'
      if (filter === 'bullish') return card.primaryBias === 'bullish'
      if (filter === 'bearish') return card.primaryBias === 'bearish'
      return true
    })
  }, [cards, filter])

  const topCard = filtered[0] || null
  const restCards = filtered.slice(1)
  const maxWidth = bp.isWide ? 1660 : bp.isDesktop ? 1480 : 1220

  return (
    <div style={pageShellStyle()}>
      <nav
        style={{
          borderBottom: `1px solid ${T.b}`,
          padding: bp.isMobile ? '14px 16px' : '16px 28px',
        }}
      >
        <div
          style={{
            width: '100%',
            maxWidth,
            margin: '0 auto',
            display: 'flex',
            alignItems: 'center',
            gap: 12,
          }}
        >
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <span style={{ fontSize: bp.isMobile ? 16 : 18, fontWeight: 900, letterSpacing: '-.04em' }}>
              {brand.name}
              <span style={{ color: T.g }}>{brand.ai}</span>
            </span>
            {!bp.isMobile && <span style={{ fontSize: 11, letterSpacing: '.12em', color: T.t2, textTransform: 'uppercase', fontWeight: 700 }}>{brand.mode}</span>}
          </div>

          <div style={{ display: 'flex', gap: 4, background: T.s2, borderRadius: 999, padding: 4, marginLeft: bp.isMobile ? 0 : 8 }}>
            {(['NSE', 'US'] as const).map((value) => (
              <button
                key={value}
                onClick={() => setMarket(value)}
                style={{
                  border: 'none',
                  background: market === value ? T.s3 : 'transparent',
                  color: market === value ? T.t : T.t3,
                  borderRadius: 999,
                  padding: '8px 13px',
                  fontSize: 13,
                  fontWeight: 700,
                  cursor: 'pointer',
                }}
              >
                {value}
              </button>
            ))}
          </div>

          <Link href="/terminal/market" style={{ marginLeft: 'auto', textDecoration: 'none', ...chipStyle() }}>
            Market
          </Link>
          <Link href="/terminal/chat" style={{ textDecoration: 'none', ...chipStyle('green') }}>
            Open chat
          </Link>
        </div>
      </nav>

      <div style={{ width: '100%', maxWidth, margin: '0 auto', padding: bp.isMobile ? '22px 16px 56px' : '30px 28px 72px' }}>
        <SectionEyebrow>Terminal</SectionEyebrow>
        <div style={{ fontSize: bp.isMobile ? 32 : 42, fontWeight: 900, letterSpacing: '-.05em', lineHeight: 1.03, marginBottom: 12, maxWidth: 1200 }}>
            {loading ? market + ' — loading...' : cards.length ? cleanFeedText(cards[0].headline) || cards[0].ticker : 'Feed updating.'}
        </div>
        <div style={{ fontSize: bp.isMobile ? 16 : 18, color: T.t2, lineHeight: 1.75, maxWidth: 1120, marginBottom: 24 }}>
          {loading ? 'Fetching live data...' : narrative(cards)}
        </div>

        <div style={{ marginBottom: 18 }}>
          <SearchBar
            value={query}
            onChange={setQuery}
            onSubmit={() => router.push(query.trim() ? `/terminal/chat?q=${encodeURIComponent(query)}` : '/terminal/chat')}
            placeholder="Ask about any stock, strategy, or market setup..."
          />
        </div>

        <div style={{ marginBottom: 26 }}>
          <FeaturedHero health={health} cards={cards} loading={loading} isDesktop={bp.isDesktop} />
        </div>

        <GlowDivider />

        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, alignItems: 'flex-end', flexWrap: 'wrap', margin: '20px 0 14px' }}>
          <div>
            <div style={{ fontSize: bp.isMobile ? 24 : 30, fontWeight: 850, letterSpacing: '-.03em', marginBottom: 6 }}>
              {filtered.length ? `${filtered.length} ${filter === 'all' ? '' : filter + ' '}opportunities ranked by edge.` : 'No opportunities match this filter.'}
            </div>
            <div style={{ fontSize: 16, color: T.t2, lineHeight: 1.7 }}>{filtered.length ? `${market} · sorted by conviction and win rate.` : `Switch to All or Bullish to see the current ${market} scan.`}</div>
            </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {FILTERS.map((item) => {
              const active = filter === item.key
              const tone = item.key === 'bearish' ? 'red' : item.key === 'high' ? 'green' : active ? 'green' : 'neutral'
              return (
                <button key={item.key} onClick={() => setFilter(item.key)} style={{ ...chipStyle(active ? tone : 'neutral'), cursor: 'pointer', background: active ? chipStyle(tone).background : 'rgba(255,255,255,0.03)' }}>
                  {item.label}
                </button>
              )
            })}
          </div>
        </div>

        <div style={{ display: 'grid', gap: 14 }}>
          {loading && [0, 1, 2].map((i) => <div key={i} style={{ ...panelStyle(22), opacity: 0.4, minHeight: 120 }} />)}

          {!loading && !filtered.length && (
            <div style={{ ...panelStyle(28), textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 800, marginBottom: 8 }}>
                {filter === 'bearish' ? `0 bearish ideas in the current ${market} scan.` :
                 filter === 'bullish' ? `0 bullish ideas in the current ${market} scan.` :
                 filter === 'high'    ? `0 high-conviction ideas in the current ${market} scan.` :
                 `No ideas in the current ${market} scan.`}
              </div>
              <div style={{ fontSize: 15, color: T.t2 }}>
                {cards.length ? `${cards.length} total ideas are available — try a different filter.` : 'Snapshot may be building. Check back in a moment.'}
              </div>
            </div>
          )}

          {!loading && topCard && (
            <OpportunityCard
              card={topCard}
              open={openCard === topCard.ticker}
              onToggle={() => setOpenCard(openCard === topCard.ticker ? null : topCard.ticker)}
              onDeepen={() => router.push(`/terminal/stock/${topCard.ticker}?signal=${topCard.signalType}&bias=${topCard.primaryBias}&market=${topCard.market}`)}
            />
          )}

          {!loading &&
            restCards.map((card) => (
              <OpportunityCard
                key={card.ticker}
                card={card}
                open={openCard === card.ticker}
                onToggle={() => setOpenCard(openCard === card.ticker ? null : card.ticker)}
                onDeepen={() => router.push(`/terminal/stock/${card.ticker}?signal=${card.signalType}&bias=${card.primaryBias}&market=${card.market}`)}
              />
            ))}
        </div>
      </div>
    </div>
  )
}
