'use client'

import React, { useEffect, useState } from 'react'
import Link from 'next/link'
import { useParams, useSearchParams } from 'next/navigation'
import { getTicker, sendChat, type ChatResponse, type TickerData } from '@/lib/api'
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
} from '@/lib/terminal-ui'

// ── Data-native hero helpers ─────────────────────────────────────────────────
// Build hero body directly from live TickerData — no canned templates.
function heroBody(
  ticker: string,
  isBull: boolean,
  regime: string,
  regimeScore: number | undefined,
  activeSide: { avg_win_rate?: number | null; total_signals?: number | null; avg_gain?: number | null; conviction?: string } | null | undefined,
): string {
  const parts: string[] = []
  const side = isBull ? 'bullish' : 'bearish'
  if (activeSide?.avg_win_rate != null && activeSide?.total_signals != null) {
    parts.push(activeSide.avg_win_rate + '% win rate across ' + activeSide.total_signals.toLocaleString() + ' ' + side + ' signals.')
  }
  if (activeSide?.avg_gain != null) {
    const g = activeSide.avg_gain
    parts.push('Average outcome ' + (g > 0 ? '+' : '') + g.toFixed(1) + '% per signal.')
  }
  const rscore = regimeScore != null ? ' (score ' + regimeScore + ')' : ''
  parts.push(regime + ' regime' + rscore + '.')
  if (!parts.length) return ticker + ' is in the current scan. Review the sections below for full context.'
  return parts.join(' ')
}

// Build 3 prompts purely from live data — no canned signal-type lookup.
function livePrompts(
  ticker: string,
  isBull: boolean,
  avgWinRate?: number | null,
  target1?: number | null,
): string[] {
  const side = isBull ? 'bullish' : 'bearish'
  return [
    'What is the most important thing to know about ' + ticker + ' right now?',
    avgWinRate != null
      ? 'How reliable is the ' + side + ' signal on ' + ticker + ' at ' + avgWinRate + '% win rate?'
      : 'What data backs the current view on ' + ticker + '?',
    target1 != null
      ? 'Walk me through the price levels on ' + ticker
      : 'What would change the current view on ' + ticker + '?',
  ]
}

// ── Signal type → display label + plain-English description ─────────────────
// Lives here, not inline in JSX — add new signal types as the backend adds them.
const SIGNAL_META: Record<string, { label: string; description: string }> = {
  PEAK_FALL:      { label: 'Peak Fall',      description: 'price drops after hitting a peak' },
  BREAKOUT_READY: { label: 'Breakout Ready', description: 'price moves higher after breaking resistance' },
  BASE_FORMING:   { label: 'Base Forming',   description: 'price consolidates before a directional move' },
  VOLUME_DRY:     { label: 'Volume Dry-Up',  description: 'volume contracts before a momentum expansion' },
  HIGH_EDGE:      { label: 'High Edge',      description: 'historically high-probability setup in current conditions' },
}

function sectionButtonStyle(open: boolean): React.CSSProperties {
  return {
    width: '100%',
    background: 'none',
    border: 'none',
    textAlign: 'left',
    padding: '18px 22px',
    cursor: 'pointer',
    fontFamily: 'inherit',
    display: 'flex',
    justifyContent: 'space-between',
    gap: 14,
    alignItems: 'center',
  }
}

function InfoSection({
  open,
  onToggle,
  title,
  copy,
  children,
}: {
  open: boolean
  onToggle: () => void
  title: string
  copy: string
  children: React.ReactNode
}) {
  return (
    <div style={{ ...panelStyle(0), overflow: 'hidden' }}>
      <button onClick={onToggle} style={sectionButtonStyle(open)}>
        <div>
          <div style={{ fontSize: 24, fontWeight: 800, marginBottom: 6 }}>{title}</div>
          <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.7 }}>{copy}</div>
        </div>
        <span style={chipStyle(open ? 'green' : 'neutral')}>{open ? 'Open' : 'Reveal'}</span>
      </button>
      {open && <div style={{ borderTop: `1px solid ${T.b}`, padding: '18px 22px 22px' }}>{children}</div>}
    </div>
  )
}

function formatPct(value: number | null | undefined, showPlus = true) {
  if (value == null) return 'N/A'
  const prefix = value > 0 && showPlus ? '+' : ''
  return `${prefix}${value}%`
}

function levelNarrative(isBull: boolean) {
  return isBull
    ? 'Targets should sit above current price and the stop should sit below as protection.'
    : 'Targets should sit below current price and the stop should sit above as invalidation.'
}

export default function StockPage() {
  const params = useParams()
  const searchParams = useSearchParams()
  const ticker     = typeof params.ticker === 'string' ? params.ticker.toUpperCase() : ''
  const signalType = searchParams?.get('signal') || 'HIGH_EDGE'
  const market     = searchParams?.get('market') || 'NSE'
  // bias from URL wins — it carries the exact side the user clicked in the feed.
  // Falls back to ticker's default (primary_bias / levels_bias) when not provided.
  const biasParam  = searchParams?.get('bias') || null  // 'bullish' | 'bearish' | null

  const [data, setData] = useState<TickerData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [openSections, setOpenSections] = useState<Set<string>>(new Set(['story', 'plan']))
  const [chatOpen, setChatOpen] = useState(false)
  const [msgs, setMsgs] = useState<{ role: string; text: string }[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [history, setHistory] = useState<{ role: string; content: string }[]>([])
  const [tradeLoading, setTradeLoading] = useState(false)
  const [tradeHistory, setTradeHistory] = useState<Record<string, unknown>[]>([])
  const [openTrade, setOpenTrade] = useState<Record<string, unknown> | null>(null)

  const toggle = (id: string) =>
    setOpenSections((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })

  useEffect(() => {
    if (!ticker) return
    getTicker(ticker, market)
      .then((result) => {
        setData(result)
        const side = result.primary_bias === 'bullish' ? result.bullish : result.bearish
        const winRateHint = side?.avg_win_rate != null ? ` Historical win rate sits at ${side.avg_win_rate}%.` : ''
        setMsgs([
          {
            role: 'assistant',
            text: `${ticker} is loaded.${winRateHint} Ask about why it matters now, what supports it, or what the next useful level is.`,
          },
        ])
      })
      .catch(() => setError(`${ticker} could not be loaded from KANIDA.AI.`))
      .finally(() => setLoading(false))

    const api = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'
    fetch(`${api}/api/ticker/${ticker}/history?market=${market}&limit=5`)
      .then((res) => (res.ok ? res.json() : null))
      .then((result) => {
        if (result?.rows) setTradeHistory(result.rows)
      })
      .catch(() => null)
  }, [ticker])

  const send = async (text: string) => {
    if (!text.trim() || sending || !data) return
    setMsgs((prev) => [...prev, { role: 'user', text }])
    setInput('')
    setSending(true)
    try {
      const response: ChatResponse = await sendChat(text, history, ticker, data.market || 'NSE')
      setMsgs((prev) => [...prev, { role: 'assistant', text: response.response }])
      setHistory((prev) => [...prev, { role: 'user', content: text }, { role: 'assistant', content: response.response }])
    } catch {
      setMsgs((prev) => [...prev, { role: 'assistant', text: 'Something went wrong. Please try again.' }])
    } finally {
      setSending(false)
    }
  }

  const startPaperTrade = async () => {
    if (!ticker || tradeLoading || !data) return
    const api = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'
    setTradeLoading(true)
    try {
      const res = await fetch(`${api}/api/ticker/${ticker}/paper-trade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ market: data.market || 'NSE' }),
      })
      if (res.ok) setOpenTrade(await res.json())
    } catch {
      setOpenTrade(null)
    } finally {
      setTradeLoading(false)
    }
  }

  if (loading) {
    return (
      <div style={pageShellStyle()}>
        <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div style={{ ...panelStyle(24), textAlign: 'center' }}>
            <div style={{ fontSize: 24, fontWeight: 800, marginBottom: 8 }}>Loading {ticker}</div>
            <div style={{ fontSize: 15, color: T.t2 }}>KANIDA.AI is preparing the next useful layer.</div>
          </div>
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div style={pageShellStyle()}>
        <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div style={{ ...panelStyle(24), textAlign: 'center', maxWidth: 520 }}>
            <div style={{ fontSize: 24, fontWeight: 800, marginBottom: 8 }}>This stock could not be loaded.</div>
            <div style={{ fontSize: 15, color: T.t2, marginBottom: 16 }}>{error || 'Please return to the feed and try another opportunity.'}</div>
            <Link href="/terminal" style={{ ...chipStyle('green'), textDecoration: 'none' }}>
              Back to feed
            </Link>
          </div>
        </div>
      </div>
    )
  }

  // Bias resolution priority (strict):
  //  1. URL param — user explicitly clicked a specific side in the feed
  //  2. active_signal.bias — what Bot 2 detected in today's snapshot
  //  3. levels_bias / primary_bias — ticker-level default fallback
  const isBull = biasParam
    ? biasParam === 'bullish'
    : data.active_signal
      ? data.active_signal.bias === 'bullish'
      : (data.levels?.levels_bias || data.primary_bias) === 'bullish'
  const accent = isBull ? T.g : T.r
  const regime = data.regime?.regime || 'UNKNOWN'
  const regimeColor = regime === 'BEAR' ? T.r : regime === 'BULL' ? T.g : T.a
  const activeSide = isBull ? data.bullish : data.bearish
  const lvl = data.levels
  const biasPatterns = isBull ? (data.bullish?.patterns || []) : (data.bearish ? [] : [])
  const _heroBody   = heroBody(ticker, isBull, regime, data.regime?.regime_score, activeSide)
  const prompts = livePrompts(ticker, isBull, activeSide?.avg_win_rate, lvl?.target_1)

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
        <span style={{ ...chipStyle(isBull ? 'green' : 'red') }}>{ticker}</span>
        <button
          onClick={() => setChatOpen((prev) => !prev)}
          style={{ marginLeft: 'auto', border: 'none', cursor: 'pointer', ...chipStyle(chatOpen ? 'green' : 'neutral') }}
        >
          {chatOpen ? 'Close chat' : 'Ask KANIDA.AI'}
        </button>
      </nav>

      <div style={{ maxWidth: 1220, margin: '0 auto', padding: '24px 20px 52px' }}>
        <SectionEyebrow>Stock Detail</SectionEyebrow>
        <div
          style={{
            ...panelStyle(26),
            marginBottom: 18,
            background: `linear-gradient(160deg, ${isBull ? 'rgba(0,201,138,0.10)' : 'rgba(255,77,109,0.10)'}, rgba(10,10,20,0.99))`,
            position: 'relative',
            overflow: 'hidden',
          }}
        >
          {/* Row 1 — badge strip + live price */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', marginBottom: 14 }}>
            <span style={chipStyle(isBull ? 'green' : 'red')}>{isBull ? 'Bullish' : 'Bearish'}</span>
            <span style={chipStyle()}>
              {SIGNAL_META[signalType]?.label || signalType.replace(/_/g, ' ')}
              {SIGNAL_META[signalType]?.description ? ` \u2014 ${SIGNAL_META[signalType].description}` : ''}
            </span>
            <span style={chipStyle('green')}>Active</span>
            <span style={{ marginLeft: 'auto', fontFamily: T.mono, fontSize: 15, color: T.t2, display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: accent, display: 'inline-block' }} />
              {ticker}
              {lvl?.price != null ? ` Rs ${lvl.price.toLocaleString()}` : ''}
            </span>
          </div>

          {/* Row 2 — main title */}
          <div style={{ fontSize: 36, fontWeight: 900, lineHeight: 1.08, letterSpacing: '-.04em', maxWidth: 820, marginBottom: 10 }}>
            {ticker}: {isBull ? 'Bullish' : 'Bearish'} signal active
          </div>

          {/* Row 3 — plain-English explanation built entirely from live data */}
          <div style={{ fontSize: 17, color: T.t2, lineHeight: 1.8, maxWidth: 840, marginBottom: 20 }}>
            {(() => {
              const sm   = SIGNAL_META[signalType]
              const name = sm?.label || signalType.replace(/_/g, ' ')
              const desc = sm?.description || 'a setup has been detected in the current data'
              const wr   = activeSide?.avg_win_rate != null ? `${activeSide.avg_win_rate}%` : null
              const sig  = activeSide?.total_signals?.toLocaleString() || null
              const gain = activeSide?.avg_gain
              const move = gain != null ? `${gain > 0 ? '+' : ''}${gain.toFixed(1)}%` : null
              const wrPart   = wr && sig ? ` In past data, it worked ${wr} of the time across ${sig} signals.` : ''
              const movePart = move ? ` After the signal triggered, price moved ${move} on average from the signal price.` : ''
              return `This \u201c${name}\u201d pattern means ${desc}.${wrPart}${movePart}`
            })()}
          </div>

          {/* Row 4 — 4 trader-readable cards, all from live data */}
          {(() => {
            const _as  = data.active_signal
            const _wr  = activeSide?.avg_win_rate != null ? activeSide.avg_win_rate : null

            // Card 2: real avg win / avg loss from paper_ledger
            const _avgWin  = _as?.avg_win  ?? null
            const _avgLoss = _as?.avg_loss ?? null

            // Card 3: signal confirmation
            // top_strategy may be a code key ("RSI_OB_VOL_DRY") or already readable.
            // Normalise: replace underscores, title-case only if all-caps code key.
            const _rawStrat  = _as?.top_strategy || ''
            const _stratLabel = _rawStrat && /^[A-Z0-9_]+$/.test(_rawStrat)
              ? _rawStrat.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase())
              : _rawStrat || (SIGNAL_META[signalType]?.label ?? signalType.replace(/_/g, ' '))
            const _tf = _as?.timeframe || '1D'
            const _tfLabel = _tf === '1D' ? 'Daily (1D)' : _tf === '1W' ? 'Weekly (1W)' : _tf
            const _firingNote = (_as?.firing_count != null && _as?.qualified_total != null)
              ? `${_as.firing_count} of ${_as.qualified_total} strategies fired`
              : ''

            // Card 4: market trend + signal freshness
            const _activeRegime      = _as?.regime ?? data.regime?.regime ?? regime
            const _activeRegimeScore = _as?.regime_score ?? data.regime?.regime_score ?? null
            const _regimeColor       = _activeRegime === 'BEAR' ? T.r : _activeRegime === 'BULL' ? T.g : T.a
            const _agedays           = _as?.signal_age_days ?? null
            const _ageLabel          = _agedays == null
              ? ''
              : _agedays <= 1 ? 'Triggered today' : `Active ${_agedays} trading days`
            const _regimeNote = [
              _activeRegimeScore != null ? `Score ${_activeRegimeScore} — positive = bullish trend, negative = bearish` : null,
              _ageLabel || null,
            ].filter(Boolean).join(' · ')

            return (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14 }}>

                {/* Card 1 — Backtest summary */}
                <MetricCard
                  label="Backtest summary"
                  value={_wr != null ? `${_wr}% win rate` : 'N/A'}
                  note={activeSide?.total_signals != null
                    ? `Based on ${activeSide.total_signals.toLocaleString()} historical signals.`
                    : 'Win rate across historical signals.'}
                  color={accent}
                />

                {/* Card 2 — Real avg win / avg loss from paper_ledger */}
                <MetricCard
                  label="What happened after"
                  value={_avgWin != null
                    ? `${_avgWin > 0 ? '+' : ''}${_avgWin}% when right`
                    : _wr != null ? `${_wr}% moved ${isBull ? 'up' : 'down'}` : 'N/A'}
                  note={_avgLoss != null
                    ? `${_avgLoss}% avg loss when wrong`
                    : 'Based on historical signal outcomes.'}
                  color={_avgWin != null ? T.g : accent}
                />

                {/* Card 3 — Signal confirmation + timeframe */}
                <MetricCard
                  label="Signal confirmed by"
                  value={_stratLabel}
                  note={[`Timeframe: ${_tfLabel}`, _firingNote].filter(Boolean).join(' · ')}
                  color={accent}
                />

                {/* Card 4 — Market trend + signal freshness */}
                <MetricCard
                  label="Market trend"
                  value={_activeRegime ? `${_activeRegime} regime` : 'N/A'}
                  note={_regimeNote || 'Regime from KANIDA.AI model.'}
                  color={_regimeColor}
                />

              </div>
            )
          })()}
        </div>

        {chatOpen && (
          <div style={{ ...panelStyle(22), display: 'grid', gap: 16, position: 'relative', overflow: 'hidden', marginBottom: 18 }}>
            <AmbientOrb color={accent} top={16} right={120} size={110} />
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <span style={chipStyle('green')}>{ticker} context loaded</span>
              <span style={chipStyle()}>Follow-up mode</span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              {msgs.map((msg, index) => (
                <div key={index} style={{ alignSelf: msg.role === 'user' ? 'flex-end' : 'flex-start', maxWidth: msg.role === 'user' ? '68%' : '74%' }}>
                  <div style={{ fontSize: 11, color: msg.role === 'user' ? T.a : T.g, fontFamily: T.mono, marginBottom: 6, fontWeight: 800 }}>
                    {msg.role === 'user' ? 'YOU' : 'KANIDA.AI'}
                  </div>
                  <div
                    style={{
                      background: msg.role === 'user' ? 'rgba(255,209,102,0.08)' : T.s2,
                      border: `1px solid ${msg.role === 'user' ? 'rgba(255,209,102,0.18)' : T.b}`,
                      borderRadius: 16,
                      padding: '16px 18px',
                      fontSize: 16,
                      lineHeight: 1.8,
                      color: T.t2,
                      whiteSpace: 'pre-line',
                    }}
                  >
                    {msg.text}
                  </div>
                </div>
              ))}
              {sending && (
                <div style={{ maxWidth: '74%' }}>
                  <div style={{ fontSize: 11, color: T.g, fontFamily: T.mono, marginBottom: 6, fontWeight: 800 }}>KANIDA.AI</div>
                  <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 16, padding: '16px 18px', fontSize: 16, color: T.t2 }}>
                    Thinking...
                  </div>
                </div>
              )}
            </div>
            <GlowDivider />
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {prompts.map((prompt) => (
                <button key={prompt} onClick={() => send(prompt)} style={{ ...chipStyle(), cursor: 'pointer', background: 'rgba(255,255,255,0.03)' }}>
                  {prompt}
                </button>
              ))}
            </div>
            <SearchBar value={input} onChange={setInput} onSubmit={() => send(input)} placeholder={`Ask about ${ticker}...`} buttonLabel="Send" />
          </div>
        )}

        <div style={{ display: 'grid', gap: 12 }}>
          <InfoSection
            open={openSections.has('story')}
            onToggle={() => toggle('story')}
            title="Why this is interesting"
            copy="One clear reason why this setup is worth paying attention to right now."
          >
            <div style={{ display: 'grid', gridTemplateColumns: '1.05fr .95fr', gap: 18 }}>
              <div style={{ fontSize: 16, color: T.t2, lineHeight: 1.85 }}>{_heroBody}</div>
              <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)' }}>
                <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 }}>Signal confidence</div>
                <div style={{ height: 10, background: 'rgba(255,255,255,0.07)', borderRadius: 999, overflow: 'hidden', marginBottom: 10 }}>
                  <div
                    style={{
                      width: `${Math.min(100, Math.max(8, activeSide?.avg_win_rate || 0))}%`,
                      height: '100%',
                      background: `linear-gradient(90deg, ${accent}, ${accent}66)`,
                    }}
                  />
                </div>
                <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.75 }}>
                  {activeSide?.avg_win_rate != null ? `${activeSide.avg_win_rate}% win rate across ${activeSide.total_signals?.toLocaleString() || 'historical'} signals.` : 'Win rate confidence across historical signals.'}
                </div>
              </div>
            </div>
          </InfoSection>

          {data.historical_context && (() => {
            const hc = data.historical_context!
            const domSide = hc.dominant_bias
            const domWr   = hc.dominant_win_rate
            const domSig  = hc.dominant_total_signals
            const isCT    = hc.is_counter_trend
            const activeBiasLabel = isBull ? 'bullish' : 'bearish'
            const domLabel = domSide === 'bullish' ? 'Bullish' : 'Bearish'
            const domAccent = domSide === 'bullish' ? T.g : T.r

            return (
              <InfoSection
                open={openSections.has('history')}
                onToggle={() => toggle('history')}
                title="Historical context"
                copy={isCT
                  ? `Today's ${activeBiasLabel} signal runs against ${ticker}'s dominant ${domLabel.toLowerCase()} historical edge. Counter-trend setup — risk accordingly.`
                  : `Today's ${activeBiasLabel} signal aligns with ${ticker}'s dominant historical direction.`
                }
              >
                <div style={{ display: 'grid', gap: 16 }}>
                  {/* Dominant side summary */}
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 14 }}>
                    <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)' }}>
                      <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 6 }}>Dominant side</div>
                      <div style={{ fontSize: 22, fontWeight: 900, color: domAccent }}>{domLabel}</div>
                      <div style={{ fontSize: 13, color: T.t2, marginTop: 4 }}>Long-term historical edge</div>
                    </div>
                    <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)' }}>
                      <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 6 }}>Historical win rate</div>
                      <div style={{ fontSize: 22, fontWeight: 900, color: domAccent }}>{domWr}%</div>
                      <div style={{ fontSize: 13, color: T.t2, marginTop: 4 }}>{domSig.toLocaleString()} qualifying signals</div>
                    </div>
                    <div style={{ ...panelStyle(18), background: isCT ? 'rgba(255,209,102,0.06)' : 'rgba(0,201,138,0.06)', border: `1px solid ${isCT ? 'rgba(255,209,102,0.2)' : 'rgba(0,201,138,0.2)'}`, borderRadius: 14 }}>
                      <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 6 }}>Signal type</div>
                      <div style={{ fontSize: 18, fontWeight: 900, color: isCT ? T.a : T.g }}>{isCT ? 'Counter-Trend' : 'Trend-Following'}</div>
                      <div style={{ fontSize: 13, color: T.t2, marginTop: 4 }}>
                        {isCT ? `Active ${activeBiasLabel} vs dominant ${domSide}` : `Active side matches dominant ${domSide}`}
                      </div>
                    </div>
                  </div>

                  {/* Counter-trend risk note */}
                  {isCT && (
                    <div style={{ ...panelStyle(18), background: 'rgba(255,209,102,0.05)', border: `1px solid rgba(255,209,102,0.18)`, borderRadius: 14 }}>
                      <div style={{ fontSize: 13, fontWeight: 800, color: T.a, textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 10 }}>
                        Counter-Trend Risk Management
                      </div>
                      <div style={{ display: 'grid', gap: 8 }}>
                        {[
                          `${ticker}'s dominant ${domLabel} edge is ${domWr}% across ${domSig.toLocaleString()} signals. Today's ${activeBiasLabel} signal trades against that historical direction.`,
                          `Consider reducing position size — half or less of your standard sizing is appropriate for counter-trend setups.`,
                          `Use tighter stop-losses than you would for a trend-following trade. The dominant ${domLabel.toLowerCase()} bias may reassert quickly.`,
                          `Target shorter, quicker profit objectives. Do not let a counter-trend trade turn into a positional hold.`,
                        ].map((line, i) => (
                          <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                            <span style={{ width: 7, height: 7, borderRadius: 999, background: T.a, marginTop: 7, flexShrink: 0 }} />
                            <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.78 }}>{line}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Trend-following confirmation */}
                  {!isCT && (
                    <div style={{ ...panelStyle(18), background: 'rgba(0,201,138,0.05)', border: `1px solid rgba(0,201,138,0.18)`, borderRadius: 14 }}>
                      <div style={{ fontSize: 13, fontWeight: 800, color: T.g, textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 10 }}>
                        Trend-Following Confirmation
                      </div>
                      <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.78 }}>
                        {`Today's ${activeBiasLabel} signal aligns with ${ticker}'s dominant historical direction (${domWr}% win rate, ${domSig.toLocaleString()} signals). Standard position sizing and stop-loss levels are appropriate for this setup.`}
                      </div>
                    </div>
                  )}
                </div>
              </InfoSection>
            )
          })()}

          <InfoSection
            open={openSections.has('plan')}
            onToggle={() => toggle('plan')}
            title="What to do with it"
            copy="Key price levels and a paper trade starting point."
          >
            <div style={{ display: 'grid', gridTemplateColumns: '1.05fr .95fr', gap: 18 }}>
              <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)' }}>
                <div style={{ fontSize: 12, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 14 }}>Price levels</div>
                {(() => {
                  const price = lvl?.price
                  const t1 = lvl?.target_1
                  const t2 = lvl?.target_2
                  const sl = lvl?.stop_loss
                  if (!price) return <div style={{ fontSize: 14, color: T.t3 }}>Price data not available.</div>
                  const levels = [
                    { label: 'Stop', value: sl, color: T.r },
                    { label: 'Price', value: price, color: T.t },
                    { label: 'T1', value: t1, color: T.g },
                    { label: 'T2', value: t2, color: T.g },
                  ].filter(l => l.value != null) as { label: string; value: number; color: string }[]
                  const sorted = [...levels].sort((a, b) => a.value - b.value)
                  const min = sorted[0].value * 0.995
                  const max = sorted[sorted.length - 1].value * 1.005
                  const range = max - min || 1
                  return (
                    <div style={{ position: 'relative', height: 160 }}>
                      {sorted.map(l => {
                        const pct = ((l.value - min) / range) * 100
                        const isPrice = l.label === 'Price'
                        return (
                          <div key={l.label} style={{ position: 'absolute', left: 0, right: 0, bottom: `${pct}%`, display: 'flex', alignItems: 'center', gap: 8 }}>
                            <div style={{ flex: 1, height: isPrice ? 2 : 1, background: l.color, opacity: isPrice ? 1 : 0.5 }} />
                            <div style={{ fontSize: 11, color: l.color, fontWeight: isPrice ? 800 : 400, whiteSpace: 'nowrap' }}>
                              {l.label} {l.value.toLocaleString()}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  )
                })()}
              </div>
              <div style={{ display: 'grid', gap: 12 }}>
                <MetricCard label="Current price" value={lvl?.price != null ? `Rs ${lvl.price.toLocaleString()}` : 'N/A'} note={lvl?.price != null ? `${isBull ? 'Entry' : 'Short entry'} anchor at last snapshot.` : 'Price unavailable.'} color={T.t} />
                <MetricCard label="Target" value={lvl?.target_1 != null ? `Rs ${lvl.target_1.toLocaleString()}` : 'N/A'} note={lvl?.t1_pct != null ? `${lvl.t1_pct > 0 ? '+' : ''}${lvl.t1_pct.toFixed(1)}% from entry.` : 'Target from KANIDA.AI levels.'} color={T.g} />
                <MetricCard label="Stop loss" value={lvl?.stop_loss != null ? `Rs ${lvl.stop_loss.toLocaleString()}` : 'Defined'} note={lvl?.sl_pct != null ? `${lvl.sl_pct.toFixed(1)}% from entry. ${isBull ? 'Below entry.' : 'Above entry.'}` : levelNarrative(isBull)} color={T.r} />
                <MetricCard label="Risk / reward" value={lvl?.rr != null ? `1 : ${lvl.rr}` : 'N/A'} note={lvl?.rr != null ? `${lvl.rr >= 2 ? 'Favourable' : 'Acceptable'} ratio for this setup.` : 'R:R not available.'} color={T.a} />
              </div>
            </div>
          </InfoSection>

          <InfoSection
            open={openSections.has('evidence')}
            onToggle={() => toggle('evidence')}
            title="What supports it"
            copy="Pattern data and historical signals that back this setup."
          >
            <div style={{ display: 'grid', gap: 10 }}>
              {[
                `Primary bias is ${data.primary_bias.toUpperCase()}, which matches the current action framing.`,
                `Regime context is ${regime}, which helps explain whether the setup is getting tailwind or friction.`,
                activeSide?.total_signals != null
                  ? `${activeSide.total_signals.toLocaleString()} historical signals support this side of the setup.`
                  : 'Signal depth is limited, so confidence should stay measured.',
              ].map((line) => (
                <div key={line} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                  <span style={{ width: 8, height: 8, borderRadius: 999, background: accent, marginTop: 8, flexShrink: 0 }} />
                  <div style={{ fontSize: 15, color: T.t2, lineHeight: 1.8 }}>{line}</div>
                </div>
              ))}

              {biasPatterns.length > 0 ? (
                <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)', marginTop: 10 }}>
                  <div style={{ fontSize: 12, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 }}>Pattern breakdown</div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1.2fr .7fr .7fr .7fr', gap: 6, fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 6 }}>
                    <span>Pattern</span><span>Win rate</span><span>Avg gain</span><span>Signals</span>
                  </div>
                  <div style={{ display: 'grid', gap: 8 }}>
                    {biasPatterns.slice(0, 4).map((pattern) => (
                      <div key={pattern.category} style={{ display: 'grid', gridTemplateColumns: '1.2fr .7fr .7fr .7fr', gap: 10, alignItems: 'center', fontSize: 14 }}>
                        <span style={{ color: T.t }}>{pattern.category}</span>
                        <span style={{ fontFamily: T.mono, color: T.g }}>{pattern.win_rate}%</span>
                        <span style={{ fontFamily: T.mono, color: T.g }}>{formatPct(pattern.avg_gain)}</span>
                        <span style={{ fontFamily: T.mono, color: T.t3 }}>{pattern.occurrences}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : activeSide && (
                <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)', marginTop: 10 }}>
                  <div style={{ fontSize: 12, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 }}>Signal breakdown</div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1.2fr .7fr .7fr .7fr', gap: 6, fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 6 }}>
                    <span>Side</span><span>Win rate</span><span>Avg gain</span><span>Signals</span>
                  </div>
                  <div style={{ display: 'grid', gap: 8 }}>
                    <div style={{ display: 'grid', gridTemplateColumns: '1.2fr .7fr .7fr .7fr', gap: 10, alignItems: 'center', fontSize: 14 }}>
                      <span style={{ color: T.t }}>{isBull ? 'Bullish aggregate' : 'Bearish aggregate'}</span>
                      <span style={{ fontFamily: T.mono, color: accent }}>{activeSide.avg_win_rate != null ? `${activeSide.avg_win_rate}%` : 'N/A'}</span>
                      <span style={{ fontFamily: T.mono, color: accent }}>{formatPct(activeSide.avg_gain)}</span>
                      <span style={{ fontFamily: T.mono, color: T.t3 }}>{activeSide.total_signals?.toLocaleString() || 'N/A'}</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </InfoSection>

          <InfoSection
            open={openSections.has('trade')}
            onToggle={() => toggle('trade')}
            title="Paper trade"
            copy="Log a paper trade at current levels to track this idea over time."
          >
            {!openTrade ? (
              <button
                onClick={startPaperTrade}
                disabled={tradeLoading}
                style={{
                  border: 'none',
                  background: T.g,
                  color: '#000',
                  padding: '14px 22px',
                  borderRadius: 12,
                  fontSize: 14,
                  fontWeight: 800,
                  cursor: tradeLoading ? 'wait' : 'pointer',
                  marginBottom: 16,
                }}
              >
                {tradeLoading ? 'Logging trade...' : 'Start paper trade at current levels'}
              </button>
            ) : (
              <div style={{ ...panelStyle(18), background: 'rgba(0,201,138,0.06)', borderColor: 'rgba(0,201,138,0.2)', marginBottom: 16 }}>
                <div style={{ fontSize: 15, color: T.g, fontWeight: 700, marginBottom: 6 }}>Paper trade logged</div>
                <div style={{ fontSize: 14, color: T.t2, lineHeight: 1.75 }}>
                  Entry {String(openTrade.entry_price)} | Stop {String(openTrade.stop)} | Target {String(openTrade.target)}
                </div>
              </div>
            )}

            {tradeHistory.length > 0 && (
              <div style={{ ...panelStyle(18), background: 'rgba(255,255,255,0.02)' }}>
                <div style={{ fontSize: 12, color: T.t3, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 }}>Recent trades</div>
                <div style={{ display: 'grid', gap: 8 }}>
                  {tradeHistory.slice(0, 4).map((trade, index) => (
                    <div key={index} style={{ display: 'grid', gridTemplateColumns: '1fr .8fr .8fr', gap: 10, fontSize: 14, paddingTop: index === 0 ? 0 : 8, borderTop: index === 0 ? 'none' : `1px solid ${T.b}` }}>
                      <span style={{ color: T.t2 }}>{String(trade.signal_date || 'Recent')}</span>
                      <span style={{ fontFamily: T.mono }}>{String(trade.status || 'Logged')}</span>
                      <span style={{ fontFamily: T.mono, color: Number(trade.outcome_pct) > 0 ? T.g : T.r }}>
                        {trade.outcome_pct != null ? `${Number(trade.outcome_pct) > 0 ? '+' : ''}${Number(trade.outcome_pct).toFixed(1)}%` : 'Open'}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </InfoSection>

          <InfoSection
            open={openSections.has('expert')}
            onToggle={() => toggle('expert')}
            title="Expert mode"
            copy="Full signal breakdown with win rate, conviction, and raw metrics."
          >
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
              <MetricCard label="Primary bias" value={data.primary_bias.toUpperCase()} note="Bias direction for this setup." color={accent} />
              <MetricCard label="Conviction" value={data.conviction} note="Strength of the current signal." color={data.conviction === 'HIGH' ? T.g : T.a} />
              <MetricCard label="Best gain" value={data.bullish ? formatPct(data.bullish.best_gain) : 'N/A'} note="Best recorded gain in historical sample." color={T.g} />
              <MetricCard label="Worst loss" value={data.bullish ? formatPct(data.bullish.worst_loss, false) : 'N/A'} note="Worst recorded loss in historical sample." color={T.r} />
            </div>
          </InfoSection>
        </div>
      </div>
    </div>
  )
}
