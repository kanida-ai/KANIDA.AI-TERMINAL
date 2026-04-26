'use client'

import React, { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { getFeed, getHealth, sendChat, type FeedCard, type Health } from '@/lib/api'
import { T, brand } from '@/lib/theme'
import {
  AmbientOrb,
  chipStyle,
  GlowDivider,
  panelStyle,
  pageShellStyle,
  SearchBar,
  SectionEyebrow,
} from '@/lib/terminal-ui'

function ChatInner() {
  const searchParams = useSearchParams()
  const initialQ = searchParams.get('q') || ''

  const [health, setHealth] = useState<Health | null>(null)
  const [cards, setCards] = useState<FeedCard[]>([])
  const [msgs, setMsgs] = useState<{ role: string; text: string }[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [history, setHistory] = useState<{ role: string; content: string }[]>([])
  const didInit = useRef(false)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    getHealth().then(setHealth).catch(() => null)
    getFeed('NSE', 5).then(setCards).catch(() => setCards([]))
  }, [])

  useEffect(() => {
    let text: string
    if (health) {
      const bullCount = cards.filter(c => c.primaryBias === 'bullish').length
      const bearCount = cards.filter(c => c.primaryBias === 'bearish').length
      const biasHint = bullCount + bearCount > 0 ? ` Today's feed is showing ${bullCount} bullish and ${bearCount} bearish ideas.` : ''
      const ageHint = health.snapshot_stale ? ' Snapshot data may be slightly stale.' : health.snapshot_age_minutes != null ? ` Data is ${health.snapshot_age_minutes} minutes fresh.` : ''
      text = `KANIDA.AI is ready. ${health.fingerprints.toLocaleString()} fingerprints across ${health.tickers} stocks.${biasHint}${ageHint} Ask about what matters now, compare ideas, or go deeper on one setup.`
    } else {
      text = 'KANIDA.AI is ready. Ask about what matters now, compare ideas, or go deeper on one setup.'
    }
    setMsgs([{ role: 'assistant', text }])
  }, [health, cards])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [msgs, sending])

  useEffect(() => {
    if (initialQ && !didInit.current) {
      didInit.current = true
      void send(initialQ)
    }
  }, [initialQ])

  const send = async (text: string) => {
    if (!text.trim() || sending) return
    setMsgs((prev) => [...prev, { role: 'user', text }])
    setInput('')
    setSending(true)
    try {
      const res = await sendChat(text, history)
      setMsgs((prev) => [...prev, { role: 'assistant', text: res.response }])
      setHistory((prev) => [...prev, { role: 'user', content: text }, { role: 'assistant', content: res.response }])
    } catch {
      setMsgs((prev) => [...prev, { role: 'assistant', text: 'Something went wrong. Please try again.' }])
    } finally {
      setSending(false)
    }
  }

  const chips = useMemo(() => cards.slice(0, 5), [cards])

  const dynamicPrompts = useMemo(() => {
    const topBull = cards.find((c) => c.primaryBias === 'bullish')
    const topBear = cards.find((c) => c.primaryBias === 'bearish')
    const topHigh = cards.find((c) => c.conviction === 'HIGH')
    const prompts: string[] = []
    if (topBull) {
      const wr = topBull.avgWinRate != null ? ' at ' + Math.round(topBull.avgWinRate * 100) + '% win rate' : ''
      prompts.push('Why is ' + topBull.ticker + ' showing a bullish signal' + wr + '?')
    }
    if (topBear) {
      prompts.push('What makes ' + topBear.ticker + ' a bearish setup at current levels?')
    }
    if (topBull && topBear) {
      prompts.push('Compare ' + topBull.ticker + ' and ' + topBear.ticker + ' side by side')
    }
    if (topHigh && topHigh.ticker !== topBull?.ticker && topHigh.ticker !== topBear?.ticker) {
      prompts.push('What supports the high-conviction signal on ' + topHigh.ticker + '?')
    }
    if (prompts.length === 0) {
      prompts.push('What does the current market scan show?')
      prompts.push('Which stock has the strongest signal right now?')
    }
    return prompts.slice(0, 4)
  }, [cards])

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
        <span style={{ ...chipStyle('green'), marginLeft: 'auto' }}>Conversation</span>
      </nav>

      <div style={{ maxWidth: 1120, margin: '0 auto', padding: '24px 20px 48px' }}>
        <SectionEyebrow>Chat</SectionEyebrow>
        <div style={{ fontSize: 42, fontWeight: 900, letterSpacing: '-.05em', lineHeight: 1.03, marginBottom: 12 }}>
          Ask anything. Go deeper on any setup.
        </div>
        <div style={{ fontSize: 18, color: T.t2, lineHeight: 1.8, maxWidth: 860, marginBottom: 22 }}>
          Compare ideas, question a setup, or get a cleaner read on what matters now. KANIDA.AI has full context on every live opportunity.
        </div>

        <div style={{ ...panelStyle(22), display: 'grid', gap: 16, position: 'relative', overflow: 'hidden' }}>
          <AmbientOrb color={T.g} top={16} right={120} size={110} />

          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <span style={chipStyle('green')}>KANIDA.AI ready</span>
            <span style={chipStyle()}>{chips.length ? `${chips.length} live idea chips` : 'Live context'}</span>
          </div>

          {chips.length > 0 && (
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {chips.map((card) => (
                <button
                  key={card.ticker}
                  onClick={() => send(`Tell me why ${card.ticker} matters today`)}
                  style={{ ...chipStyle(card.primaryBias === 'bullish' ? 'green' : 'red'), cursor: 'pointer' }}
                >
                  {card.ticker}
                </button>
              ))}
            </div>
          )}

          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {msgs.map((msg, index) => (
              <div key={index} style={{ alignSelf: msg.role === 'user' ? 'flex-end' : 'flex-start', maxWidth: msg.role === 'user' ? '68%' : '76%' }}>
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
              <div style={{ maxWidth: '76%' }}>
                <div style={{ fontSize: 11, color: T.g, fontFamily: T.mono, marginBottom: 6, fontWeight: 800 }}>KANIDA.AI</div>
                <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 16, padding: '16px 18px', fontSize: 16, color: T.t2 }}>
                  Thinking...
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>

          <GlowDivider />

          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {dynamicPrompts.map((prompt) => (
              <button key={prompt} onClick={() => send(prompt)} style={{ ...chipStyle(), cursor: 'pointer', background: 'rgba(255,255,255,0.03)' }}>
                {prompt}
              </button>
            ))}
          </div>

          <SearchBar value={input} onChange={setInput} onSubmit={() => send(input)} placeholder="Ask about any stock, strategy, or market setup..." buttonLabel="Send" />
        </div>
      </div>
    </div>
  )
}

export default function ChatPage() {
  return (
    <Suspense>
      <ChatInner />
    </Suspense>
  )
}
