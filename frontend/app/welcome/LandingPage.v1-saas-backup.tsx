'use client'

// KANIDA.AI public landing page.
// Shipped 2026-05-09 per Pudhuraja's GTM-ready brief.
// IP rules: no specific backtest numbers, no internal feature counts, no algo
// internals. Premium dark+gold styling, mobile-responsive.

import Link from 'next/link'
import { useState } from 'react'

const NAVY    = '#0B1F3A'
const OFFBLK  = '#0E1116'
const GOLD    = '#D4AF37'

export default function LandingPage() {
  return (
    <main className="min-h-screen bg-[#0B1F3A] text-neutral-100 antialiased">
      <Nav />
      <Hero />
      <TrustBar />
      <Problem />
      <Leap />
      <Bots />
      <HowItWorks />
      <WhatYouGet />
      <BattleTested />
      <DropDemo />
      <Pricing />
      <FAQ />
      <FinalCTA />
      <Footer />
    </main>
  )
}

// ─── 0. Nav ─────────────────────────────────────────────────────────────────
function Nav() {
  return (
    <nav className="sticky top-0 z-50 backdrop-blur-md bg-[#0B1F3A]/80 border-b border-white/5">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-3 flex items-center justify-between">
        <Link href="/" className="flex items-baseline gap-2 font-semibold text-white">
          KANIDA<span style={{ color: GOLD }}>.AI</span>
        </Link>
        <div className="hidden md:flex items-center gap-6 text-sm text-neutral-300">
          <a href="#bots" className="hover:text-white">Product</a>
          <Link href="/engine" className="hover:text-white">Engine</Link>
          <a href="#pricing" className="hover:text-white">Pricing</a>
          <Link href="/login" className="hover:text-white">Login</Link>
        </div>
        <Link
          href="/login"
          className="px-4 py-2 rounded-md text-sm font-semibold text-neutral-950"
          style={{ backgroundColor: GOLD }}>
          Start Free Trial →
        </Link>
      </div>
    </nav>
  )
}

// ─── 1. Hero ────────────────────────────────────────────────────────────────
function Hero() {
  return (
    <section className="relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-br from-[#0B1F3A] via-[#0E1116] to-[#0B1F3A]" />
      <ParticleField />
      <div className="relative max-w-6xl mx-auto px-4 md:px-6 pt-16 md:pt-24 pb-20 md:pb-28">
        <div className="grid md:grid-cols-2 gap-10 items-center">
          <div>
            <div className="text-xs uppercase tracking-[0.2em] mb-4" style={{ color: GOLD }}>
              Introducing KANIDA.AI · Powered by Falcon V7
            </div>
            <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold leading-[1.05] text-white">
              The 7th Generation<br/>
              of Stock Intelligence<br/>
              <span style={{ color: GOLD }}>has arrived.</span>
            </h1>
            <p className="mt-6 text-base md:text-lg text-neutral-300 max-w-xl leading-relaxed">
              The first multi-bot quant engine for Indian markets.
              Four autonomous AI agents learn, scan, execute, and answer — so you don't have to.
            </p>
            <div className="mt-8 flex flex-col sm:flex-row gap-3">
              <Link
                href="/login"
                className="px-6 py-3 rounded-md text-base font-bold text-neutral-950 inline-flex items-center justify-center"
                style={{ backgroundColor: GOLD }}>
                Get Today's Intelligence Drop →
              </Link>
              <a
                href="#how"
                className="px-6 py-3 rounded-md text-base font-semibold text-white border border-white/20 hover:border-white/40 inline-flex items-center justify-center">
                See how it works
              </a>
            </div>
            <p className="mt-4 text-xs text-neutral-500">
              No credit card · 7-day free trial · Cancel anytime
            </p>
          </div>
          <BotOrbit />
        </div>
      </div>
    </section>
  )
}

function ParticleField() {
  // Static SVG noise/dot grid — no JS animations to keep TTI snappy.
  return (
    <svg className="absolute inset-0 w-full h-full opacity-20" aria-hidden>
      <defs>
        <pattern id="dots" x="0" y="0" width="32" height="32" patternUnits="userSpaceOnUse">
          <circle cx="2" cy="2" r="1" fill={GOLD} />
        </pattern>
      </defs>
      <rect width="100%" height="100%" fill="url(#dots)" />
    </svg>
  )
}

function BotOrbit() {
  // Stylised four-bot composition. Pure SVG — no images, no external assets.
  return (
    <div className="relative aspect-square max-w-md mx-auto">
      <div className="absolute inset-0 rounded-full bg-gradient-to-br from-amber-500/10 to-amber-500/0 blur-3xl" />
      <svg viewBox="0 0 400 400" className="relative w-full h-full">
        {/* Concentric rings */}
        <circle cx="200" cy="200" r="180" fill="none" stroke={GOLD} strokeOpacity="0.15" />
        <circle cx="200" cy="200" r="130" fill="none" stroke={GOLD} strokeOpacity="0.25" />
        <circle cx="200" cy="200" r="80"  fill="none" stroke={GOLD} strokeOpacity="0.4"  />
        {/* Center logo */}
        <circle cx="200" cy="200" r="36" fill={GOLD} />
        <text x="200" y="207" textAnchor="middle" fontSize="14" fontWeight="700" fill={NAVY}>
          KANIDA
        </text>
        {/* Four bots on cardinal points */}
        <BotNode cx={200} cy={40}  label="🦅 FALCON"   />
        <BotNode cx={360} cy={200} label="🎯 HUNTER"   />
        <BotNode cx={200} cy={360} label="🛡️ SENTINEL" />
        <BotNode cx={40}  cy={200} label="💬 COMPANION"/>
        {/* Connection lines */}
        <line x1="200" y1="60"  x2="200" y2="164" stroke={GOLD} strokeOpacity="0.4" strokeDasharray="3 4" />
        <line x1="340" y1="200" x2="236" y2="200" stroke={GOLD} strokeOpacity="0.4" strokeDasharray="3 4" />
        <line x1="200" y1="340" x2="200" y2="236" stroke={GOLD} strokeOpacity="0.4" strokeDasharray="3 4" />
        <line x1="60"  y1="200" x2="164" y2="200" stroke={GOLD} strokeOpacity="0.4" strokeDasharray="3 4" />
      </svg>
    </div>
  )
}

function BotNode({ cx, cy, label }: { cx: number; cy: number; label: string }) {
  return (
    <g>
      <circle cx={cx} cy={cy} r="22" fill={NAVY} stroke={GOLD} strokeWidth="2" />
      <text x={cx} y={cy + 5} textAnchor="middle" fontSize="11" fontWeight="600" fill="white">
        {label}
      </text>
    </g>
  )
}

// ─── 2. Trust bar ───────────────────────────────────────────────────────────
function TrustBar() {
  const items = [
    '🇮🇳 Built for Indian markets',
    '🧪 Out-of-sample validated',
    '🤖 Multi-bot architecture',
    '🔐 Independent research platform',
    '⚡ Powered by Falcon V7',
  ]
  return (
    <section className="border-y border-white/5 bg-[#0E1116]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-5 flex flex-wrap items-center justify-center gap-x-8 gap-y-3 text-xs md:text-sm text-neutral-400">
        {items.map(t => <span key={t}>{t}</span>)}
      </div>
    </section>
  )
}

// ─── 3. Problem ─────────────────────────────────────────────────────────────
function Problem() {
  const cards = [
    {
      icon: '📱',
      title: 'Tips without reason',
      body: 'You buy what someone tells you. Nobody tells you why. Or when to exit.',
    },
    {
      icon: '📉',
      title: 'Late to every move',
      body: "By the time it's trending, smart money already left. You catch the crash, not the rally.",
    },
    {
      icon: '😓',
      title: 'No system, no peace',
      body: 'Random buys. Emotional sells. No discipline. No track record. No idea why you\'re up — or down.',
    },
  ]
  return (
    <section className="bg-white text-neutral-900">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <h2 className="text-3xl md:text-5xl font-bold leading-tight">
          Stock tips don't make you rich.<br/>
          <span className="text-neutral-500">They make you tired.</span>
        </h2>
        <div className="mt-12 grid md:grid-cols-3 gap-6">
          {cards.map(c => (
            <div key={c.title} className="border border-neutral-200 rounded-xl p-6 bg-neutral-50">
              <div className="text-3xl mb-3">{c.icon}</div>
              <div className="text-lg font-bold mb-2">{c.title}</div>
              <p className="text-sm text-neutral-600 leading-relaxed">{c.body}</p>
            </div>
          ))}
        </div>
        <div className="mt-12 text-center text-xl md:text-2xl italic text-neutral-700">
          "You don't need more tips. You need a system."
        </div>
      </div>
    </section>
  )
}

// ─── 4. The Leap (7 generations) ────────────────────────────────────────────
function Leap() {
  const gens: Array<{ n: number; label: string; current?: boolean }> = [
    { n: 1, label: 'Stock screeners' },
    { n: 2, label: 'Technical indicators' },
    { n: 3, label: 'Rule-based algos' },
    { n: 4, label: 'ML signals' },
    { n: 5, label: 'Deep-learning predictors' },
    { n: 6, label: 'Multi-factor models' },
    { n: 7, label: 'Pattern Intelligence', current: true },
  ]
  return (
    <section id="engine" className="relative bg-gradient-to-b from-[#0E1116] via-[#0B1F3A] to-[#0E1116]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          The evolution of quant intelligence
        </div>
        <h2 className="text-3xl md:text-5xl font-bold text-white leading-tight">
          Six generations of tools.<br/>
          One generation of <span style={{ color: GOLD }}>intelligence</span>.
        </h2>

        {/* Horizontal timeline on md+, vertical stack on mobile */}
        <div className="mt-12 grid grid-cols-2 md:grid-cols-7 gap-3 md:gap-2">
          {gens.map(g => (
            <div
              key={g.n}
              className={
                'relative rounded-lg p-3 md:p-4 text-center border transition '
                + (g.current
                  ? 'bg-[radial-gradient(ellipse_at_center,rgba(212,175,55,0.25),transparent_70%)] border-amber-500/60'
                  : 'bg-white/[0.02] border-white/10')
              }>
              <div className={'text-[10px] uppercase tracking-wider ' + (g.current ? 'text-amber-300' : 'text-neutral-500')}>
                Gen {g.n}
              </div>
              <div className={
                'mt-1 font-bold ' +
                (g.current ? 'text-lg md:text-xl text-white drop-shadow-[0_0_12px_rgba(212,175,55,0.5)]' : 'text-sm text-neutral-400')
              }>
                {g.label}
              </div>
              {g.current && (
                <div className="mt-2 text-[10px] uppercase tracking-wider" style={{ color: GOLD }}>
                  ← KANIDA is here
                </div>
              )}
            </div>
          ))}
        </div>

        <p className="mt-10 max-w-3xl text-base md:text-lg text-neutral-300 leading-relaxed">
          For 30 years, every quant tool tried to <em className="text-neutral-500">predict</em> what will happen next.
          KANIDA.AI is the first to <strong style={{ color: GOLD }}>understand</strong> what already worked — at the
          behavioral level — and detect when it's happening again, in real time.
        </p>
        <p className="mt-3 text-xl md:text-2xl font-semibold text-white">
          This isn't an upgrade. It's a category leap.
        </p>
        <Link
          href="/engine"
          className="mt-6 inline-flex items-center gap-1 text-sm font-semibold hover:gap-2 transition-all"
          style={{ color: GOLD }}>
          Explore the Falcon V7 engine →
        </Link>
      </div>
    </section>
  )
}

// ─── 5. Meet the bots ───────────────────────────────────────────────────────
function Bots() {
  const bots = [
    { icon: '🦅', name: 'FALCON',    role: 'Pattern Discovery',
      tag: 'Mines years of data. Finds the behaviors that precede real moves.',
      proof: 'Validated across bull, bear, crash.' },
    { icon: '🎯', name: 'HUNTER',    role: 'Live Market Scanner',
      tag: 'Scans every NSE stock in real time. Surfaces only high-conviction setups.',
      proof: 'No noise. Only signal.' },
    { icon: '🛡️', name: 'SENTINEL',  role: 'Risk + Auto-Execution',
      tag: 'Connects to your Zerodha. Places trades. Trails winners. Cuts losers.',
      proof: 'Never sleeps. Never panics.' },
    { icon: '💬', name: 'COMPANION', role: 'Quant Copilot',
      tag: 'Ask anything about any setup, in any language. Get a data-backed answer.',
      proof: 'Like ChatGPT — for Indian equities.' },
  ]
  return (
    <section id="bots" className="bg-[#0B1F3A]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          Multi-bot architecture
        </div>
        <h2 className="text-3xl md:text-5xl font-bold text-white">
          Four AI agents. <span style={{ color: GOLD }}>One mission.</span>
        </h2>
        <div className="mt-12 grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {bots.map(b => (
            <div key={b.name} className="rounded-xl bg-white/[0.03] border border-white/10 p-6 hover:border-amber-500/40 transition">
              <div className="text-4xl mb-3">{b.icon}</div>
              <div className="text-xl font-bold text-white">{b.name}</div>
              <div className="text-xs uppercase tracking-wider mb-4" style={{ color: GOLD }}>
                {b.role}
              </div>
              <p className="text-sm text-neutral-300 leading-relaxed">{b.tag}</p>
              <p className="mt-3 text-xs text-neutral-500 italic">{b.proof}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

// ─── 6. How it works ────────────────────────────────────────────────────────
function HowItWorks() {
  const steps = [
    {
      n: '01',
      title: 'Atoms learn',
      body: 'Falcon mines millions of historical price moves and decomposes each into behavioral atoms. The ones that precede real moves get promoted.',
    },
    {
      n: '02',
      title: 'Hunter delivers',
      body: 'Every market evening, the Intelligence Drop arrives. Curated, ranked, conviction-scored. Tomorrow\'s plan, before dinner.',
    },
    {
      n: '03',
      title: 'Sentinel executes',
      body: 'Engage Sentinel Mode. Every trade enters with auto-protection. Every winner trails. Every loser cuts at the rule. You live your life.',
    },
  ]
  return (
    <section id="how" className="bg-white text-neutral-900">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <h2 className="text-3xl md:text-5xl font-bold leading-tight">
          Three steps. <span className="text-neutral-500">Zero stress.</span>
        </h2>
        <div className="mt-12 grid md:grid-cols-3 gap-6">
          {steps.map(s => (
            <div key={s.n} className="relative">
              <div className="text-7xl font-bold leading-none" style={{ color: GOLD }}>{s.n}</div>
              <div className="mt-3 text-2xl font-bold text-neutral-900">{s.title}</div>
              <p className="mt-3 text-sm text-neutral-600 leading-relaxed">{s.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

// ─── 7. What you get ────────────────────────────────────────────────────────
function WhatYouGet() {
  const items = [
    { icon: '🎯', title: 'Daily Intelligence Drop',  body: 'Top setups, every market evening, before 5:30 PM IST.' },
    { icon: '🤖', title: 'Autonomous Sentinel Mode', body: 'Connect Zerodha. Trade hands-free with built-in safety.' },
    { icon: '🛡️', title: 'Multi-layer risk',         body: 'Auto-stops. Auto-trails. Auto-exits. Every position. Every day.' },
    { icon: '💬', title: 'Companion Copilot',         body: 'Ask anything. Plain English. No charts to decode.' },
    { icon: '📊', title: 'Transparent track record',  body: 'Live performance. Every trade. Every outcome. No hiding.' },
    { icon: '🇮🇳', title: 'Pure Indian markets',     body: 'NSE-only. Built by Indians, for Indians. INR portfolios.' },
  ]
  return (
    <section className="bg-gradient-to-b from-[#0E1116] to-[#0B1F3A]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <h2 className="text-3xl md:text-5xl font-bold text-white">
          Everything you need. <span style={{ color: GOLD }}>Nothing you don't.</span>
        </h2>
        <div className="mt-12 grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {items.map(i => (
            <div key={i.title} className="rounded-xl bg-white/[0.03] border border-white/10 p-6">
              <div className="text-3xl mb-3">{i.icon}</div>
              <div className="text-lg font-bold text-white mb-2">{i.title}</div>
              <p className="text-sm text-neutral-400 leading-relaxed">{i.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

// ─── 8. Battle tested ───────────────────────────────────────────────────────
function BattleTested() {
  return (
    <section className="bg-[#0B1F3A] relative">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          Proven across every market regime
        </div>
        <h2 className="text-3xl md:text-5xl font-bold text-white">
          Tested through <span style={{ color: GOLD }}>every storm</span>.
        </h2>
        <div className="mt-12">
          <EquityCurveSVG />
        </div>
        <div className="mt-12 grid sm:grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { icon: '⚡', label: 'Multiple market regimes tested' },
            { icon: '🧪', label: 'Out-of-sample validated' },
            { icon: '🎯', label: 'Behavioral patterns mined from years of data' },
            { icon: '🔄', label: 'Patterns re-validated continuously' },
          ].map(t => (
            <div key={t.label} className="rounded-lg bg-white/[0.03] border border-white/10 p-4">
              <div className="text-xl mb-1">{t.icon}</div>
              <div className="text-xs text-neutral-300">{t.label}</div>
            </div>
          ))}
        </div>
        <p className="mt-10 text-sm md:text-base text-neutral-400 max-w-3xl leading-relaxed">
          The Falcon engine has been stress-tested across the toughest events in Indian markets — including
          the 2023 Adani crisis, 2024 election volatility, Q4 2024 selloffs, and rotational regimes of 2025.
          The discipline doesn't blink. The bots don't panic.
        </p>
        <p className="mt-6 text-[11px] text-neutral-600">
          * Past performance does not guarantee future returns. All investment in securities is subject to market risks.
        </p>
      </div>
    </section>
  )
}

function EquityCurveSVG() {
  // Stylised equity-curve graphic. NO specific numbers — purely qualitative.
  return (
    <svg viewBox="0 0 800 200" className="w-full h-auto">
      <defs>
        <linearGradient id="eq" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={GOLD} stopOpacity="0.4" />
          <stop offset="100%" stopColor={GOLD} stopOpacity="0" />
        </linearGradient>
      </defs>
      {/* Event markers */}
      {[
        { x: 150, label: 'Adani' },
        { x: 320, label: 'Election' },
        { x: 500, label: 'Q4 selloff' },
        { x: 680, label: 'Rotation' },
      ].map(m => (
        <g key={m.label}>
          <line x1={m.x} y1="20" x2={m.x} y2="180" stroke="white" strokeOpacity="0.1" strokeDasharray="3 3" />
          <text x={m.x} y="14" textAnchor="middle" fontSize="9" fill="rgba(255,255,255,0.4)">{m.label}</text>
        </g>
      ))}
      {/* Curve fill */}
      <path
        d="M 0 180 Q 80 165, 150 150 T 320 110 T 500 80 T 680 50 T 800 30 L 800 200 L 0 200 Z"
        fill="url(#eq)"
      />
      <path
        d="M 0 180 Q 80 165, 150 150 T 320 110 T 500 80 T 680 50 T 800 30"
        fill="none" stroke={GOLD} strokeWidth="2"
      />
    </svg>
  )
}

// ─── 9. Drop demo (phone mockup) ────────────────────────────────────────────
function DropDemo() {
  return (
    <section className="bg-[#0E1116]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28 grid md:grid-cols-2 gap-10 items-center">
        <div>
          <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
            See a real Intelligence Drop
          </div>
          <h2 className="text-3xl md:text-5xl font-bold text-white">
            This is what 5:00&nbsp;PM <span style={{ color: GOLD }}>looks like</span>.
          </h2>
          <ul className="mt-8 space-y-3 text-base text-neutral-300">
            {[
              'Confidence tier per pick',
              'Setup classification',
              'Plain-English reason',
              'Suggested entry & exit',
            ].map(li => (
              <li key={li} className="flex items-start gap-3">
                <span style={{ color: GOLD }}>✓</span>
                <span>{li}</span>
              </li>
            ))}
          </ul>
          <p className="mt-8 text-sm text-neutral-400 italic">
            No charts to read. No jargon to decode. Just the next step — clearly defined.
          </p>
        </div>
        <PhoneMockup />
      </div>
    </section>
  )
}

function PhoneMockup() {
  // Pure CSS phone frame + sample drop card
  return (
    <div className="relative mx-auto w-[280px] md:w-[320px]">
      {/* Phone frame */}
      <div className="rounded-[36px] bg-neutral-900 border border-neutral-800 p-3 shadow-[0_0_60px_rgba(212,175,55,0.15)]">
        <div className="rounded-[28px] bg-[#0B1F3A] overflow-hidden">
          {/* Notch */}
          <div className="h-6 bg-neutral-900 flex items-center justify-center">
            <div className="w-20 h-2 bg-neutral-800 rounded-full" />
          </div>
          {/* Header */}
          <div className="px-4 pt-4 pb-2">
            <div className="text-[10px] uppercase tracking-wider" style={{ color: GOLD }}>
              Today's Intelligence Drop
            </div>
            <div className="text-xs text-neutral-500">Mon, 5:00 PM IST · 12 picks</div>
          </div>
          {/* Cards */}
          <div className="px-4 pb-6 space-y-2">
            {[
              { sym: 'BANDHAN BNK', tier: 'CONVICTION', reason: 'Range contraction + OI buildup' },
              { sym: 'HFCL',        tier: 'CONVICTION', reason: 'Vol dry-up + late-day surge' },
              { sym: 'GABRIEL',     tier: 'WATCH',      reason: 'MTF margin compression' },
              { sym: 'NEULAND',     tier: 'EXPLORATORY', reason: 'Bouncing from drawdown' },
            ].map((p, i) => (
              <div key={p.sym} className="rounded-lg bg-white/[0.05] border border-white/10 p-3">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-bold text-white">{p.sym}</div>
                  <span className={
                    'text-[9px] px-1.5 py-0.5 rounded font-mono ' +
                    (p.tier === 'CONVICTION' ? 'bg-amber-500/20 text-amber-300'
                      : p.tier === 'WATCH'      ? 'bg-blue-500/20 text-blue-300'
                      :                            'bg-neutral-700/50 text-neutral-400')
                  }>{p.tier}</span>
                </div>
                <div className="mt-1 text-[10px] text-neutral-400">{p.reason}</div>
                {/* Confidence ring */}
                <div className="mt-2 flex items-center gap-2">
                  <div className="flex-1 h-1 bg-neutral-800 rounded-full overflow-hidden">
                    <div className="h-full" style={{ backgroundColor: GOLD, width: `${85 - i * 12}%` }} />
                  </div>
                  <span className="text-[9px] text-neutral-500 font-mono">{85 - i * 12}%</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

// ─── 10. Pricing ────────────────────────────────────────────────────────────
function Pricing() {
  // TODO: confirm pricing with Pudhuraja before launch — placeholders below.
  const tiers = [
    {
      name: 'Starter',
      price: '₹999',
      sub: '/month',
      featured: false,
      bullets: ['Daily Intelligence Drop', 'Top 10 setups', 'Companion Copilot', 'No auto-trade'],
      cta: 'Start free trial',
    },
    {
      name: 'Pro',
      price: '₹2,999',
      sub: '/month',
      featured: true,
      bullets: ['Everything in Starter', 'Top 25 setups', 'Sector intelligence', 'Pattern audit'],
      cta: 'Start free trial',
    },
    {
      name: 'Elite',
      price: '₹9,999',
      sub: '/month',
      featured: false,
      bullets: ['Everything in Pro', 'Auto-trade (Sentinel ON)', '1:1 onboarding', 'Priority support'],
      cta: 'Talk to us',
    },
  ]
  return (
    <section id="pricing" className="bg-white text-neutral-900">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <h2 className="text-3xl md:text-5xl font-bold leading-tight">
          Simple. Transparent. <span className="text-neutral-500">Skin in the game.</span>
        </h2>
        <div className="mt-12 grid md:grid-cols-3 gap-4">
          {tiers.map(t => (
            <div
              key={t.name}
              className={
                'rounded-xl p-6 md:p-8 ' +
                (t.featured
                  ? 'bg-[#0B1F3A] text-white border-2 shadow-2xl scale-105 md:scale-110'
                  : 'bg-neutral-50 border border-neutral-200')
              }
              style={t.featured ? { borderColor: GOLD } : {}}>
              {t.featured && (
                <div className="text-xs uppercase tracking-wider mb-3 inline-block px-2 py-0.5 rounded" style={{ backgroundColor: GOLD, color: NAVY }}>
                  Most Popular
                </div>
              )}
              <div className={'text-sm uppercase tracking-wider mb-2 ' + (t.featured ? 'text-amber-300' : 'text-neutral-500')}>
                {t.name}
              </div>
              <div className="flex items-baseline gap-1">
                <span className="text-4xl md:text-5xl font-bold">{t.price}</span>
                <span className={t.featured ? 'text-neutral-400' : 'text-neutral-500'}>{t.sub}</span>
              </div>
              <ul className="mt-6 space-y-2 text-sm">
                {t.bullets.map(b => (
                  <li key={b} className="flex items-start gap-2">
                    <span style={{ color: GOLD }}>✓</span>
                    <span className={t.featured ? 'text-neutral-200' : 'text-neutral-700'}>{b}</span>
                  </li>
                ))}
              </ul>
              <Link
                href="/login"
                className={
                  'mt-8 block text-center px-4 py-3 rounded-md font-bold ' +
                  (t.featured
                    ? 'text-neutral-950'
                    : 'text-white bg-neutral-900 hover:bg-neutral-800')
                }
                style={t.featured ? { backgroundColor: GOLD } : {}}>
                {t.cta} →
              </Link>
            </div>
          ))}
        </div>
        <p className="mt-8 text-center text-sm text-neutral-500">
          7-day free trial on all tiers · No credit card · 30-day money-back guarantee
        </p>
      </div>
    </section>
  )
}

// ─── 11. FAQ ────────────────────────────────────────────────────────────────
function FAQ() {
  const items = [
    {
      q: 'How is KANIDA different from a stock screener?',
      a: 'A screener filters by rules YOU define. KANIDA discovers what works — across years of historical behavior — and surfaces setups proactively. You don\'t filter. KANIDA decides.',
    },
    {
      q: 'Is this SEBI-compliant?',
      a: 'KANIDA.AI is positioned as an independent research and analytics platform. SEBI Research Analyst registration is in process. Consult your financial advisor before acting on any output.',
    },
    {
      q: 'How does Sentinel Mode trade for me?',
      a: 'You connect your Zerodha account via official Kite Connect API. Sentinel places orders, monitors positions, and applies risk rules in real time. You can pause or override anytime. Your funds stay in YOUR Zerodha account at all times.',
    },
    {
      q: 'What markets does KANIDA cover?',
      a: 'NSE-listed Indian equities only, focused on the Nifty 500 universe. International markets and derivatives are on the roadmap.',
    },
    {
      q: 'How accurate is KANIDA?',
      a: 'We don\'t claim "99% accuracy." We claim battle-tested edge. Live performance — every trade, every outcome — is published transparently on a public dashboard. See it before you sign up.',
    },
    {
      q: 'Can I lose money?',
      a: 'Yes. All investing in securities carries risk. KANIDA reduces randomness, not risk. Strict stop-losses are built in, but a market shock can still hurt. Position size accordingly.',
    },
    {
      q: 'Who is KANIDA for?',
      a: 'Active retail investors with ₹2L+ portfolio capital who want data-driven discipline instead of WhatsApp tips. Not for day-traders, options-sellers, or buy-and-hold investors.',
    },
  ]
  const [open, setOpen] = useState<number | null>(0)
  return (
    <section className="bg-[#0E1116]">
      <div className="max-w-3xl mx-auto px-4 md:px-6 py-20 md:py-28">
        <h2 className="text-3xl md:text-5xl font-bold text-white">
          Questions, <span style={{ color: GOLD }}>answered honestly</span>.
        </h2>
        <div className="mt-12 space-y-2">
          {items.map((it, i) => {
            const isOpen = open === i
            return (
              <div key={it.q} className="rounded-lg bg-white/[0.03] border border-white/10 overflow-hidden">
                <button
                  onClick={() => setOpen(isOpen ? null : i)}
                  className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-white/[0.02]">
                  <span className="text-sm md:text-base font-semibold text-white">{it.q}</span>
                  <span className="text-neutral-500 ml-4">{isOpen ? '−' : '+'}</span>
                </button>
                {isOpen && (
                  <div className="px-5 pb-5 text-sm text-neutral-300 leading-relaxed">{it.a}</div>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </section>
  )
}

// ─── 12. Final CTA ──────────────────────────────────────────────────────────
function FinalCTA() {
  return (
    <section className="relative overflow-hidden bg-gradient-to-br from-[#0B1F3A] to-[#0E1116]">
      <ParticleField />
      <div className="relative max-w-4xl mx-auto px-4 md:px-6 py-20 md:py-32 text-center">
        <h2 className="text-4xl md:text-6xl lg:text-7xl font-bold leading-tight text-white">
          Stop chasing tips.<br/>
          <span style={{ color: GOLD }}>Start trading with intelligence.</span>
        </h2>
        <p className="mt-6 text-base md:text-lg text-neutral-300 max-w-2xl mx-auto leading-relaxed">
          Every market evening at 5 PM, the 7th generation delivers your edge.
          The bots do the work. You collect the discipline.
        </p>
        <div className="mt-10 flex flex-col sm:flex-row gap-3 justify-center">
          <Link
            href="/login"
            className="px-8 py-4 rounded-md text-lg font-bold text-neutral-950 inline-flex items-center justify-center"
            style={{ backgroundColor: GOLD }}>
            Start your 7-day free trial →
          </Link>
          <a
            href="mailto:hello@kanida.ai"
            className="px-8 py-4 rounded-md text-lg font-semibold text-white border border-white/20 hover:border-white/40 inline-flex items-center justify-center">
            Talk to our team
          </a>
        </div>
        <p className="mt-6 text-xs text-neutral-500">
          No credit card · Cancel anytime · 30-day money-back guarantee
        </p>
      </div>
    </section>
  )
}

// ─── 13. Footer ─────────────────────────────────────────────────────────────
function Footer() {
  const cols = [
    { h: 'Product', items: ['Pricing', 'Features', 'Sentinel Mode', 'Companion'] },
    { h: 'Engine',  items: ['Falcon V7', 'The Bots', '7 Generations', 'Track Record'] },
    { h: 'Company', items: ['About', 'Careers', 'Press', 'Contact'] },
    { h: 'Legal',   items: ['Terms', 'Privacy', 'Disclosures', 'SEBI Disclosures'] },
  ]
  return (
    <footer className="bg-[#06101F] border-t border-white/5">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-12">
        <div className="grid md:grid-cols-5 gap-8">
          <div className="md:col-span-1">
            <div className="text-lg font-bold text-white">
              KANIDA<span style={{ color: GOLD }}>.AI</span>
            </div>
            <p className="mt-2 text-xs text-neutral-500 leading-relaxed">
              The 7th generation of stock intelligence.<br/>
              Powered by Falcon V7.
            </p>
          </div>
          {cols.map(c => (
            <div key={c.h}>
              <div className="text-xs uppercase tracking-wider text-neutral-400 font-semibold mb-3">{c.h}</div>
              <ul className="space-y-2">
                {c.items.map(i => (
                  <li key={i}>
                    <a href="#" className="text-xs text-neutral-500 hover:text-neutral-300">{i}</a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
        <div className="mt-12 pt-8 border-t border-white/5 text-[11px] text-neutral-600 leading-relaxed">
          KANIDA.AI is a research and analytics platform. Investments in securities markets are subject to market risks.
          Read all related documents carefully before investing. Past performance does not guarantee future returns.
          Brokerage and execution provided through licensed third-party brokers (Zerodha). KANIDA.AI does not handle
          client funds at any point. SEBI RA Reg. No. (in process).
        </div>
        <div className="mt-4 text-xs text-neutral-700">
          © 2026 KANIDA.AI · Built in Bharat 🇮🇳
        </div>
      </div>
    </footer>
  )
}
