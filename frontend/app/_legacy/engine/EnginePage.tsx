'use client'

// /engine — deep-dive page for the curious prospect, the press, the skeptical analyst.
// Tells the *story* of Falcon V7 without revealing IP. Same dark+gold styling as
// the landing page so the brand is consistent.

import Link from 'next/link'

const NAVY = '#0B1F3A'
const GOLD = '#D4AF37'

export default function EnginePage() {
  return (
    <main className="min-h-screen bg-[#0B1F3A] text-neutral-100 antialiased">
      <Nav />
      <Hero />
      <Thesis />
      <AtomsExplainer />
      <BotWorkflow />
      <Validation />
      <Performance />
      <Roadmap />
      <CTA />
      <Footer />
    </main>
  )
}

function Nav() {
  return (
    <nav className="sticky top-0 z-50 backdrop-blur-md bg-[#0B1F3A]/80 border-b border-white/5">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-3 flex items-center justify-between">
        <Link href="/" className="flex items-baseline gap-2 font-semibold text-white">
          KANIDA<span style={{ color: GOLD }}>.AI</span>
        </Link>
        <div className="hidden md:flex items-center gap-6 text-sm text-neutral-300">
          <Link href="/#bots" className="hover:text-white">Product</Link>
          <Link href="/engine" className="text-white">Engine</Link>
          <Link href="/#pricing" className="hover:text-white">Pricing</Link>
          <Link href="/login" className="hover:text-white">Login</Link>
        </div>
        <Link href="/login"
          className="px-4 py-2 rounded-md text-sm font-semibold text-neutral-950"
          style={{ backgroundColor: GOLD }}>
          Start Free Trial →
        </Link>
      </div>
    </nav>
  )
}

// ─── Hero ───────────────────────────────────────────────────────────────────
function Hero() {
  return (
    <section className="relative overflow-hidden border-b border-white/5">
      <div className="absolute inset-0 bg-gradient-to-br from-[#0B1F3A] via-[#0E1116] to-[#0B1F3A]" />
      <div className="relative max-w-4xl mx-auto px-4 md:px-6 pt-20 md:pt-28 pb-16 md:pb-20">
        <div className="text-xs uppercase tracking-[0.2em] mb-4" style={{ color: GOLD }}>
          The thinking behind KANIDA
        </div>
        <h1 className="text-4xl md:text-6xl font-bold leading-[1.05] text-white">
          Inside the engine.<br/>
          <span style={{ color: GOLD }}>The 7th generation, explained.</span>
        </h1>
        <p className="mt-6 text-base md:text-lg text-neutral-300 leading-relaxed max-w-2xl">
          We won't show you the code. But we'll tell you exactly how the engine thinks —
          and why it works when most quant tools don't.
        </p>
      </div>
    </section>
  )
}

// ─── Thesis: why Pattern Intelligence ───────────────────────────────────────
function Thesis() {
  return (
    <section className="border-b border-white/5">
      <div className="max-w-4xl mx-auto px-4 md:px-6 py-20 md:py-24 space-y-8">
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          Why <span style={{ color: GOLD }}>Pattern Intelligence</span> exists
        </h2>
        <div className="space-y-6 text-neutral-300 text-base md:text-lg leading-relaxed">
          <p>
            For 30 years, every serious quant tool has tried the same thing: <em>predict</em> tomorrow's price.
            Screeners filter. Indicators flash. ML models forecast. Deep nets approximate. Multi-factor
            models attribute. They all share the same bet — that yesterday tells you tomorrow.
          </p>
          <p className="text-white">
            We made a different bet.
          </p>
          <p>
            Markets repeat <em>behavior</em>, not prices. The way a stock breathes — its compression,
            its volume signature, its volatility footprint, its position in a regime — those things
            recur. The price target after they recur is unknowable. But the <em>behavior that precedes
            a real move</em> is learnable, and detectable in real time.
          </p>
          <p>
            <strong style={{ color: GOLD }}>That's Pattern Intelligence.</strong> We don't predict.
            We recognize. And we only act when the recognition has been validated across years of out-of-sample
            data — not curve-fit in a notebook.
          </p>
        </div>
      </div>
    </section>
  )
}

// ─── Behavioral Atoms ───────────────────────────────────────────────────────
function AtomsExplainer() {
  return (
    <section className="border-b border-white/5 bg-[#0E1116]">
      <div className="max-w-5xl mx-auto px-4 md:px-6 py-20 md:py-24">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          The atomic unit
        </div>
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          What is a <span style={{ color: GOLD }}>Behavioral Atom</span>?
        </h2>
        <p className="mt-4 max-w-3xl text-base md:text-lg text-neutral-300 leading-relaxed">
          The smallest unit of market behavior the engine recognizes. Each atom captures
          a fingerprint of <em>how</em> a stock is moving — not just its direction.
        </p>

        <div className="mt-12 grid md:grid-cols-2 gap-8 items-center">
          <AtomVisual />
          <ul className="space-y-4 text-sm md:text-base">
            {[
              { k: 'Compression',  v: 'How tight is the recent range vs. its volatility floor?' },
              { k: 'Volume signature', v: 'Is this a quiet hunt or a noisy stampede?' },
              { k: 'Volatility regime', v: 'Calm, trending, breakout, or breakdown?' },
              { k: 'Structure',    v: 'Higher lows, basing, or breakdown skeleton?' },
              { k: 'Momentum phase', v: 'Early, accelerating, exhausted?' },
              { k: 'Position context', v: 'Where in the multi-month range is the stock?' },
            ].map(it => (
              <li key={it.k} className="flex items-start gap-3 border-l-2 pl-4" style={{ borderColor: GOLD }}>
                <div>
                  <div className="font-semibold text-white">{it.k}</div>
                  <div className="text-neutral-400">{it.v}</div>
                </div>
              </li>
            ))}
          </ul>
        </div>

        <div className="mt-12 rounded-xl bg-white/[0.03] border border-white/10 p-6">
          <div className="text-sm uppercase tracking-wider mb-3" style={{ color: GOLD }}>
            How atoms become signals
          </div>
          <p className="text-neutral-300 leading-relaxed text-sm md:text-base">
            Falcon decomposes every historical price move into atoms, then asks one question: which
            <em> combinations </em>of atoms preceded a meaningful move? The combinations that survive
            multiple stress tests — across years, across sectors, across regimes — get promoted into
            the live engine. The rest are discarded. <strong className="text-white">No combination
            survives unless it works out-of-sample.</strong>
          </p>
        </div>
      </div>
    </section>
  )
}

function AtomVisual() {
  return (
    <svg viewBox="0 0 320 280" className="w-full h-auto">
      {/* Central nucleus */}
      <circle cx="160" cy="140" r="40" fill={NAVY} stroke={GOLD} strokeWidth="2" />
      <text x="160" y="138" textAnchor="middle" fontSize="11" fill="white" fontWeight="600">ATOM</text>
      <text x="160" y="152" textAnchor="middle" fontSize="9" fill={GOLD}>fingerprint</text>
      {/* Orbital electrons */}
      {[
        { angle:  0, label: 'compress' },
        { angle: 60, label: 'volume' },
        { angle:120, label: 'vol regime' },
        { angle:180, label: 'structure' },
        { angle:240, label: 'momentum' },
        { angle:300, label: 'context' },
      ].map(o => {
        const rad = (o.angle - 90) * Math.PI / 180
        const r = 100
        const cx = 160 + r * Math.cos(rad)
        const cy = 140 + r * Math.sin(rad)
        return (
          <g key={o.label}>
            <line x1="160" y1="140" x2={cx} y2={cy}
              stroke={GOLD} strokeOpacity="0.3" strokeDasharray="2 3" />
            <circle cx={cx} cy={cy} r="14" fill={NAVY} stroke={GOLD} strokeOpacity="0.8" />
            <text x={cx} y={cy + 3} textAnchor="middle" fontSize="7" fill="white">{o.label}</text>
          </g>
        )
      })}
      {/* Outer orbit ring */}
      <circle cx="160" cy="140" r="100" fill="none" stroke={GOLD} strokeOpacity="0.15" />
    </svg>
  )
}

// ─── Bot workflow — the operator's day ──────────────────────────────────────
function BotWorkflow() {
  const stages = [
    {
      time: '24/7',
      icon: '🦅',
      bot:  'FALCON',
      title: 'Mines, ranks, promotes',
      body: "Continuously hunts the historical record for combinations of atoms that preceded real moves. Anything that fails out-of-sample validation is dropped. The library of patterns is alive — refined every week.",
    },
    {
      time: '15:30 — 17:00 IST',
      icon: '🎯',
      bot:  'HUNTER',
      title: 'Today\'s scan, ranked',
      body: "After market close, Hunter sweeps every NSE stock against the live pattern library. Conviction-scored, sector-aware, deduplicated. The Intelligence Drop arrives in your hand before dinner.",
    },
    {
      time: 'Tonight',
      icon: '🧠',
      bot:  'YOU',
      title: 'Review, edit, confirm',
      body: "Open the Pre-Market console. Review tomorrow's setups. Override SLs if you want. Click Confirm — that's the only friction point. The engine handles the rest.",
    },
    {
      time: '09:14 IST tomorrow',
      icon: '🛡️',
      bot:  'SENTINEL',
      title: 'Fires, monitors, trails',
      body: "At market open, Sentinel places confirmed orders on Zerodha. Auto-protects every position. Trails winners. Cuts losers at the rule. You don't open the chart. You don't need to.",
    },
    {
      time: 'Anytime',
      icon: '💬',
      bot:  'COMPANION',
      title: 'Ask anything',
      body: "Why did this stock fire? What's the conviction floor today? Did this pattern work in 2024? Ask in plain English — get a data-backed answer. The reference desk that never sleeps.",
    },
  ]
  return (
    <section className="border-b border-white/5">
      <div className="max-w-5xl mx-auto px-4 md:px-6 py-20 md:py-24">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          Your day, with the bots
        </div>
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          What each bot does — <span style={{ color: GOLD }}>and when</span>.
        </h2>

        <div className="mt-12 relative">
          {/* Vertical line connecting timeline */}
          <div className="absolute left-4 md:left-6 top-2 bottom-2 w-px bg-white/10" />
          <div className="space-y-8">
            {stages.map(s => (
              <div key={s.title} className="relative pl-12 md:pl-16">
                <div
                  className="absolute left-0 top-0 w-8 h-8 md:w-12 md:h-12 rounded-full flex items-center justify-center text-xl md:text-2xl"
                  style={{ backgroundColor: NAVY, border: `2px solid ${GOLD}` }}>
                  {s.icon}
                </div>
                <div className="text-[11px] uppercase tracking-wider font-mono" style={{ color: GOLD }}>
                  {s.time} · {s.bot}
                </div>
                <div className="mt-1 text-xl md:text-2xl font-bold text-white">{s.title}</div>
                <p className="mt-2 text-sm md:text-base text-neutral-400 leading-relaxed">{s.body}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}

// ─── Validation methodology ─────────────────────────────────────────────────
function Validation() {
  const checks = [
    {
      title: 'Out-of-sample by default',
      body:  'No pattern goes live until it survives data the model has never seen. We test the future on the past, then the past on the future.',
    },
    {
      title: 'Walk-forward, not curve-fit',
      body:  'The validation window slides forward through every market year. A pattern that only worked in 2021 dies. A pattern that worked 2021-2025 ships.',
    },
    {
      title: 'Regime stress tests',
      body:  'Every promoted pattern is replayed through the 2023 Adani crisis, 2024 election volatility, Q4 selloffs, and rotational ranges. If it breaks in a regime, it doesn\'t ship.',
    },
    {
      title: 'Live A/B against random',
      body:  'Hunter\'s daily picks are tracked against a baseline of random-pick portfolios from the same universe. The edge has to be real — not just lucky.',
    },
    {
      title: 'Continuous re-validation',
      body:  "Markets evolve. So does the engine. Patterns are re-mined weekly. Anything that has degraded gets demoted. New behavior gets promoted. The library is never stale.",
    },
    {
      title: 'No black-box claims',
      body:  "Every Hunter pick has a plain-English reason — what fired, why, with what conviction. If we can't explain it, we don't ship it.",
    },
  ]
  return (
    <section className="border-b border-white/5 bg-[#0E1116]">
      <div className="max-w-5xl mx-auto px-4 md:px-6 py-20 md:py-24">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          How we validate
        </div>
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          Six guards against <span style={{ color: GOLD }}>false confidence</span>.
        </h2>
        <p className="mt-4 max-w-3xl text-base text-neutral-400 leading-relaxed">
          The hardest part of quant isn't building a model that works on yesterday's data.
          It's killing the ones that only worked yesterday. Here's our rigor.
        </p>
        <div className="mt-12 grid md:grid-cols-2 gap-4">
          {checks.map(c => (
            <div key={c.title} className="rounded-xl bg-white/[0.03] border border-white/10 p-6">
              <div className="text-base font-bold text-white mb-2">{c.title}</div>
              <p className="text-sm text-neutral-400 leading-relaxed">{c.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

// ─── Live performance ───────────────────────────────────────────────────────
function Performance() {
  return (
    <section className="border-b border-white/5">
      <div className="max-w-4xl mx-auto px-4 md:px-6 py-20 md:py-24 text-center">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          Transparency, not testimonials
        </div>
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          Watch the engine work — <span style={{ color: GOLD }}>live</span>.
        </h2>
        <p className="mt-6 max-w-2xl mx-auto text-base md:text-lg text-neutral-300 leading-relaxed">
          We don't post screenshots of cherry-picked winners. Every Hunter pick, every Sentinel
          trade, every outcome — published continuously on a public dashboard. The good days
          and the bad ones.
        </p>
        <Link
          href="/login"
          className="mt-10 inline-block px-6 py-3 rounded-md text-base font-bold text-neutral-950"
          style={{ backgroundColor: GOLD }}>
          See the live track record →
        </Link>
        <p className="mt-4 text-[11px] text-neutral-600">
          * Past performance does not guarantee future returns. All investment in securities is subject to market risks.
        </p>
      </div>
    </section>
  )
}

// ─── Roadmap (without specifics) ────────────────────────────────────────────
function Roadmap() {
  const items = [
    {
      tag:  'Live',
      tone: 'green',
      title: 'NSE equities — Nifty 500',
      body: 'Pattern library covers the full Nifty 500 universe today.',
    },
    {
      tag:  'Q3 2026',
      tone: 'amber',
      title: 'Sector-rotation copilot',
      body: 'Companion learns to answer "which sector should I be in this month" with data-backed reasoning.',
    },
    {
      tag:  'Q4 2026',
      tone: 'amber',
      title: 'Options awareness',
      body: 'Hunter cross-references equity setups with options flow as a confirmation filter (not a derivatives trader).',
    },
    {
      tag:  '2027',
      tone: 'neutral',
      title: 'BSE + select international',
      body: 'Same engine, expanded universe. Selective international markets where the data quality permits the same rigor.',
    },
  ]
  return (
    <section className="border-b border-white/5 bg-[#0E1116]">
      <div className="max-w-4xl mx-auto px-4 md:px-6 py-20 md:py-24">
        <div className="text-xs uppercase tracking-[0.2em] mb-3" style={{ color: GOLD }}>
          What's next
        </div>
        <h2 className="text-3xl md:text-4xl font-bold text-white">
          The roadmap, <span style={{ color: GOLD }}>honestly</span>.
        </h2>
        <div className="mt-12 space-y-4">
          {items.map(it => (
            <div key={it.title} className="flex items-start gap-4">
              <span className={
                'mt-1 px-2 py-0.5 rounded text-[10px] font-mono whitespace-nowrap '
                + (it.tone === 'green'  ? 'bg-green-500/20 text-green-300 border border-green-500/40'
                :  it.tone === 'amber'  ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
                :                          'bg-neutral-700/30 text-neutral-400 border border-neutral-700')
              }>
                {it.tag}
              </span>
              <div>
                <div className="text-base md:text-lg font-bold text-white">{it.title}</div>
                <p className="mt-1 text-sm text-neutral-400 leading-relaxed">{it.body}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

function CTA() {
  return (
    <section className="bg-gradient-to-br from-[#0B1F3A] to-[#0E1116] border-b border-white/5">
      <div className="max-w-3xl mx-auto px-4 md:px-6 py-20 md:py-28 text-center">
        <h2 className="text-3xl md:text-5xl font-bold text-white leading-tight">
          The engine is built. <span style={{ color: GOLD }}>Try it.</span>
        </h2>
        <p className="mt-6 text-base md:text-lg text-neutral-300 leading-relaxed">
          Seven days, no credit card. See an Intelligence Drop. Talk to Companion. Decide for yourself.
        </p>
        <Link
          href="/login"
          className="mt-10 inline-block px-8 py-4 rounded-md text-lg font-bold text-neutral-950"
          style={{ backgroundColor: GOLD }}>
          Start your 7-day free trial →
        </Link>
      </div>
    </section>
  )
}

function Footer() {
  return (
    <footer className="bg-[#06101F]">
      <div className="max-w-6xl mx-auto px-4 md:px-6 py-10 text-xs text-neutral-600">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            KANIDA<span style={{ color: GOLD }}>.AI</span> · The 7th generation of stock intelligence
          </div>
          <div className="flex gap-4">
            <Link href="/" className="hover:text-neutral-300">Home</Link>
            <Link href="/#pricing" className="hover:text-neutral-300">Pricing</Link>
            <Link href="/login" className="hover:text-neutral-300">Login</Link>
          </div>
        </div>
        <div className="mt-6 pt-6 border-t border-white/5 leading-relaxed">
          KANIDA.AI is a research and analytics platform. Investments in securities markets are subject to
          market risks. Read all related documents carefully before investing. Past performance does not
          guarantee future returns. SEBI RA Reg. No. (in process). © 2026 KANIDA.AI · Built in Bharat 🇮🇳
        </div>
      </div>
    </footer>
  )
}
