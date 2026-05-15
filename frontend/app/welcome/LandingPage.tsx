'use client'

// KANIDA.AI landing — v2 invention
// One viewport. No scroll. ~12 total words of copy.
// Living constellation does the talking. Premium AI-system aesthetic.
//
// Inspirations: Anthropic.com minimalism, Bloomberg-terminal scan, observatory chart.
// Rule: every word costs 1000 credits.

import Link from 'next/link'

const GOLD   = '#D4AF37'
const NAVY   = '#0A1628'
const VOID   = '#04080F'

export default function LandingPage() {
  return (
    <main
      className="relative w-screen h-screen overflow-hidden"
      style={{
        background: `radial-gradient(ellipse at 50% 50%, ${NAVY} 0%, ${VOID} 60%, #000 100%)`,
        color: 'white',
      }}>
      {/* Background layers, back-to-front */}
      <ScanLine />
      <ParticleField />
      <Constellation />

      {/* Top: tiny nav */}
      <header className="absolute top-0 left-0 right-0 px-6 md:px-10 py-5 flex items-center justify-between z-20 text-xs tracking-[0.25em] uppercase">
        <Link href="/" className="font-semibold text-white">
          KANIDA<span style={{ color: GOLD }}>.AI</span>
        </Link>
        <div className="flex items-center gap-6 text-neutral-500">
          <Link href="/engine" className="hover:text-white transition">Engine</Link>
          <Link href="/login"  className="hover:text-white transition">Login</Link>
        </div>
      </header>

      {/* Center: the line. */}
      <div className="absolute inset-0 z-10 flex items-center justify-center pointer-events-none">
        <div className="text-center pointer-events-auto px-6">
          <h1
            className="font-bold leading-[0.95] tracking-tight"
            style={{
              fontSize: 'clamp(2.5rem, 7vw, 6rem)',
              color: 'white',
              textShadow: '0 0 40px rgba(212, 175, 55, 0.15)',
            }}>
            Pattern intelligence
            <br/>
            <span style={{
              color: GOLD,
              textShadow: '0 0 30px rgba(212, 175, 55, 0.45)',
            }}>
              for Indian markets.
            </span>
          </h1>
          <Link
            href="/login"
            className="mt-12 inline-flex items-center gap-2 px-8 py-3 text-sm tracking-[0.2em] uppercase font-semibold rounded-full transition-all hover:gap-3"
            style={{
              backgroundColor: GOLD,
              color: NAVY,
              boxShadow: '0 0 40px rgba(212, 175, 55, 0.35), inset 0 0 0 1px rgba(255,255,255,0.1)',
            }}>
            Begin
            <span aria-hidden>→</span>
          </Link>
        </div>
      </div>

      {/* Bottom: bot signature + version */}
      <footer className="absolute bottom-0 left-0 right-0 px-6 md:px-10 py-5 flex items-end justify-between z-20 text-[10px] tracking-[0.3em] uppercase text-neutral-500 font-mono">
        <div className="flex flex-wrap gap-4 md:gap-6">
          <span className="text-neutral-300">Falcon</span>
          <span>·</span>
          <span className="text-neutral-300">Hunter</span>
          <span>·</span>
          <span className="text-neutral-300">Sentinel</span>
          <span>·</span>
          <span className="text-neutral-300">Companion</span>
        </div>
        <div className="hidden md:block">v7 · NSE · Bharat 🇮🇳</div>
      </footer>
    </main>
  )
}

// ─── Living constellation ────────────────────────────────────────────────────
// KANIDA core at center, four bot stars orbiting, lines breathing between them.
// All pure SVG + CSS keyframes. No image assets, no JS animation loop.
function Constellation() {
  return (
    <div className="absolute inset-0 z-0 flex items-center justify-center pointer-events-none">
      <svg
        viewBox="-300 -300 600 600"
        className="w-[120vmin] h-[120vmin] max-w-none opacity-90"
        aria-hidden>
        <defs>
          <radialGradient id="coreGlow">
            <stop offset="0%"  stopColor={GOLD} stopOpacity="0.9" />
            <stop offset="40%" stopColor={GOLD} stopOpacity="0.3" />
            <stop offset="100%" stopColor={GOLD} stopOpacity="0" />
          </radialGradient>
          <radialGradient id="botGlow">
            <stop offset="0%"  stopColor={GOLD} stopOpacity="0.8" />
            <stop offset="100%" stopColor={GOLD} stopOpacity="0" />
          </radialGradient>
        </defs>

        {/* Outer breathing ring */}
        <circle cx="0" cy="0" r="240" fill="none" stroke={GOLD} strokeOpacity="0.06" />
        <circle cx="0" cy="0" r="180" fill="none" stroke={GOLD} strokeOpacity="0.12" />
        <circle cx="0" cy="0" r="120" fill="none" stroke={GOLD} strokeOpacity="0.18" />

        {/* Connection lines: data flow between bots and core */}
        {[0, 90, 180, 270].map((a) => {
          const rad = (a - 90) * Math.PI / 180
          const x = 180 * Math.cos(rad)
          const y = 180 * Math.sin(rad)
          return (
            <line
              key={a}
              x1="0" y1="0" x2={x} y2={y}
              stroke={GOLD} strokeOpacity="0.25" strokeWidth="1"
              strokeDasharray="2 6"
              style={{ animation: `pulse 4s ease-in-out infinite ${a * 11}ms` }}
            />
          )
        })}

        {/* Faint cross-bot lines (forming a square) — the "system is alive" hint */}
        {[
          { x1:  0, y1:-180, x2: 180, y2:  0 },
          { x1:180, y1:   0, x2:   0, y2:180 },
          { x1:  0, y1: 180, x2:-180, y2:  0 },
          { x1:-180, y1:  0, x2:   0, y2:-180 },
        ].map((l, i) => (
          <line
            key={i}
            {...l}
            stroke={GOLD} strokeOpacity="0.08" strokeWidth="0.7"
            strokeDasharray="1 4"
          />
        ))}

        {/* Core glow */}
        <circle cx="0" cy="0" r="80" fill="url(#coreGlow)" />
        <circle cx="0" cy="0" r="6"  fill={GOLD}
          style={{ animation: 'corePulse 3s ease-in-out infinite' }} />

        {/* Four bot nodes — N E S W cardinals */}
        {[
          { angle:   0, label: 'FALCON'    },
          { angle:  90, label: 'HUNTER'    },
          { angle: 180, label: 'SENTINEL'  },
          { angle: 270, label: 'COMPANION' },
        ].map((b) => {
          const rad = (b.angle - 90) * Math.PI / 180
          const x = 180 * Math.cos(rad)
          const y = 180 * Math.sin(rad)
          // Label offset away from core
          const lx = (180 + 28) * Math.cos(rad)
          const ly = (180 + 28) * Math.sin(rad)
          return (
            <g key={b.label}>
              {/* Glow halo */}
              <circle cx={x} cy={y} r="22" fill="url(#botGlow)" />
              {/* Hard dot */}
              <circle cx={x} cy={y} r="3.5" fill={GOLD}
                style={{ animation: `botBlink 4s ease-in-out infinite ${b.angle * 8}ms` }} />
              {/* Label */}
              <text
                x={lx} y={ly + 3}
                textAnchor="middle"
                fontSize="9"
                fontFamily="ui-monospace, monospace"
                fill={GOLD}
                fillOpacity="0.7"
                letterSpacing="2">
                {b.label}
              </text>
            </g>
          )
        })}
      </svg>

      {/* Pulse / blink keyframes — declared inline so the page is self-contained */}
      <style jsx>{`
        @keyframes pulse {
          0%, 100% { stroke-opacity: 0.10; stroke-dashoffset: 0;  }
          50%      { stroke-opacity: 0.55; stroke-dashoffset: 16; }
        }
        @keyframes corePulse {
          0%, 100% {
            r: 6;
            filter: drop-shadow(0 0 12px rgba(212, 175, 55, 0.9));
          }
          50% {
            r: 9;
            filter: drop-shadow(0 0 24px rgba(212, 175, 55, 0.7));
          }
        }
        @keyframes botBlink {
          0%, 100% { opacity: 1;  filter: drop-shadow(0 0 8px rgba(212,175,55,0.8)); }
          50%      { opacity: 0.55; filter: drop-shadow(0 0 14px rgba(212,175,55,1)); }
        }
      `}</style>
    </div>
  )
}

// ─── Drifting particles (very subtle, far back) ─────────────────────────────
function ParticleField() {
  // 16 dim points slowly drifting. Pure CSS.
  const particles = Array.from({ length: 16 }, (_, i) => ({
    id:  i,
    cx:  (i * 37) % 100,
    cy:  (i * 53) % 100,
    r:   0.4 + (i % 3) * 0.3,
    dur: 18 + (i % 5) * 4,
    dly: -(i * 1.3),
  }))
  return (
    <svg className="absolute inset-0 w-full h-full z-0" aria-hidden>
      {particles.map(p => (
        <circle
          key={p.id}
          cx={`${p.cx}%`} cy={`${p.cy}%`} r={p.r}
          fill={GOLD} fillOpacity="0.4"
          style={{
            animation: `drift ${p.dur}s linear infinite ${p.dly}s`,
          }}
        />
      ))}
      <style jsx>{`
        @keyframes drift {
          0%   { transform: translateY(0)    translateX(0); opacity: 0; }
          10%  { opacity: 0.6; }
          90%  { opacity: 0.6; }
          100% { transform: translateY(-30vh) translateX(2vw); opacity: 0; }
        }
      `}</style>
    </svg>
  )
}

// ─── Scan-line sweep (Bloomberg-terminal feel) ──────────────────────────────
function ScanLine() {
  return (
    <div
      className="absolute inset-0 z-0 pointer-events-none"
      style={{
        background: 'linear-gradient(180deg, rgba(212,175,55,0) 0%, rgba(212,175,55,0.025) 50%, rgba(212,175,55,0) 100%)',
        backgroundSize: '100% 8vh',
        animation: 'scan 11s linear infinite',
      }}
    >
      <style jsx>{`
        @keyframes scan {
          0%   { background-position: 0 -8vh; }
          100% { background-position: 0 100vh; }
        }
      `}</style>
    </div>
  )
}
