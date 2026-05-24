/**
 * CompassPersonaOrbit — KANIDA.AI hero visual cluster (Sprint 5d v2).
 *
 * Centerpiece of the redesigned landing page hero. Operator brief:
 *   "Animated compass + 5 orbiting persona cards. Brand-defining; shows
 *    product immediately; unique. Compass needle rotates SLOWLY — one
 *    full rotation per 12 seconds, ease-in-out. Cards have subtle 3-second
 *    offset pulse (border glow) — staggered so only one pulses at a time."
 *
 * Layout
 * ──────
 * Desktop (≥ md):
 *   • Compass 280×280 at centre of a 640×560 stage
 *   • 5 cards positioned pentagonally at radius 240px from compass centre
 *   • Pentagonal angles: 12°, 84°, 156°, 228°, 300° (clockwise from top)
 *     — gives a clean star/orbital feel, not a clock-face
 *
 * Mobile (< md):
 *   • Compass 180×180 centred
 *   • Cards STACK below in a 2-2-1 grid (operator spec)
 *
 * Animations
 * ──────────
 *   • Compass needle: 360° rotation, 12s ease-in-out, infinite
 *   • Card pulse: 12.5s loop, 2.5s visible per card, staggered 2.5s apart
 *     → exactly one card glows at any moment as the pulse "walks" the orbit
 *   • Card hover: translateY(-4px) + brighter border + brighter text
 *   • Compass rotation pauses when ANY card is hovered (operator spec)
 *   • `prefers-reduced-motion`: all keyframe animations killed; hover-only
 *     interactions preserved
 *
 * Returns shown
 * ─────────────
 * Operator-locked headline figures (annual return, fixed-₹ yearly-reset
 * model). If the backtest is re-run with a different config these need to
 * be updated. See PERSONAS array below.
 */
'use client'

import Link from 'next/link'

type Persona = {
  slug:        string
  shortName:   string  // 6-8 chars max for compact cards
  returnPct:   number  // annual return %, signed
  angleDeg:    number  // pentagonal position on desktop orbit (0 = top, clockwise)
}

const PERSONAS: Persona[] = [
  // Top — the flagship; first thing the eye lands on after the compass
  { slug: 'daily-trader',   shortName: 'DAILY',   returnPct: 207, angleDeg:   0 },
  // Top-right — highest absolute return, the "what's possible" anchor
  { slug: 'btst-trader',    shortName: 'BTST',    returnPct: 306, angleDeg:  72 },
  // Bottom-right
  { slug: 'weekly-trader',  shortName: 'WEEKLY',  returnPct: 194, angleDeg: 144 },
  // Bottom-left
  { slug: 'monthly-trader', shortName: 'MONTHLY', returnPct:  36, angleDeg: 216 },
  // Top-left
  { slug: 'patient-trader', shortName: 'PATIENT', returnPct: 136, angleDeg: 288 },
]

const ORBIT_RADIUS_PX  = 240   // distance from compass centre to card centre
const CARD_W_PX        = 140
const CARD_H_PX        = 80
const COMPASS_SIZE_PX  = 280
const STAGE_W_PX       = 760   // wide enough for radius+card on each side
const STAGE_H_PX       = 640

export function CompassPersonaOrbit() {
  return (
    <div className="kanida-orbit-root relative w-full">
      {/* DESKTOP — orbital layout */}
      <div className="hidden md:block">
        <div
          className="relative mx-auto"
          style={{ width: STAGE_W_PX, height: STAGE_H_PX }}
        >
          {/* Compass at centre */}
          <div
            className="absolute"
            style={{
              left:   `calc(50% - ${COMPASS_SIZE_PX / 2}px)`,
              top:    `calc(50% - ${COMPASS_SIZE_PX / 2}px)`,
              width:  COMPASS_SIZE_PX,
              height: COMPASS_SIZE_PX,
            }}
          >
            <CompassHero size={COMPASS_SIZE_PX} />
          </div>

          {/* 5 persona cards on a pentagonal orbit */}
          {PERSONAS.map((p, idx) => {
            const rad = (p.angleDeg - 90) * (Math.PI / 180)   // -90 so 0° = top
            const cx  = Math.cos(rad) * ORBIT_RADIUS_PX
            const cy  = Math.sin(rad) * ORBIT_RADIUS_PX
            return (
              <PersonaOrbitCard
                key={p.slug}
                persona={p}
                styleOverride={{
                  position: 'absolute',
                  left: `calc(50% + ${cx}px - ${CARD_W_PX / 2}px)`,
                  top:  `calc(50% + ${cy}px - ${CARD_H_PX / 2}px)`,
                  width:  CARD_W_PX,
                  height: CARD_H_PX,
                  animationDelay: `${idx * 2.5}s`,
                }}
              />
            )
          })}
        </div>
      </div>

      {/* MOBILE — stacked grid (2-2-1) */}
      <div className="md:hidden">
        <div className="flex justify-center mb-8">
          <CompassHero size={180} />
        </div>
        <div className="grid grid-cols-2 gap-3 max-w-sm mx-auto">
          {PERSONAS.slice(0, 4).map((p, idx) => (
            <PersonaOrbitCard
              key={p.slug}
              persona={p}
              styleOverride={{
                height: 72,
                animationDelay: `${idx * 2.5}s`,
              }}
            />
          ))}
        </div>
        <div className="flex justify-center mt-3 max-w-sm mx-auto">
          <div style={{ width: 'calc(50% - 6px)' }}>
            <PersonaOrbitCard
              persona={PERSONAS[4]}
              styleOverride={{
                height: 72,
                animationDelay: '10s',
                width: '100%',
              }}
            />
          </div>
        </div>
      </div>

      <style jsx>{`
        /* Whole-orbit pause on any-card hover — uses :has() with a fallback. */
        .kanida-orbit-root:hover :global(.kanida-compass-hero-needle) {
          animation-play-state: paused;
        }
        @media (prefers-reduced-motion: reduce) {
          :global(.kanida-orbit-root *) {
            animation: none !important;
          }
        }
      `}</style>
    </div>
  )
}


/* ────────────────────────────────────────────────────────────────────────
 * PersonaOrbitCard — single compact card showing persona name + return%
 * ────────────────────────────────────────────────────────────────────── */
function PersonaOrbitCard({ persona, styleOverride }: {
  persona: Persona
  styleOverride: React.CSSProperties
}) {
  return (
    <Link
      href={`/power/portfolios/${persona.slug}`}
      className="kanida-orbit-card group block rounded-lg bg-[#1a1a1a] border border-mint-500/20 transition-all duration-200 ease-out hover:-translate-y-1 hover:border-mint-400/60 hover:shadow-[0_0_20px_-2px_rgba(63, 227, 164,0.4)] flex flex-col justify-center items-center text-center px-2"
      style={styleOverride}
    >
      <span className="text-[11px] md:text-[12px] font-semibold uppercase tracking-[0.12em] text-white/70 group-hover:text-white transition-colors">
        {persona.shortName}
      </span>
      <span className="text-[20px] md:text-[24px] font-bold text-mint-400 leading-none mt-1.5 group-hover:text-mint-300 transition-colors">
        {persona.returnPct >= 0 ? '+' : ''}{persona.returnPct}%
      </span>
      <span className="text-[9px] md:text-[10px] uppercase tracking-[0.1em] text-white/40 mt-1">
        /year
      </span>

      <style jsx>{`
        .kanida-orbit-card {
          animation: kanida-card-pulse 12.5s ease-in-out infinite;
        }
        @keyframes kanida-card-pulse {
          0%, 100% {
            box-shadow: 0 0 0 0 rgba(63, 227, 164, 0);
            border-color: rgba(63, 227, 164, 0.20);
          }
          /* Each card "glows" for ~2.5s of its 12.5s cycle (= 20%).
             Staggered animation-delay on each card → only one glows at a time. */
          5% {
            box-shadow: 0 0 24px -2px rgba(63, 227, 164, 0.45);
            border-color: rgba(63, 227, 164, 0.80);
          }
          15% {
            box-shadow: 0 0 24px -2px rgba(63, 227, 164, 0.45);
            border-color: rgba(63, 227, 164, 0.80);
          }
          20% {
            box-shadow: 0 0 0 0 rgba(63, 227, 164, 0);
            border-color: rgba(63, 227, 164, 0.20);
          }
        }
      `}</style>
    </Link>
  )
}


/* ────────────────────────────────────────────────────────────────────────
 * CompassHero — large SVG compass for the hero visual cluster.
 *
 * Distinct from the nav-bar CompassLogo: this one does a full 360° rotation
 * over 12 seconds (vs. the nav's subtle 24° back-and-forth). Visible at
 * 280×280, the cardinal tick marks and the directional needle are clearly
 * legible — a small brand moment.
 * ────────────────────────────────────────────────────────────────────── */
function CompassHero({ size }: { size: number }) {
  return (
    <div
      className="kanida-compass-hero relative"
      style={{ width: size, height: size }}
      aria-hidden
    >
      <svg
        viewBox="0 0 200 200"
        width={size}
        height={size}
        xmlns="http://www.w3.org/2000/svg"
      >
        {/* Outer ring — slightly thicker for the hero scale */}
        <circle cx="100" cy="100" r="92"
                fill="none"
                stroke="rgba(63, 227, 164,0.7)"
                strokeWidth="2" />
        {/* Inner ring — subtle depth */}
        <circle cx="100" cy="100" r="78"
                fill="none"
                stroke="rgba(63, 227, 164,0.18)"
                strokeWidth="1" />

        {/* Cardinal tick marks (N/E/S/W — long) and inter-cardinal (short) */}
        <g stroke="rgba(63, 227, 164,0.55)" strokeWidth="1.5">
          <line x1="100" y1="8"    x2="100" y2="22" />
          <line x1="100" y1="178"  x2="100" y2="192" />
          <line x1="8"   y1="100"  x2="22"  y2="100" />
          <line x1="178" y1="100"  x2="192" y2="100" />
        </g>
        <g stroke="rgba(63, 227, 164,0.30)" strokeWidth="1">
          {Array.from({ length: 8 }).map((_, i) => {
            const a = (i * 45 + 22.5) * (Math.PI / 180)
            const x1 = 100 + Math.cos(a) * 88
            const y1 = 100 + Math.sin(a) * 88
            const x2 = 100 + Math.cos(a) * 94
            const y2 = 100 + Math.sin(a) * 94
            return <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} />
          })}
        </g>

        {/* Cardinal letters — subtle, gold */}
        <g fill="rgba(63, 227, 164,0.7)"
           fontFamily="ui-sans-serif, system-ui"
           fontSize="11"
           fontWeight="700"
           textAnchor="middle">
          <text x="100" y="40"   dy="0">N</text>
          <text x="100" y="167"  dy="0">S</text>
          <text x="38"  y="104"  dy="0">W</text>
          <text x="162" y="104"  dy="0">E</text>
        </g>

        {/* Rotating needle */}
        <g className="kanida-compass-hero-needle" style={{ transformOrigin: '100px 100px' }}>
          {/* North half — bright gold, sharp triangle */}
          <polygon points="100,18 88,100 112,100"
                    fill="#3FE3A4" />
          {/* South half — muted */}
          <polygon points="100,182 88,100 112,100"
                    fill="rgba(63, 227, 164,0.30)" />
        </g>

        {/* Centre pin */}
        <circle cx="100" cy="100" r="6"
                fill="#0a0a0a"
                stroke="#3FE3A4"
                strokeWidth="1.5" />
        <circle cx="100" cy="100" r="2"
                fill="#3FE3A4" />
      </svg>

      <style jsx>{`
        .kanida-compass-hero :global(.kanida-compass-hero-needle) {
          animation: kanida-compass-hero-rotate 12s ease-in-out infinite;
          transform-box: fill-box;
          transform-origin: center;
        }
        @keyframes kanida-compass-hero-rotate {
          0%   { transform: rotate(0deg);   }
          100% { transform: rotate(360deg); }
        }
        @media (prefers-reduced-motion: reduce) {
          .kanida-compass-hero :global(.kanida-compass-hero-needle) {
            animation: none;
          }
        }
      `}</style>
    </div>
  )
}
