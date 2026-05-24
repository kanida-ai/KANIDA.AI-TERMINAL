/**
 * CompassLogo — KANIDA.AI brand mark (Sprint 5d hero redesign).
 *
 * Pure SVG, no PNG dependency. The outer ring stays fixed; the needle
 * subtly rotates on an 8-second ease-in-out loop. Stops on hover so the
 * surrounding text doesn't compete for attention when the user pauses.
 *
 * Single accent colour (--kanida-gold = #3FE3A4). The needle's south end is
 * slightly muted so the north end reads as "direction" — Apple-grade detail.
 *
 * Marked `'use client'` because `<style jsx>` (styled-jsx) is a Client-only
 * primitive. The parent layout.tsx remains a Server Component — Server
 * Components can render Client Components, so the `await getCurrentUser()`
 * SSR path is unaffected.
 */
'use client'

type Props = {
  size?: number          // CSS px; defaults to 24 (nav) or 28 in hero
  className?: string
}

export function CompassLogo({ size = 24, className = '' }: Props) {
  return (
    <span
      className={['kanida-compass inline-block leading-none', className].join(' ')}
      style={{ width: size, height: size }}
      aria-hidden
    >
      <svg
        viewBox="0 0 32 32"
        width={size}
        height={size}
        xmlns="http://www.w3.org/2000/svg"
      >
        {/* Outer ring */}
        <circle cx="16" cy="16" r="14"
                fill="none"
                stroke="rgba(63, 227, 164,0.85)"
                strokeWidth="1.5" />
        {/* Tiny tick marks at N/E/S/W (very subtle) */}
        <g stroke="rgba(63, 227, 164,0.55)" strokeWidth="1">
          <line x1="16" y1="2"  x2="16" y2="4.5" />
          <line x1="16" y1="27.5" x2="16" y2="30" />
          <line x1="2"  y1="16" x2="4.5"  y2="16" />
          <line x1="27.5" y1="16" x2="30" y2="16" />
        </g>
        {/* Rotating needle — anchored at centre, animation in JSX style block */}
        <g className="kanida-compass-needle" style={{ transformOrigin: '16px 16px' }}>
          {/* North half — bright gold, sharp triangle */}
          <polygon points="16,4 13.5,16 18.5,16"
                    fill="#3FE3A4" />
          {/* South half — muted */}
          <polygon points="16,28 13.5,16 18.5,16"
                    fill="rgba(63, 227, 164,0.35)" />
        </g>
        {/* Centre pin */}
        <circle cx="16" cy="16" r="1.5" fill="#0a0a0a" stroke="#3FE3A4" strokeWidth="0.8" />
      </svg>
      <style jsx>{`
        .kanida-compass-needle {
          animation: kanida-compass-rotate 8s ease-in-out infinite;
          transform-box: fill-box;
          transform-origin: center;
        }
        .kanida-compass:hover .kanida-compass-needle {
          animation-play-state: paused;
        }
        @keyframes kanida-compass-rotate {
          0%   { transform: rotate(0deg);   }
          50%  { transform: rotate(24deg);  }
          100% { transform: rotate(0deg);   }
        }
        @media (prefers-reduced-motion: reduce) {
          .kanida-compass-needle { animation: none; }
        }
      `}</style>
    </span>
  )
}
