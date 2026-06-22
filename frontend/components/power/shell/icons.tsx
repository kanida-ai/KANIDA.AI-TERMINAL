/**
 * Inline stroke icons for the Falcon AppShell nav.
 *
 * No icon dependency is installed in this project (package.json has no
 * lucide/heroicons/react-icons), so the 6-mode nav + footer use these small
 * hand-rolled 24×24 stroke SVGs. They inherit `currentColor` so the active /
 * hover mint state is driven purely by the parent's text color class.
 */
import type { SVGProps } from 'react'

type IconProps = SVGProps<SVGSVGElement> & { size?: number }

function base({ size = 18, ...props }: IconProps) {
  return {
    width: size,
    height: size,
    viewBox: '0 0 24 24',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth: 1.6,
    strokeLinecap: 'round' as const,
    strokeLinejoin: 'round' as const,
    ...props,
  }
}

/** Ask Falcon — spark */
export function IconSpark(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M12 3v4M12 17v4M5 12H1M23 12h-4M6 6l2.5 2.5M18 18l-2.5-2.5M6 18l2.5-2.5M18 6l-2.5 2.5" />
      <circle cx="12" cy="12" r="2.4" />
    </svg>
  )
}

/** Signals — bar/signal */
export function IconSignal(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M4 20V10M10 20V4M16 20v-8M22 20v-5" />
    </svg>
  )
}

/** Co-Trading — handshake (simplified clasp) */
export function IconHandshake(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M3 11l4-4 5 4 5-4 4 4M7 7l4 4M17 7l-4 4M9 15l2 2 2-2 2 2" />
    </svg>
  )
}

/** AutoTrade — bolt */
export function IconBolt(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M13 2L4 14h7l-1 8 9-12h-7l1-8z" />
    </svg>
  )
}

/** Performance — line chart */
export function IconChart(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M3 3v18h18" />
      <path d="M7 14l3-4 3 3 4-7" />
    </svg>
  )
}

/** Plans — lock */
export function IconLock(p: IconProps) {
  return (
    <svg {...base(p)}>
      <rect x="5" y="11" width="14" height="9" rx="2" />
      <path d="M8 11V8a4 4 0 0 1 8 0v3" />
    </svg>
  )
}

/** Account — user */
export function IconUser(p: IconProps) {
  return (
    <svg {...base(p)}>
      <circle cx="12" cy="8" r="3.5" />
      <path d="M5 20a7 7 0 0 1 14 0" />
    </svg>
  )
}

/** Learn — book */
export function IconBook(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M4 5a2 2 0 0 1 2-2h12v16H6a2 2 0 0 0-2 2z" />
      <path d="M4 19a2 2 0 0 1 2-2h12" />
    </svg>
  )
}

/** Admin — sliders */
export function IconAdmin(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M4 6h16M4 12h16M4 18h16" />
      <circle cx="9" cy="6" r="2" />
      <circle cx="15" cy="12" r="2" />
      <circle cx="8" cy="18" r="2" />
    </svg>
  )
}

/** chevron / arrow used in chips + "see all" */
export function IconArrowRight(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M5 12h14M13 6l6 6-6 6" />
    </svg>
  )
}

/** small search glyph for the entity picker */
export function IconSearch(p: IconProps) {
  return (
    <svg {...base(p)}>
      <circle cx="11" cy="11" r="7" />
      <path d="M21 21l-4.3-4.3" />
    </svg>
  )
}

/** bell — used by LaunchPending notify CTA */
export function IconBell(p: IconProps) {
  return (
    <svg {...base(p)}>
      <path d="M6 9a6 6 0 0 1 12 0c0 5 2 6 2 6H4s2-1 2-6" />
      <path d="M10 20a2 2 0 0 0 4 0" />
    </svg>
  )
}
