/**
 * InfoTooltip — lightweight CSS-only tooltip for abbreviation explainers.
 *
 * Operator-requested 2026-05-14: non-trader investors looking at the
 * landing-page featured cards don't know what "WR" or "D+5" mean.
 *
 * Implementation: a small (?) icon next to the label, hover/focus reveals
 * an absolutely-positioned panel. Pure CSS, no portal, no JS state.
 * Works on touch via :focus-within (tap-and-hold reveals).
 */
import type { ReactNode } from 'react'

type Props = {
  text:       ReactNode    // content shown when hovered
  position?:  'top' | 'bottom'
  className?: string
}

export function InfoTooltip({ text, position = 'top', className = '' }: Props) {
  const posClasses =
    position === 'top'
      ? 'bottom-full left-1/2 -translate-x-1/2 mb-1'
      : 'top-full left-1/2 -translate-x-1/2 mt-1'

  return (
    <span className={`relative inline-block group ${className}`} tabIndex={0}>
      <span
        aria-label="More info"
        className="inline-flex items-center justify-center w-3.5 h-3.5 text-[9px] font-bold
                    bg-neutral-700 text-neutral-300 rounded-full
                    cursor-help select-none
                    group-hover:bg-neutral-600 group-hover:text-neutral-100
                    group-focus:bg-neutral-600 group-focus:text-neutral-100"
      >
        ?
      </span>
      <span
        role="tooltip"
        className={[
          'absolute z-20',
          posClasses,
          'w-48 px-2 py-1.5 text-[11px] leading-snug',
          'bg-neutral-900 text-neutral-100 border border-neutral-700 rounded shadow-lg',
          'opacity-0 invisible',
          'group-hover:opacity-100 group-hover:visible',
          'group-focus:opacity-100 group-focus:visible',
          'transition-opacity duration-150',
          'pointer-events-none',
        ].join(' ')}
      >
        {text}
      </span>
    </span>
  )
}
