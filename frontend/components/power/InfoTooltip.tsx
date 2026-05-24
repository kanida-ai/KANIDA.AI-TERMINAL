'use client'

/**
 * InfoTooltip — lightweight tooltip for abbreviation explainers.
 *
 * Operator-requested 2026-05-14: non-trader investors looking at the
 * landing-page featured cards don't know what "WR" or "D+5" mean.
 *
 * Sprint 5c re-fix (Bug 3): the previous CSS-only version had two problems:
 *   1. tabIndex={0} made the tooltip "sticky" after a click — focus didn't
 *      clear when the user clicked elsewhere, so tooltips piled up.
 *   2. In a 3-column grid, w-48 (192px) tooltips overflowed the column
 *      bounds; the rightmost card's tooltip drew over the centre card's
 *      tooltip whenever both ended up visible.
 *
 * This version is React-state-controlled with a single open instance at a
 * time. Hover or focus opens; pointer-leave or blur closes; Escape closes.
 * Width is bounded by viewport with max-w to handle narrow screens.
 */
import { useEffect, useRef, useState, type ReactNode } from 'react'

type Props = {
  text:       ReactNode
  position?:  'top' | 'bottom'
  className?: string
}

export function InfoTooltip({ text, position = 'top', className = '' }: Props) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLSpanElement>(null)

  // Escape closes; click outside closes.
  useEffect(() => {
    if (!open) return
    const onKey  = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('mousedown', onDown)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('mousedown', onDown)
    }
  }, [open])

  const posCls =
    position === 'top'
      ? 'bottom-full left-1/2 -translate-x-1/2 mb-2'
      : 'top-full left-1/2 -translate-x-1/2 mt-2'

  return (
    <span
      ref={ref}
      className={`relative inline-block ${className}`}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <button
        type="button"
        aria-label="More info"
        aria-expanded={open}
        onClick={(e) => { e.preventDefault(); e.stopPropagation(); setOpen(o => !o) }}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        className="inline-flex items-center justify-center w-3.5 h-3.5 text-[9px] font-bold
                    bg-neutral-700 text-neutral-300 rounded-full
                    cursor-help select-none align-middle
                    hover:bg-neutral-600 hover:text-neutral-100
                    focus:bg-neutral-600 focus:text-neutral-100 focus:outline-none"
      >
        ?
      </button>

      {open && (
        <span
          role="tooltip"
          className={[
            'absolute z-50',
            posCls,
            'w-48 max-w-[min(14rem,calc(100vw-2rem))] px-2.5 py-1.5',
            'text-[11px] leading-snug whitespace-normal',
            'bg-neutral-900 text-neutral-100 border border-neutral-700 rounded-md shadow-xl',
            'pointer-events-none',
          ].join(' ')}
        >
          {text}
        </span>
      )}
    </span>
  )
}
