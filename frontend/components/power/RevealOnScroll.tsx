/**
 * RevealOnScroll — IntersectionObserver-based fade-in wrapper.
 *
 * Operator brief (Sprint 5d v2 hero):
 *   "Scroll-trigger fade-in: each major section (hero / pain block /
 *    3-cards / close) fades in + translateY(20px) → 0 when scrolled into
 *    view. Stagger by 100ms. Reduced-motion: kill ALL animations except
 *    CTA hover."
 *
 * Implementation notes
 * ────────────────────
 * • Pure Client Component — uses IntersectionObserver
 * • `threshold: 0.15` — fire when 15% of the section enters viewport
 * • `once` is true by default — animation runs once per page-load, then the
 *   element stays visible (prevents the flicker of re-firing on scroll-back)
 * • `prefers-reduced-motion`: skip the observer entirely; render visible
 *   immediately. The CSS keyframe is also disabled at the media-query level
 *   as a belt-and-braces safeguard.
 * • Server-side render: outputs the un-revealed state. Client hydration then
 *   wires up the observer. For the FIRST viewport-visible section this can
 *   produce a brief "flash of invisible content" — to mitigate, the root
 *   element starts at `opacity-0` only AFTER the observer registers, via
 *   a `mounted` flag. Pre-hydration users (rare) see content fully visible.
 */
'use client'

import { useEffect, useRef, useState } from 'react'

type Props = {
  children:    React.ReactNode
  delayMs?:    number          // initial stagger from when in-view fires
  className?:  string
  as?:         'div' | 'section' | 'header' | 'article'
}

export function RevealOnScroll({
  children,
  delayMs = 0,
  className = '',
  as: Tag = 'div',
}: Props) {
  const ref = useRef<HTMLElement | null>(null)
  const [mounted, setMounted] = useState(false)
  const [visible, setVisible] = useState(false)

  useEffect(() => {
    setMounted(true)

    // Respect reduced-motion: skip observer, render visible.
    const prefersReduce =
      typeof window !== 'undefined' &&
      window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
    if (prefersReduce) {
      setVisible(true)
      return
    }

    const el = ref.current
    if (!el) return

    const obs = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (e.isIntersecting) {
            // Stagger by the configured delay
            window.setTimeout(() => setVisible(true), delayMs)
            obs.disconnect()
            break
          }
        }
      },
      { threshold: 0.15, rootMargin: '0px 0px -40px 0px' }
    )
    obs.observe(el)
    return () => obs.disconnect()
  }, [delayMs])

  // SSR + pre-hydration: render visible to avoid invisible-flash for users
  // with JS disabled or before hydration completes. Once mounted, switch to
  // controlled visibility.
  const cls = mounted
    ? `${className} transition-all duration-700 ease-out ${
        visible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-5'
      }`
    : className

  // TypeScript gymnastics for polymorphic `as` prop with a forwarded ref
  return (
    <Tag
      ref={ref as React.RefObject<HTMLDivElement>}
      className={cls}
    >
      {children}
    </Tag>
  )
}
